"""Per-run projection of background-task namespaces onto the replay stream."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

from langchain_core.messages import ToolMessage

from src.server.database.runs import subagent_runs as sr_db
from src.server.services.history import projector
from src.server.services.history import replay
from src.server.services.history.reader import CheckpointHistoryReader
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.replay import items
from src.server.services.history.replay import segment_claim
from src.server.services.history.replay import stored_merge
from src.server.services.history.task_status import resolve_task_details

logger = logging.getLogger(__name__)


class TaskLaneProjector:
    """Per-run projection of background-task namespaces.

    A task namespace holds every run's transcript back-to-back; each run
    opens at a plain HumanMessage (its spawn/resume input). Launch artifacts
    (action ``init``/``resume``) in the main transcript attribute to run
    segments in order. Ledgered launches join exactly: the artifact's
    ``task_run_id`` matches the stamp the run's input boundary carries in
    checkpoint metadata, and the run ledger decides projection (``in_progress``
    runs belong to their live stream). Pre-ledger launches verify by content:
    a segment's boundary HumanMessage carries the launch prompt verbatim, so
    a launch only claims a segment whose opener matches its prompt. A launch
    with no matching segment projects nothing — either its boundary isn't
    checkpointed yet (the live stream owns that run until the next rebuild)
    or the run never wrote one (a failed/no-op launch); blind positional
    pairing would hand it the NEXT run's transcript. ``update`` (steering)
    artifacts never launch a run.

    Windowed builds may start after a task's init: the cursor then starts at
    ``offset`` (the leading segments belong to out-of-window turns). Any
    namespace read failure makes checkpoint replay unavailable so
    ``source=auto`` can use the complete stored-SSE fallback.
    """

    def __init__(self, thread_id: str, *, windowed: bool):
        self._thread_id = thread_id
        self._windowed = windowed
        self._tasks: dict[str, segment_claim.TaskRuns] = {}
        self._run_started: dict[str, float] = {}
        # Turn indexes whose stamps carry trailing salvage (populated by
        # trailing_items) — those turns must not be cached, or the fast path
        # would replay them without the salvage.
        self.salvaged_turn_indexes: set[Any] = set()
        # task_id -> max started_at (epoch ms) over ledgered runs whose
        # segment THIS build claimed. Stamped onto the turn's task artifacts
        # as ``projected_run_started_ms``: the client's authority for which
        # runs its history payload already contains. Derived from the claim
        # act itself — never from a separate ledger read, which can name a
        # run the projection skipped (its skip decision and this watermark
        # must share one snapshot).
        self.claimed_watermarks: dict[str, float] = {}
        # Lanes claimed in the CURRENT turn whose run died mid-write
        # (_LOSSY_TERMINAL_STATUSES): the stored copy may hold output the
        # checkpoint never committed, so the merge may resurrect their
        # trailing rows. Reset by each project_for_turn call.
        self.turn_lossy_lanes: set[str] = set()

    @staticmethod
    def _launches_in(turn: Any) -> list[tuple[str, str, str | None, str | None]]:
        """Ordered ``(task_id, action, prompt, task_run_id)`` launch artifacts
        in a turn. ``task_run_id`` is None on pre-ledger data."""
        launches: list[tuple[str, str, str | None, str | None]] = []
        for message in turn.messages:
            if not isinstance(message, ToolMessage):
                continue
            artifact = (message.additional_kwargs or {}).get("task_artifact")
            if not isinstance(artifact, dict) or not artifact.get("task_id"):
                continue
            action = artifact.get("action", "init")
            if action in ("init", "resume"):
                prompt = artifact.get("prompt")
                run_id = artifact.get("task_run_id")
                launches.append(
                    (
                        str(artifact["task_id"]),
                        action,
                        prompt.strip() if isinstance(prompt, str) else None,
                        str(run_id) if run_id else None,
                    )
                )
        return launches

    async def prepare(
        self, reader: CheckpointHistoryReader, pairs: list[tuple[Any, Any]]
    ) -> None:
        launch_actions: dict[str, list[str]] = {}
        for _, turn in pairs:
            if turn is None:
                continue
            for task_id, action, _prompt, _run_id in self._launches_in(turn):
                launch_actions.setdefault(task_id, []).append(action)
        if not launch_actions:
            return

        task_ids = list(launch_actions)
        histories = await asyncio.gather(
            *(reader.aget_task_history(self._thread_id, tid) for tid in task_ids),
            return_exceptions=True,
        )
        stamps_by_task, run_status = await self._load_ledger(reader, task_ids)
        for task_id, history in zip(task_ids, histories):
            if isinstance(history, BaseException):
                logger.warning(
                    "[REPLAY] Failed to read subagent checkpoint state task:%s",
                    task_id,
                    exc_info=(type(history), history, history.__traceback__),
                )
                # Silent continuation would produce a plausible-looking but
                # incomplete transcript and bypass the endpoint's SSE fallback.
                raise replay.CheckpointReplayUnavailable(
                    f"subagent checkpoint state unavailable for task:{task_id}"
                ) from history
            segments = segment_claim.split_run_segments(history.messages)
            actions = launch_actions[task_id]
            # A window that opens on a resume is missing the older runs'
            # launches; their segments are skipped, not re-attributed. A full
            # build always sees the init, so its cursor starts at segment 0.
            cursor = (
                max(0, len(segments) - len(actions))
                if self._windowed and actions[0] != "init"
                else 0
            )
            self._tasks[task_id] = segment_claim.TaskRuns(
                history=history,
                segments=segments,
                cursor=cursor,
                remaining_launches=len(actions),
                stamps=stamps_by_task.get(task_id, []),
                run_status=run_status,
            )

        # Same liveness truth that stamps card status (advisory-lock probe):
        # a task is live only while its writer provably runs, so an expired
        # stream never demotes a settled run's transcript. On probe failure
        # nothing is marked live — availability over precision (a transient
        # duplicate beats a missing transcript).
        try:
            details = await resolve_task_details(
                self._thread_id, list(self._tasks)
            )
        except Exception:
            logger.warning(
                "[REPLAY] task liveness probe failed for %s",
                self._thread_id,
                exc_info=True,
            )
            details = {}
        for task_id, runs in self._tasks.items():
            runs.live = (details.get(task_id) or {}).get("status") == "running"

    async def _load_ledger(
        self, reader: CheckpointHistoryReader, task_ids: list[str]
    ) -> tuple[dict[str, list[str | None]], dict[str, str]]:
        """Boundary stamps per task + a thread-wide run_id -> status map.

        Both reads are best-effort: any failure (and readers without the
        stamp walk — test fakes) yields empty results, which routes every
        claim through the legacy content-matching path.
        """
        stamps_by_task: dict[str, list[str | None]] = {}
        run_status: dict[str, str] = {}
        # iscoroutinefunction, not truthiness: test fakes are spec'd mocks
        # whose auto-created attribute is sync — calling it would hand
        # asyncio.gather a non-awaitable. (AsyncMock passes the check.)
        stamp_walk = getattr(reader, "aget_task_run_stamps", None)
        if stamp_walk is not None and inspect.iscoroutinefunction(stamp_walk):
            results = await asyncio.gather(
                *(stamp_walk(self._thread_id, tid) for tid in task_ids),
                return_exceptions=True,
            )
            for task_id, stamps in zip(task_ids, results):
                if isinstance(stamps, BaseException):
                    logger.warning(
                        "[REPLAY] task run-stamp walk failed for task:%s",
                        task_id,
                        exc_info=(type(stamps), stamps, stamps.__traceback__),
                    )
                else:
                    stamps_by_task[task_id] = stamps
        try:
            runs = await sr_db.list_runs_for_thread(self._thread_id)
            run_status = {
                str(r["task_run_id"]): str(r["status"]) for r in runs
            }
            self._run_started = {
                str(r["task_run_id"]): r["started_at"].timestamp() * 1000.0
                for r in runs
                if r.get("started_at") is not None
            }
        except Exception:
            logger.warning(
                "[REPLAY] run-ledger read failed for %s",
                self._thread_id,
                exc_info=True,
            )
        return stamps_by_task, run_status

    def project_for_turn(
        self, turn: Any, turn_index: Any, response_id: str | None
    ) -> tuple[list[dict[str, Any]], set[str]]:
        """Items for every run launched in this turn, plus the launched task
        ids (the caller's cache guard: a turn that launched a still-writing
        run must not be cached)."""
        out: list[dict[str, Any]] = []
        launched: set[str] = set()
        self.turn_lossy_lanes = set()
        for task_id, _action, prompt, run_id in self._launches_in(turn):
            runs = self._tasks.get(task_id)
            if runs is None:
                continue
            launched.add(task_id)
            task_agent = f"task:{task_id}"
            runs.last_ctx = (turn_index, response_id)
            runs.remaining_launches -= 1
            status = runs.run_status.get(run_id) if run_id else None
            if status == "in_progress":
                # The ledger says this exact run is still executing: its
                # stream replays the epoch (opener included) from seq 1, so
                # claiming the segment here would render it twice. The turn
                # stays uncached (launched set + live stream), so the settled
                # rebuild projects it normally.
                continue
            if status is None and runs.live and runs.remaining_launches <= 0:
                # Legacy gate (no ledger row for this launch): without a
                # per-run status, only the final launch of a live task can be
                # the in-flight run.
                continue
            segment = (
                segment_claim.claim_segment_by_stamp(runs, run_id)
                if run_id and status is not None
                else None
            )
            if segment is None:
                segment = segment_claim.claim_segment(runs, prompt)
            if segment is None:
                continue
            started = self._run_started.get(run_id) if run_id else None
            if started is not None:
                prev = self.claimed_watermarks.get(task_id)
                self.claimed_watermarks[task_id] = (
                    started if prev is None else max(prev, started)
                )
            if status in stored_merge._LOSSY_TERMINAL_STATUSES:
                self.turn_lossy_lanes.add(task_agent)
            if not runs.attributed:
                # Namespace-scoped signals (compaction, model fallback) are
                # not per-run; they ride with the first projected run.
                runs.attributed = True
                out.extend(
                    projector.context_signal_items(
                        self._thread_id, runs.history, agent=task_agent
                    )
                )
                out.extend(
                    projector.model_fallback_items(
                        self._thread_id, runs.history, agent=task_agent
                    )
                )
            out.extend(self._segment_items(task_agent, segment))
        return out, launched

    def _segment_items(
        self, task_agent: str, segment: list[Any]
    ) -> list[dict[str, Any]]:
        return [
            item
            for item in history_events_to_sse(
                messages_to_history_events(segment, agent=task_agent),
                thread_id=self._thread_id,
            )
            # Live streams never emit artifact events in the task lane
            # (subagent writer events carry node labels, not task:{id});
            # the frontend subagent handler has no artifact case.
            if item.get("event") != "artifact"
        ]

    async def trailing_items(self) -> list[dict[str, Any]]:
        """Segments beyond the last in-window launch, for settled runs only.

        Covers a launch whose turn never committed (e.g. the launching turn
        errored before persist) — salvaged under the last known launch's
        stamps. Per-segment ledger gate: a stamped segment projects only when
        its run row exists and is terminal — a missing row is a
        cascade-truncated run (its launching turn was deleted; resurrecting
        it would re-attach deleted work), an ``in_progress`` row belongs to
        the live stream. Unstamped (pre-ledger) segments keep the legacy
        whole-task liveness gate."""
        out: list[dict[str, Any]] = []
        for task_id, runs in self._tasks.items():
            if runs.cursor >= len(runs.segments) or runs.last_ctx is None:
                continue
            aligned = len(runs.stamps) == len(runs.segments)
            task_agent = f"task:{task_id}"
            turn_index, response_id = runs.last_ctx
            salvaged_any = False
            for idx in range(runs.cursor, len(runs.segments)):
                stamp = runs.stamps[idx] if aligned else None
                if stamp is not None:
                    if runs.run_status.get(stamp) not in sr_db.TERMINAL_STATUSES:
                        continue
                elif runs.live:
                    continue
                salvaged_any = True
                for item in self._segment_items(task_agent, runs.segments[idx]):
                    items._enrich(item, self._thread_id, turn_index, response_id)
                    out.append(item)
            if salvaged_any:
                self.salvaged_turn_indexes.add(turn_index)
        return out
