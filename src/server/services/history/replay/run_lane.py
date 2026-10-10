"""Background-task runs on the replay stream, from stored run slices.

A launch artifact (Task ``init``/``resume``) in a turn names the run it
started; that run's transcript is its run slice, the span its namespace wrote
between its own input boundary and the next run's. A ledgered launch finds
its run by the ``task_run_id`` stamped on that boundary. A launch from before
the ledger has no stamp and is matched by its prompt, which only a pass over
the whole thread in order can do (``assign_claims``); the result is stored
with the turn, so the pass runs once.

A run still writing belongs to its live stream, not to replay, and a turn
projected while one of its runs could still change is never stored. Neither
is a run whose end rests only on the liveness probe, which fails open: its
slice serves the read that cut it and is cut again by the next.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from langchain_core.messages import ToolMessage

from ptc_agent.agent.transcript.classify import is_run_boundary_message
from src.config.settings import get_redis_ttl_workflow_events
from src.server.database.conversation import turn_slices as slices_db
from src.server.database.runs import subagent_runs as sr_db
from src.server.services.history import projector
from src.server.services.history import slices
from src.server.services.history import task_streams
from src.server.services.history.replay import facts as replay_facts
from src.server.services.history.replay.errors import (
    CheckpointReplayUnavailable,
    ClaimsNeeded,
)
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.reader import CheckpointHistoryReader, TaskRun
from src.server.services.history.slices import SpanDelta, TurnSlice
from src.server.services.history.task_status import (
    TERMINAL_TASK_STATUSES,
    resolve_task_details,
)
from src.server.utils.content_normalizer import normalize_text_content

logger = logging.getLogger(__name__)

# Cap concurrent per-task checkpointer reads: each holds a pool connection,
# so an uncapped gather over a task-heavy thread can exhaust the shared
# checkpointer pool and stall live turns.
_TASK_READ_CONCURRENCY = 8


@dataclass(frozen=True)
class Launch:
    task_id: str
    action: str
    prompt: str | None
    run_id: str | None
    # Position among the turn's init/resume launches: the index into the
    # turn's claims. None for a workflow launch, which claims nothing.
    claim_index: int | None


def launches_in(turn: SpanDelta) -> list[Launch]:
    """A turn's launch artifacts in order. ``update`` (steering) never
    launches a run."""
    out: list[Launch] = []
    claim_index = 0
    for message in turn.messages:
        if not isinstance(message, ToolMessage):
            continue
        artifact = (message.additional_kwargs or {}).get("task_artifact")
        if not isinstance(artifact, dict) or not artifact.get("task_id"):
            continue
        action = artifact.get("action", "init")
        if action not in ("init", "resume", "workflow"):
            continue
        prompt = artifact.get("prompt")
        run_id = artifact.get("task_run_id")
        out.append(
            Launch(
                task_id=str(artifact["task_id"]),
                action=action,
                prompt=prompt.strip() if isinstance(prompt, str) else None,
                run_id=str(run_id) if run_id else None,
                claim_index=None if action == "workflow" else claim_index,
            )
        )
        if action != "workflow":
            claim_index += 1
    return out


def _namespace_items(
    messages: list[Any], *, thread_id: str, agent: str
) -> list[dict[str, Any]]:
    """Task-namespace messages as SSE items tagged to the task's lane.

    Artifact events are dropped. The task lane honors only side-channel
    artifacts that act on the live workspace (a file write refreshes open
    panels, a preview server opens one), and a replay would act again on
    writes and servers long past.
    """
    return [
        item
        for item in history_events_to_sse(
            messages_to_history_events(messages, agent=agent),
            thread_id=thread_id,
        )
        if item.get("event") != "artifact"
    ]


def _run_items(thread_id: str, run: SpanDelta, agent: str) -> list[dict[str, Any]]:
    out = projector.context_signal_items(thread_id, run, agent=agent)
    out.extend(projector.model_fallback_items(thread_id, run, agent=agent))
    out.extend(_namespace_items(run.messages, thread_id=thread_id, agent=agent))
    return out


def _opener_text(messages: list[Any]) -> str | None:
    """Text of a run's input message: the prompt its launch carried."""
    for message in messages:
        if is_run_boundary_message(message):
            content = message.content
            if isinstance(content, str):
                return content.strip()
            text, _ = normalize_text_content(content)
            return text.strip() if text else None
    return None


async def project_task_transcript(
    thread_id: str, task_id: str
) -> list[dict[str, Any]]:
    """Full SSE-shaped transcript of one task namespace, on demand.

    Serves drill-in views for tasks replay never projects as lanes:
    workflow children have no Task-tool launch artifact in the main
    transcript, so no turn claims their runs. Same conversion as the lane,
    so the client reuses its reduction path.

    Empty unless the task has settled: a running task belongs to its live
    stream, and a checkpoint read here would hand the caller a partial
    transcript that never self-heals. An unresolvable status counts as
    running.
    """
    try:
        details = await resolve_task_details(thread_id, [task_id])
    except Exception:
        logger.warning(
            "[REPLAY] task status probe failed for %s/%s",
            thread_id,
            task_id,
            exc_info=True,
        )
        return []
    if (details.get(task_id) or {}).get("status") not in TERMINAL_TASK_STATUSES:
        return []

    reader = CheckpointHistoryReader.get_instance()
    history = await reader.aget_task_history(thread_id, task_id)
    return _run_items(thread_id, history, f"task:{task_id}")


async def _gather_bounded(coros: list[Any]) -> list[Any]:
    semaphore = asyncio.Semaphore(_TASK_READ_CONCURRENCY)

    async def _run(coro: Any) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(
        *(_run(coro) for coro in coros), return_exceptions=True
    )


class Finality(Enum):
    """Whether a run has finished writing its span."""

    OPEN = "open"
    FINAL = "final"
    # Final only by the liveness probe, which fails open: an unledgered run
    # then also needs its stream's seal before it or the turn that launched
    # it may be stored.
    FINAL_IF_SEALED = "final_if_sealed"


@dataclass(frozen=True)
class _Claim:
    # The run's (task id, input checkpoint), or None if it never wrote a
    # boundary.
    key: tuple[str, str] | None
    finality: Finality
    # The ledger run its boundary was stamped with.
    run_id: str | None = None


@dataclass
class _RunSlice:
    """A run's slice in hand, cut by this read or read from the table."""

    slice: SpanDelta
    # Stored, or queued to be: only a settled run is, so it is final.
    stored: bool = False
    # Where the stored row says the run ended, and the ledger run it names:
    # set only for a row read from the table.
    tail: str | None = None
    run_id: str | None = None


@dataclass(frozen=True)
class _Ledger:
    """The thread's run ledger, as one read saw it."""

    status: dict[str, str]
    started: dict[str, float]
    failure: dict[str, str]
    # Tasks with a run the ledger still holds open.
    open_tasks: frozenset[str]


# The ledger until it is read: no run in it, and nothing to keep a run open.
_UNREAD = _Ledger({}, {}, {}, frozenset())


class RunLane:
    """Resolve and project the runs a set of turns launched."""

    def __init__(
        self,
        reader: CheckpointHistoryReader,
        thread_id: str,
        run_facts: replay_facts.RunFacts,
        *,
        cache: bool = True,
    ):
        self._reader = reader
        self._thread_id = thread_id
        # Without it no stored run slice is read: every run is cut from its
        # checkpoints.
        self._cache = cache
        # The read's snapshot: the same rows the turns' lines keys covered.
        self._facts = run_facts
        self._serde = reader.serde
        # None once a read of it failed.
        self._ledger: _Ledger | None = _UNREAD
        self._stream_states: dict[str, str] = {}
        self._probed: set[str] = set()
        self._live_tasks: set[str] = set()
        self._walks: dict[str, list[TaskRun]] = {}
        # (task_id, input checkpoint) -> run slice, stored or extracted.
        self._runs: dict[tuple[str, str], _RunSlice] = {}
        # Ledger run -> the key of the stored row that names it.
        self._by_run_id: dict[str, tuple[str, str]] = {}
        self._loaded_tasks: set[str] = set()
        self._writes: list[slices_db.RunRow] = []
        self._workflow: dict[str, SpanDelta] = {}
        # (turn input checkpoint, claim index) -> resolved claim.
        self._claims: dict[tuple[str, int], _Claim] = {}

    # ------------------------------------------------------------ prepare

    @property
    def can_store(self) -> bool:
        """Whether what this lane resolved may be stored. Without the ledger
        every run's end rests on the liveness probe, and claims and lines
        resolved from it would outlive the outage."""
        return self._ledger is not None

    def take_writes(self) -> list[slices_db.RunRow]:
        """Run slice rows queued since the last call, for the caller to
        store alongside its turns."""
        writes, self._writes = self._writes, []
        return writes

    async def prepare(
        self,
        turns: list[tuple[TurnSlice, list[str | None] | None, datetime | None]],
    ) -> None:
        """Resolve every run these turns (each with its claims and the time
        its row settled) launched, slicing each finished run nothing has
        sliced yet and queueing the settled ones to store.

        Raises ``ClaimsNeeded`` when a turn holds a launch only a
        whole-thread pass can place.
        """
        launched = [
            (turn, claims, settled_at, launches_in(turn))
            for turn, claims, settled_at in turns
        ]
        all_launches = [ln for _t, _c, _s, lns in launched for ln in lns]
        if not all_launches:
            return
        await self._load_evidence(all_launches)

        # Walk every task a launch cannot settle from stored rows alone.
        walk: set[str] = set()
        for _turn, claims, _settled, lns in launched:
            for ln in lns:
                if ln.action == "workflow" or self._in_progress(ln.run_id):
                    continue
                if ln.run_id and self._stored_key(ln.run_id) is not None:
                    continue
                if not ln.run_id and claims is not None:
                    claimed = _claimed(claims, ln)
                    if claimed is None or self._is_stored((ln.task_id, claimed)):
                        continue
                walk.add(ln.task_id)
        await self._walk(sorted(walk))

        wanted: dict[str, dict[str, TaskRun]] = {}
        settled: list[TaskRun] = []
        for turn, claims, settled_at, lns in launched:
            for ln in lns:
                if ln.action == "workflow":
                    continue
                at = (turn.anchor.input_checkpoint_id, ln.claim_index)
                if self._in_progress(ln.run_id):
                    # This exact run is still executing: its stream replays
                    # the run from its first event, so a projected copy
                    # would render twice.
                    self._claims[at] = _Claim(None, Finality.OPEN)
                    continue
                found = self._find_run(ln, claims)
                if found is None:
                    self._claims[at] = _Claim(
                        None,
                        Finality.FINAL
                        if self._ledgered(ln.run_id)
                        else Finality.FINAL_IF_SEALED,
                    )
                    continue
                key, run = found
                if run is None:  # known only by its stored row
                    self._claims[at] = _Claim(key, Finality.FINAL)
                    continue
                finality = self._finality(run)
                self._claims[at] = _Claim(key, finality, run.task_run_id)
                if finality is not Finality.OPEN and key not in self._runs:
                    wanted.setdefault(run.task_id, {})[run.input_checkpoint_id] = run
                if self._settled(run.task_id, finality, settled_at):
                    settled.append(run)
        await self._extract(wanted)
        for run in settled:
            self._queue_store(run)

    async def _load_evidence(self, launches: list[Launch]) -> None:
        """What resolving these launches reads before any run's namespace:
        seal evidence first (the ledger, then the streams and liveness of
        unledgered tasks), which only moves toward terminal, so a run seen
        terminal here cannot write after the namespace reads that follow;
        then the launched tasks' stored runs and workflow snapshots."""
        await self._load_ledger()
        unledgered = sorted(
            {ln.task_id for ln in launches if not self._ledgered(ln.run_id)}
        )
        self._stream_states.update(
            await task_streams.task_stream_states(self._thread_id, unledgered)
        )
        await self._load_liveness(unledgered)
        await self._load_stored({ln.task_id for ln in launches})
        await self._read_workflows(
            sorted({ln.task_id for ln in launches if ln.action == "workflow"})
        )

    async def _load_ledger(self) -> None:
        if self._ledger is not _UNREAD:
            return
        try:
            rows = await sr_db.list_runs_for_thread(self._thread_id)
        except Exception:
            self._ledger = None
            logger.warning(
                "[REPLAY] run-ledger read failed for %s",
                self._thread_id,
                exc_info=True,
            )
            return
        status: dict[str, str] = {}
        started: dict[str, float] = {}
        failure: dict[str, str] = {}
        open_tasks: set[str] = set()
        for r in rows:
            run_id = str(r["task_run_id"])
            status[run_id] = str(r["status"])
            if r.get("started_at") is not None:
                started[run_id] = r["started_at"].timestamp() * 1000.0
            error = r.get("failure")
            if isinstance(error, dict) and error.get("error"):
                failure[run_id] = str(error["error"])
            if r.get("task_id") and str(r["status"]) not in sr_db.TERMINAL_STATUSES:
                open_tasks.add(str(r["task_id"]))
        self._ledger = _Ledger(status, started, failure, frozenset(open_tasks))

    async def _load_liveness(self, task_ids: list[str]) -> None:
        """Same liveness truth that stamps card status: an unledgered task is
        live only while its writer provably runs. On probe failure nothing is
        marked live (a transient duplicate beats a missing transcript), which
        is why such a run still needs its stream's seal to be stored."""
        task_ids = [t for t in task_ids if t not in self._probed]
        if not task_ids:
            return
        self._probed.update(task_ids)
        try:
            details = await resolve_task_details(self._thread_id, task_ids)
        except Exception:
            logger.warning(
                "[REPLAY] task liveness probe failed for %s",
                self._thread_id,
                exc_info=True,
            )
            return
        self._live_tasks.update(
            task_id
            for task_id, detail in details.items()
            if (detail or {}).get("status") == "running"
        )

    async def _load_stored(self, task_ids: set[str]) -> None:
        task_ids = task_ids - self._loaded_tasks
        if not task_ids or not self._cache:
            return
        self._loaded_tasks |= task_ids
        for row in await slices_db.get_run_slices(self._thread_id, sorted(task_ids)):
            if row.slice.slice_key != slices.SLICE_KEY:
                continue
            run_slice = slices.decoded(self._serde, row.slice)
            if run_slice is None:
                continue
            key = (row.task_id, row.slice.input_checkpoint_id)
            self._runs[key] = _RunSlice(
                run_slice,
                stored=True,
                tail=row.slice.tail_checkpoint_id,
                run_id=row.task_run_id,
            )
            if row.task_run_id:
                self._by_run_id[row.task_run_id] = key
        for task_id in task_ids & self._walks.keys():
            self._drop_moved(task_id)

    def _drop_moved(self, task_id: str) -> None:
        """Forget stored slices the walk no longer ends where they ended: the
        run wrote on after it was stored, so its slice is cut again."""
        for run in self._walks.get(task_id) or ():
            key = (task_id, run.input_checkpoint_id)
            found = self._runs.get(key)
            if (
                found
                and found.tail is not None
                and found.tail != run.tail_checkpoint_id
            ):
                del self._runs[key]

    def _is_stored(self, key: tuple[str, str]) -> bool:
        found = self._runs.get(key)
        return found is not None and found.stored

    def _stored_key(self, run_id: str) -> tuple[str, str] | None:
        """The key of the stored row naming this ledger run, while it holds."""
        key = self._by_run_id.get(run_id)
        found = self._runs.get(key) if key else None
        return key if found is not None and found.run_id == run_id else None

    async def _read_workflows(self, task_ids: list[str]) -> None:
        """A workflow run's namespace holds no transcript, only the driver's
        terminal ui snapshot (empty until the run settles), so it is read
        whole rather than sliced."""
        task_ids = [t for t in task_ids if t not in self._workflow]
        results = await _gather_bounded(
            [self._reader.aget_task_history(self._thread_id, t) for t in task_ids]
        )
        for task_id, history in zip(task_ids, results):
            if isinstance(history, BaseException):
                self._unavailable(task_id, history)
            self._workflow[task_id] = history

    async def _walk(self, task_ids: list[str]) -> None:
        task_ids = [t for t in task_ids if t not in self._walks]
        results = await _gather_bounded(
            [self._reader.aget_task_runs(self._thread_id, t) for t in task_ids]
        )
        for task_id, runs in zip(task_ids, results):
            if isinstance(runs, BaseException):
                self._unavailable(task_id, runs)
            self._walks[task_id] = runs
            self._drop_moved(task_id)

    async def _extract(self, wanted: dict[str, dict[str, TaskRun]]) -> None:
        """Slice finished runs nothing has sliced yet, for this read."""
        tasks = sorted(t for t, runs in wanted.items() if runs)
        results = await _gather_bounded(
            [
                self._reader.aget_run_slices(
                    self._thread_id, t, list(wanted[t].values())
                )
                for t in tasks
            ]
        )
        for task_id, extracted in zip(tasks, results):
            if isinstance(extracted, BaseException):
                self._unavailable(task_id, extracted)
            for run, run_slice in zip(wanted[task_id].values(), extracted):
                self._runs[(task_id, run.input_checkpoint_id)] = _RunSlice(run_slice)

    def _unavailable(self, task_id: str, error: BaseException) -> None:
        logger.warning(
            "[REPLAY] Failed to read subagent checkpoint state task:%s",
            task_id,
            exc_info=(type(error), error, error.__traceback__),
        )
        # Silent continuation would produce a plausible-looking but
        # incomplete transcript and bypass the endpoint's SSE fallback.
        raise CheckpointReplayUnavailable(
            f"subagent checkpoint state unavailable for task:{task_id}"
        ) from error

    def _settled(
        self, task_id: str, finality: Finality, settled_at: datetime | None
    ) -> bool:
        """A run's end may be stored: the ledger is readable and the run is
        final by more than the liveness probe, so a stored slice can be
        trusted as final ever after (``_finality``)."""
        if not self.can_store or finality is Finality.OPEN:
            return False
        return finality is Finality.FINAL or self._stream_sealed(task_id, settled_at)

    def _queue_store(self, run: TaskRun) -> None:
        """Queue a settled run's slice to store. Best effort: one that fails
        to store is cut again by the next read that needs it."""
        found = self._runs.get((run.task_id, run.input_checkpoint_id))
        if found is None or found.stored:
            return
        codec, data = slices.encode(self._serde, found.slice)
        self._writes.append(
            slices_db.RunRow(
                thread_id=self._thread_id,
                task_id=run.task_id,
                # A stamp the ledger no longer holds cannot be referenced.
                task_run_id=run.task_run_id
                if self._ledgered(run.task_run_id)
                else None,
                slice=slices_db.StoredSlice(
                    input_checkpoint_id=run.input_checkpoint_id,
                    tail_checkpoint_id=run.tail_checkpoint_id,
                    slice_key=slices.SLICE_KEY,
                    codec=codec,
                    data=data,
                ),
            )
        )
        found.stored = True

    # ------------------------------------------------------------ claims

    @property
    def _book(self) -> _Ledger:
        """The ledger as read: empty until then, and after a failed read."""
        return self._ledger or _UNREAD

    def _ledgered(self, run_id: str | None) -> bool:
        return bool(run_id) and run_id in self._book.status

    def _in_progress(self, run_id: str | None) -> bool:
        return (
            self._ledgered(run_id)
            and self._book.status[run_id] not in sr_db.TERMINAL_STATUSES
        )

    def _finality(self, run: TaskRun) -> Finality:
        if self._is_stored((run.task_id, run.input_checkpoint_id)):
            return Finality.FINAL  # only a settled run is ever stored
        runs = self._walks.get(run.task_id) or []
        if runs and run.ordinal < runs[-1].ordinal:
            return Finality.FINAL  # a later run started, so this one ended
        if self._ledgered(run.task_run_id):
            if self._book.status[run.task_run_id] in sr_db.TERMINAL_STATUSES:
                return Finality.FINAL
            return Finality.OPEN
        if run.task_id in self._live_tasks or run.task_id in self._book.open_tasks:
            return Finality.OPEN
        return Finality.FINAL_IF_SEALED

    def _find_run(
        self, launch: Launch, claims: list[str | None] | None
    ) -> tuple[tuple[str, str], TaskRun | None] | None:
        """The key of the run a launch started, with the run as the walk
        found it (None when only its stored row is known), or None if it
        never wrote a boundary."""
        if launch.run_id:
            stored = self._stored_key(launch.run_id)
            if stored is not None:
                return stored, None
            runs = self._walks.get(launch.task_id) or []
            for run in runs:
                if run.task_run_id == launch.run_id:
                    return (run.task_id, run.input_checkpoint_id), run
            if all(run.task_run_id for run in runs):
                return None
        if claims is None:
            raise ClaimsNeeded(launch.task_id)
        claimed = _claimed(claims, launch)
        if claimed is None:
            return None
        key = (launch.task_id, claimed)
        for run in self._walks.get(launch.task_id) or ():
            if run.input_checkpoint_id == claimed:
                return key, run
        if self._is_stored(key):
            return key, None
        # The claim was placed against a run the walk no longer has, and
        # nothing stored it: its transcript cannot be read.
        raise CheckpointReplayUnavailable(
            f"claimed run task:{launch.task_id} is no longer on its walk"
        )

    async def assign_claims(
        self, turns: list[tuple[list[Launch], datetime | None]]
    ) -> list[list[str | None]]:
        """Place every run launch of the thread, given each turn's launches
        and the time its row settled, oldest turn first.

        One cursor per task walks its runs in order. A stamped launch claims
        the run carrying its stamp; an unstamped one claims the next run whose
        input matches its prompt (or, when either side has no text to compare,
        the next run). A launch with no match claims nothing: its run never
        wrote a boundary, and pairing it by position would hand it the next
        run's transcript. Every final run is cut to compare its prompt; the
        settled ones a launch claims are queued to store.
        """
        launches = [[ln for ln in lns if ln.action != "workflow"] for lns, _ in turns]
        task_ids = sorted({ln.task_id for lns in launches for ln in lns})
        await self._load_evidence([ln for lns in launches for ln in lns])
        await self._walk(task_ids)
        await self._extract(
            {
                t: {
                    r.input_checkpoint_id: r
                    for r in self._walks[t]
                    if (t, r.input_checkpoint_id) not in self._runs
                    and self._finality(r) is not Finality.OPEN
                }
                for t in task_ids
            }
        )

        cursors = dict.fromkeys(task_ids, 0)
        out: list[list[str | None]] = []
        for lns, (_launches, settled_at) in zip(launches, turns):
            claims: list[str | None] = []
            for ln in lns:
                runs = self._walks[ln.task_id]
                cursor = cursors[ln.task_id]
                claimed: TaskRun | None = None
                if ln.run_id:
                    claimed = next(
                        (r for r in runs[cursor:] if r.task_run_id == ln.run_id), None
                    )
                if claimed is None and cursor < len(runs):
                    if ln.prompt is None or self._opener(runs[cursor]) is None:
                        claimed = runs[cursor]
                    else:
                        claimed = next(
                            (r for r in runs[cursor:] if self._opener(r) == ln.prompt),
                            None,
                        )
                if claimed is not None:
                    cursors[ln.task_id] = claimed.ordinal + 1
                    if self._settled(
                        claimed.task_id, self._finality(claimed), settled_at
                    ):
                        self._queue_store(claimed)
                claims.append(claimed.input_checkpoint_id if claimed else None)
            out.append(claims)
        return out

    def _opener(self, run: TaskRun) -> str | None:
        found = self._runs.get((run.task_id, run.input_checkpoint_id))
        return _opener_text(found.slice.messages) if found is not None else None

    # ------------------------------------------------------------ project

    def project(
        self, turn: TurnSlice, settled_at: datetime | None
    ) -> tuple[list[dict[str, Any]], bool, dict[str, float]]:
        """``(items, sealed, watermarks)`` for the runs this turn launched.

        ``sealed``: every run is settled (``_settled``), so the projection
        may be stored.
        ``watermarks``: task id -> newest ledgered run start this projection
        contains, the client's authority for which runs its history payload
        already holds.
        """
        out: list[dict[str, Any]] = []
        launches = launches_in(turn)
        sealed = not launches or self.can_store
        watermarks: dict[str, float] = {}
        book = self._book
        for ln in launches:
            agent = f"task:{ln.task_id}"
            if ln.action == "workflow":
                # Frames are reconciled against the ledger row, the status
                # authority; un-reconciled frames rebuild per read.
                if self._ledgered(ln.run_id):
                    if self._in_progress(ln.run_id):
                        sealed = False
                elif ln.run_id or not self._stream_sealed(ln.task_id, settled_at):
                    sealed = False
                out.extend(
                    projector.workflow_run_items(
                        self._workflow.get(ln.task_id) or SpanDelta(),
                        task_id=ln.task_id,
                        ledger_status=book.status.get(ln.run_id) if ln.run_id else None,
                        ledger_failure=book.failure.get(ln.run_id)
                        if ln.run_id
                        else None,
                    )
                )
                continue
            claim = self._claims.get((turn.anchor.input_checkpoint_id, ln.claim_index))
            if claim is None or claim.finality is Finality.OPEN:
                sealed = False
                continue
            if claim.finality is Finality.FINAL_IF_SEALED and not self._stream_sealed(
                ln.task_id, settled_at
            ):
                sealed = False
            if claim.key is None:
                continue
            found = self._runs.get(claim.key)
            if found is None:
                sealed = False
                continue
            if ln.run_id and ln.run_id in book.started:
                started = book.started[ln.run_id]
                watermarks[ln.task_id] = max(
                    watermarks.get(ln.task_id, started), started
                )
            out.extend(
                replay_facts.apply_run_facts(
                    _run_items(self._thread_id, found.slice, agent),
                    self._facts.for_run(claim.run_id or ln.run_id),
                    agent=agent,
                    thread_id=self._thread_id,
                    message_ids=[getattr(m, "id", None) for m in found.slice.messages],
                )
            )
        return out, sealed, watermarks

    def _stream_sealed(self, task_id: str, settled_at: datetime | None) -> bool:
        """An unledgered run is final by its stream's end sentinel, or by a
        missing stream once the turn settled long enough ago that a sealed
        stream would have expired: a task still writing always has its
        stream, which carries no TTL until it seals. Twice the TTL leaves
        room for a task that outlived its turn."""
        if task_id in self._book.open_tasks:
            return False
        state = self._stream_states.get(task_id, task_streams.STREAM_UNKNOWN)
        if state == task_streams.STREAM_SEALED:
            return True
        if state == task_streams.STREAM_MISSING and settled_at is not None:
            age = datetime.now(timezone.utc) - settled_at
            return age > timedelta(seconds=2 * get_redis_ttl_workflow_events())
        return False


def _claimed(claims: list[str | None], launch: Launch) -> str | None:
    index = launch.claim_index
    return claims[index] if index is not None and index < len(claims) else None
