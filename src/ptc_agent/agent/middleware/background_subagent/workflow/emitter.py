"""Lifecycle frames for a workflow run: their shape, their caps, their replay."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import Future
from typing import Any, Literal, get_args

import structlog

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTask,
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
    persist_task_ui_record,
)
from src.server.utils.error_sanitization import sanitize_error_text

logger = structlog.get_logger(__name__)

# Every field a lifecycle frame may carry — emitted here, replayed through the
# whitelist in ``history/projector.py``, which imports this tuple rather than
# restating it: the live path and the replay path read different lists only if
# there are two, and a name missing from the replay copy renders fine live and
# blanks on refresh. Ordered because replay projects in this order.
#
# ``error`` is deliberately absent: it is sanitized and clipped rather than
# passed through, so it never rides the whitelist. ``message`` is listed but
# scrubbed on the way out for the same reason — neither string is the driver's
# to compose. Kept closed because the live frame and the checkpointed snapshot
# are the same dict — an unlisted key would leak into both at once.
WORKFLOW_FRAME_FIELDS: tuple[str, ...] = (
    "agent",
    "run_id",
    "phase",
    "name",
    "description",
    "source",
    "title",
    "message",
    "seq",
    "label",
    "subagent_type",
    "workflow_phase",
    "child_task_id",
    "status",
    "duration_s",
    "result_preview",
    "children_total",
    "tokens_used",
    "tokens_spent",
)

# Membership is the per-field read on the emit path; the tuple stays authority.
_FRAME_FIELDS = frozenset(WORKFLOW_FRAME_FIELDS)

# ``status`` is one field carrying two closed vocabularies — the run's and its
# children's — declared here with the field list for the same reason: what a
# frame may *say* is as much of the contract as which keys it may carry, and
# the client hand-picks its unions from these values.
#
# The run's failure word is ``failed`` by the public status contract
# (``server/contracts/status.py``): ``error`` is the internal spelling, mapped
# out to ``failed`` at the boundary, so a client-facing frame says ``failed``.
WorkflowRunStatus = Literal["completed", "failed", "cancelled"]
WorkflowChildStatus = Literal["ok", "error", "timeout", "invalid_schema", "cancelled"]

WORKFLOW_RUN_STATUSES: frozenset[str] = frozenset(get_args(WorkflowRunStatus))
WORKFLOW_CHILD_STATUSES: frozenset[str] = frozenset(get_args(WorkflowChildStatus))


def truncate_to_bytes(text: str, max_bytes: int) -> tuple[str, bool]:
    """Clip text to max_bytes (codepoint-safe) + marker; the flag says it was
    clipped. Every caller wants the text, so the text is what comes back."""
    raw = text.encode("utf-8", errors="ignore")
    if len(raw) <= max_bytes:
        return text, False
    return raw[:max_bytes].decode("utf-8", errors="ignore") + "\n... [truncated]", True


def _log_emit_failure(future: Future) -> None:
    if future.cancelled():
        return
    error = future.exception()
    if error is not None:
        logger.warning("Workflow lifecycle emit failed", exc_info=error)


class WorkflowEmitter:
    """The run's frame vocabulary — what a lifecycle frame carries, how much
    payload rides along, and which frames replay keeps."""

    # Script-driven chatter (`phase()`/`log()` calls) kept in the replay
    # snapshot; structural frames (run/child boundaries) are dispatch-capped
    # and always kept in full.
    _UI_CHATTER_FRAME_CAP = 200
    # Live-emission bound for script phase()/log() calls (the snapshot cap
    # above only bounds what replay keeps, not what a run may emit).
    _SCRIPT_EMIT_CAP = 500
    _UI_CHATTER_EVENTS = frozenset({"phase", "log"})
    # Payload caps for the checkpointed snapshot. The error cap is the strict
    # one because a child_done frame carries it per child; the preview rides
    # only the terminal frame and needs the room to arrive parseable.
    _UI_RESULT_PREVIEW_CEILING = 262144
    _UI_ERROR_BYTES = 500

    def __init__(
        self,
        *,
        run_task: BackgroundTask,
        registry: BackgroundTaskRegistry,
        checkpointer: Any,
        thread_id: str,
        max_summary_bytes: int,
    ) -> None:
        self._run_task = run_task
        self._registry = registry
        self._checkpointer = checkpointer
        self._thread_id = thread_id
        # The panel never shows more than the summary the same result produced,
        # so lowering that cap lowers the panel with it. Keyed to the summary
        # rather than the result dump because both are read by someone; the
        # dump is sized for what the script manipulates in JS.
        self._result_preview_bytes = min(
            max_summary_bytes, self._UI_RESULT_PREVIEW_CEILING
        )
        self.current_phase: str | None = None
        self._task_id = run_task.task_id
        self._server_loop: asyncio.AbstractEventLoop | None = None
        self._script_emits = 0
        # Lifecycle frames mirrored for the terminal ui-channel snapshot —
        # replay rebuilds the workflow panel from these (the live SSE frames
        # are transient and their sse_events archive is being deprecated).
        self._frames: list[dict[str, Any]] = []

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Adopt the server loop that script-thread emits are handed back to."""
        self._server_loop = loop

    def phase(self, title: str) -> None:
        self.current_phase = title
        self._schedule_emit("phase", title=title)

    def log(self, message: str) -> None:
        self._schedule_emit("log", message=message)

    def clip_result_preview(self, text: str | None) -> str | None:
        return self._clip(text, self._result_preview_bytes)

    async def emit(self, event: str, *, terminal: bool = False, **fields: Any) -> None:
        """Emit a workflow lifecycle event on the run task stream.

        ``terminal`` marks settle frames. A user stop stamps the run task
        cancelled before this coroutine's unwind gets to run, and a cancelled
        task's streams are sealed against ordinary appends — so the frames the
        panel needs to leave "Stopping…" are exactly the ones the seal would
        drop. Identity-exact for the same reason the steering sweep is: the
        stop teardown may already have evicted the registry entry.
        """
        data = {
            "agent": f"task:{self._task_id}",
            "run_id": self._task_id,
            "phase": event,
        }
        # `agent`/`run_id`/`phase` are on the whitelist because it mirrors a
        # received frame's full surface, but they are the emitter's to stamp —
        # `k not in data` keeps a caller field from shadowing the event name.
        data.update(
            (k, v) for k, v in fields.items() if k in _FRAME_FIELDS and k not in data
        )
        # A `log` frame's message is script text, and since the prelude began
        # naming the absorbed exception in its `[runtime] … → null` line it can
        # be an upstream error — the same string the terminal path scrubs.
        if isinstance(data.get("message"), str):
            data["message"] = sanitize_error_text(data["message"])
        # `error` is present iff there is one. Replay's projector already drops
        # a non-string error, so emitting `error: None` on a success frame would
        # make the same run read differently before and after a reload.
        # Sanitize before clipping: clipping first could split a credential
        # across the boundary and leave a fragment the pattern misses.
        if isinstance(fields.get("error"), str) and fields["error"]:
            data["error"] = self._clip(
                sanitize_error_text(fields["error"]), self._UI_ERROR_BYTES
            )
        dropped = fields.keys() - _FRAME_FIELDS - {"error"}
        if dropped:
            logger.debug(
                "Workflow lifecycle field not on the frame whitelist",
                phase=event,
                dropped=sorted(dropped),
            )
        self._record_frame(data)
        record = {"event": "workflow_lifecycle", "data": data, "ts": time.time()}
        try:
            if terminal:
                await self._registry.append_event_for_task(
                    self._run_task, record, terminal=True
                )
            else:
                await self._registry.append_captured_event(
                    self._run_task.tool_call_id, record
                )
        except Exception:
            logger.debug("Workflow lifecycle emit failed", phase=event, exc_info=True)

    async def persist_snapshot(self) -> None:
        """Persist the run's lifecycle frames so replay can rebuild the panel
        once the live streams are gone.

        Keyed on a stable record id, so a terminal retry overwrites rather than
        duplicates, and best-effort — replay falls back to the ledger row when
        the snapshot is missing. ``ui_snapshot`` owns why the write goes to the
        run task's own namespace.
        """
        if self._checkpointer is None:
            return
        run_key = self._run_task.task_run_id or self._task_id
        try:
            await persist_task_ui_record(
                self._checkpointer,
                self._thread_id,
                self._task_id,
                "workflow_run",
                {"task_id": self._task_id, "frames": list(self._frames)},
                record_id=f"workflow-run-{run_key}",
            )
        except Exception:
            logger.warning(
                "Workflow ui snapshot persist failed",
                wf_task_id=self._task_id,
                exc_info=True,
            )

    def _clip(self, text: str | None, max_bytes: int) -> str | None:
        return None if text is None else truncate_to_bytes(text, max_bytes)[0]

    def _schedule_emit(self, event: str, **fields: Any) -> None:
        # phase()/log() are script-controlled: a tight loop would otherwise
        # queue an unbounded pile of loop coroutines + Redis writes before
        # the CPU budget fires. Structural child events are dispatch-capped.
        self._script_emits += 1
        if self._script_emits > self._SCRIPT_EMIT_CAP:
            return
        if self._script_emits == self._SCRIPT_EMIT_CAP:
            event, fields = "log", {
                "message": (
                    "script chatter emit cap reached; further phase()/log() "
                    "output is dropped"
                ),
            }
        loop = self._server_loop
        if loop is None:
            return
        # Called from the QuickJS worker thread: hand the coroutine to the
        # server loop and report its fate from a callback — awaiting it would
        # stall the script mid-execution on a Redis write.
        future = asyncio.run_coroutine_threadsafe(self.emit(event, **fields), loop)
        future.add_done_callback(_log_emit_failure)

    def _record_frame(self, data: dict[str, Any]) -> None:
        self._frames.append(data)
        if data.get("phase") not in self._UI_CHATTER_EVENTS:
            return
        chatter = [
            i
            for i, frame in enumerate(self._frames)
            if frame.get("phase") in self._UI_CHATTER_EVENTS
        ]
        if len(chatter) > self._UI_CHATTER_FRAME_CAP:
            del self._frames[chatter[0]]
