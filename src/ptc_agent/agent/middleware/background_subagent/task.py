"""One background subagent task: its state and its terminal transitions.

A leaf by construction — stdlib only. The registry, the dispatch path, the
workflow driver and the server's collectors all hold these, so anything this
module imported would become a load-bearing edge in that graph.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Literal

TerminalStatus = Literal[
    "completed", "error", "cancelled", "timeout", "never_started"
]
"""A settled task's outcome, in-memory.

Deliberately not the durable ledger's vocabulary: this is one task's local
view, the ledger's terminal CAS is the durable truth, and ``run_executor``
maps between them (a timeout, for instance, is a cancel to the ledger). The
ledger's ``interrupted`` has no in-memory counterpart — an interrupt leaves
the task pending, and ``never_started`` has none either — a run that never
spawned either has no row or already settled its own as ``error``.
"""

WORKFLOW_SUBAGENT_TYPE = "workflow"
"""The ``subagent_type`` a workflow run's task carries.

Deliberately not a key in ``subagent_graphs``: a workflow is driven by
``WorkflowDriver``, not by a subagent graph. Anything that would hand a task
to the ordinary spawn path has to refuse this type first, because that path
answers an unknown type with prose rather than an error.
"""


@dataclass
class BackgroundTask:
    """Represents a background subagent task."""

    tool_call_id: str
    """The LangGraph tool_call_id that triggered this task."""

    task_id: str
    """6-char alphanumeric identifier (e.g., 'k7Xm2p')."""

    description: str
    """Short description/label of the task."""

    prompt: str
    """Detailed instructions for the subagent."""

    subagent_type: str
    """Type of subagent (e.g., 'research', 'general-purpose'), or 'workflow'
    for a RunWorkflow run driver — the same discriminant the run ledger
    queries, so the local view and the cross-worker one agree."""

    owner_task_id: str | None = None
    """task_id of the workflow run that dispatched this task, or None for
    agent-dispatched tasks. Owned children stay on the standard collector
    claim/drain/billing path (they carry the launching turn's run_id) but are
    hidden from agent-facing aggregation: turn notifications, wait-all, and
    the TaskOutput overview."""

    asyncio_task: asyncio.Task | None = None
    """The asyncio.Task object running the background wrapper."""

    handler_task: asyncio.Task | None = None
    """The underlying tool handler task executing the subagent."""

    created_at: float = field(default_factory=time.time)
    """Timestamp when the task was created."""

    result: Any = None
    """Result from the subagent once completed."""

    error: str | None = None
    """Error message if the task failed."""

    terminal_status: TerminalStatus | None = None
    """The task's outcome once it settles; None while it is still pending.

    ``completed`` is derived from this and means *settled*, never *succeeded* —
    a distinction agent- and user-facing call sites need and used to get wrong
    by reading the old boolean as success."""

    result_seen: bool = False
    """Whether the agent has seen this task's result (via task_output, wait, or notification)."""

    # Tool call tracking
    tool_call_counts: dict[str, int] = field(default_factory=dict)
    """Count of tool calls by tool name."""

    total_tool_calls: int = 0
    """Total number of tool calls made."""

    current_tool: str = ""
    """Name of the tool currently being executed."""

    last_checked_at: float = field(default_factory=time.time)
    """Epoch seconds. Bumped whenever the agent inspects this task via the
    Task tool (status/list/update/resume/cancel actions) or via TaskOutput.
    Surfaced to the LLM so it can gauge how recently it polled, independent
    of whether anything changed."""

    last_updated_at: float = field(default_factory=time.time)
    """Epoch seconds. Bumped only on meaningful transitions:

    - Task completion (via asyncio done_callback, covers success / failure /
      cancellation).
    - Explicit ``cancelled = True``.
    - A follow-up message queued via the ``update`` action.
    - A user-visible text ``message_chunk`` event is captured.

    Reasoning, reasoning-signal, tool_calls, and tool_call_result events
    are deliberately excluded — they're high-volume pacing noise. The
    OrphanCollector liveness check falls back to ``cur_events > prev_events``
    for tool-only progression, so idle detection still works."""

    agent_id: str = ""
    """Stable unique identity: '{subagent_type}:{uuid4}'."""

    captured_event_seq: int = 0
    """Monotonic seq counter. Each captured event gets ``captured_event_seq + 1``;
    the value is also the XADD entry ID (``<seq>-0``) on the per-task stream."""

    captured_event_seq_base: int = 0
    """XADD sequence floor of the current round: everything at or below it
    belongs to a previous round on a stream the resume could not prove it
    cleared. Readers start past it so a retained epoch is skipped rather than
    counted, which is what makes the archive look identical whether or not the
    resume's spool delete landed."""

    captured_event_count: int = 0
    """Events captured in the CURRENT round — reset on every resume, whether or
    not the sequence restarts with it. The completeness gates compare it against
    what they recover above ``captured_event_seq_base``, so a cumulative value
    here would make a resumed round's archive look permanently short."""

    captured_event_bytes: int = 0
    """Bytes captured in the current round (telemetry only; estimated)."""

    redis_write_failed: bool = False
    """Sticky circuit-break: a failed spill permanently stops this task's Redis
    writes, and the run ends as ``error(transport_lost)`` rather than degrading
    to the in-memory tail — a gap in the replay archive is not recoverable."""

    spawned_turn_index: int = 0
    """The turn_index of the parent turn that spawned this subagent."""

    spawned_run_id: str | None = None
    """The run_id of the parent turn that spawned this subagent. Set from
    ``registry.current_run_id`` at register time. Collectors filter by this
    so subagents from prior turns can't get claimed by a later turn's
    collector after the registry is reused across turns."""

    task_run_id: str | None = None
    """This execution's ledger identity (subagent_runs row). Stamped by the
    middleware after the admission INSERT, re-stamped on every resume (a
    resume is a NEW run). None when no ledger is injected (CLI/tests) or
    for pre-ledger launches."""

    per_call_records: list[dict[str, Any]] = field(default_factory=list)
    """Token usage records collected when subagent completes."""

    tool_usage: dict[str, int] = field(default_factory=dict)
    """Billing-keyed infrastructure tool usage (e.g. "TavilySearchTool:deep" → 2),
    snapshotted from the per-task ToolUsageTracker when the subagent completes."""

    collector_response_id: str | None = None
    """Response ID of the collector that claimed this task for persistence.
    Set atomically during the _mark_completed filter to prevent two collectors
    from persisting the same subagent events to different response_ids."""

    sse_drain_complete: asyncio.Event = field(default_factory=asyncio.Event)
    """Set by stream_subagent_from_log after its final drain.
    The collector awaits this before clearing the captured-event tail so that
    live SSE consumers are guaranteed to have emitted all events."""

    sse_consumer_count: int = 0
    """Number of active SSE consumers for this task. sse_drain_complete is
    only set when the last consumer finishes, preventing the collector from
    clearing the captured-event tail while another consumer is still draining."""

    redis_spill_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    """Per-task lock that serializes XADD writes so concurrent appends to
    the same task can't interleave commands. Without this, two appends that
    release the registry-wide lock back-to-back can hit different Redis
    pool connections and land at the server in reverse order, breaking the
    explicit ``<seq>-0`` ordering on the stream. Off the registry-wide lock
    so a slow Redis blip on one task can't stall appends to other tasks."""

    def adopt_writer_outcome(self, writer: asyncio.Task) -> Any:
        """Settle this task from its finished writer, and return the result.

        Every collector needs this and each used to hand-roll it. Two traps
        made that costly: ``CancelledError`` is a ``BaseException``, so an
        ``except Exception`` arm lets a stopped task's cancellation escape
        into whoever is awaiting the collector — which for a tool node means
        the turn dies instead of the agent hearing about the stop; and a
        writer that returns a failure is settled, not successful, so the
        outcome has to come from the payload rather than from the mere fact
        that the writer returned.
        """
        try:
            result = writer.result()
        except asyncio.CancelledError:
            self.mark_cancelled()
            return self.result
        except Exception as exc:
            self.error = str(exc)
            self.result = {"success": False, "error": str(exc)}
            self.terminal_status = "error"
            self.last_updated_at = time.time()
            return self.result
        self.result = result
        failed = isinstance(result, dict) and result.get("success") is False
        self.terminal_status = "error" if failed else "completed"
        self.last_updated_at = time.time()
        return result

    def mark_cancelled(self, reason: str = "Cancelled") -> None:
        """User-facing cancel transition, with the result payload readers expect.

        ``reason`` is what the task reports as its error; a scoped teardown
        passes why the scope died so the child doesn't just read "Cancelled".
        """
        self.terminal_status = "cancelled"
        self.error = reason
        self.last_updated_at = time.time()
        self.result = {
            "success": False,
            "error": reason,
            "status": "cancelled",
        }

    def mark_error(self, reason: str) -> None:
        """Failure transition for a task whose writer never reported one.

        Leaves ``result`` alone on purpose: a task can fail after its answer
        was already archived, and stamping a failure payload here would hide
        that answer behind it.
        """
        self.terminal_status = "error"
        self.error = reason
        self.last_updated_at = time.time()

    def mark_never_started(self, error: str) -> None:
        """Inert pre-spawn terminal: the entry stays registered so nothing
        re-launches it, but no collector claims it and no result surfaces.

        Its own status, never ``cancelled``: every caller is an infra refusal
        (fence unusable, admission rejected, workflow cap reached, setup blew
        up before the spawn), and spelling those as a user stop tells the
        agent the abort was deliberate and must not be retried — the one
        thing that is wrong about all nine of them.
        """
        self.terminal_status = "never_started"
        self.result_seen = True
        self.error = error

    @property
    def completed(self) -> bool:
        """Settled — this task will never produce a (further) result.

        NOT success: a stopped or failed task is completed too. Read
        ``terminal_status`` for the outcome; anything user- or agent-facing
        must, or it reports a stop as finished work.
        """
        return self.terminal_status is not None

    @property
    def cancelled(self) -> bool:
        """Killed mid-flight rather than left to finish — the kill seal that
        makes this task's streams final, and the ledger's cancel mapping.

        A timeout is a kill too (it spells its outcome differently but the
        writer was still signalled); a never-started task is NOT — nothing
        ran, so there is no stream to seal and nothing to report as stopped.
        """
        return self.terminal_status in ("cancelled", "timeout")

    @property
    def display_id(self) -> str:
        """Return Task-<id> format for display."""
        return f"Task-{self.task_id}"

    @property
    def is_pending(self) -> bool:
        """True if the task is still running or registered but not yet started."""
        if self.completed:
            return False
        if self.asyncio_task is None:
            return True  # Registered but not yet started
        return not self.asyncio_task.done()

    @property
    def is_turn_visible(self) -> bool:
        """Visible to turn-level machinery (notifications, wait-all, TaskOutput
        aggregates). Workflow-owned children are driver-scoped instead — but a
        workflow run task itself IS turn-visible: it's what the agent awaits."""
        return self.owner_task_id is None

    def force_cancel(
        self,
        reason: str,
        *,
        status: TerminalStatus = "cancelled",
        cancel_handler: bool = True,
    ) -> None:
        """Cancel the live task and stamp terminal state in one step.

        Cancelling only the wrapper would let the handler keep running (the
        wrapper shields it), so force paths cancel both; ``cancel_handler``
        False is the registry's non-force mode, which lets the handler finish.
        Both handles are optional — a STARTING task has neither, and stamping
        it terminal is the whole point of the call.
        """
        if cancel_handler and self.handler_task and not self.handler_task.done():
            self.handler_task.cancel()
        if self.asyncio_task and not self.asyncio_task.done():
            self.asyncio_task.cancel()
        self.terminal_status = status
        self.error = reason
        self.result = {"success": False, "error": reason, "status": status}
        self.last_updated_at = time.time()
