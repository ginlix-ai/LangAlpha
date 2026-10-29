"""Run SSE producer.

LangGraph run SSE producer: streams graph events, normalises content,
tracks reasoning lifecycle, deduplicates tool calls, formats SSE events, and
handles timeouts. Keepalives are emitted by the SSE consumer (stream_from_log).
"""

import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Mapping, Optional, Set, Tuple, cast

import json_repair

from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage
from langgraph.types import Overwrite

from ptc_agent.agent.middleware.image_capture import collect_sandbox_paths

from ptc_agent.agent.middleware.runtime_context.turn import (
    TURN_DEADLINE_KEY,
    TURN_TIME_LIMIT_KEY,
)

from src.server.utils.content_normalizer import (
    normalize_text_content,
    is_thinking_status_signal,
)
from src.server.utils.pg_sanitize import finite_json_dumps
from src.server.contracts.status import classify_interrupts
from src.server.database.provenance import MAIN_PROVENANCE_LANE
from src.server.database.runs.lifecycle import ProducerRecord
from src.server.services.runs.credit_usage import build_credit_usage_data
from src.server.services.runs.reasoning_facts import ReasoningDurations
from src.server.services.runs.stream_errors import error_event_data
from src.tools.wakeups.models import WakeupArtifact
from src.tools.wakeups.quiet import quiet_result
from src.server.utils.text_phase import TextPhaseTracker
from src.utils.concurrency import aclose_within
from src.utils.stream_coalescing import (
    HEARTBEAT_EVENT_TYPE,
    DeltaCoalescer,
    StreamFrame,
    delta_piece,
)
from src.llms.content_utils import extract_reasoning_summary_index
from src.config.settings import (
    get_workflow_timeout,
    is_sse_event_log_enabled,
)
from opentelemetry.trace import Status, StatusCode
from src.observability.tracing import tracer as _otel_tracer
from src.server.utils.error_sanitization import (
    sanitize_error_text as _sanitize_error_text,
)

logger = logging.getLogger(__name__)

# Dedicated logger for SSE events (can be configured independently)
sse_logger = logging.getLogger("sse_events")

WORKFLOW_TIMEOUT = get_workflow_timeout()  # seconds

# 30s: enough for model calls and tools to cancel, and short of the two minutes
# a tool's cleanup can retry against a sick sandbox while the finalize waits.
GRAPH_CLOSE_TIMEOUT_S = 30.0


class TurnTimeLimitError(asyncio.TimeoutError):
    """The turn reached its wall clock. Its own error frame has gone out by the
    time this is raised, so the generic one is not sent after it: that one
    reads as an internal failure, which the client shows as a banner in place
    of the turn and its answer."""


SSE_EVENT_LOG_ENABLED = is_sse_event_log_enabled()

DEFAULT_TOKEN_THRESHOLD = 120000


def resolve_token_threshold(agent_config=None) -> int:
    """Per-request config → base config → default. Drives the UI ring.

    Shared by the live handler and checkpoint replay so both stamp the same
    ``threshold`` onto ``context_window`` token_usage events.
    """
    cfg = agent_config
    if cfg is None:
        from src.server.app import setup

        cfg = setup.agent_config
    if cfg is None:
        return DEFAULT_TOKEN_THRESHOLD
    return cfg.compaction.token_threshold


def _parse_tool_args(
    raw: str,
) -> Tuple[Optional[Dict[str, Any]], str, str]:
    """Parse accumulated tool-call argument JSON, falling back to json_repair.

    Frontier models occasionally emit nearly-valid JSON in tool-call args —
    typically an unescaped quote or control char inside a long string value.
    Strict ``json.loads`` rejects it and (without this fallback) the call is
    silently dropped, triggering an empty-tool-call retry storm.

    Returns ``(parsed_or_None, err_repr, err_window)``. On success the latter
    two are empty strings. On failure they carry diagnostic context for the
    caller's error log — the caller doesn't need to re-parse ``raw`` to build
    a window around the failure point. Tool-call args must be a JSON object
    per the LangChain contract; non-dict results are rejected so downstream
    tool dispatch sees the same shape it always has.
    """
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed, "", ""
        return (
            None,
            f"non-dict top-level JSON: {type(parsed).__name__}",
            raw[:200],
        )
    except json.JSONDecodeError as je:
        err_repr = str(je)
        start = max(0, je.pos - 80)
        end = min(len(raw), je.pos + 80)
        err_window = raw[start:end]
    try:
        repaired = json_repair.loads(raw)
    except Exception:
        return None, err_repr, err_window
    if isinstance(repaired, dict):
        return repaired, "", ""
    return None, err_repr, err_window


def _fallback_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """A model_fallback ui record as its response row keeps it. Metadata is
    cut to the anchor replay reads, since the rest is run context the row
    has no use for, and the error is scrubbed before it is stored."""
    props = dict(record.get("props") or {})
    if isinstance(props.get("error"), str):
        props["error"] = _sanitize_error_text(props["error"])
    after = (record.get("metadata") or {}).get("after_message_id")
    return {
        "type": "ui",
        "id": record.get("id"),
        "name": record.get("name"),
        "props": props,
        "metadata": {"after_message_id": after} if after else {},
    }


def fallback_from_frame(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The record a main-lane model_fallback frame names, as
    ``_fallback_record`` keeps it, so recovery keeps a dead run's notices as
    its finalize would. None for a frame that names none, another lane's or
    an older server's, whose notice replay could not tell from a saved one."""
    from src.server.services.history.projector import MODEL_FALLBACK_FIELDS

    if data.get("agent") != "main" or not data.get("fallback_id"):
        return None
    return _fallback_record(
        {
            "id": data["fallback_id"],
            "name": "model_fallback",
            "props": {
                key: data[key]
                for key in (*MODEL_FALLBACK_FIELDS, "error")
                if key in data
            },
            "metadata": {"after_message_id": data.get("after_message_id")},
        }
    )


def kept_error_frame(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """A main-lane ``error`` frame as its response row keeps it, so a reload
    shows the failure as the live stream did: the row alone lacks the model,
    the hints and the live wording. None for a subagent's, whose failure
    belongs to its card."""
    from src.server.services.history.replay.lanes import MAIN_LANE, agent_lane

    if agent_lane(data.get("agent")) != MAIN_LANE:
        return None
    return {key: value for key, value in data.items() if key != "thread_id"}


class RunSSEProducer:
    """LangGraph workflow SSE producer with reasoning lifecycle tracking, tool-call dedup, and content normalisation."""

    def __init__(
        self,
        thread_id: str,
        run_id: str,
        token_callback: Optional[Any] = None,
        tool_tracker: Optional[Any] = None,
        workflow_timeout: Optional[int] = None,
        agent_config: Optional[Any] = None,
        *,
        turn_index: Optional[int] = None,
        announced: Optional[Mapping[str, Any]] = None,
    ):
        """Initialize the workflow stream handler.

        Keepalives live in the SSE consumer (``stream_from_log``), not here —
        the producer side is just a thin LangGraph pass-through.

        ``run_id`` is REQUIRED — it's emitted as the first SSE event
        (``event: metadata``) of each stream so the frontend learns the
        canonical per-turn identity immediately and can drive reconnect /
        steering / demotion off it. Constructing a handler without a
        ``run_id`` would silently strip that frame and leave the client
        racing on reconnect.

        ``announced`` rides the same frame on a notification turn (the
        query row's ``turn_kinds.ANNOUNCED_KEYS``): a client that attaches to
        the run live has no user message to label and never reads the query
        row, so it draws the report's or the notices' row from this.

        ``turn_index`` is the turn admission gave the run, announced on the
        same frame: a view that missed turns added elsewhere cannot count its
        way to it, and replay already names every turn by this value.
        """
        self.thread_id = thread_id
        self.run_id = run_id
        self.announced = dict(announced or {})
        # How a wake turn ended quietly, for the finalize to record; reset
        # if the main agent speaks again after it.
        self.quiet: Optional[WakeupArtifact] = None
        self.turn_index = turn_index
        self.token_callback = token_callback
        self.tool_tracker = tool_tracker
        self.workflow_timeout = workflow_timeout or WORKFLOW_TIMEOUT
        self.agent_config = agent_config

        # Cache for tool usage result (for cross-context access)
        self._tool_usage_result: Optional[Dict[str, int]] = None

        # Track displayed tool IDs to prevent duplicates
        self.seen_tool_ids: Set[str] = set()

        # Track reasoning status per agent for lifecycle management
        self.reasoning_active: Set[str] = set()
        # Monotonic clock at each agent's open reasoning block, so its closing
        # signal can carry how long the model thought.
        self._reasoning_started_at: Dict[str, float] = {}
        # How long the main agent thought, for replay.
        self._reasoning = ReasoningDurations()
        # The main agent's provenance records as emitted, for the finalize to
        # write.
        self.provenance_events: List[Dict[str, Any]] = []
        # The main agent's model_fallback ui records as emitted: a step that
        # fails or is stopped after one never commits it.
        self._model_fallbacks: List[Dict[str, Any]] = []
        # The last error frame the main agent's stream sent, for the finalize
        # of a failed run to keep.
        self._error_frame: Optional[Dict[str, Any]] = None
        # The sandbox image paths each message the main graph saved still
        # names, by message id: a later write of the message replaces its
        # entry.
        self._saved_image_paths: Dict[str, Set[str]] = {}

        # Track reasoning block index per agent to detect block transitions
        # When index changes (e.g., 0→1), a separator (\n\n) is needed between blocks
        self._reasoning_block_index: dict[str, int] = {}
        self._reasoning_separator_pending: Set[str] = set()
        # Whitespace-only reasoning deltas seen before an agent's reasoning
        # section opened, held with their message id until visible reasoning
        # follows.
        self._reasoning_whitespace_held: dict[str, tuple[str, str]] = {}

        # The OpenAI Responses `phase` of streamed text, per agent and message.
        self._text_phase = TextPhaseTracker()

        # Track function_call state for Response API (per agent)
        # Response API sends name/call_id only in first chunk, need to persist across chunks
        # Key: (agent_name, index), Value: {name, call_id, args_accumulated}
        self.function_call_state: dict = {}

        # Track tool_use state for Anthropic (per agent)
        # Anthropic sends name/id in initial tool_use, then streams args via input_json_delta
        # Key: (agent_name, index), Value: {name, id, message_id, args_accumulated}
        self.anthropic_tool_call_state: dict = {}

        # Event sequence numbering for reconnection support
        self.event_sequence: int = 0

        self.event_counter: Optional[Any] = None

        # Track message IDs that have already emitted content via AIMessageChunk streaming.
        # When a streaming model produces chunks, LangGraph also emits a final AIMessage
        # with the full content at node completion. This set prevents re-emitting that
        # duplicate content while still allowing non-streaming AIMessage content through.
        self._streamed_content_ids: Set[str] = set()

        # Namespace tuples currently inside a "summarize" window. Opened on
        # the context_window summarize start signal, closed on complete OR
        # error. Keyed by the raw namespace (not the resolved agent display
        # name) so main-agent root events (namespace == ()) match consistently
        # between the custom and messages streams. Any messages-stream chunks
        # whose namespace is in this set are emitted as compaction_chunk
        # instead of message_chunk.
        self._compaction_windows: set[tuple] = set()
        # True while THIS handler holds an open compaction guard in the
        # LocalRunExecutor (driven off _compaction_windows cardinality:
        # opened when the set goes empty→non-empty, closed when it returns to
        # empty). Lets the outer-finally safety net release the guard exactly
        # once on timeout/error/cancel.
        self._compaction_active: bool = False

        # ---- Interrupt durability barrier (v4 I8) ------------------------
        # __interrupt__ events are buffered, not emitted inline: the SSE is
        # exposed only after the graph iterator is exhausted AND the pending
        # interrupt is verified durable in the checkpoint. These fields are
        # also the in-band outcome signal the finalizer classifies from
        # (aget_state polling is demoted to a no-handler fallback).
        self._pending_interrupts: list[dict] = []
        self.saw_interrupt: bool = False
        # None until the barrier runs; then True (durable, SSE exposed) or
        # False (NOT durable — the turn must finalize failed, never
        # interrupted: advertising resumability without a checkpoint to
        # resume from is the I8 lie).
        self.interrupt_verified: Optional[bool] = None
        self.interrupt_reason: Optional[str] = None
        # A credit pause's denial, which the finalize stamps on the row.
        self.pause_message: Optional[str] = None

    def _open_compaction_window(self, ns_key: tuple) -> None:
        """Track a newly-opened compaction window. The FIRST window on this
        thread opens the ThreadMutationRunner admission window, so a
        concurrent message POST waits the summarize out instead of steering
        into the mid-flight context rewrite. Overlapping windows (main +
        subgraph) keep the window open until all have closed."""
        was_empty = not self._compaction_windows
        self._compaction_windows.add(ns_key)
        if was_empty:
            from src.server.services.thread_mutation import ThreadMutationRunner
            # Honor the return value: only take ownership (and thus only later
            # close_auto_compaction_window) when THIS handler actually opened it. A False
            # return means another op (e.g. a manual /compact) already holds
            # the thread — closing it on our exit would clobber it. Mirrors
            # the manual paths' ownership flag.
            self._compaction_active = (
                ThreadMutationRunner.get_instance().open_auto_compaction_window(self.thread_id)
            )

    def _close_compaction_window(self, ns_key: tuple) -> None:
        """Close one compaction window. When the LAST window closes, release
        the admission window so a waiting POST is admitted (and steered /
        started)."""
        self._compaction_windows.discard(ns_key)
        if not self._compaction_windows and self._compaction_active:
            from src.server.services.thread_mutation import ThreadMutationRunner

            ThreadMutationRunner.get_instance().close_auto_compaction_window(self.thread_id)
            self._compaction_active = False

    async def stream_workflow(
        self,
        graph: Any,
        input_state: Any,
        config: dict,
        *,
        settled_results: list[ToolMessage] | None = None,
    ) -> AsyncGenerator[str, None]:
        """Stream workflow execution events as SSE-formatted strings.

        Keepalives are emitted by the SSE consumer (``stream_from_log``) on
        XREAD BLOCK timeout, not by the producer — the workflow no longer
        needs to interleave them with graph output. ``settled_results`` are
        tool results the turn wrote to the thread before the graph ran, so the
        graph never emits them; they go out as the turn's first results.

        Frames pass through the coalescer and are numbered as they leave it,
        so joined deltas keep the ids contiguous. A close that cannot yield
        (stop, cancel, a transport quota) loses at most the held delta from
        the live stream; replay projects the turn from its checkpoint.

        A frame is serialized where it is built, so one that cannot be fails
        inside the body's own handling: an error frame and a failed run, or a
        warning where the frame is optional (credit usage).
        """
        frames = self._stream_frames(
            graph, input_state, config, settled_results=settled_results
        )
        coalescer = DeltaCoalescer()
        try:
            async for frame in frames:
                ready = coalescer.poll() if frame is None else coalescer.offer(frame)
                for out in ready:
                    yield self._number_sse_event(out)
            for out in coalescer.drain():
                yield self._number_sse_event(out)
        finally:
            # Close the body here rather than leave it to garbage collection:
            # its finally releases the compaction admission window.
            await frames.aclose()

    async def _stream_frames(
        self,
        graph: Any,
        input_state: Any,
        config: dict,
        *,
        settled_results: list[ToolMessage] | None = None,
    ) -> AsyncGenerator[StreamFrame | None, None]:
        """The body of ``stream_workflow``: yields built frames, plus a None
        per graph event so a held delta's window is checked even when the
        event emits nothing."""
        import time

        from src.server.services.history.projector import MODEL_FALLBACK_FIELDS

        # Track start time for timeout
        workflow_start_time = time.time()
        timeout_warning_sent = False
        timeout_warning_threshold = 0.9  # Send warning at 90% of timeout

        # Set tool tracking ContextVar
        # This must be done BEFORE graph.astream() so nodes inherit the ContextVar
        if self.tool_tracker:
            from src.tools.decorators import _tool_usage_context
            _tool_usage_context.set(self.tool_tracker)
            logger.debug(f"[RunSSEProducer] Tool usage tracking ContextVar set for thread_id={self.thread_id}")

        _stream_span = _otel_tracer.start_span(
            "chat.turn.stream",
            attributes={"thread_id_hash": (self.thread_id or "")[:16]},
        )
        graph_stream = None
        try:
            # First event of every stream: ``metadata`` payload announcing
            # the canonical run_id for this turn. Mirrors the
            # langgraph_sdk SSE protocol so frontend reconnect/demotion
            # logic can latch onto the authoritative identity immediately.
            metadata: Dict[str, Any] = {
                "thread_id": self.thread_id,
                "run_id": self.run_id,
            }
            if self.turn_index is not None:
                metadata["turn_index"] = self.turn_index
            metadata.update(self.announced)
            yield self._build_sse_event("metadata", metadata)

            for result in settled_results or ():
                async for event in self._process_message_chunk(
                    result, "tools", {"langgraph_node": "tools"}
                ):
                    yield event

            # The agent reads when this stop lands, so it can answer before it
            # rather than be cut off by it.
            if self.workflow_timeout > 0:
                config = {
                    **config,
                    "configurable": {
                        **(config.get("configurable") or {}),
                        TURN_DEADLINE_KEY: workflow_start_time + self.workflow_timeout,
                        TURN_TIME_LIMIT_KEY: self.workflow_timeout,
                    },
                }

            # Create graph stream. durability="sync" awaits each checkpoint
            # put before the next step — necessary but NOT sufficient for
            # interrupt durability (output yields before the sync await, and
            # dynamic interrupts ride async aput_writes), hence the barrier
            # below.
            graph_stream = graph.astream(
                input_state,
                config=config,
                stream_mode=["messages", "updates", "custom"],
                subgraphs=True,
                durability="sync",
            )

            async for graph_event in graph_stream:
                yield None

                # Unpack graph event data
                agent_from_stream, stream_mode, event_data = graph_event

                # Check for timeout (if configured)
                if self.workflow_timeout > 0:
                    elapsed_time = time.time() - workflow_start_time

                    # Send warning at 90% of timeout
                    if not timeout_warning_sent and elapsed_time >= (self.workflow_timeout * timeout_warning_threshold):
                        timeout_warning_sent = True
                        warning_event = self._build_sse_event(
                            "warning",
                            {
                                "thread_id": self.thread_id,
                                "message": f"Turn approaching timeout ({int(elapsed_time)}s / {self.workflow_timeout}s)",
                                "type": "timeout_warning",
                                "elapsed_seconds": int(elapsed_time),
                                "timeout_seconds": self.workflow_timeout,
                            }
                        )
                        yield warning_event
                        logger.warning(
                            f"[TIMEOUT_WARNING] thread_id={self.thread_id} "
                            f"elapsed={int(elapsed_time)}s timeout={self.workflow_timeout}s"
                        )

                    # Hard timeout
                    if elapsed_time >= self.workflow_timeout:
                        timeout_error = self._build_sse_event(
                            "error",
                            {
                                "thread_id": self.thread_id,
                                "error": f"Turn timed out after {int(elapsed_time)} seconds",
                                "type": "timeout_error",
                                "elapsed_seconds": int(elapsed_time),
                                "timeout_seconds": self.workflow_timeout,
                            }
                        )
                        self._error_frame = kept_error_frame(timeout_error.data)
                        yield timeout_error
                        logger.error(
                            f"[TIMEOUT_ERROR] thread_id={self.thread_id} "
                            f"exceeded timeout of {self.workflow_timeout}s"
                        )
                        raise TurnTimeLimitError(
                            f"Workflow exceeded timeout of {self.workflow_timeout} seconds"
                        )

                # A beat only wakes the coalescer and the timeout check.
                if (
                    stream_mode == "custom"
                    and isinstance(event_data, dict)
                    and event_data.get("type") == HEARTBEAT_EVENT_TYPE
                ):
                    continue

                # Log raw stream data for debugging
                logger.debug(
                    f"[STREAM_RAW] agent={agent_from_stream} mode={stream_mode} "
                    f"event_type={type(event_data).__name__}"
                )

                # Interrupt events (any stream mode): buffer, never emit
                # inline — exposure waits for the durability barrier after
                # the stream drains (I8).
                if isinstance(event_data, dict) and "__interrupt__" in event_data:
                    self.saw_interrupt = True
                    self._pending_interrupts.append(event_data)
                    continue  # Skip further processing for interrupt events
                
                
                # Handle custom events (stream_mode="custom")
                # These are emitted by get_stream_writer() in middleware/nodes
                if stream_mode == "custom":
                    if isinstance(event_data, dict):
                        event_type = event_data.get("type")

                        # Handle unified context_window events (token_usage, summarize, offload)
                        if event_type == "context_window":
                            cw_agent = self._extract_agent_name(agent_from_stream, event_data)
                            cw_data = {
                                "thread_id": self.thread_id,
                                "agent": cw_agent,
                            }
                            # Forward all relevant fields from middleware payload
                            for key in ("action", "signal", "input_tokens", "output_tokens",
                                        "total_tokens", "summary_length", "summary_text",
                                        "original_message_count", "source",
                                        "summary_tokens", "truncated_count", "error",
                                        "kind", "offloaded_args", "offloaded_reads",
                                        "threshold"):
                                if key in event_data:
                                    cw_data[key] = event_data[key]
                            if event_data.get("action") == "token_usage":
                                cw_data.setdefault("threshold", self._resolve_token_threshold())

                            action = event_data.get("action", "")
                            signal = event_data.get("signal", "")

                            # Open/close the per-namespace compaction window so
                            # the messages stream can retag chunks emitted in
                            # between. "error" must also close the window, or
                            # we'd keep flagging regular output after a failed
                            # compaction.
                            if action == "summarize":
                                ns_key = tuple(agent_from_stream or ())
                                if signal == "start":
                                    self._open_compaction_window(ns_key)
                                elif signal in ("complete", "error"):
                                    self._close_compaction_window(ns_key)

                            logger.debug(
                                f"[CONTEXT_WINDOW] Emitting {action}/{signal} "
                                f"(thread_id={self.thread_id})"
                            )
                            yield self._build_sse_event("context_window", cw_data)
                            continue

                        # Handle provenance records (data the agent accessed).
                        # The middleware emits agent=None on purpose; resolve it
                        # here from the LangGraph namespace. All other fields
                        # pass through flat to match the frontend's
                        # ProvenanceEvent.
                        if event_type == "provenance":
                            prov_data = self._resolve_provenance_event(
                                event_data, agent_from_stream
                            )
                            self.provenance_events.append(
                                {
                                    "event": "provenance",
                                    "data": dict(prov_data),
                                    "ts": time.time(),
                                }
                            )
                            yield self._build_sse_event("provenance", prov_data)
                            continue

                        # Handle steering delivery signal
                        if event_type == "steering_delivered":
                            yield self._build_sse_event("steering_delivered", {
                                "thread_id": self.thread_id,
                                "count": event_data.get("count", 0),
                                "messages": event_data.get("messages", []),
                                "timestamp": event_data.get("timestamp"),
                            })
                            continue

                        # ``model_fallback`` arrives as a langgraph ui record
                        # (push_ui_message dual-writes the custom stream + the
                        # checkpointed ui channel); unwrap to the legacy wire
                        # shape so the resilience branch below handles it.
                        fallback_ref: Dict[str, Any] = {}
                        if (
                            event_type == "ui"
                            and event_data.get("name") == "model_fallback"
                        ):
                            if not agent_from_stream:
                                kept = _fallback_record(event_data)
                                self._model_fallbacks.append(kept)
                                # Names the record, so recovery can keep a
                                # notice whose step died with the worker.
                                fallback_ref = {
                                    "fallback_id": kept["id"],
                                    **kept["metadata"],
                                }
                            event_type = "model_fallback"
                            event_data = event_data.get("props") or {}

                        # Handle model resilience progress (retry / fallback).
                        # ``model_retry`` is live + reconnect only; replay shows
                        # ``model_fallback`` from its checkpointed ui record.
                        if event_type in ("model_retry", "model_fallback"):
                            # Same attribution contract as provenance events
                            # (a detached subagent's never reach this producer):
                            # the main agent streams with an empty namespace and
                            # the middleware payload has no langgraph_node, so
                            # pin "main" instead of _extract_agent_name's
                            # "agent" fallback.
                            resilience_data: Dict[str, Any] = {
                                "thread_id": self.thread_id,
                                "agent": self._extract_agent_name(
                                    agent_from_stream, event_data
                                )
                                if agent_from_stream
                                else "main",
                            }
                            for key in (
                                "model",
                                "attempt",
                                "max_retries",
                                "delay_seconds",
                                *MODEL_FALLBACK_FIELDS,
                            ):
                                if key in event_data:
                                    resilience_data[key] = event_data[key]
                            # Middleware error strings bypass _build_error_event's
                            # scrubbing — sanitize here or BYOK URL userinfo leaks.
                            error_text = event_data.get("error")
                            if isinstance(error_text, str):
                                resilience_data["error"] = _sanitize_error_text(
                                    error_text
                                )
                            resilience_data.update(fallback_ref)
                            yield self._build_sse_event(event_type, resilience_data)
                            continue

                        # Official langgraph.graph.ui messages (push_ui_message
                        # dual-writes to the custom stream + the ui state key).
                        # Map onto the existing artifact SSE event so the wire
                        # contract is unchanged.
                        if event_type == "ui":
                            ui_agent = self._extract_agent_name(agent_from_stream, {})
                            ui_artifact_event = {
                                "artifact_type": event_data.get("name"),
                                "artifact_id": event_data.get("id"),
                                "agent": ui_agent,
                                "timestamp": None,
                                "status": None,
                                "payload": event_data.get("props", {}),
                            }
                            yield self._build_sse_event("artifact", ui_artifact_event)
                            continue

                        # Live market-watch stamp notification. Transient like
                        # ``metadata``/``model_retry``: the durable watchlist
                        # re-seeds the chip on replay via
                        # GET /{thread}/market-watch (see
                        # tests/.../history/test_event_ledger.py LIVE_ONLY).
                        if event_type == "market_watch_update":
                            yield self._build_sse_event(
                                "market_watch_update",
                                {
                                    "thread_id": self.thread_id,
                                    "symbols": event_data.get("symbols", []),
                                    "content": event_data.get("content", ""),
                                    "timestamp": event_data.get("timestamp"),
                                },
                            )
                            continue

                        # Check if this is an artifact event from middleware
                        # Generic handler: any event with artifact_type is emitted as artifact SSE
                        artifact_type = event_data.get("artifact_type")
                        if artifact_type:
                            extracted_agent_name = self._extract_agent_name(agent_from_stream, {})
                            agent_name = event_data.get("agent") or extracted_agent_name
                            payload = event_data.get("payload", {})

                            # Build artifact event with proper structure
                            artifact_event = {
                                "artifact_type": artifact_type,
                                "artifact_id": event_data.get("artifact_id"),
                                "agent": agent_name,
                                "timestamp": event_data.get("timestamp"),
                                "status": event_data.get("status"),
                                "payload": payload,
                            }

                            logger.debug(
                                f"[ARTIFACT_CUSTOM] Emitting {artifact_type} artifact "
                                f"(agent={agent_name}, status={artifact_event.get('status')})"
                            )
                            yield self._build_sse_event("artifact", artifact_event)
                    continue

                # State updates (stream_mode="updates") carry no SSE payloads of
                # their own — the mode stays subscribed because interrupts
                # arrive through it (handled above via "__interrupt__"). A
                # main-graph node's update names the message it committed.
                if stream_mode == "updates":
                    if not agent_from_stream and isinstance(event_data, dict):
                        self._reasoning.node_updated(event_data)
                        self._note_saved_images(event_data)
                    continue

                # Process message chunks (stream_mode="messages")
                if stream_mode != "messages":
                    continue

                message_chunk, message_metadata = cast(
                    tuple[BaseMessage, dict[str, Any]], event_data
                )

                # Extract agent identity from namespace tuple (subgraphs) and metadata (parent graph)
                agent_name = self._extract_agent_name(agent_from_stream, message_metadata)

                # Chunks emitted between this namespace's "summarize" start
                # and complete/error signals are re-routed to compaction_chunk.
                # Keyed by the raw namespace so an in-flight subagent
                # compaction never swallows the main agent's regular output.
                is_compaction_chunk = (
                    tuple(agent_from_stream or ()) in self._compaction_windows
                )

                # Log metadata for debugging (guarded to avoid eager f-string evaluation)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"[MESSAGE_METADATA] agent={agent_name} metadata={message_metadata}"
                    )
                    logger.debug(
                        f"[MESSAGE_KWARGS] agent={agent_name} additional_kwargs={message_chunk.additional_kwargs}"
                    )
                    logger.debug(
                        f"[MESSAGE_RESPONSE_META] agent={agent_name} response_metadata={message_chunk.response_metadata}"
                    )
                    logger.debug(
                        f"[RAW_CONTENT] agent={agent_name} type={type(message_chunk).__name__} content={message_chunk.content}"
                    )

                    if reasoning_raw := message_chunk.additional_kwargs.get("reasoning_content"):
                        logger.debug(
                            f"[RAW_REASONING] agent={agent_name} reasoning_content={reasoning_raw}"
                        )

                node = (message_metadata or {}).get("langgraph_node")
                if (
                    not agent_from_stream
                    and not is_compaction_chunk
                    and node != "tools"
                    and isinstance(message_chunk, (AIMessage, AIMessageChunk))
                    and message_chunk.id
                ):
                    self._reasoning.saw_root_chunk(str(node), message_chunk.id)
                if not agent_from_stream and not is_compaction_chunk:
                    self._note_quiet(message_chunk, node)

                # Process the message chunk
                async for event in self._process_message_chunk(
                    message_chunk,
                    agent_name,
                    message_metadata,
                    is_compaction=is_compaction_chunk,
                ):
                    yield event

            # I8 durability barrier. The graph iterator is exhausted, so the
            # executor has torn down and awaited its submitted saver tasks;
            # now verify a run-matching pending interrupt actually reached
            # the checkpointer before exposing the interrupt to anyone.
            if self._pending_interrupts:
                self.interrupt_verified = await self._verify_interrupt_durable(graph)
                if self.interrupt_verified:
                    self.interrupt_reason, self.pause_message = classify_interrupts(
                        intr
                        for pending in self._pending_interrupts
                        for intr in pending.get("__interrupt__", ())
                    )
                    for pending in self._pending_interrupts:
                        interrupt_event = self._handle_interrupt(pending)
                        if interrupt_event:
                            yield interrupt_event
                else:
                    logger.error(
                        f"[INTERRUPT_BARRIER] Pending interrupt NOT durable for "
                        f"thread_id={self.thread_id} run_id={self.run_id} — "
                        f"suppressing interrupt SSE; turn will finalize failed"
                    )
                    yield self._build_error_event(
                        "The assistant paused for input but the pause could not be "
                        "saved. Please retry this message.",
                    )

            # After workflow completes, emit credit_usage event
            try:
                from src.server.services.persistence.usage import UsagePersistenceService

                per_call_records = None
                if self.token_callback:
                    per_call_records = self.token_callback.get_per_call_records()

                # Get tool usage (non-destructive read, can be called multiple times)
                tool_usage = self.get_tool_usage()

                # Calculate credits if we have usage data
                if per_call_records or tool_usage:
                    # Calculate total credits using same logic as persistence
                    credit_service = UsagePersistenceService(
                        thread_id=self.thread_id,
                        workspace_id="temp",  # Not needed for calculation
                        user_id="temp"
                    )

                    # One pricing pass, reused for the display payload. The
                    # authoritative pass runs again at finalize (coordinator's
                    # usage writer) against whatever records exist by then.
                    token_usage = {}
                    if per_call_records:
                        token_usage = await credit_service.track_llm_usage(per_call_records)

                    if tool_usage:
                        credit_service.record_tool_usage_batch(tool_usage)

                    total_credits = credit_service.get_total_credits()

                    # Emit credit_usage event
                    yield self._build_credit_usage_event(
                        thread_id=self.thread_id,
                        token_usage=token_usage,
                        total_credits=total_credits
                    )

                    logger.debug(
                        f"[Credit SSE] Emitted credit_usage event: "
                        f"{total_credits:.2f} credits for thread_id={self.thread_id}"
                    )
            except Exception as e:
                # Don't fail workflow if credit event fails
                logger.warning(
                    f"[Credit SSE] Failed to emit credit_usage event for thread_id={self.thread_id}: {e}"
                )

        except asyncio.CancelledError:
            logger.info(f"SSE streaming ended for thread_id={self.thread_id} (client connection lost)")
            _stream_span.set_attribute("outcome", "cancelled")
            # Don't yield error event - this is expected behavior
            raise
        except Exception as e:
            if isinstance(e, TurnTimeLimitError):
                # A planned stop the [TIMEOUT_ERROR] line has already named;
                # its traceback is only this generator's own raise.
                logger.warning(
                    f"Turn stopped at its time limit for thread_id={self.thread_id}: {e}"
                )
            else:
                logger.exception(f"Error in stream generator for thread_id={self.thread_id}: {e}")
            _stream_span.record_exception(e)
            _stream_span.set_status(Status(StatusCode.ERROR))
            if not isinstance(e, TurnTimeLimitError):
                yield self._build_error_event(str(e), exc=e)
            raise  # Re-raise so the run executor finalizes as failed
        finally:
            # Safety net: if the stream ends (timeout / error / CancelledError /
            # aclose()) while a compaction window is still open, release the
            # admission window so a queued POST is never blocked forever.
            # close_auto_compaction_window is idempotent w.r.t. the normal close path above.
            if self._compaction_active:
                try:
                    from src.server.services.thread_mutation import (
                        ThreadMutationRunner,
                    )

                    ThreadMutationRunner.get_instance().close_auto_compaction_window(
                        self.thread_id
                    )
                except Exception:
                    pass
                self._compaction_active = False
            # Clear any stale windows alongside the active flag so a reused
            # handler re-arms the guard (was_empty stays accurate) on reopen.
            self._compaction_windows.clear()
            if timeout_warning_sent:
                _stream_span.set_attribute("timeout_warning", True)
            _stream_span.end()
            # Leaving the loop does not close the graph's stream, and until it
            # closes, the model calls and tools in flight keep running. Left to
            # the garbage collector, a turn stopped at its time limit ran on
            # past its finalize, and a model call finishing then was spent with
            # no usage recorded. Closing it here cancels and awaits them before
            # the run's ending reaches the executor, within a bound, since
            # LangGraph waits on its cancelled tasks without one. Last, so a
            # cancel landing mid-close skips nothing above.
            await aclose_within(
                graph_stream,
                GRAPH_CLOSE_TIMEOUT_S,
                label=(
                    f"the graph stream for thread_id={self.thread_id} "
                    f"run_id={self.run_id}"
                ),
            )

    async def _verify_interrupt_durable(self, graph) -> bool:
        """True iff the checkpoint tip carries a pending __interrupt__ matching
        a buffered one (by interrupt id when available).

        Uses the graph's own checkpointer (in Phase 2 the session-bound
        per-run saver, so verification rides the same fenced connection);
        falls back to the global checkpointer.
        """
        try:
            checkpointer = getattr(graph, "checkpointer", None)
            if checkpointer is None:
                from src.server.app import setup

                checkpointer = setup.checkpointer
            if checkpointer is None:
                return False

            cp_tuple = await checkpointer.aget_tuple(
                {"configurable": {"thread_id": self.thread_id}}
            )
            if not cp_tuple or not cp_tuple.pending_writes:
                return False

            buffered_ids = set()
            for event_data in self._pending_interrupts:
                for intr in event_data.get("__interrupt__", ()):
                    intr_id = getattr(intr, "id", None)
                    if intr_id:
                        buffered_ids.add(intr_id)

            for _task_id, channel, value in cp_tuple.pending_writes:
                if channel != "__interrupt__":
                    continue
                if not buffered_ids:
                    return True
                interrupts = value if isinstance(value, list) else [value]
                for intr in interrupts:
                    if getattr(intr, "id", None) in buffered_ids:
                        return True
            return False
        except Exception:
            logger.error(
                f"[INTERRUPT_BARRIER] Verification read failed for "
                f"thread_id={self.thread_id}",
                exc_info=True,
            )
            return False

    def _handle_interrupt(self, event_data: dict) -> Optional[StreamFrame]:
        """Build the ``interrupt`` frame for an ``__interrupt__`` event."""
        interrupt_obj = event_data["__interrupt__"][0]

        # Log interrupt trigger
        logger.debug(f"[INTERRUPT_TRIGGER] thread_id={self.thread_id} interrupt_id={interrupt_obj.id}")
        logger.debug(f"[INTERRUPT_VALUE] value={interrupt_obj.value}")
        logger.debug(f"[INTERRUPT_FULL] event_data={event_data}")

        # Extract action requests from interrupt value
        # HITL middleware provides action_requests with tool call info and description
        interrupt_value = interrupt_obj.value
        action_requests = []

        if isinstance(interrupt_value, dict):
            # New format: value contains action_requests directly
            action_requests = interrupt_value.get("action_requests", [])
            if not action_requests and "description" in interrupt_value:
                # Fallback: description at top level
                action_requests = [{"description": interrupt_value["description"]}]
        elif isinstance(interrupt_value, list):
            # Value is already a list of action requests
            action_requests = interrupt_value
        elif isinstance(interrupt_value, str):
            # Value is a string description (plan description)
            action_requests = [{"description": interrupt_value}]

        kind = interrupt_value.get("kind") if isinstance(interrupt_value, dict) else None
        return self._build_sse_event(
            "interrupt",
            {
                "thread_id": self.thread_id,
                "interrupt_id": interrupt_obj.id,
                **({"kind": kind} if isinstance(kind, str) else {}),
                "action_requests": action_requests,
                "role": "assistant",
                "finish_reason": "interrupt",
            },
        )

    def _resolve_provenance_event(
        self, event_data: dict, namespace_tuple: tuple
    ) -> dict:
        """Strip the internal ``type`` and resolve agent attribution.

        The main agent streams with an empty namespace (and the event carries no
        ``langgraph_node``), so it is pinned to ``"main"`` rather than the
        ``_extract_agent_name`` fallback, honoring the contract that ``agent`` is
        ``"main"`` | ``"task:{id}"``. A subagent's records ride its own task
        stream, where the forwarder stamps ``task:{id}``.
        """
        if namespace_tuple:
            agent = self._extract_agent_name(namespace_tuple, event_data)
        else:
            agent = MAIN_PROVENANCE_LANE
        prov_data = {
            key: value for key, value in event_data.items() if key != "type"
        }
        prov_data["agent"] = agent
        return prov_data

    def _note_quiet(self, message: BaseMessage, node: Any) -> None:
        """Keep the main agent's quiet ending, as the finalize records it.

        The quiet guard ends the run on that result, so it stands only while
        nothing follows it: a model call after it (a background task's report
        re-invoking the agent) means the turn spoke after all.
        """
        if isinstance(message, ToolMessage):
            if (found := quiet_result(message)) is not None:
                self.quiet = found
        elif isinstance(message, (AIMessage, AIMessageChunk)) and node != "tools":
            self.quiet = None

    def _extract_agent_name(self, namespace_tuple: tuple, message_metadata: dict) -> str:
        """Return the agent identifier for an event.

        Priority:
        1. `namespace_tuple[-1]` (verbatim)
        2. `checkpoint_ns` (verbatim)
        3. `langgraph_node`
        """
        if namespace_tuple:
            return str(namespace_tuple[-1])

        checkpoint_ns = message_metadata.get("checkpoint_ns")
        if checkpoint_ns:
            return str(checkpoint_ns)

        return str(message_metadata.get("langgraph_node", "agent"))

    async def _process_message_chunk(
        self,
        message_chunk: BaseMessage,
        agent_name: str,
        message_metadata: dict[str, Any] | None = None,
        *,
        is_compaction: bool = False,
    ) -> AsyncGenerator[StreamFrame, None]:
        """Process a single message chunk and yield its built frames.

        When ``is_compaction`` is True, text / reasoning / finish events are
        emitted as ``compaction_chunk`` instead of ``message_chunk`` so the UI
        can render them in a dedicated channel.
        """
        message_id = message_chunk.id or "unknown"
        chunk_event_type = "compaction_chunk" if is_compaction else "message_chunk"

        # Tool-node inner LLM output (e.g. WebFetch's summarization model) is
        # internal — the tool's user-facing result arrives via tool_call_result.
        # Key on langgraph_node="tools" rather than agent_name: agent_name comes
        # from the namespace tuple and resolves to task:*/research:* for tools
        # invoked inside subagent subgraphs, which would mask the tool-node
        # signal. langgraph_node is set by Pregel itself and is the canonical
        # tool-node marker — see langchain/agents/factory.py:1369 where
        # create_agent registers the tool node as graph.add_node("tools", ...).
        metadata = message_metadata or {}
        is_tool_node = metadata.get("langgraph_node") == "tools"

        # Check for thinking/reasoning status signals in main content
        status_info = is_thinking_status_signal(message_chunk.content)
        if status_info:
            if not is_tool_node:
                if status_info.get("status") == "completed":
                    # Reasoning completed - emit completion signal
                    if agent_name in self.reasoning_active:
                        yield self._build_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                        self.reasoning_active.discard(agent_name)
                    self._reasoning_block_index.pop(agent_name, None)
                    self._reasoning_separator_pending.discard(agent_name)
                else:
                    # Reasoning started - emit start signal
                    if agent_name not in self.reasoning_active:
                        yield self._build_reasoning_signal(agent_name, message_id, "start", is_compaction=is_compaction)
                        self.reasoning_active.add(agent_name)
            return  # Don't process status signals as regular content

        # Check for thinking status in reasoning_content field as well
        # Support both "reasoning_content" and "reasoning" fields
        reasoning_content_from_kwargs = (
            message_chunk.additional_kwargs.get("reasoning_content") or
            message_chunk.additional_kwargs.get("reasoning")
        )
        if reasoning_content_from_kwargs:
            reasoning_status = is_thinking_status_signal(reasoning_content_from_kwargs)
            if reasoning_status:
                if not is_tool_node:
                    if reasoning_status.get("status") == "completed":
                        # Reasoning completed - emit completion signal if agent was actively streaming
                        if agent_name in self.reasoning_active:
                            yield self._build_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                            self.reasoning_active.discard(agent_name)
                        self._reasoning_block_index.pop(agent_name, None)
                        self._reasoning_separator_pending.discard(agent_name)
                    else:
                        # Reasoning started - emit start signal
                        if agent_name not in self.reasoning_active:
                            yield self._build_reasoning_signal(agent_name, message_id, "start", is_compaction=is_compaction)
                            self.reasoning_active.add(agent_name)
                return  # Don't process status signals as regular content

        # Check for function_call in content (Response API tool call streaming)
        # Response API streams tool arguments as content[type=function_call]
        # Claude (Anthropic) streams tool arguments as content[type=input_json_delta]
        if isinstance(message_chunk.content, list):
            for item in message_chunk.content:
                if isinstance(item, dict):
                    # Response API: type=function_call
                    if item.get('type') == 'function_call':
                        # Handle arguments=null (Doubao) vs arguments="" (GPT-5)
                        arguments = item.get("arguments") or ""
                        index = item.get("index")

                        # Track state: Response API sends name/call_id only in first chunk
                        # Need to accumulate arguments across chunks for final tool_calls emission
                        state_key = (agent_name, index)

                        # Initialize state if not exists
                        if state_key not in self.function_call_state:
                            self.function_call_state[state_key] = {
                                "name": None,
                                "call_id": None,
                                "args_accumulated": ""
                            }

                        # Update state with new info from this chunk
                        if item.get("name"):
                            self.function_call_state[state_key]["name"] = item.get("name")
                            self.function_call_state[state_key]["call_id"] = item.get("call_id")

                        # Accumulate arguments
                        self.function_call_state[state_key]["args_accumulated"] += arguments

                        # Retrieve current state
                        persisted_state = self.function_call_state[state_key]

                        tool_call_chunk = {
                            "name": item.get("name") or persisted_state.get("name"),
                            "args": arguments,
                            "id": item.get("call_id") or persisted_state.get("call_id"),
                            "index": index,
                            "type": "tool_call_chunk"
                        }

                        event_stream_message = {
                            "thread_id": self.thread_id,
                            "agent": agent_name,
                            "id": message_id,
                            "role": "assistant",
                            "tool_call_chunks": [tool_call_chunk],
                        }

                        # Log with final name (either from chunk or persisted)
                        final_name = item.get("name") or persisted_state.get("name")
                        logger.debug(
                            f"[FUNCTION_CALL_EXTRACTED] agent={agent_name} name={final_name} "
                            f"args_length={len(arguments)} persisted={bool(persisted_state)}"
                        )

                        yield self._build_sse_event("tool_call_chunks", event_stream_message)
                        return  # Don't process function_call as regular content

                    # Claude (Anthropic): type=tool_use (initial metadata)
                    # Anthropic sends tool name/id in initial tool_use chunk, then streams args via input_json_delta
                    elif item.get('type') == 'tool_use':
                        index = item.get("index", 0)

                        # A tool_use block always opens a new call, and block
                        # indexes restart at 0 on every model call. An entry
                        # already at this index is an earlier call that never
                        # went out (a stream that died mid-arguments and was
                        # retried here): kept, it would lend this call its
                        # name, id and arguments. Its entries at other indexes
                        # go at this message's finish.
                        self.anthropic_tool_call_state[(agent_name, index)] = {
                            "name": item.get("name"),
                            "id": item.get("id"),
                            "message_id": message_id,
                            "args_accumulated": "",
                        }

                        logger.debug(
                            f"[TOOL_USE_METADATA] agent={agent_name} name={item.get('name')} "
                            f"id={item.get('id')} index={index}"
                        )

                        # Don't emit event for metadata capture, just store for later
                        return  # Don't process tool_use metadata as regular content

                    # Claude (Anthropic): type=input_json_delta (streaming args)
                    elif item.get('type') == 'input_json_delta':
                        # Handle partial_json=null (unlikely but defensive)
                        partial_json = item.get("partial_json") or ""
                        index = item.get("index", 0)

                        # Accumulate for completion handler
                        state = self.anthropic_tool_call_state.get((agent_name, index), {})
                        if state:
                            state["args_accumulated"] += partial_json

                        # Named on every chunk, as the Response API route is:
                        # the client hides and labels a preparing call by name.
                        tool_call_chunk = {
                            "name": state.get("name"),
                            "args": partial_json,
                            "id": state.get("id"),
                            "index": index,
                            "type": "tool_call_chunk"
                        }

                        event_stream_message = {
                            "thread_id": self.thread_id,
                            "agent": agent_name,
                            "id": message_id,
                            "role": "assistant",
                            "tool_call_chunks": [tool_call_chunk],
                        }

                        logger.debug(
                            f"[INPUT_JSON_DELTA_EXTRACTED] agent={agent_name} name={state.get('name')} "
                            f"partial_json_length={len(partial_json)} accumulated={len(state.get('args_accumulated', ''))}"
                        )

                        yield self._build_sse_event("tool_call_chunks", event_stream_message)
                        return  # Don't process input_json_delta as regular content

        # Detect reasoning summary_text index transitions before normalization
        # When the summary_text index changes (e.g., 0→1), a new reasoning thought started
        reasoning_idx = self._extract_reasoning_summary_index(message_chunk.content)
        if reasoning_idx is not None:
            prev_idx = self._reasoning_block_index.get(agent_name)
            self._reasoning_block_index[agent_name] = reasoning_idx
            if prev_idx is not None and reasoning_idx != prev_idx:
                self._reasoning_separator_pending.add(agent_name)

        # Resolve before normalization: the frame that announces the phase has
        # empty text and is dropped below, so the table must be filled first.
        text_phase = self._text_phase.resolve(agent_name, message_id, message_chunk.content)

        # Normalize main content - extract text and get content type
        text_content, content_type = normalize_text_content(message_chunk.content)

        # Also check for reasoning content in additional_kwargs
        if reasoning_content_from_kwargs:
            reasoning_text, reasoning_type = normalize_text_content(reasoning_content_from_kwargs)
            if reasoning_text:
                # If we already have content, append reasoning
                # Otherwise, use reasoning as the content
                if text_content:
                    text_content += reasoning_text
                else:
                    text_content = reasoning_text
                # Override content type to reasoning since we have reasoning content
                content_type = "reasoning"

        is_inner_llm_chunk = is_tool_node and isinstance(message_chunk, (AIMessage, AIMessageChunk))

        # Whitespace alone never opens a reasoning section (the client would
        # render an empty thought), but it is still text: hold it until visible
        # reasoning follows, and drop it if the reply turns to text instead.
        if text_content and not is_inner_llm_chunk:
            held_id, held = self._reasoning_whitespace_held.pop(agent_name, ("", ""))
            if content_type == "reasoning":
                # A stream that fails mid-way sends no finish frame, so its held
                # whitespace must not lead the retry's reasoning.
                if held_id == message_id:
                    text_content = held + text_content
                if agent_name not in self.reasoning_active and not text_content.strip():
                    self._reasoning_whitespace_held[agent_name] = (message_id, text_content)
                    text_content = None

        # Prepend separator when transitioning between reasoning blocks
        if text_content and content_type == "reasoning" and agent_name in self._reasoning_separator_pending:
            text_content = "\n\n" + text_content
            self._reasoning_separator_pending.discard(agent_name)

        event_stream_message: dict[str, Any] = {
            "thread_id": self.thread_id,
            "agent": agent_name,
            "id": message_id,
            "role": "assistant",
        }

        # Add text content if present (can be regular text or reasoning).
        # Drop both text and reasoning from tool-node inner LLM AI chunks
        # (e.g. web_fetch's extraction model) — the tool's user-facing output
        # arrives via the ToolMessage that this same node emits next, so
        # surfacing the inner model's AI chunks would double-render the
        # result and leak the extraction model's reasoning to the user.
        # ToolMessages themselves carry the tool's actual return value and
        # MUST flow through (their content becomes ``tool_call_result``).
        if text_content and content_type and not is_inner_llm_chunk:
            # Check if we need to emit reasoning completion signal
            if content_type != "reasoning" and agent_name in self.reasoning_active:
                # Reasoning completed, emit completion signal before this content
                yield self._build_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                self.reasoning_active.discard(agent_name)
                self._reasoning_block_index.pop(agent_name, None)
                self._reasoning_separator_pending.discard(agent_name)

            event_stream_message["content"] = text_content
            event_stream_message["content_type"] = content_type  # "text" or "reasoning"
            if content_type == "text" and text_phase:
                event_stream_message["phase"] = text_phase

            # Handle reasoning content lifecycle
            if content_type == "reasoning":
                # Emit start signal if this is the first reasoning content
                # This handles providers that send content directly without status signal
                if agent_name not in self.reasoning_active:
                    yield self._build_reasoning_signal(agent_name, message_id, "start", is_compaction=is_compaction)
                    self.reasoning_active.add(agent_name)

        # Handle finish_reason/stop_reason - emit reasoning completion if needed
        # Different providers use different field names:
        # - Anthropic: stop_reason (e.g., "end_turn", "tool_use")
        # - OpenAI Chat: finish_reason (e.g., "stop", "length", "tool_calls")
        # - OpenAI Response API: status (e.g., "completed", "failed")
        finish_reason = (
            message_chunk.response_metadata.get("stop_reason") or
            message_chunk.response_metadata.get("finish_reason") or
            # Response API uses status="completed" instead of finish_reason
            (message_chunk.response_metadata.get("status")
             if message_chunk.response_metadata.get("status") in ["completed", "failed"]
             else None)
        )

        # Normalize finish_reason to standard values for consistent handling
        original_finish_reason = finish_reason
        if finish_reason:
            # A finish speaks for its own message's calls. A Claude entry
            # another message left in this lane is a call whose stream died
            # mid-arguments before a retry or a fallback, to any provider,
            # streamed here: kept, it would go out as a call that never runs
            # and turn a plain answer's finish into tool_calls. Every chunk of
            # one Claude call carries the same message id.
            for stale_key in [
                key
                for key, state in self.anthropic_tool_call_state.items()
                if key[0] == agent_name and state.get("message_id") != message_id
            ]:
                del self.anthropic_tool_call_state[stale_key]

            # Check if we have tool call state for this agent to disambiguate "completed"
            has_response_api_tool_state = any(
                key[0] == agent_name and state.get("args_accumulated") and state.get("name")
                for key, state in self.function_call_state.items()
            )
            has_anthropic_tool_state = any(
                key[0] == agent_name and state.get("args_accumulated") and state.get("name")
                for key, state in self.anthropic_tool_call_state.items()
            )
            has_tool_call_state = has_response_api_tool_state or has_anthropic_tool_state

            # Normalize provider-specific finish reasons to standard values
            # Standard values: "tool_calls", "stop", "error", or pass-through (e.g., "length")
            if finish_reason == "tool_use":
                # Anthropic tool call completion
                finish_reason = "tool_calls"
            elif finish_reason == "completed" and has_tool_call_state:
                # Response API tool call completion
                finish_reason = "tool_calls"
            elif finish_reason == "end_turn":
                # Anthropic normal completion
                finish_reason = "stop"
            elif finish_reason == "completed" and not has_tool_call_state:
                # Response API normal completion
                finish_reason = "stop"
            elif finish_reason == "STOP":
                # Gemini (normalize to lowercase)
                finish_reason = "stop"
            elif finish_reason == "failed":
                # Response API failure
                finish_reason = "error"
            # Other values (e.g., "length", "tool_calls") pass through unchanged

            logger.debug(
                f"[FINISH_SIGNAL] agent={agent_name} original={original_finish_reason} "
                f"normalized={finish_reason} has_tool_state={has_tool_call_state} "
                f"response_metadata={message_chunk.response_metadata}"
            )

            self._text_phase.finish(agent_name)
            self._reasoning_whitespace_held.pop(agent_name, None)

            # If finishing while reasoning is active, emit completion signal
            if agent_name in self.reasoning_active:
                yield self._build_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                self.reasoning_active.discard(agent_name)
                self._reasoning_block_index.pop(agent_name, None)
                self._reasoning_separator_pending.discard(agent_name)

            # Unified tool call completion handler for all providers
            # After normalization, both Response API "completed" and Anthropic "tool_use"
            # are normalized to "tool_calls"
            if finish_reason == "tool_calls":
                # Combine both Response API and Anthropic tool call states
                # Response API uses: {call_id, name, args_accumulated}
                # Anthropic uses: {id, name, args_accumulated}
                all_tool_states = [
                    (state_key, state, "response_api")
                    for state_key, state in self.function_call_state.items()
                ] + [
                    (state_key, state, "anthropic")
                    for state_key, state in self.anthropic_tool_call_state.items()
                ]

                for state_key, state, provider_type in all_tool_states:
                    # Only emit for current agent
                    if state_key[0] != agent_name:
                        continue

                    # Only emit if we have accumulated args and a name
                    if not state.get("args_accumulated") or not state.get("name"):
                        logger.debug(
                            f"[TOOL_CALL_SKIP] agent={agent_name} provider={provider_type} "
                            f"name={state.get('name')} has_args={bool(state.get('args_accumulated'))}"
                        )
                        continue

                    raw_args = state["args_accumulated"]
                    parsed_args, err_repr, err_window = _parse_tool_args(raw_args)

                    args_parse_error = parsed_args is None
                    if args_parse_error:
                        logger.error(
                            f"[TOOL_CALL_PARSE_ERROR] agent={agent_name} provider={provider_type} "
                            f"name={state.get('name')} args_length={len(raw_args)} "
                            f"error={err_repr} window={err_window!r}"
                        )
                        # The graph parses args independently and may still run this
                        # call — dropping the event leaves the stream without a card
                        # for a call whose tool_call_result arrives later. Emit with
                        # empty args so the call stays addressable by id.
                        parsed_args = {}

                    try:
                        # id field differs by provider: "call_id" for Response API, "id" for Anthropic.
                        tool_call_id = state.get("call_id") or state.get("id")
                        tool_calls = [{
                            "name": state["name"],
                            "args": parsed_args,
                            "id": tool_call_id,
                            "type": "tool_call"
                        }]

                        tool_calls_message = {
                            "thread_id": self.thread_id,
                            "agent": agent_name,
                            "id": message_id,
                            "role": "assistant",
                            "tool_calls": tool_calls,
                            "finish_reason": finish_reason,
                        }
                        if args_parse_error:
                            tool_calls_message["args_parse_error"] = True

                        logger.debug(
                            f"[TOOL_CALLS_COMPLETE] agent={agent_name} provider={provider_type} "
                            f"name={state['name']} args_length={len(raw_args)} "
                            f"id={tool_call_id}"
                        )

                        yield self._build_sse_event("tool_calls", tool_calls_message)

                        # Clear state after emitting from the appropriate state dictionary
                        if provider_type == "response_api":
                            self.function_call_state.pop(state_key, None)
                        else:  # anthropic
                            self.anthropic_tool_call_state.pop(state_key, None)

                    except Exception as e:
                        logger.error(
                            f"[TOOL_CALL_ERROR] agent={agent_name} provider={provider_type} error={e}"
                        )

            event_stream_message["finish_reason"] = finish_reason
        else:
            # Log when response_metadata exists but no finish reason is found
            # This helps debug cases where completion signals might be missing
            if message_chunk.response_metadata:
                logger.debug(
                    f"[NO_FINISH_SIGNAL] agent={agent_name} "
                    f"response_metadata={message_chunk.response_metadata}"
                )

        # Handle different message types
        if isinstance(message_chunk, ToolMessage):
            # Tool Message - Return the result of the tool call
            event_stream_message["tool_call_id"] = message_chunk.tool_call_id

            # The whole status travels, not just the failing one. An explicit
            # success is what stops a client from reading prose that merely
            # looks like a failure.
            tool_status = getattr(message_chunk, "status", None)
            if tool_status:
                event_stream_message["status"] = tool_status

            # Check for artifact (native LangChain pattern for metadata)
            # Artifact contains complete metadata (URLs, favicons, images) for frontend
            # while message content is filtered for LLM consumption
            if hasattr(message_chunk, 'artifact') and message_chunk.artifact:
                event_stream_message["artifact"] = message_chunk.artifact
                logger.debug(
                    f"[TOOL_ARTIFACT] agent={agent_name} tool_call_id={message_chunk.tool_call_id} "
                    f"artifact_keys={list(message_chunk.artifact.keys()) if isinstance(message_chunk.artifact, dict) else 'non-dict'}"
                )

            # Emit task artifact as a dedicated artifact SSE event
            task_artifact = message_chunk.additional_kwargs.get("task_artifact")
            if task_artifact:
                # No top-level status: a live task artifact can't know its
                # task's outcome — the client derives status from the task
                # lifecycle, replay stamps payload.status from liveness truth.
                yield self._build_sse_event("artifact", {
                    "artifact_type": "task",
                    "artifact_id": f"task:{task_artifact['task_id']}",
                    "agent": "main",
                    "thread_id": self.thread_id,
                    "payload": task_artifact,
                    "tool_call_id": message_chunk.tool_call_id,
                })

            yield self._build_sse_event("tool_call_result", event_stream_message)

        elif isinstance(message_chunk, (AIMessageChunk, AIMessage)):
            # AI Message - Raw message tokens (AIMessageChunk during streaming)
            # or complete message (AIMessage when model doesn't stream).
            #
            # During streaming, LangGraph emits AIMessageChunk tokens followed by a
            # final AIMessage with the full content at node completion. We track
            # streamed message IDs to avoid re-emitting the full content as a duplicate.
            is_chunk = isinstance(message_chunk, AIMessageChunk)
            is_complete_msg = not is_chunk

            if is_complete_msg and message_id in self._streamed_content_ids:
                # Content was already streamed via chunks — skip duplicate emission.
                # Still let finish_reason through (handled by earlier code above this block).
                pass

            elif message_chunk.tool_calls:
                # Filter tool calls: remove empty names and duplicates
                filtered_tool_calls = self._filter_tool_calls(message_chunk.tool_calls)

                # Only emit event if we have valid tool calls
                if filtered_tool_calls:
                    event_stream_message["tool_calls"] = filtered_tool_calls
                    # Don't include tool_call_chunks in complete tool_calls event
                    # This makes behavior consistent with Response API and Anthropic
                    yield self._build_sse_event("tool_calls", event_stream_message)
                    # Note: file_operation events are now emitted via custom events from middleware

            # Emit tool_call_chunks event for client consumption (if present)
            elif is_chunk and message_chunk.tool_call_chunks:
                event_stream_message["tool_call_chunks"] = message_chunk.tool_call_chunks
                yield self._build_sse_event("tool_call_chunks", event_stream_message)

            else:
                # AI Message - Raw message tokens
                # Only emit if there's actual content to send
                has_content = (
                    event_stream_message.get("content") or
                    event_stream_message.get("finish_reason")
                )

                if has_content:
                    if is_chunk and event_stream_message.get("content"):
                        self._streamed_content_ids.add(message_id)
                    yield self._build_sse_event(chunk_event_type, event_stream_message)

    def _filter_tool_calls(self, tool_calls: list) -> list:
        """Remove tool calls with empty names or already-seen IDs."""
        filtered_tool_calls = []
        for tool_call in tool_calls:
            tool_id = tool_call.get("id")
            tool_name = tool_call.get("name", "")

            # Skip if no name or empty name
            if not tool_name or not tool_name.strip():
                continue

            # Skip if already seen
            if tool_id and tool_id in self.seen_tool_ids:
                continue

            # Add to filtered list and mark as seen
            filtered_tool_calls.append(tool_call)
            if tool_id:
                self.seen_tool_ids.add(tool_id)

        return filtered_tool_calls

    @staticmethod
    def _extract_reasoning_summary_index(content: Any) -> Optional[int]:
        """Extract the summary_text item index from reasoning content.

        Thin delegate to ``content_utils.extract_reasoning_summary_index`` —
        the shared implementation is reused by the subagent token forwarder so
        both streaming paths separate reasoning sections identically.
        """
        return extract_reasoning_summary_index(content)

    def _build_reasoning_signal(
        self,
        agent_name: str,
        message_id: str,
        signal_type: str,
        *,
        is_compaction: bool = False,
    ) -> StreamFrame:
        """Build a reasoning lifecycle signal frame.

        A ``complete`` carries ``elapsed_ms`` since the matching ``start``; a
        close with no recorded open (a stop that synthesizes one) carries none.
        """
        event_type = "compaction_chunk" if is_compaction else "message_chunk"
        data: Dict[str, Any] = {
            "thread_id": self.thread_id,
            "agent": agent_name,
            "id": message_id,
            "role": "assistant",
            "content": signal_type,
            "content_type": "reasoning_signal",
        }
        if signal_type == "start":
            self._reasoning_started_at[agent_name] = time.monotonic()
        elif signal_type == "complete":
            started = self._reasoning_started_at.pop(agent_name, None)
            if started is not None:
                data["elapsed_ms"] = int((time.monotonic() - started) * 1000)
                if not is_compaction:
                    self._reasoning.closed(message_id, data["elapsed_ms"])
        return self._build_sse_event(event_type, data)

    def _note_saved_images(self, update: dict) -> None:
        for output in update.values():
            if not isinstance(output, dict):
                continue
            messages = output.get("messages")
            if isinstance(messages, Overwrite):
                continue  # the window trim, restating earlier turns' messages
            if not isinstance(messages, (list, tuple)):
                messages = [messages]
            for message in messages:
                if isinstance(message, AIMessage) and message.id:
                    self._saved_image_paths[message.id] = collect_sandbox_paths(
                        [message]
                    )

    @property
    def saved_image_paths(self) -> Set[str]:
        """The sandbox images the main agent's saved messages name, which
        the model-time capture left as paths."""
        return set().union(*self._saved_image_paths.values())

    def record(self, stop_events: Optional[List[Dict[str, Any]]] = None) -> ProducerRecord:
        """What the finalize stores beside the status that the checkpoint
        does not keep. ``stop_events`` are the lanes a stop drained, whose
        provenance reaches the row only through this finalize."""
        return ProducerRecord(
            provenance=self.provenance_events
            + [
                e
                for e in stop_events or ()
                if isinstance(e, dict) and e.get("event") == "provenance"
            ],
            reasoning_ms=self._reasoning.as_facts(),
            model_fallbacks=list(self._model_fallbacks),
            error_frame=self._error_frame,
        )

    def _format_sse_event(self, event_type: str, data: dict[str, Any]) -> str:
        """Build and number one frame in a single step, for callers outside
        the stream (``stream_workflow`` numbers through its coalescer)."""
        return self._number_sse_event(self._build_sse_event(event_type, data))

    def _build_sse_event(self, event_type: str, data: dict[str, Any]) -> StreamFrame:
        """Build a frame, before numbering."""
        # Remove empty content to reduce payload size
        if data.get("content") == "":
            data.pop("content")

        frame = StreamFrame(event_type, data)
        if delta_piece(frame) is not None:
            return frame  # serialized as it goes out, joined or not
        # NaN/Inf floats from upstream data would serialize as bare `NaN`
        # tokens (invalid JSON) and make the browser drop the whole frame.
        return frame._replace(encoded=finite_json_dumps(data, ensure_ascii=False))

    def _number_sse_event(self, frame: StreamFrame) -> str:
        """Number a built frame and serialize it as ``id/event/data``."""
        event_type = frame.event
        # Only a delta arrives unencoded, and one built from strings cannot
        # fail. Serialized before numbering, so a frame that fails spends no id.
        json_data = frame.encoded
        if json_data is None:
            json_data = finite_json_dumps(frame.data, ensure_ascii=False)

        # Increment sequence number for this event
        if self.event_counter is not None:
            self.event_sequence = self.event_counter.next()
        else:
            self.event_sequence += 1

        # Include sequence ID for reconnection support
        # Format: id: sequence_number\nevent: type\ndata: json\n\n
        result = f"id: {self.event_sequence}\nevent: {event_type}\ndata: {json_data}\n\n"

        # Log SSE events to dedicated logger if enabled
        if SSE_EVENT_LOG_ENABLED:
            sse_logger.info(f"{result}")

        return result

    def _build_error_event(
        self,
        error_message: str,
        *,
        exc: Optional[BaseException] = None,
    ) -> StreamFrame:
        """Build an error frame; ``exc`` enables the classification
        ``error_event_data`` describes."""
        frame = self._build_sse_event(
            "error",
            error_event_data(
                self.thread_id,
                error_message,
                exc=exc,
                credential_source=getattr(self.agent_config, "credential_source", None),
            ),
        )
        self._error_frame = kept_error_frame(frame.data)
        return frame

    def _build_credit_usage_event(
        self,
        thread_id: str,
        token_usage: dict,
        total_credits: float
    ) -> StreamFrame:
        """Build a credit_usage frame with aggregated token counts and total credits."""
        return self._build_sse_event(
            "credit_usage",
            build_credit_usage_data(thread_id, token_usage, total_credits),
        )

    def _resolve_token_threshold(self) -> int:
        return resolve_token_threshold(self.agent_config)

    def get_tool_usage(self) -> Optional[Dict[str, int]]:
        """Return tool-name → count usage map, or None. Result is cached for cross-context access."""
        # Return cached result if already retrieved (for cross-context access)
        if self._tool_usage_result is not None:
            logger.debug(
                f"[RunSSEProducer] Returning cached tool usage for thread_id={self.thread_id}: "
                f"{len(self._tool_usage_result)} tool types, {sum(self._tool_usage_result.values())} total calls"
            )
            return self._tool_usage_result

        # Try to read from ContextVar (may fail if called from different async context)
        from src.tools.decorators import get_tool_tracker
        tracker = get_tool_tracker()
        tool_usage = tracker.get_summary() if tracker else None

        # Cache result for future calls (enables cross-context access)
        if tool_usage is not None:
            self._tool_usage_result = tool_usage
            logger.debug(
                f"[RunSSEProducer] Retrieved and cached tool usage for thread_id={self.thread_id}: "
                f"{len(tool_usage)} tool types, {sum(tool_usage.values())} total calls - {tool_usage}"
            )
        else:
            logger.debug(f"[RunSSEProducer] No tool usage found for thread_id={self.thread_id}")

        return tool_usage
