"""
Workflow Streaming Handler

LangGraph workflow SSE producer: streams graph events, normalises content,
tracks reasoning lifecycle, deduplicates tool calls, formats SSE events, and
handles timeouts. Keepalives are emitted by the SSE consumer (stream_from_log).
"""

import asyncio
import copy
import json
import logging
import re
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, cast

from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage

from src.server.utils.content_normalizer import (
    normalize_text_content,
    is_thinking_status_signal,
)
from src.utils.tracking import ExecutionTracker
from src.config.settings import (
    get_workflow_timeout,
    is_sse_event_log_enabled,
    get_merged_chunk_max_bytes,
)

logger = logging.getLogger(__name__)

# Dedicated logger for SSE events (can be configured independently)
sse_logger = logging.getLogger("sse_events")

WORKFLOW_TIMEOUT = get_workflow_timeout()  # seconds
SSE_EVENT_LOG_ENABLED = is_sse_event_log_enabled()

MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT = get_merged_chunk_max_bytes()


# ---------------------------------------------------------------------------
# Stream error classification
# ---------------------------------------------------------------------------
#
# Chat-stream failures fall into two buckets and the user-facing remedy is
# different for each:
#
#   - ``upstream``  — the LLM provider we called returned an error (their
#                     server 500'd, the key is rejected, rate-limited, etc).
#                     User should check their key / plan / provider status.
#   - ``internal``  — our own pipeline failed (middleware bug, our DB, a
#                     schema mismatch in the payload we built). User can't
#                     do anything; we should log loudly and show a generic
#                     retry message.
#
# Classification is a module-prefix check with exception-chain walking — any
# exception in the chain sourced from a known provider SDK flips the whole
# failure to ``upstream``.

# Provider SDK and LangChain-wrapper module prefixes. Any exception in the
# cause chain whose ``__module__`` matches one of these flips the whole
# failure to ``upstream``. Keep in sync with the SDKs wired up in
# ``src/llms/llm.py`` — missing a prefix means the user sees "our service
# failed" for what's really a provider error.
#
# ``httpx`` is in this list as a last-resort catch: a bare httpx exception
# that reaches the stream error handler has almost always come from the
# LangChain call path (SDKs raise via httpx). If our own service calls
# (credit checks, workspace manager) ever start raising bare httpx errors to
# the stream path we should wrap them in a distinct exception type before
# they bubble; classification is a UI hint, not a diagnostic source of truth.
_UPSTREAM_MODULE_PREFIXES: tuple[str, ...] = (
    # Raw provider SDKs
    "anthropic",
    "openai",
    "google.api_core",
    "google.genai",
    "google.generativeai",
    "cohere",
    "httpx",
    # LangChain wrappers — their exceptions may not chain through the raw SDK
    # when the wrapper normalizes errors, so match them directly.
    "langchain_openai",
    "langchain_anthropic",
    "langchain_deepseek",
    "langchain_qwq",
    "langchain_google_genai",
    "langchain_google_vertexai",
    "langchain_mistralai",
    "langchain_together",
    "langchain_groq",
    "groq",
)

_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")

# Strip basic-auth credentials out of any URL that leaks into an exception
# message (httpx will include the request URL in ``str(exc)``; a user who
# configured a BYOK base_url as ``https://user:pass@host`` would otherwise
# ship that secret to the SSE client and the replay log).
_URL_USERINFO_RE = re.compile(r"(https?://)[^@/\s]+@")


def _parse_status_from_message(text: str) -> Optional[int]:
    match = _STATUS_CODE_RE.search(text)
    return int(match.group(1)) if match else None


def _sanitize_error_text(text: str) -> str:
    """Scrub credentials out of the raw exception text before we send it."""
    return _URL_USERINFO_RE.sub(r"\1", text)


def classify_stream_exception(exc: BaseException) -> Dict[str, Any]:
    """Classify a chat-stream exception as ``upstream`` or ``internal``.

    Walks ``__cause__`` / ``__context__`` so a wrapped provider error (e.g.
    a LangChain exception caused by ``anthropic.InternalServerError``) is
    still recognized as upstream. Returns a dict with ``kind``,
    ``status_code`` (when carried on the exception or parseable from its
    message), and ``provider_module`` (the matched SDK prefix, or None).
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        module = getattr(type(current), "__module__", "") or ""
        for prefix in _UPSTREAM_MODULE_PREFIXES:
            if module == prefix or module.startswith(prefix + "."):
                status = getattr(current, "status_code", None)
                if not isinstance(status, int):
                    status = _parse_status_from_message(str(current))
                return {
                    "kind": "upstream",
                    "status_code": status if isinstance(status, int) else None,
                    "provider_module": prefix,
                }
        current = current.__cause__ or current.__context__

    return {"kind": "internal", "status_code": None, "provider_module": None}


class StreamEventAccumulator:
    """Accumulates and merges token-level SSE events for persistence."""

    def __init__(self, max_merged_bytes: int = MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT):
        self._max_merged_bytes = max_merged_bytes
        self._events: List[Dict[str, Any]] = []

    def get_events(self) -> List[Dict[str, Any]]:
        return copy.deepcopy(self._events)

    def add(self, event_type: str, data: Dict[str, Any]) -> None:
        if not isinstance(data, dict):
            return

        incoming = copy.deepcopy(data)

        if not self._events:
            self._events.append({"event": event_type, "data": incoming})
            return

        prev = self._events[-1]
        if prev.get("event") != event_type:
            self._events.append({"event": event_type, "data": incoming})
            return

        if event_type == "message_chunk" and self._try_merge_message_chunk(prev, incoming):
            return

        if event_type == "tool_call_chunks" and self._try_merge_tool_call_chunks(prev, incoming):
            return

        self._events.append({"event": event_type, "data": incoming})

    def _try_merge_message_chunk(self, prev_event: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
        prev_data = prev_event.get("data")
        if not isinstance(prev_data, dict):
            return False

        if incoming.get("content_type") == "reasoning_signal":
            return False
        if prev_data.get("content_type") == "reasoning_signal":
            return False

        merge_keys = ("thread_id", "agent", "id", "role", "content_type")
        if any(prev_data.get(k) != incoming.get(k) for k in merge_keys):
            return False

        prev_content = prev_data.get("content") or ""
        incoming_content = incoming.get("content") or ""
        incoming_finish = incoming.get("finish_reason")

        if incoming_content:
            if len(prev_content.encode("utf-8")) + len(incoming_content.encode("utf-8")) > self._max_merged_bytes:
                return False
            prev_data["content"] = f"{prev_content}{incoming_content}"

        if incoming_finish is not None:
            prev_data["finish_reason"] = incoming_finish

        return bool(incoming_content) or (incoming_finish is not None)

    def _try_merge_tool_call_chunks(self, prev_event: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
        prev_data = prev_event.get("data")
        if not isinstance(prev_data, dict):
            return False

        merge_keys = ("thread_id", "agent", "id")
        if any(prev_data.get(k) != incoming.get(k) for k in merge_keys):
            return False

        prev_chunks = prev_data.get("tool_call_chunks")
        incoming_chunks = incoming.get("tool_call_chunks")
        if not (isinstance(prev_chunks, list) and isinstance(incoming_chunks, list)):
            return False
        if len(prev_chunks) != 1 or len(incoming_chunks) != 1:
            return False

        prev_chunk = prev_chunks[0]
        incoming_chunk = incoming_chunks[0]
        if not (isinstance(prev_chunk, dict) and isinstance(incoming_chunk, dict)):
            return False

        prev_call_id = prev_chunk.get("id")
        incoming_call_id = incoming_chunk.get("id")
        if prev_call_id is not None or incoming_call_id is not None:
            if prev_call_id != incoming_call_id:
                return False
        else:
            if prev_chunk.get("index") != incoming_chunk.get("index"):
                return False

        prev_args = prev_chunk.get("args") or ""
        incoming_args = incoming_chunk.get("args") or ""
        if not isinstance(prev_args, str) or not isinstance(incoming_args, str):
            return False

        if incoming_args:
            if len(prev_args.encode("utf-8")) + len(incoming_args.encode("utf-8")) > self._max_merged_bytes:
                return False
            prev_chunk["args"] = f"{prev_args}{incoming_args}"

        return bool(incoming_args)


class WorkflowStreamHandler:
    """LangGraph workflow SSE producer with reasoning lifecycle tracking, tool-call dedup, and content normalisation."""

    def __init__(
        self,
        thread_id: str,
        token_callback: Optional[Any] = None,
        tool_tracker: Optional[Any] = None,
        workflow_timeout: Optional[int] = None,
        background_registry: Optional[Any] = None,
        merged_stream_chunk_max_bytes: int = MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT,
        agent_config: Optional[Any] = None,
    ):
        """Initialize the workflow stream handler.

        Keepalives live in the SSE consumer (``stream_from_log``), not here —
        the producer side is just a thin LangGraph pass-through.
        """
        self.thread_id = thread_id
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

        # Track reasoning block index per agent to detect block transitions
        # When index changes (e.g., 0→1), a separator (\n\n) is needed between blocks
        self._reasoning_block_index: dict[str, int] = {}
        self._reasoning_separator_pending: Set[str] = set()

        # Track function_call state for Response API (per agent)
        # Response API sends name/call_id only in first chunk, need to persist across chunks
        # Key: (agent_name, index), Value: {name, call_id, args_accumulated}
        self.function_call_state: dict = {}

        # Track tool_use state for Anthropic (per agent)
        # Anthropic sends name/id in initial tool_use, then streams args via input_json_delta
        # Key: (agent_name, index), Value: {name, id, args_accumulated}
        self.anthropic_tool_call_state: dict = {}

        # Event sequence numbering for reconnection support
        self.event_sequence: int = 0

        # Accumulate merged streaming chunks for persistence
        self._stream_event_accumulator = StreamEventAccumulator(
            max_merged_bytes=merged_stream_chunk_max_bytes
        )

        # Background task registry (single source of truth for SSE events)
        self._background_registry = background_registry

        # Track steering messages injected mid-workflow (for query backfill)
        self.injected_steerings: list[dict] = []
        self.on_steering_delivered: Optional[Any] = None

        # Snapshot of task IDs from previous workflow (set at stream start)
        self._old_tool_call_ids: set[str] = set()

        self.event_counter: Optional[Any] = None

        # Track message IDs that have already emitted content via AIMessageChunk streaming.
        # When a streaming model produces chunks, LangGraph also emits a final AIMessage
        # with the full content at node completion. This set prevents re-emitting that
        # duplicate content while still allowing non-streaming AIMessage content through.
        self._streamed_content_ids: Set[str] = set()

        # When True, skip all subagent-related emission (drain, status) — tail handles it
        self.skip_subagent_events: bool = False

        # Namespace tuples currently inside a "summarize" window. Opened on
        # the context_window summarize start signal, closed on complete OR
        # error. Keyed by the raw namespace (not the resolved agent display
        # name) so main-agent root events (namespace == ()) match consistently
        # between the custom and messages streams. Any messages-stream chunks
        # whose namespace is in this set are emitted as compaction_chunk
        # instead of message_chunk.
        self._compaction_windows: set[tuple] = set()

    async def stream_workflow(
        self,
        graph: Any,
        input_state: Any,
        config: dict,
    ) -> AsyncGenerator[str, None]:
        """Stream workflow execution events as SSE-formatted strings.

        Keepalives are emitted by the SSE consumer (``stream_from_log``) on
        XREAD BLOCK timeout, not by the producer — the workflow no longer
        needs to interleave them with graph output.
        """
        import time

        # Track start time for timeout
        workflow_start_time = time.time()
        timeout_warning_sent = False
        timeout_warning_threshold = 0.9  # Send warning at 90% of timeout

        # Set tool tracking ContextVar (like ExecutionTracker pattern)
        # This must be done BEFORE graph.astream() so nodes inherit the ContextVar
        if self.tool_tracker:
            from src.tools.decorators import _tool_usage_context
            _tool_usage_context.set(self.tool_tracker)
            logger.debug(f"[WorkflowStreamHandler] Tool usage tracking ContextVar set for thread_id={self.thread_id}")

        try:
            # Snapshot old task IDs and emit initial batch of captured events.
            # Events are streamed with accumulate=False so they are NOT persisted
            # with this (new) response — the collector owns persistence to the
            # OLD response where the subagent was created.
            # When skip_subagent_events is True, the concurrent tail handles all
            # subagent event delivery, so this block is skipped entirely.
            if self._background_registry and not self.skip_subagent_events:
                old_tasks = list(self._background_registry._tasks.values())
                self._old_tool_call_ids = {t.tool_call_id for t in old_tasks}

            # Create graph stream
            graph_stream = graph.astream(
                input_state,
                config=config,
                stream_mode=["messages", "updates", "custom"],
                subgraphs=True,
            )

            async for graph_event in graph_stream:
                # Unpack graph event data
                agent_from_stream, stream_mode, event_data = graph_event

                # Check for timeout (if configured)
                if self.workflow_timeout > 0:
                    elapsed_time = time.time() - workflow_start_time

                    # Send warning at 90% of timeout
                    if not timeout_warning_sent and elapsed_time >= (self.workflow_timeout * timeout_warning_threshold):
                        timeout_warning_sent = True
                        warning_event = self._format_sse_event(
                            "warning",
                            {
                                "thread_id": self.thread_id,
                                "message": f"Workflow approaching timeout ({int(elapsed_time)}s / {self.workflow_timeout}s)",
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
                        timeout_error = self._format_sse_event(
                            "error",
                            {
                                "thread_id": self.thread_id,
                                "error": f"Workflow timeout after {int(elapsed_time)} seconds",
                                "type": "timeout_error",
                                "elapsed_seconds": int(elapsed_time),
                                "timeout_seconds": self.workflow_timeout,
                            }
                        )
                        yield timeout_error
                        logger.error(
                            f"[TIMEOUT_ERROR] thread_id={self.thread_id} "
                            f"exceeded timeout of {self.workflow_timeout}s"
                        )
                        raise asyncio.TimeoutError(
                            f"Workflow exceeded timeout of {self.workflow_timeout} seconds"
                        )

                # Log raw stream data for debugging
                logger.debug(
                    f"[STREAM_RAW] agent={agent_from_stream} mode={stream_mode} "
                    f"event_type={type(event_data).__name__}"
                )

                # Handle interrupt events (can be in any stream mode)
                if isinstance(event_data, dict) and "__interrupt__" in event_data:
                    interrupt_event = self._handle_interrupt(event_data)
                    if interrupt_event:
                        yield interrupt_event
                    continue  # Skip further processing for interrupt events
                
                
                # Handle custom events (stream_mode="custom")
                # These are emitted by get_stream_writer() in middleware/nodes
                if stream_mode == "custom":
                    if isinstance(event_data, dict):
                        event_type = event_data.get("type")

                        # Handle subagent identity registration
                        # Emitted by SubagentEventCaptureMiddleware on first model call.
                        # The namespace_tuple from the streaming infrastructure tells us
                        # which LangGraph UUID corresponds to which background task.
                        if event_type == "subagent_identity":
                            tool_call_id = event_data.get("tool_call_id")
                            if tool_call_id and self._background_registry and agent_from_stream:
                                ns_str = "|".join(str(ns) for ns in agent_from_stream)
                                self._background_registry.register_namespace(ns_str, tool_call_id)
                                logger.debug(
                                    f"[SUBAGENT_IDENTITY] Registered namespace mapping: "
                                    f"{ns_str} -> tool_call_id={tool_call_id}"
                                )
                            continue

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
                                        "original_message_count",
                                        "truncated_count", "error",
                                        "kind", "offloaded_args", "offloaded_reads"):
                                if key in event_data:
                                    cw_data[key] = event_data[key]
                            if event_data.get("action") == "token_usage":
                                cw_data["threshold"] = self._resolve_token_threshold()

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
                                    self._compaction_windows.add(ns_key)
                                elif signal in ("complete", "error"):
                                    self._compaction_windows.discard(ns_key)

                            logger.debug(
                                f"[CONTEXT_WINDOW] Emitting {action}/{signal} "
                                f"(thread_id={self.thread_id})"
                            )
                            yield self._format_sse_event("context_window", cw_data)
                            continue

                        # Handle steering delivery signal
                        if event_type == "steering_delivered":
                            yield self._format_sse_event("steering_delivered", {
                                "thread_id": self.thread_id,
                                "count": event_data.get("count", 0),
                                "messages": event_data.get("messages", []),
                                "timestamp": event_data.get("timestamp"),
                            })
                            # Track injected messages for later query backfill
                            if self.on_steering_delivered:
                                try:
                                    await self.on_steering_delivered(
                                        event_data.get("messages", [])
                                    )
                                except Exception:
                                    pass
                            continue

                        # Check if this is an artifact event from middleware
                        # Generic handler: any event with artifact_type is emitted as artifact SSE
                        artifact_type = event_data.get("artifact_type")
                        if artifact_type:
                            extracted_agent_name = self._extract_agent_name(agent_from_stream, {})

                            # Use agent from event payload if present (set by middleware)
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
                            yield self._format_sse_event("artifact", artifact_event)
                    continue

                # Handle state updates (stream_mode="updates")
                if stream_mode == "updates":
                    if isinstance(event_data, dict):
                        # Updates are structured as: {node_name: {field: value, ...}}
                        # Look inside each node's update for pending_file_events (now artifact events)
                        for node_name, node_update in event_data.items():
                            if isinstance(node_update, dict) and "pending_file_events" in node_update:
                                file_events = node_update.get("pending_file_events", [])
                                if file_events:  # Only emit if there are actually events
                                    # Extract agent from stream metadata (same as messages stream)
                                    agent_name = self._extract_agent_name(agent_from_stream, {})

                                    logger.debug(
                                        f"[ARTIFACT] Emitting {len(file_events)} pending artifact events from {node_name} "
                                        f"(agent={agent_name})"
                                    )
                                    for event_payload in file_events:
                                        # Enrich event with agent if not already present
                                        if "agent" not in event_payload or not event_payload["agent"]:
                                            event_payload["agent"] = agent_name
                                        yield self._format_sse_event("artifact", event_payload)
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

                # Track message for persistence (if tracking is active)
                # Only track complete messages (AIMessage, ToolMessage), not chunks.
                # Compaction chunks are internal — don't persist them as turns.
                if isinstance(message_chunk, (AIMessage, ToolMessage)) and not is_compaction_chunk:
                    ExecutionTracker.update_context(
                        agent_name=agent_name,
                        messages=message_chunk
                    )

                # Process the message chunk
                async for event in self._process_message_chunk(
                    message_chunk,
                    agent_name,
                    message_metadata,
                    is_compaction=is_compaction_chunk,
                ):
                    yield event

            # After workflow completes, emit credit_usage event
            try:
                from src.server.services.persistence.usage import UsagePersistenceService

                # Get token tracking from callback (already stored in self.token_callback)
                per_call_records = None
                if self.token_callback:
                    per_call_records = self.token_callback.per_call_records

                # Get tool usage (non-destructive read, can be called multiple times)
                tool_usage = self.get_tool_usage()

                # Calculate credits if we have usage data
                if per_call_records or tool_usage:
                    # Calculate token usage for display
                    token_usage = {}
                    if per_call_records:
                        from src.utils.tracking import calculate_cost_from_per_call_records
                        token_usage = calculate_cost_from_per_call_records(per_call_records)

                    # Calculate total credits using same logic as persistence
                    credit_service = UsagePersistenceService(
                        thread_id=self.thread_id,
                        workspace_id="temp",  # Not needed for calculation
                        user_id="temp"
                    )

                    if per_call_records:
                        await credit_service.track_llm_usage(per_call_records)

                    if tool_usage:
                        credit_service.record_tool_usage_batch(tool_usage)

                    total_credits = credit_service.get_total_credits()

                    # Emit credit_usage event
                    yield self._format_credit_usage_event(
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
            # Don't yield error event - this is expected behavior
            raise
        except Exception as e:
            logger.exception(f"Error in stream generator for thread_id={self.thread_id}: {e}")
            yield self.format_error_event(str(e), exc=e)
            raise  # Re-raise so background_task_manager calls _mark_failed()

    def _handle_interrupt(self, event_data: dict) -> Optional[str]:
        """Format an ``__interrupt__`` event as an SSE string."""
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

        return self._format_sse_event(
            "interrupt",
            {
                "thread_id": self.thread_id,
                "interrupt_id": interrupt_obj.id,
                "action_requests": action_requests,
                "role": "assistant",
                "finish_reason": "interrupt",
            },
        )

    def _extract_agent_name(self, namespace_tuple: tuple, message_metadata: dict) -> str:
        """Return the agent identifier, resolving to unified subagent identity when possible.

        Priority:
        1. `namespace_tuple[-1]` resolved via registry to `agent_id` (e.g., "research:uuid4")
        2. `namespace_tuple[-1]` (verbatim, includes UUID)
        3. `checkpoint_ns` (verbatim)
        4. `langgraph_node`
        """
        if namespace_tuple:
            raw_name = str(namespace_tuple[-1])

            # Try to resolve to task:{task_id} format via registry
            if self._background_registry:
                task = self._background_registry.get_task_by_namespace(raw_name)
                if task:
                    return f"task:{task.task_id}"

            return raw_name

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
    ) -> AsyncGenerator[str, None]:
        """Process a single message chunk and yield SSE events.

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
                        yield self._format_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                        self.reasoning_active.discard(agent_name)
                    self._reasoning_block_index.pop(agent_name, None)
                    self._reasoning_separator_pending.discard(agent_name)
                else:
                    # Reasoning started - emit start signal
                    if agent_name not in self.reasoning_active:
                        yield self._format_reasoning_signal(agent_name, message_id, "start", is_compaction=is_compaction)
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
                            yield self._format_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                            self.reasoning_active.discard(agent_name)
                        self._reasoning_block_index.pop(agent_name, None)
                        self._reasoning_separator_pending.discard(agent_name)
                    else:
                        # Reasoning started - emit start signal
                        if agent_name not in self.reasoning_active:
                            yield self._format_reasoning_signal(agent_name, message_id, "start", is_compaction=is_compaction)
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

                        yield self._format_sse_event("tool_call_chunks", event_stream_message)
                        return  # Don't process function_call as regular content

                    # Claude (Anthropic): type=tool_use (initial metadata)
                    # Anthropic sends tool name/id in initial tool_use chunk, then streams args via input_json_delta
                    elif item.get('type') == 'tool_use':
                        index = item.get("index", 0)
                        state_key = (agent_name, index)

                        # Initialize state for this tool call
                        if state_key not in self.anthropic_tool_call_state:
                            self.anthropic_tool_call_state[state_key] = {
                                "name": item.get("name"),
                                "id": item.get("id"),
                                "args_accumulated": ""
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
                        state_key = (agent_name, index)
                        if state_key in self.anthropic_tool_call_state:
                            self.anthropic_tool_call_state[state_key]["args_accumulated"] += partial_json

                        tool_call_chunk = {
                            "args": partial_json,
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
                            f"[INPUT_JSON_DELTA_EXTRACTED] agent={agent_name} "
                            f"partial_json_length={len(partial_json)} accumulated={len(self.anthropic_tool_call_state.get(state_key, {}).get('args_accumulated', ''))}"
                        )

                        yield self._format_sse_event("tool_call_chunks", event_stream_message)
                        return  # Don't process input_json_delta as regular content

        # Detect reasoning summary_text index transitions before normalization
        # When the summary_text index changes (e.g., 0→1), a new reasoning thought started
        reasoning_idx = self._extract_reasoning_summary_index(message_chunk.content)
        if reasoning_idx is not None:
            prev_idx = self._reasoning_block_index.get(agent_name)
            self._reasoning_block_index[agent_name] = reasoning_idx
            if prev_idx is not None and reasoning_idx != prev_idx:
                self._reasoning_separator_pending.add(agent_name)

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
        is_inner_llm_chunk = is_tool_node and isinstance(message_chunk, (AIMessage, AIMessageChunk))
        if text_content and content_type and not is_inner_llm_chunk:
            # Check if we need to emit reasoning completion signal
            if content_type != "reasoning" and agent_name in self.reasoning_active:
                # Reasoning completed, emit completion signal before this content
                yield self._format_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
                self.reasoning_active.discard(agent_name)
                self._reasoning_block_index.pop(agent_name, None)
                self._reasoning_separator_pending.discard(agent_name)

            event_stream_message["content"] = text_content
            event_stream_message["content_type"] = content_type  # "text" or "reasoning"

            # Handle reasoning content lifecycle
            if content_type == "reasoning":
                # Emit start signal if this is the first reasoning content
                # This handles providers that send content directly without status signal
                if agent_name not in self.reasoning_active:
                    yield self._format_reasoning_signal(agent_name, message_id, "start", is_compaction=is_compaction)
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

            # If finishing while reasoning is active, emit completion signal
            if agent_name in self.reasoning_active:
                yield self._format_reasoning_signal(agent_name, message_id, "complete", is_compaction=is_compaction)
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

                    try:
                        # Parse accumulated JSON
                        parsed_args = json.loads(state["args_accumulated"])

                        # Build complete tool_calls event (unified format for all providers)
                        # Use provider-specific id field: "call_id" for Response API, "id" for Anthropic
                        tool_call_id = state.get("call_id") or state.get("id")
                        tool_calls = [{
                            "name": state["name"],
                            "args": parsed_args,  # Parsed object, not JSON string
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

                        logger.debug(
                            f"[TOOL_CALLS_COMPLETE] agent={agent_name} provider={provider_type} "
                            f"name={state['name']} args_length={len(state['args_accumulated'])} "
                            f"id={tool_call_id}"
                        )

                        yield self._format_sse_event("tool_calls", tool_calls_message)

                        # Clear state after emitting from the appropriate state dictionary
                        if provider_type == "response_api":
                            self.function_call_state.pop(state_key, None)
                        else:  # anthropic
                            self.anthropic_tool_call_state.pop(state_key, None)

                    except json.JSONDecodeError as e:
                        logger.error(
                            f"[TOOL_CALL_PARSE_ERROR] agent={agent_name} provider={provider_type} "
                            f"args={state['args_accumulated'][:200]} error={e}"
                        )
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
                yield self._format_sse_event("artifact", {
                    "artifact_type": "task",
                    "artifact_id": f"task:{task_artifact['task_id']}",
                    "agent": "main",
                    "thread_id": self.thread_id,
                    "status": "completed",
                    "payload": task_artifact,
                    "tool_call_id": message_chunk.tool_call_id,
                })

            yield self._format_sse_event("tool_call_result", event_stream_message)

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
                    yield self._format_sse_event("tool_calls", event_stream_message)
                    # Note: file_operation events are now emitted via custom events from middleware

            # Emit tool_call_chunks event for client consumption (if present)
            elif is_chunk and message_chunk.tool_call_chunks:
                event_stream_message["tool_call_chunks"] = message_chunk.tool_call_chunks
                yield self._format_sse_event("tool_call_chunks", event_stream_message)

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
                    yield self._format_sse_event(chunk_event_type, event_stream_message)

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

        During streaming, OpenAI Response API sends reasoning chunks where each
        summary_text item has an 'index' field (0, 1, 2...) identifying which
        reasoning "thought step" it belongs to. When this index changes (e.g., 0→1),
        a new reasoning thought has started and we need to emit a separator.

        Note: The top-level reasoning dict also has an 'index' field, but that's
        the position in the content array (always 0) — NOT the thought step index.
        """
        items = [content] if isinstance(content, dict) else (content if isinstance(content, list) else [])
        for item in items:
            if not isinstance(item, dict) or item.get("type") != "reasoning":
                continue
            # Extract index from summary_text items (the thought step index)
            summary = item.get("summary")
            if isinstance(summary, list):
                for s in summary:
                    if isinstance(s, dict) and "index" in s:
                        return s["index"]
        return None

    def _format_reasoning_signal(
        self,
        agent_name: str,
        message_id: str,
        signal_type: str,
        *,
        is_compaction: bool = False,
    ) -> str:
        """Format a reasoning lifecycle signal event."""
        event_type = "compaction_chunk" if is_compaction else "message_chunk"
        return self._format_sse_event(
            event_type,
            {
                "thread_id": self.thread_id,
                "agent": agent_name,
                "id": message_id,
                "role": "assistant",
                "content": signal_type,
                "content_type": "reasoning_signal",
            },
        )

    def _format_sse_event(self, event_type: str, data: dict[str, Any], *, accumulate: bool = True) -> str:
        """
        Format data as SSE (Server-Sent Events) string with sequence numbering.

        Args:
            event_type: Type of SSE event
            data: Event data dictionary
            accumulate: Whether to add to the stream event accumulator for persistence.
                        Set to False for old subagent events that belong to a previous
                        response and should not be persisted with the current one.

        Returns:
            SSE-formatted string (id: seq\\nevent: type\\ndata: json\\n\\n)
        """
        # Remove empty content to reduce payload size
        if data.get("content") == "":
            data.pop("content")

        # Accumulate merged events for persistence (never break streaming)
        if accumulate:
            try:
                self._stream_event_accumulator.add(event_type, data)
            except Exception as e:
                logger.debug(f"[WorkflowStreamHandler] Failed to accumulate stream event: {e}")

        # Increment sequence number for this event
        if self.event_counter is not None:
            self.event_sequence = self.event_counter.next()
        else:
            self.event_sequence += 1

        json_data = json.dumps(data, ensure_ascii=False)

        # Include sequence ID for reconnection support
        # Format: id: sequence_number\nevent: type\ndata: json\n\n
        result = f"id: {self.event_sequence}\nevent: {event_type}\ndata: {json_data}\n\n"

        # Log SSE events to dedicated logger if enabled
        if SSE_EVENT_LOG_ENABLED:
            sse_logger.info(f"{result}")

        return result

    def format_error_event(
        self,
        error_message: str,
        *,
        exc: Optional[BaseException] = None,
    ) -> str:
        """Format an error event as SSE string.

        When ``exc`` is passed the event carries ``error_kind`` (``upstream``
        or ``internal``), ``status_code`` (when available), and ``hints`` for
        the frontend to render user-actionable guidance. The legacy ``error``
        and ``message`` fields stay so older clients keep working.

        Args:
            error_message: Raw error text (usually ``str(exc)``).
            exc: The exception itself — enables classification. Optional to
                keep the legacy signature working for paths that only have
                a prebuilt message.

        Returns:
            SSE-formatted error event.
        """
        data: Dict[str, Any] = {
            "thread_id": self.thread_id,
            "error": _sanitize_error_text(error_message),
            "message": "An error occurred during processing",
        }
        if exc is not None:
            info = classify_stream_exception(exc)
            data["error_kind"] = info["kind"]
            if info["status_code"] is not None:
                data["status_code"] = info["status_code"]
            if info["provider_module"]:
                data["provider_module"] = info["provider_module"]
            if info["kind"] == "upstream":
                # Order matters — frontend renders the hints as a list, so the
                # most relevant hint for this status goes first. 5xx/429 are
                # provider outages, not the user's credentials; showing
                # "check your API key" first on a 503 is misleading.
                status = info.get("status_code")
                if status in (401, 403):
                    data["hints"] = [
                        "api_key",
                        "model_access",
                        "try_another_model",
                    ]
                elif status == 404:
                    data["hints"] = ["model_access", "try_another_model"]
                elif status == 429 or (isinstance(status, int) and status >= 500):
                    data["hints"] = ["provider_status", "try_another_model"]
                else:
                    # No status (network error) — could be anything; show all.
                    data["hints"] = [
                        "api_key",
                        "model_access",
                        "provider_status",
                        "try_another_model",
                    ]
        return self._format_sse_event("error", data)

    def _format_credit_usage_event(
        self,
        thread_id: str,
        token_usage: dict,
        total_credits: float
    ) -> str:
        """Format a credit_usage SSE event with aggregated token counts and total credits.

        Intentionally omits USD costs and model names (hidden from client for privacy).
        """
        from datetime import datetime

        # Aggregate token counts across all models (NO model names exposed)
        total_input_tokens = 0
        total_output_tokens = 0
        total_tokens = 0

        for model, usage in token_usage.get("by_model", {}).items():
            total_input_tokens += usage.get("input_tokens", 0)
            total_output_tokens += usage.get("output_tokens", 0)
            total_tokens += usage.get("total_tokens", 0)

        # Build credit event data (aggregated token counts + credits only, no model names or USD costs)
        event_data = {
            "thread_id": thread_id,
            "tokens": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "total_tokens": total_tokens
            },
            "total_credits": round(total_credits, 2),
            "timestamp": datetime.now().isoformat()
        }

        return self._format_sse_event("credit_usage", event_data)

    def get_sse_events(self) -> Optional[List[Dict[str, Any]]]:
        """Return merged SSE events for persistence."""
        events = self._stream_event_accumulator.get_events()
        return events or None

    _DEFAULT_TOKEN_THRESHOLD = 120000

    def _resolve_token_threshold(self) -> int:
        """Per-request config → base config → default. Drives the UI ring."""
        cfg = self.agent_config
        if cfg is None:
            from src.server.app import setup

            cfg = setup.agent_config
        if cfg is None:
            return self._DEFAULT_TOKEN_THRESHOLD
        return cfg.compaction.token_threshold

    def get_tool_usage(self) -> Optional[Dict[str, int]]:
        """Return tool-name → count usage map, or None. Result is cached for cross-context access."""
        # Return cached result if already retrieved (for cross-context access)
        if self._tool_usage_result is not None:
            logger.debug(
                f"[WorkflowStreamHandler] Returning cached tool usage for thread_id={self.thread_id}: "
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
                f"[WorkflowStreamHandler] Retrieved and cached tool usage for thread_id={self.thread_id}: "
                f"{len(tool_usage)} tool types, {sum(tool_usage.values())} total calls - {tool_usage}"
            )
        else:
            logger.debug(f"[WorkflowStreamHandler] No tool usage found for thread_id={self.thread_id}")

        return tool_usage
