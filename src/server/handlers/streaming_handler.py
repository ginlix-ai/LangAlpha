"""
Workflow Streaming Handler

Handles LangGraph workflow streaming with SSE (Server-Sent Events) formatting.
Separates streaming business logic from HTTP endpoint concerns for better
testability and reusability.

Key responsibilities:
- Stream graph events (messages/updates/custom)
- Normalize content (text vs reasoning)
- Track reasoning lifecycle (start/complete signals)
- Deduplicate tool calls
- Format SSE events
- Handle timeouts gracefully
"""

import asyncio
import copy
import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, cast

from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage

from src.server.utils.content_normalizer import (
    normalize_text_content,
    is_thinking_status_signal,
)
from src.utils.tracking import ExecutionTracker

logger = logging.getLogger(__name__)

# Dedicated logger for SSE events (can be configured independently)
sse_logger = logging.getLogger("sse_events")

# Load configuration from config.yaml
from src.config.settings import (
    get_workflow_timeout,
    get_sse_keepalive_interval,
    is_sse_event_log_enabled,
    get_merged_chunk_max_bytes,
)

WORKFLOW_TIMEOUT = get_workflow_timeout()  # seconds
SSE_KEEPALIVE_INTERVAL = get_sse_keepalive_interval()  # seconds
SSE_EVENT_LOG_ENABLED = is_sse_event_log_enabled()

MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT = get_merged_chunk_max_bytes()


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


async def multiplex_streams(graph_stream: AsyncGenerator, keepalive_queue: asyncio.Queue):
    """
    Multiplex graph events and keepalive events into a single stream.

    This ensures keepalive events are sent even when graph.astream() is blocked
    during long-running operations (LLM calls, tool execution, etc).

    Args:
        graph_stream: AsyncGenerator from graph.astream()
        keepalive_queue: Queue containing keepalive events

    Yields:
        Tuple of (source, data) where:
        - source: "graph" or "keepalive"
        - data: Event data from that source
    """
    graph_task = None
    keepalive_task = None

    try:
        # Create initial tasks
        graph_iterator = graph_stream.__aiter__()
        graph_task = asyncio.ensure_future(graph_iterator.__anext__())
        keepalive_task = asyncio.ensure_future(keepalive_queue.get())

        while True:
            # Wait for whichever completes first
            done, pending = await asyncio.wait(
                {graph_task, keepalive_task},
                return_when=asyncio.FIRST_COMPLETED
            )

            # Process completed tasks
            for task in done:
                try:
                    if task == graph_task:
                        # Graph event received
                        graph_data = task.result()
                        yield ("graph", graph_data)
                        # Create new graph task for next event
                        graph_task = asyncio.ensure_future(graph_iterator.__anext__())

                    elif task == keepalive_task:
                        # Keepalive event received
                        keepalive_data = task.result()
                        yield ("keepalive", keepalive_data)
                        # Create new keepalive task
                        keepalive_task = asyncio.ensure_future(keepalive_queue.get())

                except StopAsyncIteration:
                    # Graph stream ended
                    logger.debug("[MULTIPLEX] Graph stream ended")
                    # Cancel keepalive task and exit
                    if keepalive_task and not keepalive_task.done():
                        keepalive_task.cancel()
                    return

                except Exception as e:
                    logger.error(f"[MULTIPLEX] Error processing task: {e}")
                    raise

    except asyncio.CancelledError:
        logger.debug("[MULTIPLEX] Multiplexer cancelled (client disconnected)")
        # Clean up pending tasks
        if graph_task and not graph_task.done():
            graph_task.cancel()
        if keepalive_task and not keepalive_task.done():
            keepalive_task.cancel()
        raise

    except Exception as e:
        logger.error(f"[MULTIPLEX] Fatal error in multiplexer: {e}")
        # Clean up
        if graph_task and not graph_task.done():
            graph_task.cancel()
        if keepalive_task and not keepalive_task.done():
            keepalive_task.cancel()
        raise


class WorkflowStreamHandler:
    """
    Handles LangGraph workflow streaming with SSE formatting.

    Manages streaming state including:
    - Reasoning lifecycle tracking per agent
    - Tool call deduplication
    - Content normalization (text vs reasoning)
    - SSE event formatting
    """

    def __init__(
        self,
        thread_id: str,
        token_callback: Optional[Any] = None,
        tool_tracker: Optional[Any] = None,
        keepalive_interval: Optional[float] = None,
        workflow_timeout: Optional[int] = None,
        background_registry: Optional[Any] = None,
        merged_stream_chunk_max_bytes: int = MERGED_STREAM_CHUNK_MAX_BYTES_DEFAULT,
    ):
        """
        Initialize the workflow stream handler.

        Args:
            thread_id: Thread identifier for this streaming session
            token_callback: Token tracking callback instance (PerCallTokenTracker)
            tool_tracker: Tool usage tracker instance (ToolUsageTracker) for infrastructure cost tracking
            keepalive_interval: Seconds between keepalive events (default from env)
            workflow_timeout: Maximum workflow execution time in seconds (default from env)
            background_registry: BackgroundTaskRegistry instance for background task status (optional)
            merged_stream_chunk_max_bytes: Max bytes per merged stored stream chunk
        """
        self.thread_id = thread_id
        self.token_callback = token_callback
        self.tool_tracker = tool_tracker
        self.keepalive_interval = keepalive_interval or SSE_KEEPALIVE_INTERVAL
        self.workflow_timeout = workflow_timeout or WORKFLOW_TIMEOUT

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

        # Keepalive task management
        self._keepalive_task: Optional[asyncio.Task] = None
        self._keepalive_stop_event: asyncio.Event = asyncio.Event()
        self._last_event_time: float = 0.0

        # Background task registry (single source of truth for SSE events)
        self._background_registry = background_registry

        # Track steering messages injected mid-workflow (for query backfill)
        self.injected_steerings: list[dict] = []
        self.on_steering_delivered: Optional[Any] = None

        # Snapshot of task IDs from previous workflow (set at stream start)
        self._old_tool_call_ids: set[str] = set()

        # Shared event sequence counter (set by BackgroundTaskManager for concurrent tail)
        self.event_counter: Optional[Any] = None

        # Track message IDs that have already emitted content via AIMessageChunk streaming.
        # When a streaming model produces chunks, LangGraph also emits a final AIMessage
        # with the full content at node completion. This set prevents re-emitting that
        # duplicate content while still allowing non-streaming AIMessage content through.
        self._streamed_content_ids: Set[str] = set()

        # When True, skip all subagent-related emission (drain, status) — tail handles it
        self.skip_subagent_events: bool = False

    async def _keepalive_loop(self, keepalive_queue: asyncio.Queue):
        """
        Background task that sends keepalive events to prevent connection timeouts.

        Args:
            keepalive_queue: Queue to send keepalive events to
        """
        import time

        try:
            while not self._keepalive_stop_event.is_set():
                # Wait for keepalive_interval or until stop event
                try:
                    await asyncio.wait_for(
                        self._keepalive_stop_event.wait(),
                        timeout=self.keepalive_interval
                    )
                    # If we get here, stop event was set
                    break
                except asyncio.TimeoutError:
                    # Timeout means we should send keepalive
                    pass

                # Check if enough time has passed since last event
                current_time = time.time()
                time_since_last_event = current_time - self._last_event_time

                # Only send keepalive if we haven't sent any events recently
                if time_since_last_event >= self.keepalive_interval:
                    keepalive_event = self._format_keepalive_event()
                    await keepalive_queue.put(keepalive_event)
                    logger.debug(f"[KEEPALIVE] Sent for thread_id={self.thread_id}")


        except asyncio.CancelledError:
            logger.debug(f"[KEEPALIVE] Task cancelled for thread_id={self.thread_id}")
            raise
        except Exception as e:
            logger.warning(f"[KEEPALIVE] Error in keepalive loop: {e}")

    def _format_keepalive_event(self) -> str:
        """
        Format a keepalive SSE event to prevent connection timeouts.

        Returns:
            SSE-formatted keepalive event string with sequence ID
        """
        # Increment sequence for keepalive too (for proper event ordering)
        if self.event_counter is not None:
            self.event_sequence = self.event_counter.next()
        else:
            self.event_sequence += 1
        return f"id: {self.event_sequence}\nevent: keepalive\ndata: {{\"status\": \"alive\"}}\n\n"

    async def stream_workflow(
        self,
        graph: Any,
        input_state: Any,
        config: dict,
    ) -> AsyncGenerator[str, None]:
        """
        Stream workflow execution events as SSE-formatted strings with timeout handling.

        Args:
            graph: LangGraph graph instance
            input_state: Initial state or Command for the workflow
            config: LangGraph config with thread_id, callbacks, etc.

        Yields:
            SSE-formatted event strings (event: type\\ndata: json\\n\\n)

        Raises:
            asyncio.TimeoutError: If workflow exceeds configured timeout
        """
        import time

        # Initialize keepalive queue and start background task
        keepalive_queue: asyncio.Queue = asyncio.Queue()
        self._keepalive_stop_event.clear()
        self._last_event_time = time.time()
        self._keepalive_task = asyncio.create_task(self._keepalive_loop(keepalive_queue))

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

            # Multiplex graph stream with keepalive queue
            async for source, data in multiplex_streams(graph_stream, keepalive_queue):
                # Update last event time
                self._last_event_time = time.time()

                # Handle keepalive events
                if source == "keepalive":
                    # Directly yield keepalive event (already formatted)
                    yield data
                    logger.debug(f"[KEEPALIVE] Yielded for thread_id={self.thread_id}")

                    continue

                # Unpack graph event data
                agent_from_stream, stream_mode, event_data = data

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
                        # Emitted by ToolCallCounterMiddleware on first model call.
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
                            cw_data = {
                                "thread_id": self.thread_id,
                                "agent": self._extract_agent_name(agent_from_stream, event_data),
                            }
                            # Forward all relevant fields from middleware payload
                            for key in ("action", "signal", "input_tokens", "output_tokens",
                                        "total_tokens", "summary_length", "original_message_count",
                                        "truncated_count", "error",
                                        "kind", "offloaded_args", "offloaded_reads"):
                                if key in event_data:
                                    cw_data[key] = event_data[key]
                            # Inject threshold for token_usage action
                            if event_data.get("action") == "token_usage":
                                from src.server.app import setup
                                cw_data["threshold"] = (
                                    setup.agent_config.summarization.token_threshold
                                    if setup.agent_config
                                    else 120000
                                )

                            action = event_data.get("action", "")
                            signal = event_data.get("signal", "")
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
                # Only track complete messages (AIMessage, ToolMessage), not chunks
                if isinstance(message_chunk, (AIMessage, ToolMessage)):
                    ExecutionTracker.update_context(
                        agent_name=agent_name,
                        messages=message_chunk
                    )

                # Process the message chunk
                async for event in self._process_message_chunk(
                    message_chunk,
                    agent_name,
                ):
                    # Update last event time for each yielded event
                    self._last_event_time = time.time()
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
            yield self.format_error_event(str(e))
            raise  # Re-raise so background_task_manager calls _mark_failed()
        finally:
            # Stop keepalive task
            logger.debug(f"[KEEPALIVE] Stopping keepalive task for thread_id={self.thread_id}")
            self._keepalive_stop_event.set()
            if self._keepalive_task and not self._keepalive_task.done():
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"[KEEPALIVE] Error stopping keepalive task: {e}")

    def _handle_interrupt(self, event_data: dict) -> Optional[str]:
        """
        Handle interrupt events from the workflow.

        Args:
            event_data: Event dictionary containing __interrupt__ key

        Returns:
            SSE-formatted interrupt event or None
        """
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
    ) -> AsyncGenerator[str, None]:
        """Process a single message chunk and yield SSE events."""
        message_id = message_chunk.id or "unknown"

        # Check for thinking/reasoning status signals in main content
        status_info = is_thinking_status_signal(message_chunk.content)
        if status_info:
            if status_info.get("status") == "completed":
                # Reasoning completed - emit completion signal
                if agent_name in self.reasoning_active:
                    yield self._format_reasoning_signal(agent_name, message_id, "complete")
                    self.reasoning_active.discard(agent_name)
                self._reasoning_block_index.pop(agent_name, None)
                self._reasoning_separator_pending.discard(agent_name)
            else:
                # Reasoning started - emit start signal
                if agent_name not in self.reasoning_active:
                    yield self._format_reasoning_signal(agent_name, message_id, "start")
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
                if reasoning_status.get("status") == "completed":
                    # Reasoning completed - emit completion signal if agent was actively streaming
                    if agent_name in self.reasoning_active:
                        yield self._format_reasoning_signal(agent_name, message_id, "complete")
                        self.reasoning_active.discard(agent_name)
                    self._reasoning_block_index.pop(agent_name, None)
                    self._reasoning_separator_pending.discard(agent_name)
                else:
                    # Reasoning started - emit start signal
                    if agent_name not in self.reasoning_active:
                        yield self._format_reasoning_signal(agent_name, message_id, "start")
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

        # Add text content if present (can be regular text or reasoning)
        if text_content and content_type:
            # Check if we need to emit reasoning completion signal
            if content_type != "reasoning" and agent_name in self.reasoning_active:
                # Reasoning completed, emit completion signal before this content
                yield self._format_reasoning_signal(agent_name, message_id, "complete")
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
                    yield self._format_reasoning_signal(agent_name, message_id, "start")
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
                yield self._format_reasoning_signal(agent_name, message_id, "complete")
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
                    yield self._format_sse_event("message_chunk", event_stream_message)

    def _filter_tool_calls(self, tool_calls: list) -> list:
        """
        Filter tool calls to remove empty names and duplicates.

        Args:
            tool_calls: List of tool call dictionaries

        Returns:
            Filtered list of valid tool calls
        """
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
    ) -> str:
        """Format a reasoning lifecycle signal event."""
        return self._format_sse_event(
            "message_chunk",
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

    def format_error_event(self, error_message: str) -> str:
        """
        Format an error event as SSE string.

        Args:
            error_message: Error message to send

        Returns:
            SSE-formatted error event
        """
        return self._format_sse_event(
            "error",
            {
                "thread_id": self.thread_id,
                "error": error_message,
                "message": "An error occurred during processing",
            }
        )

    def _format_credit_usage_event(
        self,
        thread_id: str,
        token_usage: dict,
        total_credits: float
    ) -> str:
        """
        Format credit usage event with aggregated token counts and total credits only.

        IMPORTANT: Does NOT include USD costs or model names (hidden from client for privacy).
        Only exposes:
        - Aggregated token counts (input/output tokens across all models)
        - Total credits consumed

        Args:
            thread_id: Thread identifier
            token_usage: Token usage dict from calculate_cost_from_per_call_records()
            total_credits: Total credits (token + infrastructure)

        Returns:
            SSE-formatted credit_usage event
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

    def get_tool_usage(self) -> Optional[Dict[str, int]]:
        """
        Get captured tool usage from workflow execution (with caching for cross-context access).

        This method can be called multiple times safely. It caches the result on first
        successful read to ensure availability across async context boundaries.

        Returns:
            Dict mapping tool names to usage counts, or None if no tools were used
        """
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
