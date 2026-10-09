"""Per-token stream forwarding from a subagent's astream into its task streams."""

import time
from typing import Any

import structlog
from langchain_core.messages import BaseMessage

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.outcomes import Outcome
from src.llms.content_utils import extract_reasoning_summary_index
from src.server.utils.content_normalizer import normalize_text_content

logger = structlog.get_logger(__name__)


# Custom-mode events forwarded from a subagent's astream as-is, keyed on
# ``type``. ``provenance`` is forwarded so a subagent's data-access records
# (web/file/MCP sources) reach the main turn; ``forward_custom`` stamps them
# with the ``task:{task_id}`` agent_id for correct subagent attribution.
_ALLOWED_CUSTOM_EVENT_TYPES = frozenset({"context_window", "provenance"})

# Artifacts forwarded from a subagent's astream, keyed on ``artifact_type``,
# with the payload keys each keeps. A file_operation is a path-only ping: the
# client only needs which path changed to refresh its file panel and
# memory/memo views, and the written content would bloat the per-task buffer
# and the archive.
_FORWARDED_ARTIFACT_PAYLOAD_KEYS = {
    "file_operation": ("operation", "file_path", "sandbox_path", "error"),
}


class _SubagentTokenForwarder:
    """Forward per-token ``messages``-mode chunks from subagent.astream into
    captured-event records on the registry.

    Mirrors the main streaming handler's reasoning lifecycle: a ``start``
    reasoning_signal fires on the first reasoning chunk, ``complete`` fires
    on transition to text content or message_id change. Without this, the
    frontend's reasoning UI never opens (it gates on the start signal).

    Tool-call/tool-call-result events still come from
    ``SubagentEventCaptureMiddleware.awrap_*_call`` — those are post-call
    discrete signals, not stream-able token deltas.
    """

    def __init__(
        self,
        registry: BackgroundTaskRegistry,
        tool_call_id: str,
        agent_id: str,
    ) -> None:
        self.registry = registry
        self.tool_call_id = tool_call_id
        self.agent_id = agent_id
        self._reasoning_active = False
        self._last_msg_id: str | None = None
        # Track the OpenAI reasoning summary_text index to separate sections.
        # When it changes (0→1) a new reasoning section starts; we prepend a
        # blank line so its `**Title**` header doesn't glue onto the previous
        # section's prose. Mirrors RunSSEProducer's main-agent path.
        self._reasoning_block_index: int | None = None
        self._reasoning_separator_pending = False
        # Whitespace-only reasoning deltas seen before the reasoning section
        # opened, held until visible reasoning follows.
        self._reasoning_whitespace_held = ""

    def _signal_record(self, msg_id: str, content: str) -> dict[str, Any]:
        return {
            "event": "message_chunk",
            "data": {
                "agent": self.agent_id,
                "id": msg_id,
                "role": "assistant",
                "content": content,
                "content_type": "reasoning_signal",
            },
            "ts": time.time(),
        }

    def _chunk_record(
        self,
        msg_id: str,
        text: str,
        content_type: str,
        finish_reason: str | None = None,
    ) -> dict[str, Any]:
        return {
            "event": "message_chunk",
            "data": {
                "agent": self.agent_id,
                "id": msg_id,
                "role": "assistant",
                "content": text,
                "content_type": content_type,
                "finish_reason": finish_reason,
            },
            "ts": time.time(),
        }

    def _error_record(self, message: str, error_type: str) -> dict[str, Any]:
        return {
            "event": "error",
            "data": {
                "agent": self.agent_id,
                "message": message,
                "error_type": error_type,
            },
            "ts": time.time(),
        }

    async def forward(
        self,
        message_chunk: BaseMessage,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        # Drop tool-node inner LLM chunks (e.g. WebFetch's extraction model):
        # the tool's user-facing output arrives via the tool_call_result event
        # written separately by ``SubagentEventCaptureMiddleware`` (see
        # ``event_capture.py``). Forwarding the inner model's AI chunks would
        # leak the extraction prompt's CoT to the per-task channel.
        #
        # No isinstance(AIMessageChunk) discriminant here: this forwarder's
        # input universe is narrower than ``sse_producer``'s. The caller
        # at ``_arun_subagent_streaming`` only invokes ``forward`` for chunks
        # streamed via ``stream_mode=["messages"]`` from inside the
        # subagent's own subgraph — which carries inner-LLM AI chunks but
        # NOT the ToolMessage returns (those land on the separate
        # event-capture path). So an unconditional drop on
        # ``langgraph_node == "tools"`` is correct here, where in
        # ``sse_producer`` it would clobber ToolMessage content.
        if metadata is not None and metadata.get("langgraph_node") == "tools":
            return

        msg_id = message_chunk.id or f"sg-{self.tool_call_id}"

        # A new assistant message begins a fresh reasoning stream — drop any
        # carried-over section index / pending separator / held whitespace so
        # the first chunk of the new message isn't falsely prefixed.
        if self._last_msg_id is not None and msg_id != self._last_msg_id:
            self._reasoning_block_index = None
            self._reasoning_separator_pending = False
            self._reasoning_whitespace_held = ""

        # Detect reasoning summary_text index transitions (mirror of the main
        # streaming handler): when the OpenAI summary index changes (0→1) a new
        # reasoning section started — queue a separator so its `**Title**` header
        # lands on its own line instead of gluing onto the previous section's
        # prose. The flag is pending because the index can arrive before the
        # chunk that carries the new section's first emittable text.
        reasoning_idx = extract_reasoning_summary_index(message_chunk.content)
        if reasoning_idx is not None:
            if (
                self._reasoning_block_index is not None
                and reasoning_idx != self._reasoning_block_index
            ):
                self._reasoning_separator_pending = True
            self._reasoning_block_index = reasoning_idx

        # Reasoning content can ride on either ``content`` or
        # ``additional_kwargs.reasoning[_content]`` depending on provider.
        text, content_type = normalize_text_content(message_chunk.content)
        reasoning_kw = (
            message_chunk.additional_kwargs.get("reasoning_content")
            or message_chunk.additional_kwargs.get("reasoning")
        )
        if reasoning_kw and not text:
            r_text, _ = normalize_text_content(reasoning_kw)
            if r_text:
                text = r_text
                content_type = "reasoning"

        # New message id with reasoning still active → close out the old one.
        if (
            self._last_msg_id is not None
            and msg_id != self._last_msg_id
            and self._reasoning_active
        ):
            await self.registry.append_captured_event(
                self.tool_call_id, self._signal_record(self._last_msg_id, "complete")
            )
            self._reasoning_active = False

        # Whitespace alone never opens a reasoning section (the client would
        # render an empty thought), but it is still text: hold it until visible
        # reasoning follows, and drop it if the reply turns to text instead.
        if text:
            held, self._reasoning_whitespace_held = self._reasoning_whitespace_held, ""
            if content_type == "reasoning":
                text = held + text
                if not self._reasoning_active and not text.strip():
                    self._reasoning_whitespace_held = text
                    text = None

        if text and content_type:
            # Inline reasoning lifecycle — start on first reasoning chunk,
            # complete on transition to text.
            if content_type == "reasoning" and not self._reasoning_active:
                await self.registry.append_captured_event(
                    self.tool_call_id, self._signal_record(msg_id, "start")
                )
                self._reasoning_active = True
            elif content_type == "text" and self._reasoning_active:
                await self.registry.append_captured_event(
                    self.tool_call_id, self._signal_record(msg_id, "complete")
                )
                self._reasoning_active = False
                # Reasoning ended — reset section tracking for the next stream.
                self._reasoning_block_index = None
                self._reasoning_separator_pending = False

            # Prepend the blank-line separator queued by a section transition.
            if content_type == "reasoning" and self._reasoning_separator_pending:
                text = "\n\n" + text
                self._reasoning_separator_pending = False

            await self.registry.append_captured_event(
                self.tool_call_id,
                self._chunk_record(msg_id, text, content_type),
            )

        self._last_msg_id = msg_id

    async def forward_custom(self, data: Any) -> None:
        """Forward a whitelisted ``custom``-mode event to the per-task buffer.

        Only the shapes ``_ALLOWED_CUSTOM_EVENT_TYPES`` and
        ``_FORWARDED_ARTIFACT_PAYLOAD_KEYS`` name pass, each stamped with the
        stable ``task:{task_id}`` agent_id so the per-task consumer and
        frontend can route it. Everything else (todo_operations, show_widget
        and the like) drops: it would bloat the per-task buffer and Redis
        stream, and a payload with ``type: "message_chunk"`` would spoof a real
        subagent SSE event on the frontend.
        """
        if not isinstance(data, dict):
            return
        kind = data.get("artifact_type")
        if isinstance(kind, str) and kind in _FORWARDED_ARTIFACT_PAYLOAD_KEYS:
            payload = data.get("payload")
            if not isinstance(payload, dict):
                payload = {}
            event = "artifact"
            body = {
                "artifact_type": kind,
                "artifact_id": data.get("artifact_id"),
                "agent": self.agent_id,
                "timestamp": data.get("timestamp"),
                "status": data.get("status"),
                "payload": {
                    k: payload[k]
                    for k in _FORWARDED_ARTIFACT_PAYLOAD_KEYS[kind]
                    if k in payload
                },
            }
        elif (event := data.get("type")) in _ALLOWED_CUSTOM_EVENT_TYPES:
            body = {k: v for k, v in data.items() if k != "type"}
            body["agent"] = self.agent_id
        else:
            return
        await self.registry.append_captured_event(
            self.tool_call_id, {"event": event, "data": body, "ts": time.time()}
        )

    async def forward_error(self, exc: BaseException) -> None:
        """Spill an ``error`` SSE record so per-task SSE consumers can
        distinguish a crashed subagent from a clean completion.

        Without this, both success and failure terminate the per-task stream
        with only the ``subagent_stream_end`` sentinel — leaving downstream
        trackers (the channel gateway's Slack/Discord/Feishu task tracker, the
        web frontend, ptc-cli) unable to surface failure to the user.

        Best-effort: failures here are absorbed so they cannot mask the
        original exception, which is always re-raised by the caller.
        """
        try:
            await self.registry.append_captured_event(
                self.tool_call_id,
                self._error_record(
                    str(exc) or repr(exc), Outcome.from_exception(exc).error_type
                ),
            )
        except Exception:
            logger.warning(
                "subagent_error_event_write_failed",
                tool_call_id=self.tool_call_id,
                exc_info=True,
            )

    async def finalize(self) -> None:
        """Close any still-open reasoning lifecycle at astream-loop exit.

        The stream-end sentinel is NOT written here: content spills XADD with
        explicit ``{seq}-0`` ids and Redis rejects ids behind the sentinel's
        auto-generated (timestamp) id, so anything appended after the sentinel
        is silently lost. The run wrapper's terminal sequence
        (``_run_background_task``'s finally) writes it after the terminal meta
        and the unconsumed-steering sweep, keeping it the stream's last record.
        """
        if self._reasoning_active and self._last_msg_id is not None:
            await self.registry.append_captured_event(
                self.tool_call_id,
                self._signal_record(self._last_msg_id, "complete"),
            )
            self._reasoning_active = False

