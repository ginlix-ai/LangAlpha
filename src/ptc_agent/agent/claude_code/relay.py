"""Claude Code's token stream, surfaced as a chat model's stream.

The SSE producer frames a turn from LangGraph's ``messages`` stream, which
carries what a chat model inside a node streams. The relay is that chat
model for a Claude Code step: it reads the raw Anthropic events the CLI
forwards (``include_partial_messages``) and converts each with
langchain-anthropic's own converter, so the chunks, and the ``AIMessage``
they add up to, have exactly the shape a ``ChatAnthropic`` call gives the
producer, the checkpoint and the replay projector today. It calls no API.

Its ``metadata`` is the client stamp the token tracker reads
(``billing_attribution``), which the runtime takes as given (``RuntimeSpec.billing``).
The converter is langchain-anthropic's private
``_make_message_chunk_from_anthropic_event``; ``test_runtime_cli``, in the default
unit suite, is what fails on a release that changes it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache
from typing import Any

from anthropic._models import construct_type
from anthropic.types.beta import BetaRawMessageStreamEvent
from langchain_anthropic import ChatAnthropic
from langchain_anthropic.chat_models import _format_messages
from langchain_core.language_models.chat_models import BaseChatModel, agenerate_from_stream
from langchain_core.messages import BaseMessage, ToolMessage
from langchain_core.outputs import ChatGenerationChunk, ChatResult
from pydantic import PrivateAttr



def anthropic_content(message: BaseMessage) -> str | list[dict[str, Any]]:
    """A message's content as a ``ChatAnthropic`` call sends it, by langchain-anthropic's own formatter.

    Image attachments (``image_url`` data URLs) become image blocks and PDFs
    (``file`` blocks) document blocks. A tool message gives its
    ``tool_result``'s content.
    """
    content = _format_messages([message], model=None)[1][-1]["content"]
    if isinstance(message, ToolMessage):
        return content[0]["content"]
    return content


@lru_cache(maxsize=1)
def _converter() -> ChatAnthropic:
    """Only the event converter is used; ``model_construct`` builds no client and holds no key."""
    return ChatAnthropic.model_construct()


class ClaudeStreamRelay(BaseChatModel):
    """One assistant message of a Claude Code turn, streamed as a chat model call."""

    model_name: str
    _events: AsyncIterator[dict[str, Any]] | None = PrivateAttr(default=None)

    @property
    def _llm_type(self) -> str:
        return "claude-code-relay"

    def feed(self, events: AsyncIterator[dict[str, Any]]) -> ClaudeStreamRelay:
        self._events = events
        return self

    def _generate(self, *args: Any, **kwargs: Any) -> ChatResult:  # pragma: no cover
        raise NotImplementedError("the relay only streams")

    async def _astream(
        self, messages: list[BaseMessage], stop: list[str] | None = None, run_manager: Any = None, **kwargs: Any
    ) -> AsyncIterator[ChatGenerationChunk]:
        if self._events is None:
            raise RuntimeError("feed() the step's events first")
        conv = _converter()
        block_start = None
        async for raw in self._events:
            event = construct_type(type_=BetaRawMessageStreamEvent, value=raw)
            msg, block_start = conv._make_message_chunk_from_anthropic_event(
                event, stream_usage=True, coerce_content_to_string=False, block_start_event=block_start
            )
            if msg is not None:
                yield ChatGenerationChunk(message=msg)

    async def _agenerate(self, messages: list[BaseMessage], stop: list[str] | None = None, run_manager: Any = None, **kwargs: Any) -> ChatResult:
        return await agenerate_from_stream(self._astream(messages, stop, run_manager, **kwargs))
