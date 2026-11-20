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

import asyncio
from collections.abc import AsyncIterator
from functools import lru_cache
from typing import Any, Protocol

from anthropic._models import construct_type
from anthropic.types.beta import BetaRawMessageStreamEvent
from langchain_anthropic import ChatAnthropic
from langchain_anthropic.chat_models import _format_messages
from langchain_core.language_models.chat_models import BaseChatModel, agenerate_from_stream
from langchain_core.messages import BaseMessage, ToolMessage
from langchain_core.outputs import ChatGenerationChunk, ChatResult
from pydantic import PrivateAttr

from src.utils.stream_coalescing import COALESCE_MAX_CHARS, COALESCE_WINDOW_S

#: How far apart the deltas of a block go on, at least, while they keep
#: coming. Half the SSE coalescer's window: the text can be held twice, here
#: and there, so this bounds what joining adds to how late it shows.
JOIN_WINDOW_S = COALESCE_WINDOW_S / 2

#: The deltas one CLI event can carry several of, by the field each grows.
_JOINABLE = {"text_delta": "text", "thinking_delta": "thinking", "input_json_delta": "partial_json"}


def joinable(event: Any) -> bool:
    return (
        isinstance(event, dict)
        and event.get("type") == "content_block_delta"
        and (event.get("delta") or {}).get("type") in _JOINABLE
    )


def joined(event: dict[str, Any], more: Any) -> dict[str, Any] | None:
    """``event`` grown by ``more`` where ``more`` continues it, the next
    delta of the same block and kind; None where it does not, or the two
    would pass ``COALESCE_MAX_CHARS``, past which the client shows a frame
    whole instead of typing it out. Each joined delta is one chunk fewer
    through the callbacks and the ``messages`` stream, which cost far more
    per chunk than the text does; the message they add up to is the same."""
    if not joinable(more) or more.get("index") != event.get("index"):
        return None
    delta, next_delta = event["delta"], more["delta"]
    if next_delta["type"] != delta["type"]:
        return None
    field = _JOINABLE[delta["type"]]
    head, tail = delta.get(field), next_delta.get(field)
    if not isinstance(head, str) or not isinstance(tail, str) or len(head) + len(tail) > COALESCE_MAX_CHARS:
        return None
    return {**event, "delta": {**delta, field: head + tail}}


class EventSource(Protocol):
    """Where a message's raw events are read from (``_Run``), with what was
    read past what it was read for held for the next read."""

    async def next_event(self, *, within: float | None = None) -> Any: ...

    def hold(self, item: Any) -> None: ...


class DeltaJoin:
    """Joins the token deltas a message's events carry, so a reply costs a
    chunk per batch through the callbacks rather than per delta. The first
    delta of a block goes on at once, and so does any that comes
    ``JOIN_WINDOW_S`` or more after the last one went on, so a slow stream is
    never held; one that comes sooner waits out that window for the ones
    continuing it. What does not continue it is held for the next read and
    ends the wait, so no other event is delayed. The wait is what lets
    deltas join at all: the event loop moves every stage between the CLI
    and here one event per turn, so the CLI's backlog never gathers in the
    run's queue on its own."""

    def __init__(self, source: EventSource) -> None:
        self._source = source
        #: The block and kind of the delta given last, while deltas run on.
        self._open: tuple[Any, str] | None = None
        #: When the delta given last went on, in loop time.
        self._sent = 0.0

    async def grow(self, item: Any) -> Any:
        if not joinable(item):
            self._open = None
            return item
        loop = asyncio.get_running_loop()
        block = (item.get("index"), item["delta"]["type"])
        if block != self._open:
            self._open = block
            self._sent = loop.time()
            return item
        # Past the deadline this still takes what is already queued.
        deadline = self._sent + JOIN_WINDOW_S
        while (more := await self._source.next_event(within=deadline - loop.time())) is not None:
            grown = joined(item, more)
            if grown is None:
                self._source.hold(more)
                break
            item = grown
        self._sent = loop.time()
        return item


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
        written = None
        async for raw in self._events:
            # The converter reads the usage the message ends on, which does
            # not split the cache it wrote by its life; the usage it starts on does.
            if raw.get("type") == "message_start":
                written = ((raw.get("message") or {}).get("usage") or {}).get("cache_creation")
            elif raw.get("type") == "message_delta" and written and (usage := raw.get("usage")) and "cache_creation" not in usage:
                raw = {**raw, "usage": {**usage, "cache_creation": written}}
            event = construct_type(type_=BetaRawMessageStreamEvent, value=raw)
            msg, block_start = conv._make_message_chunk_from_anthropic_event(
                event, stream_usage=True, coerce_content_to_string=False, block_start_event=block_start
            )
            if msg is not None:
                yield ChatGenerationChunk(message=msg)

    async def _agenerate(self, messages: list[BaseMessage], stop: list[str] | None = None, run_manager: Any = None, **kwargs: Any) -> ChatResult:
        return await agenerate_from_stream(self._astream(messages, stop, run_manager, **kwargs))
