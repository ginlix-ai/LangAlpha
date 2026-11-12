"""What a call sends ahead of its messages, by the part of the context each
piece is, for the composer's context panel.

Counted only on request (``between_turns.Parts``, ``ClaudeTurn.ahead_parts``),
never in a turn: either way it costs a count of every tool schema, which most
turns nobody looks at. The parts are grouped once (``_held``) and counted by
the provider where it counts a request itself, else estimated.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, NamedTuple

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

from ptc_agent.agent.middleware.compaction.measure import tool_schemas
from ptc_agent.agent.middleware.compaction.types import TokenCounter
from ptc_agent.agent.middleware.runtime_context.baseline import section_part
from src.llms.provider_compaction import token_counting_client

logger = logging.getLogger(__name__)

#: The parts of what a call sends ahead of its messages, in the order it is
#: sent. The rest of a context is its conversation.
AHEAD_PARTS = ("system", "tools", "mcp", "skills", "memory", "environment")

#: How long a provider's count of the parts may take, and each request of
#: it, before the estimate stands in: the panel that asked is open and waiting.
PROVIDER_COUNT_TIMEOUT_S = 10.0

#: The threads a provider's count requests run on, the client's count being
#: a blocking call. Their own, so a slow provider holds up other counts and
#: not the turns building their agents on the loop's shared threads. One
#: read's requests fit at once.
_COUNT_THREADS = ThreadPoolExecutor(max_workers=len(AHEAD_PARTS) + 1, thread_name_prefix="context-count")

#: The message each part is counted beside, since the count endpoint takes no
#: request without one. Its own count is taken off each part.
_BESIDE = HumanMessage(".")


class _Held(NamedTuple):
    part: str
    messages: list[BaseMessage]
    tools: Sequence[Any] = ()


async def ahead_parts(
    system_message: SystemMessage | None,
    tools: Sequence[Any] | None,
    sections: Mapping[str, str],
    *,
    model: Any,
    counter: TokenCounter,
) -> dict[str, int]:
    """What a call sends ahead of its messages, in tokens by part: the
    system prompt, the ``tools``, and the baseline's ``sections`` under the
    part each belongs to (``section_part``).

    Counted by ``model``'s provider where it counts a request itself
    (``token_counting_client``), else an estimate by ``counter``, which also
    stands in for a count that fails or runs late.
    """
    held = _held(system_message, tools, sections)
    client = token_counting_client(model, timeout=PROVIDER_COUNT_TIMEOUT_S)
    if client is not None:
        try:
            async with asyncio.timeout(PROVIDER_COUNT_TIMEOUT_S):
                return await _counted_parts(client, held)
        except Exception:  # noqa: BLE001 - the estimate stands in
            logger.warning("[Compaction] the provider's count of the context parts failed; estimating", exc_info=True)
    # Off the loop: every tool schema is converted and tokenized.
    return await asyncio.to_thread(_estimated_parts, held, counter)


def _held(
    system_message: SystemMessage | None, tools: Sequence[Any] | None, sections: Mapping[str, str]
) -> list[_Held]:
    """Each part that holds anything, in ``AHEAD_PARTS`` order, as what it
    holds. A part's sections are sent as one block, so they are joined here."""
    texts: dict[str, list[str]] = {}
    for key, text in sections.items():
        texts.setdefault(section_part(key), []).append(text)
    held: dict[str, _Held] = {
        part: _Held(part, [SystemMessage("\n\n".join(joined))]) for part, joined in texts.items()
    }
    if system_message is not None:
        held["system"] = _Held("system", [system_message])
    if tools:
        held["tools"] = _Held("tools", [], tools)
    return [held[part] for part in AHEAD_PARTS if part in held]


def _estimated_parts(held: list[_Held], counter: TokenCounter) -> dict[str, int]:
    """Each part by ``counter``, its tools as one message of their schemas
    (``tool_schemas``)."""

    def messages(h: _Held) -> list[BaseMessage]:
        schemas = tool_schemas(h.tools)
        return h.messages if schemas is None else [*h.messages, schemas]

    return {h.part: counter(messages(h)) for h in held}


async def _counted_parts(model: Any, held: list[_Held]) -> dict[str, int]:
    """Each part as ``model``'s provider counts it: one request a part, in
    the shape a call sends it, less what the request costs with nothing in
    it. The tools' count so includes the instructions the provider adds for
    them, which a call with tools pays for too."""

    loop = asyncio.get_running_loop()

    def count(messages: list[BaseMessage], tools: Sequence[Any] | None = None) -> Any:
        return loop.run_in_executor(_COUNT_THREADS, model.get_num_tokens_from_messages, messages, tools)

    empty, *counts = await asyncio.gather(
        count([_BESIDE]), *(count([*h.messages, _BESIDE], h.tools or None) for h in held)
    )
    return {h.part: max(n - empty, 0) for h, n in zip(held, counts, strict=True)}
