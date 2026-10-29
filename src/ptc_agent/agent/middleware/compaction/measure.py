"""How large the context is, and how much of it a summary keeps, as the
model counts it.

A provider's own count is the measure wherever one exists: the trigger reads
the last call's usage, and each answer carries what its call's context grew
by, which sizes the tail. Only what no call measured is estimated, and the
estimate is the model's where it is known to part from tiktoken (Claude).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage, AnyMessage

from ptc_agent.agent.middleware.compaction.summarize import model_label
from ptc_agent.agent.middleware.compaction.types import TokenCounter
from ptc_agent.agent.middleware.compaction.utils import (
    attachment_tokens,
    count_tokens_claude,
    find_group_safe_cutoff,
)
from ptc_agent.agent.middleware.model_resilience import FALLBACK_KEY
from ptc_agent.agent.middleware.runtime_context.carrier import spent_stamps


@dataclass(frozen=True)
class Estimate:
    """What messages no call measured cost a model: their text by
    ``counter``, their attachments as the model is charged for them
    (``cost``, see ``attachment_tokens``). ``model`` names it
    (``model_label``), so a growth measured on another is not taken for its
    count (``context_growth``). A market-watch stamp a later user message
    followed counts nothing (``spent_stamps``)."""

    counter: TokenCounter
    cost: Callable[[Any], int] | None = None
    model: str | None = None

    def __call__(self, messages: Sequence[Any]) -> int:
        spent = spent_stamps(list(messages))
        read = [m for m in messages if id(m) not in spent] if spent else messages
        return self.counter(read) + attachment_tokens(read, self.cost)


def is_claude(model: Any) -> bool:
    """Whether ``model`` counts with Claude's tokenizer, on whichever route
    serves it: the tokenizer goes with the model, not the host."""
    return "claude" in (model_label(model) or "").lower()


def counter_for(model: Any, default: TokenCounter) -> TokenCounter:
    """What counts the text no call to ``model`` measured: Claude's estimate
    for Claude, which tiktoken misses by a third, else ``default``."""
    return count_tokens_claude if is_claude(model) else default


def estimate_for(
    model: Any, default: TokenCounter, cost: Callable[[Any], int] | None = None
) -> Estimate:
    """How what no call to ``model`` measured is counted (``counter_for``),
    its attachments at ``cost``."""
    return Estimate(counter_for(model, default), cost, model_label(model))


def keep_tokens_for(
    model: Any, keep_tokens: int, claude_keep_tokens: int, threshold: int
) -> int:
    """How much a summary for ``model`` keeps: more for Claude, whose tokens
    hold about 1.6 times less text, so the tail holds about the same work.

    Never more than a third of the ``threshold``. The agent's prompt and
    tools measure about 27k, so on a model with a small window, such as a
    64k one capped at 48k, a full tail beside them and the summary would
    fill the threshold again, and every call would summarize.
    """
    return min(claude_keep_tokens if is_claude(model) else keep_tokens, threshold // 3)


def measured_tokens(state: Mapping[str, Any]) -> int | None:
    """The context size the last model call reported, which the next call's
    summary trigger reads; None before any call has reported usage."""
    cached_input = state.get("_cached_input_tokens") or 0
    if cached_input <= 0:
        return None
    return cached_input + (state.get("_cached_output_tokens") or 0)


@dataclass(frozen=True)
class ContextReading:
    """The context as the summary trigger reads it, ``tokens``, and where
    the last call's measurement of it ends: the ``answer`` it ended on, its
    ``input_tokens``, and the ``estimate`` of what was ``added`` after it.
    The next call's growth is stamped from them (``stamp_context_growth``);
    ``answer`` is None where nothing was measured."""

    tokens: int
    estimate: Estimate
    answer: AIMessage | None = None
    input_tokens: int = 0
    added: int = 0


async def context_tokens(
    state: Mapping[str, Any],
    view: Sequence[AnyMessage],
    estimate: Estimate,
    system: Sequence[Any] = (),
) -> ContextReading:
    """The context ``view`` makes, as the summary trigger reads it: what the
    last call measured plus an estimate of what has been added since, else
    an estimate of the whole of it.

    The measurement ends at the newest answer, which its output tokens
    count, so everything after it is new: the results of that answer's tool
    calls, which are where a Read's attachments land. Reading the measurement
    alone missed exactly those, and the call that carried them in went out
    unsummarized however large they were.

    Read before every model call, so only the added part is counted, on the
    loop. A whole history, read after a summary between turns or a cut after
    a pause, is tens of milliseconds of tokenizing, and its text goes to a
    thread so the other turns on the loop do not wait on it; its attachments
    are counted on the loop, which their parse cache is kept on.
    """
    measured = measured_tokens(state)
    if measured is None:
        whole = [*system, *view]
        tokens = await asyncio.to_thread(estimate.counter, whole)
        return ContextReading(tokens + attachment_tokens(whole, estimate.cost), estimate)
    answered = next(
        (i for i in range(len(view) - 1, -1, -1) if isinstance(view[i], AIMessage)),
        None,
    )
    if answered is None:
        return ContextReading(measured, estimate)
    added = view[answered + 1 :]
    since = estimate(added) if added else 0
    return ContextReading(
        measured + since, estimate, view[answered], state["_cached_input_tokens"], since
    )


#: The ``response_metadata`` key holding what a call's context grew by since
#: the call before: the answer that call wrote and everything after it, as
#: the provider counted them and as estimated then, and the model counted
#: for. Two calls through one model with only that added between them
#: measure it exactly, which sizes the tail a summary keeps
#: (``message_sizes``).
CONTEXT_GROWTH_KEY = "context_growth"


def stamp_context_growth(message: AIMessage, reading: ContextReading, input_tokens: int) -> None:
    """Stamp on ``message`` what its call's input grew by from ``reading``,
    which its call read, when the model that wrote the answer the reading
    ended on wrote this one too, and it is the model the reading is for: a
    fallback's answers count its tokens, not the primary's. Counted after the
    call, so the trigger before it counts nothing more."""
    answer = reading.answer
    if answer is None:
        return
    written = message.response_metadata or {}
    before = answer.response_metadata or {}
    model = written.get("model_name")
    if not model or before.get("model_name") != model or FALLBACK_KEY in written or FALLBACK_KEY in before:
        return
    measured = input_tokens - reading.input_tokens
    estimated = reading.added + reading.estimate([answer])
    if measured > 0 and estimated > 0 and reading.estimate.model:
        message.response_metadata = {
            **(message.response_metadata or {}),
            CONTEXT_GROWTH_KEY: [measured, estimated, reading.estimate.model],
        }


def context_growth(message: Any, model: str | None) -> tuple[int, int] | None:
    """The (measured, estimated) growth stamped on ``message`` by a call to
    ``model``, or None: another model's tokenizer counts the same stretch
    differently, by up to 1.7 times."""
    value = (getattr(message, "response_metadata", None) or {}).get(CONTEXT_GROWTH_KEY)
    if (
        isinstance(value, (list, tuple))
        and len(value) == 3
        and model
        and value[2] == model
        and all(isinstance(v, int) and v > 0 for v in value[:2])
    ):
        return value[0], value[1]
    return None


def message_sizes(messages: Sequence[AnyMessage], estimate: Estimate) -> Iterator[int]:
    """What each of ``messages`` costs the model, newest first: as its
    provider measured it where it did, else by ``estimate``.

    Tokenizers part from tiktoken by the content: Claude counts English and
    code about 1.6 times higher, most others count CJK text about 0.6 times,
    Gemini and Qwen a price table 1.7 times. So no factor fits, and the
    measurement is used instead. An answer stamped with its context growth
    measures the stretch from the answer before it, shared out over that
    stretch by estimate; scaled by the estimate it was taken at, a stretch
    cut since, by a hidden argument or a removed attachment, shrinks at its
    own measured rate. The newest answer and what follows it are estimated:
    no later call has measured them, and the answer's output tokens count
    reasoning the next call may not be sent. So is a stretch measured on
    another model, until a call to this one measures it again.
    """

    spent = spent_stamps(list(messages))

    def one(message: AnyMessage) -> int:
        return 0 if id(message) in spent else estimate([message])

    answers = [i for i, m in enumerate(messages) if isinstance(m, AIMessage)]
    if not answers:
        yield from (one(m) for m in reversed(messages))
        return
    yield from (one(m) for m in reversed(messages[answers[-1] :]))
    for earlier, later in zip(reversed(answers[:-1]), reversed(answers[1:])):
        stretch = reversed(messages[earlier:later])
        growth = context_growth(messages[later], estimate.model)
        if growth is None:
            yield from (one(m) for m in stretch)
        else:
            measured, estimated = growth
            yield from (round(one(m) * measured / estimated) for m in stretch)
    yield from (one(m) for m in reversed(messages[: answers[0]]))


def keep_cutoff(messages: list[AnyMessage], *, tokens: int, estimate: Estimate) -> int:
    """Where a summary of ``messages`` ends, or 0 when it would summarize
    nothing: before the newest ``tokens`` of them, as the model counts them
    (``message_sizes``).

    Sized in tokens because one message can be a page of chat or a file read
    many times that, so a count leaves the room a summary makes to chance.
    The newest message always stays, and with it its whole tool-call group,
    however far past ``tokens`` that runs: the turn is working from it.
    """
    start, kept = len(messages), 0
    for size in message_sizes(messages, estimate):
        kept += size
        if kept > tokens:
            break
        start -= 1
    start = min(start, len(messages) - 1)
    return find_group_safe_cutoff(messages, start) if start > 0 else 0
