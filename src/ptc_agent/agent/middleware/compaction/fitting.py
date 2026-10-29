"""Fitting a model call under the summary threshold by taking attachments out
of what it keeps, and on Claude choosing a summary's end so they go into the
summary instead (see ``Fit``).
"""

from __future__ import annotations

import logging
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, NamedTuple

from langchain_core.messages import AIMessage, AnyMessage

from langchain.agents.middleware.types import ModelRequest, ModelResponse

from src.llms.error_classification import is_context_overflow
from src.llms.token_counter import extract_token_usage

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.measure import Estimate
from ptc_agent.agent.middleware.compaction.offloading import removed_attachments
from ptc_agent.agent.middleware.thinking_binding import (
    checked_thinking_end,
    thinking_after,
    unsent_thinking,
    without_thinking,
)
from ptc_agent.agent.middleware.compaction.utils import (
    attachment_tokens,
    pdf_pages,
    provider_cutoff,
)
from ptc_agent.agent.middleware.file_operations.multimodal_strip import remove_attachments

logger = logging.getLogger(__name__)


def reported_usage(response: ModelResponse) -> tuple[AIMessage, int, int] | None:
    """The newest AI message in ``response`` that reports its input tokens,
    with its (input, output) tokens; None when none does."""
    for msg in reversed(response.result or ()):
        if not isinstance(msg, AIMessage):
            continue
        usage = extract_token_usage(msg)
        input_tokens = usage.get("input_tokens", 0)
        if input_tokens > 0:
            return msg, input_tokens, usage.get("output_tokens", 0)
    return None


class Fitted(NamedTuple):
    """A fitted send: its response, the ids of the messages that lose their
    attachments and of the answers whose thinking goes with them (see
    ``thinking_after``), and what the provider counted for the ones this send
    still carried (see ``Fit._measured``)."""

    response: ModelResponse
    removed: set[str]
    unsent: set[str]
    spared: int

    def update(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """The update recording ``removed`` and ``unsent`` beside what
        ``state`` holds."""
        update: dict[str, Any] = {}
        if self.removed:
            update["_removed_attachment_ids"] = removed_attachments(state) | self.removed
        if self.unsent:
            update["_unsent_thinking_ids"] = unsent_thinking(state) | self.unsent
        return update


@dataclass(frozen=True)
class Fit:
    """How a call's messages are fitted under ``threshold``, counted by
    ``estimate`` with ``system`` before them.

    ``lossless`` is for Claude through its own compaction, whose kept turns
    keep their thinking valid only while they are sent as they were: an
    attachment that no longer fits goes into the summary (see ``cut``) rather
    than out of the turns it keeps, and only those after the last checked
    thinking are taken out.
    """

    threshold: int
    system: list[Any]
    estimate: Estimate
    lossless: bool = False

    def cut(self, view: list[AnyMessage], cutoff: int, tokens: int, *, refused: bool) -> int:
        """Where a summary of ``view`` ends on Claude: at the latest point at
        or before ``cutoff`` its compaction can cut at (``provider_cutoff``),
        or at the first later one whose kept turns fit (``_fits``), so an
        attachment that would have to come out of them goes into the summary
        instead. 0 when nothing need be summarized; ``cutoff`` when there is
        no point to cut at.

        An attachment counts at its estimate times ``tokens``, what the
        provider measured, over the estimate of the view (see ``_measured``),
        and after a refusal at more than any room left. The summary is not
        counted, since it is not written yet: one that leaves the view over
        the threshold has the next call summarize again, past the old summary.
        """
        ratio = (
            math.inf
            if refused
            else max(1.0, tokens / max(self.estimate([*self.system, *view]), 1))
        )
        if cutoff <= 0 and self._fits(view, ratio):
            return cutoff
        start = max(provider_cutoff(view, cutoff) if cutoff > 0 else 0, 2)
        cuts = [
            i
            for i in range(start, len(view))
            if isinstance(view[i], AIMessage) and provider_cutoff(view, i) == i
        ]
        fitting = (i for i in cuts if self._fits(view[i:], ratio))
        return next(fitting, cuts[-1] if cuts else cutoff)

    def _fits(self, messages: list[AnyMessage], ratio: float) -> bool:
        """Whether ``messages`` need no attachment taken out ahead of the
        last checked thinking: they hold none there, or they fit under the
        threshold once those after it are, counted at ``ratio`` times their
        estimate (see ``send``)."""
        safe = checked_thinking_end(messages)
        if not attachment_tokens(messages[:safe], self.estimate.cost):
            return True
        kept = self.estimate([*self.system, *messages]) - attachment_tokens(
            messages[safe:], self.estimate.cost
        )
        return ratio * kept < self.threshold

    async def send(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        messages: list[AnyMessage],
        *,
        refusal: Exception | None = None,
    ) -> Fitted:
        """Send ``messages`` with as many of their attachments taken out as
        it takes to fit.

        Below the threshold is what fits, by estimate: the oldest go first,
        since the newest are what the agent is working on. Left over it, the
        next call would summarize again at once. A refusal means the estimate
        fell short, so after one every attachment goes. ``refusal`` is one
        ``messages`` already met as they are, which is not sent again.
        Attachments the measurement takes out go from the next call, so the
        thinking of the answer to this one goes from then too.

        ``lossless`` takes out only the attachments after the last checked
        thinking, and none by measurement: the next call summarizes past the
        rest instead. Only a refusal of what that leaves takes the others,
        and the thinking after them, since nothing else can be sent.
        """
        start = checked_thinking_end(messages) if self.lossless else 0
        removed: set[str] = set()
        if refusal is None:
            removed = self._to_remove(messages, everything=False, start=start)
            unsent = thinking_after(messages, removed)
            try:
                response = await handler(
                    request.override(messages=_without(messages, removed, unsent))
                )
            except Exception as exc:
                if not is_context_overflow(exc):
                    raise
                refusal = exc
            else:
                if self.lossless:
                    return Fitted(response, removed, unsent, 0)
                more, spared = self._measured(response, messages, removed)
                if more:
                    answered = [*messages, *ensure_message_ids(list(response.result or ()))]
                    unsent |= thinking_after(answered, more)
                return Fitted(response, removed | more, unsent, spared)
        everything = self._to_remove(messages, everything=True, start=start)
        if everything == removed:
            everything = self._to_remove(messages, everything=True)
        if everything == removed:
            raise refusal
        logger.warning("[Compaction] Context overflow, removing every attachment kept")
        unsent = thinking_after(messages, everything)
        response = await handler(
            request.override(messages=_without(messages, everything, unsent))
        )
        return Fitted(response, everything, unsent, 0)

    def _measured(
        self, response: ModelResponse, messages: list[AnyMessage], removed: set[str]
    ) -> tuple[set[str], int]:
        """The ids of the messages whose attachments go too after a send
        fitted by estimate that the provider measured over the threshold, and
        the tokens they took.

        The estimate can fall short of the provider's count, which includes
        the tool schemas and a PDF's real pages. Left over the threshold, the
        next call would summarize again and remove nothing, and so would each
        one after. The measurement corrects the estimate: each attachment
        counts at its estimate times measured over estimated for the view
        that was sent, the oldest going first until the excess is covered.
        """
        usage = reported_usage(response)
        if usage is None:
            return set(), 0
        _, input_tokens, output_tokens = usage
        over = input_tokens + output_tokens - self.threshold
        if over < 0:
            return set(), 0
        estimated = self.estimate([*self.system, *_without(messages, removed)])
        if estimated <= 0:
            return set(), 0
        more: set[str] = set()
        spared = 0
        for msg in messages:
            if over < 0:
                break
            if not msg.id or msg.id in removed:
                continue
            taken = round(attachment_tokens([msg], self.estimate.cost) * input_tokens / estimated)
            if taken:
                more.add(msg.id)
                spared += taken
                over -= taken
        return more, spared

    def _to_remove(
        self, messages: list[AnyMessage], *, everything: bool, start: int = 0
    ) -> set[str]:
        """The ids of the messages in ``messages`` from ``start`` on whose
        attachments go.

        Only attachments the target is sent count: one it cannot read reaches
        it as the strip's placeholder, whose guidance a removal would replace.
        """
        costs = [
            (msg.id, attachment_tokens([msg], self.estimate.cost)) for msg in messages[start:]
        ]
        costs = [(msg_id, tokens) for msg_id, tokens in costs if tokens and msg_id]
        if everything:
            return {msg_id for msg_id, _ in costs}
        over = self.estimate([*self.system, *messages]) - self.threshold
        removed: set[str] = set()
        for msg_id, tokens in costs:
            if over < 0:
                break
            removed.add(msg_id)
            over -= tokens
        return removed


def _without(
    messages: list[AnyMessage], removed: set[str], unsent: set[str] = frozenset()
) -> list[AnyMessage]:
    return without_thinking(remove_attachments(messages, removed, pages=pdf_pages), unsent)
