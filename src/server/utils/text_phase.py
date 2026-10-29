"""The OpenAI Responses ``phase`` on assistant text, agreed the same way live and on replay.

A text block may say whether it is ``commentary`` or the ``final_answer``. The
live stream and the history projector both join a message's text blocks into
one chunk, so both need the same rule for what phase that chunk carries: the
one phase every text block agrees on, where a block with no phase counts as
its own answer (None) rather than being ignored. One rule here, so a chunk can
never carry a phase live that it loses on replay.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def block_phase(block: Any) -> str | None:
    """The phase a text block states, or None when it states none."""
    phase = block.get("phase") if isinstance(block, dict) else None
    return phase if isinstance(phase, str) and phase else None


def agreed_phase(phases: Iterable[str | None]) -> str | None:
    """The one phase a set of text blocks agrees on, else None."""
    distinct = set(phases)
    return distinct.pop() if len(distinct) == 1 else None


class TextPhaseTracker:
    """Resolves the phase of streamed text blocks per agent and message.

    Streaming announces a phase once, in an empty text block that carries the
    block ``index``, then sends bare deltas that carry only the index. The
    tracker remembers the announcement so the deltas inherit it. Indices
    restart with every message, so the memory is scoped to one message id and
    dropped when a different one arrives or the message finishes.
    """

    def __init__(self) -> None:
        self._phases: dict[str, tuple[str, dict[int, str]]] = {}

    def resolve(self, agent: str, message_id: str, content: Any) -> str | None:
        """The agreed phase of the non-empty text blocks in *content*.

        Every text block teaches the tracker, an empty announcement included;
        only the blocks with text take part in the agreement, since an empty
        block adds nothing to the chunk the phase describes.
        """
        blocks = content if isinstance(content, list) else [content]
        remembered = self._memory(agent, message_id)
        phases: list[str | None] = []
        for block in blocks:
            if not isinstance(block, dict) or block.get("type") != "text":
                continue
            index = block.get("index")
            phase = block_phase(block)
            if phase is not None and isinstance(index, int):
                remembered[index] = phase
            elif phase is None and isinstance(index, int):
                phase = remembered.get(index)
            if block.get("text"):
                phases.append(phase)
        return agreed_phase(phases)

    def finish(self, agent: str) -> None:
        self._phases.pop(agent, None)

    def _memory(self, agent: str, message_id: str) -> dict[int, str]:
        held = self._phases.get(agent)
        if held is None or held[0] != message_id:
            held = (message_id, {})
            self._phases[agent] = held
        return held[1]
