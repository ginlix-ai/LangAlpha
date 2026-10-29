"""The live-price block a market_watch row carries, and the reminder frame.

The block used to ride the newest request for one call only. It is now
rendered once, when market_watch writes it into history as a row, with the
same header and closing guidance, so the carrier sends it on every later call
exactly as it was first sent.

One block, one budget. The hard cap drops it rather than trimming it, because a
half-rendered stamp is worse for the model than no stamp at all.
"""

from __future__ import annotations

import logging

from ptc_agent.agent.middleware.runtime_context.templates import render_template

logger = logging.getLogger(__name__)

ENVELOPE_OPEN = "<system-reminder>"
ENVELOPE_CLOSE = "</system-reminder>"

# Steady target is what the tail should cost on a normal call; the hard cap is
# what it may never exceed. Only the hard cap drops the block, because a tail a
# little over the target is worth more than a call with no runtime context.
ENVELOPE_STEADY_TOKENS = 1500
ENVELOPE_HARD_CAP_TOKENS = 2500


def count_tokens(text: str) -> int:
    """Token count for budget decisions; falls back to a chars/4 estimate.

    tiktoken is not the tokenizer of every provider we route to, so this is an
    approximation either way. It is used only to decide whether to drop the
    block, and the cheaper estimate is close enough for that.
    """
    if not text:
        return 0
    try:
        from ptc_agent.agent.middleware.compaction.utils import _get_tiktoken_encoder

        return len(_get_tiktoken_encoder().encode(text))
    except Exception:  # noqa: BLE001 - budgeting must never break a turn
        return len(text) // 4


def render_header(guidance: str) -> str:
    return render_template("envelope/header.md.j2", guidance=guidance)


def render_stamp(stamp: str, guidance: str) -> str:
    """The block a market_watch row carries for *stamp*, or "" when it is over the cap."""
    body = render_template("envelope/updates.md.j2", stamp=stamp, guidance=guidance)
    block = f"{render_header(guidance)}\n\n{body}"
    total = count_tokens(f"{ENVELOPE_OPEN}\n{block}\n{ENVELOPE_CLOSE}")
    if total > ENVELOPE_HARD_CAP_TOKENS:
        logger.warning(
            "[Envelope] stamp is %d tokens, over the %d cap; dropped it",
            total,
            ENVELOPE_HARD_CAP_TOKENS,
        )
        return ""
    if total > ENVELOPE_STEADY_TOKENS:
        logger.debug(
            "[Envelope] stamp is %d tokens, over the %d steady target",
            total,
            ENVELOPE_STEADY_TOKENS,
        )
    return block


def frame_reminder(text: str) -> str:
    """*text* inside the ``<system-reminder>`` wrapper.

    Framing belongs to the carrier, not the renderer: the wrapper is what marks
    harness text inside a user-role message, and an operator role carries that
    meaning itself, so the same text goes out bare on those channels. The budget
    still counts the wrapper because the reminder shape is the largest form the
    block can take.
    """
    return f"{ENVELOPE_OPEN}\n{text}\n{ENVELOPE_CLOSE}"
