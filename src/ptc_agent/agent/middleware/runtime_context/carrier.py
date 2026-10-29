"""How harness-authored text rides on this model: three shapes, one module.

A durable row is history, so it stays where it was written. It is text the
harness wrote rather than the user, and it takes one shape per model:

- ``system`` / ``developer``: a message on a role the user cannot forge.
  Consecutive rows are coalesced into one message with a text block each,
  because two adjacent operator entries have not been proven on the Anthropic
  wire form.
- ``reminder``: a ``<system-reminder>`` block appended inside the message it
  belongs to, which is the shape Claude Code uses: one message, the user's own
  text first, the harness blocks after it. The wrapper is added here, and
  otherwise only by the Anthropic bridge to a system entry the wire cannot
  place there (``src/llms/operator_channel.py``); a row hands over bare
  text, and an operator role carries the same meaning without it.

A tool result is never merged into. Harness text inside a tool result would sit
under the provenance label that says "untrusted", so after a tool batch the
text is a standalone message instead.

Composing the request is one pass through this module, and it hands back the
site the cache breakpoint may go on.

Everything here is request-only. The rewritten list is handed to
``request.override`` and never written back, so no shape decision reaches a
checkpoint and a row keeps its provider-neutral persisted form in state.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from ptc_agent.agent.middleware.provider_cache import tag_last_text_block
from ptc_agent.agent.middleware.runtime_context.durable import (
    RUNTIME_UPDATE_KEY,
    RUNTIME_UPDATE_SOURCE,
    is_runtime_update_message,
    update_text,
)
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from ptc_agent.agent.middleware.runtime_context.turn import MARKET_WATCH_ROW_KIND

logger = logging.getLogger(__name__)

CarrierShape = Literal["system", "developer", "reminder"]

_OPERATOR_SHAPES = ("system", "developer")


@dataclass(frozen=True, slots=True)
class PinSite:
    """Where a cache breakpoint goes: one message, and one block inside it.

    ``block`` is None when the message body is a plain string, which the marker
    turns into a single text block.
    """

    message: int
    block: int | None


@dataclass(frozen=True, slots=True)
class Carrier:
    """How this model takes harness text: the shape, and whether a per-call
    row goes out as a turn-scoped system message (see :func:`_carry`)."""

    shape: CarrierShape
    turn_scoped: bool = False


@dataclass(frozen=True, slots=True)
class ComposedRequest:
    """The message list this call sends, and the site breakpoint 4 may take."""

    messages: list[Any]
    pin: PinSite | None


def resolve_carrier(model: Any) -> Carrier:
    """Which of the three shapes this model's harness text takes, and whether
    it takes turn-scoped system messages.

    ``"reminder"`` answers for every model without a native operator channel,
    and also whenever resolution itself fails: a row is context, and a
    carrier fault is never worth failing a turn for.
    """
    try:
        from src.llms.operator_channel import (
            carries_turn_scoped,
            resolve_operator_channel,
        )
    except ImportError:
        return Carrier("reminder")
    try:
        channel = resolve_operator_channel(model)
    except Exception:  # noqa: BLE001 - a carrier fault must never break a turn
        logger.warning(
            "[Envelope] operator channel failed, using the reminder shape", exc_info=True
        )
        return Carrier("reminder")
    if channel not in _OPERATOR_SHAPES:
        return Carrier("reminder")
    return Carrier(channel, turn_scoped=channel == "system" and carries_turn_scoped(model))


@contextmanager
def operator_window(shape: CarrierShape) -> Iterator[None]:
    """The window in which the model call honors *shape* on the wire.

    The system shape needs the Anthropic client to keep a tagged message at
    its position, which it does only while the operator channel's ContextVar
    is raised; the caller wraps the one call it composed. Every other shape,
    and a build without the operator channel module, is a plain pass-through.
    """
    try:
        from src.llms.operator_channel import operator_request
    except ImportError:
        yield
        return
    with operator_request(shape):
        yield


def compose_request(messages: list[Any], carrier: Carrier) -> ComposedRequest:
    """The request this call sends: rows reshaped, pin site named.

    Neither the input list nor any input message is mutated: the rows stay
    provider-neutral in state and only the request copy takes a shape.
    """
    carried, stamp = _carry(messages, carrier)
    return ComposedRequest(messages=carried, pin=_pin_site(carried, stamp))


def apply_breakpoint(
    messages: list[Any], site: PinSite, key: str, marker: dict[str, Any]
) -> None:
    """Tag *site* with the wire marker, in place on the request-only list.

    The tagging itself is the provider-cache helper's; only the choice of
    block is made here, by handing it the content up to and including the
    site.
    """
    message = messages[site.message]
    content = message.content
    if site.block is None:
        tagged = tag_last_text_block(content, key, marker)
        rest: list[Any] = []
    else:
        tagged = tag_last_text_block(list(content[: site.block + 1]), key, marker)
        rest = list(content[site.block + 1 :])
    if tagged is None:
        return
    messages[site.message] = message.model_copy(update={"content": [*tagged, *rest]})


def _carry(
    messages: list[Any], carrier: Carrier
) -> tuple[list[Any], PinSite | None]:
    """A new message list with every persisted durable row rewritten into the
    carrier's shape, and where a per-call row the next call leaves out landed
    in the reminder shape, which the pin has to stay off.

    Rows are written at the turn boundary, so each one follows the user message
    of its turn. That ordering is what every shape needs: the operator shapes
    because a mid-conversation ``system`` entry has to follow a user message,
    the reminder shape because it merges into the message in front of it. A
    row written mid-turn, such as a market-watch stamp or the notes reminder,
    follows a tool batch instead: an operator entry follows the results, and
    the reminder shape, with no user message in front, stands alone rather
    than entering a tool result.

    A per-call row (a market-watch stamp) is true only until the next user
    message. Where the carrier is turn-scoped, every one is sent as a
    turn-scoped system message of its own, which the API clears from what the
    model reads once a later user message exists while it stays in the request
    byte for byte, so no thinking after it loses its prefix. Elsewhere, the
    stamps a user message has already followed are left out, which is what
    the API's clearing would have the model read.
    """
    shape, turn_scoped = carrier.shape, carrier.turn_scoped
    base = list(messages or [])
    if not any(is_runtime_update_message(m) for m in base):
        return base, None

    last_input = _last_input_index(base)
    carried: list[Any] = []
    stamp: PinSite | None = None
    for index, message in enumerate(base):
        if not is_runtime_update_message(message):
            carried.append(message)
            continue
        per_call = _is_per_call_row(message)
        if per_call and index < last_input:
            if not turn_scoped or _answered_by_nothing(base, index):
                continue
        text = update_text(message)
        if not text.strip():
            # A row with nothing to say is dropped rather than sent as an empty
            # message, which some providers reject outright.
            continue
        if per_call and turn_scoped and _append_turn_scoped(carried, text):
            continue
        if shape in _OPERATOR_SHAPES and _append_operator_text(
            carried, text, shape, RUNTIME_UPDATE_SOURCE
        ):
            continue
        block = frame_reminder(text)
        if carried and isinstance(carried[-1], HumanMessage):
            carried[-1] = _with_text_block(carried[-1], block)
            site = PinSite(len(carried) - 1, len(carried[-1].content) - 1)
        else:
            carried.append(message.model_copy(update={"content": block}))
            site = PinSite(len(carried) - 1, 0)
        if per_call and not turn_scoped and stamp is None:
            stamp = site
    return carried, stamp


def may_hold_breakpoint(message: Any) -> bool:
    """Whether a cache breakpoint on this message would be worth placing.

    An operator message the carrier built stays unpinned because a breakpoint
    on that role is unproven. A row carried into the reminder shape is history
    the carrier only re-shaped, so it holds the marker like any other message.
    """
    return not _is_operator_carrier(message)


# ---------------------------------------------------------------------------
# Shapes
# ---------------------------------------------------------------------------


#: Row kinds true for one model call only, superseded by the next copy.
_PER_CALL_KINDS = frozenset({MARKET_WATCH_ROW_KIND})

#: The wire value of a turn-scoped system message's ``clear_at``.
CLEAR_AT_NEXT_USER_MESSAGE = "next_user_message"


def spent_stamps(messages: list[Any]) -> set[int]:
    """The per-call rows of *messages* a later user message followed, by
    ``id()``. No model reads them: the API clears them from a turn-scoped
    request, and every other target is not sent them (see
    ``_carry``), so they cost nothing.
    """
    last_input = _last_input_index(messages)
    return {
        id(message)
        for index, message in enumerate(messages[: max(last_input, 0)])
        if is_runtime_update_message(message) and _is_per_call_row(message)
    }


def _is_per_call_row(message: Any) -> bool:
    meta = (message.additional_kwargs or {}).get(RUNTIME_UPDATE_KEY)
    return isinstance(meta, dict) and meta.get("kind") in _PER_CALL_KINDS


def _is_input(message: Any) -> bool:
    """A user-role message on the wire that is not a row: user words or tool results."""
    if isinstance(message, ToolMessage):
        return True
    return isinstance(message, HumanMessage) and not is_runtime_update_message(message)


def _last_input_index(messages: list[Any]) -> int:
    return max((i for i, m in enumerate(messages) if _is_input(m)), default=-1)


def _answered_by_nothing(messages: list[Any], index: int) -> bool:
    """Whether the row at *index* is followed by a user message with no answer between.

    A turn-scoped message the API sees directly before a user message is a
    400, not a cleared message. No answer came back to the request it ended,
    so no thinking was produced behind it, and leaving it out loses nothing.
    """
    for message in messages[index + 1 :]:
        if isinstance(message, AIMessage):
            return False
        if _is_input(message):
            return True
    return False


def _append_turn_scoped(messages: list[Any], text: str) -> bool:
    """Append *text* as a turn-scoped operator message of its own.

    Never coalesced with a neighbour: a durable row folded into it would be
    cleared along with it, and it folded into a row would render forever.
    Returns False when the message could not be built.
    """
    try:
        from src.llms.operator_channel import build_operator_message

        message = build_operator_message(text, "system", source=RUNTIME_UPDATE_SOURCE)
    except Exception:  # noqa: BLE001 - a carrier fault must never break a turn
        logger.warning("[Envelope] turn-scoped message failed, using the row shape", exc_info=True)
        return False
    message.additional_kwargs["clear_at"] = CLEAR_AT_NEXT_USER_MESSAGE
    messages.append(message)
    return True


def _pin_site(messages: list[Any], stamp: PinSite | None) -> PinSite | None:
    """The newest content the next call still carries, or None when there is none.

    Walking back from the tail the marker goes on the last text block, past
    the operator messages the carrier built, which stay unpinned because a
    breakpoint on that role is unproven, and past *stamp*, a per-call row in
    the reminder shape, and anything after it, which the next call leaves out.
    """
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if not may_hold_breakpoint(message):
            continue
        if stamp is not None and index > stamp.message:
            continue
        limit = stamp.block if stamp is not None and index == stamp.message else None
        block = _last_text_block(message.content, limit)
        if block is not None:
            return PinSite(index, None if block == _WHOLE_BODY else block)
    return None


#: Stand-in index for a plain-string body, which has no blocks to count.
_WHOLE_BODY = -1


def _last_text_block(content: Any, limit: int | None = None) -> int | None:
    """Index of the last block before *limit* that accepts a marker, or None
    when none does.

    A non-text tail (an image, a tool-use block) is walked past rather than
    tagged, because a provider may reject the marker on one.
    """
    if isinstance(content, str):
        return _WHOLE_BODY if content and limit is None else None
    if not isinstance(content, list):
        return None
    end = len(content) if limit is None else min(limit, len(content))
    for index in range(end - 1, -1, -1):
        block = content[index]
        if isinstance(block, str) and block:
            return index
        if isinstance(block, dict) and block.get("type") == "text":
            return index
    return None


def _append_operator_text(
    messages: list[Any], text: str, shape: CarrierShape, source: str
) -> bool:
    """Append *text* on the operator channel, coalescing with a trailing carrier.

    Returns False when the carrier message could not be built, leaving
    *messages* untouched so the caller can fall back to the reminder shape.
    Mutates the list it is given, which is always a copy the caller owns.
    """
    if messages and _is_operator_carrier(messages[-1]) and not _is_turn_scoped(messages[-1]):
        messages[-1] = _with_text_block(messages[-1], text)
        return True
    try:
        from src.llms.operator_channel import build_operator_message

        messages.append(build_operator_message(text, shape, source=source))
    except Exception:  # noqa: BLE001 - a carrier fault must never break a turn
        logger.warning(
            "[Envelope] operator message failed, using the reminder shape", exc_info=True
        )
        return False
    return True


def _is_operator_carrier(message: Any) -> bool:
    """True for an operator message this module built earlier in the same request.

    Only the carrier tags a ``SystemMessage`` with the row source, and the tag
    never survives the request, so this names exactly the messages more
    harness text may be folded into.
    """
    return (
        isinstance(message, SystemMessage)
        and (message.additional_kwargs or {}).get("lc_source") == RUNTIME_UPDATE_SOURCE
    )


def _is_turn_scoped(message: Any) -> bool:
    return (message.additional_kwargs or {}).get("clear_at") == CLEAR_AT_NEXT_USER_MESSAGE


def _with_text_block(message: Any, text: str) -> Any:
    """A copy of *message* with *text* appended as its own text block.

    A plain-string body is promoted to one text block first, so what the
    message already carried keeps a block of its own and never fuses with the
    harness text appended after it.
    """
    content = message.content
    if isinstance(content, list):
        blocks = list(content)
    elif content:
        blocks = [{"type": "text", "text": content}]
    else:
        blocks = []
    blocks.append({"type": "text", "text": text})
    return message.model_copy(update={"content": blocks})
