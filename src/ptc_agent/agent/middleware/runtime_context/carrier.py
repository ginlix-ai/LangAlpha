"""How harness-authored text rides on this model: three shapes, one module.

Two writers need a shape. The envelope is the newest context, so nothing
follows it. A durable row is history, so it stays where it was written. Both
are text the harness wrote rather than the user, and both take the same shape
per model:

- ``system`` / ``developer``: a message on a role the user cannot forge.
  Consecutive parts (several rows, or rows followed by the envelope) are
  coalesced into one message with a text block each, because two adjacent
  operator entries have not been proven on the Anthropic wire form.
- ``reminder``: a ``<system-reminder>`` block appended inside the message it
  belongs to, which is the shape Claude Code uses: one message, the user's own
  text first, the harness blocks after it. The wrapper is added here and only
  here; both writers hand over bare text, and an operator role carries the
  same meaning without it.

A tool result is never merged into. Harness text inside a tool result would sit
under the provenance label that says "untrusted", so after a tool batch the
text is a standalone message instead.

Composing the request is one pass through this module, and it hands back the
site the cache breakpoint may go on. The pin was once predicted before the
placement and reconstructed after it; here the site is known because this is
what placed the envelope.

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

from langchain_core.messages import HumanMessage, SystemMessage

from ptc_agent.agent.middleware.provider_cache import tag_last_text_block
from ptc_agent.agent.middleware.runtime_context.durable import (
    RUNTIME_UPDATE_SOURCE,
    is_runtime_update_message,
    update_text,
)
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder

logger = logging.getLogger(__name__)

#: ``lc_source`` tag on the envelope's own carrier message. The row's tag is
#: :data:`RUNTIME_UPDATE_SOURCE`, re-exported here so callers reach both through
#: the module that decides what each one means on the wire.
RUNTIME_CONTEXT_SOURCE = "runtime_context"

_CARRIER_SOURCES = frozenset({RUNTIME_CONTEXT_SOURCE, RUNTIME_UPDATE_SOURCE})

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
class ComposedRequest:
    """The message list this call sends, and the site breakpoint 4 may take."""

    messages: list[Any]
    pin: PinSite | None


def resolve_carrier_shape(model: Any) -> CarrierShape:
    """Which of the three shapes this model's harness text takes.

    ``"reminder"`` answers for every model without a native operator channel,
    and also whenever resolution itself fails: the envelope is context, and a
    carrier fault is never worth failing a turn for.
    """
    try:
        from src.llms.operator_channel import resolve_operator_channel
    except ImportError:
        return "reminder"
    try:
        channel = resolve_operator_channel(model)
    except Exception:  # noqa: BLE001 - a carrier fault must never break a turn
        logger.warning(
            "[Envelope] operator channel failed, using the reminder shape", exc_info=True
        )
        return "reminder"
    return channel if channel in _OPERATOR_SHAPES else "reminder"


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


def compose_request(
    messages: list[Any], envelope_text: str, shape: CarrierShape
) -> ComposedRequest:
    """The request this call sends: rows reshaped, envelope last, pin site named.

    Neither the input list nor any input message is mutated: the rows stay
    provider-neutral in state and only the request copy takes a shape.
    """
    carried = carry_durable_updates(messages, shape)
    placed, envelope_block = _place_envelope(carried, envelope_text, shape)
    return ComposedRequest(messages=placed, pin=_pin_site(placed, envelope_block))


def apply_breakpoint(
    messages: list[Any], site: PinSite, key: str, marker: dict[str, Any]
) -> None:
    """Tag *site* with the wire marker, in place on the request-only list.

    The tagging itself is the provider-cache helper's; only the choice of
    block is made here, by handing it the content up to and including the
    site so the envelope's own block, if any, stays untagged.
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


def carry_durable_updates(messages: list[Any], shape: CarrierShape) -> list[Any]:
    """A new message list with every persisted durable row rewritten into *shape*.

    Rows are written at the turn boundary, so each one follows the user message
    of its turn. That ordering is what every shape needs: the operator shapes
    because a mid-conversation ``system`` entry has to follow a user message,
    the reminder shape because it merges into the message in front of it. A
    row written mid-turn, such as the notes reminder or check-in, follows a
    tool batch instead, where the per-call envelope already lands on every call
    after one: an operator entry follows the results as the envelope's does,
    and the reminder shape, with no user message in front, stands alone rather
    than entering a tool result.
    """
    base = list(messages or [])
    if not any(is_runtime_update_message(m) for m in base):
        return base

    carried: list[Any] = []
    for message in base:
        if not is_runtime_update_message(message):
            carried.append(message)
            continue
        text = update_text(message)
        if not text.strip():
            # A row with nothing to say is dropped rather than sent as an empty
            # message, which some providers reject outright.
            continue
        if shape in _OPERATOR_SHAPES and _append_operator_text(
            carried, text, shape, RUNTIME_UPDATE_SOURCE
        ):
            continue
        block = frame_reminder(text)
        if carried and isinstance(carried[-1], HumanMessage):
            carried[-1] = _with_text_block(carried[-1], block)
        else:
            carried.append(message.model_copy(update={"content": block}))
    return carried


def may_hold_breakpoint(message: Any) -> bool:
    """Whether a cache breakpoint on this message would be worth placing.

    Two of the carrier's own outputs say no. The envelope is transient, so an
    entry ending in it is never read back on the next call. An operator message
    stays unpinned because a breakpoint on that role is unproven. A row carried
    into the reminder shape is neither: it is history the carrier only
    re-shaped, so it holds the marker like any other message.
    """
    source = (getattr(message, "additional_kwargs", None) or {}).get("lc_source")
    if isinstance(message, SystemMessage):
        return source not in _CARRIER_SOURCES
    return source != RUNTIME_CONTEXT_SOURCE


# ---------------------------------------------------------------------------
# Shapes
# ---------------------------------------------------------------------------


def _place_envelope(
    base: list[Any], envelope_text: str, shape: CarrierShape
) -> tuple[list[Any], PinSite | None]:
    """Put the envelope last, and say where it landed when it merged into a message.

    Mutates the list it is given, which is always a copy the caller owns. The
    site it returns is the envelope's own block, which is the one thing in the
    request the breakpoint must stay in front of.
    """
    if not envelope_text:
        return base, None
    if shape in _OPERATOR_SHAPES:
        if _append_operator_text(base, envelope_text, shape, RUNTIME_CONTEXT_SOURCE):
            # An operator message never holds the marker, so its own blocks
            # need no site of their own.
            return base, None
        shape = "reminder"
    framed = frame_reminder(envelope_text)
    if base and isinstance(base[-1], HumanMessage):
        index = len(base) - 1
        base[index] = _with_text_block(base[index], framed)
        return base, PinSite(index, len(base[index].content) - 1)
    base.append(_reminder_message(framed))
    return base, None


def _pin_site(messages: list[Any], envelope_block: PinSite | None) -> PinSite | None:
    """The newest content the next call still carries, or None when there is none.

    A boundary at the very end of a request is unusable by the next call: the
    entry it writes ends in this call's envelope, which the next call does not
    carry, so nothing after the baseline is ever read back (measured on the
    OpenAI Responses API: 30% recovery at a turn boundary with the pin on the
    envelope, 97% with it one message earlier). One rule covers every shape:
    walking back from the tail, the marker goes on the last text block the
    carrier did not write this call, past the envelope (a message of its own, or
    merged as the last block of the last message) and past the operator messages
    the carrier built, which stay unpinned because a breakpoint on that role is
    unproven.
    """
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if not may_hold_breakpoint(message):
            continue
        limit = (
            envelope_block.block
            if envelope_block is not None and envelope_block.message == index
            else None
        )
        block = _last_text_block(message.content, limit)
        if block is not None:
            return PinSite(index, None if block == _WHOLE_BODY else block)
    return None


#: Stand-in index for a plain-string body, which has no blocks to count.
_WHOLE_BODY = -1


def _last_text_block(content: Any, limit: int | None) -> int | None:
    """Index of the last block that accepts a marker, or None when none does.

    ``limit`` excludes the envelope's own block and anything after it. A
    non-text tail (an image, a tool-use block) is walked past rather than
    tagged, because a provider may reject the marker on one.
    """
    if isinstance(content, str):
        return _WHOLE_BODY if content else None
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
    if messages and _is_operator_carrier(messages[-1]):
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

    Only the carrier tags a ``SystemMessage`` with a runtime-context source, and
    the tag never survives the request, so this names exactly the messages more
    harness text may be folded into.
    """
    return (
        isinstance(message, SystemMessage)
        and (message.additional_kwargs or {}).get("lc_source") in _CARRIER_SOURCES
    )


def _reminder_message(envelope_text: str) -> HumanMessage:
    return HumanMessage(
        content=envelope_text,
        additional_kwargs={"lc_source": RUNTIME_CONTEXT_SOURCE},
    )


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
