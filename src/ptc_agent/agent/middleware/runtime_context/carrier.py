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
text is a standalone message instead. Nor is a summary: a provider's own
summary goes out as its block alone, which would leave behind any text merged
into it.

A row that carries text a tool returned (``USER_ROLE_KINDS``) takes the
reminder shape on every carrier: on an operator role that text would read as
the operator's own.

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

from langchain_core.messages import HumanMessage, SystemMessage

from ptc_agent.agent.middleware.provider_cache import tag_last_text_block
from ptc_agent.agent.middleware.runtime_context.durable import (
    RUNTIME_UPDATE_KEY,
    RUNTIME_UPDATE_SOURCE,
    is_runtime_update_message,
    update_text,
)
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from ptc_agent.agent.middleware.runtime_context.turn import (
    LAST_STEP_ROW_KIND,
    MARKET_WATCH_KEPT_SINCE,
    MARKET_WATCH_ROW_KIND,
)
from ptc_agent.agent.transcript.classify import is_summary_message

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
class ComposedRequest:
    """The message list this call sends, and the site breakpoint 4 may take."""

    messages: list[Any]
    pin: PinSite | None


def resolve_carrier(model: Any) -> CarrierShape:
    """Which of the three shapes this model's harness text takes.

    ``"reminder"`` answers for every model without a native operator channel,
    and also whenever resolution itself fails: a row is context, and a
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
    if channel not in _OPERATOR_SHAPES:
        return "reminder"
    return channel


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


def compose_request(messages: list[Any], shape: CarrierShape) -> ComposedRequest:
    """The request this call sends: rows reshaped, pin site named.

    Neither the input list nor any input message is mutated: the rows stay
    provider-neutral in state and only the request copy takes a shape.
    """
    carried = _carry(messages, shape)
    return ComposedRequest(messages=carried, pin=_pin_site(carried))


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


def _carry(messages: list[Any], shape: CarrierShape) -> list[Any]:
    """A new message list with every persisted durable row rewritten into
    *shape*.

    Rows are written at the turn boundary, so each one follows the user message
    of its turn. That ordering is what every shape needs: the operator shapes
    because a mid-conversation ``system`` entry has to follow a user message,
    the reminder shape because it merges into the message in front of it. A
    row written mid-turn, such as a market-watch stamp or the notes reminder,
    follows a tool batch instead: an operator entry follows the results, and
    the reminder shape, with no user message in front, stands alone rather
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
        if not text.strip() or sent_for_one_call(message):
            # A row with nothing to say is dropped rather than sent as an empty
            # message, which some providers reject outright.
            continue
        if (
            shape in _OPERATOR_SHAPES
            and _row_kind(message) not in USER_ROLE_KINDS
            and _append_operator_text(carried, text, shape, RUNTIME_UPDATE_SOURCE)
        ):
            continue
        block = frame_reminder(text)
        if carried and isinstance(carried[-1], HumanMessage) and not is_summary_message(carried[-1]):
            carried[-1] = _with_text_block(carried[-1], block)
        else:
            carried.append(message.model_copy(update={"content": block}))
    return carried


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


#: Row kinds carrying text a tool returned, which go out in the user role.
USER_ROLE_KINDS = frozenset({LAST_STEP_ROW_KIND})


def _row_meta(message: Any) -> dict[str, Any]:
    meta = (message.additional_kwargs or {}).get(RUNTIME_UPDATE_KEY)
    return meta if isinstance(meta, dict) else {}


def _row_kind(message: Any) -> str | None:
    return _row_meta(message).get("kind")


def sent_for_one_call(message: Any) -> bool:
    """A market-watch stamp written before stamps were kept (see
    ``MARKET_WATCH_KEPT_SINCE``): no request carries it, so it costs nothing."""
    meta = _row_meta(message)
    return meta.get("kind") == MARKET_WATCH_ROW_KIND and (meta.get("schema_version") or 0) < MARKET_WATCH_KEPT_SINCE


def _pin_site(messages: list[Any]) -> PinSite | None:
    """The last text block of the request, or None when there is none.

    Every row is history and stays where it was written, so the tail is the
    prefix the next call extends. Walking back from it the marker skips the
    operator messages the carrier built, which stay unpinned because a
    breakpoint on that role is unproven.
    """
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if not may_hold_breakpoint(message):
            continue
        block = _last_text_block(message.content)
        if block is not None:
            return PinSite(index, None if block == _WHOLE_BODY else block)
    return None


#: Stand-in index for a plain-string body, which has no blocks to count.
_WHOLE_BODY = -1


def _last_text_block(content: Any) -> int | None:
    """Index of the last block that accepts a marker, or None when none does.

    A non-text tail (an image, a tool-use block) is walked past rather than
    tagged, because a provider may reject the marker on one.
    """
    if isinstance(content, str):
        return _WHOLE_BODY if content else None
    if not isinstance(content, list):
        return None
    for index in range(len(content) - 1, -1, -1):
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

    Only the carrier tags a ``SystemMessage`` with the row source, and the tag
    never survives the request, so this names exactly the messages more
    harness text may be folded into.
    """
    return (
        isinstance(message, SystemMessage)
        and (message.additional_kwargs or {}).get("lc_source") == RUNTIME_UPDATE_SOURCE
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
