"""A provider's own compaction, where its API lets the client pick the moment.

Anthropic's on-demand compaction (beta ``compact-2026-09-04``) is a request
carrying the messages to summarize plus ``compaction: {"type": "summarize"}``,
answered with one signed ``compaction`` block. Sent first in place of those
messages, the block keeps the thinking in the turns after it verifiable, which
a summary of our own cannot: replacing earlier messages with other text is an
edit, and an edit drops every later thinking block.

OpenAI's is the same request with a ``compaction_trigger`` item ending its
input (the request Codex sends), answered with one encrypted ``compaction``
item. Sent back after the user messages it summarized, kept as they were up
to a budget (:func:`kept_user_items`), it stands in for everything else.
Nothing but OpenAI can read the item, so the summary text beside it is a
digest the server builds. The item decrypts on any OpenAI model, through the
API or the Codex backend, so it is kept across both routes.

The checkpoint never holds either as content. A summary message keeps the
readable text every reader and every other model takes, and carries the
provider's answer beside it (``SIDECAR_KEY``); :func:`adopt` swaps it in on
the way out, only for a client that can take it and only inside the lineage
that wrote it, so a model switch or a fallback reads the text and nothing has
to be migrated.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, ToolMessage

from src.llms.endpoints import (
    is_official_anthropic_endpoint,
    is_official_openai_endpoint,
    url_host,
)
from src.llms.anthropic_betas import client_betas
from src.llms.operator_channel import tool_change_betas
from src.llms.reasoning_lineage import ROUTE_ENDPOINT_SEP, lineage_for_route

COMPACTION_BETA = "compact-2026-09-04"

#: The ``additional_kwargs`` key a summary message carries the block under,
#: with the route that signed it and the text sent after it.
SIDECAR_KEY = "provider_compaction"

_CODEX_HOST = "chatgpt.com"

#: How much of the user messages a compaction summarized OpenAI's item is sent
#: after, in tokens counted as Codex counts them (4 bytes each): Codex's own
#: budget, which mirrors what ``/responses/compact`` keeps.
KEPT_USER_TOKENS = 64_000

# The keys the API takes back. A streamed block also carries ``index``, and
# an echo of any other key is a 400. ``tool_changes`` is under the signature,
# so a block that recorded one is sent back with it.
_BLOCK_KEYS = ("type", "content", "signature", "tool_changes")


#: Whose compaction a client takes, which decides the request and the answer.
NativeKind = Literal["anthropic", "openai"]

#: The lineages one provider's answer is read in. OpenAI's API and Codex keep
#: their reasoning apart, but an item written on either decrypts on the other.
_NATIVE_LINEAGES: dict[str, str] = {"codex-oauth": "openai"}


def native_compaction_kind(
    entry: Mapping[str, Any] | None, provider: Mapping[str, Any] | None
) -> NativeKind | None:
    """Whose compaction a manifest model takes, read from its entry and its
    flattened provider entry; None for a model whose provider cannot compact
    for it. On by default wherever it is offered.

    Claude's where the entry sends the beta; OpenAI's on the official
    Responses endpoint. Of the subscription routes, Claude's and Codex's were
    proven to serve it.
    """
    if not entry or not provider:
        return None
    sdk = provider.get("sdk")
    access = provider.get("access_type")
    if sdk == "anthropic" and access in ("api_key", "oauth"):
        if not is_official_anthropic_endpoint(provider.get("base_url")):
            return None
        return "anthropic" if COMPACTION_BETA in (entry.get("additional_betas") or ()) else None
    if sdk == "codex" and access == "oauth" and provider.get("use_response_api"):
        return "openai"
    if (
        sdk == "openai"
        and access == "api_key"
        and provider.get("use_response_api")
        and is_official_openai_endpoint(provider.get("base_url"))
    ):
        return "openai"
    return None


def native_kind(model: Any) -> NativeKind | None:
    """Whose compaction ``model`` can take, or None.

    Read off the client rather than the manifest: only the official hosts were
    proven to serve it, and a ``base_url`` override stamps the route, so a BYOK
    key repointed elsewhere is off. Claude's also needs the beta riding on every
    request that carries a block, so a client that does not send it can never
    take one.
    """
    route = (getattr(model, "metadata", None) or {}).get("provider_route")
    if isinstance(route, str) and ROUTE_ENDPOINT_SEP in route:
        return None
    from langchain_anthropic import ChatAnthropic
    from langchain_openai import ChatOpenAI

    from src.llms.extension.codex import ChatCodexOpenAI

    if isinstance(model, ChatAnthropic):
        if not is_official_anthropic_endpoint(model.anthropic_api_url):
            return None
        return "anthropic" if COMPACTION_BETA in client_betas(model) else None
    if not (isinstance(model, ChatOpenAI) and model.output_version == "responses/v1"):
        return None
    # Exactly these two: the DashScope subclass speaks the Responses API to a
    # host that does not compact.
    if type(model) is ChatOpenAI and is_official_openai_endpoint(model.openai_api_base):
        return "openai"
    if type(model) is ChatCodexOpenAI and url_host(model.openai_api_base) == _CODEX_HOST:
        return "openai"
    return None


def compaction_settings(
    model: Any, settings: Mapping[str, Any], instructions: str
) -> dict[str, Any]:
    """``settings`` asking ``model`` to summarize instead of answering.

    ``extra_body`` is merged, not set: a call-time value replaces the one the
    client was built with whole, and that one can carry the thinking settings.
    OpenAI reads the marker as a ``compaction_trigger`` item (see
    ``patches``), which takes no instructions. Claude's also carries
    :data:`INLINE_TOOLS_BETA`, so the block records the net effect of the tool
    changes in the messages it summarizes (``tool_changes``), which would
    otherwise stop applying once it replaces them.
    """
    built = {
        **((getattr(model, "model_kwargs", None) or {}).get("extra_body") or {}),
        **(getattr(model, "extra_body", None) or {}),
    }
    extra = {**built, **(settings.get("extra_body") or {})}
    extra["compaction"] = {"type": "summarize", "instructions": instructions}
    if native_kind(model) == "anthropic":
        settings = {**settings, "betas": tool_change_betas(settings.get("betas"))}
    return {**settings, "extra_body": extra}


def is_compaction_request(settings: Mapping[str, Any]) -> bool:
    return "compaction" in (settings.get("extra_body") or {})


def native_answer(
    message: AIMessage | None, kept_texts: Sequence[str] = ()
) -> dict[str, Any] | None:
    """What a compaction request answered with, cleaned to what the API takes
    back, or None when it wrote no compaction: from Claude a ``block``, and
    only under a ``compaction`` stop reason (any other means cut off, refused,
    or a tool call instead), from OpenAI its ``items``: the encrypted item
    after the user messages it summarized, ``kept_texts``, as
    :func:`kept_user_items` keeps them."""
    if message is None or not isinstance(message.content, list):
        return None
    for block in message.content:
        if not isinstance(block, dict) or block.get("type") != "compaction":
            continue
        # Claude's is told apart by its signature, which OpenAI's never has.
        cleaned = {k: block[k] for k in _BLOCK_KEYS if block.get(k) is not None}
        if isinstance(cleaned.get("content"), str) and cleaned.get("signature"):
            finished = (message.response_metadata or {}).get("stop_reason") == "compaction"
            return {"block": cleaned} if finished else None
        if isinstance(block.get("encrypted_content"), str) and not block.get("signature"):
            # Its id names an item a store=false server never kept.
            item = {"type": "compaction", "encrypted_content": block["encrypted_content"]}
            return {"items": [*kept_user_items(kept_texts), item]}
    return None


def readable_text(answer: Mapping[str, Any]) -> str | None:
    """The summary a provider's answer holds as text, or None for one only
    that provider can read (OpenAI's)."""
    block = answer.get("block")
    return block["content"] if isinstance(block, Mapping) else None


def kept_user_items(texts: Sequence[str]) -> list[dict[str, Any]]:
    """The user messages OpenAI's item is sent after, oldest first, as Codex
    keeps them: the newest of ``texts`` whole up to :data:`KEPT_USER_TOKENS`,
    the one that crosses it cut to what is left, older ones dropped."""
    kept: list[str] = []
    budget = KEPT_USER_TOKENS * 4
    for text in reversed(texts):
        if not text:
            continue
        encoded = text.encode()
        if len(encoded) > budget:
            if budget > 0:
                kept.append(encoded[:budget].decode(errors="ignore"))
            break
        kept.append(text)
        budget -= len(encoded)
    return [
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]}
        for text in reversed(kept)
    ]


def kept_user_texts(message: AnyMessage) -> list[str]:
    """The user messages a summary's OpenAI item was sent after, which the
    next compaction keeps again."""
    items = (_carried(message) or {}).get("items")
    if not isinstance(items, list):
        return []
    return [_item_text(item) for item in items if isinstance(item, dict) and _is_user_item(item)]


def _is_user_item(item: Mapping[str, Any]) -> bool:
    return item.get("type") == "message" and item.get("role") == "user"


def _item_text(item: Mapping[str, Any]) -> str:
    return "\n".join(
        part["text"]
        for part in item.get("content") or ()
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    )


def sidecar(answer: Mapping[str, Any], origin: str | None, text: str) -> dict[str, Any]:
    """What a summary message carries for :func:`adopt`: the provider's
    answer (see :func:`native_answer`), the route that wrote it, and ``text``,
    sent after it (see :func:`carried_text`)."""
    return {**answer, "origin": origin, "text": text, "alone": True}


def carried_text(message: AnyMessage) -> str | None:
    """The text a summary carrying a provider's answer has beside it (the
    transcript pointer, the skills to reload, the notes pointer), or None.

    OpenAI's items are sent with it after them. Claude's block is sent alone:
    the turns it keeps must follow it with nothing between, or their thinking
    is no longer valid, so this text is for a message after those turns.
    """
    text = (_carried(message) or {}).get("text")
    return text if isinstance(text, str) and text else None


def deferred_text(message: AnyMessage | None) -> str | None:
    """The text a summary leaves for a message after the turns it kept, or None.

    Only a summary carrying Claude's block sent alone has one: the kept turns
    must follow the block with nothing between, so its text (the transcript,
    skills and notes pointers) goes after them. Every other summary, one
    written before the block went alone included, sends that text itself.
    """
    if not _sent_alone(_carried(message)):
        return None
    text = carried_text(message)
    return text if text and text.strip() else None


def _sent_alone(carried: Mapping[str, Any] | None) -> bool:
    # TODO(2026-11-25): delete 30 days after this ships, once the threads written before it have aged out.
    return (
        carried is not None
        and isinstance(carried.get("block"), dict)
        and bool(carried.get("alone"))
    )


def is_opaque(message: AnyMessage) -> bool:
    """Whether ``message`` is a summary whose provider's answer only it can
    read, so its text is a digest rather than a summary."""
    return isinstance((_carried(message) or {}).get("items"), list)


def _carried(message: AnyMessage) -> dict[str, Any] | None:
    """The provider's answer a summary message carries (see :func:`sidecar`)."""
    if not isinstance(message, HumanMessage):
        return None
    carried = (message.additional_kwargs or {}).get(SIDECAR_KEY)
    return carried if isinstance(carried, dict) else None


def adopt(
    messages: Sequence[AnyMessage], model: Any, route: str | None
) -> list[AnyMessage] | None:
    """``messages`` with each summary carrying a provider's answer sent as
    that answer, or None when none is: the target cannot take one, or the
    answer was written in another lineage than ``route``'s, where it reads the
    summary text."""
    kind = native_kind(model)
    if kind is None:
        return None
    lineage = _native_lineage(route)
    adopted: list[AnyMessage] = []
    changed = False
    for i, message in enumerate(messages):
        following = messages[i + 1] if i + 1 < len(messages) else None
        native = _native(message, lineage, kind, before_user=isinstance(following, (HumanMessage, ToolMessage)))
        adopted.extend(native or (message,))
        changed = changed or native is not None
    return adopted if changed else None


def replays(message: AnyMessage, model: Any) -> bool:
    """Whether ``model`` is sent the provider's answer ``message`` carries,
    rather than its text (see :func:`adopt`)."""
    kind = native_kind(model)
    if kind is None:
        return False
    route = (getattr(model, "metadata", None) or {}).get("provider_route")
    route = route if isinstance(route, str) and route else None
    return _native(message, _native_lineage(route), kind) is not None


def replayed_tool_changes(message: AnyMessage, model: Any) -> list[dict[str, Any]]:
    """The net tool changes the API re-applies at ``message`` for ``model``.

    A compaction block records the ``tool_addition`` / ``tool_removal`` blocks
    of the turns it summarized in its ``tool_changes``, and the API applies
    them again where the block is adopted. Empty when ``model`` is not sent
    the block (see :func:`adopt`) or the block records none.
    """
    if not replays(message, model):
        return []
    block = (_carried(message) or {}).get("block")
    changes = block.get("tool_changes") if isinstance(block, dict) else None
    if not isinstance(changes, list):
        return []
    return [change for change in changes if isinstance(change, dict)]


def _native(
    message: AnyMessage, lineage: str, kind: NativeKind, *, before_user: bool = False
) -> list[AnyMessage] | None:
    """The provider's answer ``message`` carries, as ``kind`` is sent it.

    Claude's block goes alone in its own turn: the summary's user turn where
    the turns it kept open on an answer, an assistant turn before a user
    message (``before_user``), as after a summary that kept none, where in a
    user turn the new message would join it. A summary written before the
    block went alone (no ``alone`` in its sidecar) is sent as it was then,
    the block and its text, so the thinking after it stays valid.
    """
    carried = _carried(message)
    if carried is None:
        return None
    origin = carried.get("origin")
    if not isinstance(origin, str) or _native_lineage(origin) != lineage:
        return None
    text = carried_text(message)
    if kind == "anthropic" and isinstance(carried.get("block"), dict):
        block: list[Any] = [dict(carried["block"])]
        if not _sent_alone(carried):
            tail = [{"type": "text", "text": text}] if text else []
            return [message.model_copy(update={"content": [*block, *tail]})]
        if before_user:
            return [AIMessage(content=block, id=message.id, additional_kwargs=dict(message.additional_kwargs))]
        return [message.model_copy(update={"content": block})]
    if kind == "openai" and isinstance(carried.get("items"), list):
        replayed = [_replayed(item) for item in carried["items"] if isinstance(item, dict)]
        return [*filter(None, replayed), *([HumanMessage(text)] if text else [])]
    return None


def _native_lineage(route: str | None) -> str:
    lineage = lineage_for_route(route)
    return _NATIVE_LINEAGES.get(lineage, lineage)


def _replayed(item: Mapping[str, Any]) -> AnyMessage | None:
    """An item of the compacted window as the message langchain sends it as."""
    if item.get("type") == "compaction":
        return AIMessage(content=[dict(item)])
    if _is_user_item(item):
        return HumanMessage(content=list(item.get("content") or ()))
    return None


def without_blocks(messages: Sequence[AnyMessage]) -> list[AnyMessage] | None:
    """``messages`` with every summary reading its text, or None when none
    carried a provider's answer: the retry after a provider refused the
    request."""
    result: list[AnyMessage] = []
    changed = False
    for message in messages:
        if _carried(message) is not None:
            kwargs = {k: v for k, v in message.additional_kwargs.items() if k != SIDECAR_KEY}
            message = message.model_copy(update={"additional_kwargs": kwargs})
            changed = True
        result.append(message)
    return result if changed else None
