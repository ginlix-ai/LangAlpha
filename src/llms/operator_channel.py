"""Per-provider operator channel for harness-authored per-call context.

The runtime-context envelope is written by the harness, not by the user, so it
wants a message role the user cannot forge. Where a provider exposes one it takes
one of two forms:

- ``"developer"`` on OpenAI-shaped APIs. ``langchain_openai`` already maps a
  ``SystemMessage`` carrying ``additional_kwargs["__openai_role__"] =
  "developer"`` onto that role, on both the Chat Completions and the Responses
  layout, so nothing has to be patched for this form.
- ``"system"`` mid-conversation on Anthropic's Messages API and on the
  OpenAI-shaped GLM endpoint. ``langchain-anthropic`` hoists every
  ``SystemMessage`` into the top-level ``system`` parameter and raises on a
  second non-consecutive one, so the Messages form needs the bridge at the end
  of this module, which keeps a tagged message at its own position only inside
  :func:`operator_request`, the window the tail middleware opens around a call
  it composed in the system shape. The GLM form needs no bridge: the
  OpenAI-style serializer already emits a plain ``{"role": "system"}`` entry
  for a ``SystemMessage`` that carries no ``__openai_role__``.

Where a provider has neither, the envelope rides as a ``<system-reminder>``
block inside the last user message. That shape is the carrier's job; this
module only answers which native channel, if any, a bound model can carry.

Configuration
-------------
``provider_config`` entries in ``manifest/providers.json`` may carry an optional
``operator_channel`` key. ``providers.json`` has no comment syntax, so the valid
values are recorded here:

===============  ==========================================================
Value            Meaning
===============  ==========================================================
``"developer"``  Emit the envelope as a ``developer``-role message. Valid on
                 an entry whose client is an OpenAI-shaped LangChain model
                 (``sdk`` of ``openai``, ``codex``, ``dashscope``,
                 ``deepseek``, ``glm`` or ``qwq``).
``"system"``     Emit the envelope as a mid-conversation ``system``-role
                 message. Valid on ``sdk: "anthropic"`` pointed at
                 ``api.anthropic.com`` or at one of the compatible hosts in
                 :data:`_ANTHROPIC_COMPATIBLE_HOSTS`, and on ``sdk: "glm"``.
absent           Off. The envelope uses the user-message fallback.
``null``         Off, on a variant whose parent declares a channel: the
                 flatten merge hands a parent's key down to every variant,
                 and an explicit null is how a variant on an unproven host
                 refuses it (``moonshot-coding`` on ``api.kimi.com``).
===============  ==========================================================

A key is declared only on an entry whose endpoint a live probe proved to accept
the role; a wrong value costs a 400 on every turn rather than a degraded one.
The declared endpoint is the proven one, so a client repointed off it by a
``base_url`` override carries no channel at all (see the hard rules).

Hard rules
----------
:func:`resolve_operator_channel` re-derives eligibility from the client in hand
rather than trusting the entry, because BYOK can repoint a built-in provider at
another endpoint while keeping its provider key. Config can only turn a channel
*off*, never on:

- Any channel requires the client to dial the endpoint its entry declares.
  ``LLM.get_llm`` stamps ``provider_route`` as ``<key>@<base_url>`` exactly
  when a ``base_url`` override moved the client off the manifest endpoint, and
  a repointed client resolves to nothing: the probe that proved the role was
  run against the declared host, and says nothing about the one in hand.
- ``"developer"`` requires a ``BaseChatOpenAI`` client. This is a positive
  allowlist, so the Gemini client (whose converter silently folds a
  mid-conversation ``SystemMessage`` into the top-level system instruction, or
  drops it outright when there is none to fold into) can never resolve to a
  channel no matter what its entry says.
- ``"system"`` requires a ``ChatAnthropic`` client on a host in
  :data:`_ANTHROPIC_SYSTEM_HOSTS`, or a ``ChatZai`` client. Every other host and
  client family is forced off even when its ``sdk`` says ``anthropic``: a live
  probe is the only evidence that an endpoint serves the role rather than
  silently rewriting it.
- ``"system"`` additionally requires a model the host serves it on. Which models
  those are is per host: ``api.anthropic.com`` serves it from Opus 4.8 on
  (:data:`_ANTHROPIC_MIDTURN_SYSTEM_SINCE`) and answers every earlier model with
  ``role 'system' is not supported on this model``, the compatible hosts serve it
  on every model, and GLM serves it on :data:`_ZAI_MIDTURN_SYSTEM_MODELS` only
  (``glm-5.3-flash`` answers 200 but reads the entry as a user message, which is
  worse than a 400). An id the host's rule cannot place fails closed onto the
  user-message fallback.

The Anthropic wire form also carries placement rules the caller must honor: a
mid-conversation system message has to follow a user message (or an assistant
message ending in server-tool use) and must be either the last entry in
``messages`` or be followed by an assistant turn. It cannot be ``messages[0]``,
which is why an operator-tagged message at index 0 is left to the ordinary
hoist.
"""

from __future__ import annotations

import contextvars
import logging
import os
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Literal
from urllib.parse import urlparse

from langchain_core.messages import SystemMessage
from langchain_openai.chat_models.base import BaseChatOpenAI

from src.llms.reasoning_lineage import ROUTE_ENDPOINT_SEP

logger = logging.getLogger(__name__)

OperatorChannel = Literal["developer", "system"]

#: The ``additional_kwargs`` key the Anthropic operator client keys on. The
#: OpenAI-shaped form needs no marker of our own: ``__openai_role__`` is
#: langchain-openai's own contract and already does the routing.
OPERATOR_CHANNEL_KEY = "lc_operator_channel"

#: Default provenance tag, shared with the envelope middleware so a carrier
#: message can be recognized without re-parsing its body. A caller carrying
#: something other than the envelope passes its own ``source``.
RUNTIME_CONTEXT_SOURCE = "runtime_context"

_ANTHROPIC_HOST = "api.anthropic.com"
_OFFICIAL_OPENAI_HOST = "api.openai.com"

#: The first Claude generation, as ``(major, minor)``, that accepts
#: ``role: "system"`` inside ``messages`` on the official host: Opus 4.8. Every
#: model released since does, and every earlier one the host still serves answers
#: 400. A cutoff rather than a list, so a new release needs no edit here. It
#: stays because a custom model can still name a Claude 4 id the host refuses.
_ANTHROPIC_MIDTURN_SYSTEM_SINCE = (4, 8)

#: ``claude-<family>-<major>[-<minor>]``, matched once the date suffix is gone.
#: The pair is compared across families because the line falls in the same place
#: for all of them: every id below 4.8 refuses the role, whatever its family.
_CLAUDE_GENERATION = re.compile(r"claude-[a-z]+-(\d+)(?:-(\d+))?")

#: Third-party hosts speaking the Anthropic Messages protocol that were proven
#: on live calls to place the entry at its own position, read it as the newest
#: context, and leave the prefix cache intact. They serve it on every model, so
#: they carry no model split of their own.
_ANTHROPIC_COMPATIBLE_HOSTS = frozenset(
    {
        "api.deepseek.com",
        "api.moonshot.cn",
        "api.moonshot.ai",
        "api.minimaxi.com",
        "api.minimax.io",
        "api.minimax.cn",
    }
)

#: GLM models that read a mid-list ``system`` entry as system. ``glm-5.3-flash``
#: reads the same entry as a user message, so the split is per model, not per host.
_ZAI_MIDTURN_SYSTEM_MODELS = frozenset({"glm-5.3"})

_DATE_SUFFIX = re.compile(r"-\d{8}$")


def resolve_operator_channel(model: Any) -> OperatorChannel | None:
    """The native operator channel a bound model can carry, or None.

    Returns None for a model whose provider is absent from ``provider_config``,
    whose entry declares no ``operator_channel``, or whose client fails one of
    the hard rules in the module docstring. None means "use the user-message
    fallback", never "raise".
    """
    provider = _provider_key(model)
    if not provider:
        return None

    entry = _provider_entry(provider)
    if entry is None or _repointed(model) or _off_declared_host(model, entry):
        return None

    channel = entry.get("operator_channel")
    if channel == "developer":
        return "developer" if isinstance(model, BaseChatOpenAI) else None
    if channel == "system":
        return "system" if _carries_midturn_system(model) else None
    if channel is not None:
        logger.warning(
            "[operator_channel] provider %r declares unknown operator_channel %r; "
            "treating as off",
            provider,
            channel,
        )
    return None


def build_operator_message(
    text: str, channel: OperatorChannel, *, source: str = RUNTIME_CONTEXT_SOURCE
) -> SystemMessage:
    """The carrier message for *text* on *channel*, tagged with its *source*.

    Both forms are a ``SystemMessage``. The channel lives in
    ``additional_kwargs`` rather than in a subclass so the message survives
    LangGraph checkpointing and every message-list transform in the middleware
    stack unchanged. ``source`` names the harness writer the text came from,
    so a reader can tell the envelope from a durable row without reading either.
    """
    if channel == "developer":
        extra = {"__openai_role__": "developer"}
    elif channel == "system":
        extra = {OPERATOR_CHANNEL_KEY: "system"}
    else:
        raise ValueError(f"Unknown operator channel: {channel!r}")
    return SystemMessage(content=text, additional_kwargs={**extra, "lc_source": source})


def is_operator_system_message(message: Any) -> bool:
    """True for a SystemMessage tagged for the Anthropic mid-turn carrier."""
    if not isinstance(message, SystemMessage):
        return False
    kwargs = message.additional_kwargs or {}
    return kwargs.get(OPERATOR_CHANNEL_KEY) == "system"


# --------------------------------------------------------------------------
# Resolution helpers
# --------------------------------------------------------------------------


def _provider_key(model: Any) -> str | None:
    """The providers.json key for a bound model.

    ``LLM.get_llm`` stamps ``pricing_model_id``/``pricing_provider`` onto every
    client it builds, which is the same identity a manifest lookup would yield
    and survives the client substitution ``ModelResilienceMiddleware`` performs.
    A model built outside that factory carries no stamp and resolves to nothing,
    which is the safe answer.
    """
    metadata = getattr(model, "metadata", None)
    if not isinstance(metadata, dict):
        return None
    provider = metadata.get("pricing_provider")
    return provider if isinstance(provider, str) and provider else None


def _repointed(model: Any) -> bool:
    """True when a ``base_url`` override moved the client off its manifest endpoint."""
    route = getattr(model, "metadata", {}).get("provider_route")
    return isinstance(route, str) and ROUTE_ENDPOINT_SEP in route


def _off_declared_host(model: Any, entry: dict[str, Any]) -> bool:
    """True when an OpenAI-shaped client resolved to a host its entry does not declare.

    ``_repointed`` sees the ``base_url`` override the factory stamps into the
    route, but a custom model's ``parameters.base_url`` is merged into the
    constructor after that stamp, and the SDK reads ``OPENAI_BASE_URL`` and
    ``OPENAI_API_BASE`` after the factory is done, so the client in hand can
    dial an endpoint that nothing in its metadata names. The built client is
    the only witness: an entry that declares a host is compared with it
    outright, and one that leaves the SDK default in place (a third-party SDK
    with a default of its own) is off only when the env override took.
    """
    if not isinstance(model, BaseChatOpenAI):
        return False
    resolved = getattr(getattr(model, "root_client", None), "base_url", None)
    if not resolved:
        return False
    declared = entry.get("base_url")
    if declared:
        return _host(str(declared)) != _host(str(resolved))
    override = os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL")
    return bool(override) and _host(override) == _host(str(resolved))


def _host(url: str) -> str:
    parsed = urlparse(url)
    if parsed.hostname is None:
        parsed = urlparse(f"//{url}")
    return (parsed.hostname or "").lower()


def _provider_entry(provider: str) -> dict[str, Any] | None:
    # Imported lazily: llm.py's factory reads this module, so a module-level
    # import would close the cycle.
    from src.llms.llm import LLM

    entry = LLM.get_model_config().flat_providers.get(provider)
    return entry if isinstance(entry, dict) else None


def _carries_midturn_system(model: Any) -> bool:
    return _anthropic_midturn_system_ok(model) or _zai_midturn_system_ok(model)


def _anthropic_midturn_system_ok(model: Any) -> bool:
    try:
        from langchain_anthropic import ChatAnthropic
    except ImportError:  # pragma: no cover - langchain_anthropic is a hard dep
        return False
    if not isinstance(model, ChatAnthropic) or not _bridge_installed:
        # Without the bridge the stock hoist raises on a mid-conversation
        # system message, so the channel is off rather than fatal.
        return False
    return anthropic_serves_midturn_system(
        getattr(model, "anthropic_api_url", None), getattr(model, "model", None)
    )


def anthropic_serves_midturn_system(base_url: str | None, model_id: Any) -> bool:
    """Whether the host at *base_url* serves ``role: "system"`` on *model_id*.

    A None base_url is the official host.
    """
    rule = _ANTHROPIC_SYSTEM_HOSTS.get(_resolved_host(base_url))
    return rule is not None and rule(model_id)


def _zai_midturn_system_ok(model: Any) -> bool:
    """True for a ChatZai client on a model that reads the mid-list system role.

    The vendored client is imported lazily so this module stays importable
    wherever it is not installed, which is the same reason the Anthropic and
    Gemini checks import inside the function.
    """
    try:
        from src.llms.vendor.langchain_zai import ChatZai
    except ImportError:
        return False
    if not isinstance(model, ChatZai):
        return False
    return _in_allowlist(
        getattr(model, "model_name", None), _ZAI_MIDTURN_SYSTEM_MODELS
    )


def _resolved_host(base_url: str | None) -> str:
    """The hostname an Anthropic-protocol client with this base_url dials.

    A None base_url means the client fell through to the SDK default, which is
    the official Anthropic host. langchain-anthropic resolves the env fallbacks
    into the field before the client is built, so there is no env chain to
    re-walk here.
    """
    if not base_url:
        return _ANTHROPIC_HOST
    parsed = urlparse(base_url)
    if parsed.hostname is None:
        parsed = urlparse(f"//{base_url}")
    return (parsed.hostname or "").lower()


def _in_allowlist(model_id: Any, allowed: frozenset[str]) -> bool:
    if not isinstance(model_id, str) or not model_id:
        return False
    return _DATE_SUFFIX.sub("", model_id) in allowed


def _accepts_midturn_system(model_id: Any) -> bool:
    if not isinstance(model_id, str):
        return False
    generation = _CLAUDE_GENERATION.fullmatch(_DATE_SUFFIX.sub("", model_id))
    if generation is None:
        return False
    major, minor = generation.groups(default="0")
    return (int(major), int(minor)) >= _ANTHROPIC_MIDTURN_SYSTEM_SINCE


def _any_model(_model_id: Any) -> bool:
    return True


#: Host -> the rule a model id must pass for the mid-turn system role there.
#: A table rather than a chain of ifs because the split is per host: Anthropic
#: serves the role on part of its own fleet, the compatible hosts on all of it.
_ANTHROPIC_SYSTEM_HOSTS: dict[str, Callable[[Any], bool]] = {
    _ANTHROPIC_HOST: _accepts_midturn_system,
    **{host: _any_model for host in _ANTHROPIC_COMPATIBLE_HOSTS},
}


# --------------------------------------------------------------------------
# Anthropic wire entry
# --------------------------------------------------------------------------


def operator_system_entry(message: SystemMessage) -> dict[str, Any] | None:
    """The ``{"role": "system", ...}`` wire entry for a tagged message.

    None when nothing survives, matching the stock hoist's own rule that empty
    text blocks are not accepted: a carrier with no text is dropped rather than
    sent as a malformed message.
    """
    blocks: list[dict[str, Any]] = []
    content = message.content
    if isinstance(content, str):
        if content.strip():
            blocks.append({"type": "text", "text": content})
    elif isinstance(content, list):
        for block in content:
            if isinstance(block, str):
                if block.strip():
                    blocks.append({"type": "text", "text": block})
            elif isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text", "")
                if isinstance(text, str) and text.strip():
                    blocks.append(dict(block))
    if not blocks:
        return None
    return {"role": "system", "content": blocks}


# --------------------------------------------------------------------------
# Anthropic bridge: opt-in per request through a ContextVar
# --------------------------------------------------------------------------

# True only inside :func:`operator_request` for the system shape, which is
# exactly the window in which upstream calls the hoist the bridge wraps.
_in_operator_request: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "in_anthropic_operator_request", default=False
)


@contextmanager
def operator_request(shape: str | None) -> Iterator[None]:
    """Open the window in which a tagged system message keeps its position.

    The caller is the tail middleware, around the one model call it composed;
    it passes the shape it composed in, so every other shape is a no-op and the
    stock hoist answers. Scoping the switch to the call rather than to a client
    keeps ``ChatAnthropic`` itself the client every Anthropic-protocol model is
    served on, with no subclass carrying a flag for the rest of the fleet.
    """
    if shape != "system":
        yield
        return
    token = _in_operator_request.set(True)
    try:
        yield
    finally:
        _in_operator_request.reset(token)


def _splice_operator_entries(original: Callable[..., Any], messages: Any) -> Any:
    """Format the runs between tagged messages and put each entry back in place.

    The untouched original formats each run, so a message the caller did not
    tag takes the exact path it took before, and a second untagged system
    message is still the refusal it always was.
    """
    msgs = list(messages)
    cuts = [i for i, m in enumerate(msgs) if i > 0 and is_operator_system_message(m)]
    if not cuts:
        return original(msgs)

    system: Any = None
    formatted: list[dict[str, Any]] = []
    start = 0
    for cut in [*cuts, len(msgs)]:
        run_system, run_formatted = original(msgs[start:cut])
        if run_system is not None:
            if system is not None:
                msg = "Received multiple non-consecutive system messages."
                raise ValueError(msg)
            system = run_system
        formatted.extend(run_formatted)
        if cut < len(msgs):
            entry = operator_system_entry(msgs[cut])
            if entry is not None:
                formatted.append(entry)
        start = cut + 1
    return system, formatted


_bridge_installed = False


def _install_format_bridge() -> None:
    """Wrap ``langchain_anthropic``'s hoist once. It acts only under the flag.

    ``_format_messages`` is a bare module function called from the request
    builder, with no client argument, so a subclass cannot reach it; the
    ContextVar is the only handle for telling a composed call from any other
    Anthropic call in the process. Outside the window the wrapper is one extra
    call to the original.
    """
    global _bridge_installed
    try:
        import langchain_anthropic.chat_models as chat_models

        original = chat_models._format_messages
    except (ImportError, AttributeError):
        logger.warning(
            "[operator_channel] langchain_anthropic._format_messages is gone; "
            "providers configured for operator_channel='system' fall back to "
            "the user-message carrier"
        )
        _bridge_installed = False
        return
    _bridge_installed = True
    if getattr(original, "_operator_bridge", False):
        return

    def _bridged(messages):
        if not _in_operator_request.get():
            return original(messages)
        return _splice_operator_entries(original, messages)

    _bridged._operator_bridge = True
    chat_models._format_messages = _bridged


_install_format_bridge()
