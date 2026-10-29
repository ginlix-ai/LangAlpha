"""Monkey-patches for upstream library bugs."""

import functools
import logging
from types import SimpleNamespace

from src.llms.anthropic_betas import client_betas

logger = logging.getLogger(__name__)

_USAGE_KEYS = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)
_CACHE_CREATION_KEYS = ("ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens")


def _get(value, key, default):
    """``key`` of a dict or an SDK object alike, ``default`` when it is unset."""
    found = value.get(key) if isinstance(value, dict) else getattr(value, key, None)
    return found or default


def _billed_usage(anthropic_usage):
    """The usage a response is billed for.

    A response that ran more than one iteration (an on-demand compaction, whose
    top-level counts are all zero) reports each in ``usage.iterations``, and
    only their sum is what was billed.
    """
    iterations = getattr(anthropic_usage, "iterations", None)
    if not iterations:
        return anthropic_usage
    totals = {key: sum(_get(it, key, 0) for it in iterations) for key in _USAGE_KEYS}
    cache_creation = {
        key: sum(_get(_get(it, "cache_creation", {}), key, 0) for it in iterations)
        for key in _CACHE_CREATION_KEYS
    }
    thinking = [_get(it, "output_tokens_details", {}) for it in iterations]
    details = (
        SimpleNamespace(thinking_tokens=sum(_get(d, "thinking_tokens", 0) for d in thinking))
        if any(d for d in thinking)
        else None
    )
    return SimpleNamespace(
        **totals, cache_creation=cache_creation, output_tokens_details=details
    )


def _patch_langchain_anthropic_usage_metadata():
    """Fix TypeError when cache_creation fields are None, and count usage
    reported per iteration.

    langchain-anthropic v1.4.0 crashes on dict.get(k, 0) returning None
    when third-party providers return null for cache token fields.
    """
    try:
        import langchain_anthropic.chat_models as chat_models
        from pydantic import BaseModel

        original_fn = chat_models._create_usage_metadata
        if getattr(original_fn, "_is_patched", False):
            return

        def _patched(anthropic_usage: BaseModel):
            anthropic_usage = _billed_usage(anthropic_usage)
            cache_creation = getattr(anthropic_usage, "cache_creation", None)
            if cache_creation is not None:
                for field in ("ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"):
                    if isinstance(cache_creation, dict):
                        if cache_creation.get(field) is None:
                            cache_creation[field] = 0
                    elif getattr(cache_creation, field, None) is None:
                        object.__setattr__(cache_creation, field, 0)
            return original_fn(anthropic_usage)

        _patched._is_patched = True
        chat_models._create_usage_metadata = _patched
        logger.debug("Patched langchain_anthropic._create_usage_metadata for None cache values")
    except Exception as e:
        logger.warning(f"Failed to patch langchain_anthropic: {e}")


def _patch_langchain_anthropic_context_management():
    """Keep an unmodelled ``context_management`` stream field from killing the call.

    Some Anthropic-compatible proxies put it on the final ``message_delta``. The
    SDK's event model has no such field, so it lands in pydantic extras as a plain
    dict, and langchain-anthropic calls ``.model_dump()`` on it. The dict goes to
    ``response_metadata`` as-is, where the typed path would have put its dump.
    """
    try:
        from langchain_anthropic.chat_models import ChatAnthropic

        original_fn = ChatAnthropic._make_message_chunk_from_anthropic_event
        if getattr(original_fn, "_is_patched", False):
            return

        def _patched(self, event, *args, **kwargs):
            raw = None
            if getattr(event, "type", None) == "message_delta":
                raw = getattr(event, "context_management", None)
            if raw is None or hasattr(raw, "model_dump"):
                return original_fn(self, event, *args, **kwargs)

            # The event is the stream's own object, so the original gets a copy.
            stripped = event.model_copy()
            (stripped.__pydantic_extra__ or {}).pop("context_management", None)
            chunk, block_start_event = original_fn(self, stripped, *args, **kwargs)
            if chunk is not None and isinstance(raw, dict):
                chunk.response_metadata["context_management"] = raw
            return chunk, block_start_event

        _patched._is_patched = True
        ChatAnthropic._make_message_chunk_from_anthropic_event = _patched
        logger.debug("Patched ChatAnthropic stream event for a dict context_management")
    except Exception as e:
        logger.warning(f"Failed to patch langchain_anthropic context_management: {e}")


def _patch_langchain_anthropic_compaction_block():
    """Keep an on-demand compaction block a stream carries.

    The block arrives whole on its ``content_block_start`` event, with no
    deltas after it, and langchain-anthropic 1.7.6 turns only text, thinking
    and tool blocks on that event into a chunk, so a streamed compaction
    request came back empty.
    """
    try:
        from langchain_anthropic.chat_models import ChatAnthropic
        from langchain_core.messages import AIMessageChunk

        original = ChatAnthropic._make_message_chunk_from_anthropic_event
        # Its own marker: the context_management patch wraps the same method,
        # and a shared one would have the second to install skip itself.
        # ``wraps`` carries that patch's marker up, so it still finds itself.
        if getattr(original, "_keeps_compaction_blocks", False):
            return

        @functools.wraps(original)
        def _patched(self, event, *args, **kwargs):
            block = getattr(event, "content_block", None)
            if event.type == "content_block_start" and getattr(block, "type", None) == "compaction":
                content = block.model_dump(exclude_none=True)
                content["index"] = event.index
                return AIMessageChunk(content=[content]), event
            return original(self, event, *args, **kwargs)

        _patched._keeps_compaction_blocks = True
        ChatAnthropic._make_message_chunk_from_anthropic_event = _patched
    except Exception as e:
        logger.warning(f"Failed to patch langchain_anthropic compaction streaming: {e}")


def _patch_langchain_anthropic_input_transformations():
    """Keep the API's ``input_transformations`` on a streamed answer, verbatim.

    It lists the earlier thinking blocks the API dropped from the request, or
    let through unchecked, before the model read it, and a stream sends it on
    ``message_start``, of which langchain-anthropic 1.7.6 keeps only the model
    name. A non-streamed answer already keeps it: the body's extra fields go
    to ``llm_output``, which langchain-core merges into the message's
    metadata. Kept as the API sent it: later checks add types and reasons.
    """
    try:
        from langchain_anthropic.chat_models import ChatAnthropic

        from src.llms.input_transformations import INPUT_TRANSFORMATIONS

        original = ChatAnthropic._make_message_chunk_from_anthropic_event
        if getattr(original, "_keeps_input_transformations", False):
            return

        @functools.wraps(original)
        def _patched(self, event, *args, **kwargs):
            chunk, block_start_event = original(self, event, *args, **kwargs)
            if chunk is not None and getattr(event, "type", None) == "message_start":
                found = getattr(event.message, INPUT_TRANSFORMATIONS, None)
                if isinstance(found, list):
                    # An SDK that models the entries would hand objects the
                    # checkpoint cannot hold; the non-streamed path dumps them.
                    chunk.response_metadata[INPUT_TRANSFORMATIONS] = [
                        e.model_dump(mode="json") if hasattr(e, "model_dump") else e for e in found
                    ]
            return chunk, block_start_event

        _patched._keeps_input_transformations = True
        ChatAnthropic._make_message_chunk_from_anthropic_event = _patched
    except Exception as e:
        logger.warning(f"Failed to patch langchain_anthropic input_transformations: {e}")


def _patch_langchain_openai_compaction_trigger():
    """Ask OpenAI to compact where a Responses request carries the
    compaction marker (``provider_compaction.compaction_settings``).

    OpenAI compacts in an ordinary request whose input ends with a
    ``compaction_trigger`` item, the request Codex sends, and langchain-openai
    1.3 has no message for it: such a content block is dropped from the input.
    Turning Claude's marker into it here keeps one request for both, shaped by
    every middleware on the way and reading the turn's prompt cache.
    """
    try:
        from langchain_openai.chat_models.base import BaseChatOpenAI

        original = BaseChatOpenAI._get_request_payload
        if getattr(original, "_is_patched", False):
            return

        def _patched(self, input_, *, stop=None, **kwargs):
            payload = original(self, input_, stop=stop, **kwargs)
            extra = payload.get("extra_body")
            if isinstance(extra, dict) and "compaction" in extra and "input" in payload:
                rest = {k: v for k, v in extra.items() if k != "compaction"}
                if rest:
                    payload["extra_body"] = rest
                else:
                    payload.pop("extra_body")
                payload["input"] = [*payload["input"], {"type": "compaction_trigger"}]
            return payload

        _patched._is_patched = True
        BaseChatOpenAI._get_request_payload = _patched
    except Exception as e:
        logger.warning(f"Failed to patch langchain_openai compaction trigger: {e}")


def _patch_langchain_openai_reasoning_status():
    """Send back a streamed reasoning item as ``completed``.

    langchain-openai 1.7 builds a streamed reasoning item from its
    ``output_item.added`` event, which says ``in_progress``, and takes only the
    encrypted content from the ``done`` event, so the stored item keeps the
    stale status. OpenAI ignores it on input; Kimi Code refuses the request
    with a 400. An item being sent back has finished, whatever it was saved as.
    """
    try:
        from langchain_openai.chat_models.base import BaseChatOpenAI

        original = BaseChatOpenAI._get_request_payload
        if getattr(original, "_completes_reasoning", False):
            return

        @functools.wraps(original)
        def _patched(self, input_, *, stop=None, **kwargs):
            payload = original(self, input_, stop=stop, **kwargs)
            items = payload.get("input")
            if isinstance(items, list):
                payload["input"] = [
                    {**item, "status": "completed"}
                    if isinstance(item, dict)
                    and item.get("type") == "reasoning"
                    and item.get("status") == "in_progress"
                    else item
                    for item in items
                ]
            return payload

        _patched._completes_reasoning = True
        BaseChatOpenAI._get_request_payload = _patched
    except Exception as e:
        logger.warning(f"Failed to patch langchain_openai reasoning status: {e}")


def _patch_langchain_anthropic_header_betas():
    """Keep the client's own betas on a request that names betas.

    A request's ``betas`` replace the ``anthropic-beta`` header the client was
    built with (anthropic 0.120 sends them as that header), and
    langchain-anthropic 1.7.6 adds one there by itself for some tools, content
    and settings. Either way the header's betas would be gone from the call:
    on the OAuth route what authenticates it, and the manifest's, which let
    ``thinking.block_binding`` and turn-scoped system messages through. So
    they lead the list, ahead of the client's ``betas`` field, which a
    call-time list replaces too.
    """
    try:
        from langchain_anthropic.chat_models import ChatAnthropic

        original = ChatAnthropic._get_request_payload
        if getattr(original, "_keeps_header_betas", False):
            return

        @functools.wraps(original)
        def _patched(self, input_, *, stop=None, **kwargs):
            payload = original(self, input_, stop=stop, **kwargs)
            if payload.get("betas"):
                payload["betas"] = list(dict.fromkeys([*client_betas(self), *payload["betas"]]))
            return payload

        _patched._keeps_header_betas = True
        ChatAnthropic._get_request_payload = _patched
    except Exception as e:
        logger.warning(f"Failed to patch langchain_anthropic header betas: {e}")
