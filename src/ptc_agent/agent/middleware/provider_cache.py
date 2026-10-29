"""Shared provider prompt-cache breakpoint helpers.

The wire markers that key incremental prompt caching (Anthropic ``cache_control``;
OpenAI explicit ``prompt_cache_breakpoint``) and the str-vs-list "tag the last
text block" logic are needed by both ``SystemPromptCacheMiddleware`` (pins the
static system prefix) and ``RowCarrierMiddleware`` (pins the runtime-context
envelope at the tail). Both live here so the provider gate and the tagging are
written once.
"""

from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from langchain_openrouter import ChatOpenRouter

from src.llms.endpoints import is_official_openai_endpoint


def breakpoint_marker(model: Any) -> tuple[str, dict[str, Any]] | None:
    """Wire-format cache-breakpoint marker for models with breakpoint-keyed caching.

    Anthropic always keys reads at breakpoints; OpenAI does once a model opts into
    ``prompt_cache_options`` on the official endpoint (in "explicit" mode only
    tagged boundaries cache at all). OpenRouter forwards ``cache_control`` on
    content blocks to Anthropic, which caches nothing without one, so a Claude
    model there takes the same marker. None for everything else — automatic
    prefix matchers don't need the pin and must not see a foreign marker.
    """
    if isinstance(model, ChatAnthropic):
        return "cache_control", {"type": "ephemeral"}
    if isinstance(model, ChatOpenRouter) and model.model_name.startswith("anthropic/"):
        return "cache_control", {"type": "ephemeral"}
    if (
        isinstance(model, ChatOpenAI)
        and getattr(model, "prompt_cache_options", None) is not None
        and is_official_openai_endpoint(getattr(model, "openai_api_base", None))
    ):
        return "prompt_cache_breakpoint", {"mode": "explicit"}
    return None


def static_prefix_marker(model: Any) -> tuple[str, dict[str, Any]] | None:
    """The marker ``SystemPromptCacheMiddleware`` places on the static system prefix.

    ``breakpoint_marker`` minus ``ChatAnthropic``, whose prefix
    ``AnthropicPromptCachingMiddleware`` already tags; tagging it twice would
    spend a second breakpoint of the four Anthropic allows. The tail and baseline
    pins take ``breakpoint_marker`` directly, since they tag blocks that
    middleware never sees.
    """
    if isinstance(model, ChatAnthropic):
        return None
    return breakpoint_marker(model)


def tag_last_text_block(
    content: Any, key: str, marker: dict[str, Any]
) -> list[Any] | None:
    """New content list with ``key: marker`` on its last text block, or None.

    Tags a plain-string body as one text block; for a list body it searches
    backward for the last text-bearing block (a ``type: "text"`` dict, or a
    bare-string element promoted to text) so a non-text tail — an image or
    tool-use block a provider may reject the marker on — is never tagged.
    Returns None when no block accepts the marker — the caller then leaves the
    message unchanged (worse caching, never a malformed request).
    """
    if isinstance(content, str):
        if not content:
            return None
        return [{"type": "text", "text": content, key: dict(marker)}]
    if isinstance(content, list):
        for i in range(len(content) - 1, -1, -1):
            block = content[i]
            if isinstance(block, dict) and block.get("type") == "text":
                return [*content[:i], {**block, key: dict(marker)}, *content[i + 1:]]
            if isinstance(block, str):
                return [
                    *content[:i],
                    {"type": "text", "text": block, key: dict(marker)},
                    *content[i + 1:],
                ]
    return None
