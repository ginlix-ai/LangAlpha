"""Shared provider prompt-cache breakpoint helpers.

The wire markers that key incremental prompt caching (Anthropic ``cache_control``;
OpenAI explicit ``prompt_cache_breakpoint``) and the str-vs-list "tag the last
text block" logic are needed by both ``OpenAIPromptCachingMiddleware`` (pins the
static system prefix) and ``MarketWatchMiddleware`` (pins the last durable
message so its ephemeral tail doesn't defeat caching). Both live here so the
provider gate and the tagging are written once.
"""

from typing import Any

from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI

from src.llms.endpoints import is_official_openai_endpoint


def breakpoint_marker(model: Any) -> tuple[str, dict[str, Any]] | None:
    """Wire-format cache-breakpoint marker for models with breakpoint-keyed caching.

    Anthropic always keys reads at breakpoints; OpenAI does once a model opts into
    ``prompt_cache_options`` on the official endpoint (in "explicit" mode only
    tagged boundaries cache at all). None for everything else — automatic prefix
    matchers don't need the pin and must not see a foreign marker.
    """
    if isinstance(model, ChatAnthropic):
        return "cache_control", {"type": "ephemeral"}
    if (
        isinstance(model, ChatOpenAI)
        and getattr(model, "prompt_cache_options", None) is not None
        and is_official_openai_endpoint(getattr(model, "openai_api_base", None))
    ):
        return "prompt_cache_breakpoint", {"mode": "explicit"}
    return None


def tag_last_text_block(
    content: Any, key: str, marker: dict[str, Any]
) -> list[Any] | None:
    """New content list with ``key: marker`` on its last text block, or None.

    Tags a plain-string body as one text block, or the last block of a list body
    (a dict block keeps its type; a bare-string element is promoted to text).
    Returns None when there is no text-bearing tail to tag — the caller then
    leaves the message unchanged (worse caching, never a malformed request).
    """
    if isinstance(content, str):
        if not content:
            return None
        return [{"type": "text", "text": content, key: dict(marker)}]
    if isinstance(content, list):
        if not content:
            return None
        last = content[-1]
        if isinstance(last, dict):
            return [*content[:-1], {**last, key: dict(marker)}]
        if isinstance(last, str):
            return [*content[:-1], {"type": "text", "text": last, key: dict(marker)}]
    return None
