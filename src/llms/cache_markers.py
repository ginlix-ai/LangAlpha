"""How long an Anthropic request's cache entries live."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: Each ttl a cache marker may ask for, in seconds.
_TTL_SECONDS = {"5m": 300, "1h": 3600}


def ttl_seconds(ttl: str) -> int:
    return _TTL_SECONDS[ttl]


def written_ttl_seconds(usage: Mapping[str, Any] | None) -> int | None:
    """How long the cache entries an answer reports writing live: five
    minutes when it wrote both, since that part expires first; None when it
    wrote none."""
    details = (usage or {}).get("input_token_details") or {}
    if details.get("ephemeral_5m_input_tokens"):
        return ttl_seconds("5m")
    if details.get("ephemeral_1h_input_tokens"):
        return ttl_seconds("1h")
    return None


def route_ttl_seconds(model: Any) -> int:
    """How long the entries a call to ``model`` writes live: the ttl its
    client puts on every marker (``cache_ttl``), else Anthropic's default
    five minutes."""
    return ttl_seconds(getattr(getattr(model, "bound", model), "cache_ttl", "5m"))
