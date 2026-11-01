"""Where an Anthropic request's cache markers sit, and how long they live.

The markers are placed by middleware every Anthropic route shares, so a
policy that belongs to one route or to one call is applied to the whole
request at once, as the client builds it: the subscription client's hour
(``with_ttl``), and the markers a compaction call needs (``cache_scope``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Literal

#: The keys a cache marker can sit under: the request's system, tools and
#: messages, a message's content, and a block's nested content or document
#: source. Nothing else is walked, so a schema, a tool's arguments or examples,
#: and a map keyed by tool name keep a ``cache_control`` key as written.
_MARKER_PATHS = frozenset({"system", "tools", "messages", "content", "source"})

#: Each ttl a cache marker may ask for, in seconds.
_TTL_SECONDS = {"5m": 300, "1h": 3600}

#: ``last_tool``: the tools' marker alone. A compaction on an expired cache
#: reads the history once at the base rate, and the call after it sends the
#: summary in place of that history, so writing the history would buy
#: nothing; the tools prefix is what that call, and every thread with the
#: same tools, reads back.
CacheScope = Literal["last_tool"]

_scope: ContextVar[CacheScope | None] = ContextVar("cache_marker_scope", default=None)


@contextmanager
def cache_scope(scope: CacheScope | None) -> Iterator[None]:
    """Send the model calls made inside with the markers ``scope`` names;
    None leaves them where the middleware put them."""
    token = _scope.set(scope)
    try:
        yield
    finally:
        _scope.reset(token)


def scoped(payload: dict[str, Any]) -> dict[str, Any]:
    """``payload`` with its markers where the call's scope puts them, as a
    copy; the payload itself, untouched, outside any scope."""
    if _scope.get() != "last_tool":
        return payload
    sent = {
        key: value if key == "tools" else _rewrite(value, lambda _marker: None)
        for key, value in payload.items()
        if key != "cache_control"
    }
    tools = sent.get("tools") or []
    if any(tool.get("cache_control") for tool in tools):
        return sent
    # Where ToolSurface puts it: the API refuses a marker on a deferred tool,
    # which sits outside the cached prefix.
    last = next((i for i in range(len(tools) - 1, -1, -1) if not tools[i].get("defer_loading")), None)
    if last is not None:
        sent["tools"] = [*tools[:last], {**tools[last], "cache_control": {"type": "ephemeral"}}, *tools[last + 1 :]]
    return sent


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


def with_ttl(payload: dict[str, Any], ttl: str) -> dict[str, Any]:
    """``payload`` with every cache marker asking for ``ttl``, as a copy.
    Anthropic refuses a request whose lifetimes are out of order, so a
    route's lifetime goes on every marker at once."""
    return _rewrite(payload, lambda marker: {**marker, "ttl": ttl})


def _rewrite(value: Any, marker: Callable[[dict[str, Any]], dict[str, Any] | None]) -> Any:
    """``value`` with each cache marker under ``_MARKER_PATHS`` replaced by
    ``marker`` of it, or dropped where that is None, as a copy: the payload
    shares its marker dicts with the request state, which is not written to."""
    if isinstance(value, list):
        return [_rewrite(item, marker) for item in value]
    if not isinstance(value, dict):
        return value
    copied = {key: _rewrite(item, marker) if key in _MARKER_PATHS else item for key, item in value.items()}
    current = copied.get("cache_control")
    if isinstance(current, dict):
        replaced = marker(current)
        if replaced is None:
            del copied["cache_control"]
        else:
            copied["cache_control"] = replaced
    return copied
