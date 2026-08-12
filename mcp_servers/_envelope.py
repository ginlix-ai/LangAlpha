"""Shared agent-facing envelope helpers for market-data MCP servers.

Implements the success/error envelope and interval vocabulary defined in
AGENT_CONTRACT.md. Pure stdlib so every server can import it regardless of
which data clients it loads.
"""

from __future__ import annotations

import re
from typing import Any, Optional

# Canonical interval vocabulary (AGENT_CONTRACT.md).
CANONICAL_INTERVALS = (
    "1min",
    "5min",
    "15min",
    "30min",
    "1hour",
    "4hour",
    "1day",
    "1week",
    "1month",
)

# Provider-native and legacy spellings accepted as input.
_INTERVAL_ALIASES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "60m": "1hour",
    "1h": "1hour",
    "4h": "4hour",
    "1d": "1day",
    "daily": "1day",
    "1w": "1week",
    "1wk": "1week",
    "weekly": "1week",
    "1mo": "1month",
    "monthly": "1month",
}

# Machine-readable error codes (AGENT_CONTRACT.md).
ERROR_CODES = frozenset(
    {
        "invalid_argument",
        "unsupported_interval",
        "not_found",
        "auth_failed",
        "rate_limited",
        "upstream_error",
        "client_unavailable",
    }
)


def normalize_interval(value: str) -> Optional[str]:
    """Map an interval spelling to the canonical vocab; None if unrecognized."""
    lowered = value.strip().lower()
    if lowered in CANONICAL_INTERVALS:
        return lowered
    return _INTERVAL_ALIASES.get(lowered)


def _auto_count(data: Any) -> int:
    """Total records in a payload: list length, dict-of-all-lists sum.

    Any other shape (dict of dicts, mixed dict, scalar) counts as 1 — callers
    with multi-record payloads in those shapes must pass ``count=`` explicitly.
    """
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        if all(isinstance(v, list) for v in data.values()):
            return sum(len(v) for v in data.values())  # {} counts 0
        return 1
    return 1


def make_response(
    data: Any,
    *,
    source: str,
    symbol: Optional[str] = None,
    interval: Optional[str] = None,
    currency: Optional[str] = None,
    timezone: Optional[str] = None,
    count: Optional[int] = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build the standard success envelope around a `data` payload."""
    resp: dict[str, Any] = {}
    if symbol is not None:
        resp["symbol"] = symbol
    if interval is not None:
        resp["interval"] = interval
    if currency is not None:
        resp["currency"] = currency
    if timezone is not None:
        resp["timezone"] = timezone
    resp["count"] = _auto_count(data) if count is None else count
    resp["data"] = data
    resp["source"] = source
    resp.update(extra)
    return resp


def make_error(code: str, detail: str, **extra: Any) -> dict[str, Any]:
    """Build the standard error envelope; off-contract codes coerce to upstream_error."""
    if code not in ERROR_CODES:
        code = "upstream_error"
    return {"error": code, "detail": detail, **extra}


def error_from_upstream(
    detail: str, *, status: Optional[int] = None, **context: Any
) -> dict[str, Any]:
    """Error envelope from an upstream failure; maps HTTP status → contract code.

    ``status`` wins when given; otherwise a "not configured" detail maps to
    ``client_unavailable``, else the last embedded ``(NNN)`` in ``detail`` is used.
    """
    if status is None:
        if "not configured" in detail.lower():
            return make_error("client_unavailable", detail, **context)
        matches = re.findall(r"\((\d{3})\)", detail)
        status = int(matches[-1]) if matches else None
    if status == 404:
        code = "not_found"
    elif status in (401, 403):
        code = "auth_failed"
    elif status == 429:
        code = "rate_limited"
    elif status is not None and 400 <= status < 500:
        code = "invalid_argument"
    else:
        code = "upstream_error"
    return make_error(code, detail, **context)


def error_from_exception(
    exc: Exception, fallback_detail: str, **context: Any
) -> dict[str, Any]:
    """Error envelope from an upstream client exception.

    Only exceptions that declare a sanitized message via a ``status_code``
    attribute (e.g. ``FMPRequestError``) contribute ``str(exc)`` to the
    detail; anything else gets ``fallback_detail`` so raw exception text
    (URLs, credentials) never reaches the agent.
    """
    if hasattr(exc, "status_code"):
        return error_from_upstream(
            str(exc), status=getattr(exc, "status_code", None), **context
        )
    return make_error("upstream_error", fallback_detail, **context)
