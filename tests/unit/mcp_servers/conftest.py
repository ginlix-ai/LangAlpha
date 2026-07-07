"""Shared assertion helpers for the market-data MCP server unit tests.

The eight per-server suites all exercise the same agent-facing envelope
(AGENT_CONTRACT.md / mcp_servers/_envelope.py): a success envelope carrying
``source``/``count``/``data`` plus optional ``symbol``/``interval``/
``currency``/``timezone``, or an error envelope ``{error: <code>, detail:
<sanitized>}``. These two helpers assert the shared invariants so each test
only spells out its own tool-specific payload.

Plain functions (imported via ``from .conftest import ...``), matching the
helper idiom in tests/regression/market_data/conftest.py.
"""

from __future__ import annotations

from typing import Any


def assert_ok_envelope(
    result: dict[str, Any],
    *,
    symbol: str | None = None,
    source: str | None = None,
    currency: str | None = None,
    timezone: str | None = None,
    interval: str | None = None,
    count: int | None = None,
    extra_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Assert the standard success-envelope invariants plus any passed fields.

    Always checks: no ``error`` key, ``data`` present, and — when ``data`` is a
    list — ``count == len(data)``. Each keyword given is checked for equality;
    passing ``count`` also pins the explicit total (e.g. for dict-of-lists
    payloads). Names in ``extra_keys`` are asserted present.
    """
    assert "error" not in result, f"unexpected error envelope: {result!r}"
    assert "data" in result, f"missing data key: {result!r}"
    data = result["data"]
    if isinstance(data, list):
        assert result["count"] == len(data), (
            f"count {result['count']} != len(data) {len(data)}"
        )
    if count is not None:
        assert result["count"] == count
    if symbol is not None:
        assert result["symbol"] == symbol
    if source is not None:
        assert result["source"] == source
    if currency is not None:
        assert result["currency"] == currency
    if timezone is not None:
        assert result["timezone"] == timezone
    if interval is not None:
        assert result["interval"] == interval
    for key in extra_keys:
        assert key in result, f"expected key {key!r} in {result!r}"
    return result


def assert_error(
    result: dict[str, Any],
    code: str,
    *,
    symbol: str | None = None,
    detail_contains: str | None = None,
    detail_excludes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Assert the standard error envelope ``{error: code, detail: <str>}``.

    Optionally checks the echoed ``symbol``, that ``detail_contains`` is a
    substring of ``detail``, and that every string in ``detail_excludes`` is
    absent from ``detail`` (the sanitization / no-leak check).
    """
    assert result["error"] == code, f"expected error {code!r}, got {result!r}"
    assert "detail" in result, f"missing detail key: {result!r}"
    detail = result["detail"]
    if symbol is not None:
        assert result["symbol"] == symbol
    if detail_contains is not None:
        assert detail_contains in detail
    for excluded in detail_excludes:
        assert excluded not in detail, f"{excluded!r} leaked into detail: {detail!r}"
    return result
