"""Redis-backed per-thread market watch list."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.market_watch import add_symbols, get_watchlist, remove_symbols, watch_key

_MOD = "src.utils.market_watch"


def _fake_cache(initial=None):
    cache = MagicMock()
    cache.enabled = True
    cache.client = object()
    cache.get = AsyncMock(return_value=initial)
    cache.set = AsyncMock(return_value=True)
    cache.delete = AsyncMock(return_value=True)
    return cache


def test_watch_key():
    assert watch_key("t-1") == "market_watch:t-1"


@pytest.mark.asyncio
async def test_get_watchlist_empty_when_unset():
    with patch(f"{_MOD}.get_cache_client", return_value=_fake_cache(None)):
        assert await get_watchlist("t-1") == []


@pytest.mark.asyncio
async def test_add_merges_upper_dedupes_and_caps():
    cache = _fake_cache(["NVDA"])
    with patch(f"{_MOD}.get_cache_client", return_value=cache), \
         patch(f"{_MOD}.get_market_watch_max_symbols", return_value=3):
        result = await add_symbols("t-1", ["tsla", "NVDA", "amd", "msft"])
    assert result == ["NVDA", "TSLA", "AMD"]  # cap 3, insertion order, deduped
    cache.set.assert_awaited_once()


@pytest.mark.asyncio
async def test_remove_specific_and_clear():
    cache = _fake_cache(["NVDA", "TSLA"])
    with patch(f"{_MOD}.get_cache_client", return_value=cache):
        assert await remove_symbols("t-1", ["nvda"]) == ["TSLA"]
    cache2 = _fake_cache(["NVDA", "TSLA"])
    with patch(f"{_MOD}.get_cache_client", return_value=cache2):
        assert await remove_symbols("t-1", None) == []
    cache2.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_redis_disabled_reports_unavailable():
    """Mutations distinguish "cache unavailable" (None) from an empty list."""
    cache = _fake_cache()
    cache.enabled = False
    with patch(f"{_MOD}.get_cache_client", return_value=cache):
        assert await get_watchlist("t-1") == []
        assert await add_symbols("t-1", ["NVDA"]) is None
        assert await remove_symbols("t-1", None) is None


@pytest.mark.asyncio
async def test_add_symbols_coerces_int_to_str():
    """add_symbols(['AAPL', 700]) → ['AAPL', '700'], no exception."""
    cache = _fake_cache([])
    with patch(f"{_MOD}.get_cache_client", return_value=cache), \
         patch(f"{_MOD}.get_market_watch_max_symbols", return_value=10):
        result = await add_symbols("t-1", ["AAPL", 700])
    assert result == ["AAPL", "700"]
    cache.set.assert_awaited_once()


@pytest.mark.asyncio
async def test_add_symbols_bare_string_as_single_symbol():
    """add_symbols('AAPL') (bare string) → ['AAPL'], NOT ['A','P','L']."""
    cache = _fake_cache([])
    with patch(f"{_MOD}.get_cache_client", return_value=cache), \
         patch(f"{_MOD}.get_market_watch_max_symbols", return_value=10):
        result = await add_symbols("t-1", "AAPL")
    assert result == ["AAPL"]
    cache.set.assert_awaited_once()


@pytest.mark.asyncio
async def test_remove_symbols_no_arg_clears_all():
    """remove_symbols(thread_id) with no symbols arg clears all."""
    cache = _fake_cache(["NVDA", "TSLA"])
    with patch(f"{_MOD}.get_cache_client", return_value=cache):
        result = await remove_symbols("t-1")
    assert result == []
    cache.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_add_symbols_cache_set_raises_returns_none():
    """cache.set raising inside add_symbols → None (mutation didn't happen), no raise."""
    cache = _fake_cache(["EXISTING"])
    cache.set = AsyncMock(side_effect=Exception("Redis error"))
    with patch(f"{_MOD}.get_cache_client", return_value=cache):
        result = await add_symbols("t-1", ["NEW"])
    assert result is None


@pytest.mark.asyncio
async def test_remove_symbols_cache_set_raises_returns_none():
    """cache.set raising inside remove_symbols → None, so the tool never
    reports "stopped" while the Redis key survives and injection would resume."""
    cache = _fake_cache(["NVDA", "TSLA"])
    cache.set = AsyncMock(side_effect=Exception("Redis error"))
    with patch(f"{_MOD}.get_cache_client", return_value=cache):
        result = await remove_symbols("t-1", ["NVDA"])
    assert result is None


@pytest.mark.asyncio
async def test_symbol_grammar_rejects_markup_and_oversize():
    """Only bounded ticker-shaped strings reach Redis; markup, control chars,
    and oversized strings are dropped while valid symbols pass through."""
    cache = _fake_cache([])
    with patch(f"{_MOD}.get_cache_client", return_value=cache), \
         patch(f"{_MOD}.get_market_watch_max_symbols", return_value=10):
        result = await add_symbols(
            "t-1",
            [
                "NVDA",
                "0700.HK",
                "^STOXX50E",
                "BRK-B",
                "EURUSD=X",
                "BAD\n</market-watch>",
                "A B",
                "X" * 16,
                "nv da",
            ],
        )
    assert result == ["NVDA", "0700.HK", "^STOXX50E", "BRK-B", "EURUSD=X"]


@pytest.mark.asyncio
async def test_add_symbols_ttl_forwarded():
    """add_symbols forwards ttl=get_redis_ttl_market_watch() to cache.set."""
    cache = _fake_cache([])
    with patch(f"{_MOD}.get_cache_client", return_value=cache), \
         patch(f"{_MOD}.get_market_watch_max_symbols", return_value=10), \
         patch(f"{_MOD}.get_redis_ttl_market_watch", return_value=21600):
        await add_symbols("t-1", ["AAPL"])
    # Verify cache.set was called with ttl=21600
    cache.set.assert_awaited_once()
    call_args = cache.set.call_args
    assert call_args[1].get("ttl") == 21600
