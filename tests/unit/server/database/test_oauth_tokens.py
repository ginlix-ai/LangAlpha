"""
Tests for src/server/database/oauth_tokens.py

Verifies Redis cache behavior for has_any_oauth_token and invalidate_oauth_active_cache.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cache(enabled=True, get_return=None):
    """Create a mock RedisCacheClient with async get/set/delete."""
    cache = MagicMock()
    cache.enabled = enabled
    cache.client = AsyncMock()
    cache.client.get = AsyncMock(return_value=get_return)
    cache.client.set = AsyncMock()
    cache.client.delete = AsyncMock()
    return cache


def _make_db_fixtures(fetchone_return=None):
    """Create mock cursor, connection, and get_db_connection patch target."""
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=fetchone_return)

    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_db():
        yield conn

    return cursor, conn, _fake_db


# ===========================================================================
# Tests — has_any_oauth_token cache
# ===========================================================================


@pytest.mark.asyncio
async def test_has_any_oauth_token_cache_hit():
    """Cache returns b"1" -> True without any DB call."""
    cache = _make_cache(get_return=b"1")
    cursor, conn, fake_db = _make_db_fixtures()

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch(
        "src.server.database.oauth_tokens.get_db_connection", new=fake_db
    ):
        from src.server.database.oauth_tokens import has_any_oauth_token

        result = await has_any_oauth_token("user-1")

    assert result is True
    cache.client.get.assert_awaited_once_with("oauth_active:user-1")
    # DB should NOT have been called
    cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_has_any_oauth_token_cache_miss():
    """Cache miss -> DB queried, cache.set called with b"1", returns True."""
    cache = _make_cache(get_return=None)
    # DB returns a row (token exists)
    cursor, conn, fake_db = _make_db_fixtures(fetchone_return={"?column?": 1})

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch(
        "src.server.database.oauth_tokens.get_db_connection", new=fake_db
    ):
        from src.server.database.oauth_tokens import has_any_oauth_token

        result = await has_any_oauth_token("user-1")

    assert result is True
    # DB was queried
    cursor.execute.assert_awaited_once()
    # Cache was populated
    cache.client.set.assert_awaited_once_with(
        "oauth_active:user-1", b"1", ex=86400
    )


@pytest.mark.asyncio
async def test_invalidate_oauth_active_cache():
    """invalidate_oauth_active_cache deletes the correct cache key."""
    cache = _make_cache()

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        from src.server.database.oauth_tokens import invalidate_oauth_active_cache

        await invalidate_oauth_active_cache("user1")

    cache.client.delete.assert_awaited_once_with("oauth_active:user1")
