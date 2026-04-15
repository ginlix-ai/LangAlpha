"""
Tests for src/server/database/user.py

Verifies user CRUD, upsert, preferences, and get_user_with_preferences.
"""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixture: patch get_db_connection at the user module's import location
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cursor():
    """AsyncMock cursor with execute/fetchone/fetchall."""
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.rowcount = 0
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    conn.cursor = _cursor_cm
    return conn


@pytest.fixture
def user_mock_db(mock_connection):
    """Patch get_db_connection in the user module (its import location)."""

    @asynccontextmanager
    async def _fake():
        yield mock_connection

    with patch(
        "src.server.database.user.get_db_connection",
        new=_fake,
    ):
        yield mock_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _user_row(user_id="user-1", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "user_id": user_id,
        "email": "test@example.com",
        "name": "Test User",
        "avatar_url": None,
        "timezone": "UTC",
        "locale": "en-US",
        "onboarding_completed": False,
        "personalization_completed": False,
        "has_api_key": False,
        "auth_provider": None,
        "created_at": now,
        "updated_at": now,
        "last_login_at": None,
    }
    row.update(overrides)
    return row


def _prefs_row(user_id="user-1", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "user_preference_id": str(uuid.uuid4()),
        "user_id": user_id,
        "risk_preference": {},
        "investment_preference": {},
        "agent_preference": {},
        "other_preference": {},
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


# ===========================================================================
# Tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_user_found(user_mock_db, mock_cursor):
    """get_user returns user dict when found."""
    from src.server.database.user import get_user

    row = _user_row()
    mock_cursor.fetchone.return_value = row

    result = await get_user("user-1")

    assert result is not None
    assert result["user_id"] == "user-1"
    sql = mock_cursor.execute.call_args[0][0]
    assert "FROM users" in sql
    assert "WHERE user_id = %s" in sql


@pytest.mark.asyncio
async def test_get_user_not_found(user_mock_db, mock_cursor):
    """get_user returns None when user does not exist."""
    from src.server.database.user import get_user

    mock_cursor.fetchone.return_value = None

    result = await get_user("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_create_user(user_mock_db, mock_cursor):
    """create_user inserts a new user and creates preferences row."""
    from src.server.database.user import create_user

    row = _user_row()
    # First fetchone: check existing (None); second: INSERT RETURNING
    mock_cursor.fetchone.side_effect = [None, row]

    result = await create_user("user-1", email="test@example.com", name="Test User")

    assert result["user_id"] == "user-1"
    # Verify at least 3 execute calls: check existing, insert user, insert prefs
    assert mock_cursor.execute.await_count >= 3


@pytest.mark.asyncio
async def test_create_user_already_exists(user_mock_db, mock_cursor):
    """create_user raises ValueError when user already exists."""
    from src.server.database.user import create_user

    mock_cursor.fetchone.return_value = {"user_id": "user-1"}

    with pytest.raises(ValueError, match="already exists"):
        await create_user("user-1")


@pytest.mark.asyncio
async def test_upsert_user(user_mock_db, mock_cursor):
    """upsert_user uses ON CONFLICT DO UPDATE and returns user dict."""
    from src.server.database.user import upsert_user

    row = _user_row()
    mock_cursor.fetchone.return_value = row

    result = await upsert_user("user-1", email="test@example.com")

    assert result["user_id"] == "user-1"
    sql = mock_cursor.execute.call_args[0][0]
    assert "ON CONFLICT" in sql


@pytest.mark.asyncio
async def test_find_user_by_email(user_mock_db, mock_cursor):
    """find_user_by_email returns user when email matches."""
    from src.server.database.user import find_user_by_email

    row = _user_row(email="found@example.com")
    mock_cursor.fetchone.return_value = row

    result = await find_user_by_email("found@example.com")

    assert result is not None
    assert result["email"] == "found@example.com"
    sql = mock_cursor.execute.call_args[0][0]
    assert "WHERE email = %s" in sql


@pytest.mark.asyncio
async def test_find_user_by_email_not_found(user_mock_db, mock_cursor):
    """find_user_by_email returns None when no match."""
    from src.server.database.user import find_user_by_email

    mock_cursor.fetchone.return_value = None

    result = await find_user_by_email("missing@example.com")
    assert result is None


@pytest.mark.asyncio
async def test_get_user_preferences(user_mock_db, mock_cursor):
    """get_user_preferences returns preferences dict."""
    from src.server.database.user import get_user_preferences

    row = _prefs_row()
    mock_cursor.fetchone.return_value = row

    result = await get_user_preferences("user-1")

    assert result is not None
    assert result["user_id"] == "user-1"
    sql = mock_cursor.execute.call_args[0][0]
    assert "FROM user_preferences" in sql


@pytest.mark.asyncio
async def test_get_user_preferences_not_found(user_mock_db, mock_cursor):
    """get_user_preferences returns None when no preferences exist."""
    from src.server.database.user import get_user_preferences

    mock_cursor.fetchone.return_value = None

    result = await get_user_preferences("user-1")
    assert result is None


@pytest.mark.asyncio
async def test_get_user_with_preferences(user_mock_db, mock_cursor):
    """get_user_with_preferences returns combined user + preferences dict."""
    from src.server.database.user import get_user_with_preferences

    now = datetime.now(timezone.utc)
    combined_row = {
        "user_id": "user-1",
        "email": "test@example.com",
        "name": "Test",
        "avatar_url": None,
        "timezone": "UTC",
        "locale": "en-US",
        "onboarding_completed": False,
        "personalization_completed": False,
        "has_api_key": False,
        "has_oauth_token": False,
        "auth_provider": None,
        "created_at": now,
        "updated_at": now,
        "last_login_at": None,
        "user_preference_id": "pref-1",
        "risk_preference": {"level": "high"},
        "investment_preference": {},
        "agent_preference": {},
        "other_preference": {},
        "pref_created_at": now,
        "pref_updated_at": now,
    }
    mock_cursor.fetchone.return_value = combined_row

    result = await get_user_with_preferences("user-1")

    assert result is not None
    assert "user" in result
    assert "preferences" in result
    assert result["user"]["user_id"] == "user-1"
    assert result["preferences"]["risk_preference"] == {"level": "high"}


@pytest.mark.asyncio
async def test_get_user_with_preferences_no_prefs(user_mock_db, mock_cursor):
    """get_user_with_preferences returns None preferences when no prefs row."""
    from src.server.database.user import get_user_with_preferences

    now = datetime.now(timezone.utc)
    combined_row = {
        "user_id": "user-1",
        "email": "test@example.com",
        "name": "Test",
        "avatar_url": None,
        "timezone": "UTC",
        "locale": "en-US",
        "onboarding_completed": False,
        "personalization_completed": False,
        "has_api_key": False,
        "has_oauth_token": False,
        "auth_provider": None,
        "created_at": now,
        "updated_at": now,
        "last_login_at": None,
        "user_preference_id": None,  # No preferences
        "risk_preference": None,
        "investment_preference": None,
        "agent_preference": None,
        "other_preference": None,
        "pref_created_at": None,
        "pref_updated_at": None,
    }
    mock_cursor.fetchone.return_value = combined_row

    result = await get_user_with_preferences("user-1")

    assert result is not None
    assert result["preferences"] is None


@pytest.mark.asyncio
async def test_get_user_with_preferences_user_not_found(user_mock_db, mock_cursor):
    """get_user_with_preferences returns None when user does not exist."""
    from src.server.database.user import get_user_with_preferences

    mock_cursor.fetchone.return_value = None

    result = await get_user_with_preferences("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_delete_user_preferences(user_mock_db, mock_cursor):
    """delete_user_preferences deletes row and returns True."""
    from src.server.database.user import delete_user_preferences

    mock_cursor.rowcount = 1

    result = await delete_user_preferences("user-1")

    assert result is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "DELETE FROM user_preferences" in sql


@pytest.mark.asyncio
async def test_delete_user_preferences_not_found(user_mock_db, mock_cursor):
    """delete_user_preferences returns False when no row exists."""
    from src.server.database.user import delete_user_preferences

    mock_cursor.rowcount = 0

    result = await delete_user_preferences("user-1")
    assert result is False


# ===========================================================================
# get_user_preferences cache behavior
# ===========================================================================


def _make_cache(enabled=True, get_return=None):
    """Create a mock RedisCacheClient with async get/set/delete."""
    cache = MagicMock()
    cache.enabled = enabled
    cache.client = AsyncMock()
    cache.client.get = AsyncMock(return_value=get_return)
    cache.client.set = AsyncMock()
    cache.client.delete = AsyncMock()
    return cache


@pytest.mark.asyncio
async def test_get_user_preferences_cache_hit():
    """Cache returns JSON bytes -> parsed dict returned without DB call."""
    prefs = _prefs_row()
    cached_bytes = json.dumps(prefs, default=str).encode()
    cache = _make_cache(get_return=cached_bytes)

    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_db():
        yield conn

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch(
        "src.server.database.user.get_db_connection", new=_fake_db
    ):
        from src.server.database.user import get_user_preferences

        result = await get_user_preferences("user-1")

    assert result is not None
    assert result["user_id"] == "user-1"
    cache.client.get.assert_awaited_once_with("user_prefs:user-1")
    # DB should NOT have been called
    cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_user_preferences_cache_miss():
    """Cache miss -> DB queried, cache.set called with JSON, returns dict."""
    cache = _make_cache(get_return=None)
    prefs = _prefs_row()

    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=prefs)
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_db():
        yield conn

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch(
        "src.server.database.user.get_db_connection", new=_fake_db
    ):
        from src.server.database.user import get_user_preferences

        result = await get_user_preferences("user-1")

    assert result is not None
    assert result["user_id"] == "user-1"
    # DB was queried
    cursor.execute.assert_awaited_once()
    # Cache was populated with JSON
    cache.client.set.assert_awaited_once()
    set_call = cache.client.set.call_args
    assert set_call[0][0] == "user_prefs:user-1"
    # The second positional arg is JSON; verify it round-trips
    stored = json.loads(set_call[0][1])
    assert stored["user_id"] == "user-1"
    assert set_call[1]["ex"] == 86400


@pytest.mark.asyncio
async def test_invalidate_user_prefs_cache():
    """invalidate_user_prefs_cache deletes the correct cache key."""
    cache = _make_cache()

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        from src.server.database.user import invalidate_user_prefs_cache

        await invalidate_user_prefs_cache("user1")

    cache.client.delete.assert_awaited_once_with("user_prefs:user1")
