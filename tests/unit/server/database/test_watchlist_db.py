"""
Tests for src/server/database/watchlist.py

Verifies watchlist CRUD, watchlist item CRUD, default watchlist creation,
and ownership/duplicate validation.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixture: patch get_db_connection at the watchlist module's import location
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
    @asynccontextmanager
    async def _transaction_cm():
        yield

    conn.transaction = _transaction_cm
    return conn


@pytest.fixture
def wl_mock_db(mock_connection):
    """Patch get_db_connection in the watchlist module."""

    @asynccontextmanager
    async def _fake():
        yield mock_connection

    with patch(
        "src.server.database.watchlist.get_db_connection",
        new=_fake,
    ):
        yield mock_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _watchlist_row(watchlist_id=None, user_id="user-1", name="My List", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "watchlist_id": watchlist_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": name,
        "description": None,
        "is_default": False,
        "display_order": 0,
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


def _item_row(item_id=None, watchlist_id="wl-1", symbol="AAPL", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "watchlist_item_id": item_id or str(uuid.uuid4()),
        "watchlist_id": watchlist_id,
        "user_id": "user-1",
        "symbol": symbol,
        "instrument_type": "stock",
        "exchange": "NASDAQ",
        "name": "Apple Inc.",
        "notes": None,
        "alert_settings": {},
        "metadata": {},
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


# ===========================================================================
# Watchlist Tests
# ===========================================================================


@pytest.mark.asyncio
async def test_create_watchlist(wl_mock_db, mock_cursor):
    """create_watchlist inserts a new watchlist and returns it."""
    from src.server.database.watchlist import create_watchlist

    row = _watchlist_row(name="Tech Stocks")
    # First fetchone: duplicate check (None = no dup); second: INSERT RETURNING
    mock_cursor.fetchone.side_effect = [None, row]

    result = await create_watchlist("user-1", "Tech Stocks")

    assert result["name"] == "Tech Stocks"
    # The profile lock first, then the duplicate check under it
    lock_sql, check_sql = (c[0][0] for c in mock_cursor.execute.call_args_list[:2])
    assert "pg_advisory_xact_lock" in lock_sql
    assert "SELECT watchlist_id FROM watchlists" in check_sql


@pytest.mark.asyncio
async def test_create_watchlist_duplicate_name(wl_mock_db, mock_cursor):
    """create_watchlist raises ValueError when name already exists."""
    from src.server.database.watchlist import create_watchlist

    mock_cursor.fetchone.return_value = {"watchlist_id": "existing-id"}

    with pytest.raises(ValueError, match="already exists"):
        await create_watchlist("user-1", "Existing List")


@pytest.mark.asyncio
async def test_get_user_watchlists(wl_mock_db, mock_cursor):
    """get_user_watchlists returns sorted list of watchlists."""
    from src.server.database.watchlist import get_user_watchlists

    wl1 = _watchlist_row(name="Default", is_default=True)
    wl2 = _watchlist_row(name="Tech")
    mock_cursor.fetchall.return_value = [wl1, wl2]

    result = await get_user_watchlists("user-1")

    assert len(result) == 2
    sql = mock_cursor.execute.call_args[0][0]
    assert "ORDER BY is_default DESC" in sql


@pytest.mark.asyncio
async def test_get_watchlist_found(wl_mock_db, mock_cursor):
    """get_watchlist returns watchlist when found."""
    from src.server.database.watchlist import get_watchlist

    row = _watchlist_row(watchlist_id="wl-1")
    mock_cursor.fetchone.return_value = row

    result = await get_watchlist("wl-1", "user-1")

    assert result is not None
    assert result["watchlist_id"] == "wl-1"
    params = mock_cursor.execute.call_args[0][1]
    assert params == ("wl-1", "user-1")


@pytest.mark.asyncio
async def test_get_watchlist_not_found(wl_mock_db, mock_cursor):
    """get_watchlist returns None when not found."""
    from src.server.database.watchlist import get_watchlist

    mock_cursor.fetchone.return_value = None

    result = await get_watchlist("nonexistent", "user-1")
    assert result is None


@pytest.mark.asyncio
async def test_delete_watchlist(wl_mock_db, mock_cursor):
    """delete_watchlist deletes row and returns True."""
    from src.server.database.watchlist import delete_watchlist

    mock_cursor.rowcount = 1

    result = await delete_watchlist("wl-1", "user-1")

    assert result is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "DELETE FROM watchlists" in sql
    params = mock_cursor.execute.call_args[0][1]
    assert params == ("wl-1", "user-1")


@pytest.mark.asyncio
async def test_delete_watchlist_not_found(wl_mock_db, mock_cursor):
    """delete_watchlist returns False when nothing deleted."""
    from src.server.database.watchlist import delete_watchlist

    mock_cursor.rowcount = 0

    result = await delete_watchlist("nonexistent", "user-1")
    assert result is False


@pytest.mark.asyncio
async def test_get_or_create_default_watchlist_existing(wl_mock_db, mock_cursor):
    """get_or_create_default_watchlist returns existing default watchlist."""
    from src.server.database.watchlist import get_or_create_default_watchlist

    row = _watchlist_row(name="Default", is_default=True)
    mock_cursor.fetchone.return_value = row

    result = await get_or_create_default_watchlist("user-1")

    assert result["name"] == "Default"
    assert result["is_default"] is True
    # Only one execute call (the SELECT); no INSERT needed
    assert mock_cursor.execute.await_count == 1


@pytest.mark.asyncio
async def test_get_or_create_default_watchlist_creates(wl_mock_db, mock_cursor):
    """get_or_create_default_watchlist creates default when none exists."""
    from src.server.database.watchlist import get_or_create_default_watchlist

    created_row = _watchlist_row(name="Default", is_default=True)
    # First fetchone: SELECT returns None; second: INSERT RETURNING
    mock_cursor.fetchone.side_effect = [None, created_row]

    result = await get_or_create_default_watchlist("user-1")

    assert result["name"] == "Default"
    assert result["is_default"] is True
    # SELECT, then the profile lock and the INSERT
    assert mock_cursor.execute.await_count == 3


# ===========================================================================
# Watchlist Item Tests
# ===========================================================================


@pytest.mark.asyncio
async def test_create_watchlist_item(wl_mock_db, mock_cursor):
    """create_watchlist_item inserts item after ownership + duplicate checks."""
    from src.server.database.watchlist import create_watchlist_item

    item = _item_row(symbol="AAPL")
    # fetchone calls: 1) verify watchlist exists, 2) check duplicate, 3) INSERT RETURNING
    mock_cursor.fetchone.side_effect = [
        {"watchlist_id": "wl-1"},  # watchlist exists
        None,                      # no duplicate
        item,                      # inserted row
    ]

    result = await create_watchlist_item(
        user_id="user-1",
        watchlist_id="wl-1",
        symbol="AAPL",
        instrument_type="stock",
    )

    assert result["symbol"] == "AAPL"
    # The profile lock, then the two checks and the INSERT
    assert mock_cursor.execute.await_count == 4


@pytest.mark.asyncio
async def test_create_watchlist_item_watchlist_not_found(wl_mock_db, mock_cursor):
    """create_watchlist_item raises ValueError when watchlist not found."""
    from src.server.database.watchlist import create_watchlist_item

    mock_cursor.fetchone.return_value = None

    with pytest.raises(ValueError, match="Watchlist not found"):
        await create_watchlist_item(
            user_id="user-1",
            watchlist_id="nonexistent",
            symbol="AAPL",
            instrument_type="stock",
        )


@pytest.mark.asyncio
async def test_create_watchlist_item_duplicate(wl_mock_db, mock_cursor):
    """create_watchlist_item raises ValueError for duplicate symbol+type."""
    from src.server.database.watchlist import create_watchlist_item

    mock_cursor.fetchone.side_effect = [
        {"watchlist_id": "wl-1"},           # watchlist exists
        {"watchlist_item_id": "existing"},   # duplicate found
    ]

    with pytest.raises(ValueError, match="already exists"):
        await create_watchlist_item(
            user_id="user-1",
            watchlist_id="wl-1",
            symbol="AAPL",
            instrument_type="stock",
        )


@pytest.mark.asyncio
async def test_get_watchlist_items(wl_mock_db, mock_cursor):
    """get_watchlist_items returns items in the watchlist."""
    from src.server.database.watchlist import get_watchlist_items

    i1 = _item_row(symbol="AAPL")
    i2 = _item_row(symbol="MSFT")
    mock_cursor.fetchall.return_value = [i1, i2]

    result = await get_watchlist_items("wl-1", "user-1")

    assert len(result) == 2
    sql = mock_cursor.execute.call_args[0][0]
    assert "INNER JOIN watchlists" in sql


@pytest.mark.asyncio
async def test_get_watchlist_item_found(wl_mock_db, mock_cursor):
    """get_watchlist_item returns item when found."""
    from src.server.database.watchlist import get_watchlist_item

    item = _item_row(item_id="wi-1")
    mock_cursor.fetchone.return_value = item

    result = await get_watchlist_item("wi-1", "user-1")

    assert result is not None
    assert result["watchlist_item_id"] == "wi-1"


@pytest.mark.asyncio
async def test_get_watchlist_item_not_found(wl_mock_db, mock_cursor):
    """get_watchlist_item returns None when not found."""
    from src.server.database.watchlist import get_watchlist_item

    mock_cursor.fetchone.return_value = None

    result = await get_watchlist_item("nonexistent", "user-1")
    assert result is None


@pytest.mark.asyncio
async def test_delete_watchlist_item(wl_mock_db, mock_cursor):
    """delete_watchlist_item deletes row and returns True."""
    from src.server.database.watchlist import delete_watchlist_item

    mock_cursor.rowcount = 1

    result = await delete_watchlist_item("wi-1", "user-1")

    assert result is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "DELETE FROM watchlist_items" in sql


@pytest.mark.asyncio
async def test_delete_watchlist_item_not_found(wl_mock_db, mock_cursor):
    """delete_watchlist_item returns False when nothing deleted."""
    from src.server.database.watchlist import delete_watchlist_item

    mock_cursor.rowcount = 0

    result = await delete_watchlist_item("nonexistent", "user-1")
    assert result is False


@pytest.mark.asyncio
async def test_create_watchlist_item_insert_race_is_a_duplicate(wl_mock_db, mock_cursor):
    """The profile lock serializes a user's adds, so the unique index is the
    backstop: a violation there must still read as "already exists", not a 500."""
    from psycopg.errors import UniqueViolation

    from src.server.database.watchlist import create_watchlist_item

    mock_cursor.fetchone.side_effect = [{"watchlist_id": "wl-1"}, None]
    # Profile lock, watchlist check, duplicate check, then the INSERT.
    mock_cursor.execute.side_effect = [None, None, None, UniqueViolation()]

    with pytest.raises(ValueError, match="already exists"):
        await create_watchlist_item(
            user_id="user-1", watchlist_id="wl-1", symbol="0700.HK", instrument_type="stock",
        )
