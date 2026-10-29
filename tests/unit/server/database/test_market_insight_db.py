"""
Tests for src/server/database/market_insight.py

Verifies insight lifecycle (create, complete, fail), lookups (by ID, today's
insights, generating check, recent completed dedup). The system insight claim
is a race between workers, so it is tested against real Postgres in
tests/integration/test_market_insight_claim_db.py.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixture: patch get_db_connection at the market_insight module's import path
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
def mi_mock_db(mock_connection):
    """Patch get_db_connection in the market_insight module."""

    @asynccontextmanager
    async def _fake():
        yield mock_connection

    with patch(
        "src.server.database.market_insight.get_db_connection",
        new=_fake,
    ):
        yield mock_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insight_row(
    market_insight_id=None,
    user_id=None,
    type="daily_brief",
    status="completed",
    headline="Market rallies",
    summary="Stocks up broadly",
    content=None,
    topics=None,
    sources=None,
    model="gpt-4o",
    error_message=None,
    generation_time_ms=1200,
    metadata=None,
    created_at=None,
    completed_at=None,
    **overrides,
):
    now = datetime.now(timezone.utc)
    row = {
        "market_insight_id": market_insight_id or str(uuid.uuid4()),
        "user_id": user_id,
        "type": type,
        "status": status,
        "headline": headline,
        "summary": summary,
        "content": content or [{"section": "overview", "text": "..."}],
        "topics": topics or ["equities"],
        "sources": sources or ["reuters"],
        "model": model,
        "error_message": error_message,
        "generation_time_ms": generation_time_ms,
        "metadata": metadata,
        "created_at": created_at or now,
        "completed_at": completed_at or now,
    }
    row.update(overrides)
    return row


def _card_row(
    market_insight_id=None,
    type="daily_brief",
    headline="Market rallies",
    summary="Stocks up broadly",
    topics=None,
    model="gpt-4o",
    created_at=None,
    completed_at=None,
    **overrides,
):
    now = datetime.now(timezone.utc)
    row = {
        "market_insight_id": market_insight_id or str(uuid.uuid4()),
        "type": type,
        "headline": headline,
        "summary": summary,
        "topics": topics or ["equities"],
        "model": model,
        "created_at": created_at or now,
        "completed_at": completed_at or now,
    }
    row.update(overrides)
    return row


# ===========================================================================
# complete_market_insight
# ===========================================================================


@pytest.mark.asyncio
async def test_complete_market_insight(mi_mock_db, mock_cursor):
    """complete_market_insight sets status, content, and completed_at."""
    from src.server.database.market_insight import complete_market_insight

    await complete_market_insight(
        market_insight_id="ins-1",
        headline="Big rally",
        summary="Stocks surged",
        content=[{"section": "overview"}],
        topics=["equities", "tech"],
        sources=["reuters"],
        generation_time_ms=2500,
    )

    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE market_insights" in sql
    assert "status = 'completed'" in sql
    assert "headline = %s" in sql
    assert "completed_at = %s" in sql
    params = mock_cursor.execute.call_args[0][1]
    # Last param is market_insight_id
    assert params[-1] == "ins-1"
    # First param is headline
    assert params[0] == "Big rally"


# ===========================================================================
# fail_market_insight
# ===========================================================================


@pytest.mark.asyncio
async def test_fail_market_insight(mi_mock_db, mock_cursor):
    """fail_market_insight sets status='failed' and error_message."""
    from src.server.database.market_insight import fail_market_insight

    await fail_market_insight("ins-1", "LLM timeout")

    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE market_insights" in sql
    assert "status = 'failed'" in sql
    assert "error_message = %s" in sql
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == "LLM timeout"
    assert params[1] == "ins-1"


# ===========================================================================
# get_market_insight
# ===========================================================================


@pytest.mark.asyncio
async def test_get_market_insight_found(mi_mock_db, mock_cursor):
    """get_market_insight returns all columns when row exists."""
    from src.server.database.market_insight import get_market_insight

    row = _insight_row(market_insight_id="ins-1")
    mock_cursor.fetchone.return_value = row

    result = await get_market_insight("ins-1")

    assert result is not None
    assert result["market_insight_id"] == "ins-1"
    assert result["headline"] == "Market rallies"
    params = mock_cursor.execute.call_args[0][1]
    assert params == ("ins-1",)


@pytest.mark.asyncio
async def test_get_market_insight_not_found(mi_mock_db, mock_cursor):
    """get_market_insight returns None for non-existent ID."""
    from src.server.database.market_insight import get_market_insight

    mock_cursor.fetchone.return_value = None

    result = await get_market_insight("nonexistent")
    assert result is None


# ===========================================================================
# get_todays_market_insights
# ===========================================================================


@pytest.mark.asyncio
async def test_get_todays_insights_system_only(mi_mock_db, mock_cursor):
    """get_todays_market_insights(user_id=None) returns system insights only."""
    from src.server.database.market_insight import get_todays_market_insights

    now = datetime.now(timezone.utc)
    sys_row = _card_row(headline="System brief", created_at=now)
    mock_cursor.fetchall.return_value = [sys_row]

    result = await get_todays_market_insights()

    assert len(result) == 1
    assert result[0]["headline"] == "System brief"
    # Only one execute call (system query) - no user query
    sql = mock_cursor.execute.call_args[0][0]
    assert "user_id IS NULL" in sql
    # Should NOT have queried for a specific user_id
    assert mock_cursor.execute.await_count == 1


@pytest.mark.asyncio
async def test_get_todays_insights_with_user(mi_mock_db, mock_cursor):
    """get_todays_market_insights(user_id='abc') returns system + user merged."""
    from src.server.database.market_insight import get_todays_market_insights

    now = datetime.now(timezone.utc)
    sys_row = _card_row(headline="System brief", created_at=now - timedelta(hours=1))
    user_row = _card_row(headline="User brief", created_at=now)

    # First fetchall: system rows; second fetchall: user rows
    mock_cursor.fetchall.side_effect = [[sys_row], [user_row]]

    result = await get_todays_market_insights(user_id="abc")

    assert len(result) == 2
    # Sorted by created_at DESC: user_row first (newer), system_row second
    assert result[0]["headline"] == "User brief"
    assert result[1]["headline"] == "System brief"
    # Two execute calls: system query + user query
    assert mock_cursor.execute.await_count == 2


@pytest.mark.asyncio
async def test_get_todays_insights_fallback_yesterday(mi_mock_db, mock_cursor):
    """get_todays_market_insights falls back to yesterday when no insights today."""
    from src.server.database.market_insight import get_todays_market_insights

    yesterday_row = _card_row(
        headline="Yesterday brief",
        created_at=datetime.now(timezone.utc) - timedelta(days=1),
    )

    # First fetchall: empty (no system rows today)
    # Then fetchone: yesterday fallback row
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = yesterday_row

    result = await get_todays_market_insights()

    assert len(result) == 1
    assert result[0]["headline"] == "Yesterday brief"
    # Two execute calls: today's system query (empty) + yesterday fallback query
    assert mock_cursor.execute.await_count == 2


@pytest.mark.asyncio
async def test_get_todays_insights_fallback_no_yesterday(mi_mock_db, mock_cursor):
    """get_todays_market_insights returns empty when no today or yesterday."""
    from src.server.database.market_insight import get_todays_market_insights

    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None  # no yesterday either

    result = await get_todays_market_insights()

    assert result == []


# ===========================================================================
# get_user_generating_insight
# ===========================================================================


@pytest.mark.asyncio
async def test_get_user_generating_insight_found(mi_mock_db, mock_cursor):
    """get_user_generating_insight returns generating row when one exists."""
    from src.server.database.market_insight import get_user_generating_insight

    row = _insight_row(user_id="user-1", status="generating")
    mock_cursor.fetchone.return_value = row

    result = await get_user_generating_insight("user-1")

    assert result is not None
    assert result["status"] == "generating"
    assert result["user_id"] == "user-1"
    sql = mock_cursor.execute.call_args[0][0]
    assert "status = 'generating'" in sql
    params = mock_cursor.execute.call_args[0][1]
    assert params == ("user-1",)


@pytest.mark.asyncio
async def test_get_user_generating_insight_none(mi_mock_db, mock_cursor):
    """get_user_generating_insight returns None when no generating row."""
    from src.server.database.market_insight import get_user_generating_insight

    mock_cursor.fetchone.return_value = None

    result = await get_user_generating_insight("user-1")
    assert result is None


# ===========================================================================
# get_user_recent_completed_insight
# ===========================================================================


@pytest.mark.asyncio
async def test_get_user_recent_completed_within_window(mi_mock_db, mock_cursor):
    """get_user_recent_completed_insight returns row completed within window."""
    from src.server.database.market_insight import get_user_recent_completed_insight

    row = _insight_row(
        user_id="user-1",
        type="personalized",
        completed_at=datetime.now(timezone.utc) - timedelta(minutes=2),
    )
    mock_cursor.fetchone.return_value = row

    result = await get_user_recent_completed_insight("user-1", within_minutes=5)

    assert result is not None
    assert result["type"] == "personalized"
    sql = mock_cursor.execute.call_args[0][0]
    assert "status = 'completed'" in sql
    assert "type = 'personalized'" in sql
    assert "completed_at >= %s" in sql
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == "user-1"


@pytest.mark.asyncio
async def test_get_user_recent_completed_outside_window(mi_mock_db, mock_cursor):
    """get_user_recent_completed_insight returns None when outside window."""
    from src.server.database.market_insight import get_user_recent_completed_insight

    mock_cursor.fetchone.return_value = None

    result = await get_user_recent_completed_insight("user-1", within_minutes=5)
    assert result is None
