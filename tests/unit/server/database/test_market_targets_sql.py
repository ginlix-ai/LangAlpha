"""The statements that give a market-watch target its identity. A target is
on a listing, the symbol in its market: HSI the US ticker and HSI the Hang
Seng are both stored as HSI, so a statement matching the symbol alone would
replace or cancel the other one."""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.server.database import market_targets

_THREAD = "00000000-0000-0000-0000-00000000000a"


@pytest.fixture
def cursor(mock_connection, mock_cursor):
    @asynccontextmanager
    async def _connection(conn=None):
        yield mock_connection

    with patch.object(market_targets, "get_db_connection", _connection):
        yield mock_cursor


def _sql(call) -> str:
    return re.sub(r"\s+", " ", call.args[0])


@pytest.mark.asyncio
async def test_arming_replaces_the_same_kind_on_its_own_listing_only(cursor):
    cursor.fetchone.side_effect = [{"n": 0}, {"target_id": "t-1", "value": 21000}]
    now = datetime.now(timezone.utc)

    await market_targets.arm_target(
        thread_id=_THREAD, user_id="u-1", symbol="HSI", market="index", kind="above",
        value=21000.0, pct_from=None, armed_price=None,
        expires_at=now + timedelta(hours=6), warn_at=now + timedelta(hours=5), max_armed=10,
    )

    replace = next(
        c for c in cursor.execute.call_args_list if "SET status = 'cancelled'" in c.args[0]
    )
    assert "AND symbol = %s AND market = %s AND kind = %s" in _sql(replace)
    assert replace.args[1] == (_THREAD, "HSI", "index", "above")


@pytest.mark.asyncio
async def test_a_cancel_by_listing_matches_symbol_and_market_together(cursor):
    await market_targets.cancel_targets(_THREAD, listings=[("HSI", "index"), ("SPX", "index")])

    assert (
        "(symbol, market) IN (SELECT * FROM unnest(%s::text[], %s::text[]))"
        in _sql(cursor.execute.call_args)
    )
    assert cursor.execute.call_args.args[1] == [_THREAD, ["HSI", "SPX"], ["index", "index"]]


@pytest.mark.asyncio
async def test_a_cancel_locks_its_rows_in_id_order_as_a_claim_does(cursor):
    """A cancel of several targets and a tick claiming several of the same
    must take the row locks in one order, or they deadlock."""
    await market_targets.cancel_targets(_THREAD)

    assert "ORDER BY target_id FOR UPDATE )" in _sql(cursor.execute.call_args)


@pytest.mark.asyncio
async def test_a_workspace_cancel_reaches_each_of_its_threads_in_id_order(cursor):
    await market_targets.cancel_workspace_targets("ws-1")

    sql = _sql(cursor.execute.call_args)
    assert (
        "WHERE c.workspace_id = %s AND t.status = 'armed' AND t.expires_at > NOW() "
        "ORDER BY t.target_id FOR UPDATE OF t )" in sql
    )
    assert cursor.execute.call_args.args[1] == ("ws-1",)


@pytest.mark.asyncio
async def test_the_expiry_sweep_locks_its_rows_in_id_order_too(cursor):
    """Every worker sweeps, while ticks claim and turns cancel: every
    statement that updates several targets locks them in one order."""
    cursor.rowcount = 2

    assert await market_targets.expire_due_targets() == 2

    sql = _sql(cursor.execute.call_args)
    assert "SET status = 'expired'" in sql
    assert (
        "WHERE target_id IN ( SELECT target_id FROM market_watch_targets"
        " WHERE status = 'armed' AND expires_at <= NOW()"
        " ORDER BY target_id FOR UPDATE )"
    ) in sql


_OTHER_THREAD = "00000000-0000-0000-0000-00000000000b"


@pytest.fixture
def transactions(mock_connection):
    opened = []

    @asynccontextmanager
    async def _transaction(*args, **kwargs):
        opened.append(True)
        yield

    mock_connection.transaction = _transaction
    return opened


def _fired(target_id: str, thread_id: str = _THREAD) -> dict:
    return {
        "target_id": target_id, "conversation_thread_id": thread_id, "user_id": "u-1",
        "value": 200, "armed_price": None, "fired_price": 201.5,
    }


@pytest.mark.asyncio
async def test_the_hits_one_price_made_are_claimed_in_one_transaction(cursor, transactions):
    cursor.fetchall.return_value = [_fired("t-1"), _fired("t-2", _OTHER_THREAD)]
    printed = datetime(2026, 10, 16, 14, 30, tzinfo=timezone.utc)

    fired = await market_targets.fire_targets(["t-1", "t-2"], 201.5, printed_at=printed)

    assert len(transactions) == 1
    claim, *enqueued = cursor.execute.call_args_list
    sql = _sql(claim)
    assert "fired_at = LEAST(COALESCE(%s::timestamptz, NOW()), NOW())" in sql
    assert (
        "WHERE target_id = ANY(%s::uuid[]) AND status = 'armed' AND expires_at > NOW() "
        "ORDER BY target_id FOR UPDATE" in sql
    )
    assert claim.args[1] == (printed, 201.5, ["t-1", "t-2"])
    assert [c.args[1][-1] for c in enqueued] == [
        "market_target:t-1:hit", "market_target:t-2:hit"
    ]
    assert [(r["target_id"], r["fired_price"]) for r in fired] == [
        ("t-1", 201.5), ("t-2", 201.5)
    ]


@pytest.mark.asyncio
async def test_a_hit_lost_to_another_claim_enqueues_nothing(cursor, transactions):
    """Only the rows the guarded UPDATE returned carry a notice."""
    cursor.fetchall.return_value = [_fired("t-2")]

    fired = await market_targets.fire_targets(["t-1", "t-2"], 201.5)

    claim, *enqueued = cursor.execute.call_args_list
    assert [c.args[1][-1] for c in enqueued] == ["market_target:t-2:hit"]
    assert [r["target_id"] for r in fired] == ["t-2"]

    cursor.execute.reset_mock()
    cursor.fetchall.return_value = []
    assert await market_targets.fire_targets(["t-1"], 201.5) == []
    assert cursor.execute.await_count == 1


@pytest.mark.asyncio
async def test_no_hit_is_no_transaction(cursor, transactions):
    assert await market_targets.fire_targets([], 201.5) == []
    assert transactions == []
    cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_thread_reload_reads_only_its_threads_armed_targets(cursor):
    await market_targets.list_armed_targets(thread_ids=[_THREAD])
    sql = _sql(cursor.execute.call_args)
    assert (
        "WHERE status = 'armed' AND expires_at > NOW() "
        "AND conversation_thread_id = ANY(%s::uuid[])" in sql
    )
    assert cursor.execute.call_args.args[1] == [[_THREAD]]

    await market_targets.list_armed_targets()
    assert "conversation_thread_id" not in _sql(cursor.execute.call_args).split("WHERE")[1]
    assert cursor.execute.call_args.args[1] == []
