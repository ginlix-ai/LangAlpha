"""Persisted symbols store one spelling per listing, whichever one arrived."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database import chart_annotation
from src.server.models.chart_layer import ChartRef, make_chart_id
from src.server.models.user import (
    PortfolioHoldingCreate,
    WatchlistItemCreate,
    normalize_symbol,
)


@pytest.mark.parametrize("raw", ["600519.SS", "600519.sh", " 600519.SH "])
def test_shanghai_stores_in_display_spelling(raw):
    assert normalize_symbol(raw) == "600519.SH"
    assert WatchlistItemCreate(symbol=raw, instrument_type="stock").symbol == "600519.SH"
    assert PortfolioHoldingCreate(symbol=raw, instrument_type="stock", quantity=1).symbol == "600519.SH"


def test_other_venues_keep_their_spelling():
    assert normalize_symbol("000001.sz") == "000001.SZ"
    assert normalize_symbol("aapl") == "AAPL"


def test_one_chart_per_listing():
    assert make_chart_id("600519.SS", "1day") == make_chart_id("600519.sh", "1day") == "600519.SH:1day"


def test_a_chart_ref_names_its_chart_in_the_stored_spelling():
    ref = ChartRef.of("ws-1", "600519.SS", " 1day ")
    assert (ref.symbol, ref.timeframe, ref.chart_id) == ("600519.SH", "1day", "600519.SH:1day")


@pytest.mark.asyncio
async def test_a_runs_layers_are_written_in_one_transaction():
    """Every row of a run goes in one statement batch inside one transaction,
    so the store holds all of them or none."""
    cur = MagicMock(executemany=AsyncMock())
    cursor = MagicMock(__aenter__=AsyncMock(return_value=cur), __aexit__=AsyncMock(return_value=False))
    tx = MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    conn = MagicMock(cursor=MagicMock(return_value=cursor), transaction=MagicMock(return_value=tx))

    @asynccontextmanager
    async def _connection():
        yield conn

    chart = ChartRef.of("ws-1", "600519.SS", "1day")
    with patch.object(chart_annotation, "get_db_connection", _connection):
        await chart_annotation.add_layers(
            [(chart.layer("a"), {"id": "a"}), (chart.layer("b"), {"id": "b"})]
        )
    conn.transaction.assert_called_once()
    [(sql, params)] = [c.args for c in cur.executemany.await_args_list]
    assert [p[:5] for p in params] == [
        ("ws-1", "600519.SH:1day", "600519.SH", "1day", "a"),
        ("ws-1", "600519.SH:1day", "600519.SH", "1day", "b"),
    ]
    assert [json.loads(p[5].obj) for p in params] == [{"id": "a"}, {"id": "b"}]
