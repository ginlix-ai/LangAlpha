"""Persisted symbols store one spelling per listing, whichever one arrived."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database import chart_annotation
from src.server.database.chart_annotation import make_chart_id
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


@pytest.mark.asyncio
async def test_an_annotation_answers_in_the_stored_spelling_when_the_read_back_fails():
    """The read-back serves stored payloads, so its fallback must not echo the
    caller's ``600519.SS`` while every other answer says ``600519.SH``."""
    read_back = MagicMock()
    read_back.__aenter__ = AsyncMock(return_value=MagicMock(execute=AsyncMock(side_effect=RuntimeError("read"))))
    read_back.__aexit__ = AsyncMock(return_value=False)
    write = MagicMock()
    write.__aenter__ = AsyncMock(return_value=MagicMock(execute=AsyncMock()))
    write.__aexit__ = AsyncMock(return_value=False)
    conn = MagicMock()
    conn.cursor = MagicMock(side_effect=[write, read_back])

    @asynccontextmanager
    async def _connection():
        yield conn

    annotation = {"annotation_id": "ann_1", "symbol": "600519.SS", "type": "hline"}
    with patch.object(chart_annotation, "get_db_connection", _connection):
        served, read_at_us = await chart_annotation.add_and_list_annotations(
            "ws-1", make_chart_id("600519.SS", "1day"), "600519.SS", "1day", annotation,
        )
    stored = write.__aenter__.return_value.execute.await_args.args[1][-1].obj
    assert served == [stored]
    assert read_at_us is None
    assert served[0]["symbol"] == "600519.SH"
