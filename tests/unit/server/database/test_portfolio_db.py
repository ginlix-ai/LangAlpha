"""
Tests for src/server/database/portfolio.py

A holding written without a currency takes its listing's, and a write that
names another currency than the holding it merges into is refused rather
than averaged across the two. A cost in pence is stored in pounds.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.portfolio import (
    HoldingCurrencyMismatch,
    canonical_holding_cost,
    default_holding_currency,
    update_portfolio_holding,
    upsert_portfolio_holding,
)


@pytest.fixture
def mock_cursor():
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)
    return cursor


@pytest.fixture(autouse=True)
def portfolio_mock_db(mock_cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    @asynccontextmanager
    async def _transaction_cm():
        yield

    conn.cursor = _cursor_cm
    conn.transaction = _transaction_cm

    @asynccontextmanager
    async def _fake():
        yield conn

    with patch("src.server.database.portfolio.get_db_connection", new=_fake):
        yield conn


def _row(symbol="600519.SH", currency="CNY", **overrides):
    now = datetime.now(timezone.utc)
    row = {
        "user_portfolio_id": "up-1",
        "user_id": "user-1",
        "symbol": symbol,
        "instrument_type": "stock",
        "exchange": None,
        "name": None,
        "quantity": Decimal("100"),
        "average_cost": Decimal("1500"),
        "currency": currency,
        "account_name": None,
        "notes": None,
        "metadata": {},
        "first_purchased_at": None,
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


def _statements(cursor):
    return [c.args[0] for c in cursor.execute.call_args_list]


@pytest.mark.parametrize(
    ("symbol", "instrument_type", "currency"),
    [
        ("600519.SH", "stock", "CNY"),
        ("0700.HK", "stock", "HKD"),
        ("AAPL", "stock", "USD"),
        ("BTC-EUR", "crypto", "EUR"),
        # A stock named like an index is a US ticker, not the Hang Seng.
        ("HSI", "stock", "USD"),
        # Unreadable, yet stored as before.
        ("...", "stock", "USD"),
    ],
)
def test_default_currency_is_the_listings(symbol, instrument_type, currency):
    assert default_holding_currency(symbol, instrument_type) == currency


@pytest.mark.asyncio
async def test_a_cn_holding_written_without_currency_is_stored_in_cny(mock_cursor):
    mock_cursor.fetchone.side_effect = [None, _row()]

    holding, merge_details = await upsert_portfolio_holding(
        user_id="user-1",
        symbol="600519.ss",
        instrument_type="stock",
        quantity=Decimal("100"),
        average_cost=Decimal("1500"),
    )

    assert merge_details is None
    insert = mock_cursor.execute.call_args_list[-1]
    assert "INSERT INTO user_portfolios" in insert.args[0]
    params = insert.args[1]
    assert params[2] == "600519.SH"
    assert params[8] == "CNY"


@pytest.mark.asyncio
async def test_an_explicit_currency_is_kept_on_create(mock_cursor):
    mock_cursor.fetchone.side_effect = [None, _row(currency="USD")]

    await upsert_portfolio_holding(
        user_id="user-1",
        symbol="600519.SH",
        instrument_type="stock",
        quantity=Decimal("100"),
        currency="USD",
    )

    assert mock_cursor.execute.call_args_list[-1].args[1][8] == "USD"


@pytest.mark.asyncio
async def test_a_mismatching_currency_into_a_holding_is_refused(mock_cursor):
    mock_cursor.fetchone.side_effect = [_row(currency="CNY")]

    with pytest.raises(HoldingCurrencyMismatch) as refused:
        await upsert_portfolio_holding(
            user_id="user-1",
            symbol="600519.SH",
            instrument_type="stock",
            quantity=Decimal("10"),
            average_cost=Decimal("210"),
            currency="usd",
        )

    assert str(refused.value) == (
        "600519.SH is held in CNY, so a cost in USD cannot be averaged into it. "
        "Give the cost in CNY, or change the holding's currency first."
    )
    assert not any("UPDATE user_portfolios" in sql for sql in _statements(mock_cursor))


@pytest.mark.asyncio
async def test_the_refusal_names_the_account(mock_cursor):
    mock_cursor.fetchone.side_effect = [_row(currency="CNY", account_name="Futu")]

    with pytest.raises(HoldingCurrencyMismatch, match=r"^600519\.SH \(Futu\) is held in CNY"):
        await upsert_portfolio_holding(
            user_id="user-1",
            symbol="600519.SH",
            instrument_type="stock",
            quantity=Decimal("10"),
            currency="HKD",
            account_name="Futu",
        )


@pytest.mark.parametrize("currency", [None, "cny"])
@pytest.mark.asyncio
async def test_a_write_in_the_holdings_currency_merges(mock_cursor, currency):
    merged = _row(quantity=Decimal("200"), average_cost=Decimal("1600"))
    mock_cursor.fetchone.side_effect = [_row(currency="CNY"), merged]

    holding, merge_details = await upsert_portfolio_holding(
        user_id="user-1",
        symbol="600519.SH",
        instrument_type="stock",
        quantity=Decimal("100"),
        average_cost=Decimal("1700"),
        currency=currency,
    )

    assert holding == merged
    assert merge_details["result"] == {"quantity": "200", "average_cost": "1600"}
    update = mock_cursor.execute.call_args_list[-1]
    assert "UPDATE user_portfolios" in update.args[0]
    # The merge never rewrites the holding's currency.
    assert "currency" not in update.args[0].split("SET", 1)[1].split("WHERE", 1)[0]


@pytest.mark.parametrize(
    ("currency", "cost", "stored"),
    [
        ("GBp", Decimal("110"), ("GBP", Decimal("1.10"))),
        ("GBX", Decimal("205"), ("GBP", Decimal("2.05"))),
        (" GBx ", None, ("GBP", None)),
        ("gbx", Decimal("110"), ("GBP", Decimal("1.10"))),
        # Pounds, however it is spelled, is not pence.
        ("gbp", Decimal("1.2"), ("GBP", Decimal("1.2"))),
        (" usd ", Decimal("3"), ("USD", Decimal("3"))),
        ("", Decimal("3"), (None, Decimal("3"))),
        (None, Decimal("3"), (None, Decimal("3"))),
    ],
)
def test_a_holding_is_stored_in_its_canonical_currency(currency, cost, stored):
    assert canonical_holding_cost(currency, cost) == stored


@pytest.mark.asyncio
async def test_a_pence_holding_is_created_in_pounds(mock_cursor):
    mock_cursor.fetchone.side_effect = [None, _row(symbol="VOD.L", currency="GBP")]

    await upsert_portfolio_holding(
        user_id="user-1",
        symbol="VOD.L",
        instrument_type="stock",
        quantity=Decimal("1000"),
        average_cost=Decimal("110"),
        currency="GBp",
    )

    params = mock_cursor.execute.call_args_list[-1].args[1]
    assert (params[7], params[8]) == (Decimal("1.10"), "GBP")


@pytest.mark.asyncio
async def test_a_pence_cost_merges_into_a_pounds_holding_in_pounds(mock_cursor):
    held = _row(symbol="VOD.L", currency="GBP", quantity=Decimal("1000"), average_cost=Decimal("1.20"))
    mock_cursor.fetchone.side_effect = [held, held]

    _, merge_details = await upsert_portfolio_holding(
        user_id="user-1",
        symbol="VOD.L",
        instrument_type="stock",
        quantity=Decimal("1000"),
        average_cost=Decimal("110"),
        currency="GBp",
    )

    assert merge_details["added"]["average_cost"] == "1.1"
    assert merge_details["result"] == {"quantity": "2000", "average_cost": "1.15"}


@pytest.mark.parametrize(
    ("held", "added", "merged"),
    [
        ((Decimal("100"), Decimal("1.1")), (Decimal("50"), Decimal("1.3")), "1.16666666667"),
        (
            (Decimal("50000000"), Decimal("0.00001234")),
            (Decimal("30000000"), Decimal("0.00002")),
            "0.0000152125",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_merged_cost_keeps_twelve_significant_digits(mock_cursor, held, added, merged):
    existing = _row(symbol="AAPL", currency="USD", quantity=held[0], average_cost=held[1])
    mock_cursor.fetchone.side_effect = [existing, existing]

    _, merge_details = await upsert_portfolio_holding(
        user_id="user-1",
        symbol="AAPL",
        instrument_type="stock",
        quantity=added[0],
        average_cost=added[1],
        currency="USD",
    )

    assert merge_details["result"]["average_cost"] == merged
    assert mock_cursor.execute.call_args_list[-1].args[1][1] == Decimal(merged)


@pytest.mark.asyncio
async def test_an_update_in_pence_is_stored_in_pounds(mock_cursor):
    mock_cursor.fetchone.side_effect = [_row(symbol="VOD.L", currency="GBP")]

    await update_portfolio_holding(
        "up-1", "user-1", average_cost=Decimal("120"), currency="GBp",
    )

    update = mock_cursor.execute.call_args_list[-1]
    assert "UPDATE user_portfolios" in update.args[0]
    assert update.args[1][:2] == (Decimal("1.20"), "GBP")


@pytest.mark.asyncio
async def test_a_pence_currency_without_its_cost_is_refused(mock_cursor):
    mock_cursor.fetchone.side_effect = [_row(symbol="VOD.L", currency="GBP")]

    with pytest.raises(HoldingCurrencyMismatch, match=r"^GBX is a cost in pence"):
        await update_portfolio_holding("up-1", "user-1", currency="GBX")

    assert not any("UPDATE" in c.args[0] for c in mock_cursor.execute.call_args_list)


@pytest.mark.asyncio
async def test_a_pence_currency_on_a_holding_the_caller_lacks_is_not_found(mock_cursor):
    assert await update_portfolio_holding("up-1", "other-user", currency="GBX") is None

    assert not any("UPDATE" in c.args[0] for c in mock_cursor.execute.call_args_list)
