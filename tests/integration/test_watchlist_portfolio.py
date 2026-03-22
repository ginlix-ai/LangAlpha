"""Integration tests for watchlist and portfolio CRUD against real PostgreSQL."""

from __future__ import annotations

from decimal import Decimal

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ============================================================================
# Watchlist tests
# ============================================================================


class TestWatchlistCRUD:
    """Test watchlist create, read, update, delete."""

    async def test_create_watchlist(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import create_watchlist

        wl = await create_watchlist(
            user_id=seed_user["user_id"],
            name="Tech Stocks",
            description="Technology sector watchlist",
        )

        assert wl["name"] == "Tech Stocks"
        assert wl["description"] == "Technology sector watchlist"
        assert wl["is_default"] is False
        assert wl["user_id"] == seed_user["user_id"]

    async def test_create_duplicate_name_raises(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import create_watchlist

        await create_watchlist(user_id=seed_user["user_id"], name="Dupes")

        with pytest.raises(ValueError, match="already exists"):
            await create_watchlist(user_id=seed_user["user_id"], name="Dupes")

    async def test_get_user_watchlists(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import (
            create_watchlist,
            get_user_watchlists,
        )

        await create_watchlist(user_id=seed_user["user_id"], name="WL1")
        await create_watchlist(user_id=seed_user["user_id"], name="WL2")

        watchlists = await get_user_watchlists(seed_user["user_id"])
        assert len(watchlists) == 2

    async def test_update_watchlist(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import create_watchlist, update_watchlist

        wl = await create_watchlist(
            user_id=seed_user["user_id"], name="Old Name"
        )

        updated = await update_watchlist(
            watchlist_id=str(wl["watchlist_id"]),
            user_id=seed_user["user_id"],
            name="New Name",
            description="Updated",
        )

        assert updated is not None
        assert updated["name"] == "New Name"
        assert updated["description"] == "Updated"

    async def test_delete_watchlist(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import (
            create_watchlist,
            delete_watchlist,
            get_user_watchlists,
        )

        wl = await create_watchlist(
            user_id=seed_user["user_id"], name="ToDelete"
        )

        deleted = await delete_watchlist(
            str(wl["watchlist_id"]), seed_user["user_id"]
        )
        assert deleted is True

        remaining = await get_user_watchlists(seed_user["user_id"])
        assert len(remaining) == 0

    async def test_get_or_create_default_watchlist(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import get_or_create_default_watchlist

        # First call creates the default
        default1 = await get_or_create_default_watchlist(seed_user["user_id"])
        assert default1["name"] == "Default"
        assert default1["is_default"] is True

        # Second call returns the same one
        default2 = await get_or_create_default_watchlist(seed_user["user_id"])
        assert str(default2["watchlist_id"]) == str(default1["watchlist_id"])


class TestWatchlistItems:
    """Test watchlist item CRUD."""

    async def test_add_and_list_items(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import (
            create_watchlist,
            create_watchlist_item,
            get_watchlist_items,
        )

        wl = await create_watchlist(
            user_id=seed_user["user_id"], name="My List"
        )
        wl_id = str(wl["watchlist_id"])

        item = await create_watchlist_item(
            user_id=seed_user["user_id"],
            watchlist_id=wl_id,
            symbol="AAPL",
            instrument_type="stock",
            name="Apple Inc.",
            exchange="NASDAQ",
        )

        assert item["symbol"] == "AAPL"
        assert item["instrument_type"] == "stock"
        assert item["name"] == "Apple Inc."

        items = await get_watchlist_items(wl_id, seed_user["user_id"])
        assert len(items) == 1
        assert items[0]["symbol"] == "AAPL"

    async def test_duplicate_item_raises(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import (
            create_watchlist,
            create_watchlist_item,
        )

        wl = await create_watchlist(
            user_id=seed_user["user_id"], name="Dup Items"
        )
        wl_id = str(wl["watchlist_id"])

        await create_watchlist_item(
            user_id=seed_user["user_id"],
            watchlist_id=wl_id,
            symbol="MSFT",
            instrument_type="stock",
        )

        with pytest.raises(ValueError, match="already exists"):
            await create_watchlist_item(
                user_id=seed_user["user_id"],
                watchlist_id=wl_id,
                symbol="MSFT",
                instrument_type="stock",
            )

    async def test_delete_watchlist_item(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.watchlist import (
            create_watchlist,
            create_watchlist_item,
            delete_watchlist_item,
            get_watchlist_items,
        )

        wl = await create_watchlist(
            user_id=seed_user["user_id"], name="Del Item"
        )
        wl_id = str(wl["watchlist_id"])

        item = await create_watchlist_item(
            user_id=seed_user["user_id"],
            watchlist_id=wl_id,
            symbol="TSLA",
            instrument_type="stock",
        )

        deleted = await delete_watchlist_item(
            str(item["watchlist_item_id"]), seed_user["user_id"]
        )
        assert deleted is True

        items = await get_watchlist_items(wl_id, seed_user["user_id"])
        assert len(items) == 0


# ============================================================================
# Portfolio tests
# ============================================================================


class TestPortfolioCRUD:
    """Test portfolio holding CRUD."""

    async def test_create_portfolio_holding(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.portfolio import upsert_portfolio_holding

        holding, merge_details = await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="AAPL",
            instrument_type="stock",
            quantity=Decimal("100.5"),
            average_cost=Decimal("150.25"),
            currency="USD",
            exchange="NASDAQ",
            name="Apple Inc.",
        )

        assert holding["symbol"] == "AAPL"
        assert holding["quantity"] == Decimal("100.5")
        assert holding["average_cost"] == Decimal("150.25")
        assert holding["currency"] == "USD"
        assert merge_details is None

    async def test_duplicate_holding_merges(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.portfolio import upsert_portfolio_holding

        await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="GOOG",
            instrument_type="stock",
            quantity=Decimal("50"),
            average_cost=Decimal("100"),
        )

        holding, merge_details = await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="GOOG",
            instrument_type="stock",
            quantity=Decimal("50"),
            average_cost=Decimal("200"),
        )

        assert holding["quantity"] == Decimal("100")
        assert holding["average_cost"] == Decimal("150")
        assert merge_details is not None
        assert merge_details["previous"]["quantity"] == "50"
        assert merge_details["result"]["quantity"] == "100"

    async def test_get_user_portfolio(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.portfolio import (
            upsert_portfolio_holding,
            get_user_portfolio,
        )

        await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="NVDA",
            instrument_type="stock",
            quantity=Decimal("25"),
        )
        await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="SPY",
            instrument_type="etf",
            quantity=Decimal("10"),
        )

        holdings = await get_user_portfolio(seed_user["user_id"])
        assert len(holdings) == 2
        symbols = {h["symbol"] for h in holdings}
        assert symbols == {"NVDA", "SPY"}

    async def test_update_portfolio_holding(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.portfolio import (
            upsert_portfolio_holding,
            update_portfolio_holding,
        )

        holding, _ = await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="AMZN",
            instrument_type="stock",
            quantity=Decimal("5"),
            average_cost=Decimal("100"),
        )

        updated = await update_portfolio_holding(
            user_portfolio_id=str(holding["user_portfolio_id"]),
            user_id=seed_user["user_id"],
            quantity=Decimal("15"),
            average_cost=Decimal("110.50"),
            notes="Bought more on dip",
        )

        assert updated is not None
        assert updated["quantity"] == Decimal("15")
        assert updated["average_cost"] == Decimal("110.50")
        assert updated["notes"] == "Bought more on dip"

    async def test_delete_portfolio_holding(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.portfolio import (
            upsert_portfolio_holding,
            delete_portfolio_holding,
            get_user_portfolio,
        )

        holding, _ = await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="META",
            instrument_type="stock",
            quantity=Decimal("20"),
        )

        deleted = await delete_portfolio_holding(
            str(holding["user_portfolio_id"]), seed_user["user_id"]
        )
        assert deleted is True

        holdings = await get_user_portfolio(seed_user["user_id"])
        assert len(holdings) == 0

    async def test_get_portfolio_holding_by_id(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.portfolio import (
            upsert_portfolio_holding,
            get_portfolio_holding,
        )

        holding, _ = await upsert_portfolio_holding(
            user_id=seed_user["user_id"],
            symbol="NFLX",
            instrument_type="stock",
            quantity=Decimal("8"),
        )

        fetched = await get_portfolio_holding(
            str(holding["user_portfolio_id"]), seed_user["user_id"]
        )

        assert fetched is not None
        assert fetched["symbol"] == "NFLX"
        assert fetched["quantity"] == Decimal("8")
