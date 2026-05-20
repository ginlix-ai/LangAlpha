"""Integration tests for ``UserDataBackend`` against a real Postgres.

Validates the load-bearing invariants that unit tests can only fake:

* the advisory ``pg_advisory_xact_lock`` serializes parallel writers,
* the in-transaction content-hash recheck raises ``version_conflict`` when a
  concurrent writer changes the row(s) between read and apply,
* the SQL of inserts / updates / deletes actually mutates the underlying
  tables (no silent-no-op regressions on column-name drift),
* ``apply_preferences`` preserves the server-managed ``other_preference``
  column on update, and seeds ``{}`` on first insert,
* watchlist rename behaves as delete+insert by ``name`` identity.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.server.services import user_data_io as io
from src.server.services.user_data_io import UserDataValidationError

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _serialize(content: dict) -> str:
    return io.serialize_json(content)


# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------


class TestPortfolioApply:
    async def test_insert_round_trips(self, seed_user, patched_get_db_connection):
        user_id = seed_user["user_id"]

        # Empty start
        rows = await io.fetch_portfolio_for_user(user_id)
        assert rows == []
        version = io.serialize_portfolio(rows)["__version__"]

        diff = io.parse_and_diff_portfolio(
            _serialize({"holdings": [{
                "symbol": "ZZZ", "instrument_type": "stock",
                "quantity": "10", "average_cost": "100.00",
                "account_name": "Main",
            }]}),
            rows,
        )
        await io.apply_portfolio_diff(diff, user_id, payload_version=version)

        # Round-trip: fetch + serialize sees the new row
        rows_after = await io.fetch_portfolio_for_user(user_id)
        assert len(rows_after) == 1
        assert rows_after[0]["symbol"] == "ZZZ"
        assert rows_after[0]["quantity"] == Decimal("10")

    async def test_update_and_delete(self, seed_user, patched_get_db_connection):
        user_id = seed_user["user_id"]

        # Seed two holdings
        rows = await io.fetch_portfolio_for_user(user_id)
        v0 = io.serialize_portfolio(rows)["__version__"]
        diff0 = io.parse_and_diff_portfolio(_serialize({"holdings": [
            {"symbol": "AAA", "instrument_type": "stock", "quantity": "1", "average_cost": "10", "account_name": "Main"},
            {"symbol": "BBB", "instrument_type": "stock", "quantity": "2", "average_cost": "20", "account_name": "Main"},
        ]}), rows)
        await io.apply_portfolio_diff(diff0, user_id, payload_version=v0)

        # Now drop AAA and update BBB
        rows = await io.fetch_portfolio_for_user(user_id)
        v1 = io.serialize_portfolio(rows)["__version__"]
        diff1 = io.parse_and_diff_portfolio(_serialize({"holdings": [
            {"symbol": "BBB", "instrument_type": "stock", "quantity": "99", "average_cost": "20", "account_name": "Main"},
        ]}), rows)
        await io.apply_portfolio_diff(diff1, user_id, payload_version=v1)

        rows_after = await io.fetch_portfolio_for_user(user_id)
        symbols = {r["symbol"] for r in rows_after}
        assert symbols == {"BBB"}
        bbb = rows_after[0]
        assert bbb["quantity"] == Decimal("99")

    async def test_version_conflict_on_concurrent_change(
        self, seed_user, patched_get_db_connection,
    ):
        """Cached payload_version becomes stale → version_conflict."""
        user_id = seed_user["user_id"]

        rows = await io.fetch_portfolio_for_user(user_id)
        stale_version = io.serialize_portfolio(rows)["__version__"]

        # A concurrent writer slips a holding in
        other_diff = io.parse_and_diff_portfolio(_serialize({"holdings": [
            {"symbol": "RACE", "instrument_type": "stock", "quantity": "1", "average_cost": "5", "account_name": "Main"},
        ]}), rows)
        await io.apply_portfolio_diff(other_diff, user_id, payload_version=stale_version)

        # Our write, using the stale snapshot, must raise
        our_diff = io.parse_and_diff_portfolio(_serialize({"holdings": [
            {"symbol": "ZZZ", "instrument_type": "stock", "quantity": "1", "average_cost": "5", "account_name": "Main"},
        ]}), rows)
        with pytest.raises(UserDataValidationError) as exc:
            await io.apply_portfolio_diff(our_diff, user_id, payload_version=stale_version)
        assert exc.value.error_type == "version_conflict"

        # The losing write did NOT apply
        symbols = {r["symbol"] for r in await io.fetch_portfolio_for_user(user_id)}
        assert symbols == {"RACE"}

    async def test_advisory_lock_serializes_parallel_writers(
        self, seed_user, patched_get_db_connection,
    ):
        """Two concurrent valid writes against fresh versions: at most one wins.

        Both writers compute a payload_version against the same starting state.
        With the advisory lock + recheck, the second one to take the lock sees
        the first writer's hash and gets version_conflict — guaranteeing the
        writes don't interleave row-level.
        """
        user_id = seed_user["user_id"]

        rows = await io.fetch_portfolio_for_user(user_id)
        version = io.serialize_portfolio(rows)["__version__"]
        diff_a = io.parse_and_diff_portfolio(_serialize({"holdings": [
            {"symbol": "AAA", "instrument_type": "stock", "quantity": "1", "average_cost": "1", "account_name": "Main"},
        ]}), rows)
        diff_b = io.parse_and_diff_portfolio(_serialize({"holdings": [
            {"symbol": "BBB", "instrument_type": "stock", "quantity": "2", "average_cost": "2", "account_name": "Main"},
        ]}), rows)

        results = await asyncio.gather(
            io.apply_portfolio_diff(diff_a, user_id, payload_version=version),
            io.apply_portfolio_diff(diff_b, user_id, payload_version=version),
            return_exceptions=True,
        )
        conflicts = [r for r in results if isinstance(r, UserDataValidationError)]
        successes = [r for r in results if r is None]
        # Exactly one succeeded, one raised version_conflict.
        assert len(successes) == 1
        assert len(conflicts) == 1
        assert conflicts[0].error_type == "version_conflict"

        # Whichever symbol won is the only one present.
        rows_after = await io.fetch_portfolio_for_user(user_id)
        assert len(rows_after) == 1
        assert rows_after[0]["symbol"] in {"AAA", "BBB"}


# ---------------------------------------------------------------------------
# Preferences — other_preference preservation
# ---------------------------------------------------------------------------


class TestPreferenceApply:
    async def test_first_insert_seeds_other_preference_empty(
        self, seed_user, patched_get_db_connection,
    ):
        user_id = seed_user["user_id"]
        current = await io.fetch_preferences_for_user(user_id)
        version = io.serialize_preferences(current)["__version__"]
        values = io.parse_preferences(_serialize({
            "risk_preference": {"tolerance": "moderate"},
            "investment_preference": {},
            "agent_preference": {},
        }))
        await io.apply_preferences(values, user_id, payload_version=version)

        row = await io.fetch_preferences_for_user(user_id)
        assert row is not None
        assert row["risk_preference"] == {"tolerance": "moderate"}
        # other_preference seeded to empty object on first insert
        assert row["other_preference"] == {}

    async def test_update_preserves_other_preference(
        self, seed_user, patched_get_db_connection, test_db_pool,
    ):
        """Server-managed `other_preference` survives an agent edit."""
        user_id = seed_user["user_id"]

        # First insert (agent path)
        current = await io.fetch_preferences_for_user(user_id)
        v0 = io.serialize_preferences(current)["__version__"]
        await io.apply_preferences(
            io.parse_preferences(_serialize({"risk_preference": {"tolerance": "low"}})),
            user_id, payload_version=v0,
        )

        # Server slips in onboarding state via direct SQL (simulating internal flow).
        # Acquire-and-release the connection eagerly so the pool isn't held during
        # the next apply_preferences call below.
        async with test_db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE user_preferences SET other_preference = %s::jsonb WHERE user_id = %s",
                    ('{"onboarding_step": 3}', user_id),
                )

        # Agent edits its slice
        current = await io.fetch_preferences_for_user(user_id)
        v1 = io.serialize_preferences(current)["__version__"]
        await io.apply_preferences(
            io.parse_preferences(_serialize({
                "risk_preference": {"tolerance": "aggressive"},
                "investment_preference": {"style": "growth"},
                "agent_preference": {},
            })),
            user_id, payload_version=v1,
        )

        row = await io.fetch_preferences_for_user(user_id)
        assert row["risk_preference"] == {"tolerance": "aggressive"}
        assert row["investment_preference"] == {"style": "growth"}
        # other_preference must be untouched
        assert row["other_preference"] == {"onboarding_step": 3}


# ---------------------------------------------------------------------------
# Watchlist — rename = delete + insert by name
# ---------------------------------------------------------------------------


class TestWatchlistApply:
    async def test_rename_is_delete_plus_insert(
        self, seed_user, patched_get_db_connection,
    ):
        user_id = seed_user["user_id"]

        wls, items = await io.fetch_watchlist_for_user(user_id)
        v0 = io.serialize_watchlist(wls, items)["__version__"]
        diff0 = io.parse_and_diff_watchlist(_serialize({"watchlists": [{
            "name": "Old Name",
            "items": [{"symbol": "AAPL", "instrument_type": "stock"}],
        }]}), wls, items)
        await io.apply_watchlist_diff(diff0, user_id, payload_version=v0)

        wls, items = await io.fetch_watchlist_for_user(user_id)
        old_wl_id = str(wls[0]["watchlist_id"])

        v1 = io.serialize_watchlist(wls, items)["__version__"]
        # Rename: agent sees "Old Name", writes "New Name" with the items intact
        diff1 = io.parse_and_diff_watchlist(_serialize({"watchlists": [{
            "name": "New Name",
            "items": [{"symbol": "AAPL", "instrument_type": "stock"}],
        }]}), wls, items)
        await io.apply_watchlist_diff(diff1, user_id, payload_version=v1)

        wls_after, items_after = await io.fetch_watchlist_for_user(user_id)
        names = {w["name"] for w in wls_after}
        assert names == {"New Name"}
        # Brand-new row — DB id changed
        new_wl_id = str(wls_after[0]["watchlist_id"])
        assert new_wl_id != old_wl_id
        # Items came along
        assert items_after.get(new_wl_id, [])[0]["symbol"] == "AAPL"


# ---------------------------------------------------------------------------
# UserDataBackend.aread_text smoke — sanity-check that the read path hits DB
# ---------------------------------------------------------------------------


class TestBackendRead:
    async def test_read_portfolio_via_backend(
        self, seed_user, patched_get_db_connection,
    ):
        """End-to-end through the agent-facing surface, not just io.*."""
        from ptc_agent.agent.backends.user_data import (
            PORTFOLIO_FILE,
            UserDataBackend,
        )

        # Minimal sandbox stub — backend reads from io, not the sandbox FS
        class _StubSandbox:
            def normalize_path(self, p): return p
            def virtualize_path(self, p): return p
            def validate_path(self, p): return True
            @property
            def filesystem_config(self): return None

        backend = UserDataBackend(
            user_id=seed_user["user_id"],
            sandbox_backend=_StubSandbox(),  # type: ignore[arg-type]
            root_prefix="/work/.agents/user/profile/",
        )

        # Seed a holding via io
        rows = await io.fetch_portfolio_for_user(seed_user["user_id"])
        version = io.serialize_portfolio(rows)["__version__"]
        await io.apply_portfolio_diff(
            io.parse_and_diff_portfolio(_serialize({"holdings": [{
                "symbol": "READ", "instrument_type": "stock",
                "quantity": "7", "average_cost": "13.5", "account_name": "Main",
            }]}), rows),
            seed_user["user_id"],
            payload_version=version,
        )

        content = await backend.aread_text(f"/work/.agents/user/profile/{PORTFOLIO_FILE}")
        assert content is not None
        assert "READ" in content
        # Agent-visible JSON does NOT include __version__
        assert "__version__" not in content
