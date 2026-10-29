"""Integration tests for ``UserDataBackend`` against a real Postgres.

Validates the load-bearing invariants that unit tests can only fake:

* the advisory ``pg_advisory_xact_lock`` serializes parallel writers,
* the version check under that lock raises ``version_conflict`` when a
  concurrent writer changes the row(s) between read and write,
* the SQL of inserts / updates / deletes actually mutates the underlying
  tables (no silent-no-op regressions on column-name drift),
* ``write_preferences`` preserves the server-managed ``other_preference``
  column on update, and seeds ``{}`` on first insert,
* watchlist rename behaves as delete+insert by ``name`` identity.

Every write goes through the backend, as the agent's Write tool
(``awrite_text`` after a Read) or the file mount (``awrite_versioned``) makes
it, so the lock, the version check and the commit are the ones that ship.
"""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
from typing import Any

import pytest

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from ptc_agent.agent.backends.user_data import UserDataBackend
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.database import portfolio as portfolio_db
from src.server.database import user as user_db
from src.server.database import watchlist as watchlist_db
from src.server.services import user_data_io as io
from src.server.services.profile_files import (
    PORTFOLIO_FILE,
    PREFERENCE_FILE,
    WATCHLIST_FILE,
    PortfolioFile,
    PreferenceFile,
    WatchlistFile,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

PROFILE = "/work/.agents/user/profile/"


class _StubSandbox:
    """The backend reads and writes rows, never the sandbox's files."""

    def normalize_path(self, p): return p
    def virtualize_path(self, p): return p
    def validate_path(self, p): return True
    @property
    def filesystem_config(self): return None


def _backend(user_id: str) -> UserDataBackend:
    return UserDataBackend(
        user_id=user_id,
        call=CallContext(),
        sandbox_backend=_StubSandbox(),  # type: ignore[arg-type]
        root_prefix=PROFILE,
    )


def _holding(symbol: str, quantity: str = "1", average_cost: str = "1") -> dict[str, Any]:
    return {
        "symbol": symbol, "instrument_type": "stock",
        "quantity": quantity, "average_cost": average_cost,
        "account_name": "Main",
    }


async def _read(backend: UserDataBackend, filename: str) -> None:
    """The Read a Write is checked against, of the whole file."""
    assert await backend.aread_range(PROFILE + filename) is not None


async def _write(backend: UserDataBackend, filename: str, content: dict) -> Any:
    return await backend.awrite_text(PROFILE + filename, io.serialize_json(content))


async def _save(user_id: str, filename: str, content: dict) -> None:
    """Read then Write, as the agent's tools save."""
    backend = _backend(user_id)
    await _read(backend, filename)
    await _write(backend, filename, content)


# ---------------------------------------------------------------------------
# Portfolio
# ---------------------------------------------------------------------------


class TestPortfolioApply:
    async def test_insert_round_trips(self, seed_user, patched_get_db_connection):
        user_id = seed_user["user_id"]
        assert await io.fetch_portfolio_for_user(user_id) == []

        await _save(user_id, PORTFOLIO_FILE, {"holdings": [_holding("ZZZ", "10", "100.00")]})

        # Round-trip: fetch + serialize sees the new row
        rows_after = await io.fetch_portfolio_for_user(user_id)
        assert len(rows_after) == 1
        assert rows_after[0]["symbol"] == "ZZZ"
        assert rows_after[0]["quantity"] == Decimal("10")

    async def test_update_and_delete(self, seed_user, patched_get_db_connection):
        user_id = seed_user["user_id"]

        await _save(user_id, PORTFOLIO_FILE, {"holdings": [
            _holding("AAA", "1", "10"),
            _holding("BBB", "2", "20"),
        ]})
        # Now drop AAA and update BBB
        await _save(user_id, PORTFOLIO_FILE, {"holdings": [_holding("BBB", "99", "20")]})

        rows_after = await io.fetch_portfolio_for_user(user_id)
        assert {r["symbol"] for r in rows_after} == {"BBB"}
        assert rows_after[0]["quantity"] == Decimal("99")

    async def test_version_conflict_on_concurrent_change(
        self, seed_user, patched_get_db_connection,
    ):
        """A Read taken before another writer landed is stale → version_conflict."""
        user_id = seed_user["user_id"]
        path = PROFILE + PORTFOLIO_FILE
        ours = _backend(user_id)
        await _read(ours, PORTFOLIO_FILE)
        _, stale_version = await ours.aread_versioned(path)

        # A program saves through the mount in between.
        mount = _backend(user_id)
        _, fresh_version = await mount.aread_versioned(path)
        await mount.awrite_versioned(
            path, io.serialize_json({"holdings": [_holding("RACE", "1", "5")]}), fresh_version
        )

        # Our Write over the stale Read must raise
        with pytest.raises(UserDataValidationError) as exc:
            await _write(ours, PORTFOLIO_FILE, {"holdings": [_holding("ZZZ", "1", "5")]})
        assert exc.value.error_type == "version_conflict"
        # The refusal drops that Read, so a retry has to read again.
        with pytest.raises(UserDataValidationError) as retry:
            await _write(ours, PORTFOLIO_FILE, {"holdings": [_holding("ZZZ", "1", "5")]})
        assert retry.value.error_type == "read_required"
        # A mount save over the stale version is refused the same way.
        with pytest.raises(UserDataValidationError) as mounted:
            await mount.awrite_versioned(
                path, io.serialize_json({"holdings": [_holding("ZZZ", "1", "5")]}), stale_version
            )
        assert mounted.value.error_type == "version_conflict"

        # The losing writes did NOT apply
        symbols = {r["symbol"] for r in await io.fetch_portfolio_for_user(user_id)}
        assert symbols == {"RACE"}

    async def test_advisory_lock_serializes_parallel_writers(
        self, seed_user, patched_get_db_connection, monkeypatch,
    ):
        """Two concurrent writes over the same Read: exactly one wins.

        Each writer gets its own in-process lock, as two server workers
        would, so only the advisory lock orders them: the second to take it
        reads the first one's rows and gets version_conflict, so the writes
        don't interleave row-level. Each save holds its read open until the
        other has read too, or half a second, so without the lock both read
        the same rows and both commit.
        """
        user_id = seed_user["user_id"]
        monkeypatch.setattr(db_json_route, "lock_for_namespace", lambda _namespace: asyncio.Lock())
        real_fetch = PortfolioFile.fetch
        reads = 0
        both_read = asyncio.Event()

        async def fetch(self, user_id, conn=None):
            nonlocal reads
            rows = await real_fetch(self, user_id, conn)
            if conn is not None:
                # A save's read, under its transaction.
                reads += 1
                if reads >= 2:
                    both_read.set()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(both_read.wait(), 0.5)
            return rows

        monkeypatch.setattr(PortfolioFile, "fetch", fetch)

        a, b = _backend(user_id), _backend(user_id)
        await _read(a, PORTFOLIO_FILE)
        await _read(b, PORTFOLIO_FILE)

        results = await asyncio.gather(
            _write(a, PORTFOLIO_FILE, {"holdings": [_holding("AAA", "1", "1")]}),
            _write(b, PORTFOLIO_FILE, {"holdings": [_holding("BBB", "2", "2")]}),
            return_exceptions=True,
        )
        conflicts = [r for r in results if isinstance(r, UserDataValidationError)]
        successes = [r for r in results if not isinstance(r, BaseException)]
        # Exactly one succeeded, one raised version_conflict.
        assert len(successes) == 1, results
        assert len(conflicts) == 1, results
        assert conflicts[0].error_type == "version_conflict"

        # Whichever symbol won is the only one present.
        rows_after = await io.fetch_portfolio_for_user(user_id)
        assert len(rows_after) == 1
        assert rows_after[0]["symbol"] in {"AAA", "BBB"}


# ---------------------------------------------------------------------------
# Preferences: other_preference preservation
# ---------------------------------------------------------------------------


class TestPreferenceApply:
    async def test_first_insert_seeds_other_preference_empty(
        self, seed_user, patched_get_db_connection,
    ):
        user_id = seed_user["user_id"]
        await _save(user_id, PREFERENCE_FILE, {
            "risk_preference": {"tolerance": "moderate"},
            "investment_preference": {},
            "agent_preference": {},
        })

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
        await _save(user_id, PREFERENCE_FILE, {"risk_preference": {"tolerance": "low"}})

        # The server slips in onboarding state via direct SQL. Acquire and
        # release the connection eagerly so the pool isn't held during the
        # next write below.
        async with test_db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE user_preferences SET other_preference = %s::jsonb WHERE user_id = %s",
                    ('{"onboarding_step": 3}', user_id),
                )

        # Agent edits its slice
        await _save(user_id, PREFERENCE_FILE, {
            "risk_preference": {"tolerance": "aggressive"},
            "investment_preference": {"style": "growth"},
            "agent_preference": {},
        })

        row = await io.fetch_preferences_for_user(user_id)
        assert row["risk_preference"] == {"tolerance": "aggressive"}
        assert row["investment_preference"] == {"style": "growth"}
        # other_preference must be untouched
        assert row["other_preference"] == {"onboarding_step": 3}


# ---------------------------------------------------------------------------
# Watchlist: rename = delete + insert by name
# ---------------------------------------------------------------------------


class TestWatchlistApply:
    async def test_rename_is_delete_plus_insert(
        self, seed_user, patched_get_db_connection,
    ):
        user_id = seed_user["user_id"]
        items = [{"symbol": "AAPL", "instrument_type": "stock"}]

        await _save(user_id, WATCHLIST_FILE, {"watchlists": [{"name": "Old Name", "items": items}]})
        wls, _ = await io.fetch_watchlist_for_user(user_id)
        old_wl_id = str(wls[0]["watchlist_id"])

        # Rename: agent sees "Old Name", writes "New Name" with the items intact
        await _save(user_id, WATCHLIST_FILE, {"watchlists": [{"name": "New Name", "items": items}]})

        wls_after, items_after = await io.fetch_watchlist_for_user(user_id)
        assert {w["name"] for w in wls_after} == {"New Name"}
        # Brand-new row, so the DB id changed
        new_wl_id = str(wls_after[0]["watchlist_id"])
        assert new_wl_id != old_wl_id
        # Items came along
        assert items_after.get(new_wl_id, [])[0]["symbol"] == "AAPL"


# ---------------------------------------------------------------------------
# UserDataBackend.aread_text smoke: the read path hits the DB
# ---------------------------------------------------------------------------


class TestBackendRead:
    async def test_read_portfolio_via_backend(
        self, seed_user, patched_get_db_connection,
    ):
        """End-to-end through the agent-facing surface, not just io.*."""
        user_id = seed_user["user_id"]
        await _save(user_id, PORTFOLIO_FILE, {"holdings": [_holding("READ", "7", "13.5")]})

        content = await _backend(user_id).aread_text(PROFILE + PORTFOLIO_FILE)
        assert content is not None
        assert "READ" in content
        # Agent-visible JSON does NOT include __version__
        assert "__version__" not in content


# ---------------------------------------------------------------------------
# Dashboard and tool writers against a save in flight
# ---------------------------------------------------------------------------


async def _portfolio_race(user_id: str):
    await _save(user_id, PORTFOLIO_FILE, {"holdings": [_holding("AAA", "1", "10")]})
    [row] = await io.fetch_portfolio_for_user(user_id)

    def crud():
        return portfolio_db.update_portfolio_holding(
            str(row["user_portfolio_id"]), user_id, quantity=Decimal("5")
        )

    def landed(rows):
        assert (rows[0]["quantity"], rows[0]["notes"]) == (Decimal("5"), "trimmed")

    written = {"holdings": [{**_holding("AAA", "1", "10"), "notes": "trimmed"}]}
    return PortfolioFile, PORTFOLIO_FILE, written, crud, landed


async def _watchlist_race(user_id: str):
    aapl = {"symbol": "AAPL", "instrument_type": "stock"}
    await _save(user_id, WATCHLIST_FILE, {"watchlists": [{"name": "Tech", "items": [aapl]}]})
    _, items_by_wl = await io.fetch_watchlist_for_user(user_id)
    [[item]] = items_by_wl.values()

    def crud():
        return watchlist_db.update_watchlist_item(
            str(item["watchlist_item_id"]), user_id, alert_settings={"above": 200}
        )

    def landed(rows):
        [[after]] = rows[1].values()
        assert (after["alert_settings"], after["notes"]) == ({"above": 200}, "core")

    written = {"watchlists": [{"name": "Tech", "items": [{**aapl, "notes": "core"}]}]}
    return WatchlistFile, WATCHLIST_FILE, written, crud, landed


async def _preference_race(user_id: str):
    await _save(user_id, PREFERENCE_FILE, {"risk_preference": {"tolerance": "low"}})

    def crud():
        return user_db.upsert_user_preferences(user_id, agent_preference={"tone": "brief"})

    def landed(row):
        assert (row["risk_preference"], row["agent_preference"]) == (
            {"tolerance": "high"}, {"tone": "brief"},
        )

    written = {"risk_preference": {"tolerance": "high"}}
    return PreferenceFile, PREFERENCE_FILE, written, crud, landed


class TestCrudWriteDuringSave:
    @pytest.mark.parametrize(
        "race", [_portfolio_race, _watchlist_race, _preference_race],
        ids=["portfolio", "watchlist", "preference"],
    )
    async def test_crud_write_is_not_lost(
        self, race, seed_user, patched_get_db_connection, monkeypatch,
    ):
        """A dashboard or tool write that comes while a save holds its read
        lands after the save rather than under it.

        The save starts the CRUD write right after its read and waits up to
        half a second for it. A CRUD write outside the profile lock commits
        in that window, and the save's diff, planned from the rows it read,
        then writes the old values back over it.
        """
        user_id = seed_user["user_id"]
        file_cls, filename, written, crud, landed = await race(user_id)
        real_fetch = file_cls.fetch
        started: list[asyncio.Task] = []

        async def fetch(self, user_id, conn=None):
            rows = await real_fetch(self, user_id, conn)
            if conn is not None and not started:
                started.append(asyncio.create_task(crud()))
                await asyncio.wait(started, timeout=0.5)
            return rows

        monkeypatch.setattr(file_cls, "fetch", fetch)
        await _save(user_id, filename, written)
        await started[0]

        async with patched_get_db_connection() as conn:
            landed(await real_fetch(file_cls(), user_id, conn))
