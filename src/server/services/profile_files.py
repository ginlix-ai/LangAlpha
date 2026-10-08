"""The user's profile as four JSON files: portfolio, watchlists, preferences
and the account itself (``user.json``, the ``users`` row).

Served by ``UserDataBackend`` (`.agents/user/profile/`). ``user_data_io``
parses, diffs and writes each file's rows; this adds what a save needs on top:
the rows as the save's own transaction reads them, the lock every writer of the
profile takes, the rows each plan would delete, named for a refusal, and what
every save that wrote does after its commit.
"""

from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from ptc_agent.agent.backends.db_json_route import DbJsonFile, Plan
from ptc_agent.core.paths import USER_DATA_FILES, SandboxLayout
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.database import user as user_db
from src.server.database.user_lock import lock_user_profile
from src.server.services import user_data_io as io
from src.server.services.onboarding import maybe_complete_onboarding

PORTFOLIO_FILE, WATCHLIST_FILE, PREFERENCE_FILE, USER_FILE = USER_DATA_FILES[SandboxLayout.USER_PROFILE_DIR]

Rows = list[dict[str, Any]]
Watchlists = tuple[Rows, dict[str, Rows]]
Preferences = dict[str, Any] | None
UserRow = dict[str, Any] | None


def _rendered(payload: dict[str, Any]) -> tuple[str, str]:
    # The agent sees the business content only; the version goes beside it.
    version = payload.pop("__version__")
    return io.serialize_json(payload), version


async def _rows(conn: Any, query: str, user_id: str) -> Rows:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, (user_id,))
        return [dict(row) for row in await cur.fetchall()]


async def _drop_caches(user_id: str) -> None:
    # The prompt's profile holds the account and the agent's preferences,
    # and the preference cache is what the onboarding rule reads next.
    await user_db.invalidate_user_profile_cache(user_id)
    await user_db.invalidate_user_prefs_cache(user_id)


class _ProfileFile[R, P, C](DbJsonFile[R, P, C]):
    async def lock(self, user_id: str, conn: Any) -> None:
        """Same key for all four files, so an in-flight write of any one
        blocks the others: the version is per file, and interleaved writes
        across files can still produce inconsistent reads. The dashboard
        writers of the other three take it too, which is why they need no
        ``hold``."""
        async with conn.cursor() as cur:
            await lock_user_profile(cur, user_id)

    async def committed(self, user_id: str, changes: C) -> None:
        """After a save that wrote: drop what caches the rows, then complete
        onboarding by its fallback rule, since a save can be the one that
        meets it. Dropped after the commit, or a reader could cache the old
        rows again."""
        if changes:
            await _drop_caches(user_id)
            await maybe_complete_onboarding(user_id)


class PortfolioFile(_ProfileFile[Rows, Rows, io.PortfolioDiff]):
    async def fetch(self, user_id: str, conn: Any = None) -> Rows:
        if conn is None:
            return await io.fetch_portfolio_for_user(user_id)
        return await _rows(
            conn,
            """
            SELECT
                user_portfolio_id, user_id, symbol, instrument_type, exchange,
                name, quantity, average_cost, currency, account_name,
                notes, metadata, first_purchased_at, created_at, updated_at
            FROM user_portfolios
            WHERE user_id = %s
            ORDER BY created_at DESC
            """,
            user_id,
        )

    def render(self, rows: Rows) -> tuple[str, str]:
        return _rendered(io.serialize_portfolio(rows))

    async def parse(self, user_id: str, call: CallContext, content: str, served: str | None) -> Rows:
        return io.parse_portfolio(content)

    def plan(self, call: CallContext, parsed: Rows, rows: Rows) -> Plan[io.PortfolioDiff]:
        diff = io.diff_portfolio(parsed, rows)
        gone = set(diff.deletes)
        return Plan(diff, [_holding(r) for r in rows if str(r["user_portfolio_id"]) in gone])

    async def commit(self, user_id: str, changes: io.PortfolioDiff, conn: Any) -> None:
        async with conn.cursor() as cur:
            await io.write_portfolio_diff(cur, changes, user_id)


class WatchlistFile(_ProfileFile[Watchlists, Rows, io.WatchlistDiff]):
    async def fetch(self, user_id: str, conn: Any = None) -> Watchlists:
        """ORDER BY clauses match the reads outside a save
        (``watchlist_db.get_user_watchlists`` / ``get_all_user_watchlist_items``),
        so both hash the rows in the same order."""
        if conn is None:
            return await io.fetch_watchlist_for_user(user_id)
        watchlists = await _rows(
            conn,
            """
            SELECT watchlist_id, user_id, name, description, is_default,
                   display_order, created_at, updated_at
            FROM watchlists
            WHERE user_id = %s
            ORDER BY is_default DESC, display_order ASC, created_at ASC
            """,
            user_id,
        )
        items_by_wl: dict[str, Rows] = {}
        for item in await _rows(
            conn,
            """
            SELECT wi.watchlist_item_id, wi.watchlist_id, wi.user_id, wi.symbol,
                   wi.instrument_type, wi.exchange, wi.name, wi.notes,
                   wi.alert_settings, wi.metadata, wi.created_at, wi.updated_at
            FROM watchlist_items wi
            INNER JOIN watchlists w ON wi.watchlist_id = w.watchlist_id
            WHERE w.user_id = %s
            ORDER BY wi.created_at DESC
            """,
            user_id,
        ):
            items_by_wl.setdefault(str(item["watchlist_id"]), []).append(item)
        return watchlists, items_by_wl

    def render(self, rows: Watchlists) -> tuple[str, str]:
        return _rendered(io.serialize_watchlist(*rows))

    async def parse(self, user_id: str, call: CallContext, content: str, served: str | None) -> Rows:
        return io.parse_watchlist(content)

    def plan(self, call: CallContext, parsed: Rows, rows: Watchlists) -> Plan[io.WatchlistDiff]:
        watchlists, items_by_wl = rows
        diff = io.diff_watchlist(parsed, watchlists, items_by_wl)
        names = {str(w["watchlist_id"]): w["name"] for w in watchlists}
        gone = set(diff.item_deletes)
        deletes = [f'watchlist "{names[w]}"' for w in diff.wl_deletes]
        deletes += [
            f'"{item["symbol"]}" from "{names[wl_id]}"'
            for wl_id, items in items_by_wl.items()
            for item in items
            if str(item["watchlist_item_id"]) in gone
        ]
        return Plan(diff, deletes)

    async def commit(self, user_id: str, changes: io.WatchlistDiff, conn: Any) -> None:
        async with conn.cursor() as cur:
            await io.write_watchlist_diff(cur, changes, user_id)


# What a save writes: the values, over the row the version check read.
PreferenceChanges = tuple[dict[str, Any], Preferences] | None


class PreferenceFile(_ProfileFile[Preferences, dict[str, Any], PreferenceChanges]):
    async def fetch(self, user_id: str, conn: Any = None) -> Preferences:
        if conn is None:
            return await io.fetch_preferences_for_user(user_id)
        found = await _rows(
            conn,
            """
            SELECT user_preference_id, user_id, risk_preference, investment_preference,
                   agent_preference, other_preference, created_at, updated_at
            FROM user_preferences
            WHERE user_id = %s
            """,
            user_id,
        )
        return found[0] if found else None

    def render(self, rows: Preferences) -> tuple[str, str]:
        return _rendered(io.serialize_preferences(rows))

    async def parse(
        self, user_id: str, call: CallContext, content: str, served: str | None
    ) -> dict[str, Any]:
        return io.parse_preferences(content)

    def plan(self, call: CallContext, parsed: dict[str, Any], rows: Preferences) -> Plan[PreferenceChanges]:
        # A rewrite with identical content opens no write.
        if io.preferences_equal(rows, parsed):
            return Plan(None)
        cleared = sorted(k for k, v in parsed.items() if not v and (rows or {}).get(k))
        return Plan((parsed, rows), cleared)

    async def commit(self, user_id: str, changes: PreferenceChanges, conn: Any) -> None:
        if changes is not None:
            async with conn.cursor() as cur:
                await io.write_preferences(cur, *changes, user_id)


# The columns a save sets, by name; empty when it sets none.
UserChanges = dict[str, Any]


class UserFile(_ProfileFile[UserRow, dict[str, Any], UserChanges]):
    async def fetch(self, user_id: str, conn: Any = None) -> UserRow:
        """During a save, the row is locked too: the account's own endpoint
        writes it without the profile lock, and a save must not land over a
        change its version check never saw."""
        if conn is None:
            return await io.fetch_user_for_user(user_id)
        found = await _rows(
            conn,
            """
            SELECT name, timezone, locale, onboarding_completed
            FROM users
            WHERE user_id = %s
            FOR NO KEY UPDATE
            """,
            user_id,
        )
        return found[0] if found else None

    def render(self, rows: UserRow) -> tuple[str, str]:
        return _rendered(io.serialize_user(rows))

    async def parse(
        self, user_id: str, call: CallContext, content: str, served: str | None
    ) -> dict[str, Any]:
        return io.parse_user(content)

    def plan(self, call: CallContext, parsed: dict[str, Any], rows: UserRow) -> Plan[UserChanges]:
        return Plan(io.diff_user(parsed, rows))

    async def commit(self, user_id: str, changes: UserChanges, conn: Any) -> None:
        if changes:
            async with conn.cursor() as cur:
                await io.write_user(cur, changes, user_id)

    async def committed(self, user_id: str, changes: UserChanges) -> None:
        if "onboarding_completed" in changes:
            # The writer set it, and the fallback rule would undo a reset.
            await _drop_caches(user_id)
        else:
            await super().committed(user_id, changes)


PROFILE_FILES = {
    PORTFOLIO_FILE: PortfolioFile(),
    WATCHLIST_FILE: WatchlistFile(),
    PREFERENCE_FILE: PreferenceFile(),
    USER_FILE: UserFile(),
}


def _holding(row: dict[str, Any]) -> str:
    account = row.get("account_name")
    return f'"{row["symbol"]}" ({row["instrument_type"]}{f", {account}" if account else ""})'
