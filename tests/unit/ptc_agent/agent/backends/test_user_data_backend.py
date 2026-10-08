"""Unit tests for ``UserDataBackend``.

The DB layer is mocked: we patch the io module's read, serialize, parse and
write functions and fake the save's connection, whose cursor returns the rows
the save reads, exercising the backend's dispatch, caching, error mapping, and
version-conflict semantics in isolation.
End-to-end DB coverage (advisory lock, version_conflict races, real SQL
round-trips) lives in ``tests/integration/test_user_data_backend.py``
(requires Postgres).
"""

from __future__ import annotations

import asyncio
import contextvars
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from psycopg.errors import StringDataRightTruncation

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from ptc_agent.agent.backends.user_data import (
    PORTFOLIO_FILE,
    PREFERENCE_FILE,
    README_FILE,
    USER_FILE,
    WATCHLIST_FILE,
    UserDataBackend,
    _README_CONTENT,
)
from ptc_agent.agent.middleware.background_subagent.context import current_background_agent_id
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.services import profile_files


PREFIX = "/home/workspace/.agents/user/profile/"
PORTFOLIO_PATH = f"{PREFIX}{PORTFOLIO_FILE}"
WATCHLIST_PATH = f"{PREFIX}{WATCHLIST_FILE}"
PREFERENCE_PATH = f"{PREFIX}{PREFERENCE_FILE}"
USER_PATH = f"{PREFIX}{USER_FILE}"
README_PATH = f"{PREFIX}{README_FILE}"
USER = "user-1"
IO = "src.server.services.profile_files.io"
LOCK = "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))"


class _Conn:
    """The save's connection: records how its transaction ended. Its cursor
    returns the rows the save reads, none unless a test sets them."""

    def __init__(self) -> None:
        self.cursor_obj = MagicMock()
        self.cursor_obj.connection = self
        self.cursor_obj.execute = AsyncMock()
        self.cursor_obj.fetchall = AsyncMock(return_value=[])
        self.outcome: str | None = None

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.outcome = "rolled back"
            raise
        self.outcome = "committed"

    @asynccontextmanager
    async def cursor(self, **_kwargs):
        yield self.cursor_obj

    def lock_keys(self) -> list[tuple[str, ...]]:
        return [c.args[1] for c in self.cursor_obj.execute.await_args_list if c.args[0] == LOCK]


@pytest.fixture
def conn(monkeypatch) -> _Conn:
    fake = _Conn()

    @asynccontextmanager
    async def _connection():
        yield fake

    monkeypatch.setattr(db_json_route, "get_db_connection", _connection)
    return fake


@pytest.fixture
def mock_io():
    with patch(IO) as io:
        io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        io.fetch_watchlist_for_user = AsyncMock(return_value=([], {}))
        io.fetch_preferences_for_user = AsyncMock(return_value=None)
        io.fetch_user_for_user = AsyncMock(return_value=None)
        io.write_portfolio_diff = AsyncMock()
        io.write_watchlist_diff = AsyncMock()
        io.write_preferences = AsyncMock()
        yield io


@pytest.fixture
def prefs_cache(monkeypatch) -> AsyncMock:
    mock = AsyncMock()
    monkeypatch.setattr(profile_files.user_db, "invalidate_user_prefs_cache", mock)
    return mock


@pytest.fixture(autouse=True)
def _after_commit(monkeypatch) -> None:
    """What a save that wrote runs next reaches Redis and the database, which
    these tests fake no further than the save's own connection."""
    monkeypatch.setattr(profile_files.user_db, "invalidate_user_profile_cache", AsyncMock())
    monkeypatch.setattr(profile_files, "maybe_complete_onboarding", AsyncMock(return_value=False))


def _serve(mock_io, version: str, content: str = '{"holdings": []}\n', file: str = PORTFOLIO_FILE) -> None:
    """What ``file`` renders to from here on, a Read's or the save's own, and
    its version."""
    serializer = {
        PORTFOLIO_FILE: "serialize_portfolio",
        WATCHLIST_FILE: "serialize_watchlist",
        PREFERENCE_FILE: "serialize_preferences",
    }[file]
    setattr(mock_io, serializer, MagicMock(side_effect=lambda *_: {"__version__": version}))
    mock_io.serialize_json = MagicMock(return_value=content)


def _as_subagent(agent_id: str, work):
    """Run ``work`` as the background subagent ``agent_id``, in a context of
    its own, as the subagent's tool calls run beside the main agent's."""
    context = contextvars.copy_context()
    context.run(current_background_agent_id.set, agent_id)
    return asyncio.create_task(work, context=context)


def _make_sandbox():
    sb = MagicMock()
    sb.root_dir = "/home/workspace"
    sb.normalize_path.side_effect = lambda p: p if p.startswith("/") else f"/home/workspace/{p}"
    sb.virtualize_path.side_effect = lambda p: p
    sb.validate_path.return_value = True
    sb.filesystem_config.enable_path_validation = True
    return sb


@pytest.fixture
def backend():
    return UserDataBackend(
        user_id="user-1",
        call=CallContext(),
        sandbox_backend=_make_sandbox(),
        root_prefix=PREFIX,
    )


def _portfolio_row(version_ts):
    return {
        "user_portfolio_id": "11111111-1111-1111-1111-111111111111",
        "user_id": "user-1",
        "symbol": "AAPL",
        "instrument_type": "stock",
        "quantity": Decimal("100"),
        "average_cost": Decimal("150.25"),
        "currency": "USD",
        "account_name": "Main",
        "updated_at": version_ts,
    }


# ---------------------------------------------------------------------------
# Routing + path semantics
# ---------------------------------------------------------------------------


class TestRouting:
    def test_root_prefix_is_normalized(self, backend):
        assert backend.root_prefix.endswith("/")

    def test_filename_recognized(self, backend):
        assert backend._filename(PORTFOLIO_PATH) == PORTFOLIO_FILE
        assert backend._filename(WATCHLIST_PATH) == WATCHLIST_FILE
        assert backend._filename(PREFERENCE_PATH) == PREFERENCE_FILE

    def test_filename_unknown_returns_none(self, backend):
        assert backend._filename(f"{PREFIX}other.json") is None
        # Directory itself is not a file
        assert backend._filename(PREFIX.rstrip("/")) is None
        # Subdirectory not handled
        assert backend._filename(f"{PREFIX}sub/portfolio.json") is None


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------


class TestRead:
    @pytest.mark.asyncio
    @patch(IO)
    async def test_every_read_is_fresh(self, mock_io, backend):
        """A second Read in the turn shows rows changed since the first one, and
        moves the version a later write is checked against."""
        ts = datetime(2026, 5, 17, 8, 34, 11, tzinfo=timezone.utc)
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[_portfolio_row(ts)])
        mock_io.serialize_portfolio = MagicMock(
            side_effect=[{"__version__": "v1"}, {"__version__": "v2"}]
        )
        mock_io.serialize_json = MagicMock(side_effect=['{"holdings": []}', '{"holdings": [1]}'])

        result_a = await backend.aread_range(PORTFOLIO_PATH)
        result_b = await backend.aread_range(PORTFOLIO_PATH)

        assert (result_a, result_b) == ('{"holdings": []}', '{"holdings": [1]}')
        assert mock_io.fetch_portfolio_for_user.await_count == 2
        assert backend._read_cache["portfolio.json"].version == "v2"

    @pytest.mark.asyncio
    async def test_read_unknown_file_returns_none(self, backend):
        result = await backend.aread_text(f"{PREFIX}other.json")
        assert result is None

    @pytest.mark.asyncio
    @patch(IO)
    async def test_read_swallows_errors_and_returns_none(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(side_effect=RuntimeError("boom"))
        result = await backend.aread_text(PORTFOLIO_PATH)
        assert result is None


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


class TestWrite:
    @pytest.mark.asyncio
    async def test_write_unknown_file_names_the_real_ones(self, backend):
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(f"{PREFIX}other.json", "{}")
        assert "can't be created" in exc.value.hint
        assert "watchlist.json" in exc.value.hint

    @pytest.mark.asyncio
    async def test_write_without_prior_read_rejected(self, backend, mock_io, conn):
        """No Read in this run → read_required, naming what starts a new run.
        The agent's payload carries no version, so the backend must have
        served the file at least once to know what version to compare to."""
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "read_required"
        assert "since this run started" in exc.value.hint
        assert conn.outcome is None
        mock_io.write_portfolio_diff.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_write_version_conflict_when_db_moved(self, backend, mock_io, conn):
        """Read serves version A; DB moves to B; Write detects the mismatch
        under the profile lock."""
        _serve(mock_io, "v1")
        await backend.aread_range(PORTFOLIO_PATH)
        _serve(mock_io, "v2-moved")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "version_conflict"
        assert exc.value.hint == (
            f"{PORTFOLIO_PATH} changed since your last Read, so this write could undo that change. "
            f"Read({PORTFOLIO_PATH}) again and reapply your change."
        )
        # One key for the user's three files, so a save of one waits on the others.
        assert conn.lock_keys() == [("userdata:profile:user-1",)]
        mock_io.diff_portfolio.assert_not_called()
        mock_io.write_portfolio_diff.assert_not_awaited()
        assert conn.outcome == "rolled back"
        # Cache is invalidated on conflict so retries fetch fresh data
        assert PORTFOLIO_FILE not in backend._read_cache

    @pytest.mark.asyncio
    async def test_one_agents_read_never_vouches_for_anothers_write(self, backend, mock_io, conn):
        """The main agent read v1, the rows moved, then a subagent sharing the
        route read v2. The main agent's Write, made from v1, conflicts rather
        than landing over a change it never saw, and the subagent's Read
        still stands behind the subagent's own Write."""
        _serve(mock_io, "v1")
        await backend.aread_range(PORTFOLIO_PATH)
        _serve(mock_io, "v2")
        await _as_subagent("research:1", backend.aread_range(PORTFOLIO_PATH))
        mock_io.diff_portfolio.return_value = MagicMock(deletes=[])

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "version_conflict"
        mock_io.write_portfolio_diff.assert_not_awaited()
        assert await _as_subagent("research:1", backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')) is True
        mock_io.write_portfolio_diff.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_write_happy_path_invalidates_cache(self, backend, mock_io, conn):
        _serve(mock_io, "v1")
        await backend.aread_range(PORTFOLIO_PATH)
        assert PORTFOLIO_FILE in backend._read_cache
        rows = [_portfolio_row(None)]
        conn.cursor_obj.fetchall.return_value = rows
        diff = MagicMock(deletes=[])
        mock_io.diff_portfolio.return_value = diff

        ok = await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert ok is True
        assert PORTFOLIO_FILE not in backend._read_cache
        # The diff is planned from the rows the version check read, and
        # written on the same cursor, in the transaction holding the lock.
        mock_io.parse_portfolio.assert_called_once_with('{"holdings":[]}')
        mock_io.diff_portfolio.assert_called_once_with(mock_io.parse_portfolio.return_value, rows)
        mock_io.write_portfolio_diff.assert_awaited_once_with(conn.cursor_obj, diff, USER)
        assert conn.outcome == "committed"

    @pytest.mark.asyncio
    async def test_write_after_a_partial_read_may_not_delete_rows(self, backend, mock_io, conn):
        _serve(mock_io, "v1", content='{\n"holdings": []\n}')
        await backend.aread_range(PORTFOLIO_PATH, offset=0, limit=1)
        row = _portfolio_row(None)
        conn.cursor_obj.fetchall.return_value = [row]
        mock_io.diff_portfolio.return_value = MagicMock(deletes=[row["user_portfolio_id"]])

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "incomplete_read"
        assert exc.value.hint.startswith('this write leaves out "AAPL" (stock, Main), which would delete it,')
        mock_io.write_portfolio_diff.assert_not_awaited()
        assert conn.outcome == "rolled back"

    @pytest.mark.asyncio
    async def test_a_write_that_deletes_nothing_needs_no_whole_read(self, backend, mock_io, conn):
        _serve(mock_io, "v1", content='{\n"holdings": []\n}')
        await backend.aread_range(PORTFOLIO_PATH, offset=0, limit=1)
        mock_io.diff_portfolio.return_value = MagicMock(deletes=[])

        assert await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}') is True
        mock_io.write_portfolio_diff.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_watchlist_deletes_name_each_list_and_item(self, backend, mock_io, conn):
        _serve(mock_io, "v1", content='{\n"watchlists": []\n}', file=WATCHLIST_FILE)
        await backend.aread_range(WATCHLIST_PATH, offset=0, limit=1)
        watchlists = [{"watchlist_id": "wl-1", "name": "Tech"}, {"watchlist_id": "wl-2", "name": "Energy"}]
        items = {"wl-2": [{"watchlist_item_id": "item-1", "watchlist_id": "wl-2", "symbol": "XOM"}]}
        conn.cursor_obj.fetchall.side_effect = [watchlists, items["wl-2"]]
        mock_io.diff_watchlist.return_value = MagicMock(wl_deletes=["wl-1"], item_deletes=["item-1"])

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(WATCHLIST_PATH, '{"watchlists":[]}')

        assert 'leaves out watchlist "Tech", "XOM" from "Energy", which would delete them' in exc.value.hint
        mock_io.write_watchlist_diff.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_parse_refusal_points_at_the_readme_and_writes_nothing(self, backend, mock_io, conn):
        _serve(mock_io, "v1")
        await backend.aread_range(PORTFOLIO_PATH)
        mock_io.parse_portfolio.side_effect = UserDataValidationError(
            "schema_error", "portfolio.json", "holdings[0].quantity", "must be >= 0"
        )

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.readme == README_PATH
        assert exc.value.message.endswith(f"See {README_PATH} for the fields and examples.")
        mock_io.write_portfolio_diff.assert_not_awaited()
        # Refused before the save read or locked anything.
        assert conn.outcome is None
        assert conn.lock_keys() == []
        # Only a conflict spends the Read; a refused write may be fixed and retried.
        assert PORTFOLIO_FILE in backend._read_cache

    @pytest.mark.asyncio
    async def test_db_exception_is_a_server_error_not_the_agents(self, backend, mock_io, conn):
        _serve(mock_io, "v1")
        await backend.aread_range(PORTFOLIO_PATH)
        mock_io.diff_portfolio.return_value = MagicMock(deletes=[])
        mock_io.write_portfolio_diff.side_effect = RuntimeError("duplicate key")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "server_error"
        assert "don't add test entries" in exc.value.hint
        assert conn.outcome == "rolled back"

    @pytest.mark.asyncio
    async def test_a_value_the_column_cannot_hold_is_the_contents_problem(self, backend, mock_io, conn):
        _serve(mock_io, "v1")
        await backend.aread_range(PORTFOLIO_PATH)
        mock_io.diff_portfolio.return_value = MagicMock(deletes=[])
        mock_io.write_portfolio_diff.side_effect = StringDataRightTruncation("value too long for (Sell all TSLA)")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "schema_error"
        assert "(StringDataRightTruncation)" in exc.value.hint
        assert "Sell all TSLA" not in exc.value.message


class TestPreferenceWrite:
    @staticmethod
    async def _read(backend, mock_io) -> None:
        _serve(mock_io, "v1", file=PREFERENCE_FILE)
        await backend.aread_range(PREFERENCE_PATH)

    @pytest.mark.asyncio
    async def test_an_identical_rewrite_writes_nothing(self, backend, mock_io, conn, prefs_cache):
        await self._read(backend, mock_io)
        mock_io.parse_preferences.return_value = {"risk_preference": {}}
        mock_io.preferences_equal.return_value = True

        assert await backend.awrite_text(PREFERENCE_PATH, "{}") is True

        mock_io.write_preferences.assert_not_awaited()
        prefs_cache.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_change_is_written_over_the_checked_row_then_the_cache_dropped(
        self, backend, mock_io, conn, prefs_cache
    ):
        await self._read(backend, mock_io)
        current = {"risk_preference": {"tolerance": "high"}}
        values = {"risk_preference": {"tolerance": "low"}}
        conn.cursor_obj.fetchall.return_value = [current]
        mock_io.parse_preferences.return_value = values
        mock_io.preferences_equal.return_value = False
        # Dropped after the commit: before it, a reader could cache the old row again.
        seen: list[str | None] = []
        prefs_cache.side_effect = lambda _user_id: seen.append(conn.outcome)

        assert await backend.awrite_text(PREFERENCE_PATH, "{}") is True

        mock_io.write_preferences.assert_awaited_once_with(conn.cursor_obj, values, current, USER)
        prefs_cache.assert_awaited_once_with(USER)
        assert seen == ["committed"]

    @pytest.mark.asyncio
    async def test_clearing_a_key_after_a_partial_read_is_refused(self, backend, mock_io, conn, prefs_cache):
        _serve(mock_io, "v1", content="{\n}\n", file=PREFERENCE_FILE)
        await backend.aread_range(PREFERENCE_PATH, offset=0, limit=1)
        conn.cursor_obj.fetchall.return_value = [{"risk_preference": {"tolerance": "high"}}]
        mock_io.parse_preferences.return_value = {"risk_preference": {}}
        mock_io.preferences_equal.return_value = False

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PREFERENCE_PATH, "{}")

        assert exc.value.error_type == "incomplete_read"
        assert "leaves out risk_preference" in exc.value.hint
        mock_io.write_preferences.assert_not_awaited()
        prefs_cache.assert_not_awaited()


# ---------------------------------------------------------------------------
# Edit path
# ---------------------------------------------------------------------------


class TestEdit:
    @pytest.mark.asyncio
    async def test_edit_unknown_file(self, backend):
        result = await backend.aedit_text(f"{PREFIX}other.json", "a", "b")
        assert result["success"] is False
        assert "File not found" in result["error"]

    @pytest.mark.asyncio
    async def test_edit_identical_strings_rejected(self, backend):
        result = await backend.aedit_text(PORTFOLIO_PATH, "x", "x")
        assert result["success"] is False

    @pytest.mark.asyncio
    @patch(IO)
    async def test_edit_string_not_found(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.serialize_json = MagicMock(return_value='{"__version__":"v1"}')

        result = await backend.aedit_text(PORTFOLIO_PATH, "missing-string", "replacement")
        assert result["success"] is False
        assert "String not found" in result["error"]

    @pytest.mark.asyncio
    async def test_edit_user_data_error_returns_failure_dict(self, backend, mock_io, conn):
        _serve(mock_io, "v1", content='{"holdings":[{"quantity": "1"}]}')
        mock_io.parse_portfolio.side_effect = UserDataValidationError(
            error_type="schema_error", file="portfolio.json", field_path="root", hint="bad",
        )

        result = await backend.aedit_text(PORTFOLIO_PATH, '"1"', '"-1"')

        assert result["success"] is False
        assert result["error"].startswith("schema_error:portfolio.json:root: bad")
        assert result["error"].endswith(f"See {README_PATH} for the fields and examples.")

    @pytest.mark.asyncio
    async def test_an_edit_without_a_read_is_checked_against_the_file_it_edited(self, backend, mock_io, conn):
        """old_string has to match the live file, so the Edit carries that
        file's version, and the live load is no Read a later Write can use."""
        _serve(mock_io, "v1", content='{"holdings":[{"quantity": "1"}]}')
        mock_io.diff_portfolio.return_value = MagicMock(deletes=[])

        result = await backend.aedit_text(PORTFOLIO_PATH, '"1"', '"2"')

        assert result == {"success": True, "occurrences": 1, "size": 32, "message": f"Edited {PORTFOLIO_PATH}"}
        mock_io.parse_portfolio.assert_called_once_with('{"holdings":[{"quantity": "2"}]}')
        mock_io.diff_portfolio.assert_called_once_with(mock_io.parse_portfolio.return_value, [])
        assert backend._read_cache == {}


# ---------------------------------------------------------------------------
# Glob + grep
# ---------------------------------------------------------------------------


class TestGlobGrep:
    @pytest.mark.asyncio
    async def test_glob_matches_known_files(self, backend):
        results = await backend.aglob_paths("*.json", PREFIX)
        assert PORTFOLIO_PATH in results
        assert WATCHLIST_PATH in results
        assert PREFERENCE_PATH in results
        assert USER_PATH in results
        assert len(results) == 4

    @pytest.mark.asyncio
    async def test_glob_outside_prefix_empty(self, backend):
        results = await backend.aglob_paths("*.json", "/home/workspace/other")
        assert results == []

    @pytest.mark.asyncio
    @patch(IO)
    async def test_grep_finds_pattern(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.fetch_watchlist_for_user = AsyncMock(return_value=([], {}))
        mock_io.fetch_preferences_for_user = AsyncMock(return_value=None)
        mock_io.fetch_user_for_user = AsyncMock(return_value=None)
        # serialize_* must include __version__ since _read_serialized extracts
        # it from the payload before serializing to JSON.
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1"})
        mock_io.serialize_watchlist = MagicMock(return_value={"__version__": "v1"})
        mock_io.serialize_preferences = MagicMock(return_value={"__version__": "v1"})
        mock_io.serialize_user = MagicMock(return_value={"__version__": "v1"})
        # Return JSON content with a pattern in the portfolio file only
        mock_io.serialize_json = MagicMock(side_effect=[
            '{"holdings":[{"symbol":"AAPL"}]}',
            '{"watchlists":[]}',
            '{"prefs":{}}',
            '{"name":null}',
        ])

        results = await backend.agrep_rich("AAPL", path=PREFIX)
        # Only portfolio.json matched
        assert any("portfolio.json" in r for r in results)
        assert not any("watchlist.json" in r for r in results)


# ---------------------------------------------------------------------------
# README.md (virtual schema doc)
# ---------------------------------------------------------------------------


class TestReadme:
    @pytest.mark.asyncio
    async def test_read_returns_schema_doc(self, backend):
        content = await backend.aread_text(README_PATH)
        assert content == _README_CONTENT
        assert "portfolio.json" in content
        assert "watchlist.json" in content
        assert "preference.json" in content

    @pytest.mark.asyncio
    async def test_read_does_not_touch_db(self, backend):
        """README is static — no DB calls even if the io layer is broken."""
        with patch(IO) as mock_io:
            mock_io.fetch_portfolio_for_user = AsyncMock(side_effect=AssertionError("DB hit"))
            mock_io.fetch_watchlist_for_user = AsyncMock(side_effect=AssertionError("DB hit"))
            mock_io.fetch_preferences_for_user = AsyncMock(side_effect=AssertionError("DB hit"))
            content = await backend.aread_text(README_PATH)
        assert content == _README_CONTENT

    @pytest.mark.asyncio
    async def test_write_rejected(self, backend):
        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(README_PATH, "anything")
        assert exc.value.error_type == "schema_error"
        assert "documentation" in exc.value.hint

    @pytest.mark.asyncio
    async def test_edit_rejected(self, backend):
        result = await backend.aedit_text(README_PATH, "portfolio.json", "p.json")
        assert result["success"] is False
        assert "documentation" in result["error"]

    @pytest.mark.asyncio
    async def test_glob_star_includes_readme(self, backend):
        results = await backend.aglob_paths("*", PREFIX)
        assert README_PATH in results
        assert PORTFOLIO_PATH in results
        assert WATCHLIST_PATH in results
        assert PREFERENCE_PATH in results

    @pytest.mark.asyncio
    async def test_glob_json_excludes_readme(self, backend):
        results = await backend.aglob_paths("*.json", PREFIX)
        assert README_PATH not in results
        assert len(results) == 4

    @pytest.mark.asyncio
    async def test_aread_range_works(self, backend):
        head = await backend.aread_range(README_PATH, offset=0, limit=5)
        assert head is not None
        assert head.startswith("# User Profile Data")
