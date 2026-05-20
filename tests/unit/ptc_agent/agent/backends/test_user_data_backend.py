"""Unit tests for ``UserDataBackend``.

The DB layer is mocked: we patch the io module's fetch / count / apply
functions and exercise the backend's dispatch, caching, error mapping, and
version-conflict semantics in isolation. End-to-end DB coverage (advisory
lock, version_conflict races, real SQL round-trips) lives in
``tests/integration/test_user_data_backend.py`` (requires Postgres).
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.backends.user_data import (
    PORTFOLIO_FILE,
    PREFERENCE_FILE,
    README_FILE,
    WATCHLIST_FILE,
    UserDataBackend,
    _README_CONTENT,
)
from src.server.services.user_data_io import UserDataValidationError


PREFIX = "/home/workspace/.agents/user/profile/"
PORTFOLIO_PATH = f"{PREFIX}{PORTFOLIO_FILE}"
WATCHLIST_PATH = f"{PREFIX}{WATCHLIST_FILE}"
PREFERENCE_PATH = f"{PREFIX}{PREFERENCE_FILE}"
README_PATH = f"{PREFIX}{README_FILE}"


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
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_read_portfolio_dispatches_and_caches(self, mock_io, backend):
        ts = datetime(2026, 5, 17, 8, 34, 11, tzinfo=timezone.utc)
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[_portfolio_row(ts)])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.serialize_json = MagicMock(return_value='{"__version__": "v1"}')

        result_a = await backend.aread_text(PORTFOLIO_PATH)
        result_b = await backend.aread_text(PORTFOLIO_PATH)

        assert result_a == '{"__version__": "v1"}'
        assert result_b == result_a
        # Cache hit on second call — fetch only ran once
        assert mock_io.fetch_portfolio_for_user.await_count == 1

    @pytest.mark.asyncio
    async def test_read_unknown_file_returns_none(self, backend):
        result = await backend.aread_text(f"{PREFIX}other.json")
        assert result is None

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_read_swallows_errors_and_returns_none(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(side_effect=RuntimeError("boom"))
        result = await backend.aread_text(PORTFOLIO_PATH)
        assert result is None


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


class TestWrite:
    @pytest.mark.asyncio
    async def test_write_unknown_file_returns_false(self, backend):
        result = await backend.awrite_text(f"{PREFIX}other.json", "{}")
        assert result is False

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_write_without_prior_read_rejected(self, mock_io, backend):
        """No fresh Read in this turn → version_conflict with 'no fresh read' hint.
        The agent's payload no longer carries the version, so the backend must
        have served the file at least once to know what version to compare to."""
        mock_io.UserDataValidationError = UserDataValidationError
        mock_io.apply_portfolio_diff = AsyncMock()

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "version_conflict"
        assert "no fresh read" in exc.value.hint
        mock_io.apply_portfolio_diff.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_write_version_conflict_when_db_moved(self, mock_io, backend):
        """Read serves version A; DB moves to B; Write detects the mismatch."""
        # First read pins cache to "v1"
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.serialize_json = MagicMock(return_value='{"holdings":[]}')
        await backend.aread_text(PORTFOLIO_PATH)

        # Then DB changes — next serialize returns a different version
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v2-moved", "holdings": []})
        mock_io.parse_and_diff_portfolio = MagicMock(return_value=MagicMock(is_empty=lambda: False))
        mock_io.apply_portfolio_diff = AsyncMock()
        mock_io.UserDataValidationError = UserDataValidationError

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "version_conflict"
        mock_io.apply_portfolio_diff.assert_not_awaited()
        # Cache is invalidated on conflict so retries fetch fresh data
        assert PORTFOLIO_FILE not in backend._read_cache

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_write_happy_path_invalidates_cache(self, mock_io, backend):
        # Seed cache from a read first
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.serialize_json = MagicMock(return_value='{"__version__":"v1"}')
        await backend.aread_text(PORTFOLIO_PATH)
        assert PORTFOLIO_FILE in backend._read_cache

        mock_io.parse_and_diff_portfolio = MagicMock(return_value=MagicMock(is_empty=lambda: False))
        mock_io.apply_portfolio_diff = AsyncMock()
        mock_io.UserDataValidationError = UserDataValidationError

        ok = await backend.awrite_text(PORTFOLIO_PATH, '{"__version__":"v1","holdings":[]}')
        assert ok is True
        assert PORTFOLIO_FILE not in backend._read_cache
        mock_io.apply_portfolio_diff.assert_awaited_once()
        # CAS plumbing: backend passes the current version as payload_version
        _, kwargs = mock_io.apply_portfolio_diff.await_args
        assert kwargs == {"payload_version": "v1"}

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_write_cas_conflict_from_applier_surfaces(self, mock_io, backend):
        """A version_conflict raised by the CAS layer (rowcount mismatch) is propagated."""
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.parse_and_diff_portfolio = MagicMock(return_value=MagicMock(is_empty=lambda: False))
        mock_io.apply_portfolio_diff = AsyncMock(
            side_effect=UserDataValidationError(
                "version_conflict", "portfolio.json", "holdings[id=abc]",
                "row was modified by another writer",
            )
        )
        mock_io.UserDataValidationError = UserDataValidationError

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"__version__":"v1","holdings":[]}')

        assert exc.value.error_type == "version_conflict"

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_db_exception_wrapped_as_constraint_error(self, mock_io, backend):
        # Seed cache — writes require a prior Read in the same turn.
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.serialize_json = MagicMock(return_value='{"holdings":[]}')
        await backend.aread_text(PORTFOLIO_PATH)

        mock_io.parse_and_diff_portfolio = MagicMock(return_value=MagicMock(is_empty=lambda: False))
        mock_io.apply_portfolio_diff = AsyncMock(side_effect=RuntimeError("duplicate key"))
        mock_io.UserDataValidationError = UserDataValidationError

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(PORTFOLIO_PATH, '{"holdings":[]}')

        assert exc.value.error_type == "constraint_error"


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
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_edit_string_not_found(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        mock_io.serialize_json = MagicMock(return_value='{"__version__":"v1"}')

        result = await backend.aedit_text(PORTFOLIO_PATH, "missing-string", "replacement")
        assert result["success"] is False
        assert "String not found" in result["error"]

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_edit_user_data_error_returns_failure_dict(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1", "holdings": []})
        content = '{"__version__":"v1","holdings":[]}'
        mock_io.serialize_json = MagicMock(return_value=content)
        mock_io.parse_and_diff_portfolio = MagicMock(
            side_effect=UserDataValidationError(
                error_type="schema_error", file="portfolio.json", field_path="root", hint="bad",
            )
        )
        mock_io.UserDataValidationError = UserDataValidationError

        result = await backend.aedit_text(PORTFOLIO_PATH, "v1", "v2")
        assert result["success"] is False
        assert "schema_error" in result["error"]


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
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_glob_outside_prefix_empty(self, backend):
        results = await backend.aglob_paths("*.json", "/home/workspace/other")
        assert results == []

    @pytest.mark.asyncio
    @patch("ptc_agent.agent.backends.user_data.io")
    async def test_grep_finds_pattern(self, mock_io, backend):
        mock_io.fetch_portfolio_for_user = AsyncMock(return_value=[])
        mock_io.fetch_watchlist_for_user = AsyncMock(return_value=([], {}))
        mock_io.fetch_preferences_for_user = AsyncMock(return_value=None)
        # serialize_* must include __version__ since _read_serialized extracts
        # it from the payload before serializing to JSON.
        mock_io.serialize_portfolio = MagicMock(return_value={"__version__": "v1"})
        mock_io.serialize_watchlist = MagicMock(return_value={"__version__": "v1"})
        mock_io.serialize_preferences = MagicMock(return_value={"__version__": "v1"})
        # Return JSON content with a pattern in the portfolio file only
        mock_io.serialize_json = MagicMock(side_effect=[
            '{"holdings":[{"symbol":"AAPL"}]}',
            '{"watchlists":[]}',
            '{"prefs":{}}',
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
        with patch("ptc_agent.agent.backends.user_data.io") as mock_io:
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
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_aread_range_works(self, backend):
        head = await backend.aread_range(README_PATH, offset=0, limit=5)
        assert head is not None
        assert head.startswith("# User Profile Data")
