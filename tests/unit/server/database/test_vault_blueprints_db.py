"""Tests for src/server/database/vault_secrets.get_workspace_secret_names.

The helper powers the blueprints endpoint's "already set" subtraction without
paying the pgcrypto decryption cost that get_workspace_secrets incurs.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.vault_secrets import get_workspace_secret_names


@pytest.fixture
def mock_cursor():
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    return cursor


@pytest.fixture
def vault_mock_db(mock_cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_connection():
        yield conn

    with patch(
        "src.server.database.vault_secrets.get_db_connection",
        new=_fake_connection,
    ):
        yield mock_cursor


@pytest.mark.asyncio
async def test_empty_vault_returns_empty_set(vault_mock_db):
    vault_mock_db.fetchall.return_value = []
    names = await get_workspace_secret_names("ws-1")
    assert names == set()


@pytest.mark.asyncio
async def test_populated_vault_returns_full_name_set(vault_mock_db):
    vault_mock_db.fetchall.return_value = [
        {"name": "X_BEARER_TOKEN"},
        {"name": "FMP_API_KEY"},
        {"name": "POLYGON_KEY"},
    ]
    names = await get_workspace_secret_names("ws-1")
    assert names == {"X_BEARER_TOKEN", "FMP_API_KEY", "POLYGON_KEY"}


@pytest.mark.asyncio
async def test_query_does_not_select_value_column(vault_mock_db):
    """Guard rail: ensure we're not accidentally decrypting."""
    await get_workspace_secret_names("ws-1")
    executed_sql = vault_mock_db.execute.call_args.args[0]
    assert "name" in executed_sql.lower()
    assert "value" not in executed_sql.lower()
    assert "pgp_sym_decrypt" not in executed_sql.lower()


@pytest.mark.asyncio
async def test_query_is_parametrized(vault_mock_db):
    """Guard rail: ensure workspace_id is passed as a bound parameter, not
    interpolated. Catches accidental f-string regressions that would reintroduce
    SQL-injection risk."""
    await get_workspace_secret_names("ws-42")
    args = vault_mock_db.execute.call_args.args
    assert args[1] == ("ws-42",)
    assert "%s" in args[0]
