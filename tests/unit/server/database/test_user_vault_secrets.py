"""Vault CRUD: one vault per user, shared by all of their workspaces."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

import src.server.database.user_vault_secrets as uvs


@pytest.fixture
def mock_cursor():
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.fetchone = AsyncMock(return_value=None)
    return cursor


@pytest.fixture
def vault_mock_db(mock_cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    @asynccontextmanager
    async def _transaction():
        yield

    conn.cursor = _cursor_cm
    conn.transaction = _transaction

    @asynccontextmanager
    async def _fake_connection(conn_in=None):
        yield conn_in if conn_in is not None else conn

    with (
        patch.object(uvs, "get_db_connection", new=_fake_connection),
        patch.object(uvs, "_get_encryption_key", return_value="test-key"),
        patch.object(uvs, "encryption_configured", return_value=True),
    ):
        yield mock_cursor


# ---------------------------------------------------------------------------
# reveal_user_secret
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reveal_user_secret_returns_the_value(vault_mock_db):
    vault_mock_db.fetchone.return_value = {"plaintext": "sk-test-value"}

    assert await uvs.reveal_user_secret("user-1", "API_KEY") == "sk-test-value"

    sql, params = vault_mock_db.execute.call_args.args
    assert "user_vault_secrets" in sql
    assert params == ("test-key", "user-1", "API_KEY")


@pytest.mark.asyncio
async def test_reveal_user_secret_missing_returns_none(vault_mock_db):
    vault_mock_db.fetchone.return_value = None
    assert await uvs.reveal_user_secret("user-1", "NOPE") is None


@pytest.mark.asyncio
async def test_reveal_user_secret_does_not_decrypt_the_whole_vault(
    vault_mock_db, monkeypatch
):
    """The single-row read is the point: the old path decrypted every secret."""
    whole_vault = AsyncMock()
    monkeypatch.setattr(uvs, "get_user_secrets_decrypted", whole_vault)
    vault_mock_db.fetchone.return_value = {"plaintext": "v"}

    await uvs.reveal_user_secret("user-1", "API_KEY")

    whole_vault.assert_not_awaited()
    # The list and bulk-decrypt reads are fetchall-shaped; a scoped reveal
    # uses none.
    vault_mock_db.fetchall.assert_not_awaited()
    sql = vault_mock_db.execute.call_args.args[0]
    assert "name = %s" in sql


# ---------------------------------------------------------------------------
# get_user_secrets_decrypted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_named_read_decrypts_only_those_rows(vault_mock_db):
    """The relay resolves one row's refs on every call, and each row decrypted
    is a full S2K derivation, so the narrowing has to reach the statement."""
    vault_mock_db.fetchall.return_value = [{"name": "DESK_KEY", "plaintext": "v"}]

    assert await uvs.get_user_secrets_decrypted("user-1", ["DESK_KEY"]) == {
        "DESK_KEY": "v"
    }

    sql, params = vault_mock_db.execute.call_args.args
    assert "name = ANY(%s)" in sql
    assert params == ("test-key", "user-1", ["DESK_KEY"])


@pytest.mark.asyncio
async def test_an_unnamed_read_still_takes_the_whole_vault(vault_mock_db):
    """The sandbox push and the redactor have no name list; the filter must
    stay opt-in."""
    vault_mock_db.fetchall.return_value = []

    await uvs.get_user_secrets_decrypted("user-1")

    sql, params = vault_mock_db.execute.call_args.args
    assert "name = ANY(%s)" not in sql
    assert params == ("test-key", "user-1")


@pytest.mark.asyncio
async def test_unconfigured_encryption_with_nothing_stored_means_no_secrets(vault_mock_db):
    """Nothing is decrypted, and nothing asks for the missing key."""
    vault_mock_db.fetchone.return_value = None
    with (
        patch.object(uvs, "encryption_configured", return_value=False),
        patch.object(uvs, "_get_encryption_key", side_effect=RuntimeError("no key")),
    ):
        assert await uvs.get_user_secrets_decrypted("user-1", ["API_KEY"]) == {}

    sql, params = vault_mock_db.execute.call_args.args
    assert "pgp_sym_decrypt" not in sql
    assert params == ("user-1", ["API_KEY"])


@pytest.mark.asyncio
async def test_unconfigured_encryption_with_secrets_stored_fails_closed(vault_mock_db):
    """Stored secrets read as absent would reach file output unredacted."""
    vault_mock_db.fetchone.return_value = {"?column?": 1}
    with (
        patch.object(uvs, "encryption_configured", return_value=False),
        patch.object(uvs, "_get_encryption_key", side_effect=RuntimeError("no key")),
        pytest.raises(RuntimeError, match="no key"),
    ):
        await uvs.get_user_secrets_decrypted("user-1")


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_refuses_past_the_cap(vault_mock_db):
    vault_mock_db.fetchone.return_value = {"cnt": uvs.MAX_SECRETS_PER_USER}

    with pytest.raises(ValueError, match="Maximum"):
        await uvs.create_user_secret("user-1", "ONE_MORE", "v")

    assert not any(
        "INSERT" in call.args[0] for call in vault_mock_db.execute.await_args_list
    )


@pytest.mark.asyncio
async def test_delete_user_secret_reports_missing_rows(vault_mock_db):
    vault_mock_db.rowcount = 0
    assert await uvs.delete_user_secret("user-1", "GONE") is False

    vault_mock_db.rowcount = 1
    assert await uvs.delete_user_secret("user-1", "THERE") is True
