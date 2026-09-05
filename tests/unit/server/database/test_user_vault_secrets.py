"""User-tier vault CRUD and the tier descriptor that parameterizes its SQL.

Both tiers share one set of statements, so the table/column names ARE f-string
interpolated. `_VaultTier`'s allowlist is the compensating control for that,
and these tests pin it alongside the user tier's own behavior.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

import src.server.database.user_vault_secrets as uvs
from src.server.database.vault_secrets import _VaultTier


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

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_connection(conn_in=None):
        yield conn_in if conn_in is not None else conn

    # The user tier's SQL runs inside vault_secrets — that is where the pool
    # handle lives after the tier collapse.
    with patch(
        "src.server.database.vault_secrets.get_db_connection",
        new=_fake_connection,
    ):
        with patch(
            "src.server.database.vault_secrets._get_encryption_key",
            return_value="test-key",
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
    # _decrypted/_list are the fetchall-shaped reads; a scoped reveal uses none.
    vault_mock_db.fetchall.assert_not_awaited()
    sql = vault_mock_db.execute.call_args.args[0]
    assert "name = %s" in sql


# ---------------------------------------------------------------------------
# Tier delegation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_user_tier_queries_the_user_table(vault_mock_db):
    vault_mock_db.fetchall.return_value = [{"name": "A"}, {"name": "B"}]

    assert await uvs.get_user_secret_names("user-7") == {"A", "B"}

    sql, params = vault_mock_db.execute.call_args.args
    assert "user_vault_secrets" in sql
    assert "workspace" not in sql
    assert params == ("user-7",)


@pytest.mark.asyncio
async def test_delete_user_secret_reports_missing_rows(vault_mock_db):
    vault_mock_db.rowcount = 0
    assert await uvs.delete_user_secret("user-1", "GONE") is False

    vault_mock_db.rowcount = 1
    assert await uvs.delete_user_secret("user-1", "THERE") is True


# ---------------------------------------------------------------------------
# _VaultTier allowlist
# ---------------------------------------------------------------------------


def _tier(**overrides):
    kwargs = {
        "table": "user_vault_secrets",
        "owner_col": "user_id",
        "id_col": "user_vault_secret_id",
        "max_secrets": 5,
        "label": "user",
        "log_prefix": "[test]",
    }
    kwargs.update(overrides)
    return _VaultTier(**kwargs)


def test_tier_rejects_table_outside_allowlist():
    with pytest.raises(ValueError, match="Unknown vault table"):
        _tier(table="users")


def test_tier_rejects_injected_table_name():
    with pytest.raises(ValueError, match="Unknown vault table"):
        _tier(table="user_vault_secrets; DROP TABLE users --")


def test_tier_rejects_column_outside_allowlist():
    with pytest.raises(ValueError, match="Unknown vault column"):
        _tier(owner_col="user_id = '' OR 1=1 --")


def test_tier_rejects_injected_id_column():
    with pytest.raises(ValueError, match="Unknown vault column"):
        _tier(id_col="value")


def test_shipped_tiers_are_valid():
    from src.server.database.vault_secrets import WORKSPACE_TIER

    assert uvs.USER_TIER.table == "user_vault_secrets"
    assert uvs.USER_TIER.max_secrets == uvs.MAX_SECRETS_PER_USER
    assert WORKSPACE_TIER.table != uvs.USER_TIER.table


# ---------------------------------------------------------------------------
# get_effective_secrets — the merge rule both the sandbox push and the
# redactor read, so a fork here would leave an inherited credential unredacted.
# ---------------------------------------------------------------------------


@pytest.fixture
def merge_probes(monkeypatch):
    import src.server.database.vault_secrets as vs

    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "unit-test-key")
    ws = AsyncMock(return_value={})
    monkeypatch.setattr(vs, "_decrypted", ws)
    user = AsyncMock(return_value={})
    monkeypatch.setattr(uvs, "get_user_secrets_decrypted", user)
    return ws, user


@pytest.mark.asyncio
async def test_unconfigured_encryption_means_no_secrets(merge_probes, monkeypatch):
    """Key-less deployments can't have written a secret, so {} is the true
    answer — and the DB is never touched."""
    from src.server.database.vault_secrets import get_effective_secrets

    monkeypatch.delenv("BYOK_ENCRYPTION_KEY")
    ws, user = merge_probes
    ws.return_value = {"API_KEY": "ws-value"}

    assert await get_effective_secrets("ws-1", "user-1") == {}
    ws.assert_not_awaited()
    user.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_secret_shadows_the_user_one(merge_probes):
    from src.server.database.vault_secrets import get_effective_secrets

    ws, user = merge_probes
    ws.return_value = {"API_KEY": "ws-value"}
    user.return_value = {"API_KEY": "user-value", "OTHER": "u"}

    assert await get_effective_secrets("ws-1", "user-1") == {
        "API_KEY": "ws-value",
        "OTHER": "u",
    }


@pytest.mark.asyncio
async def test_owner_is_read_from_the_workspace_when_omitted(
    merge_probes, monkeypatch
):
    """The redactor calls with only a workspace id."""
    import src.server.database.workspace as ws_db
    from src.server.database.vault_secrets import get_effective_secrets

    _, user = merge_probes
    user.return_value = {"OTHER": "u"}
    monkeypatch.setattr(
        ws_db, "get_workspace", AsyncMock(return_value={"user_id": "user-9"})
    )

    assert await get_effective_secrets("ws-1") == {"OTHER": "u"}
    user.assert_awaited_once_with("user-9")


@pytest.mark.asyncio
async def test_ownerless_workspace_falls_back_to_workspace_only(
    merge_probes, monkeypatch
):
    import src.server.database.workspace as ws_db
    from src.server.database.vault_secrets import get_effective_secrets

    ws, user = merge_probes
    ws.return_value = {"API_KEY": "ws-value"}
    monkeypatch.setattr(ws_db, "get_workspace", AsyncMock(return_value=None))

    assert await get_effective_secrets("ws-1") == {"API_KEY": "ws-value"}
    user.assert_not_awaited()
