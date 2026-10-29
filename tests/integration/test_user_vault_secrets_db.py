"""The vault fingerprint against real PostgreSQL: what moves it and what does not."""

from __future__ import annotations

import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
def _encryption_key(monkeypatch):
    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "test-vault-fingerprint-key")


async def test_fingerprint_follows_the_stored_values(patched_get_db_connection):
    from src.server.database import user_vault_secrets as uvs

    user = f"test-vault-fp-{uuid.uuid4()}"
    fingerprint = uvs.get_user_vault_fingerprint
    assert await uvs.get_user_vault_snapshot(user) == ({}, "")

    await uvs.create_user_secret(user, "API_KEY", "v1")
    created = await fingerprint(user)
    await uvs.update_user_secret(user, "API_KEY", description="notes only")
    assert await fingerprint(user) == created
    await uvs.update_user_secret(user, "API_KEY", value="v2")
    rotated = await fingerprint(user)
    await uvs.create_user_secret(user, "OTHER", "x")
    added = await fingerprint(user)
    assert len({"", created, rotated, added}) == 4

    # The snapshot's fingerprint is the standalone read's, over the same rows.
    await uvs.delete_user_secret(user, "OTHER")
    assert await uvs.get_user_vault_snapshot(user) == ({"API_KEY": "v2"}, rotated)
    await uvs.delete_user_secret(user, "API_KEY")
    assert await uvs.get_user_vault_snapshot(user) == ({}, "")
