"""User-vault router: the single-secret reveal.

Convergence after a mutation (the vault-ref scan, the version bump and the
snapshot purge) lives in ``services/vault_invalidation`` and is pinned there —
both tiers share it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

import src.server.app.user_vault as user_vault


@pytest.mark.asyncio
async def test_reveal_endpoint_reads_one_secret(monkeypatch):
    reveal = AsyncMock(return_value="sk-test-value")
    monkeypatch.setattr(user_vault, "reveal_user_secret", reveal)

    assert await user_vault.reveal_secret("API_KEY", "user-1") == {
        "value": "sk-test-value"
    }
    reveal.assert_awaited_once_with("user-1", "API_KEY")


@pytest.mark.asyncio
async def test_reveal_endpoint_404s_on_missing(monkeypatch):
    monkeypatch.setattr(
        user_vault, "reveal_user_secret", AsyncMock(return_value=None)
    )

    with pytest.raises(HTTPException) as exc:
        await user_vault.reveal_secret("NOPE", "user-1")
    assert exc.value.status_code == 404


def test_router_no_longer_imports_the_whole_vault_decrypt():
    """The reveal path must not be able to fall back to a full-tier decrypt."""
    assert not hasattr(user_vault, "get_user_secrets_decrypted")
