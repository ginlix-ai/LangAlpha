"""The setup token's needs-reauth flag against real PostgreSQL: written only
on the token whose hash the refusal names, read back as the live flag, and
cleared by the next save.

Requires pgcrypto (pgp_sym_encrypt/decrypt and digest), which the migrations create.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_KEY = "test-byok-encryption-key-32char!"
_LATER = datetime.now(timezone.utc) + timedelta(days=300)


def _sha(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


@pytest.fixture(autouse=True)
def _encryption_key():
    with patch.dict(os.environ, {"BYOK_ENCRYPTION_KEY": _KEY}):
        yield


@pytest_asyncio.fixture
async def user(seed_user):
    from src.server.database.oauth_tokens import CLAUDE_CODE_PROVIDER, delete_oauth_tokens

    yield seed_user["user_id"]
    for provider in (CLAUDE_CODE_PROVIDER, "codex-oauth"):
        await delete_oauth_tokens(seed_user["user_id"], provider)


async def _save(user_id: str, provider: str, token: str) -> None:
    from src.server.database.oauth_tokens import upsert_oauth_tokens

    await upsert_oauth_tokens(
        user_id, provider, access_token=token, refresh_token="", account_id="", expires_at=_LATER,
    )


async def test_a_refusal_flags_only_the_save_it_read_and_a_new_save_clears_it(user):
    from src.server.database.oauth_tokens import CLAUDE_CODE_PROVIDER, get_oauth_tokens, mark_access_token_refused

    await _save(user, CLAUDE_CODE_PROVIDER, "sk-ant-oat01-new")
    saved_at = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["updated_at"]
    assert not await mark_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=_sha("sk-ant-oat01-old"), saved_at=saved_at,
        error_code="token_refused",
    ), "a refusal of the token this one replaced flags nothing"
    assert (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["needs_reauth_at"] is None

    await _save(user, CLAUDE_CODE_PROVIDER, "sk-ant-oat01-new")
    assert not await mark_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=_sha("sk-ant-oat01-new"), saved_at=saved_at,
        error_code="token_refused",
    ), "a refusal read before the same token was saved again flags nothing"
    saved_at = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["updated_at"]

    assert await mark_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=_sha("sk-ant-oat01-new"), saved_at=saved_at,
        error_code="token_refused",
    )
    row = await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER)
    assert row["needs_reauth_at"] is not None and row["last_error_code"] == "token_refused"

    await _save(user, CLAUDE_CODE_PROVIDER, "sk-ant-oat01-newer")
    row = await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER)
    assert row["needs_reauth_at"] is None and row["last_error_code"] is None


async def test_a_token_claude_serves_after_all_is_cleared_without_changing_its_save(user):
    from src.server.database.oauth_tokens import (
        CLAUDE_CODE_PROVIDER,
        clear_access_token_refused,
        get_oauth_tokens,
        mark_access_token_refused,
    )

    await _save(user, CLAUDE_CODE_PROVIDER, "sk-ant-oat01-tok")
    saved_at = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["updated_at"]
    digest = _sha("sk-ant-oat01-tok")
    assert not await clear_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=saved_at, flagged_at=saved_at,
    )
    await mark_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=saved_at, error_code="token_refused",
    )
    flagged_at = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["needs_reauth_at"]
    assert await clear_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=saved_at, flagged_at=flagged_at,
    )
    row = await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER)
    assert (row["needs_reauth_at"], row["last_error_code"], row["updated_at"]) == (None, None, saved_at)


async def test_a_clear_leaves_a_refusal_recorded_after_the_flag_it_read(user):
    from src.server.database.oauth_tokens import (
        CLAUDE_CODE_PROVIDER,
        clear_access_token_refused,
        get_oauth_tokens,
        mark_access_token_refused,
    )

    await _save(user, CLAUDE_CODE_PROVIDER, "sk-ant-oat01-tok")
    saved_at = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["updated_at"]
    digest = _sha("sk-ant-oat01-tok")
    refuse = dict(access_token_sha256=digest, saved_at=saved_at, error_code="token_refused")
    await mark_access_token_refused(user, CLAUDE_CODE_PROVIDER, **refuse)
    read = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["needs_reauth_at"]
    await mark_access_token_refused(user, CLAUDE_CODE_PROVIDER, **refuse)
    assert not await clear_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=saved_at, flagged_at=read,
    )
    newer = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["needs_reauth_at"]
    assert newer > read
    assert await clear_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=digest, saved_at=saved_at, flagged_at=newer,
    )


async def test_a_flagged_setup_token_never_stands_in_for_a_flagged_subscription(user):
    from src.server.database.oauth_tokens import (
        CLAUDE_CODE_PROVIDER,
        flagged_oauth_connection,
        get_oauth_tokens,
        mark_access_token_refused,
        mark_oauth_needs_reauth,
        upsert_oauth_tokens,
    )

    await upsert_oauth_tokens(
        user, "codex-oauth", access_token="at", refresh_token="rt", account_id="acct", expires_at=_LATER,
    )
    await mark_oauth_needs_reauth(user, "codex-oauth", refresh_token="rt", error_code="invalid_grant")
    await _save(user, CLAUDE_CODE_PROVIDER, "sk-ant-oat01-tok")
    saved_at = (await get_oauth_tokens(user, CLAUDE_CODE_PROVIDER))["updated_at"]
    await mark_access_token_refused(
        user, CLAUDE_CODE_PROVIDER, access_token_sha256=_sha("sk-ant-oat01-tok"), saved_at=saved_at,
        error_code="token_refused",
    )
    assert await flagged_oauth_connection(user) == {"provider": "codex-oauth", "last_error_code": "invalid_grant"}
