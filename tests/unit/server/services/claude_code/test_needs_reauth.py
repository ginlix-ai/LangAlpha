"""A saved token Claude refused stays refused past the hour its check is
kept for, and one past its expiry reads the same, until a new one is saved
or a check finds Claude serves it after all.

The Redis answer lapses after ``TOKEN_CHECK_TTL_S``; the row's flag, written
on the token's hash and the save it was read from, is what admission,
Settings and a relay read after it.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.claude_code import saved
from src.server.services.claude_code.machine import ComputerToken
from ptc_agent.agent.claude_code.launcher import Credential
from src.llms.claude_code import ClaudeTurnFailed
from src.server.services.claude_code.launchers import HostedCredential
from src.server.services.claude_code.saved import token_check_key, token_hash
from src.server.services.claude_code.hosted import cloud_placement
from src.server.services.llm.claude_code import (
    CLAUDE_CODE_OFF,
    _refusal,
    claude_code_refusal,
    require_computer_signed_in,
)
from tests.unit.server.services.claude_code.fakes import Cache, serving

SAVED = "src.server.services.claude_code.saved"
TOKEN = "sk-ant-oat01-saved"
CODE = "claude-sonnet-5-5-code"
NOW = datetime.now(timezone.utc)
SAVED_AT = NOW - timedelta(days=3)


def _row(*, flagged=False, expires_at=None):
    return {
        "access_token": TOKEN, "email": None, "expires_at": expires_at or NOW + timedelta(days=200),
        "updated_at": SAVED_AT, "needs_reauth_at": NOW if flagged else None,
        "last_error_code": "token_refused" if flagged else None,
    }


def _store(row):
    return patch(f"{SAVED}.stored_row", new_callable=AsyncMock, return_value=row)


def _relay_store(row):
    return patch("src.server.services.claude_code.machine.stored_row", new_callable=AsyncMock, return_value=row)


def _flag_writer(applied=True):
    return patch(f"{SAVED}.mark_access_token_refused", new_callable=AsyncMock, return_value=applied)


async def _admission_reason(row, cache):
    """What admission refuses a self-hosted turn on a cloud computer for, as it reads the saved token."""
    with serving(cache), _store(row), patch("src.server.services.claude_code.hosted.backend_available", lambda: True):
        where = await cloud_placement("user-1", None, token=True)
    return _refusal(CODE, where.refusal) if where.refusal else None


async def _refused_turn(cache, row=None):
    """A backend turn that read the saved token and then met Claude's refusal."""
    with (
        serving(cache),
        patch("src.server.services.claude_code.launchers.stored_row", AsyncMock(return_value=row or _row())),
        patch("src.server.services.claude_code.hosted.backend_available", lambda: True),
    ):
        token = HostedCredential("user-1", "saved_token", None)
        assert await token.read() == Credential(TOKEN)
        with pytest.raises(ClaudeTurnFailed) as failed:
            await token.refused()
    return failed.value


@pytest.mark.asyncio
async def test_a_turn_refused_on_the_saved_token_flags_the_save_it_read_and_the_flag_outlasts_the_hour():
    cache = Cache()
    with _flag_writer() as flag:
        failed = await _refused_turn(cache)
    assert claude_code_refusal(failed, CODE)["reason"] == "token_refused", "the turn says so, as admission will"
    flag.assert_awaited_once_with(
        "user-1", "claude-code", access_token_sha256=token_hash(TOKEN), saved_at=SAVED_AT, error_code="token_refused",
    )
    assert TOKEN not in repr(flag.await_args)
    assert cache.values == {token_check_key(TOKEN): False}
    hour_later = Cache()
    assert await _admission_reason(_row(flagged=True), hour_later) == {
        "message": CLAUDE_CODE_OFF["token_refused"], "type": "claude_code_off", "model": CODE, "reason": "token_refused",
    }
    assert CLAUDE_CODE_OFF["token_refused"] == (
        "Claude refused your setup token. It may have expired or been revoked. Add a new one in Settings under "
        "Model access to use this model."
    )


@pytest.mark.asyncio
async def test_a_refusal_that_raced_a_new_save_leaves_that_save_alone():
    cache = Cache()
    cache.values[token_check_key(TOKEN)] = True
    with _flag_writer(applied=False):
        await _refused_turn(cache)
    assert cache.values == {token_check_key(TOKEN): True}, "the save's own check stands"


@pytest.mark.asyncio
async def test_a_flagged_token_is_refused_even_where_its_kept_check_still_says_it_works():
    cache = Cache()
    cache.values[token_check_key(TOKEN)] = True
    assert (await _admission_reason(_row(flagged=True), cache))["reason"] == "token_refused"
    assert await _admission_reason(_row(), cache) is None


@pytest.mark.asyncio
async def test_a_token_past_its_expiry_is_refused_as_one_claude_refused_and_nothing_is_written():
    with _flag_writer() as flag:
        reason = await _admission_reason(_row(expires_at=NOW - timedelta(minutes=1)), Cache())
    assert reason["reason"] == "token_refused"
    flag.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_saved_token_is_still_signed_out():
    assert (await _admission_reason(None, Cache()))["reason"] == "signed_out"


@pytest.mark.asyncio
async def test_settings_reads_an_expired_token_as_refused_without_spending_a_check():
    check = AsyncMock(return_value=True)
    with serving(Cache()), patch("ptc_agent.agent.claude_code.runtime.token_works", check):
        assert await saved.token_state("user-1", _row(expires_at=NOW - timedelta(days=1))) == "rejected"
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_settings_checks_a_flagged_token_again_once_the_hour_lapsed():
    with serving(Cache()), patch(f"{SAVED}.in_background", AsyncMock(return_value=object())) as started:
        assert await saved.token_state("user-1", _row(flagged=True)) == "checking"
    started.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(("works", "flagged", "cleared"), [(False, False, 0), (True, False, 0), (True, True, 1)])
async def test_a_status_check_flags_a_refused_token_and_clears_the_flag_of_one_claude_serves(works, flagged, cleared):
    cache = Cache()
    with (
        serving(cache), _flag_writer() as flag, _store(_row()),
        patch(f"{SAVED}.clear_access_token_refused", new_callable=AsyncMock) as clear,
        patch("ptc_agent.agent.claude_code.runtime.token_works", AsyncMock(return_value=works)),
    ):
        await saved._check("user-1", _row(flagged=flagged), token_check_key(TOKEN), AsyncMock(return_value=True))
    assert flag.await_count == int(not works)
    if not works:
        assert flag.await_args.kwargs["saved_at"] == SAVED_AT
    assert clear.await_count == cleared
    if cleared:
        clear.assert_awaited_once_with(
            "user-1", "claude-code", access_token_sha256=token_hash(TOKEN), saved_at=SAVED_AT, flagged_at=NOW,
        )
    assert cache.values == {token_check_key(TOKEN): works}


@pytest.mark.asyncio
async def test_a_refusal_a_turn_met_while_a_status_check_ran_stands_over_its_answer():
    cache = Cache()

    async def served_then_refused(*_):
        await _refused_turn(cache)
        return True

    with (
        serving(cache), _flag_writer(), _store(_row(flagged=True)),
        patch(f"{SAVED}.clear_access_token_refused", new_callable=AsyncMock) as clear,
        patch("ptc_agent.agent.claude_code.runtime.token_works", served_then_refused),
    ):
        await saved._check("user-1", _row(), token_check_key(TOKEN), AsyncMock(return_value=True))
        assert await saved.token_state("user-1", _row(flagged=True)) == "rejected"
    clear.assert_not_awaited()
    assert cache.values == {token_check_key(TOKEN): False}


@pytest.mark.asyncio
async def test_a_check_of_a_flagged_token_leaves_a_refusal_a_turn_met_while_it_ran():
    cache = Cache()

    async def served_then_refused(*_):
        await _refused_turn(cache, _row(flagged=True))
        return True

    restamped = {**_row(flagged=True), "needs_reauth_at": NOW + timedelta(seconds=30)}
    with (
        serving(cache), _flag_writer(), _store(restamped),
        patch(f"{SAVED}.clear_access_token_refused", new_callable=AsyncMock, return_value=False) as clear,
        patch("ptc_agent.agent.claude_code.runtime.token_works", served_then_refused),
    ):
        await saved._check("user-1", _row(flagged=True), token_check_key(TOKEN), AsyncMock(return_value=True))
        assert await saved.token_state("user-1", restamped) == "rejected"
    assert clear.await_args.kwargs["flagged_at"] == NOW, "the clear names the flag the check read"
    assert cache.values == {token_check_key(TOKEN): False}


@pytest.mark.asyncio
async def test_without_redis_settings_reads_a_flagged_token_as_refused():
    with serving(Cache(redis=False)):
        assert await saved.token_state("user-1", _row(flagged=True)) == "rejected"
        assert await saved.token_state("user-1", _row()) == "unverified"


@pytest.mark.asyncio
async def test_a_flag_that_cannot_be_written_still_keeps_the_hour():
    cache = Cache()
    with patch(f"{SAVED}.mark_access_token_refused", AsyncMock(side_effect=RuntimeError("db down"))):
        await _refused_turn(cache)
    assert cache.values == {token_check_key(TOKEN): False}


@pytest.mark.asyncio
async def test_a_relayed_token_claude_refused_is_flagged_ends_the_turn_saying_so_and_is_not_relayed_again():
    cache = Cache()
    with (
        serving(cache), _relay_store(_row()), _flag_writer() as flag,
        patch("src.server.services.claude_code.hosted.backend_available", lambda: True),
    ):
        machine = ComputerToken("user-1", "c-1")
        assert await machine.read() == Credential(TOKEN)
        with pytest.raises(ClaudeTurnFailed) as failed:
            await machine.refused()
    assert claude_code_refusal(failed.value, CODE)["reason"] == "token_refused"
    flag.assert_awaited_once_with(
        "user-1", "claude-code", access_token_sha256=token_hash(TOKEN), saved_at=SAVED_AT, error_code="token_refused",
    )
    assert TOKEN not in repr(vars(machine))
    with (
        serving(Cache()), _relay_store(_row(flagged=True)),
        patch("src.server.services.claude_code.hosted.backend_available", lambda: True),
    ):
        assert await ComputerToken("user-1", "c-1").read() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fresh", [True, False])
async def test_a_computer_that_signs_in_with_the_relayed_token_is_refused_once_claude_refused_it(fresh):
    answer = SimpleNamespace(relays=True, fresh=fresh)
    with (
        serving(Cache()), _store(_row(flagged=True)),
        patch("src.server.services.llm.claude_code.read_answer", AsyncMock(return_value=answer)),
        patch("src.server.services.claude_code.hosted.backend_available", lambda: True),
        pytest.raises(HTTPException) as refused,
    ):
        await require_computer_signed_in(CODE, "user-1", "c-1")
    assert refused.value.detail["reason"] == "token_refused"
