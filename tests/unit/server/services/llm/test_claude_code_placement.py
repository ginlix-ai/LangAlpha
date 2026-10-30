"""Where a Claude Code turn may run: on the user's own computer on any server,
on a hosted sandbox only where the backend runs the CLI, and refused at
admission only on a computer's kept answer, never by dialing it."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.claude_code import checks
from src.server.services.claude_code.machine import ComputerToken, MachineAnswer, answer_key
from src.server.services.claude_code.saved import token_check_key
from src.server.services.llm.claude_code import CLAUDE_CODE_OFF, require_runnable
from tests.unit.server.services.claude_code.fakes import Cache, serving

SERVICE = "src.server.services.llm.claude_code"
MACHINES = "src.server.services.claude_code.machine"
SAVED = "src.server.services.claude_code.saved"
CODE = "claude-sonnet-5-5-code"
OWN = {"computer_id": "c-1", "kind": "ssh"}
HOSTED = {"computer_id": "c-2", "kind": "daytona"}


def _Cache(values=None):
    cache = Cache()
    cache.values.update(values or {})
    return cache


def _answer(file, *, age=0.0, **meta):
    return {"file": file, "email": None, "expires_at": None, "checked_at": time.time() - age, **meta}


async def _admit(computer, *, backend=False, cache=None, saved=None):
    config = SimpleNamespace(feature_enabled=lambda _: True)
    dial = AsyncMock(side_effect=AssertionError("admission dialed the machine"))
    with (
        serving(cache or _Cache()),
        patch(f"{SERVICE}.rows_available", return_value=True),
        patch(f"{SERVICE}.backend_available", return_value=backend),
        patch("src.server.database.computer.get_computer_for_workspace", new_callable=AsyncMock, return_value=computer),
        patch(f"{SAVED}.stored_token", new_callable=AsyncMock, return_value=saved),
        patch(f"{MACHINES}.machine_of", new=dial),
    ):
        await require_runnable(config, CODE, "analyst", user_id="user-1", workspace_id="ws-1")


def _reason(refused):
    return refused.value.detail["reason"]


@pytest.mark.asyncio
async def test_a_hosted_sandbox_needs_the_backend_and_is_refused_where_it_does_not_run():
    with pytest.raises(HTTPException) as refused:
        await _admit(HOSTED, backend=False, saved="sk-ant-oat01-saved")
    assert _reason(refused) == "needs_own_computer"
    assert refused.value.detail["message"] == CLAUDE_CODE_OFF["needs_own_computer"]
    await _admit(HOSTED, backend=True, saved="sk-ant-oat01-saved")


@pytest.mark.asyncio
async def test_an_own_computer_runs_on_a_hosted_server_with_nothing_known_of_it():
    await _admit(OWN, backend=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("file", ["refused", "none", "invalid"])
async def test_an_own_computer_known_to_have_no_usable_token_is_refused(file):
    with pytest.raises(HTTPException) as refused:
        await _admit(OWN, cache=_Cache({answer_key("c-1"): _answer(file)}))
    assert _reason(refused) == "signed_out"


@pytest.mark.asyncio
@pytest.mark.parametrize("file", ["works", "unknown"])
async def test_an_own_computer_whose_token_works_or_could_not_be_checked_runs(file):
    await _admit(OWN, cache=_Cache({answer_key("c-1"): _answer(file)}))


@pytest.mark.asyncio
async def test_an_answer_gone_stale_is_not_held_against_a_turn():
    await _admit(OWN, cache=_Cache({answer_key("c-1"): _answer("refused", age=checks.TOKEN_CHECK_TTL_S + 1)}))
    await _admit(OWN, cache=_Cache({answer_key("c-1"): _answer("none", age=checks.SHORT_TTL_S + 1)}))


@pytest.mark.asyncio
async def test_a_machine_without_a_token_runs_on_the_saved_one_only_where_it_is_relayed():
    kept = {answer_key("c-1"): _answer("none")}
    await _admit(OWN, backend=True, cache=_Cache(kept), saved="sk-ant-oat01-saved")
    with pytest.raises(HTTPException):
        await _admit(OWN, backend=False, cache=_Cache(kept), saved="sk-ant-oat01-saved")
    refused_saved = {**kept, token_check_key("sk-ant-oat01-saved"): "refused"}
    with pytest.raises(HTTPException):
        await _admit(OWN, backend=True, cache=_Cache(refused_saved), saved="sk-ant-oat01-saved")
    with pytest.raises(HTTPException):
        await _admit(OWN, backend=True, cache=_Cache({answer_key("c-1"): _answer("refused")}), saved="sk-ant-oat01-saved")


@pytest.mark.asyncio
async def test_resolve_names_the_turns_workspace_to_the_placement_check():
    from src.server.services.llm.config import resolve_llm_config
    from tests.unit.server.services.llm.test_claude_code_routing import _base

    seen = AsyncMock(side_effect=HTTPException(400, "stop"))
    with (
        patch("src.server.services.llm.config.require_runnable", new=seen),
        patch("src.server.services.llm.user_models.get_model_preference", new_callable=AsyncMock, return_value={"preferred_model": CODE}),
        patch("src.server.services.llm.config.load_user_context", new_callable=AsyncMock),
        patch("src.server.services.llm.config.effective_flags_for_user", new_callable=AsyncMock, return_value={}),
        pytest.raises(HTTPException),
    ):
        await resolve_llm_config(_base(), "user-1", None, False, workspace_id="ws-1")
    assert seen.await_args.kwargs == {"user_id": "user-1", "workspace_id": "ws-1"}


# -- What a turn on an own computer records --


@pytest.mark.asyncio
async def test_a_turn_refused_on_the_machines_token_records_it_for_that_computer():
    cache = _Cache({answer_key("c-1"): _answer("works", email="user@example.com")})
    with serving(cache):
        await ComputerToken("user-1", "c-1").refused("file")
    kept = MachineAnswer(**cache.values[answer_key("c-1")])
    assert (kept.file, kept.email, kept.fresh) == ("refused", "user@example.com", True)


@pytest.mark.asyncio
async def test_a_relayed_token_is_handed_over_once_asked_and_its_refusal_marks_it_by_hash_only():
    cache = _Cache()
    with serving(cache), patch(f"{SAVED}.stored_token", new_callable=AsyncMock, return_value="sk-ant-oat01-saved"):
        machine = ComputerToken("user-1", "c-1")
        assert await machine.relay() == "sk-ant-oat01-saved"
        await machine.refused("relay")
        assert cache.values == {token_check_key("sk-ant-oat01-saved"): False}
        assert "sk-ant-oat01-saved" not in repr(vars(machine))
        assert await ComputerToken("user-1", "c-1").relay() is None, "a token Claude refused is not relayed again"
