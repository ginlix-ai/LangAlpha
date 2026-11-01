"""Where a Claude Code turn may run: on the user's own computer on any server,
on a hosted sandbox only where the backend runs the CLI, and refused at
admission only on a computer's kept answer, never by dialing it."""

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.core import claude_code_managed
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from src.server.services.claude_code import checks
from src.server.services.claude_code.machine import ComputerToken, MachineAnswer, answer_key
from src.server.services.claude_code.saved import token_check_key
from src.llms.claude_code import ClaudeTurnFailed
from src.server.services.llm.claude_code import CLAUDE_CODE_OFF, claude_code_refusal, require_runnable
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
@pytest.mark.parametrize("present", [True, False])
async def test_an_own_computer_with_any_organizations_policy_installed_is_refused_whatever_its_token(present):
    """Refused before anything is installed or launched, while the answer is
    fresh: once it is not, the turn runs and the launcher finds the policy
    again."""
    kept = {answer_key("c-1"): _answer("policy", present=present)}
    with pytest.raises(HTTPException) as refused:
        await _admit(OWN, backend=True, cache=_Cache(kept), saved="sk-ant-oat01-saved")
    assert _reason(refused) == "policy_blocked"
    assert refused.value.detail["message"] == CLAUDE_CODE_OFF["policy_blocked"] == (
        "Your organization's Claude Code policy is installed on this computer, so LangAlpha can't run Claude Code here."
    )
    stale = {answer_key("c-1"): _answer("policy", present=present, age=checks.SHORT_TTL_S + 1)}
    await _admit(OWN, backend=True, cache=_Cache(stale), saved="sk-ant-oat01-saved")


@pytest.mark.asyncio
async def test_an_answer_kept_with_a_field_no_longer_known_still_reads():
    kept = {answer_key("c-1"): _answer("refused", managed=["settings"])}
    with pytest.raises(HTTPException) as refused:
        await _admit(OWN, cache=_Cache(kept))
    assert _reason(refused) == "signed_out"


@pytest.mark.asyncio
async def test_a_backend_turn_is_refused_where_an_organizations_policy_is_installed_on_the_server(tmp_path, monkeypatch):
    managed = tmp_path / "claude-code"
    (managed / "managed-settings.d").mkdir(parents=True)
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_LINUX", str(managed))
    monkeypatch.setattr(claude_code_managed, "CLAUDE_CODE_MANAGED_DIR_MAC", str(managed))
    await _admit(HOSTED, backend=True, saved="sk-ant-oat01-saved")
    (managed / "managed-settings.d" / "10-org.json").write_text("{}")
    with pytest.raises(HTTPException) as refused:
        await _admit(HOSTED, backend=True, saved="sk-ant-oat01-saved")
    assert _reason(refused) == "policy_blocked"


@pytest.mark.parametrize(("kind", "reason"), [("auth_failed", "signed_out"), ("policy_blocked", "policy_blocked")])
def test_a_turn_failing_for_what_admission_refuses_for_ends_with_admissions_refusal(kind, reason):
    failed = RuntimeError("the turn failed")
    failed.__cause__ = ClaudeTurnFailed(kind, "the CLI said so")
    assert claude_code_refusal(failed, CODE) == {
        "message": CLAUDE_CODE_OFF[reason], "type": "claude_code_off", "model": CODE, "reason": reason,
    }


@pytest.mark.parametrize("kind", ["plan_exhausted", "startup_failed", "api_error"])
def test_any_other_turn_failure_is_left_to_the_stream_errors(kind):
    assert claude_code_refusal(ClaudeTurnFailed(kind, "the CLI said so"), CODE) is None


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
@pytest.mark.parametrize("kept", [None, "works"])
async def test_a_turn_that_had_no_token_to_sign_in_with_refuses_the_next_but_one_relayed(kept):
    """No status check yet, or the token taken off the machine by hand."""
    cache = _Cache({answer_key("c-1"): _answer(kept, age=1)} if kept else {})
    with serving(cache):
        await ComputerToken("user-1", "c-1").announced("none")
    with pytest.raises(HTTPException) as refused:
        await _admit(OWN, cache=cache)
    assert _reason(refused) == "signed_out"
    with serving(cache):
        await ComputerToken("user-1", "c-1").announced("relay")
    await _admit(OWN, backend=True, cache=cache, saved="sk-ant-oat01-saved")


@pytest.mark.asyncio
async def test_a_machine_without_perl_is_refused_as_needing_perl_even_where_the_saved_token_is_relayed():
    with pytest.raises(HTTPException) as refused:
        await _admit(OWN, backend=True, cache=_Cache({answer_key("c-1"): _answer("no_perl")}), saved="sk-ant-oat01-saved")
    assert _reason(refused) == "needs_perl"
    assert refused.value.detail["message"] == CLAUDE_CODE_OFF["needs_perl"]


@pytest.mark.asyncio
async def test_a_machine_on_musl_is_refused_as_unsupported_even_where_the_saved_token_is_relayed():
    with pytest.raises(HTTPException) as refused:
        await _admit(OWN, backend=True, cache=_Cache({answer_key("c-1"): _answer("unsupported")}), saved="sk-ant-oat01-saved")
    assert _reason(refused) == "computer_unsupported"
    assert refused.value.detail["message"] == MUSL_UNSUPPORTED


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
