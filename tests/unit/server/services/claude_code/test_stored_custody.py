"""Storing a user's setup token for Claude Code on cloud computers: the
choice behind its operator's switch and the consent text, turns on the
stored token, leaving the choice (the token goes with it, and the turns that
may hold it stop), the operator's block, and the turns nobody starts, which
never run on it.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

import pytest
from fastapi import HTTPException

from ptc_agent.agent.claude_code.launcher import Credential
from src.server.database.claude_code_hosted import STORED_TURN, CLOUD_TURN
from src.server.services.claude_code import hosted as hosted_module
from src.server.services.claude_code.flow import backend_on
from src.server.services.claude_code.hosted import DISCLOSURE, Hosted, drop_stored, opted_out
from src.server.services.claude_code.launchers import HostedCredential
from src.server.services.claude_code.machine import ComputerToken
from src.server.services.claude_code.saved import SavedHome, token_check_key
from src.llms.claude_code import ClaudeTurnFailed
from src.server.services.llm.claude_code import (
    CLAUDE_CODE_OFF,
    claude_code_refusal,
    require_attended,
    require_runnable,
    run_stamp,
    unattended,
)
from src.server.services.oauth_signin import OAuthRefusal
from tests.unit.server.services.claude_code.fakes import Cache, serving
from tests.unit.server.services.claude_code.test_hosted import (
    CLOUD,
    CODE,
    HOSTED,
    LAUNCHERS,
    OWN,
    SERVICE,
    _admit,
    platform,
)
from tests.unit.server.services.claude_code.test_hosted import client as _client

client = _client

TOKEN = "sk-ant-oat01-" + "t" * 40
ROWS_ON = "src.server.services.claude_code.flow.rows_on"
#: The token store's verdict as a cloud computer's placement reads it.
SAVED_REFUSAL = "src.server.services.claude_code.saved.saved_refusal"


# -- The choice --


@pytest.mark.asyncio
async def test_with_only_its_switch_on_storing_is_the_one_choice_offered(client):
    with platform(switch=False, stored=True, key=True), patch(ROWS_ON, AsyncMock(return_value=True)):
        got = await client.get("/api/v1/claude-code/custody")
        api_key = await client.put("/api/v1/claude-code/custody", json={"mode": "api_key"})
    assert got.json() == {
        "mode": "local", "api_key_saved": True, "offered": ["stored"], "blocked": False, "runs_on": None,
        "refusal": "hosted_off", "disclosure": DISCLOSURE,
    }
    assert api_key.status_code == 409


@pytest.mark.asyncio
async def test_storing_the_token_needs_the_current_consent_text_and_records_it(client):
    saved = AsyncMock()
    with (
        platform(stored=True), patch(ROWS_ON, AsyncMock(return_value=True)), patch(f"{HOSTED}.write_custody", saved),
    ):
        unread = await client.put("/api/v1/claude-code/custody", json={"mode": "stored"})
        older = await client.put("/api/v1/claude-code/custody", json={"mode": "stored", "disclosure": "2026-01-v0"})
        accepted = await client.put("/api/v1/claude-code/custody", json={"mode": "stored", "disclosure": DISCLOSURE})
    assert (unread.status_code, older.status_code, accepted.status_code) == (400, 400, 200)
    saved.assert_awaited_once_with("test-user-123", "stored", disclosure=DISCLOSURE)


@pytest.mark.asyncio
async def test_a_blocked_user_is_not_offered_storing_and_cannot_choose_it(client):
    with platform(stored=True, blocked=True), patch(ROWS_ON, AsyncMock(return_value=True)):
        got = await client.get("/api/v1/claude-code/custody")
        stored = await client.put("/api/v1/claude-code/custody", json={"mode": "stored", "disclosure": DISCLOSURE})
    assert got.json() == {
        "mode": "local", "api_key_saved": True, "offered": ["api_key"], "blocked": True, "runs_on": None,
        "refusal": "hosted_off", "disclosure": DISCLOSURE,
    }
    assert stored.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ({"stored": True, "mode": "stored"}, Hosted("stored_token")),
        ({"stored": False, "mode": "stored"}, Hosted(refusal="hosted_paused")),
        ({"switch": False, "stored": True, "mode": "api_key"}, Hosted(refusal="hosted_paused")),
        ({"switch": False, "stored": True, "mode": "local", "blocked": True}, Hosted(refusal="stored_blocked")),
        ({"stored": True, "mode": "local", "blocked": True}, Hosted(refusal="hosted_off")),
        ({"stored": True, "mode": "stored", "blocked": True}, Hosted(refusal="stored_blocked")),
        ({"stored": True, "mode": "local"}, Hosted(refusal="hosted_off")),
    ],
)
async def test_where_a_turn_on_a_cloud_computer_runs_by_the_choice_and_its_switch(given, expected):
    with platform(**given):
        assert await hosted_module.hosted("user-1") == expected


# -- Turns on the stored token --


@pytest.mark.asyncio
async def test_a_turn_on_the_stored_token_runs_while_it_is_usable_up_to_the_users_cap():
    with platform(stored=True, mode="stored"), patch(SAVED_REFUSAL, AsyncMock(return_value=None)):
        assert await _admit() is None
    with platform(stored=True, mode="stored"), patch(SAVED_REFUSAL, AsyncMock(return_value="token_refused")):
        assert (await _admit())["reason"] == "token_refused"
    with platform(stored=True, mode="stored", live=3), patch(SAVED_REFUSAL, AsyncMock(return_value=None)):
        assert (await _admit())["reason"] == "too_many_turns"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["hosted_paused", "stored_blocked"])
async def test_a_paused_or_blocked_choice_refuses_the_turn_in_its_own_words(reason):
    given = {"stored": False, "mode": "stored"} if reason == "hosted_paused" else {"switch": False, "stored": True, "blocked": True}
    with platform(**given):
        assert await _admit() == {
            "message": CLAUDE_CODE_OFF[reason], "type": "claude_code_off", "model": CODE, "reason": reason,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "reason"),
    [({"stored": False, "mode": "stored"}, "hosted_paused"), ({"switch": False, "stored": True, "blocked": True}, "stored_blocked")],
)
async def test_a_run_paused_or_blocked_since_admission_never_starts_and_says_why(given, reason):
    with platform(**given), pytest.raises(ClaudeTurnFailed) as refused:
        await HostedCredential("user-1", "stored_token", "c-2").read()
    assert claude_code_refusal(refused.value, CODE)["reason"] == reason


@pytest.mark.asyncio
async def test_the_run_reads_the_stored_token_as_it_starts_for_the_token_descriptor():
    row = {"access_token": TOKEN, "updated_at": "saved-at"}
    with platform(stored=True, mode="stored"), patch(f"{LAUNCHERS}.stored_row", AsyncMock(return_value=row)):
        assert await HostedCredential("user-1", "stored_token", "c-2").read() == Credential(TOKEN)


@pytest.mark.asyncio
async def test_a_run_admitted_on_one_credential_never_starts_on_another_its_row_was_not_stamped_for():
    with platform(stored=True, mode="stored"), pytest.raises(ClaudeTurnFailed) as refused:
        await HostedCredential("user-1", "api_key", "c-2").read()
    assert claude_code_refusal(refused.value, CODE) == {
        "message": "How Claude Code runs on cloud computers changed as this turn started. Send your message again.",
        "type": "claude_code_off", "model": CODE, "reason": "choice_changed",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("given", "computer", "answer", "stamp"),
    [
        ({"stored": True, "mode": "stored"}, CLOUD, None, {**CLOUD_TURN, **STORED_TURN}),
        ({"stored": True, "mode": "api_key"}, CLOUD, None, CLOUD_TURN),
        ({"stored": True, "mode": "stored"}, OWN, None, STORED_TURN),
        ({"stored": True, "mode": "stored"}, OWN, SimpleNamespace(relays=True, fresh=False), STORED_TURN),
        ({"stored": True, "mode": "stored"}, OWN, SimpleNamespace(relays=False, fresh=False), {}),
        ({"stored": True, "mode": "local"}, OWN, None, {}),
        ({"stored": False}, OWN, None, {}),
    ],
)
async def test_a_run_that_may_hold_the_stored_token_is_marked_so_it_can_be_stopped(given, computer, answer, stamp):
    with (
        platform(computer=computer, **given), patch(f"{SERVICE}.read_answer", AsyncMock(return_value=answer)),
        patch(f"{SERVICE}.saved_refusal", AsyncMock(return_value=None)), patch(SAVED_REFUSAL, AsyncMock(return_value=None)),
    ):
        assert await _stamp() == stamp


async def _stamp() -> dict:
    """The stamp a turn's row gets from what admission let it run on."""
    admitted = await require_runnable(
        SimpleNamespace(feature_enabled=lambda _: True), CODE, "analyst", user_id="user-1", workspace_id="ws-1",
    )
    return run_stamp(SimpleNamespace(claude_code_hosted=admitted.hosted, claude_code_saved=admitted.saved))


# -- Turns nobody starts --


def _request(**fields) -> SimpleNamespace:
    return SimpleNamespace(**{"query_type": None, "wakeup": None, "market_target": None, "origin": None, **fields})


@pytest.mark.parametrize(
    ("request_fields", "nobody"),
    [
        ({}, False),
        ({"query_type": "initial"}, False),
        ({"query_type": "system"}, False),
        ({"wakeup": {"v": 1}}, True),
        ({"market_target": {"key": "k"}}, True),
        ({"origin": SimpleNamespace(type="automation")}, True),
        ({"origin": SimpleNamespace(type="agent", agent="chief_of_staff")}, True),
        ({"origin": SimpleNamespace(type="agent", agent="cli")}, True),
        ({"origin": SimpleNamespace(type="system")}, False),
    ],
)
def test_a_turn_nobody_started_never_runs_on_the_stored_token(request_fields, nobody):
    request = _request(**request_fields)
    assert unattended(request) is nobody
    def config(hosted, saved):
        return SimpleNamespace(claude_code_hosted=hosted, claude_code_saved=saved, llm=SimpleNamespace(name=CODE))

    for stored in (config("stored_token", "stored_token"), config(None, "stored_token")):
        if nobody:
            with pytest.raises(HTTPException) as refused:
                require_attended(stored, request)
            assert refused.value.detail["reason"] == "stored_unattended"
        else:
            require_attended(stored, request)
    for hosted, saved in (("api_key", None), (None, None), ("saved_token", "saved_token"), (None, "saved_token")):
        require_attended(config(hosted, saved), request)


@pytest.mark.asyncio
async def test_an_own_computer_relayed_the_stored_token_runs_no_turn_nobody_started():
    with (
        platform(computer=OWN, stored=True, mode="stored"), patch(f"{SERVICE}.read_answer", AsyncMock(return_value=None)),
        patch(f"{SERVICE}.saved_refusal", AsyncMock(return_value=None)),
    ):
        admitted = await require_runnable(
            SimpleNamespace(feature_enabled=lambda _: True), CODE, "analyst", user_id="user-1", workspace_id="ws-1",
        )
    config = SimpleNamespace(claude_code_hosted=admitted.hosted, claude_code_saved=admitted.saved, llm=SimpleNamespace(name=CODE))
    with pytest.raises(HTTPException) as refused:
        require_attended(config, _request(wakeup={"v": 1}))
    assert refused.value.detail["reason"] == "stored_unattended"
    require_attended(config, _request())


@pytest.mark.asyncio
async def test_an_automation_on_the_stored_token_is_refused_as_it_is_saved():
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    with platform(stored=True, mode="stored"), pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", CODE, "ws-1")
    assert str(refused.value) == CLAUDE_CODE_OFF["stored_unattended"]


@pytest.mark.asyncio
async def test_an_automation_naming_no_model_is_judged_on_the_default_its_firing_runs():
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted

    with platform(stored=True, mode="stored"), pytest.raises(AutomationRefusal) as refused:
        await _check_claude_code_hosted("user-1", None, "ws-1", pref={"preferred_model": CODE})
    assert str(refused.value) == CLAUDE_CODE_OFF["stored_unattended"]
    with platform(stored=True, mode="stored"):
        await _check_claude_code_hosted("user-1", None, "ws-1", pref={"preferred_model": "gpt-5"})
        with patch("src.server.services.automations.lifecycle.is_home_id", lambda *_: True):
            await _check_claude_code_hosted("user-1", None, "ws-1", pref={"preferred_model": CODE})


@pytest.mark.asyncio
async def test_an_automation_on_an_own_computer_relayed_the_stored_token_is_refused_as_it_is_saved():
    from src.server.services.automations.lifecycle import AutomationRefusal, _check_claude_code_hosted
    from src.server.services.claude_code.machine import MachineAnswer

    def answered(answer):
        return patch("src.server.services.claude_code.machine.read_answer", AsyncMock(return_value=answer))

    for answer in (None, MachineAnswer("none")):
        with platform(computer=OWN, stored=True, mode="stored"), answered(answer), pytest.raises(AutomationRefusal) as refused:
            await _check_claude_code_hosted("user-1", CODE, "ws-1")
        assert str(refused.value) == CLAUDE_CODE_OFF["stored_unattended"]
    with platform(computer=OWN, stored=True, mode="stored"), answered(MachineAnswer("works")):
        await _check_claude_code_hosted("user-1", CODE, "ws-1")


# -- Own computers and the token sheet --


@pytest.mark.asyncio
async def test_an_own_computer_without_a_token_is_relayed_the_stored_one_only_while_it_is_stored():
    row = AsyncMock(return_value={"access_token": TOKEN, "updated_at": "saved-at"})
    with (
        patch("src.server.services.claude_code.machine.stored_row", row),
        patch("src.server.services.claude_code.machine.usable", AsyncMock(return_value=True)),
    ):
        with platform(stored=True, mode="stored"):
            assert hosted_module.relay_offered()
            assert await ComputerToken("user-1", "c-1").read() == Credential(TOKEN)
        with platform(stored=True, mode="local"):
            assert await ComputerToken("user-1", "c-1").read() is None
    assert row.await_count == 1
    with platform(switch=True, stored=False):
        assert not hosted_module.relay_offered()


@pytest.mark.asyncio
async def test_a_token_is_pasted_on_the_platform_only_while_the_user_stores_it():
    enabled = AsyncMock(return_value=True)
    with platform(stored=True, mode="stored"), patch("src.server.services.claude_code.flow.user_feature_enabled", enabled):
        assert await backend_on("user-1")
    with platform(stored=True, mode="api_key"), patch("src.server.services.claude_code.flow.user_feature_enabled", enabled):
        assert not await backend_on("user-1")


@pytest.mark.asyncio
async def test_a_token_checked_while_the_user_stopped_storing_is_not_saved():
    @asynccontextmanager
    async def locked(*_):
        yield object()

    upsert = AsyncMock()
    with (
        serving(Cache()),
        platform(stored=True, mode="local"),
        patch("src.server.services.claude_code.saved.stored_row", AsyncMock(return_value=None)),
        patch("src.server.services.claude_code.saved.keep_check", AsyncMock()),
        patch("src.server.services.claude_code.saved.connection_change", locked),
        patch("src.server.services.claude_code.saved.upsert_oauth_tokens", upsert),
        pytest.raises(OAuthRefusal) as refused,
    ):
        await SavedHome("user-1").keep(
            TOKEN, held=AsyncMock(return_value=True), email=None, expires_at=None, token_uuid="",
        )
    assert refused.value.status_code == 409
    upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_checked_token_is_saved_reading_the_choice_on_the_writers_own_connection():
    conn = object()

    @asynccontextmanager
    async def locked(*_):
        yield conn

    upsert = AsyncMock()
    with (
        serving(Cache()),
        platform(stored=True, mode="stored") as reads,
        patch("src.server.services.claude_code.saved.stored_row", AsyncMock(return_value=None)),
        patch("src.server.services.claude_code.saved.keep_check", AsyncMock()),
        patch("src.server.services.claude_code.saved.connection_change", locked),
        patch("src.server.services.claude_code.saved.upsert_oauth_tokens", upsert),
    ):
        await SavedHome("user-1").keep(
            TOKEN, held=AsyncMock(return_value=True), email=None, expires_at=None, token_uuid="",
        )
    reads.mode.assert_awaited_once_with("user-1", conn=conn)
    assert upsert.await_args.kwargs["conn"] is conn


# -- Leaving the choice --


@pytest.mark.asyncio
async def test_any_choice_but_storing_takes_the_token_with_it():
    drop, saved = AsyncMock(return_value=0), AsyncMock()
    with (
        platform(stored=True, mode="stored"), patch(f"{HOSTED}.drop_stored", drop),
        patch(f"{HOSTED}.write_custody", saved),
    ):
        await hosted_module.choose("user-1", "api_key")
        await hosted_module.choose("user-1", "local")
        await hosted_module.choose("user-1", "stored", DISCLOSURE)
    assert drop.await_args_list == [call("user-1", "api_key"), call("user-1", "local")]
    saved.assert_awaited_once_with("user-1", "stored", disclosure=DISCLOSURE)


@pytest.mark.asyncio
async def test_dropping_the_stored_token_forgets_its_check_and_stops_each_turn_that_may_hold_it():
    cache = SimpleNamespace(delete=AsyncMock())
    with (
        patch("src.config.settings.HOST_MODE", "platform"),
        patch(f"{HOSTED}.write_custody", AsyncMock(return_value=TOKEN)) as leave,
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[("t-1", "r-1"), ("t-2", "r-2")])),
        patch("src.server.services.cancel_dispatch.cancel_workflow", AsyncMock()) as cancel,
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        stopped = await drop_stored("user-1", block_reason="terms")
    leave.assert_awaited_once_with("user-1", "local", block_reason="terms")
    cache.delete.assert_awaited_once_with(token_check_key(TOKEN))
    assert cancel.await_args_list == [call("t-1", "r-1", by_user=False), call("t-2", "r-2", by_user=False)]
    assert stopped == 2


@pytest.mark.asyncio
async def test_a_turn_that_fails_to_stop_leaves_none_of_the_others_on_the_stored_token_running():
    failed = HTTPException(status_code=500, detail="Failed to cancel the turn.")
    with (
        patch("src.config.settings.HOST_MODE", "platform"),
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[("t-1", "r-1"), ("t-2", "r-2")])),
        patch("src.server.services.cancel_dispatch.cancel_workflow", AsyncMock(side_effect=[failed, None])) as cancel,
        pytest.raises(hosted_module.TurnsNotStopped) as raised,
    ):
        await hosted_module.stop_stored_turns("user-1")
    assert raised.value.count == 1 and raised.value.__cause__ is failed
    assert cancel.await_args_list == [call("t-1", "r-1", by_user=False), call("t-2", "r-2", by_user=False)]


@pytest.mark.asyncio
async def test_turning_claude_code_off_on_the_platform_drops_the_stored_token():
    with platform(stored=True, mode="stored"), patch(f"{HOSTED}.drop_stored", AsyncMock()) as drop:
        await opted_out("user-1")
    drop.assert_awaited_once_with("user-1")
    with (
        platform(stored=True, mode="api_key"), patch(f"{HOSTED}.drop_stored", AsyncMock()) as drop,
        patch(f"{HOSTED}.stop_stored_turns", AsyncMock(return_value=0)) as stop,
    ):
        await opted_out("user-1")
    drop.assert_not_awaited()
    stop.assert_awaited_once_with("user-1")
    with (
        platform(stored=True, mode="stored"), patch("src.config.settings.HOST_MODE", "oss"),
        patch(f"{HOSTED}.drop_stored", AsyncMock()) as drop,
    ):
        await opted_out("user-1")
    drop.assert_not_awaited()


def test_claude_code_resolving_off_runs_the_opt_out_however_the_override_got_there():
    from src.server.services import features

    with (
        patch.dict(features._FEATURE_BEFORE_OFF_HOOKS, {}, clear=True),
        patch.dict(features._FEATURE_OFF_HOOKS, {}, clear=True),
    ):
        hosted_module.register_opt_out()
        hosted_module.register_opt_out()
        assert features._FEATURE_BEFORE_OFF_HOOKS["claude_code_runtime"] == [opted_out]
        assert "claude_code_runtime" not in features._FEATURE_OFF_HOOKS


@pytest.mark.asyncio
async def test_turning_claude_code_off_saves_nothing_while_the_stored_token_cannot_be_dropped():
    from httpx import ASGITransport, AsyncClient

    from src.config.features import FeatureGate, SystemFlag
    from src.server.app.features import router
    from src.server.services import features
    from tests.conftest import create_test_app

    prefs = {"other_preference": {"feature_overrides": {"claude_code_runtime": True}}}
    leave, upsert = AsyncMock(side_effect=OSError("connection lost")), AsyncMock()
    turns, cache = AsyncMock(return_value=[]), SimpleNamespace(delete=AsyncMock())
    with (
        patch.dict(features._FEATURE_BEFORE_OFF_HOOKS, {}, clear=True),
        platform(stored=True, mode="stored"),
        patch("src.server.services.features.system_flag", lambda _: SystemFlag(True, FeatureGate.OPT_IN, None)),
        patch("src.config.features.system_flag", lambda _: SystemFlag(True, FeatureGate.OPT_IN, None)),
        patch("src.server.services.features.get_user_preferences", AsyncMock(return_value=prefs)),
        patch("src.server.services.features.upsert_user_preferences", upsert),
        patch("src.server.services.features.invalidate_user_prefs_cache", AsyncMock()),
        patch(f"{HOSTED}.write_custody", leave),
        patch(f"{HOSTED}.live_stored_turns", turns),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
    ):
        hosted_module.register_opt_out()
        async with AsyncClient(transport=ASGITransport(app=create_test_app(router)), base_url="http://test") as c:
            got = await c.put("/api/v1/features/claude_code_runtime", json={"enabled": False})
    assert got.status_code == 500
    assert got.json()["detail"] == "Could not turn off feature: claude_code_runtime"
    leave.assert_awaited_once_with("test-user-123", "local", block_reason=None)
    upsert.assert_not_awaited()
    turns.assert_not_awaited()
    cache.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_turning_claude_code_off_whose_drop_left_a_turn_running_saves_no_override_and_turns_off_on_retry():
    from httpx import ASGITransport, AsyncClient

    from src.config.features import FeatureGate, SystemFlag
    from src.server.app.features import router
    from src.server.database.claude_code_hosted import Custody
    from src.server.services import features
    from tests.conftest import create_test_app

    prefs = {"other_preference": {"feature_overrides": {"claude_code_runtime": True}}}
    leave, upsert = AsyncMock(return_value=TOKEN), AsyncMock()
    cancel = AsyncMock(side_effect=[HTTPException(status_code=500, detail="Failed to cancel the turn."), None])
    with (
        patch.dict(features._FEATURE_BEFORE_OFF_HOOKS, {}, clear=True),
        platform(stored=True, mode="stored") as reads,
        patch("src.server.services.features.system_flag", lambda _: SystemFlag(True, FeatureGate.OPT_IN, None)),
        patch("src.config.features.system_flag", lambda _: SystemFlag(True, FeatureGate.OPT_IN, None)),
        patch("src.server.services.features.get_user_preferences", AsyncMock(return_value=prefs)),
        patch("src.server.services.features.upsert_user_preferences", upsert),
        patch("src.server.services.features.invalidate_user_prefs_cache", AsyncMock()),
        patch(f"{HOSTED}.write_custody", leave),
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[("t-1", "r-1")])),
        patch("src.server.services.cancel_dispatch.cancel_workflow", cancel),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=SimpleNamespace(delete=AsyncMock())),
    ):
        hosted_module.register_opt_out()
        async with AsyncClient(transport=ASGITransport(app=create_test_app(router)), base_url="http://test") as c:
            got = await c.put("/api/v1/features/claude_code_runtime", json={"enabled": False})
            assert got.status_code == 500
            assert got.json()["detail"] == "Could not turn off feature: claude_code_runtime"
            upsert.assert_not_awaited()
            reads.mode.return_value = Custody("local")
            again = await c.put("/api/v1/features/claude_code_runtime", json={"enabled": False})
    assert again.status_code == 200
    leave.assert_awaited_once()
    upsert.assert_awaited_once()
    assert cancel.await_args_list == [call("t-1", "r-1", by_user=False)] * 2


@pytest.mark.asyncio
async def test_leaving_the_stored_token_whose_turns_did_not_all_stop_says_so_not_that_it_is_still_stored(client):
    with (
        platform(stored=True, mode="stored"), patch(ROWS_ON, AsyncMock(return_value=True)),
        patch(f"{HOSTED}.drop_stored", AsyncMock(side_effect=hosted_module.TurnsNotStopped(1))),
    ):
        got = await client.put("/api/v1/claude-code/custody", json={"mode": "local"})
    assert got.status_code == 500
    assert got.json()["error"]["code"] == "turns_running"


@pytest.mark.asyncio
async def test_storing_raced_by_an_operators_block_is_refused_as_blocked(client):
    from src.server.database.claude_code_hosted import StoringBlocked

    with (
        platform(stored=True), patch(ROWS_ON, AsyncMock(return_value=True)),
        patch(f"{HOSTED}.write_custody", AsyncMock(side_effect=StoringBlocked("user-1"))),
    ):
        got = await client.put("/api/v1/claude-code/custody", json={"mode": "stored", "disclosure": DISCLOSURE})
    assert got.status_code == 403


@pytest.mark.asyncio
async def test_removing_the_token_stops_the_turns_on_it():
    @asynccontextmanager
    async def locked(*_):
        yield object()

    with (
        serving(Cache()),
        patch("src.config.settings.HOST_MODE", "platform"),
        patch("src.server.services.claude_code.saved.stored_token", AsyncMock(return_value=TOKEN)),
        patch("src.server.services.claude_code.saved.connection_change", locked),
        patch("src.server.services.claude_code.saved.delete_oauth_tokens", AsyncMock()),
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[("t-1", "r-1")])),
        patch("src.server.services.cancel_dispatch.cancel_workflow", AsyncMock()) as cancel,
    ):
        await SavedHome("user-1").remove(held=AsyncMock(return_value=True))
    assert cancel.await_args_list == [call("t-1", "r-1", by_user=False)]


@pytest.mark.asyncio
async def test_removing_the_token_whose_turn_did_not_stop_says_so_not_that_removing_failed():
    @asynccontextmanager
    async def locked(*_):
        yield object()

    delete = AsyncMock()
    with (
        serving(Cache()),
        patch("src.config.settings.HOST_MODE", "platform"),
        patch("src.server.services.claude_code.saved.stored_token", AsyncMock(return_value=TOKEN)),
        patch("src.server.services.claude_code.saved.connection_change", locked),
        patch("src.server.services.claude_code.saved.delete_oauth_tokens", delete),
        patch(f"{HOSTED}.live_stored_turns", AsyncMock(return_value=[("t-1", "r-1")])),
        patch("src.server.services.cancel_dispatch.cancel_workflow", AsyncMock(side_effect=HTTPException(status_code=500))),
        pytest.raises(OAuthRefusal) as refused,
    ):
        await SavedHome("user-1").remove(held=AsyncMock(return_value=True))
    delete.assert_awaited_once()
    assert (refused.value.status_code, refused.value.error["code"]) == (500, "turns_running")


# -- The deploy bar --


@pytest.mark.asyncio
async def test_with_both_switches_off_no_choice_is_read_anywhere():
    with platform(switch=False, stored=False, computer=OWN) as reads:
        assert not await hosted_module.saved_token_runs("user-1")
        assert not hosted_module.relay_offered()
        assert await _stamp() == {}
    reads.mode.assert_not_awaited()
