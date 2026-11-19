"""Every refusal by policy reaches the organization's activity log once: as a
route's response, in a turn's error frame, and at the relay."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.server.app.setup import RequestIDMiddleware, _policy_denied_handler
from src.server.database import activity_events
from src.server.services import activity
from src.server.services.account_contract import Account, Org
from src.server.services.egress import RelayError, RelayRejection
from src.server.services.policy import (
    PUBLIC_LINKS,
    TRADING,
    PolicyDenied,
    policy_denied,
)
from src.server.utils.api import CurrentUserId
from tests.unit.server.policy_standing import standing_under
from tests.unit.server.test_policy_trading import NO_TRADING, _live_order
from tests.unit.server.services.test_egress_order_gate import _prepare

ACME = Account(org=Org(id="org-1", name="Acme"))
WS = "90000000-0000-4000-8000-000000000001"
THREAD = "70000000-0000-4000-8000-000000000001"


@pytest.fixture
def insert(monkeypatch) -> AsyncMock:
    mock = AsyncMock()
    monkeypatch.setattr(activity_events, "insert", mock)
    return mock


class _Redis:
    """SET NX with a TTL, as ``acquire_lock`` answers it; ``down`` answers
    as a Redis that cannot be reached."""

    def __init__(self) -> None:
        self.keys: dict[str, int] = {}
        self.down = False

    async def acquire_lock(self, key: str, token: str, ttl_ms: int) -> bool | None:
        if self.down:
            return None
        if key in self.keys:
            return False
        self.keys[key] = ttl_ms
        return True


@pytest.fixture
def redis() -> _Redis:
    return _Redis()


@pytest.fixture
def in_org(redis):
    with (
        patch("src.server.services.membership.teams_door_open", return_value=True),
        patch(
            "src.server.services.account.account_for", new_callable=AsyncMock,
            return_value=ACME,
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=redis),
    ):
        yield


def _post(path: str):
    """The response, once the writes it scheduled have landed: one client
    for both, so its loop outlives the response."""
    with TestClient(_app()) as client:
        response = client.post(path)
        client.portal.call(activity.drain)
    return response


def _refusal() -> PolicyDenied:
    return policy_denied(  # type: ignore[return-value]
        PUBLIC_LINKS, "Sharing a public link is not allowed.", "organization", status_code=403
    )


def _app() -> FastAPI:
    app = FastAPI()
    # Binds the request a record names, as on every request the app serves.
    app.add_middleware(RequestIDMiddleware)
    app.add_exception_handler(PolicyDenied, _policy_denied_handler)

    @app.post("/api/v1/threads/{thread_id}/share")
    async def share(thread_id: str, user_id: CurrentUserId):
        raise _refusal()

    @app.post("/api/v1/computers/{computer_id}/folders/{folder_id}/start")
    async def start(computer_id: str, folder_id: str, user_id: CurrentUserId):
        raise _refusal()

    @app.post("/anonymous")
    async def anonymous():
        raise _refusal()

    return app


def test_a_routes_refusal_is_recorded_and_answered_unchanged(insert, in_org):
    response = _post(f"/api/v1/threads/{THREAD}/share")
    assert response.status_code == 403
    assert response.json() == {
        "detail": {
            "message": "Sharing a public link is not allowed.",
            "type": "policy_denied",
            "rule": PUBLIC_LINKS,
            "set_by": "organization",
        }
    }
    insert.assert_awaited_once()
    row = insert.await_args.kwargs
    assert (row["action"], row["outcome"], row["actor_id"]) == (
        "policy.refused", "refused", "local-dev-user",
    )
    assert row["auth"] == "session"
    # What it was about comes off the path, which the raise site never passed.
    assert (row["thread_id"], row["workspace_id"]) == (THREAD, None)
    assert row["detail"] == {
        "rule": PUBLIC_LINKS,
        "set_by": "organization",
        "method": "POST",
        "route": "/api/v1/threads/{thread_id}/share",
    }


def test_a_route_refusal_carries_every_uuid_its_path_names(insert, in_org):
    computer = "30000000-0000-4000-8000-000000000003"
    response = _post(f"/api/v1/computers/{computer}/folders/not-a-uuid/start")
    assert response.status_code == 403
    row = insert.await_args.kwargs
    assert row["detail"]["computer_id"] == computer
    assert "folder_id" not in row["detail"]  # not a UUID: left out


def test_a_refusal_with_no_known_caller_is_answered_and_not_recorded(insert, in_org):
    response = _post("/anonymous")
    assert response.status_code == 403
    insert.assert_not_awaited()


def test_a_failed_record_still_answers_the_refusal(insert, in_org):
    insert.side_effect = RuntimeError("database down")
    response = _post(f"/api/v1/threads/{THREAD}/share")
    assert response.status_code == 403
    assert response.json()["detail"]["rule"] == PUBLIC_LINKS


@pytest.mark.asyncio
async def test_a_turns_refusal_is_recorded_once_though_it_reaches_the_handler_too(
    insert, in_org
):
    from src.server.handlers.chat.error_handling import handle_workflow_error

    refusal = _refusal()
    scope = MagicMock(release_slot=AsyncMock(), owned_run_handle=None, handed_off=False)
    frames = [
        f
        async for f in handle_workflow_error(
            refusal, THREAD, "u1", WS, None, None, scope, 0.0,
            MagicMock(request_key=None), False,
        )
    ]
    assert frames and "policy_denied" in frames[0]
    request = MagicMock(scope={"state": {}})
    await _policy_denied_handler(request, refusal)
    await activity.drain()
    insert.assert_awaited_once()
    row = insert.await_args.kwargs
    assert (row["actor_id"], row["workspace_id"], row["thread_id"]) == ("u1", WS, THREAD)


@pytest.mark.asyncio
async def test_the_relays_refusal_of_a_live_order_is_recorded(insert, in_org):
    with standing_under(NO_TRADING), pytest.raises(RelayRejection) as refused:
        await _prepare(**_live_order())
    assert refused.value.code == RelayError.POLICY_WITHHELD
    await activity.drain()
    insert.assert_awaited_once()
    row = insert.await_args.kwargs
    assert (row["action"], row["outcome"]) == ("policy.refused", "refused")
    assert row["detail"]["rule"] == TRADING
    assert row["detail"]["set_by"] == "organization"
    assert row["detail"]["via"] == "relay"
    # The relay's own token, presented by sandbox code.
    assert (row["auth"], row["detail"]["caller"]) == ("agent", "sandbox")


@pytest.mark.asyncio
async def test_the_relay_records_nothing_for_an_unknown_policy(insert, in_org):
    with standing_under(None), pytest.raises(RelayRejection):
        await _prepare(**_live_order())
    await activity.drain()
    insert.assert_not_awaited()


# --- refusals that become text, never reaching a handler ---------------------


async def _refusal_rows(insert) -> list[dict]:
    await activity.drain()
    return [c.kwargs for c in insert.await_args_list]


@pytest.mark.asyncio
async def test_a_direct_call_to_a_withheld_own_server_is_recorded(insert, in_org, monkeypatch):
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from tests.unit.server.services.test_direct_tool_approval_gate import _header
    from tests.unit.server.test_policy_user_connectors_gates import NO_OWN

    _header(monkeypatch, {})
    with standing_under(NO_OWN):
        assert await DirectMCPBinding(user_id="u1", workspace_id=WS).check("acme", "read")
    (row,) = await _refusal_rows(insert)
    assert (row["action"], row["outcome"], row["workspace_id"]) == (
        "policy.refused", "refused", WS,
    )
    assert row["detail"] == {
        "rule": "user_connectors", "set_by": "organization",
        "via": "direct", "caller": "host", "server": "acme", "tool": "read",
    }
    # Made by the member's agent, not a member request.
    assert (row["auth"], row["client_id"]) == ("agent", None)


@pytest.mark.asyncio
async def test_a_direct_live_order_the_trading_rule_withholds_is_recorded(
    insert, in_org, monkeypatch
):
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from tests.unit.server.services.test_direct_tool_approval_gate import LIVE, _connected

    _connected(monkeypatch, [], order_approval={"live": False})
    with standing_under(NO_TRADING):
        assert await DirectMCPBinding(user_id="u1").check("moomoo", LIVE, False)
    (row,) = await _refusal_rows(insert)
    assert row["detail"]["rule"] == TRADING
    assert (row["detail"]["via"], row["detail"]["tool"]) == ("direct", LIVE)


@pytest.mark.asyncio
async def test_a_computer_the_policy_keeps_work_off_is_recorded_when_reached(insert, in_org):
    from src.server.services import computer_reach
    from src.server.services.computer_reach import TurnComputers
    from tests.unit.server.services.test_computer_reach import (
        HOME_ID,
        MAC_ID,
        OWN,
        USER,
        _manager,
        _reach_with_row,
    )
    from ptc_agent.agent.tools.computer_reach import ComputerUnreachable

    manager = _manager()
    manager.reach_computer = AsyncMock(
        side_effect=policy_denied("own_machines", "No.", "organization", status_code=403)
    )
    turn = TurnComputers(USER, HOME_ID, OWN)
    with patch.object(
        computer_reach, "get_computer",
        AsyncMock(return_value={"status": "stopped", "kind": "ssh", "user_id": USER}),
    ):
        with pytest.raises(ComputerUnreachable):
            await _reach_with_row(manager, turn)
    (row,) = await _refusal_rows(insert)
    assert row["actor_id"] == USER
    assert row["detail"]["rule"] == "own_machines"
    assert row["detail"]["computer_id"] == MAC_ID


@pytest.mark.asyncio
async def test_an_insight_runs_refusal_is_recorded(insert, in_org):
    from src.server.services import insight_service

    refusal = policy_denied("models.allowed", "No.", "organization", model="m")
    with (
        patch("src.server.app.setup.agent_config", MagicMock()),
        patch(
            "src.server.services.llm.config.resolve_llm_config",
            AsyncMock(side_effect=refusal),
        ),
        pytest.raises(PolicyDenied),
    ):
        await insight_service._research("brief", "u1", insight_type="personalized")
    (row,) = await _refusal_rows(insert)
    assert row["detail"]["rule"] == "models.allowed"
    assert row["detail"]["model"] == "m"


@pytest.mark.asyncio
async def test_a_sign_in_saves_refusal_is_recorded_as_it_becomes_the_callbacks(
    insert, in_org
):
    from src.server.services.mcp_oauth.finish import CallbackFailed
    from src.server.services.mcp_saves import _refused

    async def refuse():
        raise policy_denied("user_connectors", "No.", "organization", status_code=403)

    with pytest.raises(CallbackFailed):
        await _refused(refuse(), "u1")
    (row,) = await _refusal_rows(insert)
    assert row["detail"]["rule"] == "user_connectors"


# --- a refusal the agent repeats is one row a window -------------------------


@pytest.mark.asyncio
async def test_a_relay_refusal_retried_in_a_loop_is_one_row(insert, in_org, redis):
    with standing_under(NO_TRADING):
        for _ in range(5):
            with pytest.raises(RelayRejection):
                await _prepare(**_live_order())
    (row,) = await _refusal_rows(insert)
    assert row["detail"]["via"] == "relay"
    (ttl,) = redis.keys.values()
    assert ttl == activity._ONCE_WINDOW_S * 1000
    # The window over, the next refusal is recorded again.
    redis.keys.clear()
    with standing_under(NO_TRADING), pytest.raises(RelayRejection):
        await _prepare(**_live_order())
    assert len(await _refusal_rows(insert)) == 2


@pytest.mark.asyncio
async def test_a_direct_refusal_is_one_row_per_tool_a_window(insert, in_org, monkeypatch):
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from tests.unit.server.services.test_direct_tool_approval_gate import _header
    from tests.unit.server.test_policy_user_connectors_gates import NO_OWN

    _header(monkeypatch, {})
    binding = DirectMCPBinding(user_id="u1")
    with standing_under(NO_OWN):
        for tool in ("read", "read", "read", "write", "write"):
            assert await binding.check("acme", tool)
    rows = await _refusal_rows(insert)
    assert [r["detail"]["tool"] for r in rows] == ["read", "write"]


@pytest.mark.asyncio
async def test_with_redis_unreachable_a_repeatable_refusal_is_not_recorded(
    insert, in_org, redis
):
    redis.down = True
    with standing_under(NO_TRADING), pytest.raises(RelayRejection):
        await _prepare(**_live_order())
    assert await _refusal_rows(insert) == []


@pytest.mark.asyncio
async def test_a_malformed_destination_still_answers_the_relays_403(insert, in_org):
    """The host is read behind the recorder's guard, so an address urlsplit
    cannot parse costs the row its host, never the refusal its status."""
    from tests.unit.server.test_policy_user_connectors_gates import (
        NO_OWN,
        _own_grant,
        relay_frame,
    )

    grant = _own_grant(url="https://[::1/mcp")
    with standing_under(NO_OWN), pytest.raises(RelayRejection) as refused:
        await relay_frame(grant)
    assert (refused.value.status, refused.value.code) == (403, RelayError.POLICY_WITHHELD)
    (row,) = await _refusal_rows(insert)
    assert row["detail"]["host"] is None


def test_host_of_never_raises_and_keeps_only_the_host():
    assert activity.host_of("https://user:pw@api.example.com:8443/v1?k=s") == "api.example.com"
    assert activity.host_of("https://[::1/mcp") is None
    assert activity.host_of(None) is None
    assert activity.host_of(object()) is None
