"""The activity recorder: written only for an account in an organization,
never holding a credential, and never failing the act it records."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.database import activity_events
from src.server.services import activity
from src.server.services.account_contract import NO_ORG, Account, Org
from src.server.services.activity import Action, clean_detail
from src.server.services.policy import PUBLIC_LINKS, policy_denied
from src.server.utils.api import note_caller

ACCOUNT = "src.server.services.account"
ACME = Account(org=Org(id="org-1", name="Acme"))
WS = "90000000-0000-4000-8000-000000000001"


@contextmanager
def _account(answer, **kwargs):
    with (
        patch("src.server.services.membership.teams_door_open", return_value=True),
        patch(f"{ACCOUNT}.account_for", new_callable=AsyncMock, return_value=answer, **kwargs),
    ):
        yield


@pytest.fixture
def insert(monkeypatch) -> AsyncMock:
    mock = AsyncMock()
    monkeypatch.setattr(activity_events, "insert", mock)
    return mock


@pytest.mark.asyncio
async def test_an_account_in_an_organization_is_recorded(insert):
    with _account(ACME):
        activity.record(
            "u1", Action.THREAD_LINK_ON, resource=("thread", "t1"),
            workspace_id=WS, thread_id="not-a-uuid", detail={"allow_files": True},
        )
        await activity.drain()
    insert.assert_awaited_once()
    row = insert.await_args.kwargs
    assert row["org_id"] == "org-1"
    assert row["actor_id"] == "u1"
    assert row["action"] == "sharing.thread_link_on"
    assert row["outcome"] == "done"
    assert (row["resource_kind"], row["resource_id"]) == ("thread", "t1")
    assert row["workspace_id"] == WS
    assert row["thread_id"] is None  # not a UUID: left out, row still written
    assert row["detail"] == {"allow_files": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("answer", [NO_ORG, None])
async def test_no_organization_or_an_unknown_account_writes_nothing(insert, answer):
    with _account(answer):
        activity.record("u1", Action.COMPUTER_ADDED)
        await activity.drain()
    insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_the_teams_flag_off_nothing_is_read_or_written(insert):
    """The recorder pays only ``account_for``'s shut door: no cache read, no
    request, no row."""
    cache, client = MagicMock(), AsyncMock()
    with (
        patch(f"{ACCOUNT}.is_feature_enabled_system", lambda key: False),
        patch("src.server.services.membership.is_feature_enabled_system", lambda key: False),
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        patch("src.server.services.platform_client.get_http_client", return_value=client),
    ):
        activity.record("u1", Action.MODEL_KEY_SAVED, detail={"provider": "x"})
        activity.record_refusal(
            policy_denied(PUBLIC_LINKS, "no", "deployment", status_code=403), "u1"
        )
        assert not activity._pending  # nothing even scheduled
    insert.assert_not_awaited()
    assert not cache.method_calls
    client.post.assert_not_called()


@pytest.mark.asyncio
async def test_a_failed_write_is_logged_and_never_raised(insert, caplog):
    insert.side_effect = RuntimeError("database down")
    with _account(ACME):
        activity.record("u1", Action.SECRET_DELETED, detail={"name": "S"})
        await activity.drain()
    assert any(
        r.levelname == "ERROR" and "credential.secret_deleted" in r.getMessage()
        and "u1" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_the_act_never_waits_on_a_slow_account_read(insert):
    """A cold membership read that takes seconds is left to finish in the
    background, so it is cached for the member's next act and the row lands."""
    answered = asyncio.Event()

    async def slow_account(_user_id):
        await answered.wait()
        return ACME

    with _account(None, side_effect=slow_account):
        activity.record("u1", Action.COMPUTER_REMOVED)
        insert.assert_not_awaited()
        answered.set()
        await activity.drain()
    insert.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_write_past_its_bound_is_abandoned_and_logged(insert, monkeypatch, caplog):
    monkeypatch.setattr(activity, "_WRITE_TIMEOUT_S", 0.05)

    async def stall(**_):
        await asyncio.sleep(5)

    insert.side_effect = stall
    with _account(ACME):
        activity.record("u1", Action.COMPUTER_REMOVED)
        await asyncio.wait_for(activity.drain(), 1)
    assert any(
        r.levelname == "ERROR" and "computer.removed" in r.getMessage() and "u1" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_past_the_pending_bound_an_event_is_dropped_and_logged(insert, monkeypatch, caplog):
    monkeypatch.setattr(activity, "_MAX_PENDING", 1)
    answered = asyncio.Event()

    async def slow_account(_user_id):
        await answered.wait()
        return ACME

    with _account(None, side_effect=slow_account):
        activity.record("u1", Action.COMPUTER_ADDED)
        activity.record("u2", Action.COMPUTER_REMOVED)
        answered.set()
        await activity.drain()
    assert [c.kwargs["actor_id"] for c in insert.await_args_list] == ["u1"]
    assert any(
        r.levelname == "ERROR" and "computer.removed" in r.getMessage() and "u2" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_shutdown_waits_briefly_then_abandons_what_is_pending(insert, caplog):
    stalled = asyncio.Event()

    async def stall(**_):
        await stalled.wait()

    insert.side_effect = stall
    with _account(ACME):
        activity.record("u1", Action.SECRET_CREATED, detail={"name": "S"})
        await asyncio.wait_for(activity.drain(timeout=0.05), 1)
    assert not activity._pending
    assert any(
        r.levelname == "ERROR" and "credential.secret_created" in r.getMessage()
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_an_action_outside_the_catalog_is_never_written(insert):
    with _account(ACME):
        activity.record("u1", "sharing.made_up")  # type: ignore[arg-type]
        await activity.drain()
    insert.assert_not_awaited()


def test_the_detail_guard_keeps_names_and_drops_credentials():
    kept, refused = clean_detail({
        "name": "OPENAI",
        "groups": ["read", "trade"],
        "enabled": True,
        "count": 3,
        "host": "mcp.example.com",
        "api_key": "sk-x",
        "access_token": "t",
        "client_secret": "s",
        "password": "p",
        "value": "v",
        "Authorization": "Bearer x",
        "nested": {"a": 1},
        "long": "x" * 1000,
        "many": list(range(100)),
    })
    assert kept == {
        "name": "OPENAI",
        "groups": ["read", "trade"],
        "enabled": True,
        "count": 3,
        "host": "mcp.example.com",
    }
    assert set(refused) == {
        "api_key", "access_token", "client_secret", "password", "value",
        "Authorization", "nested", "long", "many",
    }


@pytest.mark.asyncio
async def test_a_refused_entry_is_left_out_and_the_event_kept(insert):
    with _account(ACME):
        activity.record("u1", Action.SECRET_CREATED, detail={"name": "S", "value": "v"})
        await activity.drain()
    assert insert.await_args.kwargs["detail"] == {"name": "S"}


@pytest.mark.asyncio
async def test_the_caller_and_trace_come_from_the_request(insert):
    request = MagicMock()
    request.scope = {"state": {"trace_id": "trace-1"}}
    note_caller(request, "u1", "oauth", "client-9")
    activity.bind_request(request.scope)
    with _account(ACME):
        activity.record("u1", Action.CONNECTOR_ADDED)
        # Another user's request says nothing about how this one called.
        activity.record("u2", Action.CONNECTOR_ADDED)
        # Taken when recorded: the request may be gone by the time it lands.
        request.scope.clear()
        await activity.drain()
    first, second = (c.kwargs for c in insert.await_args_list)
    assert (first["auth"], first["client_id"], first["trace_id"]) == ("oauth", "client-9", "trace-1")
    assert (second.get("auth"), second["trace_id"]) == (None, "trace-1")


@pytest.mark.asyncio
async def test_a_refusal_is_recorded_once_however_many_surfaces_see_it(insert):
    refusal = policy_denied(PUBLIC_LINKS, "no", "organization", status_code=403)
    with _account(ACME):
        activity.record_refusal(refusal, "u1")
        activity.record_refusal(refusal, "u1")
        await activity.drain()
    insert.assert_awaited_once()
    row = insert.await_args.kwargs
    assert (row["action"], row["outcome"]) == ("policy.refused", "refused")
    assert row["detail"] == {"rule": PUBLIC_LINKS, "set_by": "organization"}


@pytest.mark.asyncio
async def test_an_ordinary_http_error_is_not_a_refusal(insert):
    with _account(ACME):
        activity.record_refusal(HTTPException(403, "no"), "u1")
        await activity.drain()
    insert.assert_not_awaited()
