"""The routes around the user's OK for what a computer's ``la`` asks.

The app's own routes (``app.cli_approvals``) list and answer requests, for a
person only. Every route a computer's token may act through only once the
user approves calls its surface in ``dependencies.cli_approval`` once, which
these pin on the real routers, with the surface replaced by one that stops
the request, so a route that drops its gate fails here. A delete route that
forgets it is refused by ``require_scopes`` itself (``gated``), and the set
of gated deletes is pinned below, so a new one needs a pin of its own.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.app import cli_approvals as approvals_app
from src.server.dependencies import cli_approval
from src.server.services import cli_approvals as store
from tests.conftest import create_test_app
from tests.unit.server.dependencies.reach_world import (
    AUTOMATION,
    OTHER_COMPUTER,
    OTHER_THREAD,
    OTHER_WORKSPACE,
    USER,
    act_as_computer,
)

APPROVAL = "f6600000-0000-4000-8000-000000000001"


def _approval(approval_id: str = APPROVAL) -> store.Approval:
    now = datetime(2026, 11, 14, 12, tzinfo=UTC)
    return store.Approval(
        approval_id=approval_id,
        action="thread_delete",
        status=store.PENDING,
        computer_id=OTHER_COMPUTER,
        digest="d",
        summary={
            "target": {"id": OTHER_THREAD, "name": "Old notes"},
            "details": {},
            "requester": {"kind": "command"},
        },
        created_at=now,
        expires_at=now + timedelta(minutes=30),
    )


async def _client(app) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


# -- the app's routes --------------------------------------------------------------


@pytest.fixture
def approvals_api():
    return create_test_app(approvals_app.router)


@pytest.mark.asyncio
async def test_the_app_lists_what_waits_as_the_wire_shape(approvals_api, monkeypatch):
    monkeypatch.setattr(store, "pending", AsyncMock(return_value=[_approval()]))

    async with await _client(approvals_api) as c:
        resp = await c.get("/api/v1/cli/approvals")

    assert resp.status_code == 200
    assert resp.json() == {
        "approvals": [
            {
                "approval_id": APPROVAL,
                "status": "pending",
                "action": "thread_delete",
                "target": {"id": OTHER_THREAD, "name": "Old notes"},
                "details": {},
                "requester": {"kind": "command"},
                "created_at": "2026-11-14T12:00:00+00:00",
                "expires_at": "2026-11-14T12:30:00+00:00",
            }
        ]
    }


@pytest.mark.asyncio
async def test_with_redis_down_the_app_reads_an_empty_list(approvals_api, monkeypatch):
    monkeypatch.setattr(store, "pending", AsyncMock(side_effect=store.Unavailable()))

    async with await _client(approvals_api) as c:
        resp = await c.get("/api/v1/cli/approvals")

    assert resp.status_code == 200
    assert resp.json() == {"approvals": []}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "status", "kind"),
    [
        (store.Unknown(), 404, None),
        (store.AlreadyDecided(), 409, "approval_decided"),
        (store.Unavailable(), 503, "approval_unavailable"),
    ],
)
async def test_an_answer_the_store_refuses_is_answered_as_such(
    approvals_api, monkeypatch, raised, status, kind
):
    monkeypatch.setattr(store, "decide", AsyncMock(side_effect=raised))

    async with await _client(approvals_api) as c:
        resp = await c.post(f"/api/v1/cli/approvals/{APPROVAL}/decision", json={"approve": True})

    assert resp.status_code == status
    if kind is not None:
        assert resp.json()["detail"]["type"] == kind


@pytest.mark.asyncio
async def test_a_session_answers_as_itself(approvals_api, monkeypatch):
    decide = AsyncMock(return_value={"approval_id": APPROVAL, "status": "approved"})
    monkeypatch.setattr(store, "decide", decide)

    async with await _client(approvals_api) as c:
        resp = await c.post(f"/api/v1/cli/approvals/{APPROVAL}/decision", json={"approve": True})

    assert resp.status_code == 200
    decide.assert_awaited_once_with("test-user-123", APPROVAL, approve=True)


@pytest.mark.asyncio
async def test_a_computer_token_neither_lists_nor_answers(approvals_api, monkeypatch):
    decide = AsyncMock()
    monkeypatch.setattr(store, "decide", decide)
    act_as_computer(approvals_api)

    async with await _client(approvals_api) as c:
        listed = await c.get("/api/v1/cli/approvals")
        decided = await c.post(
            f"/api/v1/cli/approvals/{APPROVAL}/decision", json={"approve": True}
        )

    assert (listed.status_code, decided.status_code) == (403, 403)
    decide.assert_not_awaited()


# -- every gated route calls its gate ----------------------------------------------


@pytest.fixture
def gates(monkeypatch) -> list[str]:
    """The surfaces the request reached, each stopping it there."""
    reached: list[str] = []

    def stop(name):
        async def surface(*args, **kwargs):
            reached.append(name)
            raise HTTPException(status_code=418, detail=name)

        return surface

    for name in (
        "thread_delete",
        "workspace_delete",
        "workspace_create",
        "automation_delete",
        "computer_remove",
        "admit_turn",
        "admit_retry",
        "automation_runs",
    ):
        monkeypatch.setattr(cli_approval, name, stop(name))
    return reached


@pytest.fixture
def gated_app(monkeypatch):
    from src.server.app import automations, computers, workspaces
    from src.server.app.threads import router as threads_router

    app = create_test_app(threads_router, workspaces.router, automations.router, computers.router)
    # A command that names no call: its loop guards have nothing to check.
    act_as_computer(app, call_thread=None)
    monkeypatch.setattr("src.server.utils.api.require_thread_owner", AsyncMock())
    monkeypatch.setattr(
        "src.server.app.threads.crud.get_thread_by_id",
        AsyncMock(return_value={"workspace_id": OTHER_WORKSPACE, "title": "Old notes"}),
    )
    monkeypatch.setattr(
        workspaces,
        "db_get_workspace",
        AsyncMock(
            return_value={"workspace_id": OTHER_WORKSPACE, "user_id": USER, "status": "running"}
        ),
    )
    monkeypatch.setattr(
        computers,
        "_owned_computer",
        AsyncMock(return_value={"computer_id": OTHER_COMPUTER, "user_id": USER}),
    )
    monkeypatch.setattr(
        computers,
        "_computer_manager",
        lambda: SimpleNamespace(removal_refusal=AsyncMock(return_value=None)),
    )
    return app


_SAY = {"messages": [{"role": "user", "content": "Hi"}], "workspace_id": OTHER_WORKSPACE}
_AUTOMATION = {
    "name": "Sweep",
    "instruction": "Check.",
    "trigger_type": "cron",
    "cron_expression": "0 9 * * *",
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "body", "surface"),
    [
        ("DELETE", f"/api/v1/threads/{OTHER_THREAD}", None, "thread_delete"),
        ("DELETE", f"/api/v1/workspaces/{OTHER_WORKSPACE}", None, "workspace_delete"),
        ("POST", "/api/v1/workspaces", {"name": "Notes"}, "workspace_create"),
        ("DELETE", f"/api/v1/automations/{AUTOMATION}", None, "automation_delete"),
        ("DELETE", f"/api/v1/computers/{OTHER_COMPUTER}", None, "computer_remove"),
        ("POST", "/api/v1/threads/messages", _SAY, "admit_turn"),
        ("POST", f"/api/v1/threads/{OTHER_THREAD}/messages", _SAY, "admit_turn"),
        (
            "POST",
            f"/api/v1/threads/{OTHER_THREAD}/retry",
            {"workspace_id": OTHER_WORKSPACE},
            "admit_retry",
        ),
        ("POST", "/api/v1/automations", _AUTOMATION, "automation_runs"),
        ("PATCH", f"/api/v1/automations/{AUTOMATION}", {"name": "New"}, "automation_runs"),
        ("POST", f"/api/v1/automations/{AUTOMATION}/trigger", None, "automation_runs"),
        ("POST", f"/api/v1/automations/{AUTOMATION}/resume", None, "automation_runs"),
    ],
)
async def test_each_gated_route_calls_its_gate_once(
    gated_app, gates, method, path, body, surface
):
    async with await _client(gated_app) as c:
        resp = await c.request(method, path, json=body)

    assert resp.status_code == 418, resp.text
    assert gates == [surface]


def _gated(dependant) -> bool:
    for dep in dependant.dependencies:
        call = dep.call
        if inspect.isfunction(call) and inspect.getclosurevars(call).nonlocals.get("gated"):
            return True
        if _gated(dep):
            return True
    return False


def test_the_gated_deletes_are_the_ones_pinned_above():
    from src.server.app.setup import app

    gated = {
        (method, route.path)
        for route in app.routes
        if hasattr(route, "dependant") and _gated(route.dependant)
        for method in route.methods
    }

    assert gated == {
        ("DELETE", "/api/v1/threads/{thread_id}"),
        ("DELETE", "/api/v1/workspaces/{workspace_id}"),
        ("DELETE", "/api/v1/automations/{automation_id}"),
        ("DELETE", "/api/v1/computers/{computer_id}"),
    }
