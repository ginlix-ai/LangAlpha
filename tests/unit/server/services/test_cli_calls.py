"""Every request a computer's token makes is recorded with the thread whose
command made it; nobody else's is."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.testclient import TestClient

from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.database import computer_cli_calls
from src.server.dependencies.principal import CALL_HEADER, _bearer, get_principal
from src.server.services import cli_calls, cli_tokens
from src.server.services.cli_calls import CliCallRecorder
from src.server.services.cli_tokens import CliIdentity
from src.server.services.livefs import outcomes

COMPUTER = "c0000000-0000-4000-8000-000000000001"
THREAD = "70000000-0000-4000-8000-000000000001"
WORKSPACE = "90000000-0000-4000-8000-000000000001"
CALL_ID = "call-0123456789"


TOKEN = f"lac1.{COMPUTER}.secret"


async def _caller(
    request: Request, credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)
) -> None:
    """A computer's bearer is read as ``get_principal`` reads it; any other
    caller passes unread."""
    if credentials is not None and cli_tokens.is_cli_token(credentials.credentials):
        await get_principal(request, credentials)


def _app() -> FastAPI:
    app = FastAPI(dependencies=[Depends(_caller)])

    @app.get("/api/v1/workspaces/{workspace_id}")
    async def workspace(workspace_id: str):
        return {"workspace_id": workspace_id}

    @app.delete("/api/v1/workspaces/{workspace_id}")
    async def delete(workspace_id: str):
        raise HTTPException(status_code=403, detail="no")

    app.add_middleware(CliCallRecorder)
    return app


def _computer(extra: dict | None = None) -> dict:
    return {"Authorization": f"Bearer {TOKEN}", **(extra or {})}


@pytest.fixture
def recorded(monkeypatch) -> AsyncMock:
    record = AsyncMock()
    monkeypatch.setattr(computer_cli_calls, "record", record)
    monkeypatch.setattr(
        cli_tokens,
        "authenticate",
        AsyncMock(return_value=CliIdentity(COMPUTER, "u1", ("agent:read",))),
    )
    monkeypatch.setattr(
        outcomes,
        "call_context",
        AsyncMock(return_value=CallContext(workspace_id=WORKSPACE, thread_id=THREAD)),
    )
    return record


def test_a_computers_request_is_recorded_with_its_thread(recorded):
    client = TestClient(_app())

    client.get(
        f"/api/v1/workspaces/{WORKSPACE}",
        headers=_computer({CALL_HEADER: CALL_ID}),
    )

    recorded.assert_awaited_once()
    assert recorded.await_args.kwargs == {
        "computer_id": COMPUTER,
        "user_id": "u1",
        "thread_id": THREAD,
        "call_id": CALL_ID,
        "method": "GET",
        "route": "/api/v1/workspaces/{workspace_id}",
        "targets": {"workspace_id": WORKSPACE},
        "status": 200,
    }


def test_a_refused_request_is_recorded_with_its_status(recorded):
    client = TestClient(_app())

    client.delete(
        f"/api/v1/workspaces/{WORKSPACE}",
        headers=_computer({CALL_HEADER: CALL_ID}),
    )

    assert recorded.await_args.kwargs["status"] == 403


def test_a_request_with_no_call_is_recorded_with_no_thread(recorded):
    client = TestClient(_app())

    client.get(f"/api/v1/workspaces/{WORKSPACE}", headers=_computer())

    assert recorded.await_args.kwargs["thread_id"] is None
    assert recorded.await_args.kwargs["call_id"] is None


def test_other_callers_are_not_recorded(recorded):
    TestClient(_app()).get(f"/api/v1/workspaces/{WORKSPACE}")

    recorded.assert_not_awaited()


def test_a_failed_write_does_not_fail_the_request(recorded):
    recorded.side_effect = RuntimeError("database down")

    response = TestClient(_app()).get(
        f"/api/v1/workspaces/{WORKSPACE}", headers=_computer()
    )

    assert response.status_code == 200


def test_the_response_does_not_wait_for_the_write(recorded, monkeypatch):
    """The row is written beside the response: a write that hangs holds back
    neither the status nor the body, however long it is given."""
    monkeypatch.setattr(cli_calls, "_WRITE_TIMEOUT_S", 60.0)

    async def stuck(**fields):
        await asyncio.Event().wait()

    recorded.side_effect = stuck
    started = time.monotonic()

    response = TestClient(_app()).get(
        f"/api/v1/workspaces/{WORKSPACE}", headers=_computer({CALL_HEADER: CALL_ID})
    )

    assert response.json() == {"workspace_id": WORKSPACE}
    assert time.monotonic() - started < 10


def test_a_route_that_raises_is_recorded_as_a_500(recorded):
    app = _app()

    @app.get("/api/v1/boom")
    async def boom():
        raise RuntimeError("boom")

    TestClient(app, raise_server_exceptions=False).get("/api/v1/boom", headers=_computer())

    assert recorded.await_args.kwargs["status"] == 500
