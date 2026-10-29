"""A resend of a send that went into a running turn as a steer.

No run row holds its key, so the route's duplicate probe finds the steer
job instead and answers with the turn that took the message, the same
answer a resend of a send that ran as its own turn gets.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from tests.unit.server.app.test_threads_authz import (
    CALLER,
    OWNER,
    _app_for,
    _empty_async_gen,
    _stub_downstream,
)

KEY = "8c1d2e3f-4a5b-4c6d-8e7f-90a1b2c3d4e5"
TURN = "2e4f6a8b-0c1d-4e2f-8a3b-5c6d7e8f9a0b"


async def _resend(thread_owner, steered=True):
    turn = {
        "conversation_response_id": uuid.UUID(TURN),
        "conversation_thread_id": "tid-mine",
        "status": "in_progress",
    }
    job = {
        "id": "job-1",
        "status": "pending",
        "run_id": TURN,
        "payload": {"target": "thread", "request_key": KEY, "steered_run_id": TURN},
    }
    workflow = MagicMock(return_value=_empty_async_gen())
    with (
        _stub_downstream(owner_id=CALLER),
        patch("src.server.handlers.chat.astream_ptc_workflow", workflow),
        patch(
            "src.server.database.runs.lifecycle.find_run_by_request_key",
            AsyncMock(return_value=None if steered else turn),
        ),
        patch(
            "src.server.database.runs.outbox.get_steer_job",
            AsyncMock(return_value=job if steered else None),
            create=True,
        ),
        patch(
            "src.server.database.runs.lifecycle.get_run",
            AsyncMock(return_value=turn),
        ),
        patch(
            "src.server.app.threads.messaging.get_thread_owner_id",
            AsyncMock(return_value=thread_owner),
        ),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=_app_for(CALLER)), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/threads/tid-mine/messages",
                json={
                    "workspace_id": "ws-placeholder",
                    "messages": [{"role": "user", "content": "hello"}],
                    "request_key": KEY,
                },
            )
    return resp, workflow


@pytest.mark.asyncio
async def test_a_resend_of_a_steered_send_names_the_turn_that_took_it():
    resp, workflow = await _resend(CALLER)

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert (detail["code"], detail["run_id"], detail["run_status"]) == (
        "duplicate_request",
        TURN,
        "in_progress",
    )
    assert detail["thread_id"] == "tid-mine"
    # The turn is another message's: the client must not retry or adopt it.
    assert detail["steered"] is True
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_a_resend_of_a_send_that_started_its_turn_is_not_marked_steered():
    resp, _ = await _resend(CALLER, steered=False)

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert (detail["code"], detail["run_id"]) == ("duplicate_request", TURN)
    assert "steered" not in detail


@pytest.mark.asyncio
async def test_another_users_copy_of_the_key_learns_no_turn():
    resp, workflow = await _resend(OWNER)

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == "duplicate_request"
    assert "run_id" not in detail and "thread_id" not in detail
    assert "steered" not in detail
    workflow.assert_not_called()
