"""The public replay is the owner's page assembly plus redaction on the way out."""

from __future__ import annotations

import json
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.history.replay import ReplayPage
from src.server.services.history.replay.lines import line_of
from tests.conftest import create_test_app
from tests.unit.server.services.history.replay_builders import replay_rows

pytestmark = pytest.mark.asyncio

_SHARE_TOKEN = "share_abc123"
_THREAD_ID = "44444444-4444-4444-8444-444444444444"

_THREAD_BY_TOKEN = "src.server.app.share_access.get_thread_by_share_token"
_REPLAY_DATA = "src.server.app.public.get_replay_thread_data"
_READ_PAGE = "src.server.services.history.replay.read_replay_page"
_TASK_DETAILS = "src.server.services.history.task_status.resolve_task_details"


@pytest_asyncio.fixture
async def client():
    from src.server.app.public import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def test_public_stream_is_the_shared_page_through_redaction(client):
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": "ws-1",
        "share_permissions": {},
    }
    row = {"conversation_thread_id": _THREAD_ID, "latest_checkpoint_id": None}
    queries = [{"turn_index": 0, "content": "hi", "metadata": {}}]
    responses = [{"turn_index": 0, "conversation_response_id": "r-1"}]
    items = [
        {
            "event": "user_message",
            "data": {
                "turn_index": 0,
                "run_id": "run-secret",
                "content": "hi",
                "metadata": {"workspace_id": "ws-secret", "keep": "yes"},
            },
        },
        {
            "event": "provenance",
            "data": {"tool_call_id": "c1", "args": {"acc_id": "1234567"}},
        },
        {"event": "interrupt", "data": {"interrupt_id": "int-1"}},
        {
            "event": "message_chunk",
            "data": {"content_type": "text", "content": "hello", "id": "m1"},
        },
    ]
    page = ReplayPage(
        lines=[line_of(item) for item in items], first_turn_index=0, has_more=False
    )
    read_page = AsyncMock(return_value=(page, "checkpoint"))
    rows = replay_rows(row, queries, responses)
    with (
        patch(_THREAD_BY_TOKEN, new=AsyncMock(return_value=thread)),
        patch(_REPLAY_DATA, new=AsyncMock(return_value=rows)),
        patch(_READ_PAGE, new=read_page),
        patch(_TASK_DETAILS, new=AsyncMock(return_value={})),
    ):
        resp = await client.get(f"/api/v1/public/shared/{_SHARE_TOKEN}/replay")
    assert resp.status_code == 200

    events = []
    for block in resp.text.split("\n\n"):
        kind = payload = None
        for line in block.splitlines():
            if line.startswith("event: "):
                kind = line[len("event: ") :]
            elif line.startswith("data: "):
                payload = json.loads(line[len("data: ") :])
        if kind:
            events.append((kind, payload))

    kinds = [k for k, _ in events]
    assert kinds == ["user_message", "message_chunk", "replay_done"]
    user = events[0][1]
    assert "run_id" not in user
    assert "workspace_id" not in (user.get("metadata") or {})
    assert events[1][1]["content"] == "hello"

    read_page.assert_awaited_once()
    assert read_page.await_args.args == (rows,)
    assert read_page.await_args.args[0] is rows


# Exception text as a provider or the database raises it: credential scrubbing
# leaves every part of it in place.
_FAULT = "connection to 10.0.3.7:5432 refused for org-7Qh2x in sandbox sbx-41c9"


def _events(body: str) -> list[tuple[str, dict]]:
    out = []
    for block in body.split("\n\n"):
        fields = dict(
            line.split(": ", 1) for line in block.splitlines() if ": " in line
        )
        if "event" in fields:
            out.append((fields["event"], json.loads(fields["data"])))
    return out


async def _shared(client, row: dict, responses: list[dict], read_page=None) -> str:
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": "ws-1",
        "share_permissions": {},
    }
    queries = [{"turn_index": 0, "content": "hi", "metadata": {}}]
    patches = [
        patch(_THREAD_BY_TOKEN, new=AsyncMock(return_value=thread)),
        patch(
            _REPLAY_DATA,
            new=AsyncMock(return_value=replay_rows(row, queries, responses)),
        ),
        patch(_TASK_DETAILS, new=AsyncMock(return_value={})),
    ]
    if read_page is not None:
        patches.append(patch(_READ_PAGE, new=read_page))
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        resp = await client.get(f"/api/v1/public/shared/{_SHARE_TOKEN}/replay")
    assert resp.status_code == 200
    return resp.text


_FAILED_TURN = {
    "turn_index": 0,
    "conversation_response_id": "r-1",
    "status": "error",
    "errors": [_FAULT],
    "metadata": {"error_type": "connection", "error_class": "OperationalError"},
}


async def test_a_projected_failure_reaches_a_share_without_its_text(client):
    """The owner's assembly closes a failed turn with its error and settles a
    failed workflow run from its ledger row; a viewer gets the statuses."""
    from src.server.services.history import projector
    from src.server.services.history.slices import SpanDelta
    from src.server.services.history.replay import items as replay_items

    stamped = SpanDelta(
        new_ui_records=[
            {
                "name": projector.WORKFLOW_RUN_UI_NAME,
                "props": {
                    "frames": [
                        {
                            "agent": "task:wf-1",
                            "run_id": "wf-1",
                            "phase": "run_completed",
                            "status": "failed",
                            "error": _FAULT,
                        }
                    ]
                },
            },
            {
                "name": projector.MODEL_FALLBACK_UI_NAME,
                "props": {"from_model": "a", "to_model": "b", "error": _FAULT},
            },
        ]
    )
    page_items = [
        *projector.workflow_run_items(
            SpanDelta(), task_id="wf-2", ledger_status="failed", ledger_failure=_FAULT
        ),
        *projector.workflow_run_items(stamped, task_id="wf-1", ledger_status="failed"),
        *projector.model_fallback_items(_THREAD_ID, stamped),
        *replay_items.terminal_items(_THREAD_ID, _FAILED_TURN, []),
    ]
    assert sum(_FAULT in json.dumps(item) for item in page_items) == 4
    page = ReplayPage(
        lines=[line_of(item) for item in page_items],
        first_turn_index=0,
        has_more=False,
    )

    body = await _shared(
        client,
        {"conversation_thread_id": _THREAD_ID, "latest_checkpoint_id": "cp-1"},
        [_FAILED_TURN],
        read_page=AsyncMock(return_value=(page, "checkpoint")),
    )

    assert _FAULT not in body
    events = _events(body)
    assert [k for k, _ in events] == [
        "workflow_lifecycle",
        "workflow_lifecycle",
        "model_fallback",
        "replay_done",
    ]
    assert [d["status"] for k, d in events if k == "workflow_lifecycle"] == [
        "failed",
        "failed",
    ]


async def test_a_stored_failure_reaches_a_share_without_its_text(client):
    """The same for a turn replayed from its stored events."""
    response = {
        **_FAILED_TURN,
        "sse_events": [
            {
                "event": "workflow_lifecycle",
                "data": {
                    "agent": "task:wf-1",
                    "run_id": "wf-1",
                    "phase": "run_completed",
                    "status": "failed",
                    "error": _FAULT,
                },
            },
            {"event": "model_fallback", "data": {"to_model": "b", "error": _FAULT}},
            {"event": "error", "data": {"error": _FAULT}},
        ],
    }

    body = await _shared(
        client,
        {"conversation_thread_id": _THREAD_ID, "latest_checkpoint_id": None},
        [response],
    )

    assert _FAULT not in body
    kinds = [k for k, _ in _events(body)]
    assert "error" not in kinds
    assert "workflow_lifecycle" in kinds


async def test_a_shared_task_card_gets_its_status_and_never_the_failure(client):
    """A viewer's task card settles from current liveness like the owner's,
    but a failed task's reason is exception text and stays off."""
    card = {
        "event": "artifact",
        "data": {
            "artifact_type": "task",
            "payload": {"task_id": "tsk1", "status": "running", "workspace_id": "w"},
        },
    }
    page = ReplayPage(lines=[line_of(card)], first_turn_index=0, has_more=False)
    details = {"tsk1": {"status": "failed", "error": _FAULT, "error_type": "X"}}
    thread = {
        "conversation_thread_id": _THREAD_ID,
        "workspace_id": "ws-1",
        "share_permissions": {},
    }
    row = {"conversation_thread_id": _THREAD_ID, "latest_checkpoint_id": None}
    with (
        patch(_THREAD_BY_TOKEN, new=AsyncMock(return_value=thread)),
        patch(_REPLAY_DATA, new=AsyncMock(return_value=replay_rows(row, [], []))),
        patch(_READ_PAGE, new=AsyncMock(return_value=(page, "checkpoint"))),
        patch(_TASK_DETAILS, new=AsyncMock(return_value=details)),
    ):
        resp = await client.get(f"/api/v1/public/shared/{_SHARE_TOKEN}/replay")

    (kind, data), _done = _events(resp.text)
    assert kind == "artifact"
    assert data["payload"] == {"task_id": "tsk1", "status": "failed"}
    assert _FAULT not in resp.text
