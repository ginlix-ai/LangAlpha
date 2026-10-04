"""
Tests for the Automations API router (src/server/app/automations.py).

Covers CRUD, control actions (trigger/pause/resume), execution history, and
delivery through the messaging service.
"""

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
# A one-time run's time, a month out.
LATER = NOW + timedelta(days=30)
AUTO_ID = str(uuid.uuid4())
EXEC_ID = str(uuid.uuid4())


def _automation(automation_id=None, user_id="test-user-123", **overrides):
    data = {
        "automation_id": automation_id or AUTO_ID,
        "user_id": user_id,
        "name": "Daily Briefing",
        "description": "Morning market summary",
        "trigger_type": "cron",
        "cron_expression": "0 8 * * *",
        "timezone": "UTC",
        "trigger_config": None,
        "next_run_at": NOW,
        "last_run_at": None,
        "agent_mode": "flash",
        "instruction": "Give me a market summary",
        "workspace_id": None,
        "llm_model": None,
        "additional_context": None,
        "thread_strategy": "new",
        "conversation_thread_id": None,
        "status": "active",
        "max_failures": 3,
        "failure_count": 0,
        "delivery_config": None,
        "metadata": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    data.update(overrides)
    return data


def _execution(automation_id=None, **overrides):
    data = {
        "automation_execution_id": EXEC_ID,
        "automation_id": automation_id or AUTO_ID,
        "status": "completed",
        "conversation_thread_id": None,
        "scheduled_at": NOW,
        "started_at": NOW,
        "completed_at": NOW,
        "error_message": None,
        "server_id": None,
        "created_at": NOW,
    }
    data.update(overrides)
    return data


@pytest_asyncio.fixture
async def client():
    from src.server.app.automations import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


HANDLER = "src.server.app.automations.handler"
AUTO_DB = "src.server.app.automations.auto_db"
HANDLER_DB = "src.server.services.automations.lifecycle.auto_db"
EXEC_DB = "src.server.app.automations.exec_db"
HANDLER_EXEC_DB = "src.server.services.automations.lifecycle.exec_db"


# ---------------------------------------------------------------------------
# POST /api/v1/automations — create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_automation(client):
    auto = _automation()
    with patch(
        f"{HANDLER}.create_automation",
        new_callable=AsyncMock,
        return_value=auto,
    ):
        resp = await client.post(
            "/api/v1/automations",
            json={
                "name": "Daily Briefing",
                "trigger_type": "cron",
                "cron_expression": "0 8 * * *",
                "instruction": "Give me a market summary",
            },
        )

    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == "Daily Briefing"
    assert body["trigger_type"] == "cron"


@pytest.mark.asyncio
async def test_create_automation_duplicate_409(client):
    with patch(
        f"{HANDLER}.create_automation",
        new_callable=AsyncMock,
        side_effect=ValueError("duplicate name"),
    ):
        resp = await client.post(
            "/api/v1/automations",
            json={
                "name": "Dup",
                "trigger_type": "cron",
                "cron_expression": "0 8 * * *",
                "instruction": "test",
            },
        )

    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_create_automation_validation_error(client):
    """Missing required fields should return 422."""
    resp = await client.post(
        "/api/v1/automations",
        json={"name": "No trigger"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_refuses_a_schedule_field_of_another_kind(client):
    with patch(f"{HANDLER}.create_automation", new_callable=AsyncMock) as create:
        resp = await client.post(
            "/api/v1/automations",
            json={
                "name": "Daily Briefing",
                "trigger_type": "cron",
                "cron_expression": "0 8 * * *",
                "next_run_at": LATER.isoformat(),
                "instruction": "Give me a market summary",
            },
        )

    assert resp.status_code == 422
    create.assert_not_awaited()


# ---------------------------------------------------------------------------
# GET /api/v1/automations — list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_automations(client):
    auto = _automation()
    with patch(
        f"{AUTO_DB}.list_automations",
        new_callable=AsyncMock,
        return_value=([auto], 1),
    ):
        resp = await client.get("/api/v1/automations")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert len(body["automations"]) == 1


@pytest.mark.asyncio
async def test_list_automations_with_filters(client):
    with patch(
        f"{AUTO_DB}.list_automations",
        new_callable=AsyncMock,
        return_value=([], 0),
    ) as mock_list:
        resp = await client.get(
            "/api/v1/automations?status=active&limit=10&offset=5"
        )

    assert resp.status_code == 200
    mock_list.assert_awaited_once_with(
        "test-user-123", status="active", limit=10, offset=5
    )


# ---------------------------------------------------------------------------
# GET /api/v1/automations/{automation_id} — get
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_automation(client):
    auto = _automation()
    with patch(
        f"{AUTO_DB}.get_automation",
        new_callable=AsyncMock,
        return_value=auto,
    ):
        resp = await client.get(f"/api/v1/automations/{AUTO_ID}")

    assert resp.status_code == 200
    assert resp.json()["automation_id"] == AUTO_ID


@pytest.mark.asyncio
async def test_get_automation_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{AUTO_DB}.get_automation",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.get(f"/api/v1/automations/{fake_id}")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# PATCH /api/v1/automations/{automation_id} — update
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_automation(client):
    auto = _automation(name="Updated")
    with patch(
        f"{HANDLER}.update_automation",
        new_callable=AsyncMock,
        return_value=auto,
    ):
        resp = await client.patch(
            f"/api/v1/automations/{AUTO_ID}",
            json={"name": "Updated"},
        )

    assert resp.status_code == 200
    assert resp.json()["name"] == "Updated"


@pytest.mark.asyncio
async def test_update_automation_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{HANDLER}.update_automation",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.patch(
            f"/api/v1/automations/{fake_id}",
            json={"name": "X"},
        )

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_automation_conflict_409(client):
    with patch(
        f"{HANDLER}.update_automation",
        new_callable=AsyncMock,
        side_effect=ValueError("conflict"),
    ):
        resp = await client.patch(
            f"/api/v1/automations/{AUTO_ID}",
            json={"name": "X"},
        )

    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_update_refuses_a_schedule_field_of_another_kind(client):
    """The body parses, and only the stored row says a cron takes no
    next_run_at; the refusal answers in the 422 shape of a bad body."""
    with (
        patch(f"{HANDLER_DB}.get_automation", new_callable=AsyncMock, return_value=_automation()),
        patch(f"{HANDLER_DB}.update_automation", new_callable=AsyncMock) as update,
    ):
        resp = await client.patch(
            f"/api/v1/automations/{AUTO_ID}",
            json={"next_run_at": LATER.isoformat()},
        )

    assert resp.status_code == 422
    [error] = resp.json()["detail"]
    assert "next_run_at doesn't apply to a 'cron' automation" in error["msg"]
    update.assert_not_awaited()


# ---------------------------------------------------------------------------
# The zone every surface refuses, and the times and zones only the agent's do
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_refuses_an_unknown_zone_as_a_bad_body(client):
    body = {
        "name": "Reminder",
        "trigger_type": "once",
        "next_run_at": LATER.isoformat(),
        "instruction": "Remind me",
        "timezone": "Mars/Olympus",
    }
    with patch(f"{HANDLER}.create_automation", new_callable=AsyncMock) as create:
        resp = await client.post("/api/v1/automations", json=body)

    assert resp.status_code == 422
    assert any("unknown IANA timezone 'Mars/Olympus'" in e["msg"] for e in resp.json()["detail"])
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_refuses_an_unknown_zone_as_a_bad_body(client):
    with patch(f"{HANDLER}.update_automation", new_callable=AsyncMock) as update:
        resp = await client.patch(f"/api/v1/automations/{AUTO_ID}", json={"timezone": "Mars/Olympus"})

    assert resp.status_code == 422
    assert any("unknown IANA timezone 'Mars/Olympus'" in e["msg"] for e in resp.json()["detail"])
    update.assert_not_awaited()


_USERS_OWN = [
    ({"next_run_at": "2030-01-01"}, "next_run_at", datetime(2030, 1, 1, tzinfo=timezone.utc)),
    ({"next_run_at": "2020-01-01T09:00:00Z"}, "next_run_at", datetime(2020, 1, 1, 9, tzinfo=timezone.utc)),
    ({"timezone": "EST"}, "timezone", "EST"),
]
_USERS_OWN_IDS = ["date-only", "past", "fixed-offset"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("fields", "field", "stored"), _USERS_OWN, ids=_USERS_OWN_IDS)
async def test_create_takes_the_users_own_time_and_zone(client, fields, field, stored):
    """The page resends what it stored and reads times on the device's
    clock; the date, past-time and fixed-offset guards are for the agent."""
    body = {
        "name": "Reminder",
        "trigger_type": "once",
        "next_run_at": LATER.isoformat(),
        "instruction": "Remind me",
        **fields,
    }
    with patch(
        f"{HANDLER}.create_automation", new_callable=AsyncMock, return_value=_automation()
    ) as create:
        resp = await client.post("/api/v1/automations", json=body)

    assert resp.status_code == 201
    assert getattr(create.await_args.kwargs["data"], field) == stored


@pytest.mark.asyncio
@pytest.mark.parametrize(("fields", "field", "stored"), _USERS_OWN, ids=_USERS_OWN_IDS)
async def test_update_takes_the_users_own_time_and_zone(client, fields, field, stored):
    with patch(
        f"{HANDLER}.update_automation", new_callable=AsyncMock, return_value=_automation()
    ) as update:
        resp = await client.patch(f"/api/v1/automations/{AUTO_ID}", json=fields)

    assert resp.status_code == 200
    assert update.await_args.kwargs["fields"][field] == stored


@pytest.fixture
def unknown_models():
    """The user can run no model by the name asked for."""
    lifecycle = "src.server.services.automations.lifecycle"
    with (
        patch(f"{lifecycle}.user_models.get_model_preference", new_callable=AsyncMock, return_value={}),
        patch(
            f"{lifecycle}.user_models.classify_model",
            new_callable=AsyncMock,
            return_value=("unknown", None),
        ),
        patch(
            f"{lifecycle}.user_models.get_custom_provider_config",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(f"{lifecycle}.get_configured_llm_models", return_value={"vendor": ["model-a"]}),
    ):
        yield


@pytest.mark.asyncio
async def test_create_with_an_unknown_model_is_409(client, unknown_models):
    with patch(HANDLER_DB) as db:
        db.create_automation = AsyncMock()
        resp = await client.post(
            "/api/v1/automations",
            json={
                "name": "Daily Briefing",
                "trigger_type": "cron",
                "cron_expression": "0 8 * * *",
                "instruction": "Give me a market summary",
                "llm_model": "no-such-model",
            },
        )

    assert resp.status_code == 409
    assert "unknown model 'no-such-model'" in resp.json()["detail"]
    assert "model-a" in resp.json()["detail"]
    db.create_automation.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_to_an_unknown_model_is_409(client, unknown_models):
    with patch(HANDLER_DB) as db:
        db.get_automation = AsyncMock(return_value=_automation())
        db.update_automation = AsyncMock()
        resp = await client.patch(
            f"/api/v1/automations/{AUTO_ID}", json={"llm_model": "no-such-model"}
        )

    assert resp.status_code == 409
    assert "unknown model 'no-such-model'" in resp.json()["detail"]
    db.update_automation.assert_not_awaited()


# ---------------------------------------------------------------------------
# DELETE /api/v1/automations/{automation_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_automation(client):
    with patch(
        f"{AUTO_DB}.delete_automation",
        new_callable=AsyncMock,
        return_value=True,
    ):
        resp = await client.delete(f"/api/v1/automations/{AUTO_ID}")

    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_delete_automation_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{AUTO_DB}.delete_automation",
        new_callable=AsyncMock,
        return_value=False,
    ):
        resp = await client.delete(f"/api/v1/automations/{fake_id}")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/v1/automations/{automation_id}/trigger
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_automation(client):
    result = {"status": "triggered", "execution_id": EXEC_ID}
    with patch(
        f"{HANDLER}.trigger_automation",
        new_callable=AsyncMock,
        return_value=result,
    ):
        resp = await client.post(f"/api/v1/automations/{AUTO_ID}/trigger")

    assert resp.status_code == 200
    assert resp.json()["status"] == "triggered"


@pytest.mark.asyncio
async def test_trigger_automation_conflict(client):
    with patch(
        f"{HANDLER}.trigger_automation",
        new_callable=AsyncMock,
        side_effect=ValueError("already running"),
    ):
        resp = await client.post(f"/api/v1/automations/{AUTO_ID}/trigger")

    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# POST /api/v1/automations/{automation_id}/pause
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pause_automation(client):
    auto = _automation(status="paused")
    with patch(
        f"{HANDLER}.pause_automation",
        new_callable=AsyncMock,
        return_value=auto,
    ):
        resp = await client.post(f"/api/v1/automations/{AUTO_ID}/pause")

    assert resp.status_code == 200
    assert resp.json()["status"] == "paused"


@pytest.mark.asyncio
async def test_pause_automation_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{HANDLER}.pause_automation",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.post(f"/api/v1/automations/{fake_id}/pause")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /api/v1/automations/{automation_id}/resume
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_automation(client):
    auto = _automation(status="active")
    with patch(
        f"{HANDLER}.resume_automation",
        new_callable=AsyncMock,
        return_value=auto,
    ):
        resp = await client.post(f"/api/v1/automations/{AUTO_ID}/resume")

    assert resp.status_code == 200
    assert resp.json()["status"] == "active"


@pytest.mark.asyncio
async def test_resume_automation_not_found(client):
    fake_id = str(uuid.uuid4())
    with patch(
        f"{HANDLER}.resume_automation",
        new_callable=AsyncMock,
        return_value=None,
    ):
        resp = await client.post(f"/api/v1/automations/{fake_id}/resume")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/automations/{automation_id}/executions
# ---------------------------------------------------------------------------


def _run(**overrides):
    """An execution as the run query returns it, its automation's identity
    included."""
    return _execution(
        automation_name="Daily Briefing", agent_mode="flash", trigger_type="cron",
        **overrides,
    )


@pytest.mark.asyncio
async def test_list_executions(client):
    with patch(
        f"{EXEC_DB}.list_executions",
        new_callable=AsyncMock,
        return_value=([_run()], True),
    ):
        resp = await client.get(
            f"/api/v1/automations/{AUTO_ID}/executions"
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["has_more"] is True
    assert "total" not in body
    [run] = body["executions"]
    assert run["automation_name"] == "Daily Briefing"


@pytest.mark.asyncio
async def test_list_executions_with_pagination(client):
    with patch(
        f"{EXEC_DB}.list_executions",
        new_callable=AsyncMock,
        return_value=([], False),
    ) as mock_list:
        resp = await client.get(
            f"/api/v1/automations/{AUTO_ID}/executions?limit=5&offset=10"
        )

    assert resp.status_code == 200
    mock_list.assert_awaited_once_with(
        "test-user-123", automation_id=AUTO_ID, limit=5, offset=10
    )


# ---------------------------------------------------------------------------
# GET /api/v1/automations/executions — the run feed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_feed_is_not_captured_by_the_automation_route(client):
    """Declared before /automations/{automation_id}; moved after it, the feed
    would be read as an automation named "executions"."""
    with (
        patch(
            f"{EXEC_DB}.list_executions",
            new_callable=AsyncMock,
            return_value=([_run()], False),
        ) as mock_list,
        patch(f"{AUTO_DB}.get_automation", new_callable=AsyncMock) as mock_get,
    ):
        resp = await client.get("/api/v1/automations/executions")

    assert resp.status_code == 200
    assert resp.json()["executions"][0]["automation_name"] == "Daily Briefing"
    assert resp.json()["has_more"] is False
    mock_list.assert_awaited_once_with(
        "test-user-123", thread_id=None, status=None, limit=20, offset=0
    )
    mock_get.assert_not_awaited()


# ---------------------------------------------------------------------------
# POST /api/v1/automations/{automation_id}/executions/{execution_id}/skip
# ---------------------------------------------------------------------------

SETTLE = "src.server.services.automations.lifecycle.settle"
SKIP_URL = f"/api/v1/automations/{AUTO_ID}/executions/{EXEC_ID}/skip"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_now", "expected"),
    [("completed", 409), (None, 404)],
    ids=["no-longer-waiting", "nonexistent"],
)
async def test_skip_that_does_not_land(client, status_now, expected):
    with (
        patch(HANDLER_DB) as db,
        patch(HANDLER_EXEC_DB) as execs,
        patch(SETTLE, new_callable=AsyncMock, return_value=False),
    ):
        db.get_automation = AsyncMock(return_value=_automation())
        execs.get_execution_status = AsyncMock(return_value=status_now)
        resp = await client.post(SKIP_URL)

    assert resp.status_code == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/automations/not-a-uuid"),
        ("PATCH", "/api/v1/automations/not-a-uuid"),
        ("DELETE", "/api/v1/automations/not-a-uuid"),
        ("POST", "/api/v1/automations/not-a-uuid/trigger"),
        ("POST", "/api/v1/automations/not-a-uuid/pause"),
        ("POST", "/api/v1/automations/not-a-uuid/resume"),
        ("GET", "/api/v1/automations/not-a-uuid/executions"),
        ("POST", f"/api/v1/automations/not-a-uuid/executions/{EXEC_ID}/skip"),
        ("POST", f"/api/v1/automations/not-a-uuid/executions/{EXEC_ID}/dismiss"),
    ],
)
async def test_a_malformed_automation_id_is_422(client, method, path):
    """Refused at the path, before it reaches a uuid column as a 500."""
    with patch(AUTO_DB) as db, patch(HANDLER) as handler:
        resp = await client.request(method, path, json={"name": "X"})

    assert resp.status_code == 422
    assert not db.mock_calls and not handler.mock_calls


@pytest.mark.asyncio
async def test_skip_with_a_malformed_id_is_422(client):
    with patch(f"{HANDLER}.skip_execution", new_callable=AsyncMock) as mock_skip:
        resp = await client.post(
            f"/api/v1/automations/{AUTO_ID}/executions/not-a-uuid/skip"
        )

    assert resp.status_code == 422
    mock_skip.assert_not_awaited()


# ---------------------------------------------------------------------------
# POST /api/v1/automations/{automation_id}/executions/{execution_id}/dismiss
# ---------------------------------------------------------------------------

DISMISS_URL = f"/api/v1/automations/{AUTO_ID}/executions/{EXEC_ID}/dismiss"


@pytest.mark.asyncio
async def test_dismiss_answers_with_the_automation(client):
    dismissed = _execution(status="failed", dismissed_at=NOW)
    with patch(HANDLER_DB) as db, patch(HANDLER_EXEC_DB) as execs:
        db.get_automation = AsyncMock(
            return_value=_automation(status="disabled", last_execution=dismissed)
        )
        execs.dismiss_execution = AsyncMock(return_value=True)
        resp = await client.post(DISMISS_URL)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "disabled"
    assert body["last_execution"]["dismissed_at"] is not None
    execs.dismiss_execution.assert_awaited_once_with(EXEC_ID, automation_id=AUTO_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_now", "expected"),
    [("completed", 409), (None, 404)],
    ids=["did-not-fail", "nonexistent"],
)
async def test_dismiss_that_does_not_land(client, status_now, expected):
    with patch(HANDLER_DB) as db, patch(HANDLER_EXEC_DB) as execs:
        db.get_automation = AsyncMock(return_value=_automation())
        execs.dismiss_execution = AsyncMock(return_value=False)
        execs.get_execution_status = AsyncMock(return_value=status_now)
        resp = await client.post(DISMISS_URL)

    assert resp.status_code == expected


@pytest.mark.asyncio
async def test_dismiss_on_another_users_automation_writes_nothing(client):
    with patch(HANDLER_DB) as db, patch(HANDLER_EXEC_DB) as execs:
        db.get_automation = AsyncMock(return_value=None)
        execs.dismiss_execution = AsyncMock()
        resp = await client.post(DISMISS_URL)

    assert resp.status_code == 404
    execs.dismiss_execution.assert_not_awaited()


# ---------------------------------------------------------------------------
# Delivery through the messaging service
# ---------------------------------------------------------------------------

SERVICE = "http://messaging.test/api/prefix"
WS_ID = str(uuid.uuid4())
OPTIONS_URL = f"/api/v1/automations/delivery-options?workspace_id={WS_ID}"
DEFAULT_URL = "/api/v1/automations/delivery-default"
APPS = {
    "slack": {
        "chats": [{"address": "slack:T1/C1", "name": "#demo", "kind": "channel"}],
        "default": {"address": "slack:T1/C1", "name": "#demo", "via": "workspace"},
        "error": None,
    },
    "telegram": {"chats": [], "default": None, "error": "Telegram isn't linked"},
}


class _MessagingService:
    """Answers ``reply`` (a response, or an exception to raise) and keeps
    every request."""

    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.reply: httpx.Response | Exception = httpx.Response(200, json={})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


@pytest.fixture
def no_messaging(monkeypatch):
    from src.config import env

    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
    monkeypatch.delenv("INTERNAL_SERVICE_TOKEN", raising=False)


@pytest.fixture
def messaging_service(monkeypatch):
    from src.config import env
    from src.tools.messaging import tools as messaging

    fake = _MessagingService()
    transport = httpx.MockTransport(fake.handle)
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", SERVICE)
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "svc-token")
    monkeypatch.setattr(
        messaging,
        "_client",
        lambda timeout: httpx.AsyncClient(transport=transport, timeout=timeout),
    )
    return fake


@pytest.fixture
def workspace():
    """The workspace the request names; set ``.return_value`` to change its
    owner, or to None for one that doesn't exist."""
    with patch(
        "src.server.app.automations.get_workspace",
        new=AsyncMock(return_value={"workspace_id": WS_ID, "user_id": "test-user-123"}),
    ) as found:
        yield found


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace", "no_messaging")
async def test_delivery_options_without_a_messaging_service(client):
    resp = await client.get(OPTIONS_URL)

    assert resp.status_code == 200
    assert resp.json() == {"enabled": False, "apps": {}}


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace")
async def test_delivery_options_lists_each_apps_chats(client, messaging_service):
    messaging_service.reply = httpx.Response(200, json={"apps": APPS})

    resp = await client.get(OPTIONS_URL)

    assert resp.status_code == 200
    assert resp.json() == {"enabled": True, "apps": APPS}
    (request,) = messaging_service.requests
    assert request.method == "GET"
    assert request.url.path == "/api/prefix/agent/automation-targets"
    assert dict(request.url.params) == {"workspace_id": WS_ID}
    assert request.headers["X-User-Id"] == "test-user-123"
    assert request.headers["X-Service-Token"] == "svc-token"


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace")
@pytest.mark.parametrize(
    "reply, detail",
    [
        (httpx.ConnectError("refused"), "The messaging service could not be reached."),
        (
            httpx.Response(503, json={"code": "unavailable", "message": "Slack is down."}),
            "Slack is down.",
        ),
        (httpx.Response(500, text="boom"), "The messaging service failed (500)."),
        (
            httpx.Response(200, json={"apps": []}),
            "The messaging service sent an answer that could not be read.",
        ),
    ],
)
async def test_delivery_options_unavailable(client, messaging_service, reply, detail):
    messaging_service.reply = reply

    resp = await client.get(OPTIONS_URL)

    assert resp.status_code == 503
    assert resp.json() == {"detail": detail}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "found, status",
    [(None, 404), ({"workspace_id": WS_ID, "user_id": "someone-else"}, 403)],
    ids=["missing", "not_theirs"],
)
async def test_delivery_options_only_for_the_users_workspace(
    client, messaging_service, workspace, found, status
):
    workspace.return_value = found

    resp = await client.get(OPTIONS_URL)

    assert resp.status_code == status
    assert messaging_service.requests == []
    workspace.assert_awaited_once_with(WS_ID)


@pytest.mark.asyncio
async def test_delivery_options_needs_a_workspace(client):
    resp = await client.get("/api/v1/automations/delivery-options")

    assert resp.status_code == 422


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace")
@pytest.mark.parametrize("address", ["slack:T1/C1", None], ids=["set", "clear"])
async def test_delivery_default_saves_the_chat(client, messaging_service, address):
    messaging_service.reply = httpx.Response(
        200, json={"address": address, "name": "#demo" if address else None}
    )

    resp = await client.put(
        DEFAULT_URL, json={"workspace_id": WS_ID, "platform": "slack", "address": address}
    )

    assert resp.status_code == 200
    assert resp.json() == {"address": address, "name": "#demo" if address else None}
    (request,) = messaging_service.requests
    assert request.method == "PUT"
    assert request.url.path == "/api/prefix/agent/automation-output"
    assert request.headers["X-User-Id"] == "test-user-123"
    assert json.loads(request.content) == {
        "workspace_id": WS_ID,
        "platform": "slack",
        "address": address,
    }


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace")
async def test_delivery_default_refused_says_why(client, messaging_service):
    problems = [{"field": "address", "message": "The bot is not in #ops."}]
    messaging_service.reply = httpx.Response(
        400, json={"code": "invalid", "message": "Not saved.", "problems": problems}
    )

    resp = await client.put(
        DEFAULT_URL, json={"workspace_id": WS_ID, "platform": "slack", "address": "slack:T1/C9"}
    )

    assert resp.status_code == 400
    assert resp.json() == {"detail": "Not saved.", "problems": problems}


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace")
@pytest.mark.parametrize(
    "reply, status, detail",
    [
        (
            httpx.Response(409, json={"code": "busy", "message": "Saving already."}),
            409,
            "Saving already.",
        ),
        (
            httpx.Response(503, json={"code": "unavailable", "message": "Try later."}),
            503,
            "Try later.",
        ),
        (httpx.ConnectError("refused"), 503, "The messaging service could not be reached."),
        (httpx.Response(500, text="boom"), 503, "The messaging service failed (500)."),
        (
            httpx.Response(200, text="not json"),
            503,
            "The messaging service sent an answer that could not be read.",
        ),
    ],
)
async def test_delivery_default_not_saved(client, messaging_service, reply, status, detail):
    messaging_service.reply = reply

    resp = await client.put(
        DEFAULT_URL, json={"workspace_id": WS_ID, "platform": "slack", "address": "slack:T1/C1"}
    )

    assert resp.status_code == status
    assert resp.json() == {"detail": detail}


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace", "no_messaging")
async def test_delivery_default_without_a_messaging_service(client):
    resp = await client.put(
        DEFAULT_URL, json={"workspace_id": WS_ID, "platform": "slack", "address": None}
    )

    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "found, status",
    [(None, 404), ({"workspace_id": WS_ID, "user_id": "someone-else"}, 403)],
    ids=["missing", "not_theirs"],
)
async def test_delivery_default_only_for_the_users_workspace(
    client, messaging_service, workspace, found, status
):
    workspace.return_value = found

    resp = await client.put(
        DEFAULT_URL, json={"workspace_id": WS_ID, "platform": "slack", "address": "slack:T1/C1"}
    )

    assert resp.status_code == status
    assert messaging_service.requests == []


_REFUSALS = [
    ("slack:T1/C9", "the bot is not in that channel"),
    ("telegram", "Telegram isn't linked to your account"),
]
_JOINED = (
    "'slack:T1/C9': the bot is not in that channel; "
    "'telegram': Telegram isn't linked to your account"
)
_PROBLEMS = [
    {"entry": "slack:T1/C9", "message": "the bot is not in that channel"},
    {"entry": "telegram", "message": "Telegram isn't linked to your account"},
]


@pytest.mark.asyncio
async def test_create_refused_for_its_delivery_names_each_entry(client):
    from src.server.services.automations.lifecycle import DeliveryRefused

    with patch(
        f"{HANDLER}.create_automation",
        new_callable=AsyncMock,
        side_effect=DeliveryRefused(_REFUSALS),
    ):
        resp = await client.post(
            "/api/v1/automations",
            json={
                "name": "Brief",
                "trigger_type": "cron",
                "cron_expression": "0 8 * * *",
                "instruction": "test",
                "delivery_config": {"methods": ["slack:T1/C9", "telegram"]},
            },
        )

    assert resp.status_code == 409
    assert resp.json() == {"detail": _JOINED, "problems": _PROBLEMS}


@pytest.mark.asyncio
async def test_update_refused_for_its_delivery_names_each_entry(client):
    from src.server.services.automations.lifecycle import DeliveryRefused

    with patch(
        f"{HANDLER}.update_automation",
        new_callable=AsyncMock,
        side_effect=DeliveryRefused(_REFUSALS),
    ):
        resp = await client.patch(
            f"/api/v1/automations/{AUTO_ID}",
            json={"delivery_config": {"methods": ["slack:T1/C9", "telegram"]}},
        )

    assert resp.status_code == 409
    assert resp.json() == {"detail": _JOINED, "problems": _PROBLEMS}


# The messaging service takes at most 20 entries when a run hands it the
# delivery, and checks a chat of at most 256 characters, so a save refuses more.
_TWENTY_ONE = [f"app{i}" for i in range(21)]
_PAST_THE_LIMIT = {
    "detail": "'app20': past the limit of 20 entries",
    "problems": [{"entry": "app20", "message": "past the limit of 20 entries"}],
}


@pytest.mark.asyncio
async def test_create_with_too_many_delivery_entries_is_refused(client):
    with patch(f"{HANDLER_DB}.create_automation", new_callable=AsyncMock) as create:
        resp = await client.post(
            "/api/v1/automations",
            json={
                "name": "Brief",
                "trigger_type": "cron",
                "cron_expression": "0 8 * * *",
                "instruction": "test",
                "delivery_config": {"methods": _TWENTY_ONE},
            },
        )

    assert resp.status_code == 409
    assert resp.json() == _PAST_THE_LIMIT
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_with_an_overlong_delivery_entry_is_refused(client):
    entry = "slack:" + "C" * 300
    with (
        patch(
            f"{HANDLER_DB}.get_automation",
            new_callable=AsyncMock,
            return_value=_automation(delivery_config={"methods": ["slack"]}),
        ),
        patch(f"{HANDLER_DB}.update_automation", new_callable=AsyncMock) as update,
    ):
        resp = await client.patch(
            f"/api/v1/automations/{AUTO_ID}",
            json={"delivery_config": {"methods": ["slack", entry]}},
        )

    assert resp.status_code == 409
    assert resp.json()["problems"] == [
        {"entry": entry, "message": "longer than 256 characters"}
    ]
    update.assert_not_awaited()
