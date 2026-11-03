"""The push-device routes: register, list with ``available``, switch kinds,
remove — always the caller's own devices, never their tokens."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

MOD = "src.server.app.push_devices"
TOKEN = "AB" * 32
NOW = datetime(2026, 11, 3, tzinfo=timezone.utc)


def _row(**kw):
    row = {
        "push_device_id": uuid4(),
        "user_id": "test-user-123",
        "token": TOKEN.lower(),
        "platform": "ios",
        "bundle_id": "com.example.app.dev",
        "environment": "sandbox",
        "device_name": "iPhone",
        "app_version": "1.2.0",
        "preferences": {},
        "created_at": NOW,
        "updated_at": NOW,
        "last_sent_at": None,
    }
    row.update(kw)
    return row


@pytest_asyncio.fixture
async def client():
    from src.server.app.push_devices import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
def db(monkeypatch):
    from src.server.app import push_devices
    from src.config import env

    # The bodies register the development app; a local .env may list others.
    monkeypatch.setattr(env, "APNS_BUNDLE_IDS", ("com.example.app.dev",))

    fakes = {
        name: AsyncMock()
        for name in (
            "list_push_devices",
            "upsert_push_device",
            "update_push_device_preferences",
            "get_push_device",
            "delete_push_device",
        )
    }
    for name, fake in fakes.items():
        monkeypatch.setattr(push_devices.push_db, name, fake)
    return fakes


def _register_body(**kw):
    body = {
        "token": TOKEN,
        "platform": "ios",
        "bundle_id": "com.example.app.dev",
        "environment": "sandbox",
        "device_name": "iPhone",
        "app_version": "1.2.0",
    }
    body.update(kw)
    return body


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_the_list_says_whether_this_server_sends(client, db, monkeypatch, enabled):
    from src.config import env

    monkeypatch.setattr(env, "APNS_ENABLED", enabled)
    row = _row(preferences={"turn": False})
    db["list_push_devices"].return_value = [row]
    response = await client.get("/api/v1/users/me/push-devices")
    assert response.status_code == 200
    body = response.json()
    assert body["available"] is enabled
    (device,) = body["devices"]
    assert "token" not in device
    assert device["push_device_id"] == str(row["push_device_id"])
    assert device["preferences"] == {
        "turn": False,
        "needs_input": True,
        "wakeup": True,
        "automation": True,
    }
    assert set(device) == {
        "push_device_id",
        "platform",
        "bundle_id",
        "environment",
        "device_name",
        "app_version",
        "preferences",
        "created_at",
        "updated_at",
        "last_sent_at",
    }
    db["list_push_devices"].assert_awaited_once_with("test-user-123")


@pytest.mark.asyncio
async def test_register_stores_the_token_lowercased(client, db):
    db["upsert_push_device"].return_value = _row()
    response = await client.post("/api/v1/users/me/push-devices", json=_register_body())
    assert response.status_code == 200
    assert "token" not in response.json()
    db["upsert_push_device"].assert_awaited_once_with(
        user_id="test-user-123",
        token=TOKEN.lower(),
        platform="ios",
        bundle_id="com.example.app.dev",
        environment="sandbox",
        device_name="iPhone",
        app_version="1.2.0",
    )


@pytest.mark.asyncio
async def test_register_refuses_a_bundle_id_this_server_does_not_push_to(client, db):
    response = await client.post(
        "/api/v1/users/me/push-devices", json=_register_body(bundle_id="com.other.app")
    )
    assert response.status_code == 400
    db["upsert_push_device"].assert_not_awaited()


@pytest.mark.asyncio
async def test_register_takes_any_bundle_id_while_none_is_configured(
    client, db, monkeypatch
):
    from src.config import env

    monkeypatch.setattr(env, "APNS_BUNDLE_IDS", ())
    db["upsert_push_device"].return_value = _row()
    response = await client.post(
        "/api/v1/users/me/push-devices", json=_register_body(bundle_id="com.other.app")
    )
    assert response.status_code == 200
    db["upsert_push_device"].assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"token": "not-hex-" * 4},
        {"token": "ab"},
        {"platform": "android"},
        {"environment": "staging"},
    ],
)
async def test_register_refuses_a_malformed_device(client, db, change):
    response = await client.post(
        "/api/v1/users/me/push-devices", json=_register_body(**change)
    )
    assert response.status_code == 422
    db["upsert_push_device"].assert_not_awaited()


@pytest.mark.asyncio
async def test_patch_merges_only_the_kinds_sent(client, db):
    device_id = uuid4()
    db["update_push_device_preferences"].return_value = _row(
        push_device_id=device_id, preferences={"wakeup": False}
    )
    response = await client.patch(
        f"/api/v1/users/me/push-devices/{device_id}",
        json={"preferences": {"wakeup": False, "future_kind": False}},
    )
    assert response.status_code == 200
    assert response.json()["preferences"]["wakeup"] is False
    db["update_push_device_preferences"].assert_awaited_once_with(
        "test-user-123", str(device_id), {"wakeup": False}
    )


@pytest.mark.asyncio
async def test_patch_of_another_users_device_is_not_found(client, db):
    db["update_push_device_preferences"].return_value = None
    response = await client.patch(
        f"/api/v1/users/me/push-devices/{uuid4()}",
        json={"preferences": {"turn": True}},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_removes_the_callers_device(client, db):
    device_id = uuid4()
    db["delete_push_device"].return_value = True
    response = await client.delete(f"/api/v1/users/me/push-devices/{device_id}")
    assert response.status_code == 204
    db["delete_push_device"].assert_awaited_once_with("test-user-123", str(device_id))


@pytest.mark.asyncio
async def test_delete_of_another_users_device_is_not_found(client, db):
    db["delete_push_device"].return_value = False
    response = await client.delete(f"/api/v1/users/me/push-devices/{uuid4()}")
    assert response.status_code == 404
