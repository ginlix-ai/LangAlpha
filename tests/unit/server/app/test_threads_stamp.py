"""PUT /threads/{thread_id}/external-id stamp + PATCH /threads/{thread_id} rename.

The channel-identity stamp and the title rename are separate routes with separate
auth: the stamp accepts a privileged service caller (valid X-Service-Token, no
X-User-Id) that skips the ownership check; the rename requires a concrete user.

Covers the stamp path (happy, idempotent re-stamp, conflict → 409, missing/empty
field → 422 via the required model, not-owner → 403, vanished thread → 404, and
the service-token backfill) plus the rename path (renames, service-only → 401).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.database.conversation import ExternalIdConflictError
from src.server.utils.api import get_current_user_id, get_stamp_auth
from tests.conftest import create_test_app

THREADS_MOD = "src.server.app.threads.crud"
AUTH_MOD = "src.server.utils.api"

USER = "test-user-123"

# Sentinel so _stamp_app() can distinguish "use the default user-scoped caller"
# from an explicit ``None`` (the privileged service caller, resolved user_id None).
_UNSET = object()


def _stamp_app(auth=_UNSET):
    """Threads router with the stamp route's auth (``get_stamp_auth``) pinned.

    ``get_stamp_auth`` resolves to ``Optional[str]`` — the owner id, or ``None``
    for a privileged service caller. Defaults to a user-scoped caller (``USER``);
    pass ``None`` for a token-only service caller, or a user id for a service
    token acting as that user.
    """
    from src.server.app.threads import router

    app = create_test_app(router)
    resolved = USER if auth is _UNSET else auth
    app.dependency_overrides[get_stamp_auth] = lambda: resolved
    return app


def _title_app(user=USER):
    """Threads router with the rename route's auth (``get_current_user_id``) pinned."""
    from src.server.app.threads import router

    app = create_test_app(router)
    app.dependency_overrides[get_current_user_id] = lambda: user
    return app


def _real_app():
    """Threads router with NO auth override → the real dependencies run."""
    from src.server.app.threads import router

    return create_test_app(router)


def _thread_row(**overrides):
    now = datetime.now(timezone.utc)
    row = {
        "conversation_thread_id": "t-1",
        "workspace_id": "ws-1",
        "current_status": "completed",
        "msg_type": "ptc",
        "thread_index": 0,
        "title": "Test Thread",
        "platform": None,
        "external_id": None,
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


async def _put(app, body, headers=None):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        return await c.put("/api/v1/threads/t-1/external-id", json=body, headers=headers)


async def _patch(app, body, headers=None):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        return await c.patch("/api/v1/threads/t-1", json=body, headers=headers)


# ---------------------------------------------------------------------------
# Stamp path — PUT /threads/{id}/external-id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stamp_success():
    app = _stamp_app()
    row = _thread_row(platform="telegram", external_id="chat:42")
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()),
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=row),
        ) as m,
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 200
    assert resp.json()["platform"] == "telegram"
    m.assert_awaited_once_with("t-1", "telegram", "chat:42")


@pytest.mark.asyncio
async def test_stamp_idempotent_restamp():
    """Re-stamping the same values returns 200 (DB-level no-op-safe)."""
    app = _stamp_app()
    row = _thread_row(platform="telegram", external_id="chat:42")
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()),
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=row),
        ),
    ):
        first = await _put(app, {"platform": "telegram", "external_id": "chat:42"})
        second = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert first.status_code == 200
    assert second.status_code == 200


@pytest.mark.asyncio
async def test_stamp_conflict_returns_409():
    app = _stamp_app()
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()),
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(
                side_effect=ExternalIdConflictError(
                    platform="telegram", external_id="chat:42"
                )
            ),
        ),
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["error_type"] == "external_id_conflict"
    assert detail["platform"] == "telegram"
    assert detail["external_id"] == "chat:42"


@pytest.mark.asyncio
async def test_stamp_platform_only_is_422():
    """external_id is required — a lone field is rejected by the model (422)."""
    app = _stamp_app()
    with patch(
        f"{THREADS_MOD}.update_thread_external_id", new=AsyncMock()
    ) as m:
        resp = await _put(app, {"platform": "telegram"})

    assert resp.status_code == 422
    m.assert_not_awaited()


@pytest.mark.asyncio
async def test_stamp_external_id_only_is_422():
    app = _stamp_app()
    with patch(
        f"{THREADS_MOD}.update_thread_external_id", new=AsyncMock()
    ) as m:
        resp = await _put(app, {"external_id": "chat:42"})

    assert resp.status_code == 422
    m.assert_not_awaited()


@pytest.mark.asyncio
async def test_stamp_empty_fields_is_422():
    """Empty strings never clear external_id back to NULL — min_length rejects."""
    app = _stamp_app()
    with patch(
        f"{THREADS_MOD}.update_thread_external_id", new=AsyncMock()
    ) as m:
        resp = await _put(app, {"platform": "", "external_id": ""})

    assert resp.status_code == 422
    m.assert_not_awaited()


@pytest.mark.asyncio
async def test_stamp_not_owner_is_forbidden():
    app = _stamp_app()
    with (
        patch(
            f"{AUTH_MOD}.require_thread_owner",
            new=AsyncMock(
                side_effect=HTTPException(status_code=403, detail="Forbidden")
            ),
        ),
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(),
        ) as m,
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 403
    m.assert_not_awaited()


@pytest.mark.asyncio
async def test_stamp_thread_missing_is_404():
    """update_thread_external_id returns None (thread vanished) → 404."""
    app = _stamp_app()
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()),
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=None),
        ),
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Privileged service caller (X-Service-Token, no X-User-Id): the backfill path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stamp_service_no_user_skips_ownership():
    """Service-token-only caller (user_id=None) stamps without an ownership
    check, passing user_id-agnostic args to the DB (thread-id-scoped UPDATE)."""
    app = _stamp_app(None)
    row = _thread_row(platform="telegram", external_id="chat:42")
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()) as owner,
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=row),
        ) as m,
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 200
    owner.assert_not_awaited()  # ownership check skipped for the service caller
    m.assert_awaited_once_with("t-1", "telegram", "chat:42")


@pytest.mark.asyncio
async def test_stamp_service_with_user_keeps_ownership():
    """A service token WITH X-User-Id still enforces ownership on the stamp."""
    app = _stamp_app(USER)
    row = _thread_row(platform="telegram", external_id="chat:42")
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()) as owner,
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=row),
        ) as m,
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 200
    owner.assert_awaited_once_with("t-1", USER)
    m.assert_awaited_once_with("t-1", "telegram", "chat:42")


@pytest.mark.asyncio
async def test_stamp_service_conflict_still_409():
    """The service path surfaces the same 409 conflict body as the user path."""
    app = _stamp_app(None)
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()),
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(
                side_effect=ExternalIdConflictError(
                    platform="telegram", external_id="chat:42"
                )
            ),
        ),
    ):
        resp = await _put(app, {"platform": "telegram", "external_id": "chat:42"})

    assert resp.status_code == 409
    assert resp.json()["detail"]["error_type"] == "external_id_conflict"


# ---------------------------------------------------------------------------
# End-to-end through the REAL get_stamp_auth dependency (header handling)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stamp_real_service_token_no_user_id():
    """Real dependency: valid X-Service-Token with NO X-User-Id → privileged
    service caller, ownership skipped, DB called for the stamp."""
    app = _real_app()
    row = _thread_row(platform="telegram", external_id="chat:42")
    with (
        patch("src.server.utils.api._SERVICE_TOKEN", "svc-secret"),
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()) as owner,
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=row),
        ) as m,
    ):
        resp = await _put(
            app,
            {"platform": "telegram", "external_id": "chat:42"},
            headers={"X-Service-Token": "svc-secret"},
        )

    assert resp.status_code == 200
    owner.assert_not_awaited()
    m.assert_awaited_once_with("t-1", "telegram", "chat:42")


@pytest.mark.asyncio
async def test_stamp_real_service_token_with_user_enforces_ownership():
    """Real dependency: X-Service-Token WITH X-User-Id acts as that user and the
    stamp keeps the ownership check."""
    app = _real_app()
    row = _thread_row(platform="telegram", external_id="chat:42")
    with (
        patch("src.server.utils.api._SERVICE_TOKEN", "svc-secret"),
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()) as owner,
        patch(
            f"{THREADS_MOD}.update_thread_external_id",
            new=AsyncMock(return_value=row),
        ) as m,
    ):
        resp = await _put(
            app,
            {"platform": "telegram", "external_id": "chat:42"},
            headers={"X-Service-Token": "svc-secret", "X-User-Id": "alice"},
        )

    assert resp.status_code == 200
    owner.assert_awaited_once_with("t-1", "alice")
    m.assert_awaited_once_with("t-1", "telegram", "chat:42")


@pytest.mark.asyncio
async def test_stamp_real_invalid_service_token_is_401():
    """Real dependency: a bad X-Service-Token is rejected before the handler."""
    app = _real_app()
    with (
        patch("src.server.utils.api._SERVICE_TOKEN", "svc-secret"),
        patch(
            f"{THREADS_MOD}.update_thread_external_id", new=AsyncMock()
        ) as m,
    ):
        resp = await _put(
            app,
            {"platform": "telegram", "external_id": "chat:42"},
            headers={"X-Service-Token": "wrong-token"},
        )

    assert resp.status_code == 401
    m.assert_not_awaited()


# ---------------------------------------------------------------------------
# Rename path — PATCH /threads/{id} (title only, requires a concrete user)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_patch_title_renames():
    app = _title_app()
    row = _thread_row(title="Renamed")
    with (
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()),
        patch(
            f"{THREADS_MOD}.update_thread_title",
            new=AsyncMock(return_value=row),
        ) as m,
    ):
        resp = await _patch(app, {"title": "Renamed"})

    assert resp.status_code == 200
    assert resp.json()["title"] == "Renamed"
    m.assert_awaited_once_with("t-1", "Renamed")


@pytest.mark.asyncio
async def test_patch_title_service_no_user_is_401():
    """Rename requires a concrete user; a service-token-only caller → 401 from
    the real get_current_user_id dependency."""
    app = _real_app()
    # create_test_app bypasses get_current_user_id by default; drop that override
    # so the real dependency (which enforces X-User-Id) runs here.
    app.dependency_overrides.pop(get_current_user_id, None)
    with (
        patch("src.server.utils.api._SERVICE_TOKEN", "svc-secret"),
        patch(f"{AUTH_MOD}.require_thread_owner", new=AsyncMock()) as owner,
        patch(f"{THREADS_MOD}.update_thread_title", new=AsyncMock()) as title,
    ):
        resp = await _patch(
            app, {"title": "Renamed"}, headers={"X-Service-Token": "svc-secret"}
        )

    assert resp.status_code == 401
    owner.assert_not_awaited()
    title.assert_not_awaited()
