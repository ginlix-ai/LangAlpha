"""Route-level coverage for ``GET /threads/{id}/status``.

Pins the optimizations layered onto this endpoint:
- a single ``get_thread_auth_meta`` query authorizes AND yields ``is_shared``,
  which is threaded into the full status (no second thread lookup), and
- ``?fields=report_back`` returns only the cheap report-back slice, skipping the
  full status computation entirely.
"""

import pytest
from unittest.mock import AsyncMock, patch

STATUS_MOD = "src.server.services.report_back.flash.status"

CALLER = "test-user-123"  # create_test_app's bypassed user id
TID = "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_status_report_back_field_uses_cheap_path(threads_client):
    """``fields=report_back`` returns the cheap slice and never computes full status."""
    meta = AsyncMock(return_value={"user_id": CALLER, "is_shared": False})
    cheap = AsyncMock(
        return_value={
            "thread_id": TID,
            "pending_report_back": True,
            "report_back_run_id": "rb-1",
            "recent_report_back_run_ids": ["rb-0"],
        }
    )
    full = AsyncMock(return_value={"should": "not be called"})
    with (
        patch("src.server.database.conversation.get_thread_auth_meta", meta),
        patch(f"{STATUS_MOD}.read_report_back_status", cheap),
        patch("src.server.services.thread_status.read_thread_runtime_status", full),
    ):
        resp = await threads_client.get(
            f"/api/v1/threads/{TID}/status", params={"fields": "report_back"}
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "thread_id": TID,
        "pending_report_back": True,
        "report_back_run_id": "rb-1",
        "recent_report_back_run_ids": ["rb-0"],
    }
    cheap.assert_awaited_once_with(TID)
    full.assert_not_called()


@pytest.mark.asyncio
async def test_status_full_path_threads_is_shared(threads_client):
    """is_shared resolved while authorizing is passed into read_thread_runtime_status."""
    meta = AsyncMock(return_value={"user_id": CALLER, "is_shared": True})
    full = AsyncMock(return_value={"thread_id": TID, "is_shared": True})
    with (
        patch("src.server.database.conversation.get_thread_auth_meta", meta),
        patch("src.server.services.thread_status.read_thread_runtime_status", full),
    ):
        resp = await threads_client.get(f"/api/v1/threads/{TID}/status")

    assert resp.status_code == 200
    full.assert_awaited_once_with(TID, is_shared=True, msg_type=None)


@pytest.mark.asyncio
async def test_status_403_for_non_owner(threads_client):
    meta = AsyncMock(return_value={"user_id": "someone-else", "is_shared": False})
    with patch("src.server.database.conversation.get_thread_auth_meta", meta):
        resp = await threads_client.get(f"/api/v1/threads/{TID}/status")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_status_404_for_missing_thread(threads_client):
    meta = AsyncMock(return_value=None)
    with patch("src.server.database.conversation.get_thread_auth_meta", meta):
        resp = await threads_client.get(f"/api/v1/threads/{TID}/status")
    assert resp.status_code == 404
