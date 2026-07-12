"""Reconnect preflight classification (1.5d) — tri-state transport reads.

Pins the admission contract of ``classify_reconnect``:

- Redis reachable + stream EXISTS      -> 200 (return run_id)
- Redis reachable + confirmed missing  -> 410 stream_expired (permanent)
- Redis configured but unreachable     -> 503 transport_unavailable (I6:
  absence must never be asserted from an outage)
- explicit run_id without a ledger row -> only admitted when the local task
  registry or the tracker blob vouches for that EXACT run id
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.handlers.chat import stream_reconnect

RUN_ID = "11111111-1111-4111-8111-111111111111"
OTHER_RUN = "22222222-2222-4222-8222-222222222222"
TID = "thread-1"


class _Cache:
    def __init__(self, enabled=True, client=None):
        self.enabled = enabled
        self.client = client


def _patches(
    *,
    run_row,
    task_info=None,
    tracker_status=None,
    cache=None,
    backend="redis",
):
    from src.server.database import turn_lifecycle as tl_db
    from src.server.services import background_task_manager as btm_mod
    from src.server.services.workflow_tracker import WorkflowTracker

    manager = MagicMock()
    manager.get_task_info = AsyncMock(return_value=task_info)
    manager.event_storage_backend = backend
    tracker = MagicMock()
    tracker.get_status = AsyncMock(return_value=tracker_status)
    return (
        patch.object(tl_db, "get_run", AsyncMock(return_value=run_row)),
        patch.object(tl_db, "get_latest_attempt", AsyncMock(return_value=run_row)),
        patch.object(
            btm_mod.BackgroundTaskManager,
            "get_instance",
            classmethod(lambda cls: manager),
        ),
        patch.object(
            WorkflowTracker, "get_instance", classmethod(lambda cls: tracker)
        ),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache or _Cache(),
        ),
    )


async def _classify(run_id=RUN_ID, **kw):
    ps = _patches(**kw)
    with ps[0], ps[1], ps[2], ps[3], ps[4]:
        return await stream_reconnect.classify_reconnect(TID, run_id)


def _terminal_row(status="completed"):
    return {"conversation_thread_id": TID, "status": status}


@pytest.mark.asyncio
async def test_terminal_with_live_stream_streams():
    client = MagicMock()
    client.exists = AsyncMock(return_value=1)
    got = await _classify(run_row=_terminal_row(), cache=_Cache(client=client))
    assert got == RUN_ID


@pytest.mark.asyncio
async def test_terminal_with_confirmed_missing_stream_is_410():
    client = MagicMock()
    client.exists = AsyncMock(return_value=0)
    with pytest.raises(HTTPException) as exc:
        await _classify(run_row=_terminal_row(), cache=_Cache(client=client))
    assert exc.value.status_code == 410
    assert exc.value.detail["code"] == "stream_expired"


@pytest.mark.asyncio
async def test_terminal_with_redis_error_is_503_not_410():
    """A transport outage must read as retryable, never as permanent expiry."""
    client = MagicMock()
    client.exists = AsyncMock(side_effect=ConnectionError("redis down"))
    with pytest.raises(HTTPException) as exc:
        await _classify(run_row=_terminal_row(), cache=_Cache(client=client))
    assert exc.value.status_code == 503
    assert exc.value.detail["code"] == "transport_unavailable"


@pytest.mark.asyncio
async def test_terminal_with_cache_disabled_redis_backend_is_503():
    with pytest.raises(HTTPException) as exc:
        await _classify(
            run_row=_terminal_row(), cache=_Cache(enabled=False), backend="redis"
        )
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_terminal_with_memory_backend_is_410():
    """No stream transport configured: absence is permanent truth, not outage."""
    with pytest.raises(HTTPException) as exc:
        await _classify(
            run_row=_terminal_row(), cache=_Cache(enabled=False), backend="memory"
        )
    assert exc.value.status_code == 410


@pytest.mark.asyncio
async def test_live_run_with_executor_and_redis_down_is_503():
    """A committed 200 must not attach to a stream that can never be read."""
    client = MagicMock()
    client.exists = AsyncMock(side_effect=ConnectionError("redis down"))
    with pytest.raises(HTTPException) as exc:
        await _classify(
            run_row={"conversation_thread_id": TID, "status": "in_progress"},
            task_info=MagicMock(run_id=RUN_ID),
            cache=_Cache(client=client),
        )
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_live_run_with_memory_backend_is_503():
    """No Redis event storage means no live transport at all: a watch on a
    live run can never deliver, so admission must be refused — never a 200
    to a stream nothing will ever write."""
    with pytest.raises(HTTPException) as exc:
        await _classify(
            run_row={"conversation_thread_id": TID, "status": "in_progress"},
            task_info=MagicMock(run_id=RUN_ID),
            backend="memory",
        )
    assert exc.value.status_code == 503
    assert exc.value.detail["code"] == "transport_unavailable"


@pytest.mark.asyncio
async def test_legacy_fallback_requires_tracker_run_id_match():
    """No ledger row + no task: an unrelated tracker blob must not admit a
    made-up explicit run id onto an empty stream key."""
    with pytest.raises(HTTPException) as exc:
        await _classify(
            run_row=None,
            tracker_status={"status": "active", "run_id": OTHER_RUN},
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_legacy_fallback_admits_matching_tracker_run_id():
    got = await _classify(
        run_row=None,
        tracker_status={"status": "active", "run_id": RUN_ID},
    )
    assert got == RUN_ID
