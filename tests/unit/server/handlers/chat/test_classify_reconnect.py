"""Reconnect preflight classification (1.5d) — tri-state transport reads.

Pins the admission contract of ``classify_reconnect``:

- Redis reachable + stream EXISTS      -> 200 (return run_id)
- Redis reachable + confirmed missing  -> 410 stream_expired (permanent)
- Redis configured but unreachable     -> 503 transport_unavailable (I6:
  absence must never be asserted from an outage)
- in_progress row                      -> admits with NO local-executor
  requirement (v4 2.4: the attach is an XREAD on the shared stream, so a
  foreign worker's healthy run must admit, never 409)
- no ledger row                        -> only the local task registry (the
  pre-START placeholder window) may vouch; otherwise 404. The tracker-blob
  corroboration path is deleted.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.handlers.chat import reconnect_admission

RUN_ID = "11111111-1111-4111-8111-111111111111"
TID = "thread-1"


class _Cache:
    def __init__(self, enabled=True, client=None):
        self.enabled = enabled
        self.client = client


def _patches(
    *,
    run_row,
    task_info=None,
    cache=None,
    backend="redis",
):
    from src.server.database.runs import lifecycle as tl_db
    from src.server.services.runs import executor as btm_mod

    manager = MagicMock()
    manager.get_local_run = AsyncMock(return_value=task_info)
    manager.event_storage_backend = backend
    manager.enable_storage = backend == "redis"
    return (
        patch.object(tl_db, "get_run", AsyncMock(return_value=run_row)),
        patch.object(tl_db, "get_latest_attempt", AsyncMock(return_value=run_row)),
        patch.object(
            btm_mod.LocalRunExecutor,
            "get_instance",
            classmethod(lambda cls: manager),
        ),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=cache or _Cache(),
        ),
    )


async def _classify(run_id=RUN_ID, **kw):
    ps = _patches(**kw)
    with ps[0], ps[1], ps[2], ps[3]:
        return await reconnect_admission.classify_reconnect(TID, run_id)


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


def _live_row():
    return {"conversation_thread_id": TID, "status": "in_progress"}


@pytest.mark.asyncio
async def test_live_run_without_local_executor_admits():
    """v4 2.4: a healthy foreign-worker run must admit the attach — the watch
    is an XREAD on the shared Redis stream, so executor locality is
    irrelevant. The old 409 ``recovering`` for in_progress + no local task
    would misclassify every peer-owned run."""
    client = MagicMock()
    client.exists = AsyncMock(return_value=1)
    got = await _classify(
        run_row=_live_row(), task_info=None, cache=_Cache(client=client)
    )
    assert got == RUN_ID


@pytest.mark.asyncio
async def test_live_run_with_redis_down_is_503():
    """A committed 200 must not attach to a stream that can never be read."""
    client = MagicMock()
    client.exists = AsyncMock(side_effect=ConnectionError("redis down"))
    with pytest.raises(HTTPException) as exc:
        await _classify(run_row=_live_row(), cache=_Cache(client=client))
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_live_run_with_memory_backend_is_503():
    """No Redis event storage means no live transport at all: a watch on a
    live run can never deliver, so admission must be refused — never a 200
    to a stream nothing will ever write."""
    with pytest.raises(HTTPException) as exc:
        await _classify(run_row=_live_row(), backend="memory")
    assert exc.value.status_code == 503
    assert exc.value.detail["code"] == "transport_unavailable"


@pytest.mark.asyncio
async def test_no_row_and_no_local_task_is_404():
    """No ledger row + no local task record: nothing durable vouches for the
    run id, so the attach is refused — no tracker blob can admit it."""
    with pytest.raises(HTTPException) as exc:
        await _classify(run_row=None, task_info=None)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_no_row_with_local_task_admits():
    """The pre-START placeholder window: the local registry is keyed by
    (thread, run), so its record may vouch for the attach before the START
    txn lands the row."""
    got = await _classify(run_row=None, task_info=MagicMock(run_id=RUN_ID))
    assert got == RUN_ID
