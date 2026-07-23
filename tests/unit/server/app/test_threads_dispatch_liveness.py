"""Route coverage for ``GET /threads/dispatches/liveness``.

The batched dispatch-status read-model (v4 2.4): one ownership-filtered
ledger query (latest attempt per thread) resolves N dispatch cards in a
single round-trip. Worker-agnostic by construction — no tracker blobs, no
in-process liveness cross-check, no reader-side healing (the recovery
scanner converges orphaned in_progress rows). Threads with no attempt row
yet are omitted so the card keeps polling as 'starting'.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

CALLER = "test-user-123"  # create_test_app's bypassed user id

LEDGER = "src.server.database.runs.lifecycle.get_latest_attempts_for_threads"


def _row(tid, status, *, run_id=None, cancel_requested_at=None):
    return {
        "conversation_thread_id": tid,
        "conversation_response_id": run_id or str(uuid.uuid4()),
        "status": status,
        "cancel_requested_at": cancel_requested_at,
    }


async def _liveness(threads_client, rows, ids):
    with patch(LEDGER, AsyncMock(return_value=rows)) as ledger:
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": ids}
        )
    return resp, ledger


@pytest.mark.asyncio
async def test_live_run_maps_to_running_with_reconnect(threads_client):
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(
        threads_client, {tid: _row(tid, "in_progress", run_id="r-1")}, tid
    )

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": tid,
                "status": "running",
                "run_id": "r-1",
                "can_reconnect": True,
            }
        ]
    }


@pytest.mark.asyncio
async def test_cancel_requested_maps_to_stopping_still_reconnectable(threads_client):
    """Durable cancel intent refines the live slice to 'stopping'; the stream
    is still attachable until the finalize lands."""
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(
        threads_client,
        {
            tid: _row(
                tid,
                "in_progress",
                run_id="r-1",
                cancel_requested_at=datetime.now(timezone.utc),
            )
        },
        tid,
    )

    assert resp.status_code == 200
    assert resp.json()["liveness"] == [
        {
            "thread_id": tid,
            "status": "stopping",
            "run_id": "r-1",
            "can_reconnect": True,
        }
    ]


@pytest.mark.parametrize(
    "ledger_status,expected_status",
    [
        ("completed", "completed"),
        ("error", "failed"),
        ("cancelled", "cancelled"),
        ("interrupted", "interrupted"),
    ],
)
@pytest.mark.asyncio
async def test_terminal_attempt_maps_to_public_vocabulary(
    threads_client, ledger_status, expected_status
):
    """Terminal rows come back as the public enum value (not the raw internal
    spelling), with no run_id and no reconnect."""
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(threads_client, {tid: _row(tid, ledger_status)}, tid)

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": tid,
                "status": expected_status,
                "run_id": None,
                "can_reconnect": False,
            }
        ]
    }


@pytest.mark.asyncio
async def test_thread_absent_from_ledger_is_omitted(threads_client):
    """No attempt row (a dispatch still pre-START, an unowned thread, or a
    malformed id) → omitted, so the card keeps polling as 'starting' instead
    of freezing on a fabricated terminal."""
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(threads_client, {}, tid)

    assert resp.status_code == 200
    assert resp.json() == {"liveness": []}


@pytest.mark.asyncio
async def test_ownership_binds_the_authenticated_caller(threads_client):
    """IDOR guard: the ledger query is scoped by the authenticated caller's
    id — never anything client-supplied."""
    tid = str(uuid.uuid4())
    _, ledger = await _liveness(threads_client, {}, tid)

    ledger.assert_awaited_once_with([tid], CALLER)


@pytest.mark.asyncio
async def test_dedups_ids_into_one_query(threads_client):
    _, ledger = await _liveness(threads_client, {}, "t-1,t-1,t-2, ,t-2")

    ledger.assert_awaited_once_with(["t-1", "t-2"], CALLER)


@pytest.mark.asyncio
async def test_empty_ids_returns_empty_without_query(threads_client):
    resp, ledger = await _liveness(threads_client, {}, " , ,")

    assert resp.status_code == 200
    assert resp.json() == {"liveness": []}
    ledger.assert_not_awaited()


@pytest.mark.asyncio
async def test_caps_ids_at_max_and_only_first_100_reach_query(threads_client):
    """>100 distinct ids: only the first _MAX_LIVENESS_IDS (100) reach the
    ledger query; the remainder are dropped for this request."""
    ids = [f"t-{i}" for i in range(101)]
    _, ledger = await _liveness(threads_client, {}, ",".join(ids))

    called_with = ledger.await_args.args[0]
    assert len(called_with) == 100
    assert called_with == ids[:100]


# ---------------------------------------------------------------------------
# get_latest_attempts_for_threads itself — the pre-bind UUID guard. The
# conversation_thread_id column is uuid: one malformed client id in the
# ANY(...) bind would 22P02 the whole batch, so non-UUIDs must drop before
# the query.
# ---------------------------------------------------------------------------


def _fake_db():
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])
    conn = MagicMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _get_db_connection():
        yield conn

    return _get_db_connection, cursor


@pytest.mark.asyncio
async def test_helper_drops_non_uuid_ids_before_bind():
    from src.server.database.runs import lifecycle as tl_db

    fake_db, cursor = _fake_db()
    with patch("src.server.database.pool.get_db_connection", new=fake_db):
        got = await tl_db.get_latest_attempts_for_threads(
            ["not-a-uuid", ""], CALLER
        )

    assert got == {}
    cursor.execute.assert_not_called()


@pytest.mark.asyncio
async def test_helper_binds_normalized_ids_and_owner():
    from src.server.database.runs import lifecycle as tl_db

    valid = str(uuid.uuid4())
    fake_db, cursor = _fake_db()
    with patch("src.server.database.pool.get_db_connection", new=fake_db):
        await tl_db.get_latest_attempts_for_threads([valid, "junk"], CALLER)

    bound_ids, bound_user = cursor.execute.await_args.args[1]
    assert bound_ids == [valid]
    assert bound_user == CALLER
