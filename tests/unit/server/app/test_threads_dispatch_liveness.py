"""Route coverage for ``GET /threads/dispatches/liveness``.

The batched dispatch-status read-model: one MGET of the cheap ``workflow:status``
blobs, ownership filtered by each blob's ``user_id`` (no per-thread DB read), so
N dispatch cards cost one round-trip instead of N heavy ``/status`` polls.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

CALLER = "test-user-123"  # create_test_app's bypassed user id
OTHER = "other-user-456"


def _tracker_returning(blobs: dict):
    tracker = MagicMock()
    tracker.get_statuses = AsyncMock(return_value=blobs)
    tracker.mark_completed = AsyncMock()
    return tracker


def _btm_all_live():
    """Fake BackgroundTaskManager whose every thread has a live task, so an ACTIVE
    blob's liveness cross-check keeps it running (no heal)."""
    manager = MagicMock()
    manager.get_live_task_info = AsyncMock(
        return_value={"live": True, "run_id": "btm-run", "active_tasks": []}
    )
    return manager


def _btm_not_found():
    """Fake BackgroundTaskManager with no live task for any thread (a restart
    orphaned the no-TTL ACTIVE blob), so an ACTIVE cross-check heals it."""
    manager = MagicMock()
    manager.get_live_task_info = AsyncMock(
        return_value={"live": False, "run_id": None, "active_tasks": []}
    )
    manager.get_admission_lock = AsyncMock(return_value=asyncio.Lock())
    return manager


def _patch_btm(manager):
    return patch(
        "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
        return_value=manager,
    )


def _terminal_db(rows):
    """Fake get_db_connection whose cursor simulates the ownership-filtered
    ``get_threads_terminal_status`` query.

    ``rows`` = list of ``(thread_id, owner_user_id, current_status)`` standing in
    for the conversation_threads JOIN workspaces contents; the cursor returns only
    rows whose id is in the bound ANY(...) list and whose owner is the bound user.
    Returns ``(fake_get_db_connection, cursor)`` for post-hoc assertions.
    """
    cursor = AsyncMock()

    async def _execute(sql, params):
        cursor.last_params = params

    cursor.execute = AsyncMock(side_effect=_execute)

    async def _fetchall():
        ids, owner = cursor.last_params
        idset = set(ids)
        return [
            {"conversation_thread_id": tid, "current_status": status}
            for (tid, row_owner, status) in rows
            if tid in idset and row_owner == owner
        ]

    cursor.fetchall = AsyncMock(side_effect=_fetchall)

    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake_get_db_connection():
        yield conn

    return _fake_get_db_connection, cursor


@pytest.mark.asyncio
async def test_filters_to_owner_and_maps_liveness(threads_client):
    """Only the caller's own threads come back, mapped to the liveness shape."""
    tracker = _tracker_returning({
        "t-own": {"status": "active", "run_id": "r-1", "user_id": CALLER},
        "t-foreign": {"status": "active", "run_id": "r-x", "user_id": "someone-else"},
    })
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(_btm_all_live()):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness",
            params={"ids": "t-own,t-foreign"},
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": "t-own",
                "status": "running",
                "run_id": "r-1",
                "can_reconnect": True,
            }
        ]
    }


@pytest.mark.asyncio
async def test_dedups_ids_into_one_mget(threads_client):
    tracker = _tracker_returning({})
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness",
            params={"ids": "t-1,t-1,t-2, ,t-2"},
        )

    assert resp.status_code == 200
    tracker.get_statuses.assert_awaited_once_with(["t-1", "t-2"])


@pytest.mark.asyncio
async def test_empty_ids_returns_empty_without_tracker_call(threads_client):
    tracker = _tracker_returning({"should": "not be read"})
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": " , ,"}
        )

    assert resp.status_code == 200
    assert resp.json() == {"liveness": []}
    tracker.get_statuses.assert_not_awaited()


# ---------------------------------------------------------------------------
# Durable-status fallback for ids absent from the blob pass (expired TTL). The
# real ``get_threads_terminal_status`` runs against a simulated DB so the
# ownership scoping and UUID normalization are genuinely exercised.
# ---------------------------------------------------------------------------


async def _liveness(threads_client, tracker_blobs, db_rows, ids, manager=None):
    """Drive the endpoint with a blob-less/present tracker and a simulated DB.

    ``manager`` fakes the BTM liveness cross-check; defaults to all-live so a
    present ACTIVE blob keeps its running slice (heal behavior is exercised by the
    dedicated stale-active tests below)."""
    tracker = _tracker_returning(tracker_blobs)
    fake_db, cursor = _terminal_db(db_rows)
    with (
        patch(
            "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
            return_value=tracker,
        ),
        _patch_btm(manager or _btm_all_live()),
        patch(
            "src.server.database.conversation.get_db_connection",
            new=fake_db,
        ),
    ):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": ids}
        )
    return resp, cursor


@pytest.mark.parametrize(
    "db_status,expected_status",
    [
        ("completed", "completed"),
        ("error", "failed"),
        ("cancelled", "cancelled"),
        ("interrupted", "interrupted"),
    ],
)
@pytest.mark.asyncio
async def test_absent_owned_terminal_resolves_from_db(
    threads_client, db_status, expected_status
):
    """An id gone from the blob pass resolves to its durable status as the enum
    value (not the raw DB string), so the card doesn't re-freeze on 'starting'."""
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(
        threads_client, {}, [(tid, CALLER, db_status)], tid
    )

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
async def test_absent_in_progress_owned_is_omitted(threads_client):
    """A just-dispatched run whose blob isn't written yet stays 'starting' — its
    in_progress DB row must not resolve the card as terminal."""
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(
        threads_client, {}, [(tid, CALLER, "in_progress")], tid
    )

    assert resp.status_code == 200
    assert resp.json() == {"liveness": []}


@pytest.mark.asyncio
async def test_absent_owned_by_other_user_is_omitted(threads_client):
    """IDOR guard: a completed thread owned by a different user is omitted, and
    the caller's id is what the ownership filter binds."""
    tid = str(uuid.uuid4())
    resp, cursor = await _liveness(
        threads_client, {}, [(tid, OTHER, "completed")], tid
    )

    assert resp.status_code == 200
    assert resp.json() == {"liveness": []}
    # The durable query scopes by the authenticated caller, never the target id's
    # real owner.
    assert cursor.last_params[1] == CALLER


@pytest.mark.asyncio
async def test_absent_malformed_id_is_omitted_without_error(threads_client):
    """A non-UUID absent id is dropped before the ANY(...) bind — no 500, no
    query, and the card is simply omitted."""
    resp, cursor = await _liveness(
        threads_client, {}, [], "not-a-uuid"
    )

    assert resp.status_code == 200
    assert resp.json() == {"liveness": []}
    cursor.execute.assert_not_called()


@pytest.mark.asyncio
async def test_present_blob_resolves_via_blob_pass_not_db(threads_client):
    """Regression: an id resolved by a live blob is never in the absent set, so
    the durable DB fallback is not consulted for it."""
    tid = str(uuid.uuid4())
    resp, cursor = await _liveness(
        threads_client,
        {tid: {"status": "active", "run_id": "r-1", "user_id": CALLER}},
        [(tid, CALLER, "completed")],
        tid,
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
    cursor.execute.assert_not_called()


@pytest.mark.asyncio
async def test_present_blob_and_absent_terminal_coexist(threads_client):
    """The blob pass and the durable fallback merge: a live id and an expired,
    completed id both come back in one response."""
    live = str(uuid.uuid4())
    expired = str(uuid.uuid4())
    resp, _ = await _liveness(
        threads_client,
        {live: {"status": "active", "run_id": "r-1", "user_id": CALLER}},
        [(expired, CALLER, "completed")],
        f"{live},{expired}",
    )

    assert resp.status_code == 200
    by_id = {s["thread_id"]: s for s in resp.json()["liveness"]}
    assert by_id[live] == {
        "thread_id": live,
        "status": "running",
        "run_id": "r-1",
        "can_reconnect": True,
    }
    assert by_id[expired] == {
        "thread_id": expired,
        "status": "completed",
        "run_id": None,
        "can_reconnect": False,
    }


# ---------------------------------------------------------------------------
# Stale-ACTIVE self-heal: a no-TTL ACTIVE blob orphaned by a process restart
# would otherwise report {running, can_reconnect} forever (card zombie). The
# endpoint cross-checks the in-process BTM (authoritative under the single-worker
# invariant) and heals a stale ACTIVE to a terminal slice — mirroring /status.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_active_blob_healed_when_btm_has_no_task(threads_client):
    """ACTIVE blob + BTM 'not_found' -> healed to a terminal slice, and the stale
    no-TTL blob is marked completed so it stops zombie-reporting."""
    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {tid: {"status": "active", "run_id": "r-1", "user_id": CALLER}}
    )
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(_btm_not_found()):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": tid,
                "status": "completed",
                "run_id": None,
                "can_reconnect": False,
            }
        ]
    }
    tracker.mark_completed.assert_awaited_once()
    assert tracker.mark_completed.await_args.args[0] == tid
    # The heal is CAS-gated on the run AND the ACTIVE status the healer
    # OBSERVED — neither a newer run's marker nor the same run's later
    # terminal state (interrupted/failed/cancelled) can be overwritten.
    assert tracker.mark_completed.await_args.kwargs["run_id"] == "r-1"
    from src.server.services.workflow_tracker import WorkflowStatus

    assert (
        tracker.mark_completed.await_args.kwargs["expected_status"]
        == WorkflowStatus.ACTIVE
    )


@pytest.mark.asyncio
async def test_active_blob_fresh_is_not_healed_mid_admission(threads_client):
    """A young ACTIVE blob with no visible task can be a dispatch between
    mark_active and pre_register — the age gate must leave it running for
    another poll instead of stamping a just-admitted run completed."""
    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {
            tid: {
                "status": "active",
                "run_id": "r-2",
                "user_id": CALLER,
                "last_update": datetime.now().isoformat(),
            }
        }
    )
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(_btm_not_found()):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": tid,
                "status": "running",
                "run_id": "r-2",
                "can_reconnect": True,
            }
        ]
    }
    tracker.mark_completed.assert_not_awaited()


@pytest.mark.asyncio
async def test_heal_lost_cas_keeps_running_slice(threads_client):
    """A heal whose CAS was refused (a newer run stamped the blob between the
    read and the write) must not resolve the card as completed."""
    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {tid: {"status": "active", "run_id": "r-1", "user_id": CALLER}}
    )
    tracker.mark_completed = AsyncMock(return_value=False)
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(_btm_not_found()):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
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
    tracker.mark_completed.assert_awaited_once()


@pytest.mark.asyncio
async def test_liveness_regained_under_admission_lock_skips_heal(threads_client):
    """The heal re-checks liveness under the per-thread admission lock: a task
    that became visible while waiting for the lock (admission completing)
    aborts the heal entirely."""
    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {tid: {"status": "active", "run_id": "r-1", "user_id": CALLER}}
    )
    manager = MagicMock()
    manager.get_live_task_info = AsyncMock(
        side_effect=[
            {"live": False, "run_id": None, "active_tasks": []},
            {"live": True, "run_id": "r-1", "active_tasks": []},
        ]
    )
    manager.get_admission_lock = AsyncMock(return_value=asyncio.Lock())
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(manager):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
        )

    assert resp.status_code == 200
    assert resp.json()["liveness"][0]["status"] == "running"
    tracker.mark_completed.assert_not_awaited()
    assert manager.get_live_task_info.await_count == 2


@pytest.mark.asyncio
async def test_genless_blob_heal_gates_on_never_matching_run_id(threads_client):
    """A run_id-less (legacy) blob heals gated on a sentinel that can never
    name a real run — the write lands only while the blob stays run_id-less."""
    from src.server.handlers.workflow_handler import _GENLESS_HEAL_GATE

    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {tid: {"status": "active", "user_id": CALLER}}
    )
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(_btm_not_found()):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
        )

    assert resp.status_code == 200
    assert resp.json()["liveness"][0]["status"] == "completed"
    assert tracker.mark_completed.await_args.kwargs["run_id"] == _GENLESS_HEAL_GATE


@pytest.mark.asyncio
async def test_active_blob_unchanged_when_btm_has_task(threads_client):
    """ACTIVE blob + BTM has the live task -> running slice unchanged, no heal."""
    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {tid: {"status": "active", "run_id": "r-1", "user_id": CALLER}}
    )
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(_btm_all_live()):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
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
    tracker.mark_completed.assert_not_awaited()


@pytest.mark.asyncio
async def test_interrupted_blob_not_healed_even_without_btm_task(threads_client):
    """INTERRUPTED is resumable-by-design with no live task; the ACTIVE-only
    cross-check must leave it untouched (never queried, never healed)."""
    tid = str(uuid.uuid4())
    tracker = _tracker_returning(
        {tid: {"status": "interrupted", "run_id": "r-1", "user_id": CALLER}}
    )
    manager = _btm_not_found()
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ), _patch_btm(manager):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": tid}
        )

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": tid,
                "status": "interrupted",
                "run_id": "r-1",
                "can_reconnect": False,
            }
        ]
    }
    tracker.mark_completed.assert_not_awaited()
    manager.get_live_task_info.assert_not_awaited()


@pytest.mark.asyncio
async def test_caps_ids_at_max_and_only_first_100_reach_mget(threads_client):
    """>100 distinct ids: only the first _MAX_LIVENESS_IDS (100) reach the single
    MGET; the remainder are dropped for this request."""
    ids = [f"t-{i}" for i in range(101)]
    tracker = _tracker_returning({})
    with patch(
        "src.server.services.workflow_tracker.WorkflowTracker.get_instance",
        return_value=tracker,
    ):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params={"ids": ",".join(ids)}
        )

    assert resp.status_code == 200
    called_with = tracker.get_statuses.await_args.args[0]
    assert len(called_with) == 100
    assert called_with == ids[:100]
