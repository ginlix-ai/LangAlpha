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


STARTED = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
SETTLED = datetime(2026, 1, 5, 14, 42, tzinfo=timezone.utc)


def _row(
    tid,
    status,
    *,
    run_id=None,
    cancel_requested_at=None,
    last_activity_at=STARTED,
    settled_at=None,
    execution_time=None,
):
    return {
        "conversation_thread_id": tid,
        "conversation_response_id": run_id or str(uuid.uuid4()),
        "status": status,
        "cancel_requested_at": cancel_requested_at,
        "created_at": STARTED,
        "usage_settled_at": settled_at,
        "execution_time": execution_time,
        "last_activity_at": last_activity_at,
        "title": "Size the market",
        "workspace_name": "Semis",
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
                "run_started_at": STARTED.isoformat(),
                "run_finished_at": None,
                "last_activity_at": STARTED.isoformat(),
                "title": "Size the market",
                "workspace_name": "Semis",
            }
        ],
        "runs": [],
        "steers": [],
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
            "run_started_at": STARTED.isoformat(),
            "run_finished_at": None,
            "last_activity_at": STARTED.isoformat(),
            "title": "Size the market",
            "workspace_name": "Semis",
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
    spelling), with no run_id and no reconnect, and with the settled run's
    start and finish so a view can say how long it took."""
    tid = str(uuid.uuid4())
    row = _row(tid, ledger_status, last_activity_at=SETTLED, settled_at=SETTLED)
    resp, _ = await _liveness(threads_client, {tid: row}, tid)

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [
            {
                "thread_id": tid,
                "status": expected_status,
                "run_id": None,
                "can_reconnect": False,
                "run_started_at": STARTED.isoformat(),
                "run_finished_at": SETTLED.isoformat(),
                # Outlives the run: a settled thread still says how long it
                # has been quiet.
                "last_activity_at": SETTLED.isoformat(),
                "title": "Size the market",
                "workspace_name": "Semis",
            }
        ],
        "runs": [],
        "steers": [],
    }


@pytest.mark.parametrize(
    "execution_time,finished",
    [(720.0, SETTLED.isoformat()), (None, None)],
    ids=["start-plus-duration", "unknown"],
)
@pytest.mark.asyncio
async def test_a_run_settled_before_its_settle_time_was_kept(
    threads_client, execution_time, finished
):
    """A row with no settle instant finishes at its start plus its measured
    duration, as replay times it; one that knows neither says nothing rather
    than a zero-length run."""
    tid = str(uuid.uuid4())
    row = _row(tid, "completed", execution_time=execution_time)
    resp, _ = await _liveness(threads_client, {tid: row}, tid)

    assert resp.json()["liveness"][0]["run_finished_at"] == finished


@pytest.mark.asyncio
async def test_thread_absent_from_ledger_is_omitted(threads_client):
    """No attempt row (a dispatch still pre-START, an unowned thread, or a
    malformed id) → omitted, so the card keeps polling as 'starting' instead
    of freezing on a fabricated terminal."""
    tid = str(uuid.uuid4())
    resp, _ = await _liveness(threads_client, {}, tid)

    assert resp.status_code == 200
    assert resp.json() == {"liveness": [], "runs": [], "steers": []}


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
    assert resp.json() == {"liveness": [], "runs": [], "steers": []}
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
# ``runs``: a hand-off's own run, read by its id. Once it is over, the
# thread's latest attempt describes whatever the thread did since, so the
# hand-off reads its run instead.
# ---------------------------------------------------------------------------

RUNS = "src.server.database.runs.handoff_liveness.get_owned_runs"


async def _liveness_runs(threads_client, thread_rows, run_rows, params):
    with (
        patch(LEDGER, AsyncMock(return_value=thread_rows)) as ledger,
        patch(RUNS, AsyncMock(return_value=run_rows)) as owned,
    ):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params=params
        )
    return resp, ledger, owned


@pytest.mark.asyncio
async def test_runs_answer_per_run_beside_the_threads_latest(threads_client):
    """Two hand-offs to one thread: the thread's row is its latest run, and
    each run named by id answers with its own status and times."""
    tid = str(uuid.uuid4())
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    latest = _row(tid, "in_progress", run_id=second)
    run_rows = {
        first: _row(tid, "completed", run_id=first, settled_at=SETTLED),
        second: latest,
    }
    resp, ledger, owned = await _liveness_runs(
        threads_client, {tid: latest}, run_rows, {"ids": tid, "runs": f"{first},{second}"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert [row["status"] for row in body["liveness"]] == ["running"]
    assert body["runs"] == [
        {
            "run_id": first,
            "thread_id": tid,
            "status": "completed",
            "run_started_at": STARTED.isoformat(),
            "run_finished_at": SETTLED.isoformat(),
        },
        {
            "run_id": second,
            "thread_id": tid,
            "status": "running",
            "run_started_at": STARTED.isoformat(),
            "run_finished_at": None,
        },
    ]
    ledger.assert_awaited_once_with([tid], CALLER)
    owned.assert_awaited_once_with([first, second], CALLER)


@pytest.mark.asyncio
async def test_runs_status_reads_cancel_intent_as_the_threads_does(threads_client):
    rid = str(uuid.uuid4())
    row = _row(
        str(uuid.uuid4()),
        "in_progress",
        run_id=rid,
        cancel_requested_at=datetime.now(timezone.utc),
    )
    resp, _, _ = await _liveness_runs(threads_client, {}, {rid: row}, {"runs": rid})

    assert resp.json()["runs"][0]["status"] == "stopping"


@pytest.mark.asyncio
async def test_runs_alone_read_no_threads(threads_client):
    rid = str(uuid.uuid4())
    resp, ledger, owned = await _liveness_runs(threads_client, {}, {}, {"runs": rid})

    assert resp.status_code == 200
    assert resp.json() == {"liveness": [], "runs": [], "steers": []}
    ledger.assert_not_awaited()
    owned.assert_awaited_once_with([rid], CALLER)


@pytest.mark.asyncio
async def test_threads_alone_read_no_runs(threads_client):
    tid = str(uuid.uuid4())
    _, _, owned = await _liveness_runs(threads_client, {}, {}, {"ids": tid})

    owned.assert_not_awaited()


@pytest.mark.asyncio
async def test_runs_ownership_binds_the_authenticated_caller(threads_client):
    """IDOR guard: a run is read only through the caller's own workspaces,
    never by an owner the client names."""
    rid = str(uuid.uuid4())
    _, _, owned = await _liveness_runs(
        threads_client, {}, {}, {"runs": rid, "user_id": "someone-else"}
    )

    owned.assert_awaited_once_with([rid], CALLER)


@pytest.mark.asyncio
async def test_runs_dedup_and_cap_like_ids(threads_client):
    runs = [f"r-{i}" for i in range(101)]
    _, _, owned = await _liveness_runs(
        threads_client, {}, {}, {"runs": ",".join(["r-0", " ", *runs])}
    )

    called_with = owned.await_args.args[0]
    assert called_with == runs[:100]


# ---------------------------------------------------------------------------
# ``steers``: a hand-off steered into a running turn is taken by whichever run
# reads it first, which is a later run when that turn ended before reading
# it. Each steer says where it is, and the run that took it answers in
# ``runs`` beside the runs asked for.
# ---------------------------------------------------------------------------

STEERS = "src.server.database.runs.handoff_liveness.get_owned_steers"


async def _liveness_steers(threads_client, steer_rows, params, run_rows=None):
    with (
        patch(LEDGER, AsyncMock(return_value={})) as ledger,
        patch(RUNS, AsyncMock(return_value=run_rows or {})) as owned,
        patch(STEERS, AsyncMock(return_value=steer_rows)) as steers,
    ):
        resp = await threads_client.get(
            "/api/v1/threads/dispatches/liveness", params=params
        )
    return resp, ledger, owned, steers


def _taken(tid, rid, row):
    return {"thread_id": tid, "state": "taken", "run_id": rid, "run": row}


@pytest.mark.asyncio
async def test_a_taken_steer_names_its_run_and_that_run_answers_in_runs(
    threads_client,
):
    tid, sid, rid = (str(uuid.uuid4()) for _ in range(3))
    taker = _row(tid, "completed", run_id=rid, settled_at=SETTLED)
    resp, ledger, owned, steers = await _liveness_steers(
        threads_client, {sid: _taken(tid, rid, taker)}, {"steers": sid}
    )

    assert resp.status_code == 200
    assert resp.json() == {
        "liveness": [],
        "runs": [
            {
                "run_id": rid,
                "thread_id": tid,
                "status": "completed",
                "run_started_at": STARTED.isoformat(),
                "run_finished_at": SETTLED.isoformat(),
            }
        ],
        "steers": [
            {"steer_id": sid, "thread_id": tid, "state": "taken", "run_id": rid}
        ],
    }
    ledger.assert_not_awaited()
    owned.assert_not_awaited()
    steers.assert_awaited_once_with([sid], CALLER)


@pytest.mark.asyncio
async def test_a_taker_already_asked_for_answers_once(threads_client):
    """Two hand-offs steered into one run, beside that run read by its id."""
    tid, rid = str(uuid.uuid4()), str(uuid.uuid4())
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    row = _row(tid, "in_progress", run_id=rid)
    resp, _, owned, _ = await _liveness_steers(
        threads_client,
        {first: _taken(tid, rid, row), second: _taken(tid, rid, row)},
        {"runs": rid, "steers": f"{first},{second}"},
        run_rows={rid: row},
    )

    body = resp.json()
    assert [(r["run_id"], r["status"]) for r in body["runs"]] == [(rid, "running")]
    assert [(s["steer_id"], s["run_id"]) for s in body["steers"]] == [
        (first, rid),
        (second, rid),
    ]
    owned.assert_awaited_once_with([rid], CALLER)


@pytest.mark.parametrize("state", ["queued", "dropped"])
@pytest.mark.asyncio
async def test_a_steer_no_run_took_names_no_run(threads_client, state):
    tid, sid = str(uuid.uuid4()), str(uuid.uuid4())
    resp, _, _, _ = await _liveness_steers(
        threads_client,
        {sid: {"thread_id": tid, "state": state, "run_id": None, "run": None}},
        {"steers": sid},
    )

    assert resp.json() == {
        "liveness": [],
        "runs": [],
        "steers": [
            {"steer_id": sid, "thread_id": tid, "state": state, "run_id": None}
        ],
    }


@pytest.mark.asyncio
async def test_an_unknown_steer_is_omitted(threads_client):
    """Unowned, unknown or purged: the view keeps the run it was steered into."""
    resp, _, _, _ = await _liveness_steers(
        threads_client, {}, {"steers": str(uuid.uuid4())}
    )

    assert resp.json() == {"liveness": [], "runs": [], "steers": []}


@pytest.mark.asyncio
async def test_steers_bind_the_caller_dedup_and_cap_like_ids(threads_client):
    sids = [f"s-{i}" for i in range(101)]
    _, _, _, steers = await _liveness_steers(
        threads_client,
        {},
        {"steers": ",".join(["s-0", " ", *sids]), "user_id": "someone-else"},
    )

    assert steers.await_args.args == (sids[:100], CALLER)


@pytest.mark.asyncio
async def test_threads_and_runs_alone_read_no_steers(threads_client):
    _, _, _, steers = await _liveness_steers(
        threads_client, {}, {"ids": str(uuid.uuid4()), "runs": str(uuid.uuid4())}
    )

    steers.assert_not_awaited()


# ---------------------------------------------------------------------------
# The readers themselves: the pre-bind UUID guard. The id columns are uuid:
# one malformed client id in the ANY(...) bind would 22P02 the whole batch,
# so non-UUIDs must drop before the query. What get_owned_steers answers is
# pinned against real Postgres in tests/integration/test_steer_outbox_db.py.
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


@pytest.mark.asyncio
async def test_owned_runs_drop_non_uuid_ids_before_bind():
    """The run id column is uuid too: one malformed id would fail the batch."""
    from src.server.database.runs import handoff_liveness

    fake_db, cursor = _fake_db()
    with patch("src.server.database.pool.get_db_connection", new=fake_db):
        got = await handoff_liveness.get_owned_runs(["not-a-uuid", ""], CALLER)

    assert got == {}
    cursor.execute.assert_not_called()


@pytest.mark.asyncio
async def test_owned_runs_bind_normalized_ids_and_owner():
    from src.server.database.runs import handoff_liveness

    valid = str(uuid.uuid4())
    fake_db, cursor = _fake_db()
    with patch("src.server.database.pool.get_db_connection", new=fake_db):
        await handoff_liveness.get_owned_runs([valid.upper(), "junk"], CALLER)

    sql, (bound_ids, bound_user) = cursor.execute.await_args.args
    assert bound_ids == [valid]
    assert bound_user == CALLER
    # Ownership is in the query itself, through the run's workspace.
    assert "w.user_id = %s" in sql


@pytest.mark.asyncio
async def test_owned_steers_drop_non_uuid_ids_before_bind():
    from src.server.database.runs import handoff_liveness

    fake_db, cursor = _fake_db()
    with patch("src.server.database.pool.get_db_connection", new=fake_db):
        got = await handoff_liveness.get_owned_steers(["not-a-uuid", ""], CALLER)

    assert got == {}
    cursor.execute.assert_not_called()
