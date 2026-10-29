"""M6-A v2 write contract (STREAM_CONTRACT_V2.md).

Active per-run streams carry no TTL; the attach-grace clock starts only at
a terminal append. lane_open is fail-closed — an anchorless stream must not
start — and run_end is deferred past the wrapper's steering sweep via
``defer_run_end`` + ``append_run_end`` (idempotent by last-frame read).
"""

import datetime
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import TaskRunRejected
from src.server.services.subagent_run_coordinator import SubagentRunCoordinator
from tests.unit.redis_mock_pipeline import attach_pipeline

COORD_SRDB = "src.server.services.subagent_run_coordinator.sr_db"
CACHE = "src.utils.cache.redis_cache.get_cache_client"
TTL = "src.config.settings.get_redis_ttl_workflow_events"
SETTLED = datetime.datetime(2026, 7, 1, 0, 3, tzinfo=datetime.timezone.utc)


def _cache(*, enabled: bool = True, xadd_fails: bool = False, tail=None):
    cache = MagicMock()
    cache.enabled = enabled
    cache.client = MagicMock()
    cache.client.xadd = AsyncMock(
        side_effect=RuntimeError("redis down") if xadd_fails else None,
        return_value=b"1-0",
    )
    cache.client.expire = AsyncMock(return_value=True)
    cache.client.xrevrange = AsyncMock(return_value=tail or [])
    # The control-lane announce batches xadd+expire into one pipeline; the
    # shim replays them onto these mocks so assertions stay on the commands.
    attach_pipeline(cache.client)
    return cache


def _row(**overrides):
    return {"task_run_id": "run-1", "task_id": "abc123", "status": "completed",
            **overrides}


@pytest.mark.asyncio
async def test_lane_open_failure_refuses_the_admission_and_settles_the_row(
    monkeypatch,
):
    """Fail closed: a run whose anchor can't be written must not spawn, and
    the just-born row is settled here, not stranded for the scanner."""
    start = AsyncMock(return_value=_row())
    settle = AsyncMock(return_value={"applied": True, "run": _row(status="error")})
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(f"{COORD_SRDB}.finalize_task_run_idempotent", settle)
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))

    with pytest.raises(TaskRunRejected):
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="abc123", cause="init"
        )

    settle.assert_awaited_once()
    kwargs = settle.await_args.kwargs
    assert kwargs["status"] == "error"
    assert kwargs["failure"]["error_type"] == "transport_lost"


@pytest.mark.asyncio
async def test_lane_open_carries_no_ttl(monkeypatch):
    """Active v2 streams never expire — retention starts at terminal."""
    cache = _cache()
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(
        "src.server.services.thread_control_stream.announce_task_run_started",
        AsyncMock(),
    )

    run_id = await SubagentRunCoordinator("t-1").start_task_run(
        task_id="abc123", cause="init"
    )

    assert run_id == "run-1"
    cache.client.xadd.assert_awaited_once()
    cache.client.expire.assert_not_awaited()


@pytest.mark.asyncio
async def test_lane_open_skipped_without_cache_admission_proceeds(monkeypatch):
    """A no-Redis deployment has no stream transport contract at all."""
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(CACHE, lambda: _cache(enabled=False))

    run_id = await SubagentRunCoordinator("t-1").start_task_run(
        task_id="abc123", cause="init"
    )
    assert run_id == "run-1"


@pytest.mark.asyncio
async def test_finalize_appends_run_end_and_starts_the_attach_grace_ttl(
    monkeypatch,
):
    """Recovery paths keep the immediate append — one terminal frame plus
    the retention stamp, in that order."""
    cache = _cache()
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row(status="error")}),
    )
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(TTL, lambda: 86400)

    await SubagentRunCoordinator("t-1").finalize_task_run("run-1", "error")

    (key, fields), _ = cache.client.xadd.await_args
    assert key == "subagent:stream:t-1:run-1"
    assert fields[b"type"] == b"run_end"
    cache.client.expire.assert_awaited_once_with(
        "subagent:stream:t-1:run-1", 86400, nx=True
    )


@pytest.mark.asyncio
async def test_finalize_defer_run_end_appends_nothing(monkeypatch):
    """The wrapper's path: the CAS lands but run_end waits for the sweep."""
    cache = _cache()
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row()}),
    )
    monkeypatch.setattr(CACHE, lambda: cache)

    await SubagentRunCoordinator("t-1").finalize_task_run(
        "run-1", "completed", defer_run_end=True
    )

    cache.client.xadd.assert_not_awaited()
    cache.client.expire.assert_not_awaited()


@pytest.mark.asyncio
async def test_append_run_end_is_idempotent_by_last_frame(monkeypatch):
    """Racing a recovery finalizer must not double the terminal frame."""
    cache = _cache(tail=[(b"9-0", {b"type": b"run_end"})])
    monkeypatch.setattr(CACHE, lambda: cache)

    await SubagentRunCoordinator("t-1").append_run_end(
        "run-1", task_id="abc123", outcome="completed", finalized_at=SETTLED
    )

    cache.client.xadd.assert_not_awaited()


@pytest.mark.asyncio
async def test_append_run_end_appends_after_content_and_stamps(monkeypatch):
    cache = _cache(tail=[(b"9-0", {b"type": b"message_chunk"})])
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(TTL, lambda: 86400)

    await SubagentRunCoordinator("t-1").append_run_end(
        "run-1", task_id="abc123", outcome="completed", finalized_at=SETTLED
    )

    (key, fields), _ = cache.client.xadd.await_args
    assert key == "subagent:stream:t-1:run-1"
    assert fields[b"type"] == b"run_end"
    assert b'"outcome": "completed"' in fields[b"payload"]
    cache.client.expire.assert_awaited_once_with(
        "subagent:stream:t-1:run-1", 86400, nx=True
    )


@pytest.mark.asyncio
async def test_run_end_names_when_the_cas_settled_the_run(monkeypatch):
    """Both appends carry ``finished`` (finalized_at, epoch ms), which the mux
    forwards on chan_close: the immediate one from the row it just settled,
    the deferred one from the row the wrapper's CAS returned."""
    cache = _cache()
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(
            return_value={
                "applied": True,
                "run": _row(status="error", finalized_at=SETTLED),
            }
        ),
    )
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(TTL, lambda: 86400)

    await SubagentRunCoordinator("t-1").finalize_task_run("run-1", "error")
    (_, fields), _ = cache.client.xadd.await_args
    assert json.loads(fields[b"payload"]) == {
        "outcome": "error",
        "finished": SETTLED.timestamp() * 1000,
    }

    cache = _cache(tail=[(b"9-0", {b"type": b"message_chunk"})])
    monkeypatch.setattr(CACHE, lambda: cache)
    await SubagentRunCoordinator("t-1").append_run_end(
        "run-1", task_id="abc123", outcome="completed", finalized_at=SETTLED
    )
    (_, fields), _ = cache.client.xadd.await_args
    assert json.loads(fields[b"payload"]) == {
        "outcome": "completed",
        "finished": SETTLED.timestamp() * 1000,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model", [{"id": "glm-5.3-cn", "provider": "z-ai-cn"}, None]
)
async def test_admission_records_the_model_on_the_row_and_lane_open(
    monkeypatch, model
):
    """The row and the frame a live card is born from carry one value, and a
    run on its launcher's model carries none rather than a null."""
    start = AsyncMock(return_value=_row())
    cache = _cache()
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(
        "src.server.services.thread_control_stream.announce_task_run_started",
        AsyncMock(),
    )

    await SubagentRunCoordinator("t-1").start_task_run(
        task_id="abc123", cause="init", model=model
    )

    assert start.await_args.kwargs["model"] == model
    (_, fields), _ = cache.client.xadd.await_args
    assert fields[b"type"] == b"lane_open"
    payload = json.loads(fields[b"payload"])
    if model:
        assert payload["model"] == model
    else:
        assert "model" not in payload


# ---------------------------------------------------------------------------
# M6-B: control-lane discovery announcements
# ---------------------------------------------------------------------------

ANNOUNCE = "src.server.services.thread_control_stream.announce_task_run_started"


@pytest.mark.asyncio
async def test_admission_announces_on_the_control_lane(monkeypatch):
    """lane_open lives inside the stream it announces — discovery needs the
    push-style control-lane entry, appended only after a successful anchor."""
    announce = AsyncMock()
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(CACHE, lambda: _cache())
    monkeypatch.setattr(ANNOUNCE, announce)

    await SubagentRunCoordinator("t-1").start_task_run(
        task_id="abc123", cause="init", parent_run_id="root-1"
    )

    announce.assert_awaited_once()
    kwargs = announce.await_args.kwargs
    assert kwargs["task_id"] == "abc123"
    assert kwargs["cause"] == "init"
    assert kwargs["parent_run_id"] == "root-1"


@pytest.mark.asyncio
async def test_lane_open_failure_never_announces(monkeypatch):
    """A refused admission must not be discoverable."""
    announce = AsyncMock()
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row(status="error")}),
    )
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))
    monkeypatch.setattr(ANNOUNCE, announce)

    with pytest.raises(TaskRunRejected):
        await SubagentRunCoordinator("t-1").start_task_run(task_id="abc123", cause="init")

    announce.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_lane_is_bounded_and_ttl_refreshed(monkeypatch):
    """The control stream is a nudge surface, not an archive: MAXLEN-trimmed
    with a rolling TTL; reconciliation backstops anything dropped."""
    from src.server.services import thread_control_stream as tcs

    cache = _cache()
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(TTL, lambda: 86400)

    await tcs.announce_task_run_started(
        "t-1", task_run_id="run-1", task_id="abc123", cause="resume"
    )

    (key, fields), kwargs = cache.client.xadd.await_args
    assert key == "subagent:control:t-1"
    assert fields[b"type"] == b"task_run_started"
    assert fields[b"run_id"] == b"run-1"
    assert kwargs["maxlen"] == tcs._CONTROL_MAXLEN
    cache.client.expire.assert_awaited_once_with("subagent:control:t-1", 86400)


@pytest.mark.asyncio
async def test_announce_failure_is_swallowed(monkeypatch):
    from src.server.services import thread_control_stream as tcs

    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))

    await tcs.announce_run_started("t-1", "root-run-1")  # must not raise


@pytest.mark.asyncio
async def test_lane_open_failure_marks_its_own_result_delivered(monkeypatch):
    """The rejection raised here becomes the launching call's own reply, and
    a reply that hands the agent a run's terminal fate IS the delivery.
    Unstamped, the report-back executor still thinks a notification is owed
    and opens a synthetic turn to re-announce a failure the agent already has.
    """
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row(status="error")}),
    )
    delivered = AsyncMock(return_value=True)
    monkeypatch.setattr(f"{COORD_SRDB}.mark_result_delivered", delivered)
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))

    with pytest.raises(TaskRunRejected):
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="abc123", cause="init"
        )

    delivered.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_unsettled_row_is_not_marked_delivered(monkeypatch):
    """No settle, no delivery to claim: the scanner still owes this row a
    terminal status, and stamping one it never reached would strand it.
    """
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(side_effect=RuntimeError("db down")),
    )
    delivered = AsyncMock(return_value=True)
    monkeypatch.setattr(f"{COORD_SRDB}.mark_result_delivered", delivered)
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))

    with pytest.raises(TaskRunRejected):
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="abc123", cause="init"
        )

    delivered.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_delivery_stamp_does_not_swallow_the_rejection(monkeypatch):
    """A missed stamp costs one redundant notification. Letting it replace the
    refusal would cost the launch failure its own reply, which is worse.
    """
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(
        f"{COORD_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row(status="error")}),
    )
    monkeypatch.setattr(
        f"{COORD_SRDB}.mark_result_delivered",
        AsyncMock(side_effect=RuntimeError("db down")),
    )
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))

    with pytest.raises(TaskRunRejected):
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="abc123", cause="init"
        )


@pytest.mark.asyncio
async def test_finalize_confirms_checkpointed_task_holds_before_its_cas(monkeypatch):
    """The CAS's settle would take a row the run held, whose message its
    checkpoint already carries, as unread."""
    order = []

    async def confirm(thread_id, task_id, task_run_id):
        order.append(("confirm", thread_id, task_id, task_run_id))
        return []

    async def finalize(**kwargs):
        order.append("finalize")
        return {"applied": False, "run": _row(status="cancelled")}

    monkeypatch.setattr(
        "src.server.services.report_back.steer.confirm_checkpointed_task_holds",
        confirm,
    )
    monkeypatch.setattr(f"{COORD_SRDB}.finalize_task_run_idempotent", finalize)
    coordinator = SubagentRunCoordinator("t-1")
    monkeypatch.setattr(
        coordinator, "_read_task_checkpoint_tip", AsyncMock(return_value=None)
    )

    await coordinator.finalize_task_run("run-1", "cancelled", task_id="abc123")

    assert order == [("confirm", "t-1", "abc123", "run-1"), "finalize"]
