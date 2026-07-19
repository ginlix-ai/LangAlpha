"""M6-A v2 write contract (STREAM_CONTRACT_V2.md).

Active per-run streams carry no TTL; the attach-grace clock starts only at
a terminal append. lane_open is fail-closed — an anchorless stream must not
start — and run_end is deferred past the wrapper's steering sweep via
``defer_run_end`` + ``append_run_end`` (idempotent by last-frame read).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import TaskRunRejected
from src.server.services.subagent_run_ledger import SubagentRunLedger

LEDGER_SRDB = "src.server.services.subagent_run_ledger.sr_db"
CACHE = "src.utils.cache.redis_cache.get_cache_client"
TTL = "src.config.settings.get_redis_ttl_workflow_events"


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
    monkeypatch.setattr(f"{LEDGER_SRDB}.start_task_run", start)
    monkeypatch.setattr(f"{LEDGER_SRDB}.finalize_task_run_idempotent", settle)
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))

    with pytest.raises(TaskRunRejected):
        await SubagentRunLedger("t-1").start_task_run(
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
    monkeypatch.setattr(f"{LEDGER_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(
        "src.server.services.thread_control_stream.announce_task_run_started",
        AsyncMock(),
    )

    run_id = await SubagentRunLedger("t-1").start_task_run(
        task_id="abc123", cause="init"
    )

    assert run_id == "run-1"
    cache.client.xadd.assert_awaited_once()
    cache.client.expire.assert_not_awaited()


@pytest.mark.asyncio
async def test_lane_open_skipped_without_cache_admission_proceeds(monkeypatch):
    """A no-Redis deployment has no stream transport contract at all."""
    monkeypatch.setattr(f"{LEDGER_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(CACHE, lambda: _cache(enabled=False))

    run_id = await SubagentRunLedger("t-1").start_task_run(
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
        f"{LEDGER_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row(status="error")}),
    )
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(TTL, lambda: 86400)

    await SubagentRunLedger("t-1").finalize_task_run("run-1", "error")

    (key, fields), _ = cache.client.xadd.await_args
    assert key == "subagent:stream:t-1:run-1"
    assert fields[b"type"] == b"run_end"
    cache.client.expire.assert_awaited_once_with(
        "subagent:stream:t-1:run-1", 86400
    )


@pytest.mark.asyncio
async def test_finalize_defer_run_end_appends_nothing(monkeypatch):
    """The wrapper's path: the CAS lands but run_end waits for the sweep."""
    cache = _cache()
    monkeypatch.setattr(
        f"{LEDGER_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row()}),
    )
    monkeypatch.setattr(CACHE, lambda: cache)

    await SubagentRunLedger("t-1").finalize_task_run(
        "run-1", "completed", defer_run_end=True
    )

    cache.client.xadd.assert_not_awaited()
    cache.client.expire.assert_not_awaited()


@pytest.mark.asyncio
async def test_append_run_end_is_idempotent_by_last_frame(monkeypatch):
    """Racing a recovery finalizer must not double the terminal frame."""
    cache = _cache(tail=[(b"9-0", {b"type": b"run_end"})])
    monkeypatch.setattr(CACHE, lambda: cache)

    await SubagentRunLedger("t-1").append_run_end(
        "run-1", task_id="abc123", outcome="completed"
    )

    cache.client.xadd.assert_not_awaited()


@pytest.mark.asyncio
async def test_append_run_end_appends_after_content_and_stamps(monkeypatch):
    cache = _cache(tail=[(b"9-0", {b"type": b"message_chunk"})])
    monkeypatch.setattr(CACHE, lambda: cache)
    monkeypatch.setattr(TTL, lambda: 86400)

    await SubagentRunLedger("t-1").append_run_end(
        "run-1", task_id="abc123", outcome="completed"
    )

    (key, fields), _ = cache.client.xadd.await_args
    assert key == "subagent:stream:t-1:run-1"
    assert fields[b"type"] == b"run_end"
    assert b'"outcome": "completed"' in fields[b"payload"]
    cache.client.expire.assert_awaited_once_with(
        "subagent:stream:t-1:run-1", 86400
    )


# ---------------------------------------------------------------------------
# M6-B: control-lane discovery announcements
# ---------------------------------------------------------------------------

ANNOUNCE = "src.server.services.thread_control_stream.announce_task_run_started"


@pytest.mark.asyncio
async def test_admission_announces_on_the_control_lane(monkeypatch):
    """lane_open lives inside the stream it announces — discovery needs the
    push-style control-lane entry, appended only after a successful anchor."""
    announce = AsyncMock()
    monkeypatch.setattr(f"{LEDGER_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(CACHE, lambda: _cache())
    monkeypatch.setattr(ANNOUNCE, announce)

    await SubagentRunLedger("t-1").start_task_run(
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
    monkeypatch.setattr(f"{LEDGER_SRDB}.start_task_run", AsyncMock(return_value=_row()))
    monkeypatch.setattr(
        f"{LEDGER_SRDB}.finalize_task_run_idempotent",
        AsyncMock(return_value={"applied": True, "run": _row(status="error")}),
    )
    monkeypatch.setattr(CACHE, lambda: _cache(xadd_fails=True))
    monkeypatch.setattr(ANNOUNCE, announce)

    with pytest.raises(TaskRunRejected):
        await SubagentRunLedger("t-1").start_task_run(task_id="abc123", cause="init")

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
