"""Locks the mux discovery ledger backstop.

The active set + meta hash are TTL'd Redis state: a long-lived cross-worker
task can outlive both and become undiscoverable, so the browser never gets a
channel. The in_progress ledger row cannot lapse — discovery unions it in,
without ever overriding a fresher registry/Redis epoch.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.thread_stream_mux import _NO_EPOCH, _discover_tasks

SR_DB = "src.server.database.subagent_runs"
MUX = "src.server.handlers.chat.thread_stream_mux"


def _env(*, registry=None, ledger_rows=(), ledger_error=None):
    store = MagicMock()
    store.get_registry = AsyncMock(return_value=registry)
    lister = AsyncMock(return_value=list(ledger_rows))
    if ledger_error is not None:
        lister.side_effect = ledger_error
    cache = MagicMock()
    cache.enabled = False
    return (
        patch(f"{MUX}.BackgroundRegistryStore.get_instance", return_value=store),
        patch(f"{MUX}.get_cache_client", return_value=cache),
        patch(f"{SR_DB}.list_open_runs_for_thread", new=lister),
    )


@pytest.mark.asyncio
async def test_open_run_row_is_discovered_when_redis_state_lapsed():
    patches = _env(ledger_rows=[{"task_id": "abc123", "task_run_id": "run-1"}])
    with patches[0], patches[1], patches[2]:
        out = await _discover_tasks("t-1")
    assert out == {"abc123": "run-1"}


@pytest.mark.asyncio
async def test_registry_epoch_wins_over_ledger_row():
    task = MagicMock()
    task.task_id = "abc123"
    task.task_run_id = "run-live"
    task.spawned_run_id = None
    ato = MagicMock()
    ato.done.return_value = False
    task.asyncio_task = ato
    registry = MagicMock()
    registry.get_all_tasks = AsyncMock(return_value=[task])
    patches = _env(
        registry=registry,
        ledger_rows=[{"task_id": "abc123", "task_run_id": "run-stale"}],
    )
    with patches[0], patches[1], patches[2]:
        out = await _discover_tasks("t-1")
    assert out == {"abc123": "run-live"}


@pytest.mark.asyncio
async def test_ledger_probe_failure_never_breaks_discovery():
    patches = _env(ledger_error=RuntimeError("db down"))
    with patches[0], patches[1], patches[2]:
        out = await _discover_tasks("t-1")
    assert out == {}
    assert _NO_EPOCH  # sanity: symbol still exported for epoch fallbacks
