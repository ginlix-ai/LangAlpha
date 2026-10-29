"""The run hook every computer status write calls.

It rides the lifecycle's own writes, so the contract that matters most is
what it never does: raise into the caller, or leave a timeout behind on the
caller's transaction. The SQL it runs is pinned against Postgres in
``tests/integration/test_computer_runs_db.py``.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg.pq
import pytest

from ptc_agent.config.core import ResourceTier
from src.server.database import computer_runs
from src.server.services.computer_usage import transitions

pytestmark = pytest.mark.asyncio

COMPUTER_ID = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def cursor():
    cur = MagicMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value=None)

    @asynccontextmanager
    async def fake_hook_cursor(conn=None):
        yield cur

    with patch.object(transitions, "hook_cursor", fake_hook_cursor):
        yield cur


@pytest.fixture
def runs_db():
    with patch.object(transitions, "runs_db") as db:
        db.open_computer_run = AsyncMock()
        db.close_computer_run = AsyncMock()
        db.stamp_run_end_reason = AsyncMock()
        db.get_computer_for_run = AsyncMock(return_value=None)
        yield db


@pytest.mark.parametrize(
    "status, reason",
    [
        ("stopped", "stopped"),
        ("error", "error"),
        ("deleted", "deleted"),
        ("creating", "replaced"),
    ],
)
async def test_leaving_running_closes_the_run_for_the_reason_the_status_implies(
    cursor, runs_db, status, reason
):
    await transitions.note_computer_status({"computer_id": COMPUTER_ID}, status)

    runs_db.close_computer_run.assert_awaited_once_with(
        cursor, COMPUTER_ID, reason=reason, pausing_kinds=("e2b",)
    )
    runs_db.open_computer_run.assert_not_called()


async def test_a_replacement_claim_leaves_the_run_open(cursor, runs_db):
    await transitions.note_computer_status({"computer_id": COMPUTER_ID}, "starting")

    runs_db.close_computer_run.assert_not_called()
    runs_db.open_computer_run.assert_not_called()


async def test_a_reason_the_caller_names_wins(cursor, runs_db):
    await transitions.note_computer_status(
        {"computer_id": COMPUTER_ID}, "stopped", reason="idle"
    )
    assert runs_db.close_computer_run.await_args.kwargs["reason"] == "idle"


async def test_stopping_keeps_the_run_open_and_stamps_only_a_named_reason(
    cursor, runs_db
):
    await transitions.note_computer_status({"computer_id": COMPUTER_ID}, "stopping")
    runs_db.stamp_run_end_reason.assert_not_called()

    await transitions.note_computer_status(
        {"computer_id": COMPUTER_ID}, "stopping", reason="idle"
    )
    runs_db.stamp_run_end_reason.assert_awaited_once_with(cursor, COMPUTER_ID, "idle")
    runs_db.close_computer_run.assert_not_called()


async def test_a_row_naming_no_computer_writes_nothing(cursor, runs_db):
    await transitions.note_computer_status(None, "running")
    await transitions.note_computer_status({"workspace_id": "w"}, "stopped")
    cursor.execute.assert_not_called()
    runs_db.close_computer_run.assert_not_called()


@pytest.mark.parametrize(
    "kind, status, opened, final",
    [
        ("daytona", "running", True, False),
        ("e2b", "running", True, False),
        # Billed but with no provider history: final at open, never polled.
        ("docker", "running", True, True),
        # The owner's own machine.
        ("ssh", "running", False, None),
        # Moved on before the hook read it.
        ("daytona", "stopping", False, None),
    ],
)
async def test_running_opens_a_run_only_on_a_metered_computer_still_running(
    cursor, runs_db, kind, status, opened, final
):
    runs_db.get_computer_for_run.return_value = {
        "computer_id": COMPUTER_ID,
        "kind": kind,
        "provider_config": {},
        "resource_tier": "standard",
        "status": status,
    }
    with patch.object(transitions, "resolve_spec", return_value=(2.0, 4.0, 5.0)):
        await transitions.note_computer_status({"computer_id": COMPUTER_ID}, "running")

    if not opened:
        runs_db.open_computer_run.assert_not_called()
        return
    call = runs_db.open_computer_run.await_args
    assert call.args == (cursor, COMPUTER_ID)
    assert call.kwargs["cpu"] == 2.0
    assert call.kwargs["memory_gb"] == 4.0
    assert call.kwargs["disk_gb"] == 5.0
    assert call.kwargs["metrics_final"] is final
    assert "ssh" not in call.kwargs["metered_kinds"]


@pytest.mark.parametrize("status", ["running", "stopped", "stopping"])
async def test_a_failing_ledger_never_raises_into_the_status_write(status, caplog):
    @asynccontextmanager
    async def broken(conn=None):
        raise RuntimeError("Conversation database pool is not open")
        yield  # pragma: no cover

    with patch.object(transitions, "hook_cursor", broken):
        await transitions.note_computer_status(
            {"computer_id": COMPUTER_ID}, status, reason="idle"
        )
    assert "Usage ledger not updated" in caplog.text


async def test_a_failing_statement_never_raises_either(cursor, runs_db):
    runs_db.close_computer_run.side_effect = Exception("lock timeout")
    await transitions.note_computer_status({"computer_id": COMPUTER_ID}, "stopped")


async def test_cancellation_still_reaches_the_caller(cursor, runs_db):
    """Swallowing it would let a cancelled stop carry on as if nothing asked."""
    runs_db.close_computer_run.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await transitions.note_computer_status({"computer_id": COMPUTER_ID}, "stopped")


def _conn(status):
    conn = MagicMock()
    conn.info.transaction_status = status
    cur = MagicMock()
    cur.execute = AsyncMock()

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def cursor(row_factory=None):
        yield cur

    conn.transaction = transaction
    conn.cursor = cursor
    return conn, cur


async def test_its_own_connection_gets_short_timeouts():
    conn, cur = _conn(psycopg.pq.TransactionStatus.IDLE)
    async with computer_runs.hook_cursor(conn):
        pass
    statements = [c.args[0] for c in cur.execute.await_args_list]
    assert statements == [
        "SET LOCAL statement_timeout = '2s'",
        "SET LOCAL lock_timeout = '1s'",
    ]


async def test_inside_the_callers_transaction_it_sets_nothing_that_outlives_it():
    conn, cur = _conn(psycopg.pq.TransactionStatus.INTRANS)
    async with computer_runs.hook_cursor(conn):
        pass
    cur.execute.assert_not_called()


def _manager(tiers, default_tier="standard"):
    settings = SimpleNamespace(resource_tiers=tiers, default_tier=default_tier)
    manager = MagicMock()
    manager._provider_settings.return_value = settings
    return manager


async def test_the_spec_is_the_tier_preset_from_the_computers_own_settings():
    manager = _manager({"performance": ResourceTier(cpu=2, memory=4, disk=5)})
    computer = {"kind": "daytona", "provider_config": {"x": 1},
                "resource_tier": "performance"}
    with patch(
        "src.server.services.computer_manager.ComputerManager.current",
        return_value=manager,
    ):
        assert transitions.resolve_spec(computer) == (2.0, 4.0, 5.0)
    manager._provider_settings.assert_called_once_with("daytona", {"x": 1})


@pytest.mark.parametrize(
    "manager",
    [None, _manager({"standard": ResourceTier(cpu=1, memory=1, disk=3)})],
)
async def test_no_manager_or_a_retired_tier_records_no_spec(manager):
    computer = {"kind": "daytona", "resource_tier": "gone"}
    with patch(
        "src.server.services.computer_manager.ComputerManager.current",
        return_value=manager,
    ):
        assert transitions.resolve_spec(computer) == (None, None, None)


async def test_a_hook_that_hangs_on_its_own_connection_gives_up(caplog):
    """A pool with no free slot waits past any statement timeout."""

    @asynccontextmanager
    async def stuck(conn=None):
        await asyncio.sleep(60)
        yield  # pragma: no cover

    with patch.object(transitions, "hook_cursor", stuck), patch.object(
        transitions, "_HOOK_TIMEOUT_S", 0.01
    ):
        await transitions.note_computer_status({"computer_id": COMPUTER_ID}, "stopped")
    assert "TimeoutError" in caplog.text
