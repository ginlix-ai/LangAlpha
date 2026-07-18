"""The scanner's retroactive task-chain heal (M4-C).

The truncation paths repair their own thread transactionally. This sweep is
the backstop for damage that predates them and for any deletion path that
escapes the guard, so what matters is that it runs at all — specifically on
an IDLE deployment, which is exactly where orphaned task rows accumulate
unnoticed and where the scanner's open-run early-return would skip it.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.recovery_scanner import RecoveryScanner

SR_DB = "src.server.services.recovery_scanner.sr_db"
TL_DB = "src.server.services.recovery_scanner.tl_db"


def _idle_ledger():
    """No open runs of either kind — the early-return path."""
    return (
        patch(f"{TL_DB}.list_open_runs", new=AsyncMock(return_value=[])),
        patch(f"{SR_DB}.list_open_task_runs", new=AsyncMock(return_value=[])),
    )


@pytest.mark.asyncio
async def test_sweep_runs_even_when_there_is_nothing_to_recover():
    open_runs, open_task_runs = _idle_ledger()
    heal = AsyncMock(return_value={"rewound": 0, "deleted": 0})

    with open_runs, open_task_runs, patch.object(
        RecoveryScanner, "heal_task_chains", new=heal
    ):
        recovered = await RecoveryScanner().scan_once(assume_dead=True)

    assert recovered == 0
    heal.assert_awaited_once()


@pytest.mark.asyncio
async def test_heal_reports_what_it_repaired():
    repair = AsyncMock(return_value={"rewound": 2, "deleted": 1})

    with patch(f"{SR_DB}.repair_dangling_task_chains", new=repair):
        healed = await RecoveryScanner().heal_task_chains()

    assert healed == {"rewound": 2, "deleted": 1}
    repair.assert_awaited_once()


@pytest.mark.asyncio
async def test_heal_failure_never_aborts_the_scan():
    """A degraded heal must not cost the pass its orphan recovery — the two
    concerns are independent and only one of them is time-critical."""
    repair = AsyncMock(side_effect=RuntimeError("connection reset"))

    with patch(f"{SR_DB}.repair_dangling_task_chains", new=repair):
        healed = await RecoveryScanner().heal_task_chains()

    assert healed == {"rewound": 0, "deleted": 0}


@pytest.mark.asyncio
async def test_scan_survives_a_failing_sweep():
    open_runs, open_task_runs = _idle_ledger()
    repair = AsyncMock(side_effect=RuntimeError("connection reset"))

    with open_runs, open_task_runs, patch(
        f"{SR_DB}.repair_dangling_task_chains", new=repair
    ):
        assert await RecoveryScanner().scan_once(assume_dead=True) == 0


_ORPHAN_RUN = {
    "task_run_id": "run-9",
    "thread_id": "t-1",
    "task_id": "abc123",
    "cancel_requested_at": None,
}


def _stamp_cache(order: list, fail: bool = False):
    pipe = MagicMock()
    pipe.expire = MagicMock(return_value=pipe)
    pipe.execute = AsyncMock(
        side_effect=(
            RuntimeError("redis down")
            if fail
            else lambda: order.append("stamp") or [1, 1, 1]
        )
    )
    pipe_ctx = MagicMock()
    pipe_ctx.__aenter__ = AsyncMock(return_value=pipe)
    pipe_ctx.__aexit__ = AsyncMock(return_value=None)
    cache = SimpleNamespace(
        enabled=True,
        client=MagicMock(pipeline=MagicMock(return_value=pipe_ctx)),
    )
    return cache, pipe


@pytest.mark.asyncio
async def test_recovered_task_run_gets_its_retention_stamp_before_finalize():
    """Active task keys carry no TTL, and every other stamp site (run
    wrapper's finally, post-turn collector) lives on the dead worker — the
    scanner must start the expiry clock itself, and BEFORE the finalize: a
    terminal row is never revisited, so stamp-after would leak on failure."""
    order: list = []
    cache, pipe = _stamp_cache(order)

    async def _finalize(*_a, **_k):
        order.append("finalize")
        return {"applied": True, "run": {"status": "error"}}

    ledger = SimpleNamespace(finalize_task_run=AsyncMock(side_effect=_finalize))

    with (
        patch(
            "src.server.services.subagent_run_ledger.SubagentRunLedger",
            return_value=ledger,
        ),
        patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ),
        patch(
            "src.config.settings.get_redis_ttl_workflow_events",
            return_value=86400,
        ),
    ):
        recovered = await RecoveryScanner()._scan_task_runs(
            [dict(_ORPHAN_RUN)], lock_conn=None
        )

    assert recovered == 1
    assert order == ["stamp", "finalize"]
    stamped = [c.args for c in pipe.expire.call_args_list]
    assert stamped == [
        ("subagent:stream:t-1:abc123", 86400),
        ("subagent:events:meta:t-1:abc123", 86400),
        ("subagent:stream:t-1:run-9", 86400),
    ]


@pytest.mark.asyncio
async def test_failed_stamp_defers_the_finalize_to_the_next_scan():
    """A transient Redis failure at the stamp must NOT finalize the row —
    the open row is what makes the next scan retry the stamp; a terminal
    row would leave the dead worker's no-TTL keys immortal."""
    cache, _pipe = _stamp_cache([], fail=True)
    ledger = SimpleNamespace(finalize_task_run=AsyncMock())

    with (
        patch(
            "src.server.services.subagent_run_ledger.SubagentRunLedger",
            return_value=ledger,
        ),
        patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ),
        patch(
            "src.config.settings.get_redis_ttl_workflow_events",
            return_value=86400,
        ),
    ):
        recovered = await RecoveryScanner()._scan_task_runs(
            [dict(_ORPHAN_RUN)], lock_conn=None
        )

    assert recovered == 0
    ledger.finalize_task_run.assert_not_awaited()
