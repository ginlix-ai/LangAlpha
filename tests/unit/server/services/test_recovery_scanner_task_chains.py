"""The scanner's retroactive task-chain heal (M4-C).

The truncation paths repair their own thread transactionally. This sweep is
the backstop for damage that predates them and for any deletion path that
escapes the guard, so what matters is that it runs at all — specifically on
an IDLE deployment, which is exactly where orphaned task rows accumulate
unnoticed and where the scanner's open-run early-return would skip it.
"""

from unittest.mock import AsyncMock, patch

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
