"""Which computers the idle sweep never stops, and what each reader of that
rule makes of a computer no provider knows."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.data_token_sweep import keeps_running
from src.server.services.idle_policy import spared_by_idle_sweep


@pytest.mark.parametrize(
    ("computer", "spared"),
    [
        ({"kind": "daytona", "is_always_on": True}, True),
        ({"kind": "ssh"}, True),
        ({"kind": "daytona"}, False),
        # Always-on is read before the kind, so one no provider knows is spared.
        ({"kind": "retired", "is_always_on": True}, True),
    ],
)
def test_the_sweep_spares_an_always_on_computer_and_a_machine_of_the_users_own(
    computer, spared
):
    assert spared_by_idle_sweep(computer) is spared


def test_a_kind_no_provider_knows_is_left_to_the_caller():
    with pytest.raises(ValueError):
        spared_by_idle_sweep({"kind": "retired"})


@pytest.mark.asyncio
async def test_the_data_token_sweep_writes_nothing_for_a_kind_no_provider_knows():
    with patch(
        "src.server.services.computer_manager.ComputerManager.get_instance",
        return_value=MagicMock(),
    ) as manager:
        assert await keeps_running({"kind": "retired", "computer_id": "c-1"}) is False
        assert await keeps_running({"kind": "ssh", "computer_id": "c-1"}) is True
    manager.assert_not_called()


@pytest.mark.asyncio
async def test_the_data_token_sweep_reads_the_reapers_limit_for_any_other_computer():
    manager = MagicMock(_idle_timeout_for=MagicMock(return_value=600))
    runs = MagicMock(has_active_tasks_for_computer=AsyncMock(return_value=False))
    with (
        patch(
            "src.server.services.computer_manager.ComputerManager.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=runs,
        ),
    ):
        assert await keeps_running({"kind": "daytona", "computer_id": "c-1"}) is False
    manager._idle_timeout_for.assert_called_once()
