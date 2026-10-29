"""The spec change claim on the computer row, against a real database.

The claim, the heartbeat and the settle are single conditional UPDATEs, so
these run them on Postgres rather than on string-matched mocks: a peer is
turned away before it can tear anything down, a failed change puts the tier
back, a change that went stale can be taken over while one that keeps
reporting in cannot, a takeover rebuilds rather than answering unchanged, and
a runner that lost its claim stops before teardown.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import AgentConfig, SkillsConfig
from ptc_agent.config.core import (
    FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig,
)
from src.server.database.computer import create_computer, get_computer
from src.server.database.computer_spec_change import claim_computer_spec_change
from src.server.database.sql_fences import SPEC_CHANGE_STALE_SECONDS
from src.server.services.computer_errors import ComputerBusyError, SpecChangeLostError
from src.server.services.computer_manager import ComputerManager
from src.server.services.spec_change import SPEC_CHANGE_IN_PROGRESS

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _manager():
    return ComputerManager(
        AgentConfig(
            security=SecurityConfig(), logging=LoggingConfig(), mcp=MCPConfig(),
            sandbox=SandboxConfig(provider="docker"), filesystem=FilesystemConfig(),
            skills=SkillsConfig(enabled=False),
        )
    )


async def _stopped_computer(seed_user, test_db_pool, name):
    computer = await create_computer(seed_user["user_id"], kind="docker", name=name)
    computer_id = str(computer["computer_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET status='stopped', provider_ref='spec-test' WHERE computer_id=%s",
            (computer_id,),
        )
    return computer_id


async def test_a_peer_is_turned_away_before_it_tears_anything_down(seed_user, test_db_pool):
    first, second = _manager(), _manager()
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec race")
    entered, release, peer_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def first_teardown(*_args, **_kwargs):
        entered.set()
        await release.wait()

    async def second_teardown(*_args, **_kwargs):
        peer_entered.set()

    first._replace_stopped_sandbox = first_teardown
    second._replace_stopped_sandbox = second_teardown
    task = asyncio.create_task(first.set_computer_spec(computer_id, "performance"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        with pytest.raises(ComputerBusyError, match=SPEC_CHANGE_IN_PROGRESS):
            await second.set_computer_spec(computer_id, "max")
        assert not peer_entered.is_set()
        release.set()
        await asyncio.wait_for(task, 5)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    row = await get_computer(computer_id)
    assert row["resource_tier"] == "performance"
    assert row["spec_change"]["state"] == "succeeded"


async def test_a_failed_change_puts_the_last_good_tier_back(seed_user, test_db_pool):
    manager = _manager()
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec revert")

    async def teardown_ok(*_args, **_kwargs):
        return None

    async def teardown_refused(*_args, **_kwargs):
        raise RuntimeError("provider refused teardown")

    manager._replace_stopped_sandbox = teardown_ok
    await manager.set_computer_spec(computer_id, "performance")
    manager._replace_stopped_sandbox = teardown_refused
    with pytest.raises(RuntimeError, match="provider refused teardown"):
        await manager.set_computer_spec(computer_id, "max")

    row = await get_computer(computer_id)
    assert row["resource_tier"] == "performance"
    assert row["spec_change"]["state"] == "failed"
    assert row["spec_change"]["target_tier"] == "max"


async def _age_claim(test_db_pool, computer_id, *keys):
    """Move the record's clocks past the stale window, as a dead worker's would be."""
    async with test_db_pool.connection() as conn:
        for key in keys:
            await conn.execute(
                "UPDATE computers SET spec_change = jsonb_set(spec_change, %s, "
                "to_jsonb(NOW() - make_interval(secs => %s))) WHERE computer_id=%s",
                ([key], SPEC_CHANGE_STALE_SECONDS + 60, computer_id),
            )


async def test_only_a_stale_change_can_be_taken_over(seed_user, test_db_pool):
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec stale")
    held = await claim_computer_spec_change(computer_id, target_tier="performance")
    assert held is not None
    assert await claim_computer_spec_change(computer_id, target_tier="max") is None

    # An old start alone is not stale: the worker may still be reporting in.
    await _age_claim(test_db_pool, computer_id, "started_at")
    assert await claim_computer_spec_change(computer_id, target_tier="max") is None

    await _age_claim(test_db_pool, computer_id, "heartbeat_at")
    taken = await claim_computer_spec_change(computer_id, target_tier="max")
    assert taken is not None
    assert taken["spec_change"]["claim_id"] != held["spec_change"]["claim_id"]
    assert taken["spec_change"]["target_tier"] == "max"


async def test_a_heartbeat_keeps_a_long_change_from_being_taken_over(
    seed_user, test_db_pool
):
    """A change that outlives the stale window is still owned while its
    runner heartbeats; once the runner is gone the next stamp never comes
    and the row can be taken over."""
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec long")
    held = await claim_computer_spec_change(computer_id, target_tier="performance")
    claim_id = held["spec_change"]["claim_id"]
    manager = _manager()
    manager.spec_change_heartbeat_s = 0.05

    async with manager._spec_change_heartbeat(computer_id, claim_id):
        await _age_claim(test_db_pool, computer_id, "started_at", "heartbeat_at")
        await asyncio.sleep(0.3)
        assert await claim_computer_spec_change(computer_id, target_tier="max") is None
        row = await get_computer(computer_id)
        assert row["spec_change"]["claim_id"] == claim_id
        assert not row["spec_change_stale"]

    await _age_claim(test_db_pool, computer_id, "heartbeat_at")
    taken = await claim_computer_spec_change(computer_id, target_tier="max")
    assert taken is not None and taken["spec_change"]["claim_id"] != claim_id


async def test_a_takeover_rebuilds_rather_than_answering_unchanged(
    seed_user, test_db_pool
):
    """The dead worker persisted its target tier and never rebuilt. A retry
    to that tier is not the no-op the row makes it look like: the claim
    puts the tier back where the change started and the change runs."""
    manager = _manager()
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec phantom")
    dead = await claim_computer_spec_change(computer_id, target_tier="performance")
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET resource_tier='performance' WHERE computer_id=%s",
            (computer_id,),
        )
    await _age_claim(test_db_pool, computer_id, "started_at", "heartbeat_at")
    assert (await get_computer(computer_id))["spec_change_stale"]

    rebuilt = []

    async def teardown(binding, *_args, **_kwargs):
        rebuilt.append(binding.resource_tier)

    manager._replace_stopped_sandbox = teardown
    await manager.set_computer_spec(computer_id, "performance")

    # The stopped path is handed the tier the machine is really at.
    assert rebuilt == ["standard"]
    row = await get_computer(computer_id)
    assert row["resource_tier"] == "performance"
    assert row["spec_change"]["state"] == "succeeded"
    assert row["spec_change"]["from_tier"] == "standard"
    assert row["spec_change"]["claim_id"] != dead["spec_change"]["claim_id"]


async def test_a_takeover_back_to_the_old_tier_still_rebuilds(
    seed_user, test_db_pool
):
    """The dead worker may have recreated the machine at its target before it
    died. A retry back to the old tier then reads as unchanged once the claim
    has reverted the row, yet the machine is at the target size: the takeover
    has to rebuild it."""
    manager = _manager()
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec back")
    await claim_computer_spec_change(computer_id, target_tier="performance")
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET resource_tier='performance' WHERE computer_id=%s",
            (computer_id,),
        )
    await _age_claim(test_db_pool, computer_id, "started_at", "heartbeat_at")

    rebuilt = []

    async def teardown(binding, *_args, **_kwargs):
        rebuilt.append(binding.resource_tier)

    manager._replace_stopped_sandbox = teardown
    await manager.set_computer_spec(computer_id, "standard")

    assert rebuilt, "the takeover answered unchanged over a machine of unknown size"
    row = await get_computer(computer_id)
    assert row["resource_tier"] == "standard"
    assert row["spec_change"]["state"] == "succeeded"
    assert row["spec_change"]["took_over"] is True


async def test_a_runner_that_lost_its_claim_stops_before_teardown(
    seed_user, test_db_pool
):
    """The backup ran past the stale window and a peer took the change over.
    The ownership check ahead of the destroy misses on the real row, so the
    sandbox the peer is about to back up is left intact and the row stays
    the peer's."""
    manager = _manager()
    computer_id = await _stopped_computer(seed_user, test_db_pool, "Spec lost")
    probe = MagicMock(initialize=AsyncMock(), stop=AsyncMock(), sandbox=object())
    manager._sync_machine_assets = AsyncMock()
    manager._destroy_sandbox = AsyncMock()
    peer = {}

    async def backup_then_lose(*_args, **_kwargs):
        await _age_claim(test_db_pool, computer_id, "started_at", "heartbeat_at")
        peer.update(await claim_computer_spec_change(computer_id, target_tier="max"))
        return 1

    manager._backup_machine_files_to_db = backup_then_lose
    with patch("src.server.services.computer_manager._spec.Session", return_value=probe):
        with pytest.raises(SpecChangeLostError):
            await manager.set_computer_spec(computer_id, "performance")

    manager._destroy_sandbox.assert_not_awaited()
    probe.stop.assert_awaited_once()
    row = await get_computer(computer_id)
    assert row["status"] == "stopped"
    assert row["spec_change"]["claim_id"] == peer["spec_change"]["claim_id"]
    # The loser's settle matched nothing: the peer's change is still running
    # from the tier the takeover reverted to.
    assert row["spec_change"]["state"] == "in_progress"
    assert row["resource_tier"] == "standard"
