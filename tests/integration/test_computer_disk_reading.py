"""The disk reading write against a real database: the newest observation of
the machine's current sandbox wins, whatever order the writes land in."""

from datetime import datetime, timedelta, timezone

import pytest

from src.server.database.computer import (
    clear_computer_disk,
    create_computer,
    get_computer,
    record_computer_disk,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _running_computer(seed_user, test_db_pool, sandbox_id):
    computer = await create_computer(seed_user["user_id"], kind="docker", name="Disk")
    computer_id = str(computer["computer_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET status='running', provider_ref=%s WHERE computer_id=%s",
            (sandbox_id, computer_id),
        )
    return computer_id


def _reading(used):
    return {"total_bytes": 1000, "used_bytes": used, "free_bytes": 1000 - used}


async def test_an_older_observation_landing_late_loses(seed_user, test_db_pool):
    computer_id = await _running_computer(seed_user, test_db_pool, "sb-now")
    now = datetime.now(timezone.utc)
    assert await record_computer_disk(
        computer_id, sandbox_id="sb-now", observed_at=now, **_reading(900)
    )
    # A breakdown whose df ran first finishes its du after the newer reading.
    assert (
        await record_computer_disk(
            computer_id,
            sandbox_id="sb-now",
            observed_at=now - timedelta(seconds=20),
            **_reading(100),
        )
        is None
    )
    row = await get_computer(computer_id)
    assert row["disk_used_bytes"] == 900


async def test_a_reading_of_a_replaced_sandbox_is_dropped(seed_user, test_db_pool):
    computer_id = await _running_computer(seed_user, test_db_pool, "sb-new")
    assert (
        await record_computer_disk(
            computer_id,
            sandbox_id="sb-old",
            observed_at=datetime.now(timezone.utc),
            **_reading(500),
        )
        is None
    )
    assert (await get_computer(computer_id))["disk_measured_at"] is None


async def test_clearing_drops_only_the_named_sandboxs_reading(seed_user, test_db_pool):
    """A stopped spec change destroys the sandbox the ref still names, so its
    reading has to go explicitly; a reading of any other sandbox stays."""
    computer_id = await _running_computer(seed_user, test_db_pool, "sb-now")
    assert await record_computer_disk(
        computer_id, sandbox_id="sb-now", observed_at=datetime.now(timezone.utc), **_reading(900)
    )
    assert await clear_computer_disk(computer_id, sandbox_id="sb-other") is False
    assert (await get_computer(computer_id))["disk_used_bytes"] == 900

    assert await clear_computer_disk(computer_id, sandbox_id="sb-now") is True
    row = await get_computer(computer_id)
    assert row["provider_ref"] == "sb-now"
    assert (row["disk_total_bytes"], row["disk_measured_at"], row["disk_sandbox_ref"]) == (None, None, None)


@pytest.mark.parametrize("status,kept", [("stopping", True), ("starting", False)])
async def test_only_a_running_or_stopping_machine_takes_a_reading(
    seed_user, test_db_pool, status, kept
):
    """A stop reads the disk one last time; a replacement starting on the same
    ref must not have a late reading undo its clear."""
    computer_id = await _running_computer(seed_user, test_db_pool, "sb-now")
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET status=%s WHERE computer_id=%s", (status, computer_id)
        )
    recorded = await record_computer_disk(
        computer_id, sandbox_id="sb-now", observed_at=datetime.now(timezone.utc), **_reading(900)
    )
    assert (recorded is not None) is kept
