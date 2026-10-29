"""The host-outage clock on the computer row, against a real Postgres.

It is stamped only for the sandbox the row still names, kept across refusals,
and cleared when that sandbox comes up: by every write that marks the computer
running (a status transition, the workspace mirror of one, the provider-ref
bind) and by a reconnect that finds the row already running. A rebuild the
outage authorized binds only while that outage is still running, so whichever
of it and the old sandbox resuming writes the row second loses.
"""

from datetime import timedelta

import pytest

from src.server.database.computer import (
    clear_computer_host_unavailable,
    create_computer,
    stamp_computer_host_unavailable,
    try_bind_computer_provider_ref,
    update_computer_status,
)
from src.server.database.workspace import (
    create_workspace_on_computer,
    update_workspace_status,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _computer(seed_user, pool, *, status="starting", provider_ref="sb-old"):
    computer = await create_computer(seed_user["user_id"], kind="docker", name="Host")
    computer_id = str(computer["computer_id"])
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET status = %s, provider_ref = %s WHERE computer_id = %s",
            (status, provider_ref, computer_id),
        )
    return computer_id


async def _since(pool, computer_id):
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT host_unavailable_since FROM computers WHERE computer_id = %s",
            (computer_id,),
        )
        return (await result.fetchone())["host_unavailable_since"]


async def _backdate(pool, computer_id, minutes):
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET host_unavailable_since = NOW() - %s"
            " WHERE computer_id = %s",
            (timedelta(minutes=minutes), computer_id),
        )


async def test_only_the_sandbox_the_row_names_starts_the_clock(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, test_db_pool)

    assert await stamp_computer_host_unavailable(computer_id, "sb-other") is None
    assert await _since(test_db_pool, computer_id) is None

    first = await stamp_computer_host_unavailable(computer_id, "sb-old")
    assert first is not None
    since, unavailable_for = first
    assert unavailable_for < timedelta(seconds=5)
    assert await _since(test_db_pool, computer_id) == since


async def test_a_later_refusal_keeps_the_first_stamp(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, test_db_pool)
    await stamp_computer_host_unavailable(computer_id, "sb-old")
    await _backdate(test_db_pool, computer_id, 90)
    stamped = await _since(test_db_pool, computer_id)

    since, unavailable_for = await stamp_computer_host_unavailable(
        computer_id, "sb-old"
    )

    assert unavailable_for >= timedelta(minutes=90)
    assert since == stamped
    assert await _since(test_db_pool, computer_id) == stamped


async def test_running_clears_it_and_a_revert_to_stopped_keeps_it(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, test_db_pool)
    await stamp_computer_host_unavailable(computer_id, "sb-old")

    await update_computer_status(computer_id, "stopped", expected="starting")
    assert await _since(test_db_pool, computer_id) is not None

    await update_computer_status(computer_id, "running", expected="stopped")
    assert await _since(test_db_pool, computer_id) is None


async def test_the_workspace_mirror_of_running_clears_it(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, test_db_pool)
    workspace = await create_workspace_on_computer(
        seed_user["user_id"], "Host project", computer_id
    )
    await stamp_computer_host_unavailable(computer_id, "sb-old")

    await update_workspace_status(str(workspace["workspace_id"]), "running")

    assert await _since(test_db_pool, computer_id) is None


async def test_binding_the_rebuilt_sandbox_clears_it(seed_user, test_db_pool):
    computer_id = await _computer(seed_user, test_db_pool)
    await stamp_computer_host_unavailable(computer_id, "sb-old")

    row = await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="sb-new",
        expected_previous_provider_ref="sb-old",
        platform_secret_version=0,
    )

    assert row is not None and row["provider_ref"] == "sb-new"
    assert await _since(test_db_pool, computer_id) is None
    # The replaced sandbox's refusals no longer reach the row.
    assert await stamp_computer_host_unavailable(computer_id, "sb-old") is None


async def test_a_reconnect_clears_only_the_sandbox_the_row_names(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, test_db_pool, status="running")
    await stamp_computer_host_unavailable(computer_id, "sb-old")

    # It answers with the sandbox the row names now.
    assert await clear_computer_host_unavailable(computer_id, "sb-other") == "sb-old"
    assert await _since(test_db_pool, computer_id) is not None

    assert await clear_computer_host_unavailable(computer_id, "sb-old") == "sb-old"
    assert await _since(test_db_pool, computer_id) is None
    # Asked again with no clock running, it still says the sandbox is the row's.
    assert await clear_computer_host_unavailable(computer_id, "sb-old") == "sb-old"


async def test_a_reconnect_against_a_row_naming_no_sandbox_answers_none(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, test_db_pool, provider_ref=None)

    assert await clear_computer_host_unavailable(computer_id, "sb-first") is None


async def _rebuild_bind(computer_id, since):
    return await try_bind_computer_provider_ref(
        computer_id,
        provider_ref="sb-new",
        expected_previous_provider_ref="sb-old",
        platform_secret_version=0,
        expected_host_unavailable_since=since,
    )


async def test_a_rebuild_loses_to_the_old_sandbox_resuming_first(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, test_db_pool)
    since, _ = await stamp_computer_host_unavailable(computer_id, "sb-old")

    assert await clear_computer_host_unavailable(computer_id, "sb-old") == "sb-old"
    assert await _rebuild_bind(computer_id, since) is None

    async with test_db_pool.connection() as conn:
        result = await conn.execute(
            "SELECT provider_ref FROM computers WHERE computer_id = %s",
            (computer_id,),
        )
        assert (await result.fetchone())["provider_ref"] == "sb-old"


async def test_a_rebuild_authorized_by_an_earlier_outage_loses_to_a_new_one(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, test_db_pool)
    since, _ = await stamp_computer_host_unavailable(computer_id, "sb-old")
    await clear_computer_host_unavailable(computer_id, "sb-old")
    await stamp_computer_host_unavailable(computer_id, "sb-old")

    assert await _rebuild_bind(computer_id, since) is None


async def test_the_old_sandbox_resuming_after_the_rebuild_bound_is_told_so(
    seed_user, test_db_pool
):
    computer_id = await _computer(seed_user, test_db_pool)
    since, _ = await stamp_computer_host_unavailable(computer_id, "sb-old")

    row = await _rebuild_bind(computer_id, since)

    assert row is not None and row["provider_ref"] == "sb-new"
    assert await clear_computer_host_unavailable(computer_id, "sb-old") == "sb-new"


async def test_a_failed_recovery_releases_only_its_own_start(seed_user, test_db_pool):
    """A recovery on another worker bound its own sandbox while this start was
    still recovering; this start's revert must leave that one alone."""
    computer_id = await _computer(seed_user, test_db_pool, provider_ref="sb-new")

    released = await update_computer_status(
        computer_id,
        "stopped",
        expected="starting",
        expected_provider_ref="sb-old",
        require_provider_ref=True,
    )
    assert released is None

    released = await update_computer_status(
        computer_id,
        "stopped",
        expected="starting",
        expected_provider_ref="sb-new",
        require_provider_ref=True,
    )
    assert released is not None and released["status"] == "stopped"
