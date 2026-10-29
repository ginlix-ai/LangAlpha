"""The recreate's sibling flag against a real database: every other live
project on the machine is held back from pruning, and only while the machine
still names the sandbox the recreate is replacing."""

import pytest

from src.server.database.computer import create_computer
from src.server.database.workspace import (
    create_workspace_on_computer,
    files_restore_incomplete,
    flag_sibling_restores_pending,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _machine(seed_user, test_db_pool, names):
    computer = await create_computer(seed_user["user_id"], kind="docker", name="Flag")
    computer_id = str(computer["computer_id"])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE computers SET status='running', provider_ref='sb-old' WHERE computer_id=%s",
            (computer_id,),
        )
    ids = []
    for name in names:
        ws = await create_workspace_on_computer(seed_user["user_id"], name, computer_id)
        ids.append(str(ws["workspace_id"]))
    return computer_id, ids


async def test_every_sibling_but_the_recovering_project_is_flagged(
    seed_user, test_db_pool
):
    computer_id, (own, sib_a, sib_b) = await _machine(
        seed_user, test_db_pool, ["own", "a", "b"]
    )
    flagged = await flag_sibling_restores_pending(
        computer_id, except_workspace_id=own, expected_provider_ref="sb-old"
    )
    assert flagged == 2
    assert not await files_restore_incomplete(own)
    assert await files_restore_incomplete(sib_a)
    assert await files_restore_incomplete(sib_b)


async def test_a_recreate_that_lost_the_race_flags_nothing(seed_user, test_db_pool):
    computer_id, (own, sib) = await _machine(seed_user, test_db_pool, ["own", "sib"])
    flagged = await flag_sibling_restores_pending(
        computer_id, except_workspace_id=own, expected_provider_ref="sb-someone-else"
    )
    assert flagged == 0
    assert not await files_restore_incomplete(sib)
