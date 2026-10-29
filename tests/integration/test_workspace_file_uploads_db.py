"""The holds on browser uploads that only Postgres can keep, on a real Postgres.

A user's pulls take turns, so one user's backlog cannot hold every worker's
pull slots, and the room an upload needs is counted on its computer's disk,
which every workspace on that computer shares.
"""

import uuid
from datetime import timedelta

import pytest

from src.server.database import workspace as workspaces
from src.server.database import workspace_file_uploads as db
from src.server.database.computer import create_computer
from src.server.database.user import create_user

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

LEASE = timedelta(minutes=5)


async def _workspace(user_id, computer_id, name) -> str:
    row = await workspaces.create_workspace_on_computer(user_id, name, computer_id)
    return str(row["workspace_id"])


async def _setup(user_id, name="Research") -> tuple[str, str]:
    computer_id = str(
        (await create_computer(user_id, kind="docker", name=name))["computer_id"]
    )
    return computer_id, await _workspace(user_id, computer_id, name)


async def _upload(
    user_id, workspace_id, size=100, free_bytes=None, keep_free_bytes=0
) -> str:
    upload_id = str(uuid.uuid4())
    await db.create_upload(
        upload_id=upload_id,
        user_id=user_id,
        workspace_id=workspace_id,
        path=f"data/{upload_id}.bin",
        size=size,
        part_size=size,
        part_count=1,
        sha256=None,
        object_key=f"uploads/{user_id}/{upload_id}",
        store_upload_id="store",
        sandbox_id=None,
        max_open=8,
        free_bytes=free_bytes,
        keep_free_bytes=keep_free_bytes,
    )
    return upload_id


async def _set(pool, upload_id, sql) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            f"UPDATE workspace_file_uploads SET {sql} WHERE upload_id = %s",
            (upload_id,),
        )


async def _assembled(pool, user_id, workspace_id) -> str:
    upload_id = await _upload(user_id, workspace_id)
    await _set(pool, upload_id, "state = 'completing'")
    return upload_id


async def test_a_users_second_pull_is_left_due_while_the_first_is_live(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    _, workspace_id = await _setup(user_id)
    first = await _assembled(test_db_pool, user_id, workspace_id)
    second = await _assembled(test_db_pool, user_id, workspace_id)

    assert (await db.start_pull(first, LEASE))["lease_until"] is not None
    left = await db.start_pull(second, LEASE)
    assert (left["state"], left["lease_until"], left["attempts"]) == (
        "pulling",
        None,
        0,
    )

    assert await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5) == []
    # Once the first is in, the second is the sweep's to claim.
    await _set(test_db_pool, first, "state = 'done', lease_until = NULL")
    claimed = await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5)
    assert [(row["upload_id"], row["attempts"]) for row in claimed] == [(second, 1)]


async def test_only_the_attempt_holding_a_pull_may_mark_it_placing(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    _, workspace_id = await _setup(user_id)
    upload_id = await _assembled(test_db_pool, user_id, workspace_id)
    started = await db.start_pull(upload_id, LEASE)

    assert not await db.mark_placing(upload_id, started["attempts"] + 1, 4242)
    assert (await db.get_upload(upload_id))["placing_inode"] is None
    assert await db.mark_placing(upload_id, started["attempts"], 4242)
    # The next attempt's claim carries the mark, which is how it knows.
    await _set(test_db_pool, upload_id, "lease_until = NOW() - interval '1 second'")
    (claimed,) = await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5)
    assert claimed["placing_inode"] == 4242


async def test_a_row_an_attempt_began_placing_has_one_more_attempt(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    _, workspace_id = await _setup(user_id)
    placing = await _assembled(test_db_pool, user_id, workspace_id)
    plain = await _assembled(test_db_pool, user_id, workspace_id)
    for upload_id in (placing, plain):
        await db.start_pull(upload_id, None)
        await _set(test_db_pool, upload_id, "attempts = 5")
    await _set(test_db_pool, placing, "placing_inode = 4242")

    failed = await db.fail_exhausted_pulls(5, "Upload failed", 10)
    assert [row["upload_id"] for row in failed] == [plain]
    (claimed,) = await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5)
    assert (claimed["upload_id"], claimed["attempts"]) == (placing, 6)

    await _set(test_db_pool, placing, "lease_until = NOW() - interval '1 second'")
    assert await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5) == []
    failed = await db.fail_exhausted_pulls(5, "Upload failed", 10)
    assert [row["upload_id"] for row in failed] == [placing]


async def test_a_pull_waiting_its_turn_counts_as_use_once_per_interval(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    _, workspace_id = await _setup(user_id)
    live = await _assembled(test_db_pool, user_id, workspace_id)
    waiting = await _assembled(test_db_pool, user_id, workspace_id)
    for upload_id in (live, waiting):
        await _set(test_db_pool, upload_id, "sandbox_id = 'sb-1'")
        assert await db.start_pull(upload_id, LEASE)
    every = timedelta(seconds=60)

    touched = await db.claim_waiting_touches(every, 10)
    assert [row["upload_id"] for row in touched] == [waiting]
    assert await db.claim_waiting_touches(every, 10) == []
    await _set(test_db_pool, waiting, "touched_at = NOW() - interval '61 seconds'")
    assert [row["upload_id"] for row in await db.claim_waiting_touches(every, 10)] == [waiting]


async def test_the_sweep_claims_one_pull_per_user(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    other = (
        await create_user(user_id=f"{user_id}-b", email="b@example.com", name="B")
    )["user_id"]
    _, workspace_id = await _setup(user_id)
    _, other_workspace = await _setup(other)
    mine = [await _assembled(test_db_pool, user_id, workspace_id) for _ in range(3)]
    theirs = await _assembled(test_db_pool, other, other_workspace)
    for upload_id in [*mine, theirs]:
        await db.start_pull(upload_id, None)

    claimed = await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5)

    assert sorted(row["user_id"] for row in claimed) == sorted([user_id, other])
    assert await db.claim_lapsed("pulling", lease=LEASE, limit=4, max_attempts=5) == []


async def test_room_is_counted_across_the_workspaces_on_one_computer(
    seed_user, test_db_pool
):
    user_id = seed_user["user_id"]
    computer_id, research = await _setup(user_id)
    beside = await _workspace(user_id, computer_id, "Beside")
    _, elsewhere = await _setup(user_id, "Elsewhere")
    await _upload(user_id, research, size=600)

    with pytest.raises(db.NotEnoughRoom) as short:
        await _upload(user_id, beside, size=500, free_bytes=1049, keep_free_bytes=50)
    assert short.value.free_bytes == 449

    await _upload(user_id, beside, size=500, free_bytes=1150, keep_free_bytes=50)
    await _upload(user_id, elsewhere, size=500, free_bytes=600, keep_free_bytes=50)
