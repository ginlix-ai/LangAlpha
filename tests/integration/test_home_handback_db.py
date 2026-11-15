"""Home's hand-back ledger, on a real Postgres.

What is owed, the guarded watermark that keeps two writers from handing back
the same files twice, the record of where each hand-back's files went, and a
failing hand-back's delays and giving up.
"""

import pytest
import pytest_asyncio

from src.server.database import home_away

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _stand_in(seed_user):
    """Home's stand-in on a cloud computer, which a turn's mark requires."""
    import uuid

    from src.server.database.computer import create_computer
    from src.server.database.workspace import place_home_stand_in

    computer = await create_computer(
        seed_user["user_id"],
        kind="docker",
        name="Cloud",
        status="running",
        provider_ref=f"handback-test-{uuid.uuid4()}",
    )
    await place_home_stand_in(seed_user["user_id"], str(computer["computer_id"]))


async def test_a_turn_on_the_cloud_owes_a_hand_back(seed_user):
    user_id = seed_user["user_id"]
    assert not await home_away.handback_owed(user_id)

    await home_away.mark_away(user_id)

    assert await home_away.handback_owed(user_id)
    assert user_id in {str(r["user_id"]) for r in await home_away.list_owed()}


async def test_the_watermark_moves_once_for_the_value_read(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    through = await home_away.database_now()
    record = {
        "handback_id": "7b0b8f0e-4a8e-4f0e-9a43-6f1a2b3c4d5e",
        "folder": "From the cloud/2026-11-15",
        "files": 1,
    }

    first = await home_away.finish_handback(
        user_id, previous=None, through=through, record=record, carried={"a.md": [3, "1.0"]}
    )
    # A second writer that read the same watermark changes nothing.
    second = await home_away.finish_handback(
        user_id, previous=None, through=through, record=record, carried={"a.md": [3, "1.0"]}
    )

    row = await home_away.get_home_away(user_id)
    assert (first, second) == (True, False)
    assert row["handed_back_through"] == through and row["away_since"] is None
    assert row["last_handback"]["folder"] == "From the cloud/2026-11-15"
    assert not await home_away.handback_owed(user_id)
    assert [h["files"] for h in await home_away.list_handbacks(user_id)] == [["a.md"]]
    assert await home_away.handed_back(user_id) == {"a.md": [3, "1.0"]}


async def test_a_turn_after_the_mark_is_owed_again(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    through = await home_away.database_now()
    await home_away.finish_handback(
        user_id, previous=None, through=through, record=None
    )

    await home_away.mark_away(user_id)

    row = await home_away.get_home_away(user_id)
    assert await home_away.handback_owed(user_id)
    assert row["handed_back_through"] == through and row["away_since"] is not None


async def test_a_dismissed_notice_stays_dismissed_until_the_next_one(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    record = {
        "handback_id": "2c1d7a8e-0b7f-4c1e-8d0a-1f2e3d4c5b6a",
        "folder": "From the cloud/2026-11-15",
        "files": 2,
    }
    await home_away.finish_handback(
        user_id,
        previous=None,
        through=await home_away.database_now(),
        record=record,
        carried={"a.md": [1, "1.0"], "b.md": [2, "2.0"]},
    )

    await home_away.mark_handback_seen(user_id, "some-other-notice")
    unseen = await home_away.get_home_away(user_id)
    await home_away.mark_handback_seen(user_id, record["handback_id"])
    seen = await home_away.get_home_away(user_id)

    assert unseen["handback_seen_at"] is None
    assert seen["handback_seen_at"] is not None


async def test_a_pass_with_no_room_for_everything_stays_owed(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    row, through = await home_away.begin_handback(user_id)

    await home_away.finish_handback(
        user_id,
        previous=None,
        through=through,
        record=None,
        carried={"a.md": [1, "1.0"]},
        done=False,
    )

    assert await home_away.handback_owed(user_id)
    assert await home_away.handed_back(user_id) == {"a.md": [1, "1.0"]}


async def test_a_failing_hand_back_waits_and_then_gives_up(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    given_up = {"handback_id": "5d3c2b1a-0f9e-4d8c-b7a6-1e2f3a4b5c6d", "failed": True}

    first = await home_away.fail_handback(user_id, max_attempts=2, given_up=given_up)
    waiting = await home_away.get_home_away(user_id)
    listed = {str(r["user_id"]) for r in await home_away.list_owed()}
    second = await home_away.fail_handback(user_id, max_attempts=2, given_up=given_up)
    row = await home_away.get_home_away(user_id)

    assert (first, second) == (False, True)
    assert waiting["handback_attempts"] == 1 and waiting["next_handback_at"] is not None
    assert user_id not in listed
    assert not home_away.is_owed(row) and row["last_handback"]["failed"] is True
    assert home_away.left_on_stand_in(row) == {"failed": True, "paths": []}
    # New work on the cloud owes a hand-back again, with its tries afresh.
    await home_away.mark_away(user_id)
    again = await home_away.get_home_away(user_id)
    assert home_away.is_owed(again) and again["handback_attempts"] == 0
