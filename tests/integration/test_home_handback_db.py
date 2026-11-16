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
    from src.server.database.home_stand_in import place_home_stand_in

    await create_computer(
        seed_user["user_id"],
        kind="docker",
        name="Cloud",
        status="running",
        provider_ref=f"handback-test-{uuid.uuid4()}",
    )
    await place_home_stand_in(seed_user["user_id"])


def _record(folder, files):
    from src.server.services.home_handback import _notice

    return _notice(folder=folder, files=files, computer_name="Mac", cloud_name="Cloud")


async def _through(user_id):
    """The watermark a hand-back starting now copies up to."""
    _, through = await home_away.begin_handback(user_id)
    return through


async def test_a_turn_on_the_cloud_owes_a_hand_back(seed_user):
    user_id = seed_user["user_id"]
    assert not await home_away.handback_owed(user_id)

    await home_away.mark_away(user_id)

    assert await home_away.handback_owed(user_id)
    assert user_id in {str(r["user_id"]) for r in await home_away.list_owed()}


async def test_the_watermark_moves_once_for_the_value_read(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    through = await _through(user_id)
    record = _record("From the cloud/2026-11-15", 1)

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


async def test_each_hand_back_keeps_what_it_carried_and_the_latest_listing_wins(seed_user):
    """One row per hand-back is both where a turn's file went and what the
    next hand-back diffs against: a file carried again is listed as it last
    went."""
    user_id = seed_user["user_id"]
    for folder, carried in [
        ("From the cloud/2026-11-15", {"b.md": [3, "1.0"], "a.md": [1, "1.0"]}),
        ("From the cloud/2026-11-16", {"a.md": [4, "2.0"]}),
    ]:
        await home_away.mark_away(user_id)
        row, through = await home_away.begin_handback(user_id)
        assert await home_away.finish_handback(
            user_id,
            previous=row["handed_back_through"],
            through=through,
            record=_record(folder, len(carried)),
            carried=carried,
        )

    assert [(h["folder"], h["files"]) for h in await home_away.list_handbacks(user_id)] == [
        ("From the cloud/2026-11-15", ["a.md", "b.md"]),
        ("From the cloud/2026-11-16", ["a.md"]),
    ]
    assert await home_away.handed_back(user_id) == {"a.md": [4, "2.0"], "b.md": [3, "1.0"]}


async def test_a_turn_after_the_mark_is_owed_again(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    through = await _through(user_id)
    await home_away.finish_handback(
        user_id, previous=None, through=through, record=None
    )

    await home_away.mark_away(user_id)

    row = await home_away.get_home_away(user_id)
    assert await home_away.handback_owed(user_id)
    assert row["handed_back_through"] == through and row["away_since"] is not None


async def test_a_hand_back_reaches_its_listing_and_leaves_later_turns_owed(seed_user):
    """A turn admitted mid-copy may have files the listing caught: the row
    reaches the listing, so its cards find them there, and the watermark
    stays where the hand-back began, so the turn stays owed."""
    from datetime import timedelta

    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    _, through = await home_away.begin_handback(user_id)
    await home_away.mark_away(user_id)
    listed_at = (await home_away.get_home_away(user_id))["last_away_at"] + timedelta(
        seconds=1
    )

    await home_away.finish_handback(
        user_id,
        previous=None,
        through=through,
        listed_at=listed_at,
        record=_record("From the cloud/2026-11-15", 1),
        carried={"a.md": [1, "1.0"]},
    )

    [handback] = await home_away.list_handbacks(user_id)
    assert handback["through"] == listed_at
    assert await home_away.handback_owed(user_id)


async def test_a_dismissed_notice_stays_dismissed_until_the_next_one(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    record = _record("From the cloud/2026-11-15", 2)
    await home_away.finish_handback(
        user_id,
        previous=None,
        through=await _through(user_id),
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
        record=_record("From the cloud/2026-11-15", 1),
        carried={"a.md": [1, "1.0"]},
        done=False,
    )

    assert await home_away.handback_owed(user_id)
    assert await home_away.handed_back(user_id) == {"a.md": [1, "1.0"]}


async def test_a_failing_hand_back_waits_and_then_gives_up(seed_user):
    user_id = seed_user["user_id"]
    await home_away.mark_away(user_id)
    given_up = {**_record(None, 0), "failed": True}

    first = await home_away.fail_handback(user_id, max_attempts=2, given_up=given_up)
    waiting = await home_away.get_home_away(user_id)
    listed = {str(r["user_id"]) for r in await home_away.list_owed()}
    second = await home_away.fail_handback(user_id, max_attempts=2, given_up=given_up)
    row = await home_away.get_home_away(user_id)

    assert (first, second) == (False, True)
    assert waiting["handback_attempts"] == 1 and waiting["next_handback_at"] is not None
    assert user_id not in listed
    assert not home_away.is_owed(row) and row["last_handback"]["failed"] is True
    assert home_away.left_on_stand_in(row["last_handback"]) == {"failed": True, "paths": []}
    # New work on the cloud owes a hand-back again, with its tries afresh.
    await home_away.mark_away(user_id)
    again = await home_away.get_home_away(user_id)
    assert home_away.is_owed(again) and again["handback_attempts"] == 0
