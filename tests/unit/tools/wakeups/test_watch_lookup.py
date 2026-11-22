"""Which thread row a watch of an order placed elsewhere follows."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.tools.wakeups import orders as order_wakeups
from src.tools.wakeups.schedule import WatchAsk
from tests.unit.tools.wakeups.conftest import NOW, THREAD


def _ask(**over):
    return WatchAsk(
        **{
            "label": None,
            "order_id": "1001",
            "timezone": None,
            "brokerage": "robinhood",
            "account": "acct_a",
            **over,
        }
    )


def _row(**over):
    return {"attempt_id": "a1", "server": "robinhood", "vendor": "robinhood", "account_ref": "acct_a", **over}


@pytest.fixture
def lookup(monkeypatch):
    calls = {}

    def set_row(row):
        monkeypatch.setattr(
            order_wakeups.attempts_db, "find_open_thread_order", AsyncMock(return_value=row)
        )
        calls["watch"] = AsyncMock(return_value=_row(attempt_id="w1"))
        monkeypatch.setattr(order_wakeups, "_watch", calls["watch"])
        monkeypatch.setattr(order_wakeups, "readback_on", lambda: True)
        monkeypatch.setattr(order_wakeups, "_followed", lambda attempt, ask, now: (attempt, None))

    calls["set_row"] = set_row
    return calls


@pytest.mark.asyncio
async def test_a_row_at_the_named_brokerage_and_account_is_followed(lookup):
    lookup["set_row"](_row())
    attempt, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW)
    assert attempt["attempt_id"] == "a1"
    lookup["watch"].assert_not_awaited()


@pytest.mark.asyncio
async def test_the_lookup_is_scoped_to_the_named_brokerage_and_account(lookup):
    # Order ids are unique within an account only; the query does the match.
    lookup["set_row"](None)
    attempt, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(account=" acct_a "), NOW)
    find = order_wakeups.attempts_db.find_open_thread_order
    find.assert_awaited_once_with(THREAD, "1001", brokerage="robinhood", account_ref="acct_a")
    assert attempt["attempt_id"] == "w1"
    lookup["watch"].assert_awaited_once()


class _Room:
    def __init__(self, left, followed=()):
        self.left = left
        self.taken = 0
        self.followed = set(followed)

    async def follows(self, attempt_id):
        return attempt_id in self.followed

    async def take(self):
        self.taken += 1
        if self.left <= 0:
            return False
        self.left -= 1
        return True

    def give(self):
        self.left += 1

    def hold(self, attempt_id):
        self.followed.add(attempt_id)


@pytest.mark.asyncio
async def test_a_full_conversation_refuses_a_watch_before_reading(lookup):
    from src.tools.wakeups.schedule import Refused

    lookup["set_row"](None)
    room = _Room(0)
    with pytest.raises(Refused) as e:
        await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=room)
    assert e.value.lines == ["- this conversation already has 20 pending wakeups; cancel one first."]
    lookup["watch"].assert_not_awaited()


@pytest.mark.asyncio
async def test_an_order_the_thread_holds_needs_no_room(lookup):
    lookup["set_row"](_row())
    room = _Room(0)
    attempt, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=room)
    assert attempt["attempt_id"] == "a1"
    assert room.taken == 0


@pytest.mark.asyncio
async def test_a_watch_the_thread_holds_is_read_again_before_it_is_followed(lookup):
    # A watch nothing waited on stopped being read, so its row may be stale.
    lookup["set_row"](_row(action="watch"))
    attempt, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW)
    assert attempt["attempt_id"] == "w1"
    lookup["watch"].assert_awaited_once()


@pytest.mark.asyncio
async def test_a_refused_watch_gives_its_slot_back(lookup):
    from src.tools.wakeups.schedule import Refused

    lookup["set_row"](None)
    lookup["watch"].side_effect = Refused("- order 1001: not found.")
    room = _Room(1)
    with pytest.raises(Refused):
        await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=room)
    assert room.left == 1


@pytest.mark.asyncio
async def test_a_watch_a_wakeup_still_follows_is_not_read_again(lookup):
    # The reconciler keeps it read, so following it again needs no read and no room.
    lookup["set_row"](_row(action="watch"))
    room = _Room(0, followed={"a1"})
    attempt, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=room)
    assert attempt["attempt_id"] == "a1"
    lookup["watch"].assert_not_awaited()
    assert room.taken == 0


@pytest.mark.asyncio
async def test_a_dormant_watch_takes_room_before_it_is_read(lookup):
    from src.tools.wakeups.schedule import Refused

    lookup["set_row"](_row(action="watch"))
    with pytest.raises(Refused):
        await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=_Room(0))
    lookup["watch"].assert_not_awaited()


@pytest.mark.asyncio
async def test_the_same_order_twice_in_one_call_is_read_and_counted_once(lookup):
    room = _Room(1)
    lookup["set_row"](None)
    first, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=room)
    # The second entry finds the watch row the first one wrote.
    lookup["set_row"]({**first, "action": "watch"})
    again, _ = await order_wakeups.watch_spec(THREAD, "u1", _ask(), NOW, room=room)
    assert again["attempt_id"] == first["attempt_id"]
    lookup["watch"].assert_not_awaited()
    assert room.left == 0 and room.taken == 1
