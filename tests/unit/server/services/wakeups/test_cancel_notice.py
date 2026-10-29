"""What the agent is told of the wakeups the user cancelled: the rows
``ThreadCancelNotices`` words, and ``WakeupCancelNoticeMiddleware`` writing
them through the ``CancelNotices`` port for what its history does not name,
with the table faked."""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware import wakeups as mw
from ptc_agent.agent.middleware.runtime_context.durable import (
    build_update_message,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import WAKEUP_CANCELLED_ROW_KIND
from src.server.services.wakeups import cancel_notice
from src.server.services.wakeups.cancel_notice import ThreadCancelNotices
from tests.unit.server.services.wakeups.rows import (
    THREAD_ID,
    UTC,
    base_row,
    cron_row,
)

NOW = datetime(2026, 10, 20, 14, 31, tzinfo=UTC)
FIRST = "44444444-4444-4444-4444-444444444444"
SECOND = "55555555-5555-5555-5555-555555555555"

# The approved example, drafts.py's CANCEL_ROW {label}.
PULLBACK = (
    "NVDA pullback entry (*/5 10-15 * * 1-5 in America/New_York, set at 09:57 EDT,"
    " 6 of 24 checks done)"
)


def cancelled(wakeup_id: str, by: str = "user", **over) -> dict:
    row = cron_row(
        wakeup_id=wakeup_id, label="NVDA pullback entry", fires_done=6, last_fired_seq=6,
        delivered_seq=6, status="cancelled", cancelled_by=by, **over,
    )  # fmt: skip
    return row


def breakdown(wakeup_id: str, by: str = "user") -> dict:
    return base_row(
        wakeup_id=wakeup_id, kind="price", symbol="NVDA", market="stock", price_kind="below",
        price_value=198.0, label="NVDA breakdown", status="cancelled", cancelled_by=by,
    )  # fmt: skip


@pytest.fixture
def table(monkeypatch):
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW

    monkeypatch.setattr(cancel_notice, "datetime", Frozen)
    rows: list[dict] = []
    monkeypatch.setattr(
        cancel_notice.cancel_db,
        "user_cancels",
        AsyncMock(side_effect=lambda t: list(rows)),
    )
    return rows


@pytest.mark.asyncio
async def test_one_cancel_is_one_row_naming_what_it_was(table):
    rows = table
    rows.append(cancelled(FIRST))
    [group] = await ThreadCancelNotices(THREAD_ID, "UTC").untold(set())
    assert group.text == (
        f"The user cancelled a wakeup you set: {PULLBACK}."
        " Leave it cancelled unless the user asks for it again."
    )
    assert (group.cancelled_by, group.wakeup_ids) == ("user", (FIRST,))


@pytest.mark.asyncio
async def test_several_cancels_share_one_row_with_a_line_each(table):
    rows = table
    rows.extend([cancelled(FIRST), breakdown(SECOND)])
    [group] = await ThreadCancelNotices(THREAD_ID, "UTC").untold(set())
    assert group.text == (
        "The user cancelled wakeups you set:\n"
        f"- {PULLBACK}\n"
        "- NVDA breakdown (NVDA below $198.00, set at 09:57 EDT)\n"
        "Leave them cancelled unless the user asks for them again."
    )
    assert group.wakeup_ids == (FIRST, SECOND)


@pytest.mark.asyncio
async def test_turning_the_setting_off_has_its_own_row(table):
    rows = table
    rows.extend(
        [cancelled(FIRST, by="setting_off"), breakdown(SECOND, by="setting_off")]
    )
    [group] = await ThreadCancelNotices(THREAD_ID, "UTC").untold(set())
    assert group.cancelled_by == "setting_off"
    assert group.text == (
        "The user turned wakeups off, which cancelled every pending wakeup:\n"
        f"- {PULLBACK}\n"
        "- NVDA breakdown (NVDA below $198.00, set at 09:57 EDT)\n"
        "schedule_wakeup is no longer available in this conversation."
    )


@pytest.mark.asyncio
async def test_a_user_cancel_and_the_setting_are_told_apart(table):
    rows = table
    rows.extend([cancelled(FIRST), breakdown(SECOND, by="setting_off")])
    groups = await ThreadCancelNotices(THREAD_ID, "UTC").untold(set())
    assert [g.cancelled_by for g in groups] == ["user", "setting_off"]


@pytest.mark.asyncio
async def test_a_cancel_the_history_already_names_is_not_told_again(table):
    rows = table
    rows.extend([cancelled(FIRST), breakdown(SECOND)])
    [group] = await ThreadCancelNotices(THREAD_ID, "UTC").untold({FIRST})
    assert group.wakeup_ids == (SECOND,)
    assert group.text.startswith("The user cancelled a wakeup you set: NVDA breakdown")


@pytest.mark.asyncio
async def test_nothing_new_gives_no_row(table):
    rows = table
    rows.append(cancelled(FIRST))
    assert await ThreadCancelNotices(THREAD_ID, "UTC").untold({FIRST}) == ()


# --- the middleware ---------------------------------------------------------


class Port:
    def __init__(self, groups=(), delay: float = 0.0):
        self.groups = tuple(groups)
        self.delay = delay
        self.asked: list[set] = []

    async def untold(self, told):
        self.asked.append(set(told))
        await asyncio.sleep(self.delay)
        return self.groups


def group(text="The user cancelled a wakeup you set: x.", ids=(FIRST,), by="user"):
    return mw.CancelGroup(wakeup_ids=tuple(ids), cancelled_by=by, text=text)


def told_row(*ids):
    return build_update_message(
        mw.DurableUpdate(
            kind=WAKEUP_CANCELLED_ROW_KIND,
            schema_version=mw.UPDATE_SCHEMA_VERSION,
            text="The user cancelled a wakeup you set: x.",
            provenance={"wakeup_ids": list(ids)},
        )
    )


@pytest.mark.asyncio
async def test_the_middleware_writes_one_durable_row_per_group():
    port = Port(
        groups=[
            group(),
            group("The user turned wakeups off.", (SECOND,), "setting_off"),
        ]
    )
    update = await mw.WakeupCancelNoticeMiddleware(port).abefore_model({"messages": []})
    rows = [runtime_update_from_message(m) for m in update["messages"]]
    assert [(r.kind, r.text) for r in rows] == [
        (WAKEUP_CANCELLED_ROW_KIND, "The user cancelled a wakeup you set: x."),
        (WAKEUP_CANCELLED_ROW_KIND, "The user turned wakeups off."),
    ]
    assert rows[0].provenance["wakeup_ids"] == [FIRST]
    assert rows[1].provenance["cancelled_by"] == "setting_off"


@pytest.mark.asyncio
async def test_the_ids_rows_in_history_name_and_the_window_trimmed_are_told():
    port = Port()
    state = {"messages": [told_row(FIRST)], "_wakeup_cancels_trimmed": [SECOND]}
    assert await mw.WakeupCancelNoticeMiddleware(port).abefore_model(state) is None
    assert port.asked == [{FIRST, SECOND}]


# A turn the user retries or regenerates, or one before an edited message,
# restores a checkpoint from before the row. The told set comes from that
# history alone, so the new branch is told again.

QUESTION = HumanMessage("Is NVDA near my entry?", id="q1")


@pytest.mark.asyncio
async def test_a_history_that_holds_the_row_is_told_nothing_more(table):
    table.append(cancelled(FIRST))
    notice = mw.WakeupCancelNoticeMiddleware(ThreadCancelNotices(THREAD_ID, "UTC"))
    [row] = (await notice.abefore_model({"messages": [QUESTION]}))["messages"]
    assert await notice.abefore_model({"messages": [QUESTION, row]}) is None


@pytest.mark.asyncio
async def test_a_history_rewound_to_before_the_row_is_told_again(table):
    table.append(cancelled(FIRST))
    notice = mw.WakeupCancelNoticeMiddleware(ThreadCancelNotices(THREAD_ID, "UTC"))
    [row] = (await notice.abefore_model({"messages": [QUESTION]}))["messages"]
    assert await notice.abefore_model({"messages": [QUESTION, row]}) is None
    [again] = (await notice.abefore_model({"messages": [QUESTION]}))["messages"]
    assert runtime_update_from_message(again).provenance["wakeup_ids"] == [FIRST]
    assert runtime_update_from_message(again).text.startswith(
        f"The user cancelled a wakeup you set: {PULLBACK}."
    )


def test_the_window_carries_the_ids_of_the_rows_it_trims():
    trimmed = [QUESTION, told_row(SECOND), told_row(FIRST)]
    carry = mw.wakeup_cancels_window_carry
    assert carry(trimmed, {}) == {"_wakeup_cancels_trimmed": [FIRST, SECOND]}
    assert carry(trimmed, {"_wakeup_cancels_trimmed": [SECOND]}) == {
        "_wakeup_cancels_trimmed": [SECOND, FIRST]
    }
    assert carry(trimmed, {"_wakeup_cancels_trimmed": [FIRST, SECOND]}) == {}
    assert carry([QUESTION], {}) == {}


@pytest.mark.asyncio
async def test_a_slow_read_fails_open_within_the_timeout(monkeypatch):
    monkeypatch.setattr(mw, "_READ_TIMEOUT_S", 0.05)
    port = Port(groups=[group()], delay=5.0)
    started = asyncio.get_running_loop().time()
    update = await mw.WakeupCancelNoticeMiddleware(port).abefore_model({"messages": []})
    assert update is None
    assert asyncio.get_running_loop().time() - started < 1.0


@pytest.mark.asyncio
async def test_a_failing_read_fails_open():
    port = Port()
    port.untold = AsyncMock(side_effect=RuntimeError("pool closed"))
    assert (
        await mw.WakeupCancelNoticeMiddleware(port).abefore_model({"messages": []})
        is None
    )
