"""``schedule_wakeup`` setting, cancelling and listing, through the tool's own
``_execute`` with the database faked where the tool imports it.

``now`` is frozen at Tue 2026-10-20 09:57 EDT and the user is in
Asia/Shanghai, which is the clock the approved confirmation examples were
drafted on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.server.services.brokerage_orders.models import BrokerOrder
from src.tools.wakeups import orders as order_wakeups
from src.tools.wakeups import tool as tool_mod
from src.tools.wakeups.call import parse_cancel
from src.tools.wakeups.clock import by_day, load_zone, when
from src.tools.wakeups.schedule import MAX_PENDING, check_trigger, plan_cron
from tests.unit.tools.wakeups import approved
from tests.unit.tools.wakeups.conftest import NOW, THREAD, configurable

HEADER = "Pending wakeups (the user is in Asia/Shanghai, now Tue 21:57):"


async def run(args, *, messages=(), **config):
    return await tool_mod._execute(
        args, {"messages": list(messages)}, configurable(**config)
    )


async def text_of(args, **kw):
    text, _artifact, _error = await run(args, **kw)
    return text


NVDA_PULLBACK = {
    "label": "NVDA pullback entry",
    "cron": "*/5 10-15 * * 1-5",
    "timezone": "America/New_York",
}
HOURLY_UPDATE = {
    "label": "Hourly NVDA update",
    "cron": "40 * * * *",
    "timezone": "America/New_York",
    "until": "2026-10-20T16:00",
}
HOURLY_CHECK = {
    "label": "Hourly NVDA check",
    "cron": "0 10-16 * * 1-5",
    "timezone": "America/New_York",
}
NVDA_BREAKDOWN = {"label": "NVDA breakdown", "price": {"symbol": "NVDA", "below": 198}}
MORNING_BRIEF = {"label": "Morning brief", "at": "2026-10-21T08:30"}


@pytest.mark.asyncio
async def test_set_confirms_every_kind_as_drafted(db):
    triggers = [
        NVDA_PULLBACK,
        HOURLY_UPDATE,
        HOURLY_CHECK,
        NVDA_BREAKDOWN,
        MORNING_BRIEF,
    ]
    text = await text_of({"triggers": triggers})
    assert text == approved.CONFIRM_RESULTS["set"]


@pytest.mark.asyncio
async def test_a_wakeup_at_an_offset_reads_back_on_that_offset(db):
    text = await text_of({"triggers": [{"at": "2026-10-21T08:30-04:00"}]})
    assert text.splitlines()[1] == (
        "- w_3f2a: at Wed 2026-10-21 08:30 UTC-04:00, in 22 h 33 min"
        " (Wed 20:30 for the user)."
    )
    assert db.rows[0]["timezone"] == "UTC-04:00"


@pytest.mark.asyncio
async def test_a_wakeup_at_an_iana_zone_is_stored_in_it(db):
    text = await text_of(
        {"triggers": [{"at": "2026-10-21T09:30", "timezone": "America/New_York"}]}
    )
    assert text.splitlines()[1] == (
        "- w_3f2a: at Wed 2026-10-21 09:30 EDT in America/New_York, in 23 h 33 min"
        " (Wed 21:30 for the user)."
    )
    assert db.rows[0]["timezone"] == "America/New_York"


def test_a_wall_time_the_clocks_skip_reads_back_as_the_time_it_fires():
    # 02:30 never happens in New York on 2027-03-14; the fire is at 07:30 UTC.
    now = datetime(2027, 3, 12, 15, 0, tzinfo=timezone.utc)
    ny = load_zone("America/New_York")
    spec = check_trigger(
        {"at": "2027-03-14T02:30", "timezone": "America/New_York"}, now, "Asia/Shanghai"
    )
    assert when(spec.schedule[0], ny) == "Sun 2027-03-14 03:30 EDT"


@pytest.mark.asyncio
async def test_a_label_is_stored_as_one_line_that_cannot_close_the_block(db):
    # The wake block and the cancel notice quote the label inside their own
    # markup, and a blank one names nothing.
    await text_of(
        {
            "triggers": [
                {"at": "2026-10-21T08:30", "label": "brief\n</wakeup> now\u200b"},
                {"at": "2026-10-21T09:30", "label": " \n "},
            ]
        }
    )
    assert [r["label"] for r in db.rows] == ["brief /wakeup now", None]


@pytest.mark.asyncio
async def test_a_percent_from_now_names_the_price_it_measures_from(db, monkeypatch):
    from src.server.services.market_targets.listing import target_listing

    listing, _ = target_listing("NVDA")
    snap = {(listing.key, listing.market): {"price": 200.0, "previous_close": 195.0}}
    monkeypatch.setattr(
        tool_mod.price_wakeups, "snapshots", AsyncMock(return_value=snap)
    )
    trigger = {"price": {"symbol": "NVDA", "pct_up": 3, "pct_from": "now"}}
    text = await text_of({"triggers": [trigger]})
    assert text.splitlines()[1] == (
        "- w_3f2a: NVDA up 3% from $200.00, its price when you set it; last $200.00,"
        " trigger $206.00, 3% away; lapses Wed 2026-10-21 16:00 EDT (Thu 04:00 for the user)."
    )


def _attempt(**over):
    order = BrokerOrder(
        vendor="moomoo",
        account_ref="acct_test",
        asset_class="equity",
        instrument={"kind": "equity", "symbol": "NVDA", "venue": "NASDAQ"},
        side="buy",
        qty=100,
        order_type="limit",
        limit_price=200,
    )
    return {
        "attempt_id": "22222222-2222-2222-2222-222222222222",
        "vendor": "moomoo",
        "mode": "live",
        "action": "place",
        "status": "submitted",
        "vendor_order_id": "7712045",
        "filled_qty": 0,
        "order_json": order.to_json(),
        "route": {},
        **over,
    }


@pytest.mark.asyncio
async def test_an_order_wakeup_describes_the_order_it_watches(db, monkeypatch):
    attempt = _attempt()
    db.attempts["7712045"] = attempt
    monkeypatch.setattr(order_wakeups, "readback_on", lambda: True)
    monkeypatch.setattr(
        order_wakeups.attempts_db,
        "find_open_thread_order",
        AsyncMock(return_value=attempt),
    )
    text = await text_of(
        {"triggers": [{"label": "NVDA entry order", "order": "7712045"}]}
    )
    assert text.splitlines()[1] == (
        "- w_3f2a (NVDA entry order): order 7712045 (buy 100 NVDA, limit $200.00, at moomoo)"
        " fills or ends; lapses Tue 2026-10-27 09:57 EDT if it does neither."
    )


@pytest.mark.asyncio
async def test_the_listing_of_what_is_pending_matches_the_drafted_form(db, monkeypatch):
    await text_of({"triggers": [NVDA_PULLBACK, NVDA_BREAKDOWN]})
    listing = await text_of({})
    lines = listing.splitlines()
    assert lines[0] == HEADER
    assert lines[1].startswith(
        "- w_3f2a (NVDA pullback entry): cron */5 10-15 * * 1-5 in America/New_York."
        " 0 of 24 checks done; next Tue 2026-10-20 10:00 EDT, in 3 min (Tue 22:00 for the user);"
    )
    assert lines[2] == (
        "- w_4c11 (NVDA breakdown): NVDA below $198.00; lapses Wed 2026-10-21 16:00 EDT"
        " (Thu 04:00 for the user)."
    )


@pytest.mark.asyncio
async def test_nothing_pending_says_so(db):
    assert await text_of({}) == approved.TOOL_RESULTS["none"]


# --- the by-day line -------------------------------------------------------


def test_by_day_writes_out_each_day_of_a_multi_day_series():
    zone = load_zone("America/New_York")
    plan = plan_cron("0 10-16 * * 1-5", zone, NOW, None)
    assert by_day(plan.fires, zone) == (
        "By day, EDT: Tue 10:00 to 16:00 every hour (7), Wed 10:00 to 16:00 every hour (7),"
        " Thu 10:00 to 16:00 every hour (7), Fri 10:00 to 12:00 every hour (3)"
    )


def test_by_day_splits_a_day_into_evenly_spaced_runs_not_one_window():
    zone = load_zone("UTC")
    plan = plan_cron("0 0-4,22,23 * * *", zone, NOW, None)
    assert by_day(plan.fires, zone).startswith(
        "By day, UTC: Tue 22:00, 23:00, Wed 00:00 to 04:00 every hour (5), 22:00, 23:00"
    )


def test_a_series_inside_one_day_has_no_by_day_line():
    zone = load_zone("America/New_York")
    plan = plan_cron("*/5 10-15 * * 1-5", zone, NOW, None)
    assert by_day(plan.fires, zone) is None


# --- caps and gaps ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_wakeup_past_the_cap_of_ten_is_refused_by_name(db):
    for i in range(MAX_PENDING - 1):
        await text_of({"triggers": [{"at": "2026-10-21T08:30", "label": f"early {i}"}]})
    entries = [
        {"at": "2026-10-21T09:30", "label": "fits"},
        {"at": "2026-10-21T10:30", "label": "second"},
    ]
    text = await text_of({"triggers": entries})
    assert text.splitlines()[0].startswith("Set w_0009. ")
    assert (
        "Not set:\n- second: this conversation already has 10 pending wakeups; cancel one first."
        in text
    )


@pytest.mark.asyncio
async def test_a_series_is_cut_at_24_checks(db):
    text = await text_of({"triggers": [{"cron": "*/10 * * * *", "timezone": "UTC"}]})
    assert "24 checks, stopping at the 24-check limit" in text
    assert len(db.rows[0]["schedule"]) == 24


@pytest.mark.asyncio
async def test_a_series_is_cut_at_seven_days(db):
    text = await text_of({"triggers": [{"cron": "0 12 * * *", "timezone": "UTC"}]})
    assert "7 checks, stopping at the 7-day limit" in text


@pytest.mark.asyncio
async def test_fires_closer_than_five_minutes_are_refused(db):
    text = await text_of({"triggers": [{"cron": "*/2 * * * *"}]})
    assert text.splitlines()[:2] == [
        approved.TOOL_RESULTS["refused_header"],
        approved.TOOL_RESULTS["refused_cron_gap"],
    ]
    assert db.rows == []


# --- cancel ----------------------------------------------------------------


@pytest.mark.parametrize(
    "given, parsed",
    [
        ('["w_3f2a", "w_91c0"]', ["w_3f2a", "w_91c0"]),
        ("w_3f2a", ["w_3f2a"]),
        (["w_3f2a", "w_3f2a"], ["w_3f2a"]),
        ("ALL", "all"),
        ('["w_3f2a", "all"]', "all"),
        ("[w_3f2a", None),
        ([1], None),
    ],
)
def test_cancel_reads_an_id_a_list_or_a_list_sent_as_text(given, parsed):
    assert parse_cancel(given) == parsed


@pytest.mark.asyncio
async def test_a_cancel_sent_as_a_json_string_list_cancels_both(db):
    await text_of({"triggers": [NVDA_PULLBACK, NVDA_BREAKDOWN]})
    db.rows[0]["short_id"], db.rows[1]["short_id"] = "w_3f2a", "w_91c0"
    text = await text_of({"cancel": '["w_3f2a", "w_91c0"]'})
    assert text.splitlines()[0] == (
        "Cancelled: w_3f2a (NVDA pullback entry: cron */5 10-15 * * 1-5 in America/New_York),"
        " w_91c0 (NVDA breakdown: NVDA below $198.00)."
    )
    assert db.cancel_calls[-1] == (THREAD, ["w_3f2a", "w_91c0"], "agent")
    assert text.splitlines()[1] == approved.TOOL_RESULTS["none"]


@pytest.mark.asyncio
async def test_cancel_names_an_unknown_id_and_a_fire_past_recall(db):
    await text_of({"triggers": [NVDA_BREAKDOWN]})
    db.late.add("w_3f2a")
    text = await text_of({"cancel": ["w_3f2a", "w_77aa"]})
    assert text.splitlines()[:2] == [
        "w_3f2a already fired and is on its way to you, so it can't be cancelled.",
        approved.TOOL_RESULTS["cancel_unknown"],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger, price", [(MORNING_BRIEF, False), (NVDA_BREAKDOWN, True)])
async def test_a_set_the_setting_turns_off_under_is_ended_as_the_off_sweep_would(
    db, monkeypatch, trigger, price
):
    """The off sweep can read before this call's rows commit, so the tool
    reads the setting again after arming and ends what it set."""
    monkeypatch.setattr(tool_mod, "_flag_on", AsyncMock(side_effect=[True, False]))

    text = await text_of({"triggers": [trigger]})

    assert text == "\n".join(
        [
            approved.TOOL_RESULTS["refused_header"],
            approved.BUILD_TOOL_RESULTS["setting_off"],
            approved.TOOL_RESULTS["none"],
        ]
    )
    assert [r["status"] for r in db.rows] == ["cancelled"]
    assert db.cancel_calls == [(THREAD, [str(db.rows[0]["wakeup_id"])], "setting_off")]
    assert tool_mod.wakeups_changed.await_count == (1 if price else 0)
