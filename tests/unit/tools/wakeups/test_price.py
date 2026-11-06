"""The price checks a price wakeup keeps from the old targets: what counts as
a hit, whether a trigger would fire the moment it is set, and the refusals
that name why one cannot be watched. (The in-session % base is pinned in
``test_market_watch_tools.py``.)"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.server.services.price_monitor import target_hit
from src.server.models.automation import MarketType
from src.tools.wakeups.price import already_met, last_and_base, plan_price, price_spec
from src.tools.wakeups.schedule import PriceAsk, Refused, check_trigger

NY = ZoneInfo("America/New_York")
IN_SESSION = datetime(2026, 10, 6, 11, 0, tzinfo=NY)


# --- target_hit -------------------------------------------------------------


@pytest.mark.parametrize(
    "kind, value, ref, price, hit",
    [
        ("above", 200.0, 0.0, 200.0, True),  # a print on the level is the hit
        ("above", 200.0, 0.0, 200.01, True),
        ("above", 200.0, 0.0, 199.99, False),
        ("below", 198.0, 0.0, 198.0, True),
        ("below", 198.0, 0.0, 197.5, True),
        ("below", 198.0, 0.0, 198.01, False),
        ("pct_up", 3.0, 100.0, 103.0, True),
        ("pct_up", 3.0, 100.0, 102.99, False),
        ("pct_down", 3.0, 100.0, 97.0, True),
        ("pct_down", 3.0, 100.0, 97.01, False),
        ("pct_up", 3.0, 0.0, 500.0, False),  # no base to measure from never hits
        ("pct_down", 3.0, 0.0, 1.0, False),
        ("sideways", 1.0, 100.0, 100.0, False),  # a kind it does not know
    ],
)
def test_target_hit_at_the_boundary(kind, value, ref, price, hit):
    assert target_hit(kind, value, ref, price) is hit


# --- already_met ------------------------------------------------------------


def _plan(kind: str, value: float, pct_from=None):
    return plan_price(PriceAsk(None, "NVDA", kind, value, pct_from))


@pytest.mark.parametrize(
    "kind, value, last, expected",
    [
        ("above", 200.0, 201.0, "NVDA is already above 200 (last 201.00)"),
        ("above", 200.0, 200.0, "NVDA is already at 200 (last 200.00)"),
        ("below", 198.0, 197.0, "NVDA is already below 198 (last 197.00)"),
        ("below", 198.0, 198.0, "NVDA is already at 198 (last 198.00)"),
        ("above", 200.0, 199.0, None),
        ("below", 198.0, 199.0, None),
    ],
)
def test_a_level_the_last_price_already_sits_past_is_refused_as_met(
    kind, value, last, expected
):
    assert already_met(_plan(kind, value), last, 195.0, IN_SESSION) == expected


def test_without_a_last_price_nothing_is_met():
    assert already_met(_plan("above", 1.0), None, None, IN_SESSION) is None


def test_a_percent_fall_from_the_previous_close_is_met_in_session():
    plan = _plan("pct_down", 3.0, "previous_close")
    assert (
        already_met(plan, 96.0, 100.0, IN_SESSION)
        == "NVDA is already down 4.00% from the previous close (last 96.00)"
    )
    assert already_met(plan, 99.0, 100.0, IN_SESSION) is None


def test_a_move_made_after_the_close_is_met_against_the_close_just_made():
    plan = _plan("pct_down", 3.0, "previous_close")
    closed = datetime(2026, 10, 6, 21, 0, tzinfo=NY)
    assert already_met(plan, 95.0, 100.0, closed) is None
    assert (
        already_met(plan, 95.0, 100.0, closed, closed_base=True)
        == "NVDA is already down 5.00% from the previous close (last 95.00)"
    )


@pytest.mark.parametrize(
    "closed, close_fields",
    [
        # Not rolled: the row still holds the close.
        (datetime(2026, 11, 5, 21, 0, tzinfo=NY),
         {"regular_close": 100.0, "previous_close": 103.0}),
        # Rolled before the open, still holding the after-hours trade.
        (datetime(2026, 11, 6, 3, 46, tzinfo=NY),
         {"regular_close": None, "previous_close": 100.0}),
    ],
)
def test_setting_after_the_close_refuses_a_move_the_after_hours_already_made(
    closed, close_fields
):
    row = {
        "last_trade_price": 95.0, **close_fields,
        "as_of": _ms(datetime(2026, 11, 5, 19, 59, tzinfo=NY)),
    }
    with pytest.raises(Refused) as refused:
        price_spec(_plan("pct_down", 3.0, "previous_close"), None, row, closed)
    assert refused.value.lines == [
        "- NVDA down 3% from the previous close: NVDA is already down 5.00%"
        " from the previous close (last 95.00)."
    ]


def test_a_thin_name_that_has_not_traded_since_the_close_is_not_met():
    """Quoted after the close, last traded before it: no trade has been
    measured from the close yet."""
    row = {
        "last_trade_price": 95.0, "regular_close": 100.0, "previous_close": 103.0,
        "last_trade_at": _ms(datetime(2026, 11, 5, 15, 40, tzinfo=NY)),
        "as_of": _ms(datetime(2026, 11, 5, 19, 59, tzinfo=NY)),
    }
    closed = datetime(2026, 11, 5, 21, 0, tzinfo=NY)
    spec, _, level = price_spec(_plan("pct_down", 3.0, "previous_close"), None, row, closed)
    assert level == pytest.approx(97.0)


def test_a_percent_from_now_is_never_met_at_set_time():
    assert already_met(_plan("pct_up", 3.0, "now"), 500.0, 100.0, IN_SESSION) is None


# --- the % base -------------------------------------------------------------

_AAPL = ("AAPL", MarketType.STOCK)


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


# A US row after the close keeps the session that just ran until the feed's
# overnight reset, so its previous close is the session before.
_UNROLLED = {
    "last_trade_price": 343.32,
    "price": 344.17,
    "regular_close": 344.17,
    "previous_close": 349.51,
    "as_of": _ms(datetime(2026, 11, 5, 19, 59, tzinfo=NY)),
}


@pytest.mark.parametrize(
    "now, row, base",
    [
        # Closed overnight: the next session measures from the close just made.
        (datetime(2026, 11, 5, 22, 0, tzinfo=NY), _UNROLLED, 344.17),
        # A vendor with no regular_close: its session price may not be the close.
        (
            datetime(2026, 11, 5, 22, 0, tzinfo=NY),
            {**_UNROLLED, "regular_close": None},
            349.51,
        ),
        # Rolled before the open: its previous close is already the base.
        (
            datetime(2026, 11, 6, 3, 45, tzinfo=NY),
            {"previous_close": 344.17, "as_of": _UNROLLED["as_of"]},
            344.17,
        ),
        # After hours the session is still the day's: the base stays put.
        (datetime(2026, 11, 5, 17, 0, tzinfo=NY), _UNROLLED, 349.51),
        # A row last printed in an earlier session says nothing new.
        (
            datetime(2026, 11, 5, 22, 0, tzinfo=NY),
            {**_UNROLLED, "as_of": _ms(datetime(2026, 11, 4, 19, 59, tzinfo=NY))},
            349.51,
        ),
        # A thin name that last traded before the close, quoted after it.
        (
            datetime(2026, 11, 5, 22, 0, tzinfo=NY),
            {**_UNROLLED,
             "last_trade_at": _ms(datetime(2026, 11, 5, 15, 40, tzinfo=NY))},
            344.17,
        ),
        # Printed before the close: no final close yet.
        (
            datetime(2026, 11, 5, 22, 0, tzinfo=NY),
            {**_UNROLLED, "as_of": _ms(datetime(2026, 11, 5, 15, 46, tzinfo=NY))},
            349.51,
        ),
    ],
)
def test_the_base_is_the_close_the_next_print_measures_from(now, row, base):
    assert last_and_base(_AAPL, row, now) == (row.get("last_trade_price"), base)


SH = ZoneInfo("Asia/Shanghai")


@pytest.mark.parametrize(
    "listing, now, row, base",
    [
        # An index trades in the regular session only, so after its close the
        # row's close is the next session's base once the row has seen it.
        (
            ("SPX", MarketType.INDEX),
            datetime(2026, 11, 5, 17, 0, tzinfo=NY),
            {"price": 5900.0, "regular_close": 5900.0, "previous_close": 6000.0,
             "as_of": _ms(datetime(2026, 11, 5, 16, 0, tzinfo=NY))},
            5900.0,
        ),
        # A delayed index read before its close has printed has no final close.
        (
            ("SPX", MarketType.INDEX),
            datetime(2026, 11, 5, 16, 5, tzinfo=NY),
            {"price": 5910.0, "regular_close": 5910.0, "previous_close": 6000.0,
             "as_of": _ms(datetime(2026, 11, 5, 15, 50, tzinfo=NY))},
            6000.0,
        ),
        # An opening-auction row has already rolled: its previous close is the base.
        (
            ("600519.SH", MarketType.STOCK),
            datetime(2026, 11, 6, 9, 26, tzinfo=SH),
            {"price": 1460.0, "regular_close": 1460.0, "previous_close": 1441.56,
             "as_of": _ms(datetime(2026, 11, 6, 9, 25, tzinfo=SH))},
            1441.56,
        ),
    ],
)
def test_a_closed_venue_measures_from_the_close_its_row_has_seen(listing, now, row, base):
    assert last_and_base(listing, row, now)[1] == base


# --- the ported refusals ----------------------------------------------------


def _refusal(price: dict) -> str:
    try:
        plan_price(check_trigger({"price": price}, IN_SESSION, "UTC"))
    except Refused as e:
        return "\n".join(e.lines)
    raise AssertionError("not refused")


@pytest.mark.parametrize(
    "price, refusal",
    [
        ({"symbol": "!!!", "above": 1}, "- !!!: not a ticker symbol."),
        (
            {"symbol": "NVDA", "above": -5},
            "- NVDA above -5: above must be greater than 0.",
        ),
        (
            {"symbol": "NVDA", "pct_down": 100},
            "- NVDA down 100% from the previous close: a fall of 100% or more can never hit.",
        ),
        (
            {"symbol": "NVDA", "pct_up": 0, "pct_from": "now"},
            "- NVDA up 0% from now: pct_up must be greater than 0.",
        ),
        (
            {"symbol": "BTC-USD", "above": 1},
            "- BTC-USD above 1: a crypto, currency or futures symbol;"
            " the price monitor cannot watch those.",
        ),
        (
            {"symbol": "ZZZZ.ZZ", "below": 5},
            "- ZZZZ.ZZ below 5: an exchange suffix the price monitor does not know,"
            " so it would never be checked.",
        ),
        (
            {"symbol": "123456", "below": 140},
            "- 123456 below 140: a bare number names no market; add its exchange suffix,"
            " as in 600519.SH, 830799.BJ, 0700.HK, 7203.T, 005930.KS or 2330.TW.",
        ),
    ],
)
def test_each_ported_price_refusal_reads_as_drafted(price, refusal):
    assert _refusal(price) == refusal
