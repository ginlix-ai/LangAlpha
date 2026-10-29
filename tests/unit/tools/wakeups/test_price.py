"""The price checks a price wakeup keeps from the old targets: what counts as
a hit, whether a trigger would fire the moment it is set, and the refusals
that name why one cannot be watched. (The in-session % base is pinned in
``test_market_watch_tools.py``.)"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.server.services.price_monitor import target_hit
from src.tools.wakeups.price import already_met, plan_price
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


def test_a_percent_from_now_is_never_met_at_set_time():
    assert already_met(_plan("pct_up", 3.0, "now"), 500.0, 100.0, IN_SESSION) is None


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
