"""Lag ceilings: both sides of each tier boundary."""

import pytest

from market_protocol import Tier, classify_lag
from market_protocol.freshness import DELAYED_LAG_CEILING_S, LIVE_LAG_CEILING_S, MIN_CREDIBLE_LAG_S


def test_ceilings():
    assert (LIVE_LAG_CEILING_S, DELAYED_LAG_CEILING_S) == (90, 900)


@pytest.mark.parametrize(
    ("lag_s", "tier"),
    [
        (MIN_CREDIBLE_LAG_S, Tier.REALTIME),  # clock skew, not a fault
        (0, Tier.REALTIME),
        (LIVE_LAG_CEILING_S, Tier.REALTIME),
        (LIVE_LAG_CEILING_S + 1, Tier.DELAYED_15M),
        (DELAYED_LAG_CEILING_S, Tier.DELAYED_15M),
        (DELAYED_LAG_CEILING_S + 1, Tier.EOD),
    ],
)
def test_classify_lag(lag_s, tier):
    assert classify_lag(lag_s) is tier


@pytest.mark.parametrize("lag_s", [MIN_CREDIBLE_LAG_S - 1, -28800])
def test_a_lag_ahead_of_the_clock_is_refused(lag_s):
    # -28800 is Shanghai time stamped as UTC; the routing policy ranks it unmeasured.
    with pytest.raises(ValueError, match="ahead of the clock"):
        classify_lag(lag_s)
