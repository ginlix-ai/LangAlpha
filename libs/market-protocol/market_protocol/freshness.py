"""Lag ceilings: how far behind the clock a feed may run and still hold a tier.

Every consumer that turns a measured lag into a tier or a label reads these,
so a probe ranking a feed and a response labelling it cannot disagree about
where "delayed" ends.
"""

from __future__ import annotations

from .enums import Tier

# A feed measured at most this far behind ranks as realtime. The probe compares
# feeds against each other at one instant, so the minute of print granularity
# every feed shares cancels out and a tight line separates them.
LIVE_LAG_CEILING_S = 90

# A single served quote stays "live" this long after its print. Print times
# arrive at minute granularity (a quote seconds old can carry an ``as_of`` up to
# a minute behind), so one minute of granularity plus one of transport keeps a
# realtime feed from flapping to delayed in the back half of every minute.
# Looser than LIVE_LAG_CEILING_S because nothing here cancels the granularity.
QUOTE_LIVE_S = 120

# 15 minutes is the widest delay any provider declares; beyond it a feed is not
# merely delayed but unusable as a current picture.
DELAYED_LAG_CEILING_S = 900

# A few seconds ahead is clock skew; further ahead, the feed stamps local time
# as UTC, and the lag measures that fault rather than freshness.
MIN_CREDIBLE_LAG_S = -60


def classify_lag(lag_s: int) -> Tier:
    """Tier a feed earns from its measured lag.

    Raises ValueError for a lag further ahead of the clock than
    ``MIN_CREDIBLE_LAG_S``: a feed stamped hours in the future is broken, not
    realtime, and the routing policy ranks it as unmeasured for the same reason.
    """
    if lag_s < MIN_CREDIBLE_LAG_S:
        raise ValueError(f"lag {lag_s}s is ahead of the clock: a timestamp fault, not a tier")
    if lag_s <= LIVE_LAG_CEILING_S:
        return Tier.REALTIME
    if lag_s <= DELAYED_LAG_CEILING_S:
        return Tier.DELAYED_15M
    return Tier.EOD
