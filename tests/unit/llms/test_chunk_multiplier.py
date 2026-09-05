"""What a spend budget for one model is worth against another's.

Every assertion here is a relationship, never a rate: the manifest's numbers
move whenever a vendor reprices, and a test that pins them turns a price cut
into a failure. What must hold is the ordering and the fallbacks.
"""

from datetime import datetime, timezone

import pytest

from src.llms import pricing_utils
from src.llms.pricing_utils import (
    _BASELINE_BLENDED_RATE,
    blended_rate,
    chunk_multiplier,
)

# DeepSeek's card is the one with hours on it: peak is 01-04 and 06-10 UTC on
# weekdays, so these two stamps straddle a boundary the manifest defines.
_SCHEDULED = "deepseek-v4-pro"
_PEAK = datetime(2026, 8, 27, 2, 0, tzinfo=timezone.utc)  # a Thursday
_OFF_PEAK = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)


def test_a_model_with_no_card_has_no_opinion():
    """None, not 1.0. The caller decides what a missing rate means, and the
    gate's answer — reserve at baseline — is a different statement from
    'this model costs the baseline'."""
    assert chunk_multiplier("not-a-real-model-anywhere") is None
    assert blended_rate("not-a-real-model-anywhere") is None


def test_a_pricier_model_buys_a_bigger_budget():
    """The ordering is the whole product. Sonnet over Haiku, Opus over Sonnet:
    if this inverts, a premium turn is reserving less than a cheap one."""
    haiku = chunk_multiplier("claude-haiku-4-5")
    sonnet = chunk_multiplier("claude-sonnet-5")
    opus = chunk_multiplier("claude-opus-5")
    assert haiku is not None and sonnet is not None and opus is not None
    assert haiku < sonnet < opus


def test_an_hourly_card_is_cheaper_off_peak():
    """A model priced by the clock genuinely costs less at some hours, and a
    budget that ignores that over-reserves for most of the day."""
    assert blended_rate(_SCHEDULED, at=_OFF_PEAK) < blended_rate(_SCHEDULED, at=_PEAK)


def test_the_mix_weights_reads_far_above_input():
    """Traffic is mostly cache reads, so the blended rate has to sit nearer the
    read rate than the input rate. A blend that landed at or above input would
    mean the mix was being ignored."""
    from src.llms.pricing_utils import find_model_pricing

    card = find_model_pricing("claude-sonnet-5", "anthropic")
    rate = blended_rate("claude-sonnet-5")
    assert rate is not None and card is not None
    assert card["cached_input"] < rate < card["input"]


@pytest.mark.parametrize("model", ["claude-opus-5", "deepseek-v4-pro", "gpt-5.6-sol"])
def test_a_multiplier_is_a_coarse_figure(model):
    """Snapped, because a budget is read by people. Anything below the baseline
    lands on a tenth, anything above on a half — a rate that drifts by a cent
    must not restate every reservation on the platform."""
    m = chunk_multiplier(model)
    assert m is not None
    step = 0.1 if m < 1 else 0.5
    assert round(m / step) == pytest.approx(m / step)


@pytest.mark.parametrize(
    "raw, expected",
    [
        (0.32, 0.3),
        (0.38, 0.4),
        (1.20, 1.0),
        (1.30, 1.5),
        (2.74, 2.5),
        (2.76, 3.0),
        (8.24, 8.0),
        (8.26, 8.5),
    ],
)
def test_a_multiplier_lands_on_the_nearest_rung(monkeypatch, raw, expected):
    """Nearest, not up and not down.

    The granularity check above passes just as happily on a floor, so the
    direction needs its own witness. Synthetic ratios rather than manifest
    models: a repricing must never fail this. No case sits on an exact tie —
    there both rungs are equally near, so the direction is free to change.
    """
    monkeypatch.setattr(
        pricing_utils, "blended_rate", lambda *a, **k: raw * _BASELINE_BLENDED_RATE
    )
    assert chunk_multiplier("any-model") == pytest.approx(expected)
