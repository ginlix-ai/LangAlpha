"""What a spend budget for one model is worth against another's.

Every assertion here is a relationship, never a rate: the manifest's numbers
move whenever a vendor reprices, and a test that pins them turns a price cut
into a failure. What must hold is the ordering and the fallbacks.
"""

from datetime import datetime, timezone

import pytest

from src.llms import pricing_utils
from src.llms.llm import ModelConfig
from src.llms.pricing_utils import (
    _BASELINE_BLENDED_RATE,
    blended_rate,
    chunk_multiplier,
    find_model_pricing,
    resolve_pricing_identity,
    resolve_schedule,
)

# Two stamps straddling a peak boundary: 02:00 UTC sits inside a 01-04 window and
# 12:00 UTC outside any, on a weekday.
_PEAK = datetime(2026, 8, 27, 2, 0, tzinfo=timezone.utc)  # a Thursday
_OFF_PEAK = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)


def _visible_models() -> list[str]:
    return list(ModelConfig().get_model_metadata())


def _first_scheduled_model():
    """The first visible model whose card has hours that the two stamps above
    fall either side of, or None when the manifest carries no such card."""
    for model in _visible_models():
        provider, pricing_id = resolve_pricing_identity(model)
        card = find_model_pricing(pricing_id, provider)
        if not card or not pricing_utils.has_schedule(card):
            continue
        if (
            resolve_schedule(card, _PEAK)[1] == "peak"
            and resolve_schedule(card, _OFF_PEAK)[1] == "off_peak"
        ):
            return model
    return None


_SCHEDULED = _first_scheduled_model()
# Every visible model with rates to size a budget from, read at one fixed stamp
# so the set does not move with the hour the suite runs.
_PRICED = [m for m in _visible_models() if blended_rate(m, at=_PEAK) is not None]

needs_schedule = pytest.mark.skipif(
    _SCHEDULED is None, reason="no visible model is priced by the hour"
)


def test_a_model_with_no_card_has_no_opinion():
    """None, not 1.0. The caller decides what a missing rate means, and the
    gate's answer — reserve at baseline — is a different statement from
    'this model costs the baseline'."""
    assert chunk_multiplier("not-a-real-model-anywhere") is None
    assert blended_rate("not-a-real-model-anywhere") is None


def test_a_pricier_model_buys_a_bigger_budget():
    """The ordering is the whole product. A model whose call costs more never
    reserves less than one that costs less: if this inverts, a premium turn is
    reserving less than a cheap one.

    Ordered by the blended rate the multiplier is sized from, not by the 1-5
    price rating, which blends list rates 3:1 rather than by cache mix: two
    models can rank differently on the two without either being mispriced."""
    sized = [
        (rate, chunk_multiplier(model, at=_PEAK), model)
        for model in _visible_models()
        if (rate := blended_rate(model, at=_PEAK)) is not None
    ]
    assert len({m for _, m, _ in sized}) > 1, "every model snapped to one rung"
    for rate, multiplier, model in sized:
        for other_rate, other_multiplier, other in sized:
            if rate < other_rate:
                assert multiplier <= other_multiplier, (model, other)


@needs_schedule
def test_an_hourly_card_is_cheaper_off_peak():
    """A model priced by the clock genuinely costs less at some hours, and a
    budget that ignores that over-reserves for most of the day."""
    assert blended_rate(_SCHEDULED, at=_OFF_PEAK) < blended_rate(_SCHEDULED, at=_PEAK)


@pytest.mark.parametrize("model", _PRICED)
def test_the_mix_weights_reads_far_above_input(model):
    """Traffic is mostly cache reads, so the blended rate has to sit nearer the
    read rate than the input rate. A blend that landed at or above input would
    mean the mix was being ignored. The card and the blend are read at the
    same stamp, so an hourly card is held to the rates in force then."""
    provider, pricing_id = resolve_pricing_identity(model)
    card = resolve_schedule(find_model_pricing(pricing_id, provider), _PEAK)[0]
    rate = blended_rate(model, at=_PEAK)
    if "cached_input" not in card or card["cached_input"] >= card["input"]:
        pytest.skip("this card has no cache discount to weigh")
    assert rate is not None
    assert card["cached_input"] < rate < card["input"]


@pytest.mark.parametrize("model", _PRICED)
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
