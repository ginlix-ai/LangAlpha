"""The mid-run spend figure and the billed figure are one arithmetic.

``platform_usd_total`` gates a live run; ``calculate_cost_from_per_call_records``
bills it. They read the same buffer, so any divergence is the gate stopping a
run over money the turn was never charged, or failing to stop one it was. The
tracker used to reimplement the pricing walk to keep the read O(1); it now
prices the tail through the billing pass itself, and these pin that they agree
across the shapes that made the two paths differ at all — the ``billing_type``
default, a non-platform call, and a manifest miss — under the append-then-read
interleave the credit gate actually produces.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.utils.tracking.core import calculate_cost_from_per_call_records
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

_BASE = datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)


def _record(i, model, *, billing_type="platform"):
    # Five hours apart so the series straddles any peak/off-peak window a
    # scheduled card names, rather than landing entirely inside one.
    started = _BASE + timedelta(hours=i * 5)
    record = {
        "model_name": model,
        "served_model": model,
        "pricing_model_id": model,
        "pricing_provider": "anthropic",
        "usage": {
            "input_tokens": 3_000 + i * 71,
            "output_tokens": 400 + i * 13,
            "cached_tokens": i * 29,
            "cache_5m_tokens": i * 7,
            "cache_1h_tokens": 0,
        },
        "started_at": started.isoformat(),
        "timestamp": (started + timedelta(seconds=4)).isoformat(),
        "run_id": f"run-{i}",
        "parent_run_id": None,
    }
    if billing_type is not None:
        record["billing_type"] = billing_type
    return record


def test_the_platform_fixture_actually_prices():
    """Positive control. Two of the cases below assert that a priced total and
    a billed total agree, and two assert a total is zero; if the fixture model
    ever leaves the manifest, the first pair silently becomes 0 == 0 and stops
    telling "byok was excluded" apart from "nothing priced at all". No value is
    pinned here, only that pricing happened."""
    priced = calculate_cost_from_per_call_records(
        [_record(i, "claude-opus-5") for i in range(6)]
    )
    assert priced["platform_cost"] > 0


@pytest.mark.parametrize(
    "records",
    [
        pytest.param(
            [_record(i, "claude-opus-5") for i in range(6)],
            id="stamped-platform",
        ),
        pytest.param(
            [_record(i, "claude-opus-5", billing_type=None) for i in range(6)],
            id="billing-type-absent-defaults-to-platform",
        ),
        pytest.param(
            [_record(i, "claude-opus-5", billing_type="byok") for i in range(6)],
            id="own-key-call-contributes-nothing",
        ),
        pytest.param(
            [_record(i, "no-such-model-anywhere") for i in range(4)],
            id="manifest-miss-prices-zero-both-ways",
        ),
    ],
)
def test_the_running_total_matches_the_billed_pass(records):
    tracker = PerCallTokenTracker()
    for record in records:
        # Read between appends: the cursor has to survive the interleaving,
        # not just a single pass at the end.
        tracker.per_call_records.append(record)
        tracker.platform_usd_total()

    assert tracker.platform_usd_total() == pytest.approx(
        calculate_cost_from_per_call_records(records)["platform_cost"], rel=1e-12
    )


def test_reset_clears_the_running_total_and_its_cursor():
    tracker = PerCallTokenTracker()
    tracker.per_call_records.append(_record(0, "claude-opus-5"))
    assert tracker.platform_usd_total() > 0

    tracker.reset()
    assert tracker.platform_usd_total() == 0.0


def test_a_record_that_cannot_be_priced_is_skipped_rather_than_freezing_the_meter():
    """Mid-run, inability to price must not stop the metering.

    The cursor advances past a failed tail instead of retrying it: a record
    that raises here raises in the billing pass too, so retrying would hold
    this figure at its last good value for the rest of the turn and quietly
    ungate a run that is still spending.
    """
    tracker = PerCallTokenTracker()
    tracker.per_call_records.append({"no": "model_name here"})
    assert tracker.platform_usd_total() == 0.0

    tracker.per_call_records.append(_record(1, "claude-opus-5"))
    assert tracker.platform_usd_total() > 0
