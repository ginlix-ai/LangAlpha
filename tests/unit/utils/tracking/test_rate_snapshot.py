"""The usage row records the rate card it was billed at.

A row holding only token counts and a total is repriced by any later manifest
edit, so a rate change silently restates turns that were metered correctly at
the time. These pin that the card travels with the row.

Rates here are synthetic. Asserting shipped numbers would make every vendor
repricing a test edit, which is the same churn the snapshot exists to absorb.
"""

from unittest.mock import patch
from uuid import uuid4

from src.utils.tracking.core import calculate_cost_from_per_call_records

PRICING_MODULE = "src.llms.pricing_utils"

FLAT = {"input": 10.0, "output": 40.0, "unit": "per_1m_tokens"}
SCHEDULED = {
    **FLAT,
    "peak_utc": [[1, 4], [6, 10]],
    "off_peak": {"input": 5.0, "output": 20.0},
}


def _record(model="a-model", at="2026-01-01T02:30:00+00:00", started_at=None):
    return {
        "model_name": model,
        "served_model": model,
        "usage": {"input_tokens": 1_000_000, "output_tokens": 0, "total_tokens": 1_000_000},
        "billing_type": "platform",
        "started_at": started_at or at,
        "timestamp": at,
        "run_id": str(uuid4()),
        "parent_run_id": None,
    }


def _priced(records, pricing=FLAT):
    with patch(f"{PRICING_MODULE}.find_model_pricing", return_value=pricing):
        return calculate_cost_from_per_call_records(records)


class TestTheCardTravelsWithTheRow:
    def test_the_applied_rates_are_stored_verbatim(self):
        rates = _priced([_record()])["by_model"]["a-model"]["rates"]
        assert len(rates) == 1
        assert rates[0]["pricing"] == FLAT

    def test_a_snapshot_is_not_a_live_reference_to_the_manifest(self):
        """Persisted rows must not alias a dict the manifest loader still owns."""
        card = dict(FLAT)
        rates = _priced([_record()], pricing=card)["by_model"]["a-model"]["rates"]
        card["input"] = 999.0
        assert rates[0]["pricing"]["input"] == FLAT["input"]

    def test_repeated_calls_collapse_into_one_entry(self):
        """One entry per distinct card, not per call: a long turn is hundreds of
        calls and the row is already the widest thing we persist."""
        result = _priced([_record(), _record(), _record()])
        rates = result["by_model"]["a-model"]["rates"]
        assert len(rates) == 1
        assert rates[0]["call_count"] == 3
        assert rates[0]["cost"] == result["by_model"]["a-model"]["total_cost"]

    def test_an_unscheduled_card_carries_no_window(self):
        rates = _priced([_record()])["by_model"]["a-model"]["rates"]
        assert "window" not in rates[0]

    def test_models_keep_their_own_cards(self):
        result = _priced([_record("a-model"), _record("b-model")])
        assert len(result["by_model"]["a-model"]["rates"]) == 1
        assert len(result["by_model"]["b-model"]["rates"]) == 1

    def test_an_unpriced_model_stamps_nothing(self):
        """Tokens still aggregate on a miss; there is just no card to record."""
        result = _priced([_record()], pricing=None)
        entry = result["by_model"]["a-model"]
        assert entry["total_tokens"] == 1_000_000
        assert "rates" not in entry


class TestATurnThatStraddlesAWindow:
    def test_both_cards_are_recorded_and_sum_to_the_total(self):
        result = _priced(
            [
                _record(at="2026-01-01T00:50:00+00:00"),  # off-peak
                _record(at="2026-01-01T01:10:00+00:00"),  # peak
            ],
            pricing=SCHEDULED,
        )
        entry = result["by_model"]["a-model"]
        rates = sorted(entry["rates"], key=lambda r: r["cost"])

        assert [r["window"] for r in rates] == ["off_peak", "peak"]
        assert sum(r["cost"] for r in rates) == entry["total_cost"]
        assert sum(r["call_count"] for r in rates) == entry["call_count"]

    def test_the_stored_cards_carry_the_rates_that_were_charged(self):
        result = _priced(
            [
                _record(at="2026-01-01T00:50:00+00:00"),
                _record(at="2026-01-01T01:10:00+00:00"),
            ],
            pricing=SCHEDULED,
        )
        by_window = {r["window"]: r["pricing"] for r in result["by_model"]["a-model"]["rates"]}
        assert by_window["peak"]["input"] == SCHEDULED["input"]
        assert by_window["off_peak"]["input"] == SCHEDULED["off_peak"]["input"]

    def test_calls_in_one_window_stay_one_entry(self):
        result = _priced(
            [
                _record(at="2026-01-01T01:10:00+00:00"),
                _record(at="2026-01-01T02:10:00+00:00"),
            ],
            pricing=SCHEDULED,
        )
        assert len(result["by_model"]["a-model"]["rates"]) == 1


class TestTheKeySpaceMarker:
    """Says which namespace by_model is keyed in, so a reader can tell a row
    written by this build from one written before it. Derived per payload rather
    than asserted, because a turn can mix stamped and unstamped clients.
    """

    def _shape(self, records, pricing=FLAT):
        return _priced(records, pricing=pricing)["model_key_shape"]

    def test_every_stamped_record_reads_as_manifest(self):
        stamped = _record()
        stamped["pricing_model_id"] = "a-model-id"
        stamped["pricing_provider"] = "a-provider"
        assert self._shape([stamped, dict(stamped)]) == "manifest"

    def test_no_stamp_anywhere_reads_as_vendor(self):
        """A consumer-supplied client keys on the echo, which is a different
        namespace even when the string happens to match."""
        assert self._shape([_record(), _record()]) == "vendor"

    def test_one_of_each_reads_as_mixed(self):
        """A turn can swap clients mid-flight. Reporting either pure shape here
        would tell a reader the whole row is safe to fold when half of it is not.
        """
        stamped = _record()
        stamped["pricing_model_id"] = "a-model-id"
        assert self._shape([stamped, _record()]) == "mixed"

    def test_an_empty_turn_still_declares_a_shape(self):
        """The degraded and no-usage paths return this payload too; a missing
        marker there is indistinguishable from a row an older writer produced.
        """
        assert self._shape([]) == "manifest"


class TestAStraddlingCallIsMarked:
    """The two windows differ by 2x and the whole call bills at one of them, so
    the unbilled end is the error. Nothing acts on this yet; it is recorded so
    the exposure is a number before anyone argues about which end is right.
    """

    def _call(self, start, end):
        return _record(started_at=f"2026-01-01T{start}+00:00", at=f"2026-01-01T{end}+00:00")

    def test_a_call_crossing_into_peak_is_marked(self):
        result = _priced([self._call("00:50:00", "01:10:00")], pricing=SCHEDULED)
        call = result["per_call_costs"][0]
        assert call["window"] == "peak"
        assert call["straddled"] is True
        assert result["by_model"]["a-model"]["rates"][0]["straddled_calls"] == 1

    def test_a_call_crossing_out_of_peak_is_marked(self):
        result = _priced([self._call("09:50:00", "10:10:00")], pricing=SCHEDULED)
        assert result["per_call_costs"][0]["window"] == "off_peak"
        assert result["per_call_costs"][0]["straddled"] is True

    def test_a_call_inside_one_window_is_not_marked(self):
        result = _priced([self._call("02:00:00", "02:05:00")], pricing=SCHEDULED)
        assert "straddled" not in result["per_call_costs"][0]
        assert "straddled_calls" not in result["by_model"]["a-model"]["rates"][0]

    def test_a_record_with_no_start_claims_no_straddle(self):
        """Absent evidence is not evidence of a crossing. Reading a missing
        stamp as 'peak' would mark every legacy off-peak call as straddling."""
        legacy = _record(at="2026-01-01T12:00:00+00:00")
        del legacy["started_at"]
        result = _priced([legacy], pricing=SCHEDULED)
        assert result["per_call_costs"][0]["window"] == "off_peak"
        assert "straddled" not in result["per_call_costs"][0]

    def test_an_unscheduled_model_carries_no_window_fields(self):
        """Every other model keeps the row shape it already had."""
        call = _priced([_record()])["per_call_costs"][0]
        assert not {"window", "started_at", "straddled"} & set(call)

    def test_straddling_calls_accumulate_per_window(self):
        result = _priced(
            [
                self._call("00:50:00", "01:10:00"),  # into peak, straddles
                self._call("00:40:00", "01:20:00"),  # into peak, straddles
                self._call("01:30:00", "01:35:00"),  # inside peak
            ],
            pricing=SCHEDULED,
        )
        peak = result["by_model"]["a-model"]["rates"][0]
        assert peak["call_count"] == 3
        assert peak["straddled_calls"] == 2


class TestTheAnchorDecidesWhichStampIsRead:
    _STRADDLE = {"started_at": "2026-01-01T00:50:00+00:00", "at": "2026-01-01T01:10:00+00:00"}

    def test_completion_prices_on_the_end_of_the_call(self):
        result = _priced([_record(**self._STRADDLE)], pricing=SCHEDULED)
        assert result["by_model"]["a-model"]["rates"][0]["window"] == "peak"

    def test_request_prices_on_the_start_of_the_call(self):
        card = {**SCHEDULED, "schedule_anchor": "request"}
        result = _priced([_record(**self._STRADDLE)], pricing=card)
        assert result["by_model"]["a-model"]["rates"][0]["window"] == "off_peak"

    def test_a_record_written_before_started_at_existed_still_prices(self):
        """Rows already in Redis and the checkpointer have no start stamp."""
        legacy = _record()
        del legacy["started_at"]
        card = {**SCHEDULED, "schedule_anchor": "request"}
        result = _priced([legacy], pricing=card)
        assert result["by_model"]["a-model"]["rates"][0]["window"] == "peak"

    def test_a_missing_anchor_stamp_reads_the_other_end_before_defaulting(self):
        """The stamp a legacy row does carry is a real observation of the same
        call, so it places it. Dropping straight to the no-stamp peak default
        would overcharge a row we can read, and then mark it straddled for
        disagreeing with the default it was never compared against.
        """
        legacy = _record(at="2026-01-01T12:00:00+00:00")
        del legacy["started_at"]
        card = {**SCHEDULED, "schedule_anchor": "request"}
        result = _priced([legacy], pricing=card)
        assert result["by_model"]["a-model"]["rates"][0]["window"] == "off_peak"
        assert "straddled" not in result["per_call_costs"][0]
