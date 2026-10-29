"""Which rate card is in force at a given moment.

Pinned against synthetic cards, never the shipped manifest. A vendor changing its
hours should churn the manifest shape test and leave this contract alone, and
asserting real rates here would turn every repricing into a test edit.

The hazard this guards is quiet: a wrong window bills a real turn at the wrong
rate and looks exactly like a correct one in every log we keep.
"""

from datetime import datetime, timedelta, timezone

from src.llms.pricing_utils import (
    has_schedule,
    parse_stamp,
    resolve_schedule,
    schedule_anchor,
)

# Two peak blocks with a trough between them, which is the shape vendors
# actually publish and the one a single (start, end) pair would get wrong.
CARD = {
    "input": 10.0,
    "cached_input": 1.0,
    "output": 40.0,
    "unit": "per_1m_tokens",
    "peak_utc": [[1, 4], [6, 10]],
    "off_peak": {"input": 5.0, "cached_input": 0.5, "output": 20.0},
}


def _at(hour, minute=0, offset_hours=0):
    return datetime(
        2026, 1, 1, hour, minute, tzinfo=timezone(timedelta(hours=offset_hours))
    )


def _window(at):
    return resolve_schedule(CARD, at)[1]


class TestWindowBoundaries:
    def test_a_peak_block_is_half_open(self):
        """01:00 is peak and 04:00 is not, or an hour bills twice or never."""
        assert _window(_at(0, 59)) == "off_peak"
        assert _window(_at(1, 0)) == "peak"
        assert _window(_at(3, 59)) == "peak"
        assert _window(_at(4, 0)) == "off_peak"

    def test_the_trough_between_two_peak_blocks_is_off_peak(self):
        """A schedule read as one span from the first start to the last end
        would bill these hours at double."""
        assert _window(_at(4, 30)) == "off_peak"
        assert _window(_at(5, 30)) == "off_peak"

    def test_the_second_block_is_honored(self):
        assert _window(_at(6, 0)) == "peak"
        assert _window(_at(9, 59)) == "peak"
        assert _window(_at(10, 0)) == "off_peak"

    def test_the_hours_outside_every_block_are_off_peak(self):
        for hour in (0, 11, 12, 18, 23):
            assert _window(_at(hour)) == "off_peak", hour


class TestTheStampIsReadAsUTC:
    def test_a_non_utc_stamp_is_converted_not_truncated(self):
        """Reading the local hour off the stamp would put this in off-peak."""
        assert _at(11, 30, offset_hours=9).hour == 11
        assert _window(_at(11, 30, offset_hours=9)) == "peak"  # 02:30 UTC

    def test_a_naive_stamp_is_read_as_utc(self):
        """Records written before the tz-aware writer carry no offset and came
        from hosts running UTC. Any other reading reprices that history."""
        assert _window(parse_stamp("2026-01-01T02:30:00")) == "peak"
        assert _window(parse_stamp("2026-01-01T12:30:00")) == "off_peak"

    def test_an_aware_stamp_keeps_its_offset(self):
        assert _window(parse_stamp("2026-01-01T11:30:00+09:00")) == "peak"

    def test_an_unreadable_stamp_is_no_stamp(self):
        for junk in (None, "", "not-a-time", "2026-13-45T99:99:99"):
            assert parse_stamp(junk) is None


class TestTheCardHandedToTheEngine:
    def test_the_schedule_keys_are_stripped(self):
        """They describe when a card applies, not what it charges. Left in, they
        reach the engine, the logs, and the snapshot stored on the usage row."""
        for at in (_at(2), _at(12)):
            card, _ = resolve_schedule(CARD, at)
            assert not {"peak_utc", "off_peak", "schedule_anchor"} & set(card)

    def test_off_peak_overrides_only_what_it_names(self):
        """It carries the difference, so a key it omits still has to resolve."""
        card, window = resolve_schedule(CARD, _at(12))
        assert window == "off_peak"
        assert card["input"] == 5.0
        assert card["unit"] == "per_1m_tokens"

    def test_peak_rates_live_at_the_top_level(self):
        """Every reader predating schedules -- the price tier on the model
        picker, the aggregate display path -- reads the top level untouched."""
        card, window = resolve_schedule(CARD, _at(2))
        assert window == "peak"
        assert card["input"] == CARD["input"]
        assert card["output"] == CARD["output"]

    def test_the_source_card_is_never_mutated(self):
        resolve_schedule(CARD, _at(12))
        assert CARD["input"] == 10.0
        assert "off_peak" in CARD


class TestTheCommonPathIsUntouched:
    def test_a_card_with_no_schedule_is_returned_as_is(self):
        """Almost every model. Returning the same object keeps the check ahead
        of the copy, so the overwhelming case pays nothing."""
        flat = {"input": 3.0, "output": 15.0}
        card, window = resolve_schedule(flat, _at(2))
        assert card is flat
        assert window is None

    def test_has_schedule_is_false_for_the_ordinary_shapes(self):
        assert not has_schedule(None)
        assert not has_schedule({})
        assert not has_schedule({"input": 3.0})
        assert has_schedule(CARD)


class TestTheAnchor:
    def test_completion_is_the_default(self):
        """No vendor documents which end of a streamed call picks the window, so
        the default is the stamp we have always written."""
        assert schedule_anchor(CARD) == "completion"
        assert schedule_anchor(None) == "completion"

    def test_the_manifest_can_name_the_other_end(self):
        assert schedule_anchor({**CARD, "schedule_anchor": "request"}) == "request"


class TestMissingTimeChargesPeak:
    def test_no_stamp_resolves_to_peak(self):
        """A record with no usable time still has to price. Guessing downward
        would make an unparseable stamp a discount."""
        card, window = resolve_schedule(CARD, None)
        assert window == "peak"
        assert card["input"] == CARD["input"]


class TestAnUnreadableScheduleCannotDiscount:
    """The committed manifest has a shape test, but a rate card is hand-authored
    config that can change without one, so the engine validates its own windows.

    Every case here is a card a maintainer can plausibly write. All of them must
    land on peak: an unreadable window says nothing about where the call sat, and
    the alternative is handing out a discount on the strength of a typo.
    """

    def test_a_discount_without_a_window_still_arms_the_schedule(self):
        """The half a maintainer writes first. Keying has_schedule on peak_utc
        alone would read this as an ordinary card and bill peak around the clock
        with nothing logged, invisible to every check that selects on the window.
        """
        half_written = {"input": 10.0, "output": 40.0, "off_peak": {"input": 5.0}}
        assert has_schedule(half_written)
        card, window = resolve_schedule(half_written, _at(12))
        assert window == "peak"
        assert card["input"] == 10.0

    def test_a_window_that_wraps_midnight_is_rejected_not_ignored(self):
        """``[[22, 2]]`` is the natural way to write a 22:00-02:00 peak and no
        hour satisfies it, so trusting it would bill the discount 24/7.
        """
        wrapping = {**CARD, "peak_utc": [[22, 2]]}
        assert resolve_schedule(wrapping, _at(23))[1] == "peak"
        assert resolve_schedule(wrapping, _at(12))[1] == "peak"

    def test_a_malformed_window_prices_instead_of_raising(self):
        """A raise here escapes into the caller's blanket except, which zeroes the
        computed cost for every model in the turn, not just this one.
        """
        bad_windows = ([1, 4], [[1, "4"]], [[4, 1]], [["1", "4"]], [[1, 2, 3]], "1-4")
        # bool subclasses int, so these read as hours 1 and 0 under an isinstance
        # check and pass the range test. ``[[0, True]]`` is the one that costs money:
        # it installs [0,1) and discounts the other 23 hours.
        bad_bools = ([[True, 4]], [[False, 4]], [[0, True]], [[True, False]])
        # A scalar is the container case, not a window case: it raises on the ``for``
        # statement itself, ahead of every per-window rule. A string is iterable and so
        # never exercised this.
        bad_containers = (5, True, 1.5, {"start": 1})
        for bad in bad_windows + bad_bools + bad_containers:
            card, window = resolve_schedule({**CARD, "peak_utc": bad}, _at(12))
            assert window == "peak", bad
            assert card["input"] == 10.0, bad
            # 02:00 is inside the card's real peak block, so a boolean bound that
            # survived would show up here as an off_peak read rather than a no-op.
            assert resolve_schedule({**CARD, "peak_utc": bad}, _at(2))[1] == "peak", bad

    def test_one_readable_window_survives_an_unreadable_sibling(self):
        """Dropping the whole schedule would be a second failure on top of the
        typo; the windows that still parse keep deciding.
        """
        mixed = {**CARD, "peak_utc": [[1, 4], "nonsense"]}
        assert resolve_schedule(mixed, _at(2))[1] == "peak"
        assert resolve_schedule(mixed, _at(12))[1] == "off_peak"

    def test_a_malformed_off_peak_override_prices_instead_of_raising(self):
        """The discount block is the other half of the card and was unguarded: a
        truthy non-mapping reached dict.update and raised, which is the one
        outcome the window validation above exists to prevent.
        """
        for bad in ("half price", ["input", 0.5], 0.5, [["input", 0.5]]):
            card, window = resolve_schedule({**CARD, "off_peak": bad}, _at(12))
            assert window == "peak", bad
            assert card["input"] == 10.0, bad

    def test_an_absent_off_peak_still_reads_as_off_peak(self):
        """Absent is not malformed. A card with hours but no discount charges the
        same rates outside them, and mislabelling that window as peak would make
        every straddle comparison wrong.
        """
        no_discount = {k: v for k, v in CARD.items() if k != "off_peak"}
        card, window = resolve_schedule(no_discount, _at(12))
        assert window == "off_peak"
        assert card["input"] == 10.0


# A card whose peak block reaches past 16:00 UTC, which is the only stretch of
# the week where the UTC and Beijing calendars name different days. DeepSeek's
# real windows both close before it, so a card built from the shipped manifest
# cannot tell a UTC weekday from a Beijing one -- the two price identically at
# all 168 hours. Synthetic, like every card in this file, so the contract is
# pinned where it is visible rather than where today's rates happen to hide it.
LATE_CARD = {
    "input": 10.0,
    "output": 40.0,
    "unit": "per_1m_tokens",
    "peak_utc": [[15, 20]],
    "peak_days": [1, 2, 3, 4, 5],
    "peak_days_utc_offset": 8,
    "off_peak": {"input": 5.0, "output": 20.0},
}

WEEKDAY_CARD = {**CARD, "peak_days": [1, 2, 3, 4, 5], "peak_days_utc_offset": 8}


def _on(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


class TestPeakDays:
    """A vendor that runs its peak windows on weekdays only.

    Without a day axis the discount is billed at the peak rate for two days a
    week, and every hour-based assertion above still passes.
    """

    def test_a_weekend_hour_inside_a_window_is_off_peak(self):
        # 2026-01-03 is a Saturday, 02:00 UTC is inside the first block.
        assert resolve_schedule(WEEKDAY_CARD, _on(2026, 1, 3, 2))[1] == "off_peak"
        assert resolve_schedule(WEEKDAY_CARD, _on(2026, 1, 4, 7))[1] == "off_peak"

    def test_the_same_hour_on_a_weekday_is_still_peak(self):
        """The day rule must not leak into the days the windows do apply on."""
        assert resolve_schedule(WEEKDAY_CARD, _on(2026, 1, 5, 2))[1] == "peak"
        assert resolve_schedule(WEEKDAY_CARD, _on(2026, 1, 1, 7))[1] == "peak"

    def test_a_card_without_peak_days_is_unrestricted(self):
        """The overwhelming majority of scheduled cards name no days, and they
        must keep billing peak on a Saturday inside a window."""
        assert resolve_schedule(CARD, _on(2026, 1, 3, 2))[1] == "peak"

    def test_the_day_is_read_on_the_vendors_clock_not_utc(self):
        """16:00Z Friday is already Saturday in Beijing, and 16:00Z Sunday is
        already Monday. Read off UTC, both land on the wrong rate."""
        assert resolve_schedule(LATE_CARD, _on(2026, 1, 2, 15, 59))[1] == "peak"
        assert resolve_schedule(LATE_CARD, _on(2026, 1, 2, 16, 0))[1] == "off_peak"
        assert resolve_schedule(LATE_CARD, _on(2026, 1, 4, 15, 59))[1] == "off_peak"
        assert resolve_schedule(LATE_CARD, _on(2026, 1, 4, 16, 0))[1] == "peak"

    def test_days_without_an_offset_are_counted_in_utc(self):
        """Zero is the default, so a card that names days without naming a
        calendar still restricts them -- it does not lose the restriction."""
        utc_card = {**CARD, "peak_days": [1, 2, 3, 4, 5]}
        assert resolve_schedule(utc_card, _on(2026, 1, 3, 2))[1] == "off_peak"
        assert resolve_schedule(utc_card, _on(2026, 1, 5, 2))[1] == "peak"

    def test_an_unreadable_day_list_charges_peak_rather_than_discounting(self):
        """Same direction as an unreadable window: never guess downward off a
        card that was just found malformed."""
        for broken in ([], "1-5", [0], [8], [True], [1, "2"], 5):
            card = {**CARD, "peak_days": broken}
            assert resolve_schedule(card, _on(2026, 1, 3, 2))[1] == "peak", broken

    def test_an_unreadable_offset_falls_back_to_utc(self):
        for broken in ("8", 99, True, None):
            card = {**WEEKDAY_CARD, "peak_days_utc_offset": broken}
            assert resolve_schedule(card, _on(2026, 1, 3, 2))[1] == "off_peak", broken

    def test_the_day_keys_never_reach_the_rate_card(self):
        """They describe when the card applies, not what it charges, so a
        snapshot of the priced card must not carry them."""
        card, _ = resolve_schedule(WEEKDAY_CARD, _on(2026, 1, 5, 2))
        assert "peak_days" not in card and "peak_days_utc_offset" not in card
