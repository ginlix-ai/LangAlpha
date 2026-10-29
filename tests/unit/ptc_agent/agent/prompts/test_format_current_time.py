"""``format_current_time`` stamps the turn's clock and must never fail a turn."""

from datetime import UTC, datetime

import pytest

from ptc_agent.agent.prompts import format_current_time

_NOW = datetime(2026, 2, 23, 20, 0, tzinfo=UTC)


def test_a_named_zone_converts_the_stamp():
    assert (
        format_current_time(_NOW, "America/New_York")
        == "3:00 PM EST, Monday, February 23, 2026"
    )


@pytest.mark.parametrize(
    "zone",
    ["Not/AZone", "../etc/passwd", "America", "x" * 300],
    ids=["unknown", "path-like", "directory", "overlong"],
)
def test_an_unusable_zone_keeps_the_clock_it_was_given(zone):
    """ZoneInfo refuses the last three as ValueError or OSError rather than as
    an unknown name, and each must fall back the same way."""
    assert format_current_time(_NOW, zone) == format_current_time(_NOW)
