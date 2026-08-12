"""Interval bijection and parity with the legacy market_hours table."""

import pytest

from src.market_protocol.enums import OHLCV_SCHEMAS
from src.market_protocol.intervals import (
    is_intraday_schema,
    legacy_for_schema,
    schema_for_legacy,
    schema_seconds,
)
from src.utils.market_hours import _INTERVAL_SECONDS, interval_seconds


def test_bijection():
    for schema in OHLCV_SCHEMAS:
        assert schema_for_legacy(legacy_for_schema(schema)) == schema


def test_parity_with_market_hours():
    """schema_seconds must agree with the legacy staleness table exactly."""
    for legacy, seconds in _INTERVAL_SECONDS.items():
        assert schema_seconds(schema_for_legacy(legacy)) == seconds
        assert schema_seconds(schema_for_legacy(legacy)) == interval_seconds(legacy)
    # And cover the same interval set — no legacy interval left unmapped.
    assert {legacy_for_schema(s) for s in OHLCV_SCHEMAS} == set(_INTERVAL_SECONDS)


def test_intraday_classification():
    assert is_intraday_schema("ohlcv-1s")
    assert is_intraday_schema("ohlcv-4h")
    assert not is_intraday_schema("ohlcv-1d")


@pytest.mark.parametrize("bad", ["1m", "ohlcv-1min", "daily", ""])
def test_unknown_legacy_raises(bad):
    with pytest.raises(ValueError):
        schema_for_legacy(bad)
