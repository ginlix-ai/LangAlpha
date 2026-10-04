"""Interval schemas: the bijection with legacy spellings and their lengths."""

import pytest

from market_protocol.enums import OHLCV_SCHEMAS
from market_protocol.intervals import (
    is_intraday_schema,
    legacy_for_schema,
    schema_for_legacy,
    schema_seconds,
    to_schema,
)


def test_bijection():
    for schema in OHLCV_SCHEMAS:
        assert schema_for_legacy(legacy_for_schema(schema)) == schema


@pytest.mark.parametrize(
    "schema,legacy,seconds",
    [
        ("ohlcv-1s", "1s", 1), ("ohlcv-1m", "1min", 60), ("ohlcv-5m", "5min", 300),
        ("ohlcv-15m", "15min", 900), ("ohlcv-30m", "30min", 1800), ("ohlcv-1h", "1hour", 3600),
        ("ohlcv-4h", "4hour", 14400), ("ohlcv-1d", "1day", 86400),
    ],
)
def test_every_schema_has_its_legacy_spelling_and_length(schema, legacy, seconds):
    assert legacy_for_schema(schema) == legacy
    assert schema_seconds(schema) == seconds


def test_the_table_covers_every_schema():
    assert {legacy_for_schema(s) for s in OHLCV_SCHEMAS} == {
        "1s", "1min", "5min", "15min", "30min", "1hour", "4hour", "1day",
    }


def test_intraday_classification():
    assert is_intraday_schema("ohlcv-1s")
    assert is_intraday_schema("ohlcv-4h")
    assert not is_intraday_schema("ohlcv-1d")


@pytest.mark.parametrize("bad", ["1m", "ohlcv-1min", "daily", ""])
def test_unknown_legacy_raises(bad):
    with pytest.raises(ValueError):
        schema_for_legacy(bad)


@pytest.mark.parametrize("spelling", ["ohlcv-1m", "1m", "1min", "1MIN"])
def test_to_schema_accepts_every_spelling(spelling):
    assert to_schema(spelling) == "ohlcv-1m"


def test_to_schema_rejects_unknown():
    with pytest.raises(ValueError):
        to_schema("3min")


@pytest.mark.parametrize("schema", OHLCV_SCHEMAS)
def test_to_schema_reads_every_table_spelling(schema):
    width = schema.removeprefix("ohlcv-")
    legacy = legacy_for_schema(schema)
    for spelling in (schema, schema.upper(), width, legacy, legacy.upper()):
        assert to_schema(spelling) == schema, spelling
    if not width.endswith("m"):
        assert to_schema(width.upper()) == schema


@pytest.mark.parametrize("spelling", ["1M", "5M", " 15M ", "30M"])
def test_to_schema_refuses_a_bare_uppercase_m_as_a_month(spelling):
    # Lowercased, "1M" would route a monthly request to one-minute bars.
    with pytest.raises(ValueError, match="month"):
        to_schema(spelling)
