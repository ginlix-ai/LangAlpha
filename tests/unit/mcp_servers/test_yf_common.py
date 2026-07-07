"""Tests for the shared yfinance helpers (mcp_servers/_yf_common.py).

The missing-value guard is a crash seam: a ``pd.NaT`` in a datetime column
used to raise ``ValueError`` from ``strftime`` because the isoformat branch
ran before the NaN check, losing the whole payload. Order now checks
``pd.isna`` first and coerces ``inf`` to ``None``.
"""

import datetime

import pandas as pd

from mcp_servers._yf_common import clean_value, format_datetime, serialize_records


class TestCleanValue:
    def test_missing_and_non_finite_values_become_none(self):
        assert clean_value(pd.NaT) is None
        assert clean_value(float("nan")) is None
        assert clean_value(float("inf")) is None
        assert clean_value(float("-inf")) is None

    def test_date_only_datetime_formats_as_date(self):
        assert clean_value(datetime.date(2024, 1, 2)) == "2024-01-02"
        assert clean_value(pd.Timestamp("2024-01-02")) == "2024-01-02"

    def test_datetime_with_time_keeps_time_component(self):
        assert (
            clean_value(datetime.datetime(2024, 1, 2, 9, 30, 15))
            == "2024-01-02 09:30:15"
        )

    def test_nested_containers_are_cleaned_recursively(self):
        cleaned = clean_value({"a": [float("nan"), 1.0], "b": float("inf")})
        assert cleaned == {"a": [None, 1.0], "b": None}


class TestFormatDatetime:
    def test_date_only_and_with_time(self):
        assert format_datetime(pd.Timestamp("2024-01-02")) == "2024-01-02"
        assert (
            format_datetime(datetime.datetime(2024, 1, 2, 9, 30, 15))
            == "2024-01-02 09:30:15"
        )


class TestSerializeRecords:
    def test_nat_in_datetime_column_becomes_none_without_crashing(self):
        # Regression: a NaT used to raise ValueError from strftime and lose the
        # whole payload; now the field is None and every row survives.
        df = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-01"), pd.NaT],
                "close": [151.0, 152.0],
            }
        )
        records = serialize_records(df)
        assert records == [
            {"date": "2024-01-01", "close": 151.0},
            {"date": None, "close": 152.0},
        ]

    def test_numeric_nan_becomes_none(self):
        df = pd.DataFrame({"value": [1.0, float("nan")]})
        records = serialize_records(df)
        assert records[0]["value"] == 1.0
        assert records[1]["value"] is None

    def test_empty_dataframe_is_empty_list(self):
        assert serialize_records(pd.DataFrame()) == []
