"""NaN-safety for the yfinance data source — no network.

Yahoo can return a forming daily bar with NaN OHLC (seen live for a CN index
during its trading session) and `fast_info` fields can be NaN. Non-finite
floats must never leave this module: they serialize as bare `NaN` tokens
(invalid JSON) downstream.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd

from src.data_client.yfinance.data_source import (
    _fetch_history,
    _fetch_single_snapshot,
)


def _history_df(rows: list[dict], dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, index=pd.to_datetime(dates, utc=True))


def _mock_ticker_with_history(df: pd.DataFrame) -> MagicMock:
    ticker = MagicMock()
    ticker.history.return_value = df
    return ticker


class TestFetchHistoryNanSafety:
    def test_nan_ohlc_row_is_dropped(self):
        df = _history_df(
            [
                {"Open": 10.0, "High": 11.0, "Low": 9.0, "Close": 10.5, "Volume": 100},
                {
                    "Open": float("nan"),
                    "High": float("nan"),
                    "Low": float("nan"),
                    "Close": float("nan"),
                    "Volume": 0,
                },
            ],
            ["2026-06-30", "2026-07-01"],
        )
        with patch(
            "src.data_client.yfinance.data_source.yf.Ticker",
            return_value=_mock_ticker_with_history(df),
        ):
            bars = _fetch_history("000000.SS", "1d", None, None)

        assert len(bars) == 1
        assert bars[0]["close"] == 10.5
        # Must be serializable as strict JSON — no bare NaN tokens.
        json.dumps(bars, allow_nan=False)

    def test_nan_volume_survives_as_zero(self):
        # An index bar can carry valid prices but NaN volume; it must survive
        # with volume=0 rather than raising on int(NaN).
        df = _history_df(
            [
                {
                    "Open": 10.0,
                    "High": 11.0,
                    "Low": 9.0,
                    "Close": 10.5,
                    "Volume": float("nan"),
                }
            ],
            ["2026-06-30"],
        )
        with patch(
            "src.data_client.yfinance.data_source.yf.Ticker",
            return_value=_mock_ticker_with_history(df),
        ):
            bars = _fetch_history("000000.SS", "1d", None, None)

        assert len(bars) == 1
        assert bars[0]["volume"] == 0
        json.dumps(bars, allow_nan=False)

    def test_all_rows_nan_returns_empty(self):
        df = _history_df(
            [
                {
                    "Open": float("nan"),
                    "High": float("nan"),
                    "Low": float("nan"),
                    "Close": float("nan"),
                    "Volume": 0,
                }
            ],
            ["2026-07-01"],
        )
        with patch(
            "src.data_client.yfinance.data_source.yf.Ticker",
            return_value=_mock_ticker_with_history(df),
        ):
            assert _fetch_history("000000.SS", "1d", None, None) == []


class TestFetchSnapshotNanSafety:
    def _mock_ticker_with_fast_info(self, fast_info: dict) -> MagicMock:
        ticker = MagicMock()
        ticker.fast_info = fast_info
        return ticker

    def test_nan_last_price_becomes_zero(self):
        fi = {"lastPrice": float("nan"), "previousClose": 100.0}
        with patch(
            "src.data_client.yfinance.data_source.yf.Ticker",
            return_value=self._mock_ticker_with_fast_info(fi),
        ):
            snap = _fetch_single_snapshot("TEST")

        assert snap is not None
        assert snap["price"] == 0.0
        json.dumps(snap, allow_nan=False)

    def test_all_nan_fast_info_is_finite(self):
        fi = {
            "lastPrice": float("nan"),
            "previousClose": float("nan"),
            "open": float("nan"),
            "dayHigh": float("nan"),
            "dayLow": float("nan"),
            "lastVolume": float("nan"),
        }
        with patch(
            "src.data_client.yfinance.data_source.yf.Ticker",
            return_value=self._mock_ticker_with_fast_info(fi),
        ):
            snap = _fetch_single_snapshot("TEST")

        assert snap is not None
        assert snap["volume"] == 0
        json.dumps(snap, allow_nan=False)

    def test_clean_fast_info_unchanged(self):
        fi = {
            "lastPrice": 105.5,
            "previousClose": 100.0,
            "open": 101.0,
            "dayHigh": 106.0,
            "dayLow": 100.5,
            "lastVolume": 12345,
        }
        with patch(
            "src.data_client.yfinance.data_source.yf.Ticker",
            return_value=self._mock_ticker_with_fast_info(fi),
        ):
            snap = _fetch_single_snapshot("TEST")

        assert snap is not None
        assert snap["price"] == 105.5
        assert snap["change"] == 5.5
        assert snap["change_percent"] == 5.5
        assert snap["volume"] == 12345
