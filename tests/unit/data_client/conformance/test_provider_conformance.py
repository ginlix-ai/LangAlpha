"""Provider normalizer contract (Phase 1: implemented — xfails flipped).

Each provider data_source exposes ``normalize_series(rows, *, ref, schema)
-> Series``. These tests run it against live-captured fixtures; they were
strict-xfail during Phase 0 and flipped green with the Phase 1 normalizers.
"""

from datetime import datetime, timezone

from src.market_protocol import OhlcvBar, Series, to_canonical
from src.market_protocol.calendars import get_calendar
from src.market_protocol.enums import MarketPhase

from .conftest import series_normalizer


def _assert_full_header(series: Series, *, instrument_key: str, schema: str, publisher: str) -> None:
    header = series.header
    assert header.instrument_key == instrument_key
    assert header.schema_id == schema
    assert header.publisher == publisher
    assert header.price_treatment is not None
    assert header.tier is not None
    assert header.price_currency
    assert header.ts_unit == "ms"
    assert header.asof > 0 and header.fetched_at > 0


def test_fmp_hk_ts_event_is_utc_session_anchored(fmp_hk_raw):
    """The tz fix: FMP's HKT wall-clock strings become UTC epochs whose
    anchors land inside XHKG sessions, ascending, bar-open anchored."""
    normalize = series_normalizer("fmp")
    ref = to_canonical("0700.HK")
    series = normalize(fmp_hk_raw["data"], ref=ref, schema="ohlcv-1h")
    _assert_full_header(series, instrument_key="0700.XHKG", schema="ohlcv-1h", publisher="fmp")
    cal = get_calendar("XHKG")
    times = [r.ts_event for r in series.records]
    assert times == sorted(times), "normalized records must be ascending"
    for record in series.records:
        at = datetime.fromtimestamp(record.ts_event / 1000, tz=timezone.utc)
        assert cal.phase_at(at) != MarketPhase.CLOSED, record.ts_event


def test_fmp_vodl_pence_converted_to_major_units(fmp_vodl_daily_raw):
    normalize = series_normalizer("fmp")
    ref = to_canonical("VOD.L")
    series = normalize(fmp_vodl_daily_raw["data"], ref=ref, schema="ohlcv-1d")
    _assert_full_header(series, instrument_key="VOD.XLON", schema="ohlcv-1d", publisher="fmp")
    assert series.header.price_currency == "GBP"
    assert all(0.1 < r.close < 10 for r in series.records), "VOD must be pounds, not pence"


def test_yfinance_vodl_pence_converted_to_major_units(yf_vodl_adjusted):
    """Paired with the FMP pence test: same instrument, second provider —
    conversion is per-provider-normalizer, keyed on (provider, currency, exchange)."""
    normalize = series_normalizer("yfinance")
    ref = to_canonical("VOD.L")
    rows = [
        {"time": r["ts_utc_ms"], "open": r["open"], "high": r["high"],
         "low": r["low"], "close": r["close"], "volume": r["volume"]}
        for r in yf_vodl_adjusted["data"]
    ]
    series = normalize(rows, ref=ref, schema="ohlcv-1d")
    _assert_full_header(series, instrument_key="VOD.XLON", schema="ohlcv-1d", publisher="yfinance")
    assert all(0.1 < r.close < 10 for r in series.records)


def test_yfinance_hk_epochs_pass_through(yf_hk_1h):
    """yfinance epochs are already correct — the normalizer must not shift them."""
    normalize = series_normalizer("yfinance")
    ref = to_canonical("0700.HK")
    rows = [
        {"time": r["ts_utc_ms"], "open": r["open"], "high": r["high"],
         "low": r["low"], "close": r["close"], "volume": r["volume"]}
        for r in yf_hk_1h["data"]
    ]
    series = normalize(rows, ref=ref, schema="ohlcv-1h")
    assert [r.ts_event for r in series.records] == sorted(r["ts_utc_ms"] for r in yf_hk_1h["data"])


def test_ginlix_data_normalizes_to_series():
    """ginlix-data bars are epoch-ms already; normalize_series wraps them with
    a declared header (publisher, treatment, tier) and ascending records."""
    normalize = series_normalizer("ginlix-data")
    ref = to_canonical("AAPL")
    rows = [
        {"time": 1_782_000_000_000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10},
        {"time": 1_782_003_600_000, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0, "volume": 20},
    ]
    series = normalize(rows, ref=ref, schema="ohlcv-1h")
    _assert_full_header(series, instrument_key="AAPL.XNAS", schema="ohlcv-1h", publisher="ginlix-data")


def test_ws_forming_bar_golden():
    """WS role split (Phase 1): the parsed upstream bar becomes a protocol
    record with is_final=False and ts_event = window OPEN (Polygon `s`)."""
    from src.server.services.market_data_feed import parse_ws_bar

    frame = '{"ev":"AM","sym":"AAPL","o":100.0,"h":101.0,"l":99.5,"c":100.5,"v":1200,"s":1782000000000,"e":1782000060000}'
    parsed = parse_ws_bar(frame)
    assert parsed == {
        "symbol": "AAPL", "time": 1782000000000, "open": 100.0,
        "high": 101.0, "low": 99.5, "close": 100.5, "volume": 1200,
    }
    # Today's parser already anchors on `s` (window open) — the protocol
    # record is a field-faithful lift:
    record = OhlcvBar.model_validate({**parsed, "is_final": False})
    assert record.ts_event == 1782000000000
    assert record.is_final is False
