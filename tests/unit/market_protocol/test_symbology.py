"""Symbology: canonicalization, provider spellings, and legacy parity."""

import pytest

from src.data_client.market_data_provider import symbol_timezone
from src.market_protocol import (
    AssetClass,
    display_decimals_for,
    parse_instrument_key,
    to_canonical,
    to_display,
    to_legacy_api,
    to_provider,
)


class TestSpellingCollapse:
    """Every spelling of an instrument must resolve to ONE canonical key."""

    @pytest.mark.parametrize(
        ("spellings", "expected_key"),
        [
            (["GSPC", "^GSPC", "I:SPX", "SPX", "^SPX", "SPX.INDEX", "gspc"], "SPX.INDEX"),
            (["IXIC", "^IXIC", "I:COMP", "COMP", "COMP.INDEX"], "COMP.INDEX"),
            (["DJI", "^DJI", "I:DJI", "DJI.INDEX"], "DJI.INDEX"),
            (["0700.HK", "0700.hk", "0700.XHKG"], "0700.XHKG"),
            (["VOD.L", "VOD.XLON", "vod.l"], "VOD.XLON"),
            (["AAPL", "aapl", "AAPL.US", "AAPL.XNAS"], "AAPL.XNAS"),
        ],
    )
    def test_collapse(self, spellings, expected_key):
        keys = {to_canonical(s).instrument_key for s in spellings}
        assert keys == {expected_key}

    def test_canonical_is_idempotent(self):
        for spelling in ["AAPL", "0700.HK", "GSPC", "VOD.L", "BRK.B"]:
            ref = to_canonical(spelling)
            again = to_canonical(ref.instrument_key)
            assert again.instrument_key == ref.instrument_key


class TestEquityResolution:
    def test_seeded_us_listing(self):
        ref = to_canonical("AAPL")
        assert ref.instrument_key == "AAPL.XNAS"
        assert ref.mic == "XNAS"
        assert ref.calendar_id == "XNYS"  # XNAS shares the XNYS calendar
        assert ref.currency == "USD"

    def test_unseeded_bare_us_defaults(self):
        ref = to_canonical("IBM")
        assert ref.instrument_key == "IBM.XNYS"
        assert ref.tz == "America/New_York"

    def test_hk(self):
        ref = to_canonical("0700.HK")
        assert ref.instrument_key == "0700.XHKG"
        assert (ref.currency, ref.price_currency) == ("HKD", "HKD")
        assert ref.calendar_id == "XHKG"
        assert ref.tz == "Asia/Hong_Kong"
        assert ref.display_unit is None

    def test_lse_carries_pence_hint(self):
        ref = to_canonical("VOD.L")
        assert ref.instrument_key == "VOD.XLON"
        assert ref.currency == "GBP"
        assert ref.display_unit == "GBX"  # hint only; conversion is per-provider

    def test_unknown_suffix_is_share_class_not_venue(self):
        ref = to_canonical("BRK.B")
        assert ref.symbol == "BRK.B"
        assert ref.mic == "XXXX"
        assert ref.tz == "America/New_York"

    def test_asset_class_hint_beats_index_autodetect(self):
        ref = to_canonical("SPX", asset_class=AssetClass.EQUITY)
        assert ref.asset_class == AssetClass.EQUITY


class TestIndexResolution:
    def test_known_family(self):
        ref = to_canonical("GSPC", asset_class=AssetClass.INDEX)
        assert ref.instrument_key == "SPX.INDEX"
        assert ref.index_family == "SPX"
        assert ref.calendar_id == "XNYS"

    def test_unknown_index_keeps_bare_spelling(self):
        ref = to_canonical("FTSE", asset_class=AssetClass.INDEX)
        assert ref.instrument_key == "FTSE.INDEX"
        assert to_provider(ref, "ginlix-data") == "I:FTSE"


class TestPairs:
    def test_crypto(self):
        ref = to_canonical("BTC-USD", asset_class=AssetClass.CRYPTO)
        assert ref.instrument_key == "BTC-USD.CRYPTO"
        assert ref.calendar_id == "ALWAYS_24_7"

    def test_fx_yahoo_spelling(self):
        ref = to_canonical("EURUSD=X")
        assert ref.instrument_key == "EUR-USD.FX"
        assert ref.calendar_id == "WEEKDAYS_24_5"


class TestProviderSpellings:
    def test_equity(self):
        aapl = to_canonical("AAPL")
        hk = to_canonical("0700.HK")
        assert to_provider(aapl, "fmp") == "AAPL"
        assert to_provider(aapl, "yfinance") == "AAPL"
        assert to_provider(aapl, "ginlix-data") == "AAPL"
        assert to_provider(hk, "fmp") == "0700.HK"
        assert to_provider(hk, "yfinance") == "0700.HK"

    def test_index(self):
        spx = to_canonical("GSPC")
        assert to_provider(spx, "fmp") == "^GSPC"
        assert to_provider(spx, "yfinance") == "^GSPC"
        assert to_provider(spx, "ginlix-data") == "I:SPX"

    def test_pairs(self):
        btc = to_canonical("BTC-USD", asset_class=AssetClass.CRYPTO)
        assert to_provider(btc, "yfinance") == "BTC-USD"
        assert to_provider(btc, "fmp") == "BTCUSD"
        fx = to_canonical("EURUSD=X")
        assert to_provider(fx, "yfinance") == "EURUSD=X"
        assert to_provider(fx, "fmp") == "EURUSD"

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError):
            to_provider(to_canonical("AAPL"), "polygon")


class TestLegacyAndDisplay:
    @pytest.mark.parametrize(
        ("spelling", "legacy", "display"),
        [
            ("AAPL", "AAPL", "AAPL"),
            ("0700.HK", "0700.HK", "0700.HK"),
            ("VOD.L", "VOD.L", "VOD.L"),
            ("GSPC", "GSPC", "SPX"),
            ("IXIC", "IXIC", "COMP"),
            ("I:SPX", "GSPC", "SPX"),
        ],
    )
    def test_round_trip(self, spelling, legacy, display):
        ref = to_canonical(spelling)
        assert to_legacy_api(ref) == legacy
        assert to_display(ref) == display


class TestParseInstrumentKey:
    def test_parse(self):
        assert parse_instrument_key("0700.XHKG") == ("0700", "XHKG")
        assert parse_instrument_key("BRK.B.XXXX") == ("BRK.B", "XXXX")

    @pytest.mark.parametrize("bad", ["AAPL", ".XNAS", "AAPL.", ""])
    def test_malformed(self, bad):
        with pytest.raises(ValueError):
            parse_instrument_key(bad)


class TestLegacyParity:
    """InstrumentRef.tz must stay in parity with the legacy symbol resolver."""

    def test_tz_parity(self):
        """InstrumentRef.tz must never disagree with symbol_timezone's UTC offset."""
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo

        probe = datetime(2026, 7, 15, tzinfo=timezone.utc)
        for sym in ["AAPL", "0700.HK", "600519.SS", "VOD.L", "7203.T",
                    "SHOP.TO", "BHP.AX", "SAP.DE", "005930.KS", "2330.TW",
                    "D05.SI", "RELIANCE.BO", "BRK.B"]:
            ref = to_canonical(sym)
            legacy_offset = probe.astimezone(symbol_timezone(sym)).utcoffset()
            protocol_offset = probe.astimezone(ZoneInfo(ref.tz)).utcoffset()
            assert protocol_offset == legacy_offset, sym


class TestDisplayDecimals:
    def test_defaults(self):
        assert display_decimals_for("USD", AssetClass.EQUITY) == 2
        assert display_decimals_for("JPY", AssetClass.EQUITY) == 0
        assert display_decimals_for("KRW", AssetClass.EQUITY) == 0
        assert display_decimals_for("USD", AssetClass.CRYPTO) == 8
