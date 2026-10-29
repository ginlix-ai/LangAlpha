"""Symbology: canonicalization, display, legacy and vendor-neutral spellings."""

import pytest

from market_protocol import (
    CARET_INDEX_REGIONS,
    AssetClass,
    PriceTreatment,
    Tier,
    build_series,
    display_decimals_for,
    display_spelling,
    exchange_code,
    from_instrument_key,
    index_families,
    is_family_index,
    market_home,
    parse_instrument_key,
    to_canonical,
    to_display,
    to_legacy_api,
    vendor_spelling,
    vendor_symbol,
    venue_suffixes,
)
from market_protocol import symbology
from market_protocol.symbology import _parse_seed, _seed_registry


def _served_close(ref):
    """The close build_series serves for one row that closed at 105."""
    row = {"time": 1_750_000_000_000, "open": 100.0, "high": 110.0, "low": 90.0,
           "close": 105.0, "volume": 1000.0}
    series = build_series(
        [row], ref=ref, schema="ohlcv-1d", publisher="test",
        price_treatment=PriceTreatment.RAW, tier=Tier.EOD,
    )
    return series.records[0].close


class TestSpellingCollapse:
    """Every spelling of an instrument must resolve to ONE canonical key."""

    @pytest.mark.parametrize(
        ("spellings", "expected_key"),
        [
            (
                ["GSPC", "^GSPC", "I:SPX", "SPX", "^SPX", "SPX.INDEX", "gspc",
                 "^SPX.INDEX", "I:SPX.INDEX", "^GSPC.INDEX", "GSPC.INDEX",
                 "^I:SPX", "I:I:SPX", "I:^SPX"],
                "SPX.INDEX",
            ),
            (["IXIC", "^IXIC", "I:COMP", "COMP", "COMP.INDEX"], "COMP.INDEX"),
            (["DJI", "^DJI", "I:DJI", "DJI.INDEX"], "DJI.INDEX"),
            # HKEX codes are written with any number of leading zeros.
            (["0700.HK", "0700.hk", "0700.XHKG", "700.HK", "00700.HK", "700.XHKG"], "0700.XHKG"),
            (["VOD.L", "VOD.XLON", "vod.l"], "VOD.XLON"),
            (["AAPL", "aapl", "AAPL.US", "AAPL.XNAS"], "AAPL.XNAS"),
            # Shanghai reads ``.SH`` (exchange, Tushare) or ``.SS`` (Yahoo, FMP),
            # and a Chinese IME types full-width letters and stops.
            (
                ["600519.SS", "600519.SH", "600519.sh", "600519.XSHG",
                 "600519．SH", "600519.ＳＳ", "600519。SH", "６００５１９.SH"],
                "600519.XSHG",
            ),
            # An exchange index keeps its venue key whether or not it is
            # spelled with a caret or hinted as an index.
            (["000001.SS", "000001.SH", "^000001.SS", "000001.XSHG"], "000001.XSHG"),
            (["430047.BJ", "430047.BJSE"], "430047.BJSE"),
            (["^HSI", "HSI", "HSI.INDEX"], "HSI.INDEX"),
            # A venue whose codes do not separate indexes cannot key one.
            (["^FTSE", "FTSE", "FTSE.INDEX", "^FTSE.L"], "FTSE.INDEX"),
            # Yahoo names a USD-based rate by its quote alone.
            (["JPY=X", "jpy=x", "USDJPY=X", "USD-JPY.FX", "USDJPY.FX"], "USD-JPY.FX"),
            (["EURUSD=X", "EUR-USD.FX", "EURUSD.FX", "eurusd.fx"], "EUR-USD.FX"),
            (["BTC-USD.CRYPTO", "BTCUSD.CRYPTO"], "BTC-USD.CRYPTO"),
        ],
    )
    def test_collapse(self, spellings, expected_key):
        keys = {to_canonical(s).instrument_key for s in spellings}
        assert keys == {expected_key}

    @pytest.mark.parametrize(
        ("key", "canonical"),
        [
            ("^SPX.INDEX", "SPX.INDEX"), ("I:SPX.INDEX", "SPX.INDEX"),
            ("^GSPC.INDEX", "SPX.INDEX"), ("I:^HSI.INDEX", "HSI.INDEX"),
            ("^XYZIDX.INDEX", "XYZIDX.INDEX"),
            # An exchange index keeps its venue key, as ^000001.SS does.
            ("^000001.SS.INDEX", "000001.XSHG"),
            ("EURUSD.FX", "EUR-USD.FX"), ("USDJPY.FX", "USD-JPY.FX"),
            ("BTCUSD.CRYPTO", "BTC-USD.CRYPTO"),
        ],
    )
    def test_a_key_is_normalized_like_a_spelling(self, key, canonical):
        """A key spelled the legacy way names the same ref, under the key that reparses to it."""
        ref = to_canonical(key)
        assert ref == to_canonical(canonical)
        assert ref.instrument_key == canonical
        assert to_canonical(ref.instrument_key) == ref

    def test_a_run_together_key_is_quoted_in_its_second_leg(self):
        ref = to_canonical("USDJPY.FX")
        assert (ref.symbol, ref.currency, ref.price_currency) == ("USD-JPY", "JPY", "JPY")

    @pytest.mark.parametrize(
        "spelling",
        [
            "AAPL", "IBM", "AMD.DE", "SAP.DE", "0700.HK", "700.HK", "VOD.L",
            "7203.T", "600519.SS", "000001.SH", "000001.SZ", "510300.SH",
            "899050.BJ", "BRK.B", "GSPC", "^HSI", "EURUSD=X", "JPY=X", "^FTSE.L",
        ],
    )
    def test_canonical_is_idempotent(self, spelling):
        """A key is reparsed downstream, so it must name the very same ref."""
        ref = to_canonical(spelling)
        assert to_canonical(ref.instrument_key) == ref

    @pytest.mark.parametrize("bare", ["^", "^^", "I:", "I:^", ".US", "=X"])
    def test_decoration_without_a_symbol_is_refused(self, bare):
        with pytest.raises(ValueError):
            to_canonical(bare)


class TestJunkInput:
    """Anything that cannot spell an instrument is one ValueError.

    A consumer interpolates the symbol into a URL path, so a character that
    would end the segment, or a segment that climbs a level, never resolves.
    """

    @pytest.mark.parametrize(
        "junk",
        [
            None, 42, b"AAPL",
            "", "   ", "\n\t",
            # Internal whitespace and control or format characters.
            "BRK B", "AA\x00PL", "AAPL\x00", "AAPL\x7f", "​AAPL", "AAPL‮",
            "A　B",
            # Path and URL metacharacters, before and after the fold.
            "A/B", "A\\B", "A?B", "A#B", "A%2FB", "../ETC", "．．／", "AAPL／X",
            # A batch separator: one symbol would name two listings upstream.
            "600519.SH,000858.SZ", "AAPL,MSFT", "６００５１９．ＳＨ，０００８５８．ＳＺ",
            # A stem that names nothing.
            ".HK", "..XXXX", "..", ".", "AAPL.", "A..HK", "^.", "-.FX", "BTC-.CRYPTO",
            "^I:", "A" * 65,
        ],
    )
    def test_refused(self, junk):
        with pytest.raises(ValueError):
            to_canonical(junk)

    @pytest.mark.parametrize("hint", list(AssetClass))
    @pytest.mark.parametrize("junk", ["A/B", "..", "A..HK", "^."])
    def test_refused_under_any_hint(self, junk, hint):
        with pytest.raises(ValueError):
            to_canonical(junk, asset_class=hint)

    @pytest.mark.parametrize("pair", ["BTC-", "-USD", "BTC--USD", "-USD=X"])
    def test_a_pair_needs_both_legs(self, pair):
        with pytest.raises(ValueError):
            to_canonical(pair, asset_class=AssetClass.CRYPTO)

    @pytest.mark.parametrize(
        ("spelling", "hint"),
        [("A" * 60, None), ("A" * 60 + ".HK", None), ("A" * 30 + "-" + "B" * 30, AssetClass.CRYPTO)],
    )
    def test_a_key_too_long_to_parse_back_is_refused(self, spelling, hint):
        with pytest.raises(ValueError):
            to_canonical(spelling, asset_class=hint)

    @pytest.mark.parametrize("hint", ["option", "", 3])
    def test_an_unknown_hint_is_refused(self, hint):
        with pytest.raises(ValueError):
            to_canonical("AAPL", asset_class=hint)

    @pytest.mark.parametrize(
        ("spelling", "hint"),
        [
            ("BTC-USD", AssetClass.CRYPTO), ("EUR-USD", AssetClass.FX), ("SPX", AssetClass.INDEX),
            ("COMP", AssetClass.EQUITY), ("510300.SH", AssetClass.FUND),
        ],
    )
    def test_a_string_hint_reads_as_the_enum(self, spelling, hint):
        assert to_canonical(spelling, asset_class=hint.value) == to_canonical(spelling, asset_class=hint)

    @pytest.mark.parametrize(
        ("spelling", "key"),
        [
            ("BRK.B", "BRK.B.XXXX"), ("BRK-B", "BRK-B.XNYS"), ("M&M.NS", "M&M.XNSE"),
            ("ES=F", "ES=F.XNYS"), ("^GSPC", "SPX.INDEX"), ("I:SPX", "SPX.INDEX"),
            ("EURUSD=X", "EUR-USD.FX"), ("BTC-USD.CRYPTO", "BTC-USD.CRYPTO"),
            # Surrounding whitespace is trimmed, as a pasted symbol carries it.
            (" aapl\n", "AAPL.XNAS"),
            ("A" * 59, "A" * 59 + ".XNYS"),
        ],
    )
    def test_real_spellings_still_resolve(self, spelling, key):
        assert to_canonical(spelling).instrument_key == key
        assert to_canonical(key).instrument_key == key


class TestSeedRegistry:
    def test_the_shipped_file_loads(self):
        seeds = _seed_registry()
        assert seeds["AAPL"]["mic"] == "XNAS"
        assert all(key == key.upper() for key in seeds)

    @pytest.mark.parametrize(
        ("text", "named"),
        [
            # YAML 1.1 reads an unquoted ON as a boolean and 700 as an int.
            ("instruments:\n  ON:\n    mic: XNAS\n", "True"),
            ("instruments:\n  700:\n    mic: XHKG\n", "700"),
            ("instruments:\n  AAPL:\n    mic: XNSA\n", "XNSA"),
            ("instruments:\n  AAPL:\n    mics: XNAS\n", "mics"),
            ("instruments:\n  AAPL: XNAS\n", "AAPL"),
            ("instruments:\n  aapl: {name: a}\n  AAPL: {name: b}\n", "AAPL"),
            # Two spellings of one listing read as one key.
            (
                "instruments:\n  600519.SS: {name: a}\n  600519.SH: {name: b}\n",
                "600519.SH: duplicate of 600519.SS",
            ),
            (
                "instruments:\n  700.HK: {name: a}\n  0700.HK: {name: b}\n",
                "0700.HK: duplicate of 700.HK",
            ),
            ("instruments:\n  - AAPL\n", "instruments"),
            ("- AAPL\n", "top level"),
            # A typo that would load and misbehave later fails here instead.
            ("instruments:\n  VOD.L:\n    display_unit: pence\n", "pence"),
            ("instruments:\n  VOD.L:\n    display_unit: GBP\n", "display_unit 'GBP'"),
            ("instruments:\n  VOD.L:\n    calendar_id: BOGUS\n", "BOGUS"),
            ("instruments:\n  VOD.L:\n    calendar_id: xlon\n", "xlon"),
            ("instruments:\n  VOD.L:\n    currency: GB\n", "currency 'GB'"),
            ("instruments:\n  VOD.L:\n    currency: 826\n", "currency 826"),
            ("instruments:\n  VOD.L:\n    price_currency: USDT\n", "price_currency 'USDT'"),
            ("instruments:\n  VOD.L:\n    price_currency: null\n", "price_currency None"),
            ("instruments:\n  ABC:\n    name: 1984\n", "name 1984"),
        ],
    )
    def test_a_bad_row_names_its_key(self, text, named):
        with pytest.raises(ValueError, match=named):
            _parse_seed(text)

    def test_a_quoted_key_is_kept(self):
        assert _parse_seed('instruments:\n  "ON":\n    mic: XTSE\n') == {"ON": {"mic": "XTSE"}}
        assert _parse_seed("") == {}

    def test_a_key_is_read_in_our_spelling(self):
        seeds = _parse_seed("instruments:\n  600519.ss: {}\n  700.HK: {}\n  aapl: {}\n")
        assert set(seeds) == {"600519.SH", "0700.HK", "AAPL"}

    def test_values_are_normalized(self):
        text = (
            "instruments:\n  ABC.L:\n    mic: xlon\n    currency: ' gbp '\n"
            "    price_currency: usd\n    display_unit: gbx\n    calendar_id: XLON\n"
        )
        assert _parse_seed(text)["ABC.L"] == {
            "mic": "XLON", "currency": "GBP", "price_currency": "USD",
            "display_unit": "GBX", "calendar_id": "XLON",
        }

    @pytest.mark.parametrize("calendar_id", ["XNYS", "XSHG", "ALWAYS_24_7", "WEEKDAYS_24_5"])
    def test_every_calendar_the_package_keeps_is_accepted(self, calendar_id):
        text = f"instruments:\n  ABC:\n    calendar_id: {calendar_id}\n"
        assert _parse_seed(text)["ABC"]["calendar_id"] == calendar_id

    def test_shipped_rows_keep_their_keys(self):
        """A row that moves changes a key every reader holds, which makes the move breaking."""
        assert {k: to_canonical(k).instrument_key for k in _seed_registry()} == {
            "AAPL": "AAPL.XNAS", "MSFT": "MSFT.XNAS", "GOOGL": "GOOGL.XNAS",
            "AMZN": "AMZN.XNAS", "META": "META.XNAS", "NVDA": "NVDA.XNAS",
            "TSLA": "TSLA.XNAS", "AVGO": "AVGO.XNAS", "NFLX": "NFLX.XNAS",
            "AMD": "AMD.XNAS", "INTC": "INTC.XNAS", "QCOM": "QCOM.XNAS",
            "CSCO": "CSCO.XNAS", "ADBE": "ADBE.XNAS", "PEP": "PEP.XNAS",
            "COST": "COST.XNAS", "TMUS": "TMUS.XNAS", "MU": "MU.XNAS",
            "PLTR": "PLTR.XNAS", "COIN": "COIN.XNAS",
        }

    @pytest.fixture
    def fresh_registry(self):
        _seed_registry.cache_clear()
        yield
        _seed_registry.cache_clear()

    @pytest.fixture
    def seed_dir(self, tmp_path, monkeypatch, fresh_registry):
        monkeypatch.setattr(symbology, "files", lambda _package: tmp_path)
        return tmp_path

    def test_a_missing_file_raises(self, seed_dir):
        with pytest.raises(FileNotFoundError):
            _seed_registry()

    def test_the_file_is_read_as_utf8(self, monkeypatch, fresh_registry):
        """A locale-default read would garble a non-ASCII name wherever the locale is not UTF-8."""
        encodings = []

        class Resource:
            def __truediv__(self, name):
                assert name == "instruments.yaml"
                return self

            def read_text(self, encoding=None):
                encodings.append(encoding)
                return ""

        monkeypatch.setattr(symbology, "files", lambda _package: Resource())
        _seed_registry()
        assert encodings == ["utf-8"]

    def test_a_row_in_another_spelling_still_matches(self, seed_dir):
        (seed_dir / "instruments.yaml").write_text(
            "instruments:\n  600519.SS: {name: a}\n  700.HK: {name: b}\n", encoding="utf-8"
        )
        assert to_canonical("600519.SH").name == "a"
        assert to_canonical("00700.HK").name == "b"

    @pytest.fixture
    def override_rows(self, seed_dir):
        # A USD line on a GBP venue, and a USD listing on TSX that trades the
        # US calendar: every override field but mic and name.
        (seed_dir / "instruments.yaml").write_text(
            "instruments:\n"
            "  USDF.L:\n    price_currency: usd\n    display_unit: null\n"
            "  XYZ.TO:\n    currency: USD\n    calendar_id: XNYS\n",
            encoding="utf-8",
        )

    def test_overrides_reach_the_ref(self, override_rows):
        usd_line = to_canonical("USDF.L")
        assert (usd_line.currency, usd_line.price_currency) == ("GBP", "USD")
        assert (usd_line.display_unit, usd_line.calendar_id) == (None, "XLON")
        listing = to_canonical("XYZ.TO")
        # price_currency follows the overridden currency, not the venue's.
        assert (listing.currency, listing.price_currency) == ("USD", "USD")
        assert listing.calendar_id == "XNYS"
        # An unseeded listing on the same venues keeps the defaults.
        assert to_canonical("ABC.L").display_unit == "GBX"
        plain = to_canonical("ABC.TO")
        assert (plain.currency, plain.calendar_id) == ("CAD", "XTSE")

    def test_a_usd_line_on_xlon_is_not_scaled(self, override_rows):
        assert _served_close(to_canonical("USDF.L")) == 105.0

    @pytest.mark.parametrize("unit", sorted(symbology.MINOR_UNIT_PLACES))
    def test_every_unit_a_seed_may_name_is_scaled(self, unit):
        """A unit the seed accepts but the builder passed through would serve pence as pounds."""
        ref = to_canonical("ABC.L").model_copy(update={"display_unit": unit})
        assert _served_close(ref) != 105.0


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

    def test_us_seed_stays_off_a_foreign_listing(self):
        """The AMD seed pins the US listing; AMD on Xetra and its key keep XETR."""
        assert to_canonical("AMD").instrument_key == "AMD.XNAS"
        assert to_canonical("AMD.DE").instrument_key == "AMD.XETR"
        assert to_canonical("AMD.XETR").instrument_key == "AMD.XETR"
        assert to_canonical("AMD.XETR").name is None

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
        ref = to_canonical("XYZIDX", asset_class=AssetClass.INDEX)
        assert (ref.instrument_key, ref.index_family) == ("XYZIDX.INDEX", "XYZIDX")
        assert to_legacy_api(ref) == "XYZIDX"
        assert to_display(ref) == "XYZIDX"
        assert vendor_symbol(ref) == "XYZIDX"


class TestPairs:
    def test_crypto(self):
        ref = to_canonical("BTC-USD", asset_class=AssetClass.CRYPTO)
        assert ref.instrument_key == "BTC-USD.CRYPTO"
        assert ref.calendar_id == "ALWAYS_24_7"

    def test_fx_yahoo_spelling(self):
        ref = to_canonical("EURUSD=X")
        assert ref.instrument_key == "EUR-USD.FX"
        assert (ref.currency, ref.price_currency) == ("USD", "USD")
        assert ref.calendar_id == "WEEKDAYS_24_5"

    def test_fx_yahoo_single_currency_is_usd_based(self):
        """``JPY=X`` is USD-JPY, priced in yen, not the yen priced in dollars."""
        ref = to_canonical("JPY=X")
        assert ref.instrument_key == "USD-JPY.FX"
        assert (ref.currency, ref.price_currency) == ("JPY", "JPY")
        assert vendor_symbol(ref) == "USD-JPY"

    @pytest.mark.parametrize("hint", [AssetClass.CRYPTO, AssetClass.FX, "crypto", "fx"])
    @pytest.mark.parametrize(
        "listing",
        [
            # Canonical venue keys, the unknown venue included.
            "AAPL.XNAS", "0700.XHKG", "600519.XSHG", "BRK.B.XXXX",
            # Venue suffixes, with the decorations the pair path strips.
            "0700.HK", "600519.SS", "VOD.L", "AAPL.US", "^0700.HK", "VOD.L=X",
        ],
    )
    def test_a_pair_hint_cannot_relabel_a_listing(self, listing, hint):
        """The key carries no class, so the pair's data would land under the listing's key."""
        with pytest.raises(ValueError, match="venue listing"):
            to_canonical(listing, asset_class=hint)

    @pytest.mark.parametrize("hint", [None, AssetClass.EQUITY])
    @pytest.mark.parametrize("listing", ["VOD.L=X", "AAPL.US=X", "600519.SS=X"])
    def test_an_fx_suffix_cannot_relabel_a_listing(self, listing, hint):
        # =X spells FX on its own, so no hint is needed to refuse VOD.L.FX.
        with pytest.raises(ValueError, match="venue listing"):
            to_canonical(listing, asset_class=hint)

    @pytest.mark.parametrize("hint", [None, AssetClass.FX, AssetClass.CRYPTO, AssetClass.EQUITY])
    def test_an_fx_suffix_outranks_the_hint(self, hint):
        ref = to_canonical("EURUSD=X", asset_class=hint)
        assert (ref.instrument_key, ref.asset_class) == ("EUR-USD.FX", AssetClass.FX)

    @pytest.mark.parametrize(
        ("spelling", "hint", "key"),
        [
            ("BTC-USD", AssetClass.CRYPTO, "BTC-USD.CRYPTO"),
            ("BTCUSD", AssetClass.CRYPTO, "BTC-USD.CRYPTO"),
            ("EURUSD", AssetClass.FX, "EUR-USD.FX"),
            ("JPY=X", AssetClass.FX, "USD-JPY.FX"),
            ("EUR-USD.FX", AssetClass.FX, "EUR-USD.FX"),
            # A listing under any other hint resolves as before.
            ("AAPL.XNAS", AssetClass.EQUITY, "AAPL.XNAS"),
            ("0700.HK", AssetClass.EQUITY, "0700.XHKG"),
            ("000001.XSHG", AssetClass.INDEX, "000001.XSHG"),
            ("510300.XSHG", AssetClass.FUND, "510300.XSHG"),
        ],
    )
    def test_a_hint_that_fits_still_resolves(self, spelling, hint, key):
        assert to_canonical(spelling, asset_class=hint).instrument_key == key


class TestVendorSymbol:
    """The vendor-neutral spelling every adapter builds its own on."""

    def test_equity(self):
        assert vendor_symbol(to_canonical("AAPL")) == "AAPL"
        assert vendor_symbol(to_canonical("0700.HK")) == "0700.HK"
        assert vendor_symbol(to_canonical("00700.HK")) == "0700.HK"

    def test_index(self):
        spx = to_canonical("GSPC")
        assert vendor_symbol(spx) == "GSPC"
        assert to_legacy_api(spx) == "GSPC"

    def test_pairs(self):
        btc = to_canonical("BTC-USD", asset_class=AssetClass.CRYPTO)
        assert vendor_symbol(btc) == "BTC-USD"
        assert vendor_symbol(to_canonical("EURUSD=X")) == "EUR-USD"


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


class TestSuffixSpellings:
    """String-level respelling: only a venue suffix moves, never the rest."""

    @pytest.mark.parametrize(
        ("raw", "display", "vendor"),
        [
            ("600519.SS", "600519.SH", "600519.SS"),
            ("600519.SH", "600519.SH", "600519.SS"),
            ("600519.sh", "600519.SH", "600519.SS"),
            ("000001.SZ", "000001.SZ", "000001.SZ"),
            ("0700.HK", "0700.HK", "0700.HK"),
            ("700.HK", "0700.HK", "0700.HK"),
            ("00700.hk", "0700.HK", "0700.HK"),
            # Padding never truncates a five-digit code.
            ("80700.HK", "80700.HK", "80700.HK"),
            ("600519．ｓｓ", "600519.SH", "600519.SS"),
            # A family index keeps its vendor name; that respelling is to_display's.
            ("GSPC", "GSPC", "GSPC"),
            ("BRK.B", "BRK.B", "BRK.B"),
        ],
    )
    def test_respell(self, raw, display, vendor):
        assert display_spelling(raw) == display
        assert vendor_spelling(raw) == vendor

    def test_display_spelling_takes_any_string(self):
        assert display_spelling("") == ""
        # No int() conversion, so a digit string past the 4300 limit passes through.
        assert display_spelling("9" * 5000 + ".HK") == "9" * 5000 + ".HK"

    @pytest.mark.parametrize(
        "raw",
        ["", "9" * 5000 + ".HK", "A/B.SS", "．．／ETC.SS", "AAPL .SS",
         "..", "．．", ".", "AAPL.", ".HK", "-"],
        ids=["empty", "too-long", "slash", "full-width-climb", "space",
             "climb", "full-width-climb-bare", "dot", "trailing-dot", "no-stem", "punctuation"],
    )
    def test_vendor_spelling_refuses_what_to_canonical_refuses(self, raw):
        # The fold turns the full-width form into ../ETC.SS, a path in a vendor URL.
        with pytest.raises(ValueError):
            vendor_spelling(raw)

    def test_agrees_with_the_ref_spellings(self):
        for raw in ["600519.SS", "600519.SH", "000001.SZ", "AAPL", "700.HK"]:
            ref = to_canonical(raw)
            assert display_spelling(raw) == to_legacy_api(ref)
            assert vendor_spelling(raw) == vendor_symbol(ref)


class TestParseInstrumentKey:
    def test_parse(self):
        assert parse_instrument_key("0700.XHKG") == ("0700", "XHKG")
        assert parse_instrument_key("BRK.B.XXXX") == ("BRK.B", "XXXX")

    @pytest.mark.parametrize("bad", ["AAPL", ".XNAS", "AAPL.", ""])
    def test_malformed(self, bad):
        with pytest.raises(ValueError):
            parse_instrument_key(bad)


class TestFromInstrumentKey:
    @pytest.mark.parametrize(
        "spelling", ["AAPL", "0700.HK", "600519.SH", "430047.BJ", "^GSPC", "BRK.B", "BTC-USD.CRYPTO",
                     "EUR-USD.FX"],
    )
    def test_every_minted_key_parses_back(self, spelling):
        ref = to_canonical(spelling)
        assert from_instrument_key(ref.instrument_key) == to_canonical(ref.instrument_key)

    @pytest.mark.parametrize(
        "key",
        [
            "THYAO.XIST",  # a venue this pin does not know
            "430047.XBSE",  # Bucharest, not Beijing
            "ES-2026Z.FUT",  # a segment this pin does not know
            "600519.SH",  # a spelling, not a key
            "AAPL",
        ],
    )
    def test_an_unknown_segment_is_refused(self, key):
        assert to_canonical(key)  # the lenient parser still answers
        with pytest.raises(ValueError):
            from_instrument_key(key)


class TestVenueTimezone:
    @pytest.mark.parametrize(
        ("symbol", "tz"),
        [
            ("AAPL", "America/New_York"), ("BRK.B", "America/New_York"),
            ("0700.HK", "Asia/Hong_Kong"), ("600519.SS", "Asia/Shanghai"),
            ("VOD.L", "Europe/London"), ("7203.T", "Asia/Tokyo"),
            ("SHOP.TO", "America/Toronto"), ("BHP.AX", "Australia/Sydney"),
            ("SAP.DE", "Europe/Berlin"), ("005930.KS", "Asia/Seoul"),
            ("2330.TW", "Asia/Taipei"), ("D05.SI", "Asia/Singapore"),
            ("RELIANCE.BO", "Asia/Kolkata"),
        ],
    )
    def test_listing_carries_its_venue_tz(self, symbol, tz):
        assert to_canonical(symbol).tz == tz


class TestDisplayDecimals:
    def test_defaults(self):
        assert display_decimals_for("USD", AssetClass.EQUITY) == 2
        assert display_decimals_for("JPY", AssetClass.EQUITY) == 0
        assert display_decimals_for("KRW", AssetClass.EQUITY) == 0
        assert display_decimals_for("USD", AssetClass.CRYPTO) == 8
        assert display_decimals_for("JPY", AssetClass.INDEX) == 2
        assert display_decimals_for("CNY", AssetClass.FUND) == 3

    @pytest.mark.parametrize("currency", ["USD", "JPY", "CNY"])
    @pytest.mark.parametrize("asset_class", list(AssetClass))
    def test_a_string_class_reads_as_the_enum(self, currency, asset_class):
        assert display_decimals_for(currency, asset_class.value) == display_decimals_for(
            currency, asset_class
        )

    def test_a_string_class_takes_its_class_branch(self):
        assert display_decimals_for("USD", "crypto") == 8
        assert display_decimals_for("JPY", "index") == 2
        assert display_decimals_for("USD", "fx") == 4

    def test_an_unknown_class_is_refused(self):
        with pytest.raises(ValueError):
            display_decimals_for("USD", "option")

    @pytest.mark.parametrize(
        ("spelling", "decimals"),
        [("EURUSD=X", 4), ("GBP-USD.FX", 4), ("JPY=X", 2), ("EUR-JPY.FX", 2), ("USD-KRW.FX", 2)],
    )
    def test_fx_prints_to_the_pip(self, spelling, decimals):
        """Two places past the quote currency's minor unit: 1.1690, 147.42."""
        ref = to_canonical(spelling)
        assert display_decimals_for(ref.price_currency, ref.asset_class) == decimals


class TestCnVenueInstruments:
    """CN code ranges name the asset class; every class shares one venue ref."""

    def test_beijing_keys_on_its_iso_mic(self):
        # XBSE is Bucharest's MIC; a Beijing listing never answers to it.
        assert to_canonical("430047.BJ").mic == "BJSE"
        assert to_canonical("430047.XBSE").mic != "BJSE"

    @pytest.mark.parametrize(
        ("spelling", "asset_class", "legacy", "display"),
        [
            ("600519.SH", AssetClass.EQUITY, "600519.SH", "600519.SH"),
            ("000001.SZ", AssetClass.EQUITY, "000001.SZ", "000001.SZ"),
            ("000001.SS", AssetClass.INDEX, "000001.SH", "000001.SH"),
            ("000300.SH", AssetClass.INDEX, "000300.SH", "000300.SH"),
            ("399001.SZ", AssetClass.INDEX, "399001.SZ", "399001.SZ"),
            ("510300.SS", AssetClass.FUND, "510300.SH", "510300.SH"),
            ("159915.SZ", AssetClass.FUND, "159915.SZ", "159915.SZ"),
            ("430047.BJ", AssetClass.EQUITY, "430047.BJ", "430047.BJ"),
            ("899050.BJ", AssetClass.INDEX, "899050.BJ", "899050.BJ"),
        ],
    )
    def test_class_and_spellings(self, spelling, asset_class, legacy, display):
        ref = to_canonical(spelling)
        assert ref.asset_class is asset_class
        assert ref.currency == "CNY" and ref.tz == "Asia/Shanghai" and ref.calendar_id == "XSHG"
        assert to_legacy_api(ref) == legacy
        assert to_display(ref) == display
        # A venue-listed index keeps its venue spelling, never a bare family name.
        assert vendor_symbol(ref) == vendor_spelling(legacy)

    @pytest.mark.parametrize(
        ("spelling", "currency"),
        [
            ("900901.SH", "USD"),
            ("900901.SS", "USD"),
            ("200002.SZ", "HKD"),
            ("200002.XSHE", "HKD"),
            # Each range belongs to its own venue's code space.
            ("900901.SZ", "CNY"),
            ("200002.SH", "CNY"),
        ],
    )
    def test_b_shares_carry_their_trading_currency(self, spelling, currency):
        ref = to_canonical(spelling)
        assert ref.asset_class is AssetClass.EQUITY and ref.calendar_id == "XSHG"
        assert (ref.currency, ref.price_currency) == (currency, currency)
        assert to_canonical(ref.instrument_key) == ref

    def test_equity_hint_cannot_demote_an_exchange_index(self):
        assert to_canonical("000001.SS", asset_class=AssetClass.EQUITY).asset_class is AssetClass.INDEX

    @pytest.mark.parametrize(
        ("spelling", "asset_class"),
        [
            ("600519.SH", AssetClass.EQUITY),
            ("000001.SZ", AssetClass.EQUITY),
            ("430047.BJ", AssetClass.EQUITY),
            ("000001.SH", AssetClass.INDEX),
            ("399001.SZ", AssetClass.INDEX),
            ("899050.BJ", AssetClass.INDEX),
            ("510300.SH", AssetClass.FUND),
            ("159915.SZ", AssetClass.FUND),
        ],
    )
    @pytest.mark.parametrize("hint", [None, AssetClass.EQUITY, AssetClass.INDEX, AssetClass.FUND])
    def test_code_range_outranks_any_hint(self, spelling, asset_class, hint):
        """The key carries no class, so a hint that disagrees with the code
        (a stock on the indexes endpoint) must not relabel the key's data."""
        ref = to_canonical(spelling, asset_class=hint)
        assert ref.asset_class is asset_class
        assert ref == to_canonical(ref.instrument_key)

    def test_index_hint_keeps_venue_key(self):
        ref = to_canonical("000001.SS", asset_class=AssetClass.INDEX)
        assert ref.instrument_key == "000001.XSHG"
        assert ref.mic == "XSHG"

    @pytest.mark.parametrize("spelling", ["FTSE.L", "FTSE.XLON"])
    def test_index_hint_off_a_code_range_takes_the_family_key(self, spelling):
        # A venue key would reparse without the hint as a stock quoted in pence.
        ref = to_canonical(spelling, asset_class=AssetClass.INDEX)
        assert (ref.instrument_key, ref.display_unit) == ("FTSE.INDEX", None)
        assert from_instrument_key(ref.instrument_key) == ref


class TestIndexHomeVenue:
    """Foreign index families carry their home venue's calendar, tz, currency."""

    @pytest.mark.parametrize(
        ("spelling", "calendar_id", "tz", "currency"),
        [
            ("^GSPC", "XNYS", "America/New_York", "USD"),
            ("^HSI", "XHKG", "Asia/Hong_Kong", "HKD"),
            ("^N225", "XTKS", "Asia/Tokyo", "JPY"),
            ("^FTSE", "XLON", "Europe/London", "GBP"),
            ("^GDAXI", "XETR", "Europe/Berlin", "EUR"),
        ],
    )
    def test_home_venue(self, spelling, calendar_id, tz, currency):
        ref = to_canonical(spelling)
        assert ref.asset_class is AssetClass.INDEX and ref.mic == "INDEX"
        assert (ref.calendar_id, ref.tz, ref.currency) == (calendar_id, tz, currency)

    def test_unknown_family_defaults_to_us(self):
        ref = to_canonical("XYZIDX", asset_class=AssetClass.INDEX)
        assert (ref.calendar_id, ref.currency) == ("XNYS", "USD")


class TestVenueHelpers:
    """Exported venue lookups, pinned to today's venue table."""

    @pytest.mark.parametrize(
        ("market", "home"),
        [
            ("us", ("XNYS", "America/New_York")),
            ("cn", ("XSHG", "Asia/Shanghai")),
            ("hk", ("XHKG", "Asia/Hong_Kong")),
            ("kr", ("XKRX", "Asia/Seoul")),
            ("in", ("XBOM", "Asia/Kolkata")),
            ("uk", ("XLON", "Europe/London")),
            # eu venues keep their own calendars; the rest name no venue.
            ("eu", None), ("other", None), ("crypto", None), ("fx", None),
        ],
    )
    def test_market_home(self, market, home):
        assert market_home(market) == home

    def test_venue_suffixes(self):
        assert venue_suffixes("cn") == {"SH", "SS", "SZ", "BJ"}
        assert venue_suffixes("hk", "uk") == {"HK", "L"}
        assert venue_suffixes("us") == frozenset()  # US listings are bare

    @pytest.mark.parametrize(
        ("mic", "code"),
        [
            ("XSHG", "SSE"), ("XSHE", "SZSE"), ("BJSE", "BSE"), ("XHKG", "HKEX"),
            ("XNYS", None), ("XBSE", None), (None, None),
        ],
    )
    def test_exchange_code(self, mic, code):
        assert exchange_code(mic) == code

    def test_caret_index_regions_name_only_foreign_families(self):
        assert CARET_INDEX_REGIONS == {
            "^HSI": "hk", "^HSCE": "hk", "^N225": "jp", "^FTSE": "uk",
            "^GDAXI": "eu", "^FCHI": "eu", "^STOXX50E": "eu",
        }

    def test_index_families_cover_every_family(self):
        refs = index_families()
        assert len(refs) == 13
        assert {ref.index_family for ref in refs} == {
            "SPX", "DJI", "COMP", "NDX", "RUT", "VIX", "HSI", "HSCE",
            "N225", "FTSE", "GDAXI", "FCHI", "STOXX50E",
        }
        assert all(is_family_index(ref) and ref == to_canonical(ref.instrument_key) for ref in refs)
