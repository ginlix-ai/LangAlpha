"""Protocol boundary on the market-data router: spelling collapse + reverse map.

Every inbound spelling of one instrument must map to a single legacy-form
symbol before touching caches or providers (Phase 1; cache keys cut over to
instrument_key in Phase 3).
"""

from src.server.app.market_data import _boundary_symbol, _boundary_symbols


class TestBoundarySymbol:
    def test_us_spellings_collapse(self):
        assert _boundary_symbol("AAPL") == "AAPL"
        assert _boundary_symbol("aapl") == "AAPL"
        assert _boundary_symbol("AAPL.US") == "AAPL"

    def test_index_spellings_collapse_to_legacy_bare(self):
        for spelling in ("GSPC", "^GSPC", "I:SPX", "SPX.INDEX"):
            assert _boundary_symbol(spelling, is_index=True) == "GSPC", spelling

    def test_foreign_suffixes_round_trip(self):
        assert _boundary_symbol("0700.HK") == "0700.HK"
        assert _boundary_symbol("0700.XHKG") == "0700.HK"
        assert _boundary_symbol("VOD.L") == "VOD.L"

    def test_us_class_shares_survive(self):
        # Dotted class shares must not lose their suffix (BRK.B ≠ BRK).
        assert _boundary_symbol("BRK.B") == "BRK.B"
        assert _boundary_symbol("BF.B") == "BF.B"

    def test_unknown_symbol_passes_through(self):
        assert _boundary_symbol("ZZZZFAKE1") == "ZZZZFAKE1"


class TestBoundarySymbols:
    def test_collapsing_spellings_dedupe(self):
        assert _boundary_symbols(["AAPL", "aapl", "AAPL.US", "MSFT"]) == ["AAPL", "MSFT"]

    def test_index_batch(self):
        assert _boundary_symbols(["^GSPC", "I:SPX", "IXIC"], is_index=True) == ["GSPC", "IXIC"]
