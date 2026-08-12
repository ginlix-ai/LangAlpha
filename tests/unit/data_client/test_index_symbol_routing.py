"""Index symbol caret handling and region routing."""

from src.data_client.fmp.data_source import FMPDataSource
from src.data_client.market_data_provider import symbol_market


class TestCaretLogic:
    def test_us_index_gets_caret(self):
        assert FMPDataSource._api_symbol("GSPC", is_index=True) == "^GSPC"

    def test_already_caret_unchanged(self):
        assert FMPDataSource._api_symbol("^HSI", is_index=True) == "^HSI"

    def test_suffixed_cn_index_not_careted(self):
        # ^000300.SS is not a valid ticker anywhere; suffixed symbols pass through
        assert FMPDataSource._api_symbol("000300.SS", is_index=True) == "000300.SS"

    def test_non_index_unchanged(self):
        assert FMPDataSource._api_symbol("AAPL", is_index=False) == "AAPL"


class TestIndexRegionRouting:
    def test_known_foreign_indices_skip_us(self):
        assert symbol_market("^HSI") == "hk"
        assert symbol_market("^N225") == "jp"
        assert symbol_market("^FTSE") == "uk"
        assert symbol_market("^GDAXI") == "eu"

    def test_case_insensitive_caret_indices(self):
        assert symbol_market("^hsi") == "hk"
        assert symbol_market("^n225") == "jp"

    def test_us_indices_stay_us(self):
        assert symbol_market("^GSPC") == "us"
        assert symbol_market("^VIX") == "us"

    def test_plain_us_equity_unchanged(self):
        assert symbol_market("AAPL") == "us"

    def test_cn_suffix_unchanged(self):
        assert symbol_market("000300.SS") == "cn"
