"""Lineage is per series: publisher plus asset class plus venue calendar."""

import pytest

from src.data_client.normalize import publisher_lineage
from market_protocol.enums import AssetClass, PriceTreatment, Tier


def test_fmp_is_realtime_and_split_adjusted_on_the_us_tape_only():
    assert publisher_lineage("fmp", AssetClass.EQUITY, "XNYS") == (PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME)
    assert publisher_lineage("fmp", AssetClass.EQUITY, None) == (PriceTreatment.SPLIT_ADJUSTED, Tier.REALTIME)
    assert publisher_lineage("fmp", AssetClass.EQUITY, "XHKG") == (PriceTreatment.SPLIT_ADJUSTED, Tier.DELAYED_15M)
    assert publisher_lineage("fmp", AssetClass.INDEX, "XHKG")[1] is Tier.DELAYED_15M


def test_fmp_cn_bars_are_raw_and_delayed():
    # calendar_id is XSHG for Shanghai, Shenzhen and Beijing alike.
    assert publisher_lineage("fmp", AssetClass.EQUITY, "XSHG") == (PriceTreatment.RAW, Tier.DELAYED_15M)


def test_tushare_adjusts_equities_only_regardless_of_venue():
    assert publisher_lineage("tushare", AssetClass.EQUITY, "XSHG")[0] is PriceTreatment.DIVIDEND_ADJUSTED
    assert publisher_lineage("tushare", AssetClass.INDEX, "XSHG")[0] is PriceTreatment.RAW
    assert publisher_lineage("tushare", AssetClass.FUND, "XSHG")[0] is PriceTreatment.RAW


def test_series_lineage_reads_the_ref_and_bars_and_quotes_agree_on_fmp():
    from market_protocol import to_canonical

    from src.data_client.normalize import series_lineage, snapshot_tier

    assert series_lineage("fmp", to_canonical("0700.HK")) == (PriceTreatment.SPLIT_ADJUSTED, Tier.DELAYED_15M)
    assert series_lineage("tushare", to_canonical("510300.SS"))[0] is PriceTreatment.RAW
    assert series_lineage("fmp", None) == publisher_lineage("fmp")
    assert (snapshot_tier("fmp", "XHKG"), snapshot_tier("fmp", "XNYS")) == (Tier.DELAYED_15M, Tier.REALTIME)


@pytest.mark.asyncio
async def test_fmp_is_delayed_on_a_venue_the_protocol_does_not_know():
    # PETR4.SA parses to the unknown venue on XNYS, its default home; it is not
    # on the US tape, so neither its bars nor its stamped quote are realtime. A
    # US class share parses the same way and stays realtime, as does a cell
    # declared without an instrument.
    from market_protocol import to_canonical

    from src.data_client.market_data_provider import MarketDataProvider, ProviderEntry
    from src.data_client.normalize import series_lineage, snapshot_tier

    assert series_lineage("fmp", to_canonical("PETR4.SA"))[1] is Tier.DELAYED_15M
    assert series_lineage("fmp", to_canonical("BRK.B"))[1] is Tier.REALTIME
    assert snapshot_tier("fmp", None) is Tier.REALTIME

    class Quotes:
        async def get_snapshots(self, *, symbols, **_):
            return [{"symbol": s, "price": 1.0} for s in symbols]

    provider = MarketDataProvider([ProviderEntry("fmp", Quotes(), {"all"})])
    rows = {r["symbol"]: r["tier"] for r in await provider.get_snapshots(["PETR4.SA", "BRK.B", "AAPL"])}
    assert rows == {"PETR4.SA": "delayed_15m", "BRK.B": "realtime", "AAPL": "realtime"}


def test_ginlix_data_indexes_are_delayed_and_its_equities_realtime():
    assert publisher_lineage("ginlix-data", AssetClass.INDEX, "XNYS")[1] is Tier.DELAYED_15M
    assert publisher_lineage("ginlix-data", AssetClass.EQUITY, "XNYS")[1] is Tier.REALTIME
