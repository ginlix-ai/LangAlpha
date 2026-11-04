"""Financial data source backed by FMP (Financial Modeling Prep)."""

from __future__ import annotations

from typing import Any

from src.data_client.base import SCREEN_HOME_EXCHANGES
from src.data_client.normalize import minor_unit_places, scale_quote_prices

from .fmp_client import FMPClient

_EXCHANGE_HOME = {ex: country for country, exs in SCREEN_HOME_EXCHANGES.items() for ex in exs}


def _names_its_unit(row: dict[str, Any]) -> dict[str, Any]:
    """*row*, with ``currency`` null when its unit is unknown.

    London trades many lines in their own currency (Toyota's in yen, the
    international order book's 0-prefixed codes in dollars or euros even for a
    UK issuer), and FMP's row names no currency. An issuer domiciled elsewhere
    may still quote in pence, but a figure left unlabelled is better than one
    labelled wrongly, so only a home issuer's ordinary London line is pence.
    """
    symbol = str(row.get("symbol") or "")
    foreign = _EXCHANGE_HOME.get(row.get("exchangeShortName")) != row.get("country")
    if minor_unit_places(symbol) and (foreign or symbol.startswith("0")):
        return {**row, "currency": None}
    return row


class FMPFinancialSource:
    """FMP implementation of FinancialDataSource.

    Receives a shared :class:`FMPClient` instance (typically the singleton from
    ``get_fmp_client()``) to reuse its connection pool and response cache.
    """

    def __init__(self, client: FMPClient) -> None:
        self._client = client

    async def get_company_profile(self, symbol: str) -> list[dict[str, Any]]:
        return scale_quote_prices(await self._client.get_profile(symbol), symbol)

    async def get_realtime_quote(self, symbol: str) -> list[dict[str, Any]]:
        return scale_quote_prices(await self._client.get_quote(symbol), symbol)

    async def get_income_statements(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        return await self._client.get_income_statement(
            symbol, period=period, limit=limit
        )

    async def get_cash_flows(
        self, symbol: str, period: str = "quarter", limit: int = 8
    ) -> list[dict[str, Any]]:
        return await self._client.get_cash_flow(symbol, period=period, limit=limit)

    async def get_key_metrics(self, symbol: str) -> list[dict[str, Any]]:
        return await self._client.get_key_metrics_ttm(symbol)

    async def get_financial_ratios(self, symbol: str) -> list[dict[str, Any]]:
        return await self._client.get_ratios_ttm(symbol)

    async def get_price_performance(self, symbol: str) -> list[dict[str, Any]]:
        return await self._client.get_stock_price_change(symbol)

    async def get_analyst_price_targets(
        self, symbol: str
    ) -> list[dict[str, Any]]:
        return await self._client.get_price_target_consensus(symbol)

    async def get_analyst_ratings(
        self, symbol: str
    ) -> list[dict[str, Any]]:
        return await self._client.get_grades_summary(symbol)

    async def get_earnings_history(
        self, symbol: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        return await self._client.get_historical_earnings_calendar(
            symbol, limit=limit
        )

    async def get_revenue_by_segment(
        self, symbol: str, segment_type: str = "product", **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Rows in the ``[{date: {segment: value}}]`` shape every source answers.

        FMP's stable API answers ``{date, data, ...}`` rows whatever
        ``structure`` asks for; left as is, the first key read as the date is
        ``symbol``.
        """
        fetch = (
            self._client.get_revenue_geographic_segmentation
            if segment_type == "geography"
            else self._client.get_revenue_product_segmentation
        )
        return [
            {row["date"]: row["data"]}
            if isinstance(row, dict) and row.get("date") and isinstance(row.get("data"), dict)
            else row
            for row in await fetch(symbol, **kwargs) or []
        ]

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        """A stock screen: funds and ETFs only when asked, on the country's own exchanges.

        FMP includes both fund types unless told otherwise, each share class
        carrying the whole fund's assets as its market cap.
        """
        filters = {"isEtf": False, "isFund": False, **filters}
        country = str(filters.get("country") or "").strip().upper()
        if not filters.get("exchange") and country in SCREEN_HOME_EXCHANGES:
            filters["exchange"] = ",".join(SCREEN_HOME_EXCHANGES[country])
        rows = await self._client.get_company_screener(**filters)
        return [_names_its_unit(row) for row in rows] if isinstance(rows, list) else rows

    async def search_stocks(
        self, query: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        return await self._client.search_stocks(query=query, limit=limit)

    async def close(self) -> None:
        pass  # client lifecycle managed by get_fmp_client singleton
