"""Financial data source backed by FMP (Financial Modeling Prep)."""

from __future__ import annotations

from typing import Any

from .fmp_client import FMPClient


class FMPFinancialSource:
    """FMP implementation of FinancialDataSource.

    Receives a shared :class:`FMPClient` instance (typically the singleton from
    ``get_fmp_client()``) to reuse its connection pool and response cache.
    """

    def __init__(self, client: FMPClient) -> None:
        self._client = client

    async def get_company_profile(self, symbol: str) -> list[dict[str, Any]]:
        return await self._client.get_profile(symbol)

    async def get_realtime_quote(self, symbol: str) -> list[dict[str, Any]]:
        return await self._client.get_quote(symbol)

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
        if segment_type == "geography":
            return await self._client.get_revenue_geographic_segmentation(
                symbol, **kwargs
            )
        return await self._client.get_revenue_product_segmentation(
            symbol, **kwargs
        )

    async def get_sector_performance(self) -> list[dict[str, Any]]:
        return await self._client._make_request("sectors-performance")

    async def screen_stocks(self, **filters: Any) -> list[dict[str, Any]]:
        return await self._client.get_company_screener(**filters)

    async def search_stocks(
        self, query: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        return await self._client.search_stocks(query=query, limit=limit)

    async def close(self) -> None:
        pass  # client lifecycle managed by get_fmp_client singleton
