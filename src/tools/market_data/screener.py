# pyright: ignore
"""Stock screener (FMP) with snake_case -> camelCase parameter mapping."""

from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import logging


from .currency import fmt_count, fmt_money, fmt_price
from .display import (
    _symbol_currency,
    resolve_ref,
)
from src.data_client import get_financial_data_provider

logger = logging.getLogger(__name__)


# Mapping from snake_case parameter names to FMP camelCase API params
_SCREENER_PARAM_MAP = {
    "market_cap_more_than": "marketCapMoreThan",
    "market_cap_lower_than": "marketCapLowerThan",
    "price_more_than": "priceMoreThan",
    "price_lower_than": "priceLowerThan",
    "volume_more_than": "volumeMoreThan",
    "volume_lower_than": "volumeLowerThan",
    "beta_more_than": "betaMoreThan",
    "beta_lower_than": "betaLowerThan",
    "dividend_more_than": "dividendMoreThan",
    "dividend_lower_than": "dividendLowerThan",
    "is_etf": "isEtf",
    "is_fund": "isFund",
    "is_actively_trading": "isActivelyTrading",
}


def _screened_currency(results: list) -> Optional[str]:
    """The listing currency every returned row shares; None (dollars) otherwise.

    The screen that answers a market applies the price and cap thresholds in
    that market's currency (a CN screen in CNY), so the echoed filter has to
    name it. A mixed answer has no single currency and keeps the dollar.
    """
    currencies = set()
    for row in results:
        ref = resolve_ref(row.get("symbol")) if isinstance(row, dict) else None
        if ref is not None and ref.price_currency:
            currencies.add(ref.price_currency)
    return currencies.pop() if len(currencies) == 1 else None


async def fetch_stock_screener(
    market_cap_more_than: Optional[float] = None,
    market_cap_lower_than: Optional[float] = None,
    price_more_than: Optional[float] = None,
    price_lower_than: Optional[float] = None,
    volume_more_than: Optional[float] = None,
    volume_lower_than: Optional[float] = None,
    beta_more_than: Optional[float] = None,
    beta_lower_than: Optional[float] = None,
    dividend_more_than: Optional[float] = None,
    dividend_lower_than: Optional[float] = None,
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    exchange: Optional[str] = None,
    country: Optional[str] = None,
    is_etf: Optional[bool] = None,
    is_fund: Optional[bool] = None,
    is_actively_trading: Optional[bool] = None,
    limit: int = 50,
) -> Tuple[str, Dict[str, Any]]:
    """
    Screen stocks using FMP company screener API.

    Returns:
        Tuple of (markdown content, artifact dict for frontend rendering)
    """
    try:
        provider = await get_financial_data_provider()
        financial = provider.financial

        # Build API params with camelCase conversion
        local_params = {
            "market_cap_more_than": market_cap_more_than,
            "market_cap_lower_than": market_cap_lower_than,
            "price_more_than": price_more_than,
            "price_lower_than": price_lower_than,
            "volume_more_than": volume_more_than,
            "volume_lower_than": volume_lower_than,
            "beta_more_than": beta_more_than,
            "beta_lower_than": beta_lower_than,
            "dividend_more_than": dividend_more_than,
            "dividend_lower_than": dividend_lower_than,
            "is_etf": is_etf,
            "is_fund": is_fund,
            "is_actively_trading": is_actively_trading,
        }
        api_params = {}
        for snake_key, value in local_params.items():
            if value is not None:
                camel_key = _SCREENER_PARAM_MAP.get(snake_key, snake_key)
                api_params[camel_key] = value

        # String params pass through directly
        if sector:
            api_params["sector"] = sector
        if industry:
            api_params["industry"] = industry
        if exchange:
            api_params["exchange"] = exchange
        if country:
            api_params["country"] = country
        if limit:
            api_params["limit"] = limit

        if financial is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return (
                f"## Stock Screener\n**Retrieved:** {timestamp}\n\nNo financial data source configured.",
                {"type": "stock_screener", "results": [], "filters": {}, "count": 0},
            )

        results = await financial.screen_stocks(**api_params)

        if not results or not isinstance(results, list):
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return (
                f"## Stock Screener Results\n**Retrieved:** {timestamp}\n\nNo stocks matched the given criteria.",
                {"type": "stock_screener", "results": [], "filters": api_params, "count": 0},
            )

        # Build active filters summary for display
        filter_cur = _screened_currency(results)
        active_filters = {}
        if sector:
            active_filters["Sector"] = sector
        if industry:
            active_filters["Industry"] = industry
        if exchange:
            active_filters["Exchange"] = exchange
        if country:
            active_filters["Country"] = country
        if market_cap_more_than is not None:
            active_filters["Mkt Cap >"] = fmt_money(market_cap_more_than, filter_cur)
        if market_cap_lower_than is not None:
            active_filters["Mkt Cap <"] = fmt_money(market_cap_lower_than, filter_cur)
        if price_more_than is not None:
            active_filters["Price >"] = fmt_price(price_more_than, filter_cur)
        if price_lower_than is not None:
            active_filters["Price <"] = fmt_price(price_lower_than, filter_cur)
        if volume_more_than is not None:
            active_filters["Vol >"] = fmt_count(volume_more_than)
        if volume_lower_than is not None:
            active_filters["Vol <"] = fmt_count(volume_lower_than)
        if beta_more_than is not None:
            active_filters["Beta >"] = f"{beta_more_than:.2f}"
        if beta_lower_than is not None:
            active_filters["Beta <"] = f"{beta_lower_than:.2f}"
        if dividend_more_than is not None:
            active_filters["Dividend/Share >"] = f"{dividend_more_than:.2f}"
        if dividend_lower_than is not None:
            active_filters["Dividend/Share <"] = f"{dividend_lower_than:.2f}"

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = []
        lines.append(f"## Stock Screener Results ({len(results)} stocks)")
        lines.append(f"**Retrieved:** {timestamp}")
        lines.append("")

        if active_filters:
            filter_parts = [f"{k}: {v}" for k, v in active_filters.items()]
            lines.append(f"**Filters:** {' | '.join(filter_parts)}")
            lines.append("")

        # Results table
        lines.append("| Symbol | Name | Price | Mkt Cap | Sector | Beta | Volume | Change% |")
        lines.append("|--------|------|-------|---------|--------|------|--------|---------|")

        for stock in results:
            sym = stock.get("symbol", "N/A")
            # A CN row carries these keys as null, which .get's default misses.
            name = stock.get("companyName") or "N/A"
            if len(name) > 25:
                name = name[:22] + "..."
            price = stock.get("price")
            mkt_cap = stock.get("marketCap")
            sect = stock.get("sector") or "N/A"
            beta = stock.get("beta")
            volume = stock.get("volume")
            change = stock.get("change")

            row_cur = _symbol_currency(resolve_ref(sym))
            price_str = fmt_price(price, row_cur)
            cap_str = fmt_money(mkt_cap, row_cur)
            beta_str = f"{beta:.2f}" if beta is not None else "N/A"
            vol_str = fmt_count(volume) if volume is not None else "N/A"
            if change is not None:
                sign = "+" if change >= 0 else ""
                change_str = f"{sign}{change:.2f}%"
            else:
                change_str = "N/A"

            lines.append(f"| {sym} | {name} | {price_str} | {cap_str} | {sect} | {beta_str} | {vol_str} | {change_str} |")

        content = "\n".join(lines)

        artifact = {
            "type": "stock_screener",
            "results": results,
            "filters": active_filters,
            "count": len(results),
        }

        return content, artifact

    except Exception as e:
        logger.error(f"Error in stock screener: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        error_content = f"## Stock Screener\n**Retrieved:** {timestamp}\n**Status:** Error\n\nError screening stocks: {str(e)}"
        return error_content, {"type": "stock_screener", "results": [], "filters": {}, "count": 0}
