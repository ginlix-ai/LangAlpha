# pyright: ignore
"""Stock screener (FMP) with snake_case -> camelCase parameter mapping."""

from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import logging

from market_protocol import AssetClass, display_decimals_for
from market_protocol.symbology import MINOR_UNIT_PLACES

from ._shared import _mark_failure
from .currency import DisplaySpec, fmt_count, fmt_money, fmt_price
from src.data_client import get_financial_data_provider
from src.data_client.normalize import screen_row_units

logger = logging.getLogger(__name__)


# The market overview names S&P 500 sectors by GICS; FMP's screener files the
# same companies under its own names, and matches no other spelling.
_FMP_SECTORS = {
    "information technology": "Technology",
    "health care": "Healthcare",
    "financials": "Financial Services",
    "consumer discretionary": "Consumer Cyclical",
    "consumer staples": "Consumer Defensive",
    "materials": "Basic Materials",
}


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


# Spellings a caller uses for a country the screens know only by its ISO code.
_COUNTRY_ALIASES = {"UK": "GB", "USA": "US", "CHN": "CN", "CHINA": "CN"}

_UNKNOWN_UNIT = DisplaySpec(None, 2, bare=True)


def _row_specs(stock: Dict[str, Any]) -> Tuple[DisplaySpec, DisplaySpec]:
    """(price, dividend) specs for *stock*, from the units ``screen_row_units`` named.

    A pence quote keeps its places in pounds (0.35p is £0.0035), with trailing
    zeros dropped so a whole-penny price still reads £37.84.
    """
    currency = stock["currency"]
    if currency is None:
        return _UNKNOWN_UNIT, _UNKNOWN_UNIT
    try:
        decimals = display_decimals_for(currency, AssetClass.EQUITY)
    except Exception:
        decimals = 2
    places = MINOR_UNIT_PLACES.get(stock["quoteUnit"] or "", 0)
    return (
        DisplaySpec(currency, decimals + places, trim=bool(places)),
        DisplaySpec(currency, max(decimals, 2) + places, trim=True),
    )


def _threshold_units(rows: list) -> Tuple[DisplaySpec, DisplaySpec]:
    """(money unit, quote unit) the screen applied its thresholds in.

    Each listing is compared in its own currency (a JP screen in yen), and a
    London price or dividend in pence although its cap is in pounds. Only a
    screen whose rows all share one unit can name it; any other prints the
    bare figure.
    """
    currencies = {row["currency"] for row in rows}
    if len(currencies) != 1 or None in currencies:
        return _UNKNOWN_UNIT, _UNKNOWN_UNIT
    quote_units = {row["quoteUnit"] for row in rows}
    quote_unit = DisplaySpec(quote_units.pop(), 2) if len(quote_units) == 1 else _UNKNOWN_UNIT
    return DisplaySpec(currencies.pop(), 2), quote_unit


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
    if country:
        country = country.strip().upper()
        country = _COUNTRY_ALIASES.get(country, country)
    if sector:
        sector = _FMP_SECTORS.get(sector.strip().lower(), sector)
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
            _mark_failure("not_configured")
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

        results = screen_row_units(results)
        # A screen of every venue compares each listing in its own currency,
        # whatever the few rows it returns happen to share.
        unscoped = not country and not exchange
        money_unit, quote_unit = (_UNKNOWN_UNIT, _UNKNOWN_UNIT) if unscoped else _threshold_units(results)
        active_filters = {}
        if sector:
            active_filters["Sector"] = sector
        if industry:
            active_filters["Industry"] = industry
        if exchange:
            active_filters["Exchange"] = exchange
        if country:
            active_filters["Country"] = country
        # (label, bar, how it prints, whether it is money)
        bars = [
            ("Mkt Cap >", market_cap_more_than, lambda v: fmt_money(v, money_unit), True),
            ("Mkt Cap <", market_cap_lower_than, lambda v: fmt_money(v, money_unit), True),
            ("Price >", price_more_than, lambda v: fmt_price(v, quote_unit), True),
            ("Price <", price_lower_than, lambda v: fmt_price(v, quote_unit), True),
            ("Vol >", volume_more_than, lambda v: fmt_count(v, places=0), False),
            ("Vol <", volume_lower_than, lambda v: fmt_count(v, places=0), False),
            ("Beta >", beta_more_than, lambda v: f"{v:.2f}", False),
            ("Beta <", beta_lower_than, lambda v: f"{v:.2f}", False),
            ("Dividend/Share >", dividend_more_than, lambda v: fmt_price(v, quote_unit), True),
            ("Dividend/Share <", dividend_lower_than, lambda v: fmt_price(v, quote_unit), True),
        ]
        active_filters.update((label, show(bar)) for label, bar, show, _ in bars if bar is not None)
        priced = any(money and bar is not None for _, bar, _, money in bars)

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = []
        lines.append(f"## Stock Screener Results ({len(results)} stocks)")
        lines.append(f"**Retrieved:** {timestamp}")
        lines.append("")

        if active_filters:
            filter_parts = [f"{k}: {v}" for k, v in active_filters.items()]
            mixed = " (each listing's own currency)" if priced and money_unit.currency is None else ""
            lines.append(f"**Filters:** {' | '.join(filter_parts)}{mixed}")
            lines.append("")

        table = []
        for stock in results:
            price_spec, dividend_spec = _row_specs(stock)
            name = stock.get("companyName") or "N/A"
            beta = stock.get("beta")
            change = stock.get("change")
            table.append({
                "Symbol": stock.get("symbol") or "N/A",
                "Name": name if len(name) <= 25 else name[:22] + "...",
                "Price": fmt_price(stock.get("price"), price_spec),
                "Mkt Cap": fmt_money(stock.get("marketCap"), price_spec),
                "Sector": stock.get("sector") or "N/A",
                "Beta": f"{beta:.2f}" if beta is not None else "N/A",
                "Volume": fmt_count(stock.get("volume"), places=0),
                "Dividend/Share": fmt_price(stock.get("lastAnnualDividend"), dividend_spec),
                "Change%": f"{change:+.2f}%" if change is not None else "N/A",
            })
        # A column no row fills says nothing: FMP's screen sends no change, the
        # CN one no volume, sector or beta.
        heads = [h for h in table[0] if any(row[h] != "N/A" for row in table)]
        lines.append(f"| {' | '.join(heads)} |")
        lines.append(f"|{'|'.join('-' * (len(h) + 2) for h in heads)}|")
        lines.extend(f"| {' | '.join(row[h] for h in heads)} |" for row in table)

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
        _mark_failure(e)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        error_content = f"## Stock Screener\n**Retrieved:** {timestamp}\n**Status:** Error\n\nError screening stocks: {str(e)}"
        return error_content, {"type": "stock_screener", "results": [], "filters": {}, "count": 0}
