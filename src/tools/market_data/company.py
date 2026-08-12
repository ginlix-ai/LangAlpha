# pyright: ignore
"""Company overview: profile, financials, analyst views, and fiscal-period matching."""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
import logging
import asyncio

from langchain_core.runnables import RunnableConfig

from .currency import fmt_count, fmt_money, fmt_price
from .display import (
    _is_us_clock,
    _market_status_line,
    _symbol_currency,
    resolve_ref,
)
from .quote_format import build_live_stamp
from .utils import format_number, format_percentage, get_market_session
from src.data_client import get_financial_data_provider, get_market_data_provider
from src.market_protocol import to_legacy_api

from ._shared import _fmp_request, _get_user_id, _safe_result

logger = logging.getLogger(__name__)


# Constants for fiscal period matching
FILING_DATE_TOLERANCE_DAYS = (
    5  # Allow 5 days difference when matching filings to earnings
)
DAYS_PER_QUARTER = 90  # Approximate days per fiscal quarter


def _build_fiscal_period_lookup(income_stmt: List[Dict]) -> Dict[str, str]:
    """Build a lookup dict mapping fiscal end dates to period names (e.g., 'Q3 FY2026')."""
    lookup = {}
    for stmt in income_stmt:
        stmt_date = stmt.get("date")
        period = stmt.get("period")  # Q1, Q2, Q3, Q4
        fiscal_year = stmt.get("fiscalYear")
        if stmt_date and period and fiscal_year:
            lookup[stmt_date] = f"{period} FY{fiscal_year}"
    return lookup


def _margin(stmt: Dict, ratio_key: str, numerator_key: str) -> Optional[float]:
    """Margin fraction for an income-statement row.

    Prefers the provider's ratio field when present; FMP's stable API dropped
    the v3-era ``*Ratio`` fields, so otherwise it is derived from the raw
    dollar fields still in the payload (``numerator / revenue``).
    """
    ratio = stmt.get(ratio_key)
    if ratio is not None:
        return ratio
    revenue = stmt.get("revenue")
    numerator = stmt.get(numerator_key)
    if revenue and numerator is not None:
        return numerator / revenue
    return None


def _infer_fiscal_period(
    fiscal_ending: str, fiscal_period_lookup: Dict[str, str]
) -> Optional[str]:
    """
    Infer fiscal period name for a date not in the lookup.
    Uses the pattern from existing quarters to estimate future quarters.
    """
    if not fiscal_ending or not fiscal_period_lookup:
        return None

    try:
        fe_date = datetime.strptime(fiscal_ending, "%Y-%m-%d")

        # Find the most recent known quarter
        for date_str, period_str in sorted(fiscal_period_lookup.items(), reverse=True):
            if not period_str.startswith("Q"):
                continue

            last_date = datetime.strptime(date_str, "%Y-%m-%d")
            last_q = int(period_str[1])
            last_fy = int(period_str.split("FY")[1])

            # Calculate quarter offset from days difference
            days_diff = (fe_date - last_date).days
            quarters_ahead = round(days_diff / DAYS_PER_QUARTER)
            next_q = last_q + quarters_ahead
            next_fy = last_fy

            # Handle fiscal year rollover
            while next_q > 4:
                next_q -= 4
                next_fy += 1
            while next_q < 1:
                next_q += 4
                next_fy -= 1

            return f"Q{next_q} FY{next_fy}"

    except (ValueError, KeyError) as e:
        logger.debug(f"Could not infer fiscal period for {fiscal_ending}: {e}")

    return None


def _match_filing_to_fiscal_period(
    filing_date: str,
    earnings_calendar: List[Dict],
    fiscal_period_lookup: Dict[str, str],
) -> str:
    """
    Match a SEC filing date to its fiscal period using earnings calendar.
    Returns the fiscal period name or 'Quarterly' if no match found.
    """
    if not earnings_calendar or not filing_date or filing_date == "N/A":
        return "Quarterly"

    try:
        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        best_match = None
        min_diff = float("inf")

        for cal in earnings_calendar:
            cal_date = cal.get("date")
            fiscal_ending = cal.get("fiscalDateEnding")
            if not cal_date or not fiscal_ending:
                continue

            try:
                cal_dt = datetime.strptime(cal_date, "%Y-%m-%d")
                diff = abs((filing_dt - cal_dt).days)
                if diff < min_diff and diff <= FILING_DATE_TOLERANCE_DAYS:
                    min_diff = diff
                    if fiscal_ending in fiscal_period_lookup:
                        best_match = fiscal_period_lookup[fiscal_ending]
            except ValueError:
                continue

        return best_match or "Quarterly"

    except ValueError:
        return "Quarterly"


async def fetch_company_overview_data(symbol: str) -> Dict[str, Any]:
    """
    Fetch company overview data and return structured artifact dict.

    Shared by both the agent tool and the REST API endpoint.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")

    Returns:
        Dict with structured data for charts (same shape as agent artifact)
    """
    provider = await get_financial_data_provider()
    financial = provider.financial
    if financial is None:
        return {"type": "company_overview", "symbol": symbol}

    profile_data = await financial.get_company_profile(symbol)
    if not profile_data:
        return {"type": "company_overview", "symbol": symbol}

    profile = profile_data[0]
    company_name = profile.get("companyName", symbol)

    # === PARALLEL DATA FETCH ===
    (
        income_stmt_result,
        earnings_calendar_result,
        price_change_result,
        key_metrics_result,
        ratios_result,
        price_target_consensus_result,
        grades_summary_result,
        product_data_result,
        geo_data_result,
        quote_result,
        cash_flow_result,
    ) = await asyncio.gather(
        financial.get_income_statements(symbol, period="quarter", limit=8),
        financial.get_earnings_history(symbol, limit=10),
        financial.get_price_performance(symbol),
        financial.get_key_metrics(symbol),
        financial.get_financial_ratios(symbol),
        financial.get_analyst_price_targets(symbol),
        financial.get_analyst_ratings(symbol),
        financial.get_revenue_by_segment(
            symbol, segment_type="product", period="quarter", structure="flat"
        ),
        financial.get_revenue_by_segment(
            symbol, segment_type="geography", period="quarter", structure="flat"
        ),
        financial.get_realtime_quote(symbol),
        financial.get_cash_flows(symbol, period="quarter", limit=8),
        return_exceptions=True,
    )

    income_stmt = _safe_result(income_stmt_result, [])
    earnings_calendar = _safe_result(earnings_calendar_result, [])
    price_change_data = _safe_result(price_change_result, [])
    quote_data = _safe_result(quote_result, [])
    grades_summary_data = _safe_result(grades_summary_result, [])
    product_data = _safe_result(product_data_result, [])
    geo_data = _safe_result(geo_data_result, [])
    cash_flow_data = _safe_result(cash_flow_result, [])

    fiscal_period_lookup = _build_fiscal_period_lookup(income_stmt)

    # Build artifact
    artifact: Dict[str, Any] = {
        "type": "company_overview",
        "symbol": symbol,
        "name": company_name,
    }

    # Quote data
    if quote_data and len(quote_data) > 0:
        quote = quote_data[0]
        artifact["quote"] = {
            "price": quote.get("price"),
            "change": quote.get("change"),
            "changePct": quote.get("changePercentage"),
            "dayHigh": quote.get("dayHigh"),
            "dayLow": quote.get("dayLow"),
            "yearHigh": quote.get("yearHigh"),
            "yearLow": quote.get("yearLow"),
            "open": quote.get("open"),
            "previousClose": quote.get("previousClose"),
            "volume": quote.get("volume"),
            "avgVolume": quote.get("avgVolume"),
            "marketCap": quote.get("marketCap"),
            "pe": quote.get("pe"),
            "eps": quote.get("eps"),
        }

    # Performance data
    if price_change_data:
        changes = price_change_data[0]
        artifact["performance"] = {
            k: changes.get(k)
            for k in ["1D", "5D", "1M", "3M", "6M", "ytd", "1Y", "3Y", "5Y"]
            if changes.get(k) is not None
        }

    # Analyst ratings
    if grades_summary_data:
        gs = grades_summary_data[0]
        artifact["analystRatings"] = {
            "strongBuy": gs.get("strongBuy", 0),
            "buy": gs.get("buy", 0),
            "hold": gs.get("hold", 0),
            "sell": gs.get("sell", 0),
            "strongSell": gs.get("strongSell", 0),
            "consensus": gs.get("consensus", "N/A"),
        }

    # Revenue by product
    if product_data and len(product_data) > 0:
        latest_product_record = product_data[0]
        if latest_product_record and isinstance(latest_product_record, dict):
            fiscal_date = list(latest_product_record.keys())[0]
            product_revenues = latest_product_record[fiscal_date]
            if product_revenues and isinstance(product_revenues, dict) and len(product_revenues) > 0:
                artifact["revenueByProduct"] = product_revenues

    # Revenue by geography
    if geo_data and len(geo_data) > 0:
        latest_geo_record = geo_data[0]
        if latest_geo_record and isinstance(latest_geo_record, dict):
            geo_date = list(latest_geo_record.keys())[0]
            geo_revenues = latest_geo_record[geo_date]
            if geo_revenues and isinstance(geo_revenues, dict) and len(geo_revenues) > 0:
                artifact["revenueByGeo"] = geo_revenues

    # Quarterly fundamentals from income statement (oldest-first for charting)
    if income_stmt:
        artifact["quarterlyFundamentals"] = [
            {
                "period": fiscal_period_lookup.get(stmt.get("date"), stmt.get("date", "")),
                "date": stmt.get("date"),
                "revenue": stmt.get("revenue"),
                "netIncome": stmt.get("netIncome"),
                "grossProfit": stmt.get("grossProfit"),
                "operatingIncome": stmt.get("operatingIncome"),
                "ebitda": stmt.get("ebitda"),
                "epsDiluted": stmt.get("epsdiluted"),
                "grossMargin": _margin(stmt, "grossProfitRatio", "grossProfit"),
                "operatingMargin": _margin(stmt, "operatingIncomeRatio", "operatingIncome"),
                "netMargin": _margin(stmt, "netIncomeRatio", "netIncome"),
            }
            for stmt in reversed(income_stmt)
        ]

    # Earnings surprises (reported only, oldest-first)
    reported_for_artifact = [
        e for e in earnings_calendar if e.get("epsActual") is not None
    ]
    if reported_for_artifact:
        artifact["earningsSurprises"] = [
            {
                "period": fiscal_period_lookup.get(
                    e.get("fiscalDateEnding"), e.get("date", "")
                ),
                "date": e.get("date"),
                "epsActual": e.get("epsActual"),
                "epsEstimate": e.get("epsEstimated"),
                "revenueActual": e.get("revenueActual"),
                "revenueEstimate": e.get("revenueEstimated"),
            }
            for e in reversed(reported_for_artifact)
        ]

    # Cash flow (oldest-first for charting)
    if cash_flow_data:
        artifact["cashFlow"] = [
            {
                "period": fiscal_period_lookup.get(cf.get("date"), cf.get("date", "")),
                "date": cf.get("date"),
                "operatingCashFlow": cf.get("operatingCashFlow"),
                "capitalExpenditure": cf.get("capitalExpenditure"),
                "freeCashFlow": cf.get("freeCashFlow"),
            }
            for cf in reversed(cash_flow_data)
        ]

    return artifact


async def fetch_company_overview(
    symbol: str,
    config: Optional[RunnableConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch comprehensive investment analysis overview for a company.

    Retrieves and formats investment-relevant data including financial health ratings,
    analyst consensus, earnings performance, and revenue segmentation.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        config: LangChain RunnableConfig (injected by @tool decorator)

    Returns:
        Tuple of (content string, artifact dict with structured data for charts)
    """
    try:
        provider = await get_financial_data_provider()
        financial = provider.financial
        user_id = _get_user_id(config)
        # Resolve once: normalize the agent-supplied spelling to the legacy form
        # provider calls use, then reuse the ref for currency, session gating, and
        # the market-status line.
        ref = resolve_ref(symbol)
        if ref is not None:
            symbol = to_legacy_api(ref)
        cur = _symbol_currency(ref)
        is_us = _is_us_clock(ref)
        if financial is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            content = f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

No financial data source configured"""
            return content, {"type": "company_overview", "symbol": symbol}

        output_lines = []

        # ═══ BASIC INFORMATION ═══
        profile_data = await financial.get_company_profile(symbol)
        if not profile_data:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            content = f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

No data found for symbol {symbol}"""
            return content, {"type": "company_overview", "symbol": symbol}

        profile = profile_data[0]
        company_name = profile.get("companyName", symbol)
        sector = profile.get("sector", "N/A")
        industry = profile.get("industry", "N/A")
        market_cap = profile.get("marketCap")
        price = profile.get("price")
        exchange = profile.get("exchangeShortName", "N/A")

        # Add file-ready header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        output_lines.append(f"## Company Overview: {symbol}")
        output_lines.append(f"**Company:** {company_name}")
        output_lines.append(f"**Retrieved:** {timestamp}")
        output_lines.append(f"**Market:** {exchange}")
        output_lines.append("")

        output_lines.append(f"Company: {company_name} ({symbol})")
        output_lines.append(f"Sector: {sector} | Industry: {industry}")
        output_lines.append(
            f"Market Cap: {fmt_money(market_cap, cur)} | Current Price: {fmt_price(price, cur)}"
            if price
            else f"Market Cap: {fmt_money(market_cap, cur)}"
        )
        output_lines.append("")

        # === PARALLEL DATA FETCH ===
        # Fetch all data in parallel for performance optimization
        # Build optional intel/snapshot calls
        async def _fetch_snapshot():
            """Fetch ginlix-data snapshot for real-time extended-hours data."""
            try:
                mdp = await get_market_data_provider()
                snaps = await mdp.get_snapshots([symbol], asset_type="stocks", user_id=user_id)
                return snaps[0] if snaps else None
            except Exception:
                return None

        async def _fetch_float():
            if provider.intel is None:
                return None
            return await provider.intel.get_float_shares(symbol, user_id=user_id)

        async def _fetch_short_interest():
            if provider.intel is None:
                return None
            result = await provider.intel.get_short_interest(
                symbol, limit=1, sort="settlement_date.desc", user_id=user_id,
            )
            return result[0] if result else None

        async def _fetch_short_volume():
            if provider.intel is None:
                return None
            result = await provider.intel.get_short_volume(
                symbol, limit=1, sort="date.desc", user_id=user_id,
            )
            return result[0] if result else None

        (
            income_stmt_result,
            earnings_calendar_result,
            price_change_result,
            key_metrics_result,
            ratios_result,
            filings_10q_result,
            filings_10k_result,
            price_target_consensus_result,
            grades_summary_result,
            stock_grades_result,
            price_target_summary_result,
            product_data_result,
            geo_data_result,
            quote_result,
            cash_flow_result,
            snapshot_result,
            float_result,
            short_interest_result,
            short_volume_result,
        ) = await asyncio.gather(
            financial.get_income_statements(symbol, period="quarter", limit=8),
            financial.get_earnings_history(symbol, limit=10),
            financial.get_price_performance(symbol),
            financial.get_key_metrics(symbol),
            financial.get_financial_ratios(symbol),
            _fmp_request("get_sec_filings", symbol, filing_type="10-Q", limit=3),
            _fmp_request("get_sec_filings", symbol, filing_type="10-K", limit=2),
            financial.get_analyst_price_targets(symbol),
            financial.get_analyst_ratings(symbol),
            _fmp_request("get_stock_grades", symbol, limit=10),
            _fmp_request("get_price_target_summary", symbol),
            financial.get_revenue_by_segment(
                symbol, segment_type="product", period="quarter", structure="flat"
            ),
            financial.get_revenue_by_segment(
                symbol, segment_type="geography", period="quarter", structure="flat"
            ),
            financial.get_realtime_quote(symbol),
            financial.get_cash_flows(symbol, period="quarter", limit=8),
            _fetch_snapshot(),
            _fetch_float(),
            _fetch_short_interest(),
            _fetch_short_volume(),
            return_exceptions=True,
        )

        # Extract safe results
        income_stmt = _safe_result(income_stmt_result, [])
        earnings_calendar = _safe_result(earnings_calendar_result, [])
        price_change_data = _safe_result(price_change_result, [])
        key_metrics_data = _safe_result(key_metrics_result, [])
        ratios_data = _safe_result(ratios_result, [])
        filings_10q = _safe_result(filings_10q_result, [])
        filings_10k = _safe_result(filings_10k_result, [])
        price_target_consensus = _safe_result(price_target_consensus_result, [])
        grades_summary_data = _safe_result(grades_summary_result, [])
        recent_grades = _safe_result(stock_grades_result, [])
        price_target_summary = _safe_result(price_target_summary_result, [])
        product_data = _safe_result(product_data_result, [])
        geo_data = _safe_result(geo_data_result, [])
        quote_data = _safe_result(quote_result, [])
        cash_flow_data = _safe_result(cash_flow_result, [])
        snapshot_data = _safe_result(snapshot_result, None)
        float_data = _safe_result(float_result, None)
        short_interest_data = _safe_result(short_interest_result, None)
        short_volume_data = _safe_result(short_volume_result, None)

        # Build fiscal_period_lookup using helper function
        fiscal_period_lookup = _build_fiscal_period_lookup(income_stmt)

        # === REAL-TIME QUOTE ===
        # Prefer ginlix-data snapshot (has extended-hours breakdown), fall back to FMP quote
        _has_snapshot = snapshot_data is not None and snapshot_data.get("price") is not None
        _has_fmp_quote = quote_data and len(quote_data) > 0

        if _has_snapshot or _has_fmp_quote:
            session_name, current_time_et = get_market_session()
            output_lines.append("### Real-Time Quote")

            if _has_snapshot:
                snap = snapshot_data
                # Map ginlix-data market_status to display label
                _STATUS_LABELS = {
                    "early_trading": "Pre-Market",
                    "open": "Regular Hours",
                    "late_trading": "After-Hours",
                    "closed": "Market Closed",
                }
                market_status_raw = snap.get("market_status", "")
                market_label = _STATUS_LABELS.get(market_status_raw, session_name.replace("_", " ").title())
                # US: snapshot/session label + ET clock. Non-US: phase from the
                # exchange calendar + exchange-local clock (the US-Eastern phase is
                # meaningless for a foreign listing; snapshot.market_status is US-centric).
                status_line = _market_status_line(ref, is_us, market_label, current_time_et)
                if status_line:
                    output_lines.append(status_line)
                output_lines.append("")

                prev_close = snap.get("previous_close")
                reg_close = snap.get("price")  # session.close = regular session close
                last_price = snap.get("last_trade_price")  # actual current price

                # Regular session close with change from previous close
                reg_change = snap.get("regular_trading_change")
                reg_change_pct = snap.get("regular_trading_change_percent")

                if reg_close is not None:
                    if reg_change is not None and reg_change_pct is not None:
                        sign = "+" if reg_change >= 0 else ""
                        output_lines.append(
                            f"**Regular Close:** {fmt_price(reg_close, cur)} ({sign}{reg_change:.2f} / {sign}{reg_change_pct:.3f}%)"
                        )
                    else:
                        output_lines.append(f"**Regular Close:** {fmt_price(reg_close, cur)}")

                # Extended-hours current price (if different from regular close)
                is_extended = market_status_raw in ("early_trading", "late_trading")
                if is_extended and last_price is not None and reg_close is not None and last_price != reg_close:
                    ext_label = "Pre-Market" if market_status_raw == "early_trading" else "After-Hours"
                    if market_status_raw == "early_trading":
                        ext_change = snap.get("early_trading_change")
                        ext_change_pct = snap.get("early_trading_change_percent")
                    else:
                        ext_change = snap.get("late_trading_change")
                        ext_change_pct = snap.get("late_trading_change_percent")

                    if ext_change is not None and ext_change_pct is not None:
                        ext_sign = "+" if ext_change >= 0 else ""
                        output_lines.append(
                            f"**{ext_label} Price:** {fmt_price(last_price, cur)} ({ext_sign}{ext_change:.2f} / {ext_sign}{ext_change_pct:.3f}% from close)"
                        )
                    else:
                        # Compute from regular close
                        diff = last_price - reg_close
                        diff_pct = (diff / reg_close * 100) if reg_close else 0
                        diff_sign = "+" if diff >= 0 else ""
                        output_lines.append(
                            f"**{ext_label} Price:** {fmt_price(last_price, cur)} ({diff_sign}{diff:.2f} / {diff_sign}{diff_pct:.2f}% from close)"
                        )

                # Total day change (from previous close)
                total_change = snap.get("change")
                total_change_pct = snap.get("change_percent")
                if total_change is not None and total_change_pct is not None:
                    t_sign = "+" if total_change >= 0 else ""
                    output_lines.append(
                        f"**Day Change (from prev close):** {t_sign}{total_change:.2f} / {t_sign}{total_change_pct:.3f}%"
                    )

                output_lines.append("")

                # Build quote detail table from snapshot + FMP (FMP has 52-week range)
                quote_rows = []
                if snap.get("open"):
                    quote_rows.append(("Open", fmt_price(snap["open"], cur)))
                if prev_close:
                    quote_rows.append(("Previous Close", fmt_price(prev_close, cur)))
                if snap.get("low") and snap.get("high"):
                    quote_rows.append(
                        ("Day Range", f"{fmt_price(snap['low'], cur)} - {fmt_price(snap['high'], cur)}")
                    )
                # 52-week range from FMP quote
                fmp_quote = quote_data[0] if _has_fmp_quote else {}
                year_low = fmp_quote.get("yearLow")
                year_high = fmp_quote.get("yearHigh")
                if year_low and year_high:
                    quote_rows.append(
                        ("52-Week Range", f"{fmt_price(year_low, cur)} - {fmt_price(year_high, cur)}")
                    )
                if snap.get("volume"):
                    vol_str = fmt_count(snap["volume"])
                    avg_volume = fmp_quote.get("avgVolume") if _has_fmp_quote else None
                    if avg_volume:
                        avg_str = fmt_count(avg_volume)
                        quote_rows.append(("Volume", f"{vol_str} (Avg: {avg_str})"))
                    else:
                        quote_rows.append(("Volume", vol_str))

                if quote_rows:
                    output_lines.append("| Metric | Value |")
                    output_lines.append("|--------|-------|")
                    for metric, value in quote_rows:
                        output_lines.append(f"| {metric} | {value} |")
                    output_lines.append("")

            else:
                # FMP-only fallback (no extended-hours breakdown available)
                quote = quote_data[0]
                session_str = session_name.replace("_", " ").title()
                status_line = _market_status_line(ref, is_us, session_str, current_time_et)
                if status_line:
                    output_lines.append(status_line)
                output_lines.append("")

                q_price = quote.get("price", 0)
                q_change = quote.get("change", 0)
                q_change_pct = quote.get("changePercentage", 0)
                change_sign = "+" if q_change >= 0 else ""
                output_lines.append(
                    f"**Price:** {fmt_price(q_price, cur)} ({change_sign}{q_change:.2f} / {change_sign}{q_change_pct:.2f}%)"
                )
                output_lines.append("")

                quote_rows = []
                open_price = quote.get("open")
                day_low = quote.get("dayLow")
                day_high = quote.get("dayHigh")
                year_low = quote.get("yearLow")
                year_high = quote.get("yearHigh")
                volume = quote.get("volume")
                avg_volume = quote.get("avgVolume")
                previous_close = quote.get("previousClose")

                if open_price:
                    quote_rows.append(("Open", fmt_price(open_price, cur)))
                if previous_close:
                    quote_rows.append(("Previous Close", fmt_price(previous_close, cur)))
                if day_low and day_high:
                    quote_rows.append(
                        ("Day Range", f"{fmt_price(day_low, cur)} - {fmt_price(day_high, cur)}")
                    )
                if year_low and year_high:
                    quote_rows.append(
                        ("52-Week Range", f"{fmt_price(year_low, cur)} - {fmt_price(year_high, cur)}")
                    )
                if volume:
                    vol_str = fmt_count(volume)
                    if avg_volume:
                        avg_str = fmt_count(avg_volume)
                        quote_rows.append(("Volume", f"{vol_str} (Avg: {avg_str})"))
                    else:
                        quote_rows.append(("Volume", vol_str))

                if quote_rows:
                    output_lines.append("| Metric | Value |")
                    output_lines.append("|--------|-------|")
                    for metric, value in quote_rows:
                        output_lines.append(f"| {metric} | {value} |")
                    output_lines.append("")

        # === FLOAT & SHORT DATA ===
        _has_float = float_data is not None and isinstance(float_data, dict) and float_data.get("free_float") is not None
        _has_si = short_interest_data is not None and isinstance(short_interest_data, dict) and short_interest_data.get("short_interest") is not None
        _has_sv = short_volume_data is not None and isinstance(short_volume_data, dict) and short_volume_data.get("short_volume_ratio") is not None

        if _has_float or _has_si or _has_sv:
            output_lines.append("### Share Structure")
            output_lines.append("")

            struct_rows = []
            if _has_float:
                free_float = float_data.get("free_float")
                if free_float:
                    struct_rows.append(("Float", fmt_count(free_float)))
                ff_pct = float_data.get("free_float_percent")
                if ff_pct is not None:
                    struct_rows.append(("Float %", f"{ff_pct:.1f}%"))

            if _has_si:
                si_val = short_interest_data["short_interest"]
                si_date = short_interest_data.get("settlement_date", "")
                struct_rows.append(("Short Interest", f"{si_val:,} (as of {si_date})" if si_date else f"{si_val:,}"))
                if _has_float and float_data.get("free_float"):
                    si_pct = si_val / float_data["free_float"] * 100
                    struct_rows.append(("Short % of Float", f"{si_pct:.2f}%"))
                dtc = short_interest_data.get("days_to_cover")
                if dtc:
                    struct_rows.append(("Days to Cover", f"{dtc:.2f}"))

            if _has_sv:
                sv_ratio = short_volume_data["short_volume_ratio"]
                sv_date = short_volume_data.get("date", "")
                struct_rows.append(("Short Volume Ratio", f"{sv_ratio:.1f}% (as of {sv_date})" if sv_date else f"{sv_ratio:.1f}%"))

            if struct_rows:
                output_lines.append("| Metric | Value |")
                output_lines.append("|--------|-------|")
                for metric, value in struct_rows:
                    output_lines.append(f"| {metric} | {value} |")
                output_lines.append("")

        # === STOCK PRICE PERFORMANCE ===
        if price_change_data:
            changes = price_change_data[0]

            output_lines.append("### Stock Price Performance")
            output_lines.append("")

            # Build performance table
            performance_rows = []

            # Short-term (up to 1 month)
            if changes.get("1D") is not None:
                performance_rows.append(("1 Day", format_percentage(changes.get("1D"))))
            if changes.get("5D") is not None:
                performance_rows.append(
                    ("5 Days", format_percentage(changes.get("5D")))
                )
            if changes.get("1M") is not None:
                performance_rows.append(
                    ("1 Month", format_percentage(changes.get("1M")))
                )

            # Medium-term (3-6 months)
            if changes.get("3M") is not None:
                performance_rows.append(
                    ("3 Months", format_percentage(changes.get("3M")))
                )
            if changes.get("6M") is not None:
                performance_rows.append(
                    ("6 Months", format_percentage(changes.get("6M")))
                )
            if changes.get("ytd") is not None:
                performance_rows.append(("YTD", format_percentage(changes.get("ytd"))))

            # Long-term (1+ years)
            if changes.get("1Y") is not None:
                performance_rows.append(
                    ("1 Year", format_percentage(changes.get("1Y")))
                )
            if changes.get("3Y") is not None:
                performance_rows.append(
                    ("3 Years", format_percentage(changes.get("3Y")))
                )
            if changes.get("5Y") is not None:
                performance_rows.append(
                    ("5 Years", format_percentage(changes.get("5Y")))
                )

            if performance_rows:
                output_lines.append("| Period | Performance |")
                output_lines.append("|--------|-------------|")
                for period, perf in performance_rows:
                    output_lines.append(f"| {period} | {perf} |")
                output_lines.append("")

        # === KEY FINANCIAL METRICS ===
        if key_metrics_data:
            metrics = key_metrics_data[0]
            ratios = ratios_data[0] if ratios_data else {}

            output_lines.append("### Key Financial Metrics (TTM)")
            output_lines.append("*Data based on Trailing Twelve Months*")
            output_lines.append("")

            # Collect all metrics for table
            metrics_rows = []

            # Valuation Ratios
            pe_ratio = metrics.get("peRatioTTM") or profile.get("pe")
            pb_ratio = metrics.get("pbRatioTTM")
            peg_ratio = metrics.get("pegRatioTTM")
            ev_to_ebitda = metrics.get("evToOperatingCashFlowTTM")

            if pe_ratio:
                metrics_rows.append(("P/E Ratio", f"{pe_ratio:.2f}x"))
            if pb_ratio:
                metrics_rows.append(("P/B Ratio", f"{pb_ratio:.2f}x"))
            if peg_ratio:
                metrics_rows.append(("PEG Ratio", f"{peg_ratio:.2f}"))
            if ev_to_ebitda:
                metrics_rows.append(("EV/OCF", f"{ev_to_ebitda:.2f}x"))

            # Profitability Metrics
            roe = metrics.get("roeTTM") or ratios.get("returnOnEquityTTM")
            roa = metrics.get("roaTTM") or ratios.get("returnOnAssetsTTM")
            net_margin = ratios.get("netProfitMarginTTM")
            operating_margin = ratios.get("operatingProfitMarginTTM")

            if roe:
                roe_val = f"{roe * 100:.2f}%" if roe < 1 else f"{roe:.2f}%"
                metrics_rows.append(("ROE (Return on Equity)", roe_val))
            if roa:
                roa_val = f"{roa * 100:.2f}%" if roa < 1 else f"{roa:.2f}%"
                metrics_rows.append(("ROA (Return on Assets)", roa_val))
            if net_margin:
                nm_val = (
                    f"{net_margin * 100:.2f}%"
                    if net_margin < 1
                    else f"{net_margin:.2f}%"
                )
                metrics_rows.append(("Net Profit Margin", nm_val))
            if operating_margin:
                om_val = (
                    f"{operating_margin * 100:.2f}%"
                    if operating_margin < 1
                    else f"{operating_margin:.2f}%"
                )
                metrics_rows.append(("Operating Margin", om_val))

            # Leverage & Liquidity
            debt_to_equity = ratios.get("debtEquityRatioTTM")
            current_ratio = ratios.get("currentRatioTTM")
            quick_ratio = ratios.get("quickRatioTTM")
            interest_coverage = ratios.get("interestCoverageTTM")

            if debt_to_equity:
                metrics_rows.append(("Debt/Equity Ratio", f"{debt_to_equity:.2f}"))
            if current_ratio:
                metrics_rows.append(("Current Ratio", f"{current_ratio:.2f}"))
            if quick_ratio:
                metrics_rows.append(("Quick Ratio", f"{quick_ratio:.2f}"))
            if interest_coverage:
                metrics_rows.append(("Interest Coverage", f"{interest_coverage:.2f}x"))

            # Output as markdown table
            if metrics_rows:
                output_lines.append("| Metric | Value |")
                output_lines.append("|--------|-------|")
                for metric, value in metrics_rows:
                    output_lines.append(f"| {metric} | {value} |")
            else:
                output_lines.append("*No financial metrics available*")

            output_lines.append("")

        # === SEC FILING DATES ===
        has_filing_data = bool(filings_10q or filings_10k)

        if has_filing_data:
            output_lines.append("### SEC Filing Dates")
            output_lines.append("")

            output_lines.append("| Filing Type | Filing Date | Fiscal Period |")
            output_lines.append("|-------------|-------------|---------------|")

            # Show latest 10-K (annual report that includes Q4)
            if filings_10k:
                for filing in filings_10k[:1]:  # Just the latest
                    filing_date = filing.get("filingDate", "N/A")
                    if filing_date and " " in filing_date:
                        filing_date = filing_date.split(" ")[0]  # Remove time part

                    # For 10-K, find Q4 fiscal period (10-K includes Q4)
                    fiscal_period = "Annual"
                    if fiscal_period_lookup:
                        # Find Q4 entries to determine fiscal year
                        for date_key, period_name in sorted(
                            fiscal_period_lookup.items(), reverse=True
                        ):
                            if period_name.startswith("Q4"):
                                # Extract FY from "Q4 FY2025" and show as "Q4 FY2025 (Annual)"
                                fiscal_period = f"{period_name} (Annual)"
                                break

                    output_lines.append(
                        f"| **10-K** | {filing_date} | {fiscal_period} |"
                    )

            # Show latest 10-Q filings
            if filings_10q:
                for filing in filings_10q[:3]:  # Last 3 quarterly reports
                    filing_date = filing.get("filingDate", "N/A")
                    if filing_date and " " in filing_date:
                        filing_date = filing_date.split(" ")[0]

                    # Match filing to fiscal period using helper
                    fiscal_period = _match_filing_to_fiscal_period(
                        filing_date, earnings_calendar, fiscal_period_lookup
                    )
                    output_lines.append(
                        f"| **10-Q** (Quarterly) | {filing_date} | {fiscal_period} |"
                    )

            output_lines.append("")

            # Add tip for US stocks about get_sec_filing tool
            # US stocks don't have exchange suffix (.SS, .SZ, .HK, etc.)
            is_us_stock = "." not in symbol or symbol.endswith(".US")
            if is_us_stock:
                output_lines.append(
                    "*Tip: Use `get_sec_filing` tool to fetch complete earnings call transcripts and SEC filings.*"
                )
                output_lines.append("")

        # === NEXT EARNINGS REPORT ===
        if earnings_calendar:
            # Find upcoming reports (epsActual is None) and pick the earliest one
            upcoming_reports = [
                cal
                for cal in earnings_calendar
                if cal.get("epsActual") is None and cal.get("date")
            ]

            if upcoming_reports:
                upcoming_reports.sort(key=lambda x: x.get("date", "9999-99-99"))
                next_report = upcoming_reports[0]

                output_lines.append("### Next Earnings Report")
                output_lines.append("")

                report_date = next_report.get("date", "N/A")
                fiscal_ending = next_report.get("fiscalDateEnding", "N/A")
                time_slot = next_report.get("time", "")
                eps_estimate = next_report.get("epsEstimated")
                rev_estimate = next_report.get("revenueEstimated")

                # Determine fiscal period name (lookup first, then infer)
                fiscal_period_name = fiscal_period_lookup.get(fiscal_ending)
                if not fiscal_period_name and fiscal_ending != "N/A":
                    fiscal_period_name = _infer_fiscal_period(
                        fiscal_ending, fiscal_period_lookup
                    )
                fiscal_period_name = fiscal_period_name or "N/A"

                # Format time slot
                time_desc = {
                    "amc": " (After Market Close)",
                    "bmo": " (Before Market Open)",
                }.get(time_slot, "")

                output_lines.append(f"**Report Date:** {report_date}{time_desc}")
                output_lines.append(f"**Fiscal Period:** {fiscal_period_name}")
                output_lines.append(f"**Fiscal Period End:** {fiscal_ending}")

                if eps_estimate is not None:
                    output_lines.append(f"**EPS Estimate:** {fmt_price(eps_estimate, cur)}")
                if rev_estimate is not None:
                    output_lines.append(
                        f"**Revenue Estimate:** {format_number(rev_estimate)}"
                    )

                output_lines.append("")

        # === EARNINGS PERFORMANCE ===
        # Filter to get reported quarters only (epsActual is not None means already reported)
        reported_earnings = [e for e in earnings_calendar if e.get("epsActual") is not None]

        if reported_earnings:
            output_lines.append("### Earnings Performance")
            output_lines.append("")

            # Show latest quarter in detail
            latest = reported_earnings[0]
            announce_date = latest.get("date", "N/A")
            fiscal_ending = latest.get("fiscalDateEnding")
            eps_actual = latest.get("epsActual")
            eps_estimate = latest.get("epsEstimated")
            revenue_actual = latest.get("revenueActual")
            revenue_estimate = latest.get("revenueEstimated")

            # Get fiscal period label
            fiscal_label = (
                fiscal_period_lookup.get(fiscal_ending, "") if fiscal_ending else ""
            )
            latest_label = (
                f"{announce_date} ({fiscal_label})" if fiscal_label else announce_date
            )

            output_lines.append(f"**Latest Quarter ({latest_label}):**")
            output_lines.append("")

            # EPS data
            if eps_actual is not None:
                if eps_estimate and eps_estimate != 0:
                    eps_surprise = (
                        (eps_actual - eps_estimate) / abs(eps_estimate)
                    ) * 100
                    output_lines.append(
                        f"- **EPS:** {fmt_price(eps_actual, cur)} actual vs {fmt_price(eps_estimate, cur)} estimate ({format_percentage(eps_surprise)} surprise)"
                    )
                else:
                    output_lines.append(
                        f"- **EPS:** {fmt_price(eps_actual, cur)} (no estimate available)"
                    )

            # Revenue data
            if revenue_actual is not None:
                if revenue_estimate and revenue_estimate != 0:
                    rev_surprise = (
                        (revenue_actual - revenue_estimate) / abs(revenue_estimate)
                    ) * 100
                    output_lines.append(
                        f"- **Revenue:** {format_number(revenue_actual)} actual vs {format_number(revenue_estimate)} estimate ({format_percentage(rev_surprise)} surprise)"
                    )
                else:
                    output_lines.append(
                        f"- **Revenue:** {format_number(revenue_actual)} (no estimate available)"
                    )

            # Show earnings trend for last 4 quarters with fiscal period column
            if len(reported_earnings) > 1:
                output_lines.append("")
                output_lines.append("**Recent Earnings Trend:**")
                output_lines.append("")
                output_lines.append("| Date | Fiscal Period | EPS | Revenue |")
                output_lines.append("|------|---------------|-----|---------|")

                for quarter in reported_earnings[:4]:
                    q_date = quarter.get("date", "N/A")
                    q_fiscal_ending = quarter.get("fiscalDateEnding")
                    q_eps = quarter.get("epsActual")
                    q_revenue = quarter.get("revenueActual")

                    # Get fiscal period label
                    q_fiscal_label = (
                        fiscal_period_lookup.get(q_fiscal_ending, "N/A")
                        if q_fiscal_ending
                        else "N/A"
                    )
                    eps_str = fmt_price(q_eps, cur)
                    revenue_str = format_number(q_revenue)
                    output_lines.append(
                        f"| {q_date} | {q_fiscal_label} | {eps_str} | {revenue_str} |"
                    )

            output_lines.append("")

        # === CASH FLOW (QUARTERLY) ===
        if cash_flow_data:
            output_lines.append("### Cash Flow (Quarterly)")
            output_lines.append("")
            output_lines.append("| Period | Operating CF | CapEx | Free CF |")
            output_lines.append("|--------|-------------|-------|---------|")

            for cf in cash_flow_data[:8]:
                cf_date = cf.get("date", "N/A")
                cf_label = fiscal_period_lookup.get(cf_date, cf_date)
                op_cf = cf.get("operatingCashFlow")
                capex = cf.get("capitalExpenditure")
                fcf = cf.get("freeCashFlow")
                op_cf_str = format_number(op_cf)
                capex_str = format_number(capex)
                fcf_str = format_number(fcf)
                output_lines.append(
                    f"| {cf_label} | {op_cf_str} | {capex_str} | {fcf_str} |"
                )

            output_lines.append("")

        # === ANALYST CONSENSUS & RATINGS ===
        output_lines.append("### Analyst Consensus & Ratings")
        output_lines.append("")

        # Price Targets Section
        if price_target_consensus:
            pt = price_target_consensus[0]
            median = pt.get("targetMedian")
            low = pt.get("targetLow")
            high = pt.get("targetHigh")
            consensus = pt.get("targetConsensus")

            output_lines.append("**Price Targets:**")
            output_lines.append("")
            pt_rows = []
            if median and price:
                upside = ((median - price) / price * 100) if price else 0
                upside_sign = "+" if upside >= 0 else ""
                pt_rows.append(
                    (
                        "Consensus Target",
                        f"{fmt_price(median, cur)} ({upside_sign}{upside:.1f}% from current)",
                    )
                )
            if low and high:
                pt_rows.append(
                    ("Target Range", f"{fmt_price(low, cur)} - {fmt_price(high, cur)}")
                )
            if consensus:
                pt_rows.append(("Analyst Consensus", str(consensus)))

            if pt_rows:
                for label, value in pt_rows:
                    output_lines.append(f"- **{label}:** {value}")
                output_lines.append("")

        # Rating Distribution
        if grades_summary_data:
            gs = grades_summary_data[0]
            strong_buy = gs.get("strongBuy", 0)
            buy = gs.get("buy", 0)
            hold = gs.get("hold", 0)
            sell = gs.get("sell", 0)
            strong_sell = gs.get("strongSell", 0)
            consensus = gs.get("consensus", "N/A")

            total_ratings = strong_buy + buy + hold + sell + strong_sell
            if total_ratings > 0:
                output_lines.append("**Rating Distribution:**")
                output_lines.append("")
                output_lines.append("| Rating | Count | Percentage |")
                output_lines.append("|--------|-------|------------|")

                if strong_buy > 0:
                    pct = strong_buy / total_ratings * 100
                    output_lines.append(f"| Strong Buy | {strong_buy} | {pct:.1f}% |")
                if buy > 0:
                    pct = buy / total_ratings * 100
                    output_lines.append(f"| Buy | {buy} | {pct:.1f}% |")
                if hold > 0:
                    pct = hold / total_ratings * 100
                    output_lines.append(f"| Hold | {hold} | {pct:.1f}% |")
                if sell > 0:
                    pct = sell / total_ratings * 100
                    output_lines.append(f"| Sell | {sell} | {pct:.1f}% |")
                if strong_sell > 0:
                    pct = strong_sell / total_ratings * 100
                    output_lines.append(f"| Strong Sell | {strong_sell} | {pct:.1f}% |")

                output_lines.append("")
                output_lines.append(f"**Overall Consensus:** {consensus.upper()}")
                output_lines.append("")

        # Recent Analyst Actions
        if recent_grades:
            output_lines.append("**Recent Analyst Actions:**")
            output_lines.append("")
            output_lines.append("| Date | Firm | Action |")
            output_lines.append("|------|------|--------|")

            for grade in recent_grades[:5]:  # Show top 5 recent actions
                company = grade.get("gradingCompany", "N/A")
                new_grade = grade.get("newGrade", "N/A")
                previous_grade = grade.get("previousGrade", "")
                action = grade.get("action", "N/A")
                date = grade.get("date", "N/A")

                # Format action string
                if previous_grade and previous_grade != new_grade:
                    action_str = f"{action} to {new_grade} (from {previous_grade})"
                else:
                    action_str = f"{action} {new_grade}"

                output_lines.append(f"| {date} | {company} | {action_str} |")

            output_lines.append("")

        # Top Analyst Firms (from price target summary)
        if price_target_summary:
            output_lines.append("**Top Analyst Firms:**")
            output_lines.append("")
            output_lines.append("| Firm | Analyst | Price Target |")
            output_lines.append("|------|---------|--------------|")

            for firm_target in price_target_summary[:5]:
                analyst_company = firm_target.get("analystCompany", "N/A")
                target_price = firm_target.get("adjPriceTarget")
                analyst_name = firm_target.get("analystName", "-")

                target_str = fmt_price(target_price, cur) if target_price else "N/A"
                output_lines.append(
                    f"| {analyst_company} | {analyst_name} | {target_str} |"
                )

            output_lines.append("")

        # === REVENUE BREAKDOWN ===
        has_product_data = False
        has_geo_data = False

        # Check if we have any data
        if product_data and len(product_data) > 0:
            latest_product_record = product_data[0]
            # Extract date and nested data (structure: {"2024-09-28": {"Mac": 123, ...}})
            if latest_product_record and isinstance(latest_product_record, dict):
                fiscal_date = list(latest_product_record.keys())[0]
                product_revenues = latest_product_record[fiscal_date]
                if (
                    product_revenues
                    and isinstance(product_revenues, dict)
                    and len(product_revenues) > 0
                ):
                    has_product_data = True

        if geo_data and len(geo_data) > 0:
            latest_geo_record = geo_data[0]
            # Extract date and nested data
            if latest_geo_record and isinstance(latest_geo_record, dict):
                geo_date = list(latest_geo_record.keys())[0]
                geo_revenues = latest_geo_record[geo_date]
                if (
                    geo_revenues
                    and isinstance(geo_revenues, dict)
                    and len(geo_revenues) > 0
                ):
                    has_geo_data = True

        # Only show section if we have data
        if has_product_data or has_geo_data:
            output_lines.append("### Revenue Breakdown (Latest Quarter)")
            output_lines.append("")

        # Product breakdown
        if has_product_data:
            latest_product_record = product_data[0]
            fiscal_date = list(latest_product_record.keys())[0]
            product_revenues = latest_product_record[fiscal_date]

            # Get fiscal period name from lookup
            period_label = fiscal_period_lookup.get(
                fiscal_date, f"Period ending {fiscal_date}"
            )
            output_lines.append(f"**By Product ({period_label}):**")
            output_lines.append(f"*Report Date: {fiscal_date}*")
            output_lines.append("")

            total_revenue = sum(product_revenues.values())

            # Sort by revenue (descending) and show top items
            sorted_products = sorted(
                product_revenues.items(), key=lambda x: x[1], reverse=True
            )
            output_lines.append("| Product | Revenue | Percentage |")
            output_lines.append("|---------|---------|------------|")
            for product, revenue in sorted_products[:5]:  # Top 5 products
                percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                output_lines.append(
                    f"| {product} | {format_number(revenue)} | {percentage:.1f}% |"
                )

            output_lines.append("")

        # Geographic breakdown
        if has_geo_data:
            latest_geo_record = geo_data[0]
            geo_date = list(latest_geo_record.keys())[0]
            geo_revenues = latest_geo_record[geo_date]

            # Get fiscal period name from lookup
            period_label = fiscal_period_lookup.get(
                geo_date, f"Period ending {geo_date}"
            )
            output_lines.append(f"**By Region ({period_label}):**")
            output_lines.append(f"*Report Date: {geo_date}*")
            output_lines.append("")

            total_revenue = sum(geo_revenues.values())

            # Sort by revenue (descending)
            sorted_regions = sorted(
                geo_revenues.items(), key=lambda x: x[1], reverse=True
            )
            output_lines.append("| Region | Revenue | Percentage |")
            output_lines.append("|--------|---------|------------|")
            for region, revenue in sorted_regions:
                percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                output_lines.append(
                    f"| {region} | {format_number(revenue)} | {percentage:.1f}% |"
                )

            output_lines.append("")

        # Prefix a live freshness stamp when a real-time snapshot is available and
        # the market is open. Best-effort: a malformed snapshot must never blow
        # away the already-assembled overview.
        stamp = None
        if snapshot_data:
            try:
                stamp = build_live_stamp([snapshot_data])
            except Exception:
                stamp = None
        if stamp:
            output_lines.insert(0, stamp + "\n")

        result = "\n".join(output_lines)
        logger.debug(f"Retrieved comprehensive investment overview for {symbol}")

        # Build artifact with structured data for frontend charts
        artifact: Dict[str, Any] = {
            "type": "company_overview",
            "symbol": symbol,
            "name": company_name,
        }

        # Quote data for artifact — prefer snapshot for extended-hours detail
        if _has_snapshot:
            snap = snapshot_data
            fmp_quote = quote_data[0] if _has_fmp_quote else {}
            artifact["quote"] = {
                "regularClose": snap.get("price"),
                "lastTradePrice": snap.get("last_trade_price"),
                "marketStatus": snap.get("market_status"),
                "change": snap.get("change"),
                "changePct": snap.get("change_percent"),
                "regularChange": snap.get("regular_trading_change"),
                "regularChangePct": snap.get("regular_trading_change_percent"),
                "earlyTradingChangePct": snap.get("early_trading_change_percent"),
                "lateTradingChangePct": snap.get("late_trading_change_percent"),
                "dayHigh": snap.get("high"),
                "dayLow": snap.get("low"),
                "yearHigh": fmp_quote.get("yearHigh"),
                "yearLow": fmp_quote.get("yearLow"),
                "open": snap.get("open"),
                "previousClose": snap.get("previous_close"),
                "volume": snap.get("volume"),
                "avgVolume": fmp_quote.get("avgVolume"),
                "marketCap": fmp_quote.get("marketCap"),
            }
        elif _has_fmp_quote:
            quote = quote_data[0]
            artifact["quote"] = {
                "price": quote.get("price"),
                "change": quote.get("change"),
                "changePct": quote.get("changePercentage"),
                "dayHigh": quote.get("dayHigh"),
                "dayLow": quote.get("dayLow"),
                "yearHigh": quote.get("yearHigh"),
                "yearLow": quote.get("yearLow"),
                "open": quote.get("open"),
                "previousClose": quote.get("previousClose"),
                "volume": quote.get("volume"),
                "avgVolume": quote.get("avgVolume"),
                "marketCap": quote.get("marketCap"),
            }

        # Float & short data for artifact (single latest records, not full arrays)
        if _has_float:
            artifact["float"] = float_data
        if _has_si:
            artifact["shortInterest"] = short_interest_data
        if _has_sv:
            artifact["shortVolume"] = short_volume_data

        # Performance data
        if price_change_data:
            changes = price_change_data[0]
            artifact["performance"] = {
                k: changes.get(k)
                for k in ["1D", "5D", "1M", "3M", "6M", "ytd", "1Y", "3Y", "5Y"]
                if changes.get(k) is not None
            }

        # Analyst ratings
        if grades_summary_data:
            gs = grades_summary_data[0]
            artifact["analystRatings"] = {
                "strongBuy": gs.get("strongBuy", 0),
                "buy": gs.get("buy", 0),
                "hold": gs.get("hold", 0),
                "sell": gs.get("sell", 0),
                "strongSell": gs.get("strongSell", 0),
                "consensus": gs.get("consensus", "N/A"),
            }

        # Revenue by product
        if has_product_data:
            latest_product_record = product_data[0]
            fiscal_date = list(latest_product_record.keys())[0]
            artifact["revenueByProduct"] = latest_product_record[fiscal_date]

        # Revenue by geography
        if has_geo_data:
            latest_geo_record = geo_data[0]
            geo_date = list(latest_geo_record.keys())[0]
            artifact["revenueByGeo"] = latest_geo_record[geo_date]

        # Quarterly fundamentals from income statement (oldest-first for charting)
        if income_stmt:
            artifact["quarterlyFundamentals"] = [
                {
                    "period": fiscal_period_lookup.get(stmt.get("date"), stmt.get("date", "")),
                    "date": stmt.get("date"),
                    "revenue": stmt.get("revenue"),
                    "netIncome": stmt.get("netIncome"),
                    "grossProfit": stmt.get("grossProfit"),
                    "operatingIncome": stmt.get("operatingIncome"),
                    "ebitda": stmt.get("ebitda"),
                    "epsDiluted": stmt.get("epsdiluted"),
                    "grossMargin": _margin(stmt, "grossProfitRatio", "grossProfit"),
                    "operatingMargin": _margin(stmt, "operatingIncomeRatio", "operatingIncome"),
                    "netMargin": _margin(stmt, "netIncomeRatio", "netIncome"),
                }
                for stmt in reversed(income_stmt)
            ]

        # Earnings surprises from earnings calendar (reported only, oldest-first)
        reported_for_artifact = [
            e for e in earnings_calendar if e.get("epsActual") is not None
        ]
        if reported_for_artifact:
            artifact["earningsSurprises"] = [
                {
                    "period": fiscal_period_lookup.get(
                        e.get("fiscalDateEnding"), e.get("date", "")
                    ),
                    "date": e.get("date"),
                    "epsActual": e.get("epsActual"),
                    "epsEstimate": e.get("epsEstimated"),
                    "revenueActual": e.get("revenueActual"),
                    "revenueEstimate": e.get("revenueEstimated"),
                }
                for e in reversed(reported_for_artifact)
            ]

        # Cash flow (oldest-first for charting)
        if cash_flow_data:
            artifact["cashFlow"] = [
                {
                    "period": fiscal_period_lookup.get(cf.get("date"), cf.get("date", "")),
                    "date": cf.get("date"),
                    "operatingCashFlow": cf.get("operatingCashFlow"),
                    "capitalExpenditure": cf.get("capitalExpenditure"),
                    "freeCashFlow": cf.get("freeCashFlow"),
                }
                for cf in reversed(cash_flow_data)
            ]

        return result, artifact

    except Exception as e:
        logger.error(f"Error retrieving company overview for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        content = f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving company overview: {str(e)}"""
        return content, {"type": "company_overview", "symbol": symbol, "error": str(e)}
