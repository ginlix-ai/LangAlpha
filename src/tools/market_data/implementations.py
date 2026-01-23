# pyright: ignore
"""
Core implementation logic for market data tools.

Contains business logic separated from LangChain tool decorators.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
import logging
import json

from .utils import (
    format_number,
    format_percentage,
    get_rating_label,
    get_market_session
)
from src.data_client.fmp import get_fmp_client

logger = logging.getLogger(__name__)

# Constants for fiscal period matching
FILING_DATE_TOLERANCE_DAYS = 5  # Allow 5 days difference when matching filings to earnings
DAYS_PER_QUARTER = 90  # Approximate days per fiscal quarter


def _build_fiscal_period_lookup(income_stmt: List[Dict]) -> Dict[str, str]:
    """Build a lookup dict mapping fiscal end dates to period names (e.g., 'Q3 FY2026')."""
    lookup = {}
    for stmt in income_stmt:
        stmt_date = stmt.get('date')
        period = stmt.get('period')  # Q1, Q2, Q3, Q4
        calendar_year = stmt.get('calendarYear')
        if stmt_date and period and calendar_year:
            lookup[stmt_date] = f"{period} FY{calendar_year}"
    return lookup


def _infer_fiscal_period(fiscal_ending: str, fiscal_period_lookup: Dict[str, str]) -> Optional[str]:
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
    fiscal_period_lookup: Dict[str, str]
) -> str:
    """
    Match a SEC filing date to its fiscal period using earnings calendar.
    Returns the fiscal period name or 'Quarterly' if no match found.
    """
    if not earnings_calendar or not filing_date or filing_date == 'N/A':
        return "Quarterly"

    try:
        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        best_match = None
        min_diff = float('inf')

        for cal in earnings_calendar:
            cal_date = cal.get('date')
            fiscal_ending = cal.get('fiscalDateEnding')
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


def _format_price_data_as_table(data: List[Dict[str, Any]]) -> str:
    """
    Format OHLCV price data as a markdown table.

    Args:
        data: List of daily OHLCV dictionaries (newest first)

    Returns:
        Markdown-formatted table string
    """
    if not data or len(data) == 0:
        return "No price data available."

    symbol = data[0].get('symbol', 'N/A')
    num_days = len(data)

    # Get date range
    dates = [d.get('date') for d in data if d.get('date')]
    if dates:
        sorted_dates = sorted(dates)
        start_date = sorted_dates[0]
        end_date = sorted_dates[-1]
    else:
        start_date = end_date = 'N/A'

    lines = []

    # Header
    lines.append(f"## {symbol} - Daily Prices ({num_days} Trading Days)")
    lines.append("")
    lines.append(f"**Period:** {start_date} to {end_date}")
    lines.append("")

    # Table header
    lines.append("| Date       | Open      | High      | Low       | Close     | Volume    | Change    |")
    lines.append("|------------|-----------|-----------|-----------|-----------|-----------|-----------|")

    # Table rows
    total_volume = 0
    for record in data:
        date = record.get('date', 'N/A')
        open_price = record.get('open')
        high_price = record.get('high')
        low_price = record.get('low')
        close_price = record.get('close')
        volume = record.get('volume')
        change_pct = record.get('changePercent')

        # Format prices
        open_str = f"${open_price:.2f}" if open_price is not None else 'N/A'
        high_str = f"${high_price:.2f}" if high_price is not None else 'N/A'
        low_str = f"${low_price:.2f}" if low_price is not None else 'N/A'
        close_str = f"${close_price:.2f}" if close_price is not None else 'N/A'

        # Format volume
        volume_str = format_number(volume).replace('$', '') if volume is not None else 'N/A'
        if volume is not None:
            total_volume += volume

        # Format change percentage
        if change_pct is not None:
            sign = '+' if change_pct >= 0 else ''
            change_str = f"{sign}{change_pct:.2f}%"
        else:
            change_str = 'N/A'

        lines.append(f"| {date} | {open_str:>9} | {high_str:>9} | {low_str:>9} | {close_str:>9} | {volume_str:>9} | {change_str:>9} |")

    # Summary
    lines.append("")
    total_vol_str = format_number(total_volume).replace('$', '')
    lines.append(f"**Total Volume:** {total_vol_str}")

    return "\n".join(lines)


def _format_indices_data_as_table(indices_data: Dict[str, List[Dict[str, Any]]]) -> str:
    """
    Format multiple market indices data as markdown tables.

    Args:
        indices_data: Dictionary mapping index symbol to list of price data

    Returns:
        Markdown-formatted tables string (one table per index)
    """
    if not indices_data:
        return "No index data available."

    lines = []

    # Count total days
    all_dates = set()
    for data_list in indices_data.values():
        for record in data_list:
            if record.get('date'):
                all_dates.add(record.get('date'))

    num_days = len(all_dates)
    sorted_dates = sorted(all_dates)
    start_date = sorted_dates[0] if sorted_dates else 'N/A'
    end_date = sorted_dates[-1] if sorted_dates else 'N/A'

    # Header
    lines.append(f"## Market Indices ({num_days} Trading Days)")
    lines.append("")
    lines.append(f"**Period:** {start_date} to {end_date}")
    lines.append("")

    # Create table for each index
    for i, (symbol, data) in enumerate(indices_data.items()):
        if not data:
            continue

        # Index name
        index_name = _get_index_name(symbol)
        lines.append(f"### {index_name} ({symbol})")
        lines.append("")

        # Table header
        lines.append("| Date       | Open        | High        | Low         | Close       | Volume      | Change    |")
        lines.append("|------------|-------------|-------------|-------------|-------------|-------------|-----------|")

        # Table rows
        for record in data:
            date = record.get('date', 'N/A')
            open_price = record.get('open')
            high_price = record.get('high')
            low_price = record.get('low')
            close_price = record.get('close')
            volume = record.get('volume')
            change_pct = record.get('changePercent')

            # Format prices
            open_str = f"{open_price:,.2f}" if open_price is not None else 'N/A'
            high_str = f"{high_price:,.2f}" if high_price is not None else 'N/A'
            low_str = f"{low_price:,.2f}" if low_price is not None else 'N/A'
            close_str = f"{close_price:,.2f}" if close_price is not None else 'N/A'

            # Format volume
            volume_str = format_number(volume).replace('$', '') if volume is not None else 'N/A'

            # Format change percentage
            if change_pct is not None:
                sign = '+' if change_pct >= 0 else ''
                change_str = f"{sign}{change_pct:.2f}%"
            else:
                change_str = 'N/A'

            lines.append(f"| {date} | {open_str:>11} | {high_str:>11} | {low_str:>11} | {close_str:>11} | {volume_str:>11} | {change_str:>9} |")

        # Add spacing between indices
        if i < len(indices_data) - 1:
            lines.append("")

    return "\n".join(lines)


def _format_sectors_as_table(sectors_data: List[Dict[str, Any]]) -> str:
    """
    Format sector performance data as a markdown table.

    Args:
        sectors_data: List of sector performance dictionaries

    Returns:
        Markdown-formatted table string
    """
    if not sectors_data or len(sectors_data) == 0:
        return "No sector performance data available."

    lines = []

    # Header
    lines.append("## Sector Performance")
    lines.append("")

    # Table header
    lines.append("| Sector                      | Change    | Status    |")
    lines.append("|-----------------------------|-----------|-----------|")

    # Parse and sort sectors by performance
    parsed_sectors = []
    for sector in sectors_data:
        sector_name = sector.get('sector', 'N/A')
        change_str = sector.get('changesPercentage', '0%')

        # Parse percentage (handle formats like "+1.50%" or "-0.42%")
        try:
            change_val = float(change_str.replace('%', '').replace('+', ''))
        except (ValueError, AttributeError):
            change_val = 0.0

        parsed_sectors.append({
            'name': sector_name,
            'change_str': change_str,
            'change_val': change_val
        })

    # Sort by performance (descending)
    parsed_sectors.sort(key=lambda x: x['change_val'], reverse=True)

    # Table rows
    for sector in parsed_sectors:
        name = sector['name']
        change_str = sector['change_str']
        change_val = sector['change_val']

        # Add status indicator
        if change_val > 0:
            status = "📈 Up"
        elif change_val < 0:
            status = "📉 Down"
        else:
            status = "➡️ Flat"

        # Pad percentage for alignment
        if not change_str.startswith('+') and not change_str.startswith('-'):
            if change_val >= 0:
                change_str = '+' + change_str

        lines.append(f"| {name:27} | {change_str:>9} | {status:9} |")

    # Summary
    if parsed_sectors:
        best = parsed_sectors[0]
        worst = parsed_sectors[-1]

        lines.append("")
        lines.append(f"**Best Performing:** {best['name']} ({best['change_str']})")
        lines.append(f"**Worst Performing:** {worst['name']} ({worst['change_str']})")

    return "\n".join(lines)


def _calculate_price_statistics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate aggregated statistics for a list of daily price data.

    Args:
        data: List of daily OHLCV dictionaries (sorted newest first)

    Returns:
        Dictionary containing aggregated statistics
    """
    if not data or len(data) == 0:
        return {}

    # Sort to have oldest first for calculations
    sorted_data = sorted(data, key=lambda x: x.get('date', ''), reverse=False)

    # Extract closing prices for calculations
    closes = [d.get('close') for d in sorted_data if d.get('close') is not None]
    if not closes:
        return {}

    # Aggregated OHLC
    first_day = sorted_data[0]
    last_day = sorted_data[-1]

    stats = {
        'symbol': data[0].get('symbol', 'N/A'),
        'period_days': len(data),
        'start_date': first_day.get('date', 'N/A'),
        'end_date': last_day.get('date', 'N/A'),

        # Aggregated OHLC
        'period_open': first_day.get('open'),
        'period_close': last_day.get('close'),
        'period_high': max(d.get('high') for d in sorted_data if d.get('high') is not None),
        'period_low': min(d.get('low') for d in sorted_data if d.get('low') is not None),

        # Price range
        'min_close': min(closes),
        'max_close': max(closes),

        # Period performance
        'period_change': None,
        'period_change_pct': None,
    }

    # Calculate period performance
    if stats['period_open'] and stats['period_close']:
        stats['period_change'] = stats['period_close'] - stats['period_open']
        stats['period_change_pct'] = (stats['period_change'] / stats['period_open']) * 100

    # Moving averages (only calculate if enough data)
    stats['ma_20'] = None
    stats['ma_50'] = None
    stats['ma_200'] = None

    if len(closes) >= 20:
        stats['ma_20'] = sum(closes[-20:]) / 20
    if len(closes) >= 50:
        stats['ma_50'] = sum(closes[-50:]) / 50
    if len(closes) >= 200:
        stats['ma_200'] = sum(closes[-200:]) / 200

    # Volatility (standard deviation of daily returns)
    if len(closes) >= 2:
        daily_returns = []
        for i in range(1, len(closes)):
            if closes[i-1] != 0:
                ret = ((closes[i] - closes[i-1]) / closes[i-1]) * 100
                daily_returns.append(ret)

        if daily_returns:
            # Calculate standard deviation
            mean_return = sum(daily_returns) / len(daily_returns)
            variance = sum((r - mean_return) ** 2 for r in daily_returns) / len(daily_returns)
            stats['volatility'] = variance ** 0.5  # Standard deviation
        else:
            stats['volatility'] = None
    else:
        stats['volatility'] = None

    # Volume statistics
    volumes = [d.get('volume') for d in sorted_data if d.get('volume') is not None]
    if volumes:
        stats['avg_volume'] = sum(volumes) / len(volumes)
        stats['total_volume'] = sum(volumes)
    else:
        stats['avg_volume'] = None
        stats['total_volume'] = None

    return stats


def _format_price_summary(stats: Dict[str, Any]) -> str:
    """
    Format price statistics into a human-readable summary report.

    Args:
        stats: Dictionary of calculated statistics

    Returns:
        Formatted string report
    """
    if not stats:
        return "No data available for summary"

    from .utils import format_number, format_percentage

    lines = []

    # Header
    symbol = stats.get('symbol', 'N/A')
    period_days = stats.get('period_days', 0)
    start_date = stats.get('start_date', 'N/A')
    end_date = stats.get('end_date', 'N/A')

    lines.append(f"**Period:** {start_date} to {end_date} ({period_days} trading days)")
    lines.append("")

    # Collect all metrics for table
    metrics_rows = []

    # Period OHLC
    period_open = stats.get('period_open')
    period_close = stats.get('period_close')
    period_high = stats.get('period_high')
    period_low = stats.get('period_low')

    if period_open is not None:
        metrics_rows.append(("Period Open", f"${period_open:.2f}"))
    if period_close is not None:
        metrics_rows.append(("Period Close", f"${period_close:.2f}"))
    if period_high is not None:
        metrics_rows.append(("Period High", f"${period_high:.2f}"))
    if period_low is not None:
        metrics_rows.append(("Period Low", f"${period_low:.2f}"))

    # Performance
    period_change = stats.get('period_change')
    period_change_pct = stats.get('period_change_pct')

    if period_change is not None and period_change_pct is not None:
        sign = "+" if period_change >= 0 else ""
        metrics_rows.append(("Period Change", f"{sign}${period_change:.2f} ({format_percentage(period_change_pct)})"))

    min_close = stats.get('min_close')
    max_close = stats.get('max_close')
    if min_close is not None and max_close is not None:
        range_pct = ((max_close - min_close) / min_close) * 100 if min_close != 0 else 0
        metrics_rows.append(("Price Range", f"${min_close:.2f} - ${max_close:.2f} ({format_percentage(range_pct)} range)"))

    volatility = stats.get('volatility')
    if volatility is not None:
        metrics_rows.append(("Volatility (Daily Std Dev)", f"{volatility:.2f}%"))

    # Moving Averages
    ma_20 = stats.get('ma_20')
    ma_50 = stats.get('ma_50')
    ma_200 = stats.get('ma_200')

    if ma_20 is not None:
        metrics_rows.append(("20-Day MA", f"${ma_20:.2f}"))
    if ma_50 is not None:
        metrics_rows.append(("50-Day MA", f"${ma_50:.2f}"))
    if ma_200 is not None:
        metrics_rows.append(("200-Day MA", f"${ma_200:.2f}"))

    # Volume Statistics
    avg_volume = stats.get('avg_volume')
    total_volume = stats.get('total_volume')

    if avg_volume is not None:
        avg_vol_formatted = format_number(avg_volume).replace('$', '')
        metrics_rows.append(("Average Daily Volume", avg_vol_formatted))
    if total_volume is not None:
        total_vol_formatted = format_number(total_volume).replace('$', '')
        metrics_rows.append(("Total Volume", total_vol_formatted))

    # Output as markdown table
    if metrics_rows:
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        for metric, value in metrics_rows:
            lines.append(f"| {metric} | {value} |")
        lines.append("")

    return "\n".join(lines)


def _format_indices_summary(indices_data: Dict[str, List[Dict[str, Any]]], period_info: Dict[str, Any]) -> str:
    """
    Format multiple market indices statistics into a summary report.

    Args:
        indices_data: Dictionary mapping index symbol to list of price data
        period_info: Dictionary with period metadata (num_days, start_date, end_date)

    Returns:
        Formatted string report with sections for each index
    """
    if not indices_data:
        return "No index data available for summary"

    from .utils import format_number, format_percentage

    lines = []

    # Header
    num_indices = len(indices_data)
    num_days = period_info.get('num_days', 0)
    start_date = period_info.get('start_date', 'N/A')
    end_date = period_info.get('end_date', 'N/A')

    lines.append(f"**Period:** {start_date} to {end_date} ({num_days} trading days)")
    lines.append("")

    # Process each index
    for i, (symbol, data) in enumerate(indices_data.items()):
        if not data:
            continue

        # Calculate statistics for this index using existing helper
        stats = _calculate_price_statistics(data)

        if not stats:
            continue

        # Index section header
        index_name = _get_index_name(symbol)
        lines.append(f"### {index_name} ({symbol})")
        lines.append("")

        # Collect metrics for table
        metrics_rows = []

        # Period OHLC
        period_open = stats.get('period_open')
        period_close = stats.get('period_close')
        period_high = stats.get('period_high')
        period_low = stats.get('period_low')

        if period_open is not None and period_close is not None:
            metrics_rows.append(("Period", f"${period_open:.2f} → ${period_close:.2f}"))
        if period_high is not None and period_low is not None:
            metrics_rows.append(("Range", f"${period_low:.2f} - ${period_high:.2f}"))

        # Performance
        period_change = stats.get('period_change')
        period_change_pct = stats.get('period_change_pct')
        if period_change is not None and period_change_pct is not None:
            sign = "+" if period_change >= 0 else ""
            metrics_rows.append(("Change", f"{sign}${period_change:.2f} ({format_percentage(period_change_pct)})"))

        # Volatility
        volatility = stats.get('volatility')
        if volatility is not None:
            metrics_rows.append(("Volatility", f"{volatility:.2f}%"))

        # Moving Averages
        ma_20 = stats.get('ma_20')
        ma_50 = stats.get('ma_50')
        ma_200 = stats.get('ma_200')

        if ma_20 is not None:
            metrics_rows.append(("20-Day MA", f"${ma_20:.2f}"))
        if ma_50 is not None:
            metrics_rows.append(("50-Day MA", f"${ma_50:.2f}"))
        if ma_200 is not None:
            metrics_rows.append(("200-Day MA", f"${ma_200:.2f}"))

        # Output as markdown table
        if metrics_rows:
            lines.append("| Metric | Value |")
            lines.append("|--------|-------|")
            for metric, value in metrics_rows:
                lines.append(f"| {metric} | {value} |")

        # Add spacing between indices (except for last one)
        if i < len(indices_data) - 1:
            lines.append("")

    return "\n".join(lines)


def _get_index_name(symbol: str) -> str:
    """Get human-readable name for common market indices."""
    index_names = {
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ Composite",
        "^DJI": "Dow Jones Industrial",
        "^RUT": "Russell 2000",
        "^VIX": "CBOE Volatility Index",
        "000001.SS": "SSE Composite",
        "399001.SZ": "SZSE Component",
        "000300.SS": "CSI 300",
        "^HSI": "Hang Seng Index",
        "^HSCE": "Hang Seng China Enterprises",
    }
    return index_names.get(symbol, symbol)


async def fetch_stock_daily_prices(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """
    Fetch historical daily OHLCV price data for a stock.

    For periods < 14 trading days: Returns markdown table with daily OHLCV data
    For periods >= 14 trading days: Returns formatted summary report with aggregated statistics

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Limit number of records (if not using date range)

    Returns:
        - If < 14 trading days: Markdown table with daily OHLCV data (newest first)
        - If >= 14 trading days: Formatted string summary with aggregated statistics
    """
    try:
        # Get FMP client (async to handle event loop properly)
        fmp_client = await get_fmp_client()

        # Default to last 60 trading days if no parameters
        if not start_date and not end_date and not limit:
            limit = 60

        # Use FMP client's get_stock_price method
        if start_date or end_date:
            # Use date range
            results = await fmp_client.get_stock_price(
                symbol=symbol,
                from_date=start_date,
                to_date=end_date
            )
        else:
            # Use limit - need to calculate date range
            if limit:
                end = datetime.now().date()
                # Estimate: ~252 trading days per year, add 50% buffer for weekends/holidays
                days_back = int(limit * 1.5)
                start = end - timedelta(days=days_back)

                results = await fmp_client.get_stock_price(
                    symbol=symbol,
                    from_date=start.isoformat(),
                    to_date=end.isoformat()
                )

                # Apply limit after fetching
                if results and len(results) > limit:
                    results = results[:limit]
            else:
                # Get recent data (default behavior)
                results = await fmp_client.get_stock_price(symbol=symbol)

        if not results:
            logger.warning(f"No price data found for {symbol}")
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            return f"""## Stock Price Data: {symbol}
**Retrieved:** {timestamp}
**Status:** No data available

No price data available for the specified period."""

        # Generate file-ready header
        num_days = len(results)
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        # Get actual date range from results
        dates = [d.get('date') for d in results if d.get('date')]
        if dates:
            sorted_dates = sorted(dates)
            actual_start = sorted_dates[0]
            actual_end = sorted_dates[-1]
        else:
            actual_start = start_date or 'N/A'
            actual_end = end_date or 'N/A'

        # Generate descriptive title
        if start_date and end_date:
            title = f"Stock Price Data: {symbol} ({start_date} to {end_date})"
        elif actual_start != 'N/A' and actual_end != 'N/A':
            title = f"Stock Price Data: {symbol} ({actual_start} to {actual_end})"
        else:
            title = f"Stock Price Data: {symbol}"

        header = f"""## {title}
**Retrieved:** {timestamp}
**Market:** US Stock
**Period:** {actual_start} to {actual_end}
**Data Points:** {num_days} trading days

"""

        # Check if we should return normalized summary or markdown table
        if num_days >= 14:
            # Return normalized summary for long periods
            logger.debug(f"Retrieved {num_days} days for {symbol}, returning normalized summary")
            stats = _calculate_price_statistics(results)
            return header + _format_price_summary(stats)
        else:
            # Return markdown table for short periods
            logger.debug(f"Retrieved {num_days} daily price records for {symbol}, returning markdown table")
            return header + _format_price_data_as_table(results)

    except Exception as e:
        logger.error(f"Error retrieving daily prices for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        return f"""## Stock Price Data: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving price data: {str(e)}"""


async def fetch_company_overview(symbol: str) -> str:
    """
    Fetch comprehensive investment analysis overview for a company.

    Retrieves and formats investment-relevant data including financial health ratings,
    analyst consensus, earnings performance, and revenue segmentation.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")

    Returns:
        Formatted string with comprehensive investment intelligence
    """
    try:
        # Get FMP client (async to handle event loop properly)
        fmp_client = await get_fmp_client()

        output_lines = []

        # ═══ BASIC INFORMATION ═══
        profile_data = await fmp_client.get_profile(symbol)
        if not profile_data:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            return f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

No data found for symbol {symbol}"""

        profile = profile_data[0]
        company_name = profile.get('companyName', symbol)
        sector = profile.get('sector', 'N/A')
        industry = profile.get('industry', 'N/A')
        market_cap = profile.get('mktCap')
        price = profile.get('price')
        exchange = profile.get('exchangeShortName', 'N/A')

        # Add file-ready header
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        output_lines.append(f"## Company Overview: {symbol}")
        output_lines.append(f"**Company:** {company_name}")
        output_lines.append(f"**Retrieved:** {timestamp}")
        output_lines.append(f"**Market:** {exchange}")
        output_lines.append("")

        output_lines.append(f"Company: {company_name} ({symbol})")
        output_lines.append(f"Sector: {sector} | Industry: {industry}")
        output_lines.append(f"Market Cap: {format_number(market_cap)} | Current Price: ${price:.2f}" if price else f"Market Cap: {format_number(market_cap)}")
        output_lines.append("")

        # === SHARED DATA (fetched once, used by multiple sections) ===
        # Income statement for fiscal period lookup (Q1, Q2, Q3, Q4 FY20XX)
        try:
            income_stmt = await fmp_client.get_income_statement(symbol, period="quarter", limit=8) or []
        except Exception as e:
            logger.warning(f"Could not fetch income statement for {symbol}: {e}")
            income_stmt = []

        # Build fiscal_period_lookup using helper function
        fiscal_period_lookup = _build_fiscal_period_lookup(income_stmt)

        # Earnings calendar (used by SEC Filing Dates, Next Earnings, Earnings Performance)
        try:
            earnings_calendar = await fmp_client.get_historical_earnings_calendar(symbol, limit=10) or []
        except Exception as e:
            logger.warning(f"Could not fetch earnings calendar for {symbol}: {e}")
            earnings_calendar = []

        # === STOCK PRICE PERFORMANCE ===
        try:
            price_change_data = await fmp_client.get_stock_price_change(symbol)
            if price_change_data:
                changes = price_change_data[0]

                output_lines.append("### Stock Price Performance")
                output_lines.append("")

                # Build performance table
                performance_rows = []

                # Short-term (up to 1 month)
                if changes.get('1D') is not None:
                    performance_rows.append(("1 Day", format_percentage(changes.get('1D'))))
                if changes.get('5D') is not None:
                    performance_rows.append(("5 Days", format_percentage(changes.get('5D'))))
                if changes.get('1M') is not None:
                    performance_rows.append(("1 Month", format_percentage(changes.get('1M'))))

                # Medium-term (3-6 months)
                if changes.get('3M') is not None:
                    performance_rows.append(("3 Months", format_percentage(changes.get('3M'))))
                if changes.get('6M') is not None:
                    performance_rows.append(("6 Months", format_percentage(changes.get('6M'))))
                if changes.get('ytd') is not None:
                    performance_rows.append(("YTD", format_percentage(changes.get('ytd'))))

                # Long-term (1+ years)
                if changes.get('1Y') is not None:
                    performance_rows.append(("1 Year", format_percentage(changes.get('1Y'))))
                if changes.get('3Y') is not None:
                    performance_rows.append(("3 Years", format_percentage(changes.get('3Y'))))
                if changes.get('5Y') is not None:
                    performance_rows.append(("5 Years", format_percentage(changes.get('5Y'))))

                if performance_rows:
                    output_lines.append("| Period | Performance |")
                    output_lines.append("|--------|-------------|")
                    for period, perf in performance_rows:
                        output_lines.append(f"| {period} | {perf} |")
                    output_lines.append("")
        except Exception as e:
            logger.warning(f"Could not fetch stock price changes for {symbol}: {e}")

        # === KEY FINANCIAL METRICS ===
        try:
            # Fetch real financial metrics
            key_metrics_data = await fmp_client.get_key_metrics_ttm(symbol)
            ratios_data = await fmp_client.get_ratios_ttm(symbol)

            if key_metrics_data:
                metrics = key_metrics_data[0]
                ratios = ratios_data[0] if ratios_data else {}

                output_lines.append("### Key Financial Metrics (TTM)")
                output_lines.append("*Data based on Trailing Twelve Months*")
                output_lines.append("")

                # Collect all metrics for table
                metrics_rows = []

                # Valuation Ratios
                pe_ratio = metrics.get('peRatioTTM') or profile.get('pe')
                pb_ratio = metrics.get('pbRatioTTM')
                peg_ratio = metrics.get('pegRatioTTM')
                ev_to_ebitda = metrics.get('evToOperatingCashFlowTTM')

                if pe_ratio:
                    metrics_rows.append(("P/E Ratio", f"{pe_ratio:.2f}x"))
                if pb_ratio:
                    metrics_rows.append(("P/B Ratio", f"{pb_ratio:.2f}x"))
                if peg_ratio:
                    metrics_rows.append(("PEG Ratio", f"{peg_ratio:.2f}"))
                if ev_to_ebitda:
                    metrics_rows.append(("EV/OCF", f"{ev_to_ebitda:.2f}x"))

                # Profitability Metrics
                roe = metrics.get('roeTTM') or ratios.get('returnOnEquityTTM')
                roa = metrics.get('roaTTM') or ratios.get('returnOnAssetsTTM')
                net_margin = ratios.get('netProfitMarginTTM')
                operating_margin = ratios.get('operatingProfitMarginTTM')

                if roe:
                    roe_val = f"{roe*100:.2f}%" if roe < 1 else f"{roe:.2f}%"
                    metrics_rows.append(("ROE (Return on Equity)", roe_val))
                if roa:
                    roa_val = f"{roa*100:.2f}%" if roa < 1 else f"{roa:.2f}%"
                    metrics_rows.append(("ROA (Return on Assets)", roa_val))
                if net_margin:
                    nm_val = f"{net_margin*100:.2f}%" if net_margin < 1 else f"{net_margin:.2f}%"
                    metrics_rows.append(("Net Profit Margin", nm_val))
                if operating_margin:
                    om_val = f"{operating_margin*100:.2f}%" if operating_margin < 1 else f"{operating_margin:.2f}%"
                    metrics_rows.append(("Operating Margin", om_val))

                # Leverage & Liquidity
                debt_to_equity = ratios.get('debtEquityRatioTTM')
                current_ratio = ratios.get('currentRatioTTM')
                quick_ratio = ratios.get('quickRatioTTM')
                interest_coverage = ratios.get('interestCoverageTTM')

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
        except Exception as e:
            logger.warning(f"Could not fetch key financial metrics for {symbol}: {e}")

        # === SEC FILING DATES ===
        # Note: fiscal_period_lookup and earnings_calendar already fetched at beginning
        try:
            # Get latest 10-Q and 10-K filings
            filings_10q = await fmp_client.get_sec_filings(symbol, filing_type="10-Q", limit=3)
            filings_10k = await fmp_client.get_sec_filings(symbol, filing_type="10-K", limit=2)

            has_filing_data = bool(filings_10q or filings_10k)

            if has_filing_data:
                output_lines.append("### SEC Filing Dates")
                output_lines.append("")

                output_lines.append("| Filing Type | Filing Date | Fiscal Period |")
                output_lines.append("|-------------|-------------|---------------|")

                # Show latest 10-K (annual report that includes Q4)
                if filings_10k:
                    for filing in filings_10k[:1]:  # Just the latest
                        filing_date = filing.get('fillingDate', 'N/A')
                        if filing_date and ' ' in filing_date:
                            filing_date = filing_date.split(' ')[0]  # Remove time part

                        # For 10-K, find Q4 fiscal period (10-K includes Q4)
                        fiscal_period = "Annual"
                        if fiscal_period_lookup:
                            # Find Q4 entries to determine fiscal year
                            for date_key, period_name in sorted(fiscal_period_lookup.items(), reverse=True):
                                if period_name.startswith("Q4"):
                                    # Extract FY from "Q4 FY2025" and show as "Q4 FY2025 (Annual)"
                                    fiscal_period = f"{period_name} (Annual)"
                                    break

                        output_lines.append(f"| **10-K** | {filing_date} | {fiscal_period} |")

                # Show latest 10-Q filings
                if filings_10q:
                    for filing in filings_10q[:3]:  # Last 3 quarterly reports
                        filing_date = filing.get('fillingDate', 'N/A')
                        if filing_date and ' ' in filing_date:
                            filing_date = filing_date.split(' ')[0]

                        # Match filing to fiscal period using helper
                        fiscal_period = _match_filing_to_fiscal_period(
                            filing_date, earnings_calendar, fiscal_period_lookup
                        )
                        output_lines.append(f"| **10-Q** (Quarterly) | {filing_date} | {fiscal_period} |")

                output_lines.append("")

                # Add tip for US stocks about get_sec_filing tool
                # US stocks don't have exchange suffix (.SS, .SZ, .HK, etc.)
                is_us_stock = '.' not in symbol or symbol.endswith('.US')
                if is_us_stock:
                    output_lines.append("*Tip: Use `get_sec_filing` tool to fetch complete earnings call transcripts and SEC filings.*")
                    output_lines.append("")

            # === NEXT EARNINGS REPORT ===
            if earnings_calendar:
                # Find upcoming reports (eps is None) and pick the earliest one
                upcoming_reports = [
                    cal for cal in earnings_calendar
                    if cal.get('eps') is None and cal.get('date')
                ]

                if upcoming_reports:
                    upcoming_reports.sort(key=lambda x: x.get('date', '9999-99-99'))
                    next_report = upcoming_reports[0]

                    output_lines.append("### Next Earnings Report")
                    output_lines.append("")

                    report_date = next_report.get('date', 'N/A')
                    fiscal_ending = next_report.get('fiscalDateEnding', 'N/A')
                    time_slot = next_report.get('time', '')
                    eps_estimate = next_report.get('epsEstimated')
                    rev_estimate = next_report.get('revenueEstimated')

                    # Determine fiscal period name (lookup first, then infer)
                    fiscal_period_name = fiscal_period_lookup.get(fiscal_ending)
                    if not fiscal_period_name and fiscal_ending != 'N/A':
                        fiscal_period_name = _infer_fiscal_period(fiscal_ending, fiscal_period_lookup)
                    fiscal_period_name = fiscal_period_name or "N/A"

                    # Format time slot
                    time_desc = {
                        "amc": " (After Market Close)",
                        "bmo": " (Before Market Open)"
                    }.get(time_slot, "")

                    output_lines.append(f"**Report Date:** {report_date}{time_desc}")
                    output_lines.append(f"**Fiscal Period:** {fiscal_period_name}")
                    output_lines.append(f"**Fiscal Period End:** {fiscal_ending}")

                    if eps_estimate is not None:
                        output_lines.append(f"**EPS Estimate:** ${eps_estimate:.2f}")
                    if rev_estimate is not None:
                        output_lines.append(f"**Revenue Estimate:** {format_number(rev_estimate)}")

                    output_lines.append("")

        except Exception as e:
            logger.warning(f"Could not fetch SEC filings/earnings calendar for {symbol}: {e}")

        # === EARNINGS PERFORMANCE ===
        # Use earnings_calendar (already fetched at beginning) instead of separate get_earnings_report
        try:
            # Filter to get reported quarters only (eps is not None means already reported)
            reported_earnings = [e for e in earnings_calendar if e.get('eps') is not None]

            if reported_earnings:
                output_lines.append("### Earnings Performance")
                output_lines.append("")

                # Show latest quarter in detail
                latest = reported_earnings[0]
                announce_date = latest.get('date', 'N/A')
                fiscal_ending = latest.get('fiscalDateEnding')
                eps_actual = latest.get('eps')
                eps_estimate = latest.get('epsEstimated')
                revenue_actual = latest.get('revenue')
                revenue_estimate = latest.get('revenueEstimated')

                # Get fiscal period label
                fiscal_label = fiscal_period_lookup.get(fiscal_ending, '') if fiscal_ending else ''
                latest_label = f"{announce_date} ({fiscal_label})" if fiscal_label else announce_date

                output_lines.append(f"**Latest Quarter ({latest_label}):**")
                output_lines.append("")

                # EPS data
                if eps_actual is not None:
                    if eps_estimate and eps_estimate != 0:
                        eps_surprise = ((eps_actual - eps_estimate) / abs(eps_estimate)) * 100
                        output_lines.append(f"- **EPS:** ${eps_actual:.2f} actual vs ${eps_estimate:.2f} estimate ({format_percentage(eps_surprise)} surprise)")
                    else:
                        output_lines.append(f"- **EPS:** ${eps_actual:.2f} (no estimate available)")

                # Revenue data
                if revenue_actual is not None:
                    if revenue_estimate and revenue_estimate != 0:
                        rev_surprise = ((revenue_actual - revenue_estimate) / abs(revenue_estimate)) * 100
                        output_lines.append(f"- **Revenue:** {format_number(revenue_actual)} actual vs {format_number(revenue_estimate)} estimate ({format_percentage(rev_surprise)} surprise)")
                    else:
                        output_lines.append(f"- **Revenue:** {format_number(revenue_actual)} (no estimate available)")

                # Show earnings trend for last 4 quarters with fiscal period column
                if len(reported_earnings) > 1:
                    output_lines.append("")
                    output_lines.append("**Recent Earnings Trend:**")
                    output_lines.append("")
                    output_lines.append("| Date | Fiscal Period | EPS | Revenue |")
                    output_lines.append("|------|---------------|-----|---------|")

                    for quarter in reported_earnings[:4]:
                        q_date = quarter.get('date', 'N/A')
                        q_fiscal_ending = quarter.get('fiscalDateEnding')
                        q_eps = quarter.get('eps')
                        q_revenue = quarter.get('revenue')

                        # Get fiscal period label
                        q_fiscal_label = fiscal_period_lookup.get(q_fiscal_ending, 'N/A') if q_fiscal_ending else 'N/A'
                        eps_str = f"${q_eps:.2f}" if q_eps is not None else "N/A"
                        revenue_str = format_number(q_revenue) if q_revenue is not None else "N/A"
                        output_lines.append(f"| {q_date} | {q_fiscal_label} | {eps_str} | {revenue_str} |")

                output_lines.append("")
        except Exception as e:
            logger.warning(f"Could not process earnings performance for {symbol}: {e}")

        # === ANALYST CONSENSUS & RATINGS ===
        try:
            # Fetch all analyst data
            price_target_consensus = await fmp_client.get_price_target_consensus(symbol)
            grades_summary_data = await fmp_client.get_grades_summary(symbol)
            price_target_summary = await fmp_client.get_price_target_summary(symbol)
            recent_grades = await fmp_client.get_stock_grades(symbol, limit=10)

            output_lines.append("### Analyst Consensus & Ratings")
            output_lines.append("")

            # Price Targets Section
            if price_target_consensus:
                pt = price_target_consensus[0]
                median = pt.get('targetMedian')
                low = pt.get('targetLow')
                high = pt.get('targetHigh')
                consensus = pt.get('targetConsensus')

                output_lines.append("**Price Targets:**")
                output_lines.append("")
                pt_rows = []
                if median and price:
                    upside = ((median - price) / price * 100) if price else 0
                    upside_sign = "+" if upside >= 0 else ""
                    pt_rows.append(("Consensus Target", f"${median:.2f} ({upside_sign}{upside:.1f}% from current)"))
                if low and high:
                    pt_rows.append(("Target Range", f"${low:.2f} - ${high:.2f}"))
                if consensus:
                    pt_rows.append(("Analyst Consensus", str(consensus)))

                if pt_rows:
                    for label, value in pt_rows:
                        output_lines.append(f"- **{label}:** {value}")
                    output_lines.append("")

            # Rating Distribution
            if grades_summary_data:
                gs = grades_summary_data[0]
                strong_buy = gs.get('strongBuy', 0)
                buy = gs.get('buy', 0)
                hold = gs.get('hold', 0)
                sell = gs.get('sell', 0)
                strong_sell = gs.get('strongSell', 0)
                consensus = gs.get('consensus', 'N/A')

                total_ratings = strong_buy + buy + hold + sell + strong_sell
                if total_ratings > 0:
                    output_lines.append("**Rating Distribution:**")
                    output_lines.append("")
                    output_lines.append("| Rating | Count | Percentage |")
                    output_lines.append("|--------|-------|------------|")

                    if strong_buy > 0:
                        pct = (strong_buy / total_ratings * 100)
                        output_lines.append(f"| Strong Buy | {strong_buy} | {pct:.1f}% |")
                    if buy > 0:
                        pct = (buy / total_ratings * 100)
                        output_lines.append(f"| Buy | {buy} | {pct:.1f}% |")
                    if hold > 0:
                        pct = (hold / total_ratings * 100)
                        output_lines.append(f"| Hold | {hold} | {pct:.1f}% |")
                    if sell > 0:
                        pct = (sell / total_ratings * 100)
                        output_lines.append(f"| Sell | {sell} | {pct:.1f}% |")
                    if strong_sell > 0:
                        pct = (strong_sell / total_ratings * 100)
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
                    company = grade.get('gradingCompany', 'N/A')
                    new_grade = grade.get('newGrade', 'N/A')
                    previous_grade = grade.get('previousGrade', '')
                    action = grade.get('action', 'N/A')
                    date = grade.get('date', 'N/A')

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
                    analyst_company = firm_target.get('analystCompany', 'N/A')
                    target_price = firm_target.get('adjPriceTarget')
                    analyst_name = firm_target.get('analystName', '-')

                    target_str = f"${target_price:.2f}" if target_price else "N/A"
                    output_lines.append(f"| {analyst_company} | {analyst_name} | {target_str} |")

                output_lines.append("")

        except Exception as e:
            logger.warning(f"Could not fetch analyst consensus for {symbol}: {e}")

        # === REVENUE BREAKDOWN ===
        try:
            # Product segmentation - use QUARTERLY for fresher data
            product_data = await fmp_client.get_revenue_product_segmentation(symbol, period="quarter", structure="flat")
            geo_data = await fmp_client.get_revenue_geographic_segmentation(symbol, period="quarter", structure="flat")

            has_product_data = False
            has_geo_data = False

            # Check if we have any data
            if product_data and len(product_data) > 0:
                latest_product_record = product_data[0]
                # Extract date and nested data (structure: {"2024-09-28": {"Mac": 123, ...}})
                if latest_product_record and isinstance(latest_product_record, dict):
                    fiscal_date = list(latest_product_record.keys())[0]
                    product_revenues = latest_product_record[fiscal_date]
                    if product_revenues and isinstance(product_revenues, dict) and len(product_revenues) > 0:
                        has_product_data = True

            if geo_data and len(geo_data) > 0:
                latest_geo_record = geo_data[0]
                # Extract date and nested data
                if latest_geo_record and isinstance(latest_geo_record, dict):
                    geo_date = list(latest_geo_record.keys())[0]
                    geo_revenues = latest_geo_record[geo_date]
                    if geo_revenues and isinstance(geo_revenues, dict) and len(geo_revenues) > 0:
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
                period_label = fiscal_period_lookup.get(fiscal_date, f"Period ending {fiscal_date}")
                output_lines.append(f"**By Product ({period_label}):**")
                output_lines.append(f"*Report Date: {fiscal_date}*")
                output_lines.append("")

                total_revenue = sum(product_revenues.values())

                # Sort by revenue (descending) and show top items
                sorted_products = sorted(product_revenues.items(), key=lambda x: x[1], reverse=True)
                output_lines.append("| Product | Revenue | Percentage |")
                output_lines.append("|---------|---------|------------|")
                for product, revenue in sorted_products[:5]:  # Top 5 products
                    percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                    output_lines.append(f"| {product} | {format_number(revenue)} | {percentage:.1f}% |")

                output_lines.append("")

            # Geographic breakdown
            if has_geo_data:
                latest_geo_record = geo_data[0]
                geo_date = list(latest_geo_record.keys())[0]
                geo_revenues = latest_geo_record[geo_date]

                # Get fiscal period name from lookup
                period_label = fiscal_period_lookup.get(geo_date, f"Period ending {geo_date}")
                output_lines.append(f"**By Region ({period_label}):**")
                output_lines.append(f"*Report Date: {geo_date}*")
                output_lines.append("")

                total_revenue = sum(geo_revenues.values())

                # Sort by revenue (descending)
                sorted_regions = sorted(geo_revenues.items(), key=lambda x: x[1], reverse=True)
                output_lines.append("| Region | Revenue | Percentage |")
                output_lines.append("|--------|---------|------------|")
                for region, revenue in sorted_regions:
                    percentage = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                    output_lines.append(f"| {region} | {format_number(revenue)} | {percentage:.1f}% |")

                output_lines.append("")
        except Exception as e:
            logger.warning(f"Could not fetch revenue breakdown for {symbol}: {e}")

        result = "\n".join(output_lines)
        logger.debug(f"Retrieved comprehensive investment overview for {symbol}")
        return result

    except Exception as e:
        logger.error(f"Error retrieving company overview for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        return f"""## Company Overview: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving company overview: {str(e)}"""


async def fetch_stock_realtime_quote(symbol: str) -> str:
    """
    Fetch real-time stock quote with market hours detection.

    Automatically detects current market session (regular hours, after-market, or closed)
    and fetches appropriate real-time quote data.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")

    Returns:
        Formatted string with real-time quote information
    """
    try:
        # Get FMP client (async to handle event loop properly)
        fmp_client = await get_fmp_client()

        output_lines = []

        # Detect market session
        session, current_time_et = get_market_session()

        # Get regular quote data (always available)
        quote_data = await fmp_client.get_quote(symbol)
        if not quote_data or len(quote_data) == 0:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            return f"""## Real-Time Quote: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

No quote data found for {symbol}"""

        quote = quote_data[0]

        # Extract data
        company_name = quote.get('name', symbol)
        price = quote.get('price', 0)
        change = quote.get('change', 0)
        change_pct = quote.get('changesPercentage', 0)
        open_price = quote.get('open')
        day_low = quote.get('dayLow')
        day_high = quote.get('dayHigh')
        year_low = quote.get('yearLow')
        year_high = quote.get('yearHigh')
        volume = quote.get('volume')
        avg_volume = quote.get('avgVolume')
        market_cap = quote.get('marketCap')
        previous_close = quote.get('previousClose')
        exchange = quote.get('exchange', 'N/A')

        # Add file-ready header
        timestamp_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        output_lines.append(f"## Real-Time Quote: {symbol}")
        output_lines.append(f"**Company:** {company_name}")
        output_lines.append(f"**Retrieved:** {timestamp_utc}")
        output_lines.append(f"**Market:** {exchange}")
        output_lines.append(f"**Price:** ${price:.2f}")
        output_lines.append("")

        # Market status
        timestamp = current_time_et.strftime("%Y-%m-%d %H:%M:%S ET")
        output_lines.append(f"**Market Status:** {session.replace('_', ' ').upper()} | **Last Updated:** {timestamp}")
        output_lines.append("")

        # Current price
        change_sign = "+" if change >= 0 else ""
        output_lines.append(f"**Current Price:** ${price:.2f} ({change_sign}{change:.2f} / {change_sign}{change_pct:.2f}%)")

        # After-hours price (if in after-hours session)
        if session == "AFTER_HOURS":
            try:
                aftermarket_data = await fmp_client.get_aftermarket_quote(symbol)
                if aftermarket_data and len(aftermarket_data) > 0:
                    ah = aftermarket_data[0]
                    ah_price = ah.get('price')
                    if ah_price and ah_price != price:
                        ah_change = ah_price - price
                        ah_change_pct = (ah_change / price * 100) if price > 0 else 0
                        ah_sign = "+" if ah_change >= 0 else ""
                        output_lines.append(f"**After-Hours:** ${ah_price:.2f} ({ah_sign}{ah_change:.2f} / {ah_sign}{ah_change_pct:.2f}%)")
            except Exception as e:
                logger.warning(f"Could not fetch after-hours quote for {symbol}: {e}")

        output_lines.append("")

        # Build quote details table
        quote_rows = []

        # Basic info
        if open_price:
            quote_rows.append(("Open", f"${open_price:.2f}"))
        if day_low and day_high:
            quote_rows.append(("Day Range", f"${day_low:.2f} - ${day_high:.2f}"))
        if year_low and year_high:
            quote_rows.append(("52-Week Range", f"${year_low:.2f} - ${year_high:.2f}"))
        if previous_close:
            quote_rows.append(("Previous Close", f"${previous_close:.2f}"))

        # Volume
        if volume:
            volume_str = format_number(volume).replace('$', '')
            if avg_volume:
                avg_volume_str = format_number(avg_volume).replace('$', '')
                quote_rows.append(("Volume", f"{volume_str} (Avg: {avg_volume_str})"))
            else:
                quote_rows.append(("Volume", volume_str))

        # Financial metrics
        eps = quote.get('eps')
        pe = quote.get('pe')

        if market_cap:
            quote_rows.append(("Market Cap", format_number(market_cap)))
        if eps:
            quote_rows.append(("EPS (TTM)", f"${eps:.2f}"))
        if pe:
            quote_rows.append(("P/E Ratio", f"{pe:.2f}"))

        # Output as markdown table
        if quote_rows:
            output_lines.append("| Metric | Value |")
            output_lines.append("|--------|-------|")
            for metric, value in quote_rows:
                output_lines.append(f"| {metric} | {value} |")

        output_lines.append("")

        result = "\n".join(output_lines)
        logger.debug(f"Retrieved real-time quote for {symbol}")
        return result

    except Exception as e:
        logger.error(f"Error retrieving real-time quote for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        return f"""## Real-Time Quote: {symbol}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving real-time quote: {str(e)}"""


async def fetch_market_indices(
    indices: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 60
) -> str:
    """
    Fetch market indices data (S&P 500, NASDAQ, Dow Jones).

    For periods < 14 trading days: Returns markdown tables with OHLCV data per index
    For periods >= 14 trading days: Returns formatted summary with sections per index

    Args:
        indices: List of index symbols, default is major US indices
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Number of records per index (default 60)

    Returns:
        - If < 14 trading days: Markdown tables with index OHLCV data (one table per index)
        - If >= 14 trading days: Formatted string summary with statistics per index
    """
    try:
        # Get FMP client (async to handle event loop properly)
        fmp_client = await get_fmp_client()

        # Default indices if not specified
        if indices is None:
            indices = ["^GSPC", "^IXIC", "^DJI", "^RUT"]

        # Store results per index (for normalization logic)
        indices_data = {}
        all_results = []

        # Fetch data for each index
        for index_symbol in indices:
            try:
                if start_date or end_date:
                    # Use date range
                    index_data = await fmp_client.get_stock_price(
                        symbol=index_symbol,
                        from_date=start_date,
                        to_date=end_date
                    )
                else:
                    # Use limit
                    end = datetime.now().date()
                    days_back = int(limit * 1.5)  # Buffer for weekends/holidays
                    start = end - timedelta(days=days_back)

                    index_data = await fmp_client.get_stock_price(
                        symbol=index_symbol,
                        from_date=start.isoformat(),
                        to_date=end.isoformat()
                    )

                    # Apply limit
                    if index_data and len(index_data) > limit:
                        index_data = index_data[:limit]

                if index_data:
                    indices_data[index_symbol] = index_data
                    all_results.extend(index_data)

            except Exception as e:
                logger.warning(f"Error fetching data for index {index_symbol}: {e}")
                continue

        if not all_results:
            logger.warning(f"No index data found for {indices}")
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            indices_str = ", ".join(indices[:3]) if len(indices) <= 3 else f"{', '.join(indices[:3])} and {len(indices)-3} more"
            return f"""## Market Indices: {indices_str}
**Retrieved:** {timestamp}
**Status:** No data available

No index data available for the specified period."""

        # Determine if we should normalize based on limit/date range
        # For date ranges, estimate number of days
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            calendar_days = (end_dt - start_dt).days
            # Rough estimate: 252 trading days per 365 calendar days
            estimated_trading_days = int(calendar_days * 252 / 365)
            should_normalize = estimated_trading_days >= 14
        else:
            # Use limit directly
            should_normalize = limit >= 14

        # Find actual date range from data
        all_dates = [d.get('date') for d in all_results if d.get('date')]
        if all_dates:
            all_dates_sorted = sorted(all_dates)
            actual_start = all_dates_sorted[0]
            actual_end = all_dates_sorted[-1]
            # Count unique trading days
            unique_days = len(set(all_dates))
        else:
            actual_start = start_date or 'N/A'
            actual_end = end_date or 'N/A'
            unique_days = limit

        # Generate file-ready header
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        indices_str = ", ".join(indices[:3]) if len(indices) <= 3 else f"{', '.join(indices[:3])} and {len(indices)-3} more"

        if start_date and end_date:
            title = f"Market Indices: {indices_str} ({start_date} to {end_date})"
        elif actual_start != 'N/A' and actual_end != 'N/A':
            title = f"Market Indices: {indices_str} ({actual_start} to {actual_end})"
        else:
            title = f"Market Indices: {indices_str}"

        header = f"""## {title}
**Retrieved:** {timestamp}
**Market:** US Stock Indices
**Period:** {actual_start} to {actual_end}
**Data Points:** {unique_days} trading days
**Indices:** {len(indices_data)} indices

"""

        if should_normalize and indices_data:
            # Return normalized summary
            period_info = {
                'num_days': unique_days,
                'start_date': actual_start,
                'end_date': actual_end
            }

            logger.debug(f"Retrieved {len(all_results)} records for {len(indices)} indices, returning normalized summary")
            return header + _format_indices_summary(indices_data, period_info)
        else:
            # Return markdown tables for short periods
            logger.debug(f"Retrieved {len(all_results)} records for {len(indices)} indices, returning markdown tables")
            return header + _format_indices_data_as_table(indices_data)

    except Exception as e:
        logger.error(f"Error retrieving market indices: {e}")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        indices_str = ", ".join(indices[:3]) if len(indices) <= 3 else f"{', '.join(indices[:3])} and {len(indices)-3} more"
        return f"""## Market Indices: {indices_str}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving index data: {str(e)}"""


async def fetch_sector_performance(date: Optional[str] = None) -> str:
    """
    Fetch market sector performance.

    Args:
        date: Analysis date in YYYY-MM-DD format (default: latest available)

    Returns:
        Markdown table with sector performance data sorted by performance
    """
    try:
        # Get FMP client (async to handle event loop properly)
        fmp_client = await get_fmp_client()

        # Generate file-ready header
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        date_str = f" ({date})" if date else ""
        header = f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Market:** US Stock Market

"""

        # Try the standard v3 endpoint first (more widely available)
        try:
            results = await fmp_client._make_request(
                "sectors-performance",
                params={},
                version="v3"
            )

            if results:
                logger.debug(f"Retrieved performance data for {len(results)} sectors")
                return header + _format_sectors_as_table(results)
        except:
            pass

        # Fallback to stable version with date if provided
        if date:
            params = {'date': date}
            results = await fmp_client._make_request(
                "sector-performance-snapshot",
                params=params,
                version="stable"
            )

            if results:
                logger.debug(f"Retrieved performance data for {len(results)} sectors")
                return header + _format_sectors_as_table(results)

        logger.warning("No sector performance data found - endpoint may not be available on this FMP plan")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        return f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Status:** No data available

No sector performance data available for the specified period."""

    except Exception as e:
        logger.error(f"Error retrieving sector performance: {e}")
        logger.warning("Sector performance endpoint may require a higher FMP API tier")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        date_str = f" ({date})" if date else ""
        return f"""## Sector Performance Analysis{date_str}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving sector performance data: {str(e)}"""


async def fetch_earnings_transcript(
    symbol: str,
    year: int,
    quarter: int
) -> str:
    """
    Fetch earnings call transcript.

    Retrieves the full transcript of a company's earnings call, formatted for
    easy reading and analysis of management's communication about financial
    performance, future plans, and strategy.

    Args:
        symbol: Stock ticker symbol (e.g., "AAPL", "600519.SS", "0700.HK")
        year: Fiscal year (e.g., 2020) - REQUIRED
        quarter: Fiscal quarter (1, 2, 3, or 4) - REQUIRED

    Returns:
        Formatted string with earnings call transcript
    """
    try:
        # Get FMP client (async to handle event loop properly)
        fmp_client = await get_fmp_client()

        output_lines = []

        # Fetch transcript data
        transcript_data = await fmp_client.get_earnings_call_transcript(
            symbol=symbol,
            year=year,
            quarter=quarter
        )

        if not transcript_data or len(transcript_data) == 0:
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            return f"""## Earnings Transcript: {symbol} Q{quarter} {year}
**Retrieved:** {timestamp}
**Status:** No data available

No earnings transcript found for {symbol} Q{quarter} {year}"""

        transcript = transcript_data[0]

        # Extract metadata
        company_symbol = transcript.get('symbol', symbol)
        period = transcript.get('period', 'N/A')
        fiscal_year = transcript.get('year', 'N/A')
        call_date = transcript.get('date', 'N/A')
        content = transcript.get('content', '')

        # Add file-ready header
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        output_lines.append(f"## Earnings Transcript: {symbol} Q{quarter} {year}")
        output_lines.append(f"**Retrieved:** {timestamp}")
        output_lines.append(f"**Fiscal Period:** {period} {fiscal_year}")
        output_lines.append(f"**Call Date:** {call_date}")
        output_lines.append("")

        # Header section
        output_lines.append(f"Earnings Call Transcript: {company_symbol}")
        output_lines.append("═" * 70)
        output_lines.append(f"Fiscal Period: {period} {fiscal_year}")
        output_lines.append(f"Call Date: {call_date}")
        output_lines.append("═" * 70)
        output_lines.append("")

        # Add transcript content
        if content:
            # Split content into lines for better formatting
            content_lines = content.split('\n')

            # If content is very long, provide full transcript
            # (LLMs can handle large context, and users want full analysis capability)
            output_lines.append("Transcript Content:")
            output_lines.append("")
            output_lines.append("```text")
            output_lines.extend(content_lines)
            output_lines.append("```")
            output_lines.append("")

            # Add transcript stats
            word_count = len(content.split())
            char_count = len(content)
            output_lines.append(f"Transcript Statistics:")
            output_lines.append(f"├─ Words: {word_count:,}")
            output_lines.append(f"└─ Characters: {char_count:,}")
        else:
            output_lines.append("Note: Transcript content is empty or not available.")

        result = "\n".join(output_lines)
        logger.debug(f"Retrieved earnings transcript for {symbol} {period} {fiscal_year}")
        return result

    except Exception as e:
        logger.error(f"Error retrieving earnings transcript for {symbol}: {e}")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        return f"""## Earnings Transcript: {symbol} Q{quarter} {year}
**Retrieved:** {timestamp}
**Status:** Error

Error retrieving earnings transcript: {str(e)}"""
