"""
US Market Analysis Tools (Async)

Centralized registry of all US market analysis tools.
Core implementations are in dedicated files within src/tools/us/.

This file provides the LangChain @tool decorated wrappers that agents use.
"""

from langchain_core.tools import tool
from src.data_client.fmp import FMPClient


@tool
async def technical_analyze(
    symbol: str,
    start_date: str,
    end_date: str,
    benchmark: str = "SPY"
) -> str:
    """
    Technical analysis for stocks using FMP API.

    Args:
        symbol: Stock ticker (e.g., "AAPL", "600519.SS", "0700.HK")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        benchmark: Benchmark symbol for beta calculation (default: SPY)

    Returns:
        Dict with technical analysis including indicators, trend, and chart
    """
    from src.tools.analysis.technical_analysis_tool import technical_analyze_stock_fmp_impl

    return await technical_analyze_stock_fmp_impl(symbol, start_date, end_date, benchmark)


@tool
async def financial_analyze(
    symbol: str,
    period_type: str = "annual",
    lookback_periods: int = 5,
    detail_level: str = "compact"
) -> str:
    """
    Comprehensive financial statement analysis using FMP API.

    Analyzes company financial health across 5 dimensions:
    - Financial Attitude: Accounting conservatism/aggressiveness patterns
    - Income Statement: Revenue/profit growth, margins, expense efficiency
    - Cash Flow: Cash generation quality, liquidity, free cash flow
    - Debt Risk: Leverage ratios, solvency, coverage metrics

    Args:
        symbol: Stock ticker (e.g., "AAPL", "TSLA", "CRWV", "600519.SS")
        period_type: "annual" or "quarterly" analysis
        lookback_periods: Number of historical periods to analyze (default 5)
        detail_level: "compact" or "extended" (includes statistical details)

    Returns:
        Dictionary with comprehensive financial analysis
    """
    from src.tools.analysis.financials_analysis_tool import analyze_financials_fmp_impl

    return await analyze_financials_fmp_impl(symbol, period_type, lookback_periods, detail_level)


@tool
async def dcf_analyze(
    symbol: str,
    use_analyst_estimates: bool = True,
    detail_level: str = "compact"
) -> str:
    """
    Custom DCF valuation with 3 scenarios (Low/Neutral/High).

    Generates scenario-based DCF valuations using:
    - FMP Custom DCF Advanced API with configurable parameters
    - Analyst consensus estimates for forward projections
    - Historical financial patterns for baseline assumptions
    - WACC calculation using CAPM model

    Scenarios:
    - Low: Conservative assumptions (lower growth, higher discount rate)
    - Neutral: Base case (historical averages, market WACC)
    - High: Optimistic assumptions (higher growth, lower discount rate)

    Args:
        symbol: Stock ticker (e.g., "AAPL", "MSFT", "TSLA")
        use_analyst_estimates: Incorporate analyst projections (default True)
        detail_level: "compact" or "extended" (adds WACC/growth sensitivity)

    Returns:
        Dictionary with 3 DCF scenarios, valuation range, and human-friendly explanations.

        Structure:
        - summary: Current price and valuation range across scenarios
        - scenarios_explained: Detailed explanations of each scenario's assumptions
        - methodology_notes: Data sources and DCF methodology explanation
        - technical_details: Raw numerical data for backward compatibility

    Example:
        result = await dcf_analyze("AAPL")
        print(result['summary']['valuation_range']['base_case_estimate'])
        # Output: "$104.03"
    """
    from src.tools.analysis.dcf_scenario_valuation_tool import analyze_dcf_scenarios_fmp_impl

    return await analyze_dcf_scenarios_fmp_impl(symbol, use_analyst_estimates, detail_level)
