"""
Financial Statement Analysis Tool (US Markets - FMP API)

Comprehensive financial analysis across 5 dimensions:
- Financial Attitude: Accounting conservatism/aggressiveness patterns
- Income Statement: Revenue/profit growth, margins, expense efficiency
- Cash Flow: Cash generation quality, liquidity, free cash flow
- Debt Risk: Leverage ratios, solvency, coverage metrics
- Overall Assessment: Integrated financial health evaluation
"""

from typing import Dict, List, Optional, Any
import logging
import asyncio

from src.tools.analysis.base import FundamentalAnalysisBase
from src.data_client.fmp import FMPClient

logger = logging.getLogger(__name__)


class FinancialsAnalysisTool(FundamentalAnalysisBase):
    """Comprehensive financial statement analysis using FMP API"""

    # Thresholds for financial attitude classification
    DEP_TO_REV_LOW, DEP_TO_REV_HIGH = 0.02, 0.06
    CAPEX_TO_REV_LOW, CAPEX_TO_REV_HIGH = 0.03, 0.08

    async def analyze(
        self,
        symbol: str,
        period_type: str = "annual",
        lookback_periods: int = 5,
        detail_level: str = "compact"
    ) -> str:
        """
        Perform comprehensive financial analysis (async)

        Args:
            symbol: Stock ticker symbol
            period_type: 'annual' or 'quarterly'
            lookback_periods: Number of historical periods
            detail_level: 'compact' or 'extended'

        Returns:
            Markdown-formatted string with comprehensive financial analysis
        """
        try:
            # Fetch all required data in parallel using asyncio.gather for HTTP/2 multiplexing
            # Extra period for YoY calculations on income and cash flow
            results = await asyncio.gather(
                self.client.get_income_statement(symbol, period=period_type, limit=lookback_periods + 1),
                self.client.get_cash_flow(symbol, period=period_type, limit=lookback_periods + 1),
                self.client.get_balance_sheet(symbol, period=period_type, limit=lookback_periods),
            )
            income_statements, cash_flows, balance_sheets = results

            if not income_statements or len(income_statements) < 2:
                available = len(income_statements) if income_statements else 0
                return f"""## Financial Analysis: {symbol}
**Status:** Error

Insufficient data for analysis - need at least 2 periods. Available periods: {available}"""

            # Adjust lookback to actual available data
            actual_periods = min(lookback_periods, len(income_statements) - 1)

            # Extract period labels
            periods = [stmt.get('date', stmt.get('calendarYear', 'N/A'))[:4]
                      for stmt in income_statements[:actual_periods]]

            # Perform analysis across all dimensions
            financial_attitude = self._analyze_financial_attitude(
                income_statements[:actual_periods],
                cash_flows[:actual_periods] if cash_flows else []
            )

            income_analysis = self._analyze_income_statement(
                income_statements[:actual_periods + 1],  # Need extra for YoY
                actual_periods
            )

            cash_flow_analysis = self._analyze_cash_flow(
                cash_flows[:actual_periods + 1] if cash_flows else [],
                income_statements[:actual_periods + 1],
                actual_periods
            )

            debt_risk_analysis = self._analyze_debt_risk(
                balance_sheets[:actual_periods] if balance_sheets else [],
                cash_flows[:actual_periods] if cash_flows else [],
                periods
            )

            # Format as markdown
            return self._format_markdown_output(
                symbol=symbol,
                period_type=period_type,
                periods=periods,
                latest_date=income_statements[0].get('date',
                            income_statements[0].get('calendarYear', 'N/A')),
                financial_attitude=financial_attitude,
                income_analysis=income_analysis,
                cash_flow_analysis=cash_flow_analysis,
                debt_risk_analysis=debt_risk_analysis,
                detail_level=detail_level,
                income_statements=income_statements[:actual_periods] if detail_level == "extended" else None,
                cash_flows=cash_flows[:actual_periods] if detail_level == "extended" and cash_flows else None
            )

        except Exception as e:
            logger.error(f"Error in financial analysis for {symbol}: {e}")
            return f"""## Financial Analysis: {symbol}
**Status:** Error

{type(e).__name__}: {str(e)}"""

    def _analyze_financial_attitude(
        self,
        income_statements: List[Dict],
        cash_flows: List[Dict]
    ) -> Dict[str, Any]:
        """
        Analyze company's accounting conservatism/aggressiveness

        Conservative: High depreciation/capex ratios → understating earnings
        Aggressive: Low depreciation/capex ratios → maximizing reported earnings
        """
        if not income_statements or not cash_flows:
            return {"error": "Insufficient data for financial attitude analysis"}

        dep_to_rev_series = []
        capex_to_rev_series = []

        for i in range(len(income_statements)):
            income = income_statements[i]
            cash_flow = cash_flows[i] if i < len(cash_flows) else {}

            revenue = income.get('revenue')
            depreciation = cash_flow.get('depreciationAndAmortization')
            capex = cash_flow.get('capitalExpenditure')

            # Calculate ratios
            dep_ratio = self._safe_divide(depreciation, revenue)
            capex_ratio = self._safe_divide(abs(capex) if capex else None, revenue)

            dep_to_rev_series.append(self._format_ratio(dep_ratio))
            capex_to_rev_series.append(self._format_ratio(capex_ratio))

        # Classify patterns
        dep_pattern = self._classify_attitude_pattern(
            dep_to_rev_series,
            self.DEP_TO_REV_LOW,
            self.DEP_TO_REV_HIGH
        )
        capex_pattern = self._classify_attitude_pattern(
            capex_to_rev_series,
            self.CAPEX_TO_REV_LOW,
            self.CAPEX_TO_REV_HIGH
        )

        # Overall classification
        classification, interpretation = self._determine_overall_attitude(
            dep_pattern, capex_pattern
        )

        return {
            "depreciation_to_revenue": dep_to_rev_series,
            "capex_to_revenue": capex_to_rev_series,
            "depreciation_pattern": dep_pattern,
            "capex_pattern": capex_pattern,
            "classification": classification,
            "interpretation": interpretation
        }

    def _classify_attitude_pattern(
        self,
        ratios: List[Optional[float]],
        low_threshold: float,
        high_threshold: float
    ) -> str:
        """Classify a series of ratios as persistently low/high/neutral"""
        clean_ratios = [r for r in ratios if r is not None]

        if len(clean_ratios) < 2:
            return "insufficient_data"

        low_count = sum(1 for r in clean_ratios if r < low_threshold)
        high_count = sum(1 for r in clean_ratios if r > high_threshold)

        min_periods = max(3, len(clean_ratios) - 1)

        if high_count >= min_periods:
            return "persistently_high_conservative"
        elif low_count >= min_periods:
            return "persistently_low_aggressive"
        else:
            return "neutral"

    def _determine_overall_attitude(
        self,
        dep_pattern: str,
        capex_pattern: str
    ) -> tuple[str, str]:
        """Determine overall financial attitude and interpretation"""

        aggressive_signals = sum([
            "aggressive" in dep_pattern,
            "aggressive" in capex_pattern
        ])

        conservative_signals = sum([
            "conservative" in dep_pattern,
            "conservative" in capex_pattern
        ])

        if aggressive_signals > 0 and conservative_signals == 0:
            return (
                "aggressive",
                "Company shows aggressive accounting with low depreciation/capex ratios. "
                "Reported earnings may have limited upside potential."
            )
        elif conservative_signals > 0 and aggressive_signals == 0:
            return (
                "conservative",
                "Company shows conservative accounting with high depreciation/capex ratios. "
                "Earnings may have room for improvement as investments mature."
            )
        elif aggressive_signals > 0 and conservative_signals > 0:
            return (
                "mixed",
                "Company shows mixed signals with some aggressive and some conservative patterns."
            )
        else:
            return (
                "neutral",
                "Company maintains balanced accounting practices without persistent extremes."
            )

    def _analyze_income_statement(
        self,
        income_statements: List[Dict],
        actual_periods: int
    ) -> Dict[str, Any]:
        """Analyze profitability, growth, and margin trends"""

        if not income_statements or len(income_statements) < 2:
            return {"error": "Insufficient income statement data"}

        # Extract key metrics
        revenue_series = []
        net_income_series = []
        gross_profit_series = []
        operating_income_series = []

        for stmt in income_statements[:actual_periods]:
            revenue_series.append(stmt.get('revenue'))
            net_income_series.append(stmt.get('netIncome'))
            gross_profit_series.append(stmt.get('grossProfit'))
            operating_income_series.append(stmt.get('operatingIncome'))

        # Calculate growth metrics
        growth_metrics = self._calculate_growth_metrics(
            revenue_series, net_income_series, actual_periods
        )

        # Calculate margin analysis
        margin_analysis = self._calculate_margin_analysis(
            income_statements[:actual_periods]
        )

        # Calculate expense ratios
        expense_ratios = self._calculate_expense_ratios(
            income_statements[:actual_periods]
        )

        return {
            "growth_metrics": growth_metrics,
            "margin_analysis": margin_analysis,
            "expense_ratios": expense_ratios
        }

    def _calculate_growth_metrics(
        self,
        revenue_series: List[Optional[float]],
        net_income_series: List[Optional[float]],
        periods: int
    ) -> Dict[str, Optional[float]]:
        """Calculate CAGR and YoY growth rates"""

        metrics = {}

        # Revenue CAGR
        if len(revenue_series) >= 3:
            rev_cagr_3y = self._calculate_cagr(
                revenue_series[0], revenue_series[2], 2
            )
            metrics["revenue_cagr_3y"] = self._format_ratio(rev_cagr_3y)

        if len(revenue_series) >= 5:
            rev_cagr_5y = self._calculate_cagr(
                revenue_series[0], revenue_series[4], 4
            )
            metrics["revenue_cagr_5y"] = self._format_ratio(rev_cagr_5y)

        # Net income CAGR (handle negative values)
        if len(net_income_series) >= 3:
            ni_0, ni_2 = net_income_series[0], net_income_series[2]
            if ni_0 and ni_2 and ni_0 > 0 and ni_2 > 0:
                ni_cagr_3y = self._calculate_cagr(ni_0, ni_2, 2)
                metrics["net_income_cagr_3y"] = self._format_ratio(ni_cagr_3y)

        if len(net_income_series) >= 5:
            ni_0, ni_4 = net_income_series[0], net_income_series[4]
            if ni_0 and ni_4 and ni_0 > 0 and ni_4 > 0:
                ni_cagr_5y = self._calculate_cagr(ni_0, ni_4, 4)
                metrics["net_income_cagr_5y"] = self._format_ratio(ni_cagr_5y)

        # YoY growth (latest vs prior)
        if len(revenue_series) >= 2:
            rev_yoy = self._calculate_growth_rate(
                revenue_series[0], revenue_series[1]
            )
            metrics["revenue_yoy"] = self._format_ratio(rev_yoy)

        if len(net_income_series) >= 2:
            ni_yoy = self._calculate_growth_rate(
                net_income_series[0], net_income_series[1]
            )
            metrics["net_income_yoy"] = self._format_ratio(ni_yoy)

        return metrics

    def _calculate_margin_analysis(
        self,
        income_statements: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate margin trends"""

        gross_margins = []
        operating_margins = []
        net_margins = []

        for stmt in income_statements:
            revenue = stmt.get('revenue')

            # Gross margin
            gross_profit = stmt.get('grossProfit')
            gross_margin = self._safe_divide(gross_profit, revenue)
            gross_margins.append(gross_margin)

            # Operating margin
            operating_income = stmt.get('operatingIncome')
            operating_margin = self._safe_divide(operating_income, revenue)
            operating_margins.append(operating_margin)

            # Net margin
            net_income = stmt.get('netIncome')
            net_margin = self._safe_divide(net_income, revenue)
            net_margins.append(net_margin)

        return {
            "gross_margin_latest": self._format_ratio(gross_margins[0]),
            "gross_margin_trend": self._calculate_trend(gross_margins),
            "operating_margin_latest": self._format_ratio(operating_margins[0]),
            "operating_margin_trend": self._calculate_trend(operating_margins),
            "net_margin_latest": self._format_ratio(net_margins[0]),
            "net_margin_trend": self._calculate_trend(net_margins)
        }

    def _calculate_expense_ratios(
        self,
        income_statements: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate expense efficiency ratios"""

        sales_expense_ratios = []
        admin_expense_ratios = []
        rd_expense_ratios = []

        for stmt in income_statements:
            revenue = stmt.get('revenue')

            # Sales & marketing expense ratio
            sales_exp = stmt.get('sellingAndMarketingExpenses')
            sales_ratio = self._safe_divide(sales_exp, revenue)
            sales_expense_ratios.append(sales_ratio)

            # Admin expense ratio
            admin_exp = stmt.get('generalAndAdministrativeExpenses')
            admin_ratio = self._safe_divide(admin_exp, revenue)
            admin_expense_ratios.append(admin_ratio)

            # R&D expense ratio
            rd_exp = stmt.get('researchAndDevelopmentExpenses')
            rd_ratio = self._safe_divide(rd_exp, revenue)
            rd_expense_ratios.append(rd_ratio)

        # Gross vs sales margin differential (pricing power indicator)
        gross_vs_sales = None
        if income_statements:
            latest = income_statements[0]
            gross_margin = self._safe_divide(
                latest.get('grossProfit'),
                latest.get('revenue')
            )
            sales_ratio = sales_expense_ratios[0]
            if gross_margin and sales_ratio:
                gross_vs_sales = gross_margin - sales_ratio

        return {
            "sales_expense_ratio_latest": self._format_ratio(sales_expense_ratios[0]),
            "sales_expense_trend": self._calculate_trend(sales_expense_ratios),
            "admin_expense_ratio_latest": self._format_ratio(admin_expense_ratios[0]),
            "admin_expense_trend": self._calculate_trend(admin_expense_ratios),
            "rd_expense_ratio_latest": self._format_ratio(rd_expense_ratios[0]),
            "rd_expense_trend": self._calculate_trend(rd_expense_ratios),
            "gross_vs_sales_margin": self._format_ratio(gross_vs_sales),
            "interpretation": self._interpret_expense_efficiency(
                sales_expense_ratios[0],
                self._calculate_trend(sales_expense_ratios),
                gross_vs_sales
            )
        }

    def _interpret_expense_efficiency(
        self,
        sales_ratio: Optional[float],
        sales_trend: Optional[str],
        gross_vs_sales: Optional[float]
    ) -> str:
        """Interpret expense efficiency metrics"""

        if not sales_ratio:
            return "Insufficient data for interpretation"

        interpretations = []

        if sales_trend == "decreasing":
            interpretations.append("Improving sales efficiency")
        elif sales_trend == "increasing":
            interpretations.append("Rising sales costs")

        if gross_vs_sales and gross_vs_sales > 0.20:
            interpretations.append("strong pricing power with healthy margin buffer")
        elif gross_vs_sales and gross_vs_sales > 0.10:
            interpretations.append("moderate pricing power")
        elif gross_vs_sales and gross_vs_sales < 0.05:
            interpretations.append("thin margin buffer, limited pricing power")

        return "; ".join(interpretations) if interpretations else "Neutral expense profile"

    def _analyze_cash_flow(
        self,
        cash_flows: List[Dict],
        income_statements: List[Dict],
        actual_periods: int
    ) -> Dict[str, Any]:
        """Analyze cash generation quality and liquidity"""

        if not cash_flows or not income_statements:
            return {"error": "Insufficient cash flow data"}

        ocf_series = []
        fcf_series = []
        ocf_to_ni_ratios = []
        cash_reserves_series = []

        for i in range(min(actual_periods, len(cash_flows), len(income_statements))):
            cf = cash_flows[i]
            inc = income_statements[i]

            # Operating cash flow
            ocf = cf.get('operatingCashFlow')
            ocf_series.append(ocf)

            # Free cash flow
            capex = cf.get('capitalExpenditure', 0)
            fcf = (ocf + capex) if ocf else None  # capex is negative
            fcf_series.append(fcf)

            # OCF to net income ratio (cash quality)
            net_income = inc.get('netIncome')
            ocf_to_ni = self._safe_divide(ocf, net_income)
            ocf_to_ni_ratios.append(ocf_to_ni)

            # Cash reserves
            cash = cf.get('cashAtEndOfPeriod') or cf.get('cashAtBeginningOfPeriod')
            cash_reserves_series.append(cash)

        # Calculate metrics
        ocf_metrics = self._calculate_ocf_metrics(ocf_series, actual_periods)
        fcf_metrics = self._calculate_fcf_metrics(fcf_series, income_statements[:actual_periods])
        cash_quality = self._calculate_cash_quality(ocf_to_ni_ratios)
        cash_reserves = self._calculate_cash_reserves(cash_reserves_series, actual_periods)

        return {
            "operating_cash_flow": ocf_metrics,
            "free_cash_flow": fcf_metrics,
            "cash_quality": cash_quality,
            "cash_reserves": cash_reserves
        }

    def _calculate_ocf_metrics(
        self,
        ocf_series: List[Optional[float]],
        periods: int
    ) -> Dict[str, Any]:
        """Calculate OCF growth and trends"""

        metrics = {}

        # OCF CAGR
        if len(ocf_series) >= 5:
            ocf_cagr = self._calculate_cagr(
                ocf_series[0], ocf_series[4], 4
            )
            metrics["ocf_cagr_5y"] = self._format_ratio(ocf_cagr)

        # OCF YoY
        if len(ocf_series) >= 2:
            ocf_yoy = self._calculate_growth_rate(
                ocf_series[0], ocf_series[1]
            )
            metrics["ocf_yoy"] = self._format_ratio(ocf_yoy)

        metrics["ocf_trend"] = self._calculate_trend(ocf_series)
        metrics["latest_ocf"] = self._format_large_number(ocf_series[0]) if ocf_series else None

        return metrics

    def _calculate_fcf_metrics(
        self,
        fcf_series: List[Optional[float]],
        income_statements: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate free cash flow metrics"""

        metrics = {}

        # FCF CAGR
        clean_fcf = [f for f in fcf_series if f is not None and f > 0]
        if len(clean_fcf) >= 5:
            fcf_cagr = self._calculate_cagr(clean_fcf[0], clean_fcf[4], 4)
            metrics["fcf_cagr_5y"] = self._format_ratio(fcf_cagr)

        # FCF margin
        if fcf_series and income_statements:
            latest_fcf = fcf_series[0]
            latest_revenue = income_statements[0].get('revenue')
            fcf_margin = self._safe_divide(latest_fcf, latest_revenue)
            metrics["fcf_margin_latest"] = self._format_ratio(fcf_margin)

        metrics["fcf_trend"] = self._calculate_trend(fcf_series)
        metrics["latest_fcf"] = self._format_large_number(fcf_series[0]) if fcf_series else None

        return metrics

    def _calculate_cash_quality(
        self,
        ocf_to_ni_ratios: List[Optional[float]]
    ) -> Dict[str, Any]:
        """Calculate cash earnings quality metrics"""

        clean_ratios = [r for r in ocf_to_ni_ratios if r is not None]

        if not clean_ratios:
            return {"error": "No valid OCF/NI ratios"}

        min_ratio = min(clean_ratios)
        min_idx = ocf_to_ni_ratios.index(min_ratio)

        interpretation = "Strong cash conversion" if clean_ratios[0] > 1.0 else "Weak cash conversion"
        if min_ratio < 0.8:
            interpretation += f" with concerning quality in period {min_idx}"

        return {
            "ocf_to_net_income_latest": self._format_ratio(clean_ratios[0]),
            "ocf_to_net_income_min": self._format_ratio(min_ratio),
            "min_ratio_period_index": min_idx,
            "interpretation": interpretation
        }

    def _calculate_cash_reserves(
        self,
        cash_series: List[Optional[float]],
        periods: int
    ) -> Dict[str, Any]:
        """Calculate cash reserve metrics"""

        metrics = {}

        # Cash CAGR
        clean_cash = [c for c in cash_series if c is not None and c > 0]
        if len(clean_cash) >= 5:
            cash_cagr = self._calculate_cagr(clean_cash[0], clean_cash[4], 4)
            metrics["cash_cagr_5y"] = self._format_ratio(cash_cagr)

        if clean_cash:
            stats = self._calculate_statistics(clean_cash)
            latest_cash = clean_cash[0]

            # Percentile classification
            if latest_cash >= stats.get('max', 0):
                percentile = "high"
            elif latest_cash >= stats.get('mean', 0):
                percentile = "medium_high"
            elif latest_cash >= stats.get('median', 0):
                percentile = "medium"
            else:
                percentile = "low"

            metrics["latest_cash"] = self._format_large_number(latest_cash)
            metrics["cash_percentile"] = percentile
            metrics["cash_statistics"] = stats

        return metrics

    def _analyze_debt_risk(
        self,
        balance_sheets: List[Dict],
        cash_flows: List[Dict],
        periods: List[str]
    ) -> Dict[str, Any]:
        """Analyze financial leverage and solvency"""

        if not balance_sheets:
            return {"error": "Insufficient balance sheet data"}

        leverage_metrics = self._calculate_leverage_metrics(balance_sheets)
        liquidity_metrics = self._calculate_liquidity_metrics(balance_sheets)
        coverage_analysis = self._calculate_coverage_analysis(
            balance_sheets, cash_flows, periods
        )

        risk_classification = self._classify_debt_risk(
            leverage_metrics, liquidity_metrics, coverage_analysis
        )

        return {
            "leverage_metrics": leverage_metrics,
            "liquidity": liquidity_metrics,
            "coverage_analysis": coverage_analysis,
            "risk_classification": risk_classification
        }

    def _calculate_leverage_metrics(
        self,
        balance_sheets: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate debt leverage ratios"""

        debt_to_equity_series = []
        debt_to_assets_series = []

        for bs in balance_sheets:
            total_debt = bs.get('totalDebt')
            total_equity = bs.get('totalEquity') or bs.get('totalStockholdersEquity')
            total_assets = bs.get('totalAssets')

            # Debt to equity
            d_to_e = self._safe_divide(total_debt, total_equity)
            debt_to_equity_series.append(d_to_e)

            # Debt to assets
            d_to_a = self._safe_divide(total_debt, total_assets)
            debt_to_assets_series.append(d_to_a)

        interpretation = "Conservative leverage" if (debt_to_equity_series[0] or 0) < 0.5 else "Moderate to high leverage"

        return {
            "debt_to_equity_latest": self._format_ratio(debt_to_equity_series[0]),
            "debt_to_equity_trend": self._calculate_trend(debt_to_equity_series),
            "debt_to_assets_latest": self._format_ratio(debt_to_assets_series[0]),
            "debt_to_assets_trend": self._calculate_trend(debt_to_assets_series),
            "interpretation": interpretation
        }

    def _calculate_liquidity_metrics(
        self,
        balance_sheets: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate liquidity ratios"""

        if not balance_sheets:
            return {}

        latest_bs = balance_sheets[0]

        cash = latest_bs.get('cashAndCashEquivalents', 0)
        total_assets = latest_bs.get('totalAssets')
        current_assets = latest_bs.get('totalCurrentAssets')
        current_liabilities = latest_bs.get('totalCurrentLiabilities')
        inventory = latest_bs.get('inventory', 0)

        cash_to_assets = self._safe_divide(cash, total_assets)
        current_ratio = self._safe_divide(current_assets, current_liabilities)
        quick_ratio = self._safe_divide(
            (current_assets - inventory) if current_assets else None,
            current_liabilities
        )

        assessment = "Strong liquidity" if (current_ratio or 0) > 1.5 else "Adequate liquidity"
        if (current_ratio or 0) < 1.0:
            assessment = "Weak liquidity - potential solvency concern"

        return {
            "cash_to_assets_latest": self._format_ratio(cash_to_assets),
            "current_ratio_latest": self._format_ratio(current_ratio),
            "quick_ratio_latest": self._format_ratio(quick_ratio),
            "assessment": assessment
        }

    def _calculate_coverage_analysis(
        self,
        balance_sheets: List[Dict],
        cash_flows: List[Dict],
        periods: List[str]
    ) -> Dict[str, Any]:
        """Analyze debt coverage and cash sufficiency"""

        coverage_status = []

        for i in range(min(len(balance_sheets), len(cash_flows), len(periods))):
            bs = balance_sheets[i]
            cf = cash_flows[i]

            cash = bs.get('cashAndCashEquivalents', 0)
            short_term_investments = bs.get('shortTermInvestments', 0)
            total_cash = cash + short_term_investments

            capex = cf.get('capitalExpenditure', 0)

            covers_capex = total_cash >= abs(capex) if capex else None
            coverage_status.append(covers_capex)

        # Calculate net cash position
        net_cash = None
        if balance_sheets:
            latest = balance_sheets[0]
            cash = latest.get('cashAndCashEquivalents', 0)
            short_term_inv = latest.get('shortTermInvestments', 0)
            total_debt = latest.get('totalDebt', 0)
            net_cash = (cash + short_term_inv) - total_debt

        return {
            "cash_covers_capex_latest": coverage_status[0] if coverage_status else None,
            "coverage_history": coverage_status,
            "periods": periods,
            "net_cash_position": self._format_large_number(net_cash)
        }

    def _classify_debt_risk(
        self,
        leverage: Dict[str, Any],
        liquidity: Dict[str, Any],
        coverage: Dict[str, Any]
    ) -> str:
        """Classify overall debt risk level"""

        d_to_e = leverage.get('debt_to_equity_latest', 0) or 0
        current_ratio = liquidity.get('current_ratio_latest', 0) or 0
        covers_capex = coverage.get('cash_covers_capex_latest', False)

        risk_score = 0

        # Leverage risk
        if d_to_e > 1.0:
            risk_score += 2
        elif d_to_e > 0.5:
            risk_score += 1

        # Liquidity risk
        if current_ratio < 1.0:
            risk_score += 2
        elif current_ratio < 1.5:
            risk_score += 1

        # Coverage risk
        if not covers_capex:
            risk_score += 1

        if risk_score >= 4:
            return "high_risk"
        elif risk_score >= 2:
            return "moderate_risk"
        else:
            return "low_risk"

    def _format_markdown_output(
        self,
        symbol: str,
        period_type: str,
        periods: List[str],
        latest_date: str,
        financial_attitude: Dict,
        income_analysis: Dict,
        cash_flow_analysis: Dict,
        debt_risk_analysis: Dict,
        detail_level: str,
        income_statements: List[Dict] = None,
        cash_flows: List[Dict] = None
    ) -> str:
        """Format analysis results as markdown"""
        from datetime import datetime, timezone as dt_timezone

        lines = []

        # Header
        timestamp_utc = datetime.now(dt_timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        lines.append(f"## Financial Analysis: {symbol}")
        lines.append(f"**Retrieved:** {timestamp_utc}")
        lines.append(f"**Period Type:** {period_type.capitalize()}")
        lines.append(f"**Periods Analyzed:** {', '.join(periods)}")
        lines.append(f"**Latest Period:** {latest_date}")
        lines.append("")

        # Financial Attitude
        lines.append("### Financial Attitude")
        lines.append("")
        if "error" not in financial_attitude:
            classification = financial_attitude.get('classification', 'N/A')
            interpretation = financial_attitude.get('interpretation', 'N/A')
            lines.append(f"**Classification:** {classification}")
            lines.append(f"**Interpretation:** {interpretation}")
            lines.append("")
        else:
            lines.append(f"*{financial_attitude.get('error')}*")
            lines.append("")

        # Income Statement Analysis
        lines.append("### Income Statement Analysis")
        lines.append("")
        if "error" not in income_analysis:
            # Revenue growth
            if 'revenue_growth' in income_analysis:
                rev_growth = income_analysis['revenue_growth']
                lines.append("**Revenue Growth:**")
                lines.append(f"- Latest: {rev_growth.get('latest_yoy', 'N/A')}")
                lines.append(f"- Average: {rev_growth.get('average_yoy', 'N/A')}")
                lines.append(f"- Trend: {rev_growth.get('trend', 'N/A')}")
                lines.append("")

            # Profit margins
            if 'profit_margins' in income_analysis:
                margins = income_analysis['profit_margins']
                lines.append("**Profit Margins:**")
                lines.append("")
                margin_rows = [
                    ("Gross Margin", margins.get('gross_margin_latest', 'N/A')),
                    ("Operating Margin", margins.get('operating_margin_latest', 'N/A')),
                    ("Net Margin", margins.get('net_margin_latest', 'N/A'))
                ]
                lines.append("| Metric | Latest |")
                lines.append("|--------|--------|")
                for metric, value in margin_rows:
                    lines.append(f"| {metric} | {value} |")
                lines.append("")
        else:
            lines.append(f"*{income_analysis.get('error')}*")
            lines.append("")

        # Cash Flow Analysis
        lines.append("### Cash Flow Analysis")
        lines.append("")
        if "error" not in cash_flow_analysis:
            # OCF quality
            if 'ocf_quality' in cash_flow_analysis:
                ocf = cash_flow_analysis['ocf_quality']
                lines.append("**Operating Cash Flow Quality:**")
                lines.append(f"- OCF/Net Income Ratio: {ocf.get('ocf_to_ni_ratio', 'N/A')}")
                lines.append(f"- Quality Assessment: {ocf.get('quality_assessment', 'N/A')}")
                lines.append("")

            # Free cash flow
            if 'free_cash_flow' in cash_flow_analysis:
                fcf = cash_flow_analysis['free_cash_flow']
                lines.append("**Free Cash Flow:**")
                lines.append(f"- Latest FCF: {fcf.get('latest_fcf', 'N/A')}")
                lines.append(f"- FCF Margin: {fcf.get('fcf_margin', 'N/A')}")
                lines.append("")
        else:
            lines.append(f"*{cash_flow_analysis.get('error')}*")
            lines.append("")

        # Debt Risk Analysis
        lines.append("### Debt Risk Analysis")
        lines.append("")
        if "error" not in debt_risk_analysis:
            risk_class = debt_risk_analysis.get('risk_classification', 'N/A')
            lines.append(f"**Risk Classification:** {risk_class}")
            lines.append("")

            # Leverage ratios table
            if 'leverage_ratios' in debt_risk_analysis:
                ratios = debt_risk_analysis['leverage_ratios']
                lines.append("**Leverage Ratios:**")
                lines.append("")
                lines.append("| Metric | Value |")
                lines.append("|--------|-------|")
                if 'debt_to_equity' in ratios:
                    lines.append(f"| Debt/Equity | {ratios['debt_to_equity']} |")
                if 'debt_to_assets' in ratios:
                    lines.append(f"| Debt/Assets | {ratios['debt_to_assets']} |")
                lines.append("")
        else:
            lines.append(f"*{debt_risk_analysis.get('error')}*")
            lines.append("")

        return "\n".join(lines)

    def _calculate_extended_statistics(
        self,
        income_statements: List[Dict],
        cash_flows: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate additional statistical metrics for extended mode"""

        # Revenue volatility
        revenues = [stmt.get('revenue') for stmt in income_statements]
        revenue_stats = self._calculate_statistics([r for r in revenues if r])

        # Earnings volatility
        earnings = [stmt.get('netIncome') for stmt in income_statements]
        earnings_stats = self._calculate_statistics([e for e in earnings if e])

        # Positive earnings periods
        positive_periods = sum(1 for e in earnings if e and e > 0)

        return {
            "revenue_statistics": revenue_stats,
            "earnings_statistics": earnings_stats,
            "positive_earnings_periods": positive_periods,
            "total_periods": len(income_statements)
        }


# Core implementation - Tool decorator moved to src/tools/us/tool.py
async def analyze_financials_fmp_impl(
    symbol: str,
    period_type: str = "annual",
    lookback_periods: int = 5,
    detail_level: str = "compact"
) -> str:
    """
    Comprehensive financial statement analysis using FMP API (async).

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
        Markdown-formatted string with comprehensive financial analysis
    """
    async with FMPClient() as fmp_client:
        analyzer = FinancialsAnalysisTool(fmp_client)
        return await analyzer.analyze(symbol, period_type, lookback_periods, detail_level)


if __name__ == "__main__":
    # Test with various cases
    import json

    print("=" * 80)
    print("Testing US Financial Analysis Tool")
    print("=" * 80)

    # Test 1: Young company (CRWV) - edge case
    print("\n1. CRWV (Young Company - Edge Case)")
    print("-" * 80)
    result = analyze_financials_fmp_impl(
        symbol="CRWV",
        period_type="annual",
        lookback_periods=5
    )
    print(json.dumps(result, indent=2))

    # Test 2: Established company (AAPL)
    print("\n\n2. AAPL (Established Company)")
    print("-" * 80)
    result = analyze_financials_fmp_impl(
        symbol="AAPL",
        period_type="annual",
        lookback_periods=5
    )
    print(f"Periods analyzed: {result.get('periods_analyzed')}")
    print(f"Classification: {result.get('financial_attitude', {}).get('classification')}")
    print(f"Risk level: {result.get('debt_risk', {}).get('risk_classification')}")
