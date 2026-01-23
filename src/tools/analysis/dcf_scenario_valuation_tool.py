"""
DCF Scenario Valuation Tool

Custom DCF valuation with 3 scenarios (Low/Neutral/High) using:
- FMP Custom DCF Advanced API
- Analyst consensus estimates
- Historical financial patterns
"""

from typing import Dict, List, Optional, Any, Tuple
import logging
import statistics
import asyncio

from src.tools.analysis.base import FundamentalAnalysisBase
from src.data_client.fmp import FMPClient

logger = logging.getLogger(__name__)


class DCFScenarioValuationTool(FundamentalAnalysisBase):
    """Multi-scenario DCF valuation with customizable assumptions"""

    # Default assumptions
    DEFAULT_RISK_FREE_RATE = 0.045  # 4.5% (10Y Treasury)
    DEFAULT_MARKET_RISK_PREMIUM = 0.07  # 7% historical equity premium
    DEFAULT_TERMINAL_GROWTH_LOW = 0.02  # 2% for conservative
    DEFAULT_TERMINAL_GROWTH_MID = 0.025  # 2.5% for neutral
    DEFAULT_TERMINAL_GROWTH_HIGH = 0.03  # 3% for optimistic

    async def analyze(
        self,
        symbol: str,
        use_analyst_estimates: bool = True,
        period_type: str = "annual",
        lookback_periods: int = 5,
        detail_level: str = "compact"
    ) -> str:
        """
        Perform multi-scenario DCF valuation (async)

        Args:
            symbol: Stock ticker symbol
            use_analyst_estimates: Use analyst projections (default True)
            period_type: 'annual' or 'quarterly'
            lookback_periods: Historical periods for base assumptions
            detail_level: 'compact' or 'extended' (adds sensitivity)

        Returns:
            Markdown-formatted string with 3 DCF scenarios and valuation range
        """
        try:
            # Fetch all required data
            data = await self._fetch_all_data(symbol, period_type, lookback_periods, use_analyst_estimates)

            if "error" in data:
                from datetime import datetime, timezone as dt_timezone
                timestamp_utc = datetime.now(dt_timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
                return f"""## DCF Scenario Valuation: {symbol}
**Status:** Error

**Retrieved:** {timestamp_utc}

{data.get('error', 'Unknown error occurred')}"""

            # Calculate base assumptions from historical data
            base_assumptions = self._calculate_base_assumptions(
                data["income_statements"],
                data["balance_sheets"],
                data["cash_flows"]
            )

            # Calculate WACC components
            wacc_components = self._calculate_wacc_components(
                data["profile"],
                data["balance_sheets"],
                data["income_statements"]
            )

            # Generate 3 scenarios
            scenarios = {}

            scenarios["low"] = self._generate_low_scenario(
                base_assumptions,
                wacc_components,
                data.get("analyst_estimates")
            )

            scenarios["neutral"] = self._generate_neutral_scenario(
                base_assumptions,
                wacc_components,
                data.get("analyst_estimates")
            )

            scenarios["high"] = self._generate_high_scenario(
                base_assumptions,
                wacc_components,
                data.get("analyst_estimates")
            )

            # Run custom DCF for all 3 scenarios in parallel
            scenario_names = list(scenarios.keys())
            dcf_tasks = [self._run_custom_dcf(symbol, scenarios[name]) for name in scenario_names]
            dcf_values = await asyncio.gather(*dcf_tasks)
            dcf_results = dict(zip(scenario_names, dcf_values))

            # Format as markdown
            markdown_output = self._format_markdown_output(
                symbol=symbol,
                current_price=data["profile"].get("price"),
                analysis_date=data["income_statements"][0].get("date") if data["income_statements"] else None,
                dcf_results=dcf_results,
                scenarios_assumptions=scenarios,
                base_assumptions=base_assumptions,
                wacc_components=wacc_components,
                use_analyst_estimates=use_analyst_estimates,
                analyst_estimates=data.get("analyst_estimates"),
                detail_level=detail_level
            )

            return markdown_output

        except Exception as e:
            logger.error(f"Error in DCF scenario analysis for {symbol}: {e}", exc_info=True)
            from datetime import datetime, timezone as dt_timezone
            timestamp_utc = datetime.now(dt_timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            return f"""## DCF Scenario Valuation: {symbol}
**Status:** Error

**Retrieved:** {timestamp_utc}
**Error Type:** {type(e).__name__}

{str(e)}"""

    async def _fetch_all_data(
        self,
        symbol: str,
        period_type: str,
        lookback_periods: int,
        use_analyst_estimates: bool
    ) -> Dict[str, Any]:
        """Fetch all required data for DCF analysis (async with parallel requests)"""

        # Fetch all data in parallel using asyncio.gather for HTTP/2 multiplexing
        tasks = [
            self.client.get_income_statement(symbol, period=period_type, limit=lookback_periods),
            self.client.get_balance_sheet(symbol, period=period_type, limit=lookback_periods),
            self.client.get_cash_flow(symbol, period=period_type, limit=lookback_periods),
            self.client.get_profile(symbol),
        ]

        # Add analyst estimates task if requested
        if use_analyst_estimates:
            tasks.append(self.client.get_analyst_estimates(symbol, period=period_type, limit=3))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Unpack results
        income_statements = results[0] if not isinstance(results[0], Exception) else []
        balance_sheets = results[1] if not isinstance(results[1], Exception) else []
        cash_flows = results[2] if not isinstance(results[2], Exception) else []
        profile = results[3] if not isinstance(results[3], Exception) else []

        # Handle analyst estimates
        analyst_estimates = None
        if use_analyst_estimates and len(results) > 4:
            if isinstance(results[4], Exception):
                logger.warning(f"Could not fetch analyst estimates: {results[4]}")
            else:
                analyst_estimates = results[4]

        if not income_statements or not balance_sheets or not cash_flows or not profile:
            return {
                "error": "Insufficient financial data available",
                "symbol": symbol
            }

        profile_data = profile[0] if profile else {}

        return {
            "income_statements": income_statements,
            "balance_sheets": balance_sheets,
            "cash_flows": cash_flows,
            "profile": profile_data,
            "analyst_estimates": analyst_estimates
        }

    def _calculate_base_assumptions(
        self,
        income_statements: List[Dict],
        balance_sheets: List[Dict],
        cash_flows: List[Dict]
    ) -> Dict[str, float]:
        """Calculate baseline assumptions from historical financials"""

        assumptions = {}

        # Revenue growth (5-year CAGR)
        if len(income_statements) >= 5:
            latest_revenue = income_statements[0].get("revenue")
            oldest_revenue = income_statements[4].get("revenue")
            revenue_cagr = self._calculate_cagr(latest_revenue, oldest_revenue, 4)
            assumptions["revenue_growth_hist"] = revenue_cagr if revenue_cagr else 0.05
        else:
            assumptions["revenue_growth_hist"] = 0.05

        # Margins (5-year averages)
        ebitda_margins = []
        ebit_margins = []
        net_margins = []

        for stmt in income_statements:
            revenue = stmt.get("revenue")
            if revenue:
                ebitda = stmt.get("ebitda")
                if ebitda:
                    ebitda_margins.append(ebitda / revenue)

                ebit = stmt.get("ebit") or stmt.get("operatingIncome")
                if ebit:
                    ebit_margins.append(ebit / revenue)

                net_income = stmt.get("netIncome")
                if net_income:
                    net_margins.append(net_income / revenue)

        assumptions["ebitda_margin_hist"] = statistics.mean(ebitda_margins) if ebitda_margins else 0.15
        assumptions["ebit_margin_hist"] = statistics.mean(ebit_margins) if ebit_margins else 0.10
        assumptions["net_margin_hist"] = statistics.mean(net_margins) if net_margins else 0.08

        # D&A as % of revenue
        da_ratios = []
        for cf, inc in zip(cash_flows, income_statements):
            da = cf.get("depreciationAndAmortization")
            revenue = inc.get("revenue")
            if da and revenue:
                da_ratios.append(da / revenue)

        assumptions["da_pct_hist"] = statistics.mean(da_ratios) if da_ratios else 0.03

        # Working capital ratios
        cash_ratios = []
        receivables_ratios = []
        inventory_ratios = []
        payables_ratios = []

        for bs, inc in zip(balance_sheets, income_statements):
            revenue = inc.get("revenue")
            if revenue:
                cash = bs.get("cashAndCashEquivalents", 0) + bs.get("shortTermInvestments", 0)
                cash_ratios.append(cash / revenue)

                receivables = bs.get("netReceivables", 0)
                receivables_ratios.append(receivables / revenue)

                inventory = bs.get("inventory", 0)
                inventory_ratios.append(inventory / revenue)

                payables = bs.get("accountPayables", 0)
                payables_ratios.append(payables / revenue)

        assumptions["cash_pct_hist"] = statistics.mean(cash_ratios) if cash_ratios else 0.10
        assumptions["receivables_pct_hist"] = statistics.mean(receivables_ratios) if receivables_ratios else 0.12
        assumptions["inventory_pct_hist"] = statistics.mean(inventory_ratios) if inventory_ratios else 0.10
        assumptions["payables_pct_hist"] = statistics.mean(payables_ratios) if payables_ratios else 0.08

        # Capex as % of revenue
        capex_ratios = []
        ocf_ratios = []
        sga_ratios = []

        for cf, inc in zip(cash_flows, income_statements):
            revenue = inc.get("revenue")
            if revenue:
                capex = cf.get("capitalExpenditure", 0)
                capex_ratios.append(abs(capex) / revenue)

                ocf = cf.get("operatingCashFlow", 0)
                ocf_ratios.append(ocf / revenue)

            sga = inc.get("sellingGeneralAndAdministrativeExpenses", 0)
            if revenue and sga:
                sga_ratios.append(sga / revenue)

        assumptions["capex_pct_hist"] = statistics.mean(capex_ratios) if capex_ratios else 0.05
        assumptions["ocf_pct_hist"] = statistics.mean(ocf_ratios) if ocf_ratios else 0.15
        assumptions["sga_pct_hist"] = statistics.mean(sga_ratios) if sga_ratios else 0.20

        # Tax rate
        tax_rates = []
        for inc in income_statements:
            income_before_tax = inc.get("incomeBeforeTax")
            tax_provision = inc.get("incomeTaxExpense")
            if income_before_tax and tax_provision and income_before_tax > 0:
                tax_rates.append(tax_provision / income_before_tax)

        assumptions["tax_rate_hist"] = statistics.mean(tax_rates) if tax_rates else 0.21

        return assumptions

    def _calculate_wacc_components(
        self,
        profile: Dict,
        balance_sheets: List[Dict],
        income_statements: List[Dict]
    ) -> Dict[str, float]:
        """Calculate WACC components using CAPM"""

        components = {}

        # Beta from profile
        beta = profile.get("beta", 1.0)
        components["beta"] = beta

        # Risk-free rate (can be fetched from treasury data)
        components["risk_free_rate"] = self.DEFAULT_RISK_FREE_RATE

        # Market risk premium
        components["market_risk_premium"] = self.DEFAULT_MARKET_RISK_PREMIUM

        # Cost of equity (CAPM)
        cost_of_equity = components["risk_free_rate"] + (beta * components["market_risk_premium"])
        components["cost_of_equity"] = cost_of_equity

        # Cost of debt
        latest_bs = balance_sheets[0] if balance_sheets else {}
        latest_inc = income_statements[0] if income_statements else {}

        total_debt = latest_bs.get("totalDebt", 0)
        interest_expense = latest_inc.get("interestExpense", 0)

        if total_debt > 0 and interest_expense:
            cost_of_debt = abs(interest_expense) / total_debt
        else:
            cost_of_debt = 0.04  # Default 4%

        components["cost_of_debt"] = cost_of_debt

        # Calculate WACC
        total_equity = profile.get("mktCap", 0)
        if total_equity > 0:
            total_capital = total_equity + total_debt
            equity_weight = total_equity / total_capital
            debt_weight = total_debt / total_capital

            # After-tax cost of debt
            tax_rate = 0.21  # Default
            wacc = (equity_weight * cost_of_equity) + (debt_weight * cost_of_debt * (1 - tax_rate))
            components["wacc"] = wacc
        else:
            components["wacc"] = cost_of_equity  # If no debt info, use cost of equity

        return components

    def _generate_low_scenario(
        self,
        base_assumptions: Dict,
        wacc_components: Dict,
        analyst_estimates: Optional[List[Dict]]
    ) -> Dict[str, float]:
        """Generate conservative (low) scenario assumptions"""

        scenario = {}

        # Growth - conservative
        hist_growth = base_assumptions.get("revenue_growth_hist", 0.05)

        if analyst_estimates and len(analyst_estimates) > 0:
            analyst_low_rev = analyst_estimates[0].get("estimatedRevenueLow")
            analyst_avg_rev = analyst_estimates[0].get("estimatedRevenueAvg")
            if analyst_low_rev and analyst_avg_rev and analyst_avg_rev > 0:
                analyst_low_growth = (analyst_low_rev / analyst_avg_rev) - 1
                revenue_growth = min(analyst_low_growth, hist_growth - 0.02)
            else:
                revenue_growth = hist_growth - 0.02
        else:
            revenue_growth = hist_growth - 0.02

        scenario["revenueGrowthPct"] = max(revenue_growth, 0.0)  # No negative growth

        # Margins - conservative (95% of historical)
        scenario["ebitdaPct"] = base_assumptions.get("ebitda_margin_hist", 0.15) * 0.95
        scenario["ebitPct"] = base_assumptions.get("ebit_margin_hist", 0.10) * 0.95

        # Working capital - conservative (higher receivables/inventory)
        scenario["cashAndShortTermInvestmentsPct"] = base_assumptions.get("cash_pct_hist", 0.10)
        scenario["receivablesPct"] = base_assumptions.get("receivables_pct_hist", 0.12) * 1.05
        scenario["inventoriesPct"] = base_assumptions.get("inventory_pct_hist", 0.10) * 1.05
        scenario["payablePct"] = base_assumptions.get("payables_pct_hist", 0.08) * 0.95

        # Operating metrics
        scenario["depreciationAndAmortizationPct"] = base_assumptions.get("da_pct_hist", 0.03)
        scenario["capitalExpenditurePct"] = base_assumptions.get("capex_pct_hist", 0.05) * 1.10  # Higher capex
        scenario["operatingCashFlowPct"] = base_assumptions.get("ocf_pct_hist", 0.15) * 0.95
        scenario["sellingGeneralAndAdministrativeExpensesPct"] = base_assumptions.get("sga_pct_hist", 0.20)

        # Tax rate
        scenario["taxRate"] = base_assumptions.get("tax_rate_hist", 0.21)

        # Terminal growth - conservative
        scenario["longTermGrowthRate"] = self.DEFAULT_TERMINAL_GROWTH_LOW * 100  # API expects percentage

        # WACC - higher (more risk)
        scenario["costOfDebt"] = wacc_components.get("cost_of_debt", 0.04) * 100 + 1.0  # +100bps
        scenario["costOfEquity"] = wacc_components.get("cost_of_equity", 0.10) * 100 + 1.5  # +150bps
        scenario["marketRiskPremium"] = wacc_components.get("market_risk_premium", 0.07) * 100
        scenario["beta"] = wacc_components.get("beta", 1.0)
        scenario["riskFreeRate"] = wacc_components.get("risk_free_rate", 0.045) * 100

        return scenario

    def _generate_neutral_scenario(
        self,
        base_assumptions: Dict,
        wacc_components: Dict,
        analyst_estimates: Optional[List[Dict]]
    ) -> Dict[str, float]:
        """Generate base case (neutral) scenario assumptions"""

        scenario = {}

        # Growth - base case
        hist_growth = base_assumptions.get("revenue_growth_hist", 0.05)

        if analyst_estimates and len(analyst_estimates) > 0:
            analyst_avg_rev = analyst_estimates[0].get("estimatedRevenueAvg")
            current_rev = analyst_estimates[0].get("revenue")  # May not be present
            if analyst_avg_rev:
                # Use analyst average if available
                revenue_growth = hist_growth  # Placeholder, would calculate from estimate
            else:
                revenue_growth = hist_growth
        else:
            revenue_growth = hist_growth

        scenario["revenueGrowthPct"] = revenue_growth

        # Margins - historical averages
        scenario["ebitdaPct"] = base_assumptions.get("ebitda_margin_hist", 0.15)
        scenario["ebitPct"] = base_assumptions.get("ebit_margin_hist", 0.10)

        # Working capital - historical
        scenario["cashAndShortTermInvestmentsPct"] = base_assumptions.get("cash_pct_hist", 0.10)
        scenario["receivablesPct"] = base_assumptions.get("receivables_pct_hist", 0.12)
        scenario["inventoriesPct"] = base_assumptions.get("inventory_pct_hist", 0.10)
        scenario["payablePct"] = base_assumptions.get("payables_pct_hist", 0.08)

        # Operating metrics
        scenario["depreciationAndAmortizationPct"] = base_assumptions.get("da_pct_hist", 0.03)
        scenario["capitalExpenditurePct"] = base_assumptions.get("capex_pct_hist", 0.05)
        scenario["operatingCashFlowPct"] = base_assumptions.get("ocf_pct_hist", 0.15)
        scenario["sellingGeneralAndAdministrativeExpensesPct"] = base_assumptions.get("sga_pct_hist", 0.20)

        # Tax rate
        scenario["taxRate"] = base_assumptions.get("tax_rate_hist", 0.21)

        # Terminal growth - base case
        scenario["longTermGrowthRate"] = self.DEFAULT_TERMINAL_GROWTH_MID * 100

        # WACC - market rates
        scenario["costOfDebt"] = wacc_components.get("cost_of_debt", 0.04) * 100
        scenario["costOfEquity"] = wacc_components.get("cost_of_equity", 0.10) * 100
        scenario["marketRiskPremium"] = wacc_components.get("market_risk_premium", 0.07) * 100
        scenario["beta"] = wacc_components.get("beta", 1.0)
        scenario["riskFreeRate"] = wacc_components.get("risk_free_rate", 0.045) * 100

        return scenario

    def _generate_high_scenario(
        self,
        base_assumptions: Dict,
        wacc_components: Dict,
        analyst_estimates: Optional[List[Dict]]
    ) -> Dict[str, float]:
        """Generate optimistic (high) scenario assumptions"""

        scenario = {}

        # Growth - optimistic
        hist_growth = base_assumptions.get("revenue_growth_hist", 0.05)

        if analyst_estimates and len(analyst_estimates) > 0:
            analyst_high_rev = analyst_estimates[0].get("estimatedRevenueHigh")
            analyst_avg_rev = analyst_estimates[0].get("estimatedRevenueAvg")
            if analyst_high_rev and analyst_avg_rev and analyst_avg_rev > 0:
                analyst_high_growth = (analyst_high_rev / analyst_avg_rev) - 1
                revenue_growth = max(analyst_high_growth, hist_growth + 0.03)
            else:
                revenue_growth = hist_growth + 0.03
        else:
            revenue_growth = hist_growth + 0.03

        scenario["revenueGrowthPct"] = revenue_growth

        # Margins - optimistic (105% of historical)
        scenario["ebitdaPct"] = base_assumptions.get("ebitda_margin_hist", 0.15) * 1.05
        scenario["ebitPct"] = base_assumptions.get("ebit_margin_hist", 0.10) * 1.05

        # Working capital - efficient (lower receivables/inventory)
        scenario["cashAndShortTermInvestmentsPct"] = base_assumptions.get("cash_pct_hist", 0.10)
        scenario["receivablesPct"] = base_assumptions.get("receivables_pct_hist", 0.12) * 0.95
        scenario["inventoriesPct"] = base_assumptions.get("inventory_pct_hist", 0.10) * 0.95
        scenario["payablePct"] = base_assumptions.get("payables_pct_hist", 0.08) * 1.05

        # Operating metrics
        scenario["depreciationAndAmortizationPct"] = base_assumptions.get("da_pct_hist", 0.03)
        scenario["capitalExpenditurePct"] = base_assumptions.get("capex_pct_hist", 0.05) * 0.90  # Lower capex
        scenario["operatingCashFlowPct"] = base_assumptions.get("ocf_pct_hist", 0.15) * 1.05
        scenario["sellingGeneralAndAdministrativeExpensesPct"] = base_assumptions.get("sga_pct_hist", 0.20)

        # Tax rate
        scenario["taxRate"] = base_assumptions.get("tax_rate_hist", 0.21)

        # Terminal growth - optimistic
        scenario["longTermGrowthRate"] = self.DEFAULT_TERMINAL_GROWTH_HIGH * 100

        # WACC - lower (less risk)
        scenario["costOfDebt"] = max(wacc_components.get("cost_of_debt", 0.04) * 100 - 1.0, 2.0)  # -100bps, min 2%
        scenario["costOfEquity"] = wacc_components.get("cost_of_equity", 0.10) * 100 - 1.5  # -150bps
        scenario["marketRiskPremium"] = wacc_components.get("market_risk_premium", 0.07) * 100
        scenario["beta"] = wacc_components.get("beta", 1.0)
        scenario["riskFreeRate"] = wacc_components.get("risk_free_rate", 0.045) * 100

        return scenario

    async def _run_custom_dcf(self, symbol: str, assumptions: Dict) -> Dict:
        """Run custom DCF with given assumptions (async)"""

        # Map camelCase to snake_case for FMP client method
        param_mapping = {
            "revenueGrowthPct": "revenue_growth_pct",
            "ebitdaPct": "ebitda_pct",
            "depreciationAndAmortizationPct": "depreciation_and_amortization_pct",
            "cashAndShortTermInvestmentsPct": "cash_and_short_term_investments_pct",
            "receivablesPct": "receivables_pct",
            "inventoriesPct": "inventories_pct",
            "payablePct": "payable_pct",
            "ebitPct": "ebit_pct",
            "capitalExpenditurePct": "capital_expenditure_pct",
            "operatingCashFlowPct": "operating_cash_flow_pct",
            "sellingGeneralAndAdministrativeExpensesPct": "selling_general_and_administrative_expenses_pct",
            "taxRate": "tax_rate",
            "longTermGrowthRate": "long_term_growth_rate",
            "costOfDebt": "cost_of_debt",
            "costOfEquity": "cost_of_equity",
            "marketRiskPremium": "market_risk_premium",
            "beta": "beta",
            "riskFreeRate": "risk_free_rate"
        }

        # Convert assumptions to snake_case parameters
        params = {}
        for camel_key, snake_key in param_mapping.items():
            if camel_key in assumptions:
                params[snake_key] = assumptions[camel_key]

        try:
            result = await self.client.get_custom_dcf(
                symbol=symbol,
                **params
            )
            return result[0] if result and len(result) > 0 else {"error": "No DCF result"}
        except Exception as e:
            logger.error(f"Error running custom DCF: {e}")
            return {"error": str(e)}

    def _format_scenario_result(
        self,
        dcf_result: Dict,
        assumptions: Dict,
        current_price: Optional[float]
    ) -> Dict:
        """Format scenario result with DCF value and assumptions"""

        # FMP Custom DCF returns equityValuePerShare as the DCF value
        dcf_value = dcf_result.get("equityValuePerShare")

        result = {
            "dcf_value": self._format_decimal(dcf_value, 2) if dcf_value else None,
            "vs_current_price": self._format_ratio(
                (dcf_value - current_price) / current_price if dcf_value and current_price else None
            )
        }

        # Add key assumptions
        result["assumptions"] = {
            "revenue_growth": self._format_ratio(assumptions.get("revenueGrowthPct")),
            "ebitda_margin": self._format_ratio(assumptions.get("ebitdaPct")),
            "terminal_growth": self._format_ratio(assumptions.get("longTermGrowthRate") / 100 if assumptions.get("longTermGrowthRate") else None),
            "cost_of_equity": self._format_ratio(assumptions.get("costOfEquity") / 100 if assumptions.get("costOfEquity") else None),
            "cost_of_debt": self._format_ratio(assumptions.get("costOfDebt") / 100 if assumptions.get("costOfDebt") else None)
        }

        # Add error if present
        if "error" in dcf_result:
            result["error"] = dcf_result["error"]

        return result

    def _create_valuation_summary(
        self,
        dcf_results: Dict[str, Dict],
        current_price: Optional[float]
    ) -> Dict:
        """Create valuation summary across scenarios"""

        dcf_values = []
        for scenario_name, result in dcf_results.items():
            dcf_value = result.get("equityValuePerShare")
            if dcf_value:
                dcf_values.append(dcf_value)

        if not dcf_values:
            return {"error": "No valid DCF values"}

        summary = {
            "dcf_range": {
                "min": self._format_decimal(min(dcf_values), 2),
                "median": self._format_decimal(statistics.median(dcf_values), 2),
                "max": self._format_decimal(max(dcf_values), 2)
            }
        }

        # Margin of safety
        if current_price:
            summary["margin_of_safety"] = {
                "vs_low": self._format_ratio((min(dcf_values) - current_price) / current_price),
                "vs_neutral": self._format_ratio((statistics.median(dcf_values) - current_price) / current_price),
                "vs_high": self._format_ratio((max(dcf_values) - current_price) / current_price)
            }

            # Simple probability-weighted value (equal weights)
            avg_dcf = statistics.mean(dcf_values)
            summary["probability_weighted_value"] = self._format_decimal(avg_dcf, 2)
            summary["probability_weighted_upside"] = self._format_ratio((avg_dcf - current_price) / current_price)

        return summary

    def _format_analyst_estimates(self, analyst_estimates: List[Dict]) -> Dict:
        """Format analyst estimates for output"""

        if not analyst_estimates or len(analyst_estimates) == 0:
            return {}

        latest = analyst_estimates[0]

        return {
            "date": latest.get("date"),
            "revenue": {
                "low": self._format_large_number(latest.get("estimatedRevenueLow")),
                "avg": self._format_large_number(latest.get("estimatedRevenueAvg")),
                "high": self._format_large_number(latest.get("estimatedRevenueHigh"))
            },
            "ebitda": {
                "low": self._format_large_number(latest.get("estimatedEbitdaLow")),
                "avg": self._format_large_number(latest.get("estimatedEbitdaAvg")),
                "high": self._format_large_number(latest.get("estimatedEbitdaHigh"))
            },
            "eps": {
                "low": self._format_decimal(latest.get("estimatedEpsLow"), 2),
                "avg": self._format_decimal(latest.get("estimatedEpsAvg"), 2),
                "high": self._format_decimal(latest.get("estimatedEpsHigh"), 2)
            }
        }

    def _format_percentage_display(self, value: Optional[float]) -> str:
        """Format percentage for display (e.g., 0.658 -> '-65.8%')"""
        if value is None:
            return "N/A"
        return f"{value:+.1%}"

    def _format_currency_display(self, value: Optional[float]) -> str:
        """Format currency for display (e.g., 247.93 -> '$247.93')"""
        if value is None:
            return "N/A"
        return f"${value:,.2f}"

    def _explain_metric(self, metric_name: str, value: Optional[float]) -> str:
        """Provide plain English explanation of what a metric means"""

        if value is None:
            return ""

        # Format without the +/- sign for display in explanations
        pct_str = f"{value:.1%}"

        explanations = {
            "revenue_growth": f"Annual revenue growth rate. {pct_str} means revenue {'increases' if value >= 0 else 'decreases'} by {abs(value)*100:.1f}% each year.",

            "ebitda_margin": f"Operating profitability: {pct_str} means the company generates ${abs(value)*100:.2f} in operating profit (before interest, taxes, depreciation, and amortization) for every $100 of revenue.",

            "terminal_growth": f"Perpetual growth rate after the forecast period. {pct_str} represents the assumed long-term growth rate, typically aligned with GDP growth expectations.",

            "cost_of_equity": f"Required return for equity investors. {pct_str} represents the annual return investors require to invest in this stock, calculated using the Capital Asset Pricing Model (CAPM).",

            "cost_of_debt": f"The interest rate the company pays on its debt obligations. {pct_str} is the effective borrowing cost."
        }

        return explanations.get(metric_name, "")

    def _explain_scenario_context(self, scenario_name: str, base_assumptions: Dict) -> str:
        """Explain what each scenario assumes"""

        hist_growth = base_assumptions.get("revenue_growth_hist", 0.05)
        hist_margin = base_assumptions.get("ebitda_margin_hist", 0.15)

        contexts = {
            "conservative": f"This scenario uses below-historical assumptions: {max(hist_growth - 0.02, 0):.1%} revenue growth (vs historical {hist_growth:.1%}), margins at 95% of historical average, and elevated discount rates (+100 basis points on cost of debt, +150 on cost of equity).",

            "base_case": f"This scenario uses historical 5-year averages: {hist_growth:.1%} revenue growth and {hist_margin:.1%} EBITDA margin, with discount rates calculated from current market conditions and the company's capital structure.",

            "optimistic": f"This scenario assumes above-historical performance: {hist_growth + 0.03:.1%} revenue growth (+3 percentage points above historical), margin expansion to 105% of historical average, and lower discount rates reflecting reduced risk (-100 basis points on cost of debt, -150 on cost of equity)."
        }

        return contexts.get(scenario_name, "")

    def _create_human_friendly_output(
        self,
        symbol: str,
        current_price: float,
        analysis_date: str,
        dcf_results: Dict[str, Dict],
        scenarios_assumptions: Dict[str, Dict],
        base_assumptions: Dict,
        wacc_components: Dict
    ) -> Dict[str, Any]:
        """Create human-friendly restructured output with explanations"""

        # Extract DCF values
        conservative_val = dcf_results["low"].get("equityValuePerShare")
        base_val = dcf_results["neutral"].get("equityValuePerShare")
        optimistic_val = dcf_results["high"].get("equityValuePerShare")

        # Helper to calculate difference from current
        def calc_diff(dcf_val):
            if dcf_val and current_price:
                return (dcf_val - current_price) / current_price
            return None

        result = {
            "summary": {
                "company": symbol,
                "current_price": self._format_currency_display(current_price),
                "analysis_date": analysis_date,
                "valuation_range": {
                    "conservative_estimate": self._format_currency_display(conservative_val),
                    "base_case_estimate": self._format_currency_display(base_val),
                    "optimistic_estimate": self._format_currency_display(optimistic_val),
                    "range_explanation": "This represents the estimated fair value per share under three different sets of assumptions about the company's future performance."
                }
            },

            "scenarios_explained": {}
        }

        # Build detailed scenario explanations
        scenario_configs = {
            "conservative": {
                "fair_value_estimate": self._format_currency_display(conservative_val),
                "difference_from_current": self._format_percentage_display(calc_diff(conservative_val)),
                "assumptions": scenarios_assumptions["low"]
            },
            "base_case": {
                "fair_value_estimate": self._format_currency_display(base_val),
                "difference_from_current": self._format_percentage_display(calc_diff(base_val)),
                "assumptions": scenarios_assumptions["neutral"]
            },
            "optimistic": {
                "fair_value_estimate": self._format_currency_display(optimistic_val),
                "difference_from_current": self._format_percentage_display(calc_diff(optimistic_val)),
                "assumptions": scenarios_assumptions["high"]
            }
        }

        for scenario_key, config in scenario_configs.items():
            assumptions = config["assumptions"]
            fair_value = config["fair_value_estimate"]
            diff = config["difference_from_current"]

            # Extract key assumptions with percentages converted
            rev_growth = assumptions.get("revenueGrowthPct")
            ebitda_margin = assumptions.get("ebitdaPct")
            terminal_growth = assumptions.get("longTermGrowthRate") / 100 if assumptions.get("longTermGrowthRate") else None
            cost_of_equity = assumptions.get("costOfEquity") / 100 if assumptions.get("costOfEquity") else None
            cost_of_debt = assumptions.get("costOfDebt") / 100 if assumptions.get("costOfDebt") else None

            result["scenarios_explained"][scenario_key] = {
                "fair_value_estimate": fair_value,
                "difference_from_current": diff,
                "what_this_means": f"Under {scenario_key} assumptions, the estimated fair value is {fair_value} per share, which is {diff} from the current market price of {self._format_currency_display(current_price)}.",

                "key_assumptions_explained": {
                    "revenue_growth": {
                        "value": f"{rev_growth:.1%}" if rev_growth is not None else "N/A",
                        "explanation": self._explain_metric("revenue_growth", rev_growth)
                    },
                    "ebitda_margin": {
                        "value": f"{ebitda_margin:.1%}" if ebitda_margin is not None else "N/A",
                        "explanation": self._explain_metric("ebitda_margin", ebitda_margin)
                    },
                    "terminal_growth": {
                        "value": f"{terminal_growth:.1%}" if terminal_growth is not None else "N/A",
                        "explanation": self._explain_metric("terminal_growth", terminal_growth)
                    },
                    "cost_of_equity": {
                        "value": f"{cost_of_equity:.1%}" if cost_of_equity is not None else "N/A",
                        "explanation": self._explain_metric("cost_of_equity", cost_of_equity)
                    },
                    "cost_of_debt": {
                        "value": f"{cost_of_debt:.1%}" if cost_of_debt is not None else "N/A",
                        "explanation": self._explain_metric("cost_of_debt", cost_of_debt)
                    }
                },

                "scenario_context": self._explain_scenario_context(scenario_key, base_assumptions)
            }

        # Add methodology notes
        result["methodology_notes"] = {
            "data_sources": "5 years of historical financial statements and analyst consensus estimates",
            "dcf_approach": "Discounted Cash Flow (DCF) values a company by projecting future free cash flows and discounting them to present value using the weighted average cost of capital (WACC).",
            "wacc_calculation": f"WACC calculated using CAPM with beta of {wacc_components.get('beta', 'N/A')}, risk-free rate of {self._format_percentage_display(wacc_components.get('risk_free_rate'))}, and market risk premium of {self._format_percentage_display(wacc_components.get('market_risk_premium'))}.",
            "scenario_methodology": {
                "conservative": "Uses below-historical assumptions to stress-test the valuation",
                "base_case": "Uses historical 5-year averages as the most likely outcome",
                "optimistic": "Uses above-historical assumptions to model upside potential"
            }
        }

        return result

    def _format_markdown_output(
        self,
        symbol: str,
        current_price: float,
        analysis_date: str,
        dcf_results: Dict[str, Dict],
        scenarios_assumptions: Dict[str, Dict],
        base_assumptions: Dict,
        wacc_components: Dict,
        use_analyst_estimates: bool,
        analyst_estimates: Optional[List[Dict]],
        detail_level: str
    ) -> str:
        """Format DCF analysis results as markdown"""
        from datetime import datetime, timezone as dt_timezone

        lines = []
        timestamp_utc = datetime.now(dt_timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

        # Header
        lines.append(f"## DCF Scenario Valuation: {symbol}")
        lines.append(f"**Retrieved:** {timestamp_utc}")
        lines.append(f"**Analysis Date:** {analysis_date or 'N/A'}")
        lines.append(f"**Current Price:** {self._format_currency_display(current_price)}")
        lines.append("")

        # Extract DCF values
        conservative_val = dcf_results["low"].get("equityValuePerShare")
        base_val = dcf_results["neutral"].get("equityValuePerShare")
        optimistic_val = dcf_results["high"].get("equityValuePerShare")

        # Valuation Summary
        lines.append("### Valuation Summary")
        lines.append("")
        lines.append("| Scenario | Fair Value | vs Current Price |")
        lines.append("|----------|-----------|-----------------|")

        def calc_diff_pct(dcf_val):
            if dcf_val and current_price:
                return self._format_percentage_display((dcf_val - current_price) / current_price)
            return "N/A"

        lines.append(f"| Conservative | {self._format_currency_display(conservative_val)} | {calc_diff_pct(conservative_val)} |")
        lines.append(f"| Base Case | {self._format_currency_display(base_val)} | {calc_diff_pct(base_val)} |")
        lines.append(f"| Optimistic | {self._format_currency_display(optimistic_val)} | {calc_diff_pct(optimistic_val)} |")
        lines.append("")

        # Valuation Range
        if conservative_val and base_val and optimistic_val:
            import statistics
            dcf_values = [conservative_val, base_val, optimistic_val]
            avg_dcf = statistics.mean(dcf_values)
            prob_weighted_upside = (avg_dcf - current_price) / current_price if current_price else None

            lines.append("**Valuation Range:**")
            lines.append(f"- Min: {self._format_currency_display(min(dcf_values))}")
            lines.append(f"- Median: {self._format_currency_display(statistics.median(dcf_values))}")
            lines.append(f"- Max: {self._format_currency_display(max(dcf_values))}")
            lines.append(f"- Probability-Weighted Value: {self._format_currency_display(avg_dcf)} ({self._format_percentage_display(prob_weighted_upside)} upside)")
            lines.append("")

        # Detailed Scenario Assumptions
        lines.append("### Scenario Assumptions")
        lines.append("")

        scenario_configs = {
            "Conservative": ("low", conservative_val),
            "Base Case": ("neutral", base_val),
            "Optimistic": ("high", optimistic_val)
        }

        for scenario_name, (scenario_key, fair_value) in scenario_configs.items():
            assumptions = scenarios_assumptions[scenario_key]

            lines.append(f"#### {scenario_name} Scenario")
            lines.append(f"**Fair Value:** {self._format_currency_display(fair_value)} ({calc_diff_pct(fair_value)} vs market)")
            lines.append("")

            # Key assumptions table
            lines.append("| Assumption | Value |")
            lines.append("|------------|-------|")

            rev_growth = assumptions.get("revenueGrowthPct")
            ebitda_margin = assumptions.get("ebitdaPct")
            terminal_growth = assumptions.get("longTermGrowthRate") / 100 if assumptions.get("longTermGrowthRate") else None
            cost_of_equity = assumptions.get("costOfEquity") / 100 if assumptions.get("costOfEquity") else None
            cost_of_debt = assumptions.get("costOfDebt") / 100 if assumptions.get("costOfDebt") else None

            lines.append(f"| Revenue Growth | {rev_growth:.1%} |" if rev_growth is not None else "| Revenue Growth | N/A |")
            lines.append(f"| EBITDA Margin | {ebitda_margin:.1%} |" if ebitda_margin is not None else "| EBITDA Margin | N/A |")
            lines.append(f"| Terminal Growth | {terminal_growth:.1%} |" if terminal_growth is not None else "| Terminal Growth | N/A |")
            lines.append(f"| Cost of Equity | {cost_of_equity:.1%} |" if cost_of_equity is not None else "| Cost of Equity | N/A |")
            lines.append(f"| Cost of Debt | {cost_of_debt:.1%} |" if cost_of_debt is not None else "| Cost of Debt | N/A |")
            lines.append("")

        # WACC Components
        lines.append("### WACC Components")
        lines.append("")
        lines.append("| Component | Value |")
        lines.append("|-----------|-------|")
        lines.append(f"| Beta | {wacc_components.get('beta', 'N/A'):.2f} |" if wacc_components.get('beta') else "| Beta | N/A |")
        lines.append(f"| Risk-Free Rate | {self._format_percentage_display(wacc_components.get('risk_free_rate'))} |")
        lines.append(f"| Market Risk Premium | {self._format_percentage_display(wacc_components.get('market_risk_premium'))} |")
        lines.append(f"| Cost of Equity (CAPM) | {self._format_percentage_display(wacc_components.get('cost_of_equity'))} |")
        lines.append(f"| Cost of Debt | {self._format_percentage_display(wacc_components.get('cost_of_debt'))} |")
        lines.append(f"| WACC | {self._format_percentage_display(wacc_components.get('wacc'))} |")
        lines.append("")

        # Historical Base Assumptions
        lines.append("### Historical Base Assumptions (5-Year Averages)")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        lines.append(f"| Revenue Growth (CAGR) | {base_assumptions.get('revenue_growth_hist', 0):.1%} |")
        lines.append(f"| EBITDA Margin | {base_assumptions.get('ebitda_margin_hist', 0):.1%} |")
        lines.append(f"| EBIT Margin | {base_assumptions.get('ebit_margin_hist', 0):.1%} |")
        lines.append(f"| Net Margin | {base_assumptions.get('net_margin_hist', 0):.1%} |")
        lines.append(f"| D&A % of Revenue | {base_assumptions.get('da_pct_hist', 0):.1%} |")
        lines.append(f"| Capex % of Revenue | {base_assumptions.get('capex_pct_hist', 0):.1%} |")
        lines.append(f"| Tax Rate | {base_assumptions.get('tax_rate_hist', 0):.1%} |")
        lines.append("")

        # Analyst Estimates (if available)
        if use_analyst_estimates and analyst_estimates and len(analyst_estimates) > 0:
            latest = analyst_estimates[0]
            lines.append("### Analyst Consensus Estimates")
            lines.append("")
            lines.append(f"**Fiscal Period:** {latest.get('date', 'N/A')}")
            lines.append("")
            lines.append("| Metric | Low | Average | High |")
            lines.append("|--------|-----|---------|------|")

            rev_low = self._format_large_number(latest.get("estimatedRevenueLow"))
            rev_avg = self._format_large_number(latest.get("estimatedRevenueAvg"))
            rev_high = self._format_large_number(latest.get("estimatedRevenueHigh"))
            lines.append(f"| Revenue | {rev_low} | {rev_avg} | {rev_high} |")

            ebitda_low = self._format_large_number(latest.get("estimatedEbitdaLow"))
            ebitda_avg = self._format_large_number(latest.get("estimatedEbitdaAvg"))
            ebitda_high = self._format_large_number(latest.get("estimatedEbitdaHigh"))
            lines.append(f"| EBITDA | {ebitda_low} | {ebitda_avg} | {ebitda_high} |")

            eps_low = self._format_decimal(latest.get("estimatedEpsLow"), 2)
            eps_avg = self._format_decimal(latest.get("estimatedEpsAvg"), 2)
            eps_high = self._format_decimal(latest.get("estimatedEpsHigh"), 2)
            lines.append(f"| EPS | ${eps_low} | ${eps_avg} | ${eps_high} |")
            lines.append("")

        # Sensitivity Analysis (if extended mode)
        if detail_level == "extended":
            lines.append("### Sensitivity Analysis")
            lines.append("")
            lines.append("*Note: Sensitivity analysis data available in extended detail level*")
            lines.append("")

        # Methodology Note
        lines.append("### Methodology")
        lines.append("")
        lines.append("**Approach:** Discounted Cash Flow (DCF) values a company by projecting future free cash flows and discounting them to present value using the weighted average cost of capital (WACC).")
        lines.append("")
        lines.append("**Data Sources:** 5 years of historical financial statements and analyst consensus estimates")
        lines.append("")
        lines.append("**Scenario Methodology:**")
        lines.append("- **Conservative:** Uses below-historical assumptions to stress-test the valuation")
        lines.append("- **Base Case:** Uses historical 5-year averages as the most likely outcome")
        lines.append("- **Optimistic:** Uses above-historical assumptions to model upside potential")
        lines.append("")

        return "\n".join(lines)

    def _calculate_sensitivity_analysis(
        self,
        symbol: str,
        neutral_assumptions: Dict
    ) -> Dict:
        """Calculate sensitivity to WACC and terminal growth changes"""

        sensitivity = {
            "wacc_sensitivity": [],
            "terminal_growth_sensitivity": []
        }

        # WACC sensitivity: +/- 1% from neutral
        neutral_wacc = neutral_assumptions.get("costOfEquity", 10.0)
        wacc_variations = [neutral_wacc - 1.0, neutral_wacc, neutral_wacc + 1.0]

        for wacc in wacc_variations:
            modified_assumptions = neutral_assumptions.copy()
            modified_assumptions["costOfEquity"] = wacc

            dcf_result = self._run_custom_dcf(symbol, modified_assumptions)
            dcf_value = dcf_result.get("equityValuePerShare")

            sensitivity["wacc_sensitivity"].append({
                "wacc": self._format_ratio(wacc / 100),
                "dcf_value": self._format_decimal(dcf_value, 2) if dcf_value else None
            })

        # Terminal growth sensitivity: 2%, 2.5%, 3%
        growth_variations = [2.0, 2.5, 3.0]

        for growth in growth_variations:
            modified_assumptions = neutral_assumptions.copy()
            modified_assumptions["longTermGrowthRate"] = growth

            dcf_result = self._run_custom_dcf(symbol, modified_assumptions)
            dcf_value = dcf_result.get("equityValuePerShare")

            sensitivity["terminal_growth_sensitivity"].append({
                "terminal_growth": self._format_ratio(growth / 100),
                "dcf_value": self._format_decimal(dcf_value, 2) if dcf_value else None
            })

        return sensitivity


# Core implementation - Tool decorator moved to src/tools/us/tool.py
async def analyze_dcf_scenarios_fmp_impl(
    symbol: str,
    use_analyst_estimates: bool = True,
    detail_level: str = "compact"
) -> str:
    """
    Custom DCF valuation with 3 scenarios (Low/Neutral/High) - async.

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
        Markdown-formatted string with 3 DCF scenarios, valuation range, and assumptions

    Example Output (markdown format):
        ## DCF Scenario Valuation: AAPL
        **Retrieved:** 2025-11-15 10:30 UTC
        **Current Price:** $150.00

        ### Valuation Summary
        | Scenario | Fair Value | vs Current Price |
        |----------|-----------|-----------------|
        | Conservative | $135.50 | -9.7% |
        | Base Case | $165.00 | +10.0% |
        | Optimistic | $195.00 | +30.0% |
    """
    async with FMPClient() as fmp_client:
        analyzer = DCFScenarioValuationTool(fmp_client)
        return await analyzer.analyze(symbol, use_analyst_estimates, detail_level=detail_level)


if __name__ == "__main__":
    # Quick test
    import json
    print("Testing DCF Scenario Valuation Tool...")
    print("="*80)

    result = analyze_dcf_scenarios_fmp_impl(
        symbol="AAPL",
        use_analyst_estimates=True,
        detail_level="compact"
    )

    if "error" in result:
        print(f"\n❌ Error: {result['error']}")
    else:
        print(f"\n✅ Success! Analyzed {result['metadata']['symbol']}")
        print(f"\n📊 DCF Scenario Analysis Results:")
        print(f"   Current Price: ${result['current_market_data']['price']}")
        print(f"\n   Scenario Valuations:")
        for scenario in ["low", "neutral", "high"]:
            dcf_val = result['scenarios'][scenario]['dcf_value']
            vs_price = result['scenarios'][scenario]['vs_current_price']
            if dcf_val and vs_price:
                print(f"   • {scenario.capitalize()}: ${dcf_val} ({vs_price:+.1%} vs market)")
            else:
                print(f"   • {scenario.capitalize()}: N/A")

        if "dcf_range" in result['valuation_summary']:
            dcf_range = result['valuation_summary']['dcf_range']
            print(f"\n   Valuation Range: ${dcf_range['min']} - ${dcf_range['max']}")

            if "probability_weighted_value" in result['valuation_summary']:
                weighted = result['valuation_summary']['probability_weighted_value']
                upside = result['valuation_summary']['probability_weighted_upside']
                print(f"   Probability-Weighted: ${weighted} ({upside:+.1%} upside)")

        print("\n" + "="*80)
        print("\nFull result (JSON):")
        print(json.dumps(result, indent=2))
