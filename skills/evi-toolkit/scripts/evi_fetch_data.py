#!/usr/bin/env python3
"""Sirius Valuation Skill — 自包含数据获取脚本。

**不依赖宿主项目任何模块**。使用纯 requests 调用 FMP API。

用法：
    python scripts/fetch_data.py --symbol 1357.HK --market hk
    python scripts/fetch_data.py --symbol AAPL --market us
    python scripts/fetch_data.py --symbol 600519.SS --market cn

输出到 data/{symbol}/ 目录：
    data/{symbol}/
    ├── raw/                      # FMP 原始 JSON
    │   ├── profile.json
    │   ├── income_statement.json
    │   ├── balance_sheet.json
    │   ├── cash_flow.json
    │   ├── key_metrics.json
    │   └── ratios.json
    ├── financial_context.md      # 格式化的财务数据（供 Agent 读取）
    └── engine_result.json        # 估值引擎计算结果
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("skill.fetch_data")

SKILL_DIR = Path(__file__).resolve().parent.parent

# EVI 数据布局：默认输出到 data/{symbol_dir}/base/，由调用方通过 --data-dir 指定。
# 兼容老路径（沙箱根 data/<symbol>/）：未传 --data-dir 时按 sirius 老约定回退。
_in_sandbox = ".agents/skills" in str(SKILL_DIR)
if _in_sandbox:
    DATA_DIR = SKILL_DIR.parent.parent.parent / "data"
else:
    DATA_DIR = SKILL_DIR / "data"

# ═══════════════════════════════════════════
# FMP API 配置
# ═══════════════════════════════════════════

FMP_BASE = "https://financialmodelingprep.com/api/v3"

def _fmp_key() -> str:
    """从 Skill 自身的 .env 或环境变量获取 FMP API key。"""
    # 1. 环境变量优先
    key = os.environ.get("FMP_API_KEY", "")
    if key:
        return key
    # 2. Skill 目录下的 .env
    env_file = SKILL_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("FMP_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise ValueError(
        "FMP_API_KEY not found. Set via:\n"
        "  1. export FMP_API_KEY=xxx\n"
        "  2. or create skills/sirius_valuation/.env with FMP_API_KEY=xxx"
    )


def _fmp_get(endpoint: str, params: dict | None = None, timeout: int = 15) -> Any:
    import requests
    params = dict(params or {})
    params["apikey"] = _fmp_key()
    url = f"{FMP_BASE}/{endpoint}"
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════
# 数据获取
# ═══════════════════════════════════════════

def fetch_all(symbol: str) -> Dict[str, Any]:
    """并发获取 FMP 全部财务数据。"""
    tasks = {
        "profile": lambda: (_fmp_get(f"profile/{symbol}") or [None])[0],
        "incomeStatement": lambda: _fmp_get(f"income-statement/{symbol}", {"period": "annual", "limit": 10}),
        "balanceSheet": lambda: _fmp_get(f"balance-sheet-statement/{symbol}", {"period": "annual", "limit": 10}),
        "cashFlow": lambda: _fmp_get(f"cash-flow-statement/{symbol}", {"period": "annual", "limit": 10}),
        "keyMetrics": lambda: _fmp_get(f"key-metrics/{symbol}", {"period": "annual", "limit": 10}),
        "ratios": lambda: _fmp_get(f"ratios/{symbol}", {"period": "annual", "limit": 10}),
    }

    results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fn): key for key, fn in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                log.error("  %s failed: %s", key, e)
                results[key] = None
    return results


def fetch_quarterly(symbol: str) -> Dict[str, Any]:
    """获取最近 12 个季度的财务数据。"""
    tasks = {
        "incomeStatement": lambda: _fmp_get(f"income-statement/{symbol}", {"period": "quarter", "limit": 12}),
        "balanceSheet": lambda: _fmp_get(f"balance-sheet-statement/{symbol}", {"period": "quarter", "limit": 12}),
        "cashFlow": lambda: _fmp_get(f"cash-flow-statement/{symbol}", {"period": "quarter", "limit": 12}),
        "keyMetrics": lambda: _fmp_get(f"key-metrics/{symbol}", {"period": "quarter", "limit": 12}),
    }
    results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fn): key for key, fn in tasks.items()}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                log.error("  quarterly %s failed: %s", key, e)
                results[key] = None
    return results


def compute_ttm(quarterly_data: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, Any]:
    """从最近 4 个季度计算 TTM 指标。"""
    income_q = (quarterly_data.get("incomeStatement") or [])[:4]
    cash_q = (quarterly_data.get("cashFlow") or [])[:4]

    if len(income_q) < 4:
        return {"error": "insufficient quarterly data", "periods": len(income_q)}

    def _sum_field(data_list: list, field: str) -> Optional[float]:
        vals = [_sf(d.get(field)) for d in data_list]
        if any(v is None for v in vals):
            return None
        return sum(v for v in vals if v is not None)

    revenue_ttm = _sum_field(income_q, "revenue")
    net_income_ttm = _sum_field(income_q, "netIncome")
    ebitda_ttm = _sum_field(income_q, "ebitda")
    operating_income_ttm = _sum_field(income_q, "operatingIncome")

    fcf_ttm = None
    if len(cash_q) >= 4:
        ocf_ttm = _sum_field(cash_q, "operatingCashFlow")
        capex_ttm = _sum_field(cash_q, "capitalExpenditure")
        if ocf_ttm is not None and capex_ttm is not None:
            fcf_ttm = ocf_ttm - abs(capex_ttm)

    # 估值倍数 TTM
    price = _sf(profile.get("price"))
    mkt_cap = _sf(profile.get("mktCap"))
    # EV = market_cap + total_debt - cash（简化）
    ev = mkt_cap  # 简化版，完整版需要 balance sheet

    pe_ttm = (mkt_cap / net_income_ttm) if (mkt_cap and net_income_ttm and net_income_ttm > 0) else None
    ps_ttm = (mkt_cap / revenue_ttm) if (mkt_cap and revenue_ttm and revenue_ttm > 0) else None
    ev_ebitda_ttm = (ev / ebitda_ttm) if (ev and ebitda_ttm and ebitda_ttm > 0) else None

    # Margins TTM
    gross_profit_ttm = _sum_field(income_q, "grossProfit")
    gm_ttm = (gross_profit_ttm / revenue_ttm * 100) if (gross_profit_ttm and revenue_ttm and revenue_ttm > 0) else None
    om_ttm = (operating_income_ttm / revenue_ttm * 100) if (operating_income_ttm and revenue_ttm and revenue_ttm > 0) else None
    nm_ttm = (net_income_ttm / revenue_ttm * 100) if (net_income_ttm and revenue_ttm and revenue_ttm > 0) else None

    result = {
        "as_of": income_q[0].get("date") if income_q else None,
        "periods_used": [d.get("date") for d in income_q],
        "revenue_ttm": round(revenue_ttm) if revenue_ttm else None,
        "net_income_ttm": round(net_income_ttm) if net_income_ttm else None,
        "ebitda_ttm": round(ebitda_ttm) if ebitda_ttm else None,
        "fcf_ttm": round(fcf_ttm) if fcf_ttm else None,
        "gross_margin_ttm_pct": round(gm_ttm, 2) if gm_ttm else None,
        "operating_margin_ttm_pct": round(om_ttm, 2) if om_ttm else None,
        "net_margin_ttm_pct": round(nm_ttm, 2) if nm_ttm else None,
        "pe_ttm": round(pe_ttm, 2) if pe_ttm else None,
        "ps_ttm": round(ps_ttm, 2) if ps_ttm else None,
        "ev_ebitda_ttm": round(ev_ebitda_ttm, 2) if ev_ebitda_ttm else None,
        "market_cap": mkt_cap,
        "price": price,
    }
    return result


def compute_growth_rates(quarterly_data: Dict[str, Any], annual_data: Dict[str, Any]) -> Dict[str, Any]:
    """计算 YoY% 和 QoQ% 增长率矩阵。"""
    income_q = quarterly_data.get("incomeStatement") or []
    income_a = annual_data.get("incomeStatement") or []

    growth_quarterly: List[Dict[str, Any]] = []
    for i, q in enumerate(income_q):
        entry: Dict[str, Any] = {"period": q.get("date"), "fiscal_quarter": q.get("period")}
        rev = _sf(q.get("revenue"))
        ni = _sf(q.get("netIncome"))
        gp = _sf(q.get("grossProfit"))
        oi = _sf(q.get("operatingIncome"))

        gm = (gp / rev * 100) if (gp is not None and rev and rev > 0) else None
        om = (oi / rev * 100) if (oi is not None and rev and rev > 0) else None

        # QoQ (vs previous quarter)
        if i + 1 < len(income_q):
            prev = income_q[i + 1]
            prev_rev = _sf(prev.get("revenue"))
            prev_ni = _sf(prev.get("netIncome"))
            prev_gp = _sf(prev.get("grossProfit"))
            prev_oi = _sf(prev.get("operatingIncome"))

            entry["revenue_qoq_pct"] = round((rev - prev_rev) / abs(prev_rev) * 100, 2) if (rev is not None and prev_rev and prev_rev != 0) else None
            entry["net_income_qoq_pct"] = round((ni - prev_ni) / abs(prev_ni) * 100, 2) if (ni is not None and prev_ni and prev_ni != 0) else None

            prev_gm = (prev_gp / prev_rev * 100) if (prev_gp is not None and prev_rev and prev_rev > 0) else None
            prev_om = (prev_oi / prev_rev * 100) if (prev_oi is not None and prev_rev and prev_rev > 0) else None
            entry["gross_margin_qoq_pp"] = round(gm - prev_gm, 2) if (gm is not None and prev_gm is not None) else None
            entry["operating_margin_qoq_pp"] = round(om - prev_om, 2) if (om is not None and prev_om is not None) else None

        # YoY (vs same quarter last year = index + 4)
        if i + 4 < len(income_q):
            yoy_q = income_q[i + 4]
            yoy_rev = _sf(yoy_q.get("revenue"))
            yoy_ni = _sf(yoy_q.get("netIncome"))
            yoy_gp = _sf(yoy_q.get("grossProfit"))
            yoy_oi = _sf(yoy_q.get("operatingIncome"))

            entry["revenue_yoy_pct"] = round((rev - yoy_rev) / abs(yoy_rev) * 100, 2) if (rev is not None and yoy_rev and yoy_rev != 0) else None
            entry["net_income_yoy_pct"] = round((ni - yoy_ni) / abs(yoy_ni) * 100, 2) if (ni is not None and yoy_ni and yoy_ni != 0) else None

            yoy_gm = (yoy_gp / yoy_rev * 100) if (yoy_gp is not None and yoy_rev and yoy_rev > 0) else None
            yoy_om = (yoy_oi / yoy_rev * 100) if (yoy_oi is not None and yoy_rev and yoy_rev > 0) else None
            entry["gross_margin_yoy_pp"] = round(gm - yoy_gm, 2) if (gm is not None and yoy_gm is not None) else None
            entry["operating_margin_yoy_pp"] = round(om - yoy_om, 2) if (om is not None and yoy_om is not None) else None

        entry["revenue"] = rev
        entry["net_income"] = ni
        entry["gross_margin_pct"] = round(gm, 2) if gm is not None else None
        entry["operating_margin_pct"] = round(om, 2) if om is not None else None
        growth_quarterly.append(entry)

    # Annual YoY
    growth_annual: List[Dict[str, Any]] = []
    for i, a in enumerate(income_a):
        entry = {"period": a.get("date")}
        rev = _sf(a.get("revenue"))
        ni = _sf(a.get("netIncome"))
        gp = _sf(a.get("grossProfit"))
        oi = _sf(a.get("operatingIncome"))

        gm = (gp / rev * 100) if (gp is not None and rev and rev > 0) else None
        om = (oi / rev * 100) if (oi is not None and rev and rev > 0) else None

        if i + 1 < len(income_a):
            prev = income_a[i + 1]
            prev_rev = _sf(prev.get("revenue"))
            prev_ni = _sf(prev.get("netIncome"))
            prev_gp = _sf(prev.get("grossProfit"))
            prev_oi = _sf(prev.get("operatingIncome"))

            entry["revenue_yoy_pct"] = round((rev - prev_rev) / abs(prev_rev) * 100, 2) if (rev is not None and prev_rev and prev_rev != 0) else None
            entry["net_income_yoy_pct"] = round((ni - prev_ni) / abs(prev_ni) * 100, 2) if (ni is not None and prev_ni and prev_ni != 0) else None

            prev_gm = (prev_gp / prev_rev * 100) if (prev_gp is not None and prev_rev and prev_rev > 0) else None
            prev_om = (prev_oi / prev_rev * 100) if (prev_oi is not None and prev_rev and prev_rev > 0) else None
            entry["gross_margin_yoy_pp"] = round(gm - prev_gm, 2) if (gm is not None and prev_gm is not None) else None
            entry["operating_margin_yoy_pp"] = round(om - prev_om, 2) if (om is not None and prev_om is not None) else None

        entry["revenue"] = rev
        entry["net_income"] = ni
        entry["gross_margin_pct"] = round(gm, 2) if gm is not None else None
        entry["operating_margin_pct"] = round(om, 2) if om is not None else None
        growth_annual.append(entry)

    return {
        "quarterly": growth_quarterly,
        "annual": growth_annual,
    }


# ═══════════════════════════════════════════
# 财务数据格式化（financial_context.md）
# ═══════════════════════════════════════════

def build_financial_context(data: Dict[str, Any]) -> str:
    """将 FMP 数据格式化为 Markdown 表格，供 Agent 读取。"""
    profile = data.get("profile") or {}
    income = data.get("incomeStatement") or []
    balance = data.get("balanceSheet") or []
    cash_flow = data.get("cashFlow") or []
    key_metrics = data.get("keyMetrics") or []
    ratios = data.get("ratios") or []

    lines = []

    # 公司概况
    lines.append("## 公司概况\n")
    lines.append(f"- 名称: {profile.get('companyName', 'N/A')}")
    lines.append(f"- 代码: {profile.get('symbol', 'N/A')}")
    lines.append(f"- 行业: {profile.get('sector', '')} / {profile.get('industry', '')}")
    lines.append(f"- 市值: {profile.get('mktCap', 'N/A')}")
    lines.append(f"- 价格: {profile.get('price', 'N/A')}")
    lines.append(f"- Beta: {profile.get('beta', 'N/A')}")
    lines.append(f"- 简介: {(profile.get('description') or '')[:500]}")
    lines.append("")

    # 利润表
    if income:
        lines.append("## 利润表 (近5年)\n")
        lines.append("| 年份 | 营收 | 营业利润 | 净利润 | EPS | EBITDA | 利息费用 | 所得税 | 税前利润 |")
        lines.append("|------|------|---------|--------|-----|--------|---------|--------|---------|")
        for inc in income[:5]:
            lines.append(f"| {inc.get('date', '')[:4]} | {inc.get('revenue', '')} | {inc.get('operatingIncome', '')} | {inc.get('netIncome', '')} | {inc.get('eps', '')} | {inc.get('ebitda', '')} | {inc.get('interestExpense', '')} | {inc.get('incomeTaxExpense', '')} | {inc.get('incomeBeforeTax', '')} |")
        lines.append("")

    # 资产负债表
    if balance:
        lines.append("## 资产负债表 (近5年)\n")
        lines.append("| 年份 | 总资产 | 总负债 | 股东权益 | 现金 | 短期债 | 长期债 | 商誉 |")
        lines.append("|------|--------|--------|---------|------|--------|--------|------|")
        for bs in balance[:5]:
            lines.append(f"| {bs.get('date', '')[:4]} | {bs.get('totalAssets', '')} | {bs.get('totalLiabilities', '')} | {bs.get('totalStockholdersEquity', '')} | {bs.get('cashAndCashEquivalents', '')} | {bs.get('shortTermDebt', '')} | {bs.get('longTermDebt', '')} | {bs.get('goodwill', '')} |")
        lines.append("")

    # 现金流量表
    if cash_flow:
        lines.append("## 现金流量表 (近5年)\n")
        lines.append("| 年份 | 经营CF | 投资CF | 融资CF | Capex | 折旧摊销 | 股息支出 |")
        lines.append("|------|--------|--------|--------|-------|---------|---------|")
        for cf in cash_flow[:5]:
            lines.append(f"| {cf.get('date', '')[:4]} | {cf.get('operatingCashFlow', '')} | {cf.get('netCashUsedForInvestingActivites', '')} | {cf.get('netCashUsedProvidedByFinancingActivities', '')} | {cf.get('capitalExpenditure', '')} | {cf.get('depreciationAndAmortization', '')} | {cf.get('dividendsPaid', '')} |")
        lines.append("")

    # 核心指标
    if key_metrics:
        lines.append("## 核心指标 (近5年)\n")
        lines.append("| 年份 | ROE | ROA | PE | PB | DPS | 股息率 | 每股FCF |")
        lines.append("|------|-----|-----|----|----|-----|--------|---------|")
        for km in key_metrics[:5]:
            roe = km.get('roe')
            roa = km.get('returnOnAssets')
            roe_s = f"{roe*100:.1f}%" if roe else ''
            roa_s = f"{roa*100:.1f}%" if roa else ''
            lines.append(f"| {km.get('date', '')[:4]} | {roe_s} | {roa_s} | {km.get('peRatio', '')} | {km.get('pbRatio', '')} | {km.get('dividendPerShare', '')} | {km.get('dividendYield', '')} | {km.get('freeCashFlowPerShare', '')} |")
        lines.append("")

    # 财务比率
    if ratios:
        lines.append("## 财务比率 (近5年)\n")
        lines.append("| 年份 | 毛利率 | 净利率 | 分红率 | 流动比 | 速动比 | 负债权益比 |")
        lines.append("|------|--------|--------|--------|--------|--------|-----------|")
        for r in ratios[:5]:
            gm = r.get('grossProfitMargin')
            nm = r.get('netProfitMargin')
            pr = r.get('payoutRatio')
            lines.append(f"| {r.get('date', '')[:4]} | {f'{gm*100:.1f}%' if gm else ''} | {f'{nm*100:.1f}%' if nm else ''} | {f'{pr*100:.1f}%' if pr else ''} | {r.get('currentRatio', '')} | {r.get('quickRatio', '')} | {r.get('debtEquityRatio', '')} |")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════
# 估值引擎（自包含，核心逻辑移植自 valuation_engine.py）
# ═══════════════════════════════════════════

MARKET_PARAMS = {
    "cn": {"erp": 6.0, "rf": 2.5, "tax": 25.0, "g_terminal": 3.0},
    "hk": {"erp": 5.5, "rf": 4.0, "tax": 16.5, "g_terminal": 2.5},
    "us": {"erp": 5.0, "rf": 4.0, "tax": 21.0, "g_terminal": 2.5},
}

METHOD_WEIGHTS = {
    "蓝筹价值型": {"DCF": 40, "DDM": 30, "PE_Band": 30},
    "成长型": {"PEG": 35, "DCF_Scenarios": 35, "PS": 30},
    "混合型": {"DCF": 35, "PE_Band": 25, "PEG": 25, "DDM": 15},
}


def _sf(v) -> Optional[float]:
    if v is None: return None
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None

def _cagr(vals: List[Optional[float]]) -> Optional[float]:
    clean = [v for v in vals if v is not None and v > 0]
    if len(clean) < 2: return None
    return (clean[0] / clean[-1]) ** (1 / (len(clean) - 1)) - 1

def _pct(data: List[float], p: float) -> float:
    if not data: return 0
    k = (len(data) - 1) * p / 100
    f, c = math.floor(k), math.ceil(k)
    return data[f] * (c - k) + data[c] * (k - f) if f != c else data[int(k)]


def compute_valuation(data: Dict[str, Any], market: str = "hk") -> Dict[str, Any]:
    """自包含估值引擎——WACC + 分类 + 6 种方法 + 交叉验证 + 反向估值。"""
    mp = MARKET_PARAMS.get(market, MARKET_PARAMS["us"])
    profile = data.get("profile") or {}
    income = data.get("incomeStatement") or []
    balance = data.get("balanceSheet") or []
    cash_flow = data.get("cashFlow") or []
    key_metrics = data.get("keyMetrics") or []
    ratios = data.get("ratios") or []

    price = _sf(profile.get("price"))
    mkt_cap = _sf(profile.get("mktCap"))
    if not price or not mkt_cap:
        return {"error": "No price/mktCap data"}

    total_shares = mkt_cap / price if price > 0 else 0

    # ── 分类 ──
    roe_vals = [v for km in key_metrics[:5] if (v := _sf(km.get("roe"))) is not None]
    roe_avg = statistics.mean([r * 100 for r in roe_vals]) if roe_vals else None

    payout_vals = [v for r in ratios[:3] if (v := _sf(r.get("payoutRatio"))) is not None]
    payout_avg = statistics.mean([p * 100 for p in payout_vals]) if payout_vals else None

    rev_series = [_sf(r.get("revenue")) for r in income[:5]]
    rev_cagr = _cagr(rev_series)
    rev_cagr_pct = rev_cagr * 100 if rev_cagr else None

    np_series = [_sf(r.get("netIncome")) for r in income[:5]]
    np_cagr = _cagr(np_series)
    np_cagr_pct = np_cagr * 100 if np_cagr else None

    blue, growth = 0, 0
    if roe_avg and roe_avg > 15: blue += 1
    if payout_avg and payout_avg > 30: blue += 1
    if rev_cagr_pct is not None and rev_cagr_pct < 20: blue += 1
    if rev_cagr_pct is not None and rev_cagr_pct > 20: growth += 1
    if np_cagr_pct is not None and np_cagr_pct > 25: growth += 1

    if blue >= 2 and growth == 0: ctype = "蓝筹价值型"
    elif growth >= 2 and blue <= 1: ctype = "成长型"
    else: ctype = "混合型"

    latest_np = _sf(income[0].get("netIncome")) if income else None
    if latest_np is not None and latest_np < 0: ctype = "成长型"

    methods_to_run = list(METHOD_WEIGHTS.get(ctype, METHOD_WEIGHTS["混合型"]).keys())
    weights = dict(METHOD_WEIGHTS.get(ctype, METHOD_WEIGHTS["混合型"]))

    classification = {
        "type": ctype, "blue_score": blue, "growth_score": growth,
        "roe_avg": round(roe_avg, 2) if roe_avg else None,
        "payout_avg": round(payout_avg, 2) if payout_avg else None,
        "rev_cagr_pct": round(rev_cagr_pct, 2) if rev_cagr_pct else None,
        "np_cagr_pct": round(np_cagr_pct, 2) if np_cagr_pct else None,
        "methods": methods_to_run, "weights": weights,
    }

    # ── WACC ──
    beta = _sf(profile.get("beta"))
    if beta is None:
        beta = 0.8 if mkt_cap > 1e12 else (1.0 if mkt_cap > 1e11 else 1.2)
    rf = mp["rf"]
    erp = mp["erp"]
    ke = rf + beta * erp

    fin_exp = _sf(income[0].get("interestExpense")) if income else None
    d0 = sum(_sf(balance[0].get(f)) or 0 for f in ("shortTermDebt", "longTermDebt")) if balance else 0
    d1 = sum(_sf(balance[1].get(f)) or 0 for f in ("shortTermDebt", "longTermDebt")) if len(balance) > 1 else 0
    avg_d = (d0 + d1) / 2 if (d0 + d1) > 0 else 0
    kd = (fin_exp / avg_d * 100) if (fin_exp and fin_exp > 0 and avg_d > 0) else rf + 1.0

    tax_rates = []
    for inc in income[:5]:
        tx, pt = _sf(inc.get("incomeTaxExpense")), _sf(inc.get("incomeBeforeTax"))
        if tx is not None and pt and pt > 0:
            tax_rates.append(tx / pt * 100)
    tax = statistics.mean(tax_rates) if tax_rates else mp["tax"]

    total = mkt_cap + d0
    ew = mkt_cap / total * 100 if total > 0 else 100
    dw = d0 / total * 100 if total > 0 else 0
    wacc = ke * ew / 100 + kd * (1 - tax / 100) * dw / 100 if d0 > 0 else ke

    wacc_data = {"rf": round(rf, 2), "beta": round(beta, 2), "erp": round(erp, 2),
                 "ke": round(ke, 2), "kd_pre": round(kd, 2), "tax_rate": round(tax, 2),
                 "e_weight": round(ew, 2), "d_weight": round(dw, 2), "wacc": round(wacc, 2)}

    g_terminal = mp["g_terminal"]
    if g_terminal >= wacc: g_terminal = wacc - 2.0

    # ── 估值方法 ──
    method_results = []
    cash = _sf(balance[0].get("cashAndCashEquivalents")) if balance else 0

    # DCF Stable
    if "DCF" in methods_to_run:
        fcf_raw = []
        for cf in cash_flow[:5]:
            ocf, capex = _sf(cf.get("operatingCashFlow")), _sf(cf.get("capitalExpenditure"))
            if ocf is not None and capex is not None:
                fcf_raw.append(ocf - abs(capex))
        if len(fcf_raw) >= 2:
            fcf_base = statistics.mean(fcf_raw[:3])
            fcf_cagr = _cagr(fcf_raw)
            g_hist = fcf_cagr * 100 if fcf_cagr and fcf_cagr > 0 else (rev_cagr_pct or 5.0)
            g_cons = g_hist * 0.8
            g_fade = (g_cons + g_terminal) / 2
            g_fade2 = (g_fade + g_terminal) / 2
            rates = [g_cons, g_cons, g_fade, g_fade2, g_terminal]
            proj = []; prev = fcf_base
            for g in rates:
                prev = prev * (1 + g / 100); proj.append(prev)
            tv = proj[-1] * (1 + g_terminal / 100) / (wacc / 100 - g_terminal / 100)
            pv_fcf = sum(f / (1 + wacc / 100) ** (i + 1) for i, f in enumerate(proj))
            pv_tv = tv / (1 + wacc / 100) ** 5
            ev = pv_fcf + pv_tv
            intrinsic = (ev + (cash or 0) - d0) / total_shares if total_shares > 0 else 0
            # 5x5 sensitivity
            wr = [wacc - 2, wacc - 1, wacc, wacc + 1, wacc + 2]
            gr = [g_terminal - 1, g_terminal - 0.5, g_terminal, g_terminal + 0.5, g_terminal + 1]
            sens = []
            for w in wr:
                row = []
                for g in gr:
                    if g >= w: row.append(None); continue
                    tv_s = proj[-1] * (1 + g / 100) / (w / 100 - g / 100)
                    pv_s = sum(f / (1 + w / 100) ** (i + 1) for i, f in enumerate(proj))
                    pv_tv_s = tv_s / (1 + w / 100) ** 5
                    row.append(round((pv_s + pv_tv_s + (cash or 0) - d0) / total_shares, 2) if total_shares > 0 else 0)
                sens.append(row)
            method_results.append({
                "method": "DCF", "intrinsic": round(intrinsic, 2),
                "fcf_base": round(fcf_base), "g_conservative": round(g_cons, 2),
                "g_terminal": round(g_terminal, 2), "g_hist": round(g_hist, 2),
                "tv_pct": round(pv_tv / ev * 100, 1) if ev > 0 else 0,
                "sensitivity": sens, "wacc_range": [round(w, 2) for w in wr],
                "g_range": [round(g, 2) for g in gr],
            })

    # DDM
    if "DDM" in methods_to_run:
        dps_list = [v for km in key_metrics[:5] if (v := _sf(km.get("dividendPerShare"))) is not None and v > 0]
        if len(dps_list) >= 3:
            dps_latest = dps_list[0]
            dps_cagr = _cagr(dps_list)
            dps_cagr_pct = dps_cagr * 100 if dps_cagr else 2.0
            g2 = min(g_terminal, ke - 1.0)
            pv_vals = []; dps_t = dps_latest
            for t in range(1, 6):
                dps_t *= (1 + dps_cagr_pct / 100)
                pv_vals.append(dps_t / (1 + ke / 100) ** t)
            dps_6 = dps_t * (1 + g2 / 100)
            pv_phase2 = (dps_6 / (ke / 100 - g2 / 100)) / (1 + ke / 100) ** 5
            intrinsic = sum(pv_vals) + pv_phase2
            method_results.append({
                "method": "DDM", "intrinsic": round(intrinsic, 2),
                "model_type": "Two-stage", "dps_latest": round(dps_latest, 4),
                "dps_cagr_pct": round(dps_cagr_pct, 2), "g_used": round(dps_cagr_pct, 2),
            })

    # PE Band
    if "PE_Band" in methods_to_run:
        pe_series = [pe for km in key_metrics if (pe := _sf(km.get("peRatio"))) and 0 < pe < 200]
        eps_vals = [e for inc in income[:3] if (e := _sf(inc.get("eps"))) and e > 0]
        if len(pe_series) >= 3 and eps_vals:
            pe_sorted = sorted(pe_series)
            eps_norm = statistics.mean(eps_vals)
            intrinsic = _pct(pe_sorted, 50) * eps_norm
            method_results.append({
                "method": "PE_Band", "intrinsic": round(intrinsic, 2),
                "low": round(_pct(pe_sorted, 25) * eps_norm, 2),
                "high": round(_pct(pe_sorted, 75) * eps_norm, 2),
                "pe_median": round(_pct(pe_sorted, 50), 2),
                "eps_norm": round(eps_norm, 4),
            })

    # PEG
    if "PEG" in methods_to_run:
        pe_ttm = None
        if key_metrics:
            pe_ttm = _sf(key_metrics[0].get("peRatio"))
        if pe_ttm and pe_ttm > 0 and np_cagr_pct and np_cagr_pct > 0:
            peg_val = pe_ttm / np_cagr_pct
            eps_ttm = price / pe_ttm if pe_ttm > 0 else None
            fair_pe = np_cagr_pct * 1.0
            fair_price = fair_pe * eps_ttm if eps_ttm else None
            method_results.append({
                "method": "PEG", "intrinsic": round(fair_price, 2) if fair_price else None,
                "peg_value": round(peg_val, 2), "pe": round(pe_ttm, 2),
                "g_pct": round(np_cagr_pct, 2),
            })

    # PS
    if "PS" in methods_to_run:
        rev = _sf(income[0].get("revenue")) if income else None
        ps_series = [ps for km in key_metrics if (ps := _sf(km.get("priceToSalesRatio"))) and ps > 0]
        if rev and rev > 0 and len(ps_series) >= 3:
            ps_sorted = sorted(ps_series)
            rps = rev / total_shares if total_shares > 0 else 0
            intrinsic = _pct(ps_sorted, 50) * rps
            method_results.append({
                "method": "PS", "intrinsic": round(intrinsic, 2),
                "low": round(_pct(ps_sorted, 25) * rps, 2),
                "high": round(_pct(ps_sorted, 75) * rps, 2),
            })

    # DCF Scenarios
    if "DCF_Scenarios" in methods_to_run and income and cash_flow:
        rev = _sf(income[0].get("revenue")) or 0
        ni = _sf(income[0].get("netIncome")) or 0
        nm = ni / rev * 100 if rev > 0 else 5.0
        da = _sf(cash_flow[0].get("depreciationAndAmortization")) or 0
        capex_r = abs(_sf(cash_flow[0].get("capitalExpenditure")) or 0) / rev if rev > 0 else 0.05
        rg = rev_cagr_pct or 10.0

        def _proj(gfs, m_adj, cf):
            proj = []; r = rev
            for i in range(5):
                g = gfs[i] if i < len(gfs) else gfs[-1]
                r *= (1 + g / 100)
                proj.append(r * (nm + m_adj * (i + 1)) / 100 + da - r * capex_r * cf)
            return proj

        def _dcf_v(proj):
            if not proj or proj[-1] <= 0: tv = 0
            else: tv = proj[-1] * (1 + g_terminal / 100) / (wacc / 100 - g_terminal / 100)
            pv = sum(f / (1 + wacc / 100) ** (i + 1) for i, f in enumerate(proj))
            return ((pv + tv / (1 + wacc / 100) ** 5 + (cash or 0) - d0) / total_shares) if total_shares > 0 else 0

        v_opt = _dcf_v(_proj([rg] * 5, 0.5, 1.0))
        v_base = _dcf_v(_proj([rg * 0.7] * 5, 0.0, 1.0))
        v_pess = _dcf_v(_proj([rg * 0.4, rg * 0.4, 0, 0, 0], -0.3, 1.2))
        weighted = 0.25 * v_opt + 0.50 * v_base + 0.25 * v_pess
        method_results.append({
            "method": "DCF_Scenarios", "intrinsic": round(weighted, 2),
            "v_optimistic": round(v_opt, 2), "v_base": round(v_base, 2),
            "v_pessimistic": round(v_pess, 2), "scenario_weights": [25, 50, 25],
        })

    # ── 交叉验证 ──
    valid = [(r["method"], r["intrinsic"]) for r in method_results if r.get("intrinsic") and r["intrinsic"] > 0]
    if valid:
        available = {m for m, _ in valid}
        aw = {m: w for m, w in weights.items() if m in available}
        tw = sum(aw.values())
        if tw > 0: aw = {m: w / tw * 100 for m, w in aw.items()}
        weighted_avg = sum(v * aw.get(m, 100 / len(valid)) / 100 for m, v in valid)
        values = [v for _, v in valid]
        mean_v = statistics.mean(values)
        std_v = statistics.stdev(values) if len(values) >= 2 else 0
        cv = std_v / mean_v * 100 if mean_v > 0 else 0
        safety = (weighted_avg / price - 1) * 100 if price > 0 else 0

        if safety > 30: judgment = "显著低估"
        elif safety > 10: judgment = "低估"
        elif safety > -10: judgment = "合理"
        elif safety > -30: judgment = "偏高"
        else: judgment = "高估"
    else:
        weighted_avg, cv, safety, judgment = None, None, None, "N/A"

    cross_validation = {
        "weighted_avg": round(weighted_avg, 2) if weighted_avg else None,
        "cv": round(cv, 1) if cv is not None else None,
        "consistency": "高" if cv and cv < 15 else ("中" if cv and cv < 30 else "低"),
        "safety_margin": round(safety, 1) if safety is not None else None,
        "judgment": judgment,
        "current_price": round(price, 2),
    }

    return {
        "classification": classification,
        "wacc": wacc_data,
        "methods": method_results,
        "crossValidation": cross_validation,
    }


# ═══════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="EVI Toolkit — fetch FMP data + run valuation engine (forked from sirius)")
    parser.add_argument("--symbol", required=True, help="股票代码（如 1357.HK / AAPL / 600519.SS）")
    parser.add_argument("--market", default="", choices=["cn", "hk", "us", ""],
                        help="市场（空=自动推断）")
    parser.add_argument("--data-dir", default=None,
                        help="EVI 项目根目录（如 data/0700_HK）。若提供：FMP 原始数据写入 "
                             "<data-dir>/base/fmp/，financial_context 写入 <data-dir>/base/financials/，"
                             "engine_result 写入 <data-dir>/base/validation/。"
                             "若不提供：回退到 sirius 老布局 data/<symbol>/raw/。")
    parser.add_argument("--quarterly", action="store_true",
                        help="同时获取季度数据（period=quarter, limit=12）")
    parser.add_argument("--ttm", action="store_true",
                        help="自动计算 TTM 指标（Revenue/NI/EBITDA/FCF/PE/PS/EV_EBITDA）")
    parser.add_argument("--growth-rates", action="store_true",
                        help="自动计算增长率矩阵（YoY%/QoQ% for Revenue/NI/Margins）")
    args = parser.parse_args()

    symbol = args.symbol
    market = args.market
    if not market:
        if ".HK" in symbol.upper(): market = "hk"
        elif ".SS" in symbol.upper() or ".SZ" in symbol.upper(): market = "cn"
        else: market = "us"

    # ---- 输出目录策略 ----
    if args.data_dir:
        data_dir = Path(args.data_dir).resolve()
        raw_dir   = data_dir / "base" / "fmp"
        ctx_dir   = data_dir / "base" / "financials"
        engine_dir = data_dir / "base" / "validation"
        ctx_path  = ctx_dir / "financial_context.md"
        engine_path = engine_dir / "engine_result.json"
        out_dir = data_dir  # 仅用于日志显示
    else:
        out_dir = DATA_DIR / symbol.replace(".", "_")
        raw_dir = out_dir / "raw"
        ctx_path = out_dir / "financial_context.md"
        engine_path = out_dir / "engine_result.json"

    raw_dir.mkdir(parents=True, exist_ok=True)
    ctx_path.parent.mkdir(parents=True, exist_ok=True)
    engine_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("EVI Toolkit — fetch FMP + valuation engine")
    log.info("  Symbol: %s | Market: %s", symbol, market)
    log.info("  Output: %s", out_dir)
    log.info("=" * 60)

    # 1. 获取 FMP 数据
    log.info("\n[1/3] 获取 FMP 财务数据...")
    t0 = time.time()
    data = fetch_all(symbol)
    profile = data.get("profile")
    if not profile:
        log.error("FMP 无法获取 %s 的数据", symbol)
        sys.exit(1)
    log.info("  %s | 价格=%s | 市值=%s | %.1fs",
             profile.get("companyName"), profile.get("price"), profile.get("mktCap"), time.time() - t0)

    # 保存原始数据
    for key in ("profile", "incomeStatement", "balanceSheet", "cashFlow", "keyMetrics", "ratios"):
        fpath = raw_dir / f"{key}.json"
        fpath.write_text(json.dumps(data.get(key), ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    # 1b. 季度数据（如果请求）
    quarterly_data: Dict[str, Any] = {}
    if args.quarterly or args.ttm or args.growth_rates:
        log.info("\n[1b/3] 获取季度数据（最近12个季度）...")
        t_q = time.time()
        quarterly_data = fetch_quarterly(symbol)
        q_dir = raw_dir / "quarterly"
        q_dir.mkdir(parents=True, exist_ok=True)
        for key in ("incomeStatement", "balanceSheet", "cashFlow", "keyMetrics"):
            qd = quarterly_data.get(key)
            if qd:
                (q_dir / f"{key}.json").write_text(
                    json.dumps(qd, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        q_count = sum(len(v) for v in quarterly_data.values() if isinstance(v, list))
        log.info("  季度数据: %d 条记录 | %.1fs", q_count, time.time() - t_q)

    # 1c. TTM 指标
    if args.ttm and quarterly_data:
        log.info("\n[1c/3] 计算 TTM 指标...")
        ttm_result = compute_ttm(quarterly_data, profile)
        indicators_dir = ctx_dir if args.data_dir else (out_dir / "indicators")
        if args.data_dir:
            indicators_dir = data_dir / "base" / "financials" / "indicators"
        indicators_dir.mkdir(parents=True, exist_ok=True)
        ttm_path = indicators_dir / "ttm_metrics.json"
        ttm_path.write_text(json.dumps(ttm_result, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("  TTM: PE=%.2f PS=%.2f EV/EBITDA=%.2f → %s",
                 ttm_result.get("pe_ttm") or 0, ttm_result.get("ps_ttm") or 0,
                 ttm_result.get("ev_ebitda_ttm") or 0, ttm_path)

    # 1d. 增长率矩阵
    if args.growth_rates and quarterly_data:
        log.info("\n[1d/3] 计算增长率矩阵...")
        growth_result = compute_growth_rates(quarterly_data, data)
        indicators_dir = data_dir / "base" / "financials" / "indicators" if args.data_dir else (out_dir / "indicators")
        indicators_dir.mkdir(parents=True, exist_ok=True)
        growth_path = indicators_dir / "growth_rates.json"
        growth_path.write_text(json.dumps(growth_result, ensure_ascii=False, indent=2), encoding="utf-8")
        q_count = len(growth_result.get("quarterly") or [])
        a_count = len(growth_result.get("annual") or [])
        log.info("  增长率: %d 季度 + %d 年度 → %s", q_count, a_count, growth_path)

    # 2. 构建 financial_context
    log.info("\n[2/3] 构建 financial_context.md...")
    fin_ctx = build_financial_context(data)
    ctx_path.write_text(fin_ctx, encoding="utf-8")
    log.info("  %d 字符 → %s", len(fin_ctx), ctx_path)

    # 3. 运行估值引擎
    log.info("\n[3/3] 运行估值引擎...")
    t1 = time.time()
    engine_result = compute_valuation(data, market=market)
    engine_path.write_text(
        json.dumps(engine_result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    if engine_result.get("error"):
        log.warning("  %s", engine_result["error"])
    else:
        xv = engine_result.get("crossValidation", {})
        log.info("  分类=%s | 内在价值=%s | 安全边际=%s%% | 判断=%s",
                 engine_result.get("classification", {}).get("type"),
                 xv.get("weighted_avg"), xv.get("safety_margin"), xv.get("judgment"))
        for m in engine_result.get("methods", []):
            log.info("    - %s: %s/股", m.get("method"), m.get("intrinsic"))
    log.info("  %.1fs", time.time() - t1)

    # 输出摘要
    log.info("\n" + "=" * 60)
    log.info("数据准备完成！")
    log.info("📁 %s", out_dir)
    log.info("")
    log.info("文件清单：")
    log.info("  %s/*.json", raw_dir)
    log.info("  %s", ctx_path)
    log.info("  %s", engine_path)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
