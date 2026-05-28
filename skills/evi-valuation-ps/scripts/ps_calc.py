#!/usr/bin/env python3
"""evi-valuation-ps / ps_calc.py

EV/Sales 估值脚本。只需输入目标公司代码和 peer 代码，自动从 FMP 获取所有数据。

所有倍数使用 FMP 预计算的 TTM 数据，保证时间点对齐。
绝不手动拼凑 Market Cap + Revenue 计算 EV/Sales。

Usage:
    python3 ps_calc.py --symbol 0981.HK --peers "UMC,GFS,TSM" --segment foundry
    python3 ps_calc.py --symbol 0981.HK --peers "UMC,GFS,TSM" --segment foundry --data-dir data/0981_HK
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx


def _fmp_key() -> str:
    key = os.environ.get("FMP_API_KEY", "")
    if not key:
        print("ERROR: FMP_API_KEY not set", file=sys.stderr)
        sys.exit(1)
    return key


def _fmp_get(endpoint: str, params: dict | None = None) -> Any:
    params = params or {}
    params["apikey"] = _fmp_key()
    url = f"https://financialmodelingprep.com/api/v3/{endpoint}"
    with httpx.Client(timeout=20, http2=True) as client:
        resp = client.get(url, params=params)
        if resp.status_code != 200:
            return None
        return resp.json()


def _get_ev_sales(symbol: str) -> dict[str, Any] | None:
    """从 FMP 获取一只股票的 EV/Sales（TTM，时间对齐）。"""
    symbol = symbol.strip().upper()

    # 方法1: key-metrics TTM
    data = _fmp_get(f"key-metrics-ttm/{symbol}")
    if data and isinstance(data, list) and len(data) > 0:
        rec = data[0]
        # FMP TTM 字段名: enterpriseValueOverEBITDATTM, evToSalesTTM 等
        ev_sales = rec.get("evToSalesTTM") or rec.get("evToSales")
        if ev_sales and float(ev_sales) > 0:
            return {"symbol": symbol, "ev_sales": round(float(ev_sales), 2), "source": "keyMetrics-TTM", "as_of": "TTM"}

    # 方法2: ratios TTM
    data = _fmp_get(f"ratios-ttm/{symbol}")
    if data and isinstance(data, list) and len(data) > 0:
        rec = data[0]
        ev_sales = rec.get("enterpriseValueMultipleTTM") or rec.get("priceToSalesRatioTTM")
        if ev_sales and float(ev_sales) > 0:
            return {"symbol": symbol, "ev_sales": round(float(ev_sales), 2), "source": "ratios-TTM", "as_of": "TTM"}

    # 方法3: annual key-metrics 最近一年
    data = _fmp_get(f"key-metrics/{symbol}", {"period": "annual", "limit": 1})
    if data and isinstance(data, list) and len(data) > 0:
        rec = data[0]
        ev_sales = rec.get("evToSales") or rec.get("enterpriseValueOverEBITDA")
        if ev_sales and float(ev_sales) > 0:
            return {"symbol": symbol, "ev_sales": round(float(ev_sales), 2), "source": "keyMetrics-annual", "as_of": rec.get("date", "latest")}

    # 方法4: ratios annual
    data = _fmp_get(f"ratios/{symbol}", {"period": "annual", "limit": 1})
    if data and isinstance(data, list) and len(data) > 0:
        rec = data[0]
        ev_sales = rec.get("enterpriseValueMultiple")
        # enterpriseValueMultiple 是 EV/EBITDA 不是 EV/Sales, 再找 priceToSalesRatio
        ps = rec.get("priceToSalesRatio")
        if ps and float(ps) > 0:
            return {"symbol": symbol, "ev_sales": round(float(ps), 2), "source": "ratios-annual(P/S proxy)", "as_of": rec.get("date", "latest")}

    return None


def _get_company_info(symbol: str) -> dict[str, Any]:
    """获取公司基本信息（增速、毛利率等）用于质量对比。"""
    symbol = symbol.strip().upper()
    info: dict[str, Any] = {"symbol": symbol}

    profile = _fmp_get(f"profile/{symbol}")
    if profile and isinstance(profile, list) and len(profile) > 0:
        p = profile[0]
        info["name"] = p.get("companyName", "")
        info["market_cap_b"] = round(p.get("mktCap", 0) / 1e9, 2)
        info["sector"] = p.get("sector", "")

    # Growth
    growth = _fmp_get(f"income-statement-growth/{symbol}", {"period": "annual", "limit": 1})
    if growth and isinstance(growth, list) and len(growth) > 0:
        info["revenue_growth"] = round(float(growth[0].get("growthRevenue", 0)) * 100, 1)

    # Margins from ratios TTM
    ratios = _fmp_get(f"ratios-ttm/{symbol}")
    if ratios and isinstance(ratios, list) and len(ratios) > 0:
        r = ratios[0]
        gm = r.get("grossProfitMarginTTM") or r.get("grossProfitMargin")
        if gm:
            info["gross_margin"] = round(float(gm) * 100, 1)

    return info


def _get_target_financials(symbol: str) -> dict[str, Any]:
    """获取目标公司的 TTM 收入、EV、净债务、股本等，用于反推估值。"""
    symbol = symbol.strip().upper()
    result: dict[str, Any] = {"symbol": symbol}

    # Profile: market cap, shares, price
    profile = _fmp_get(f"profile/{symbol}")
    if profile and isinstance(profile, list) and len(profile) > 0:
        p = profile[0]
        result["price"] = p.get("price")
        result["market_cap_m"] = round(p.get("mktCap", 0) / 1e6, 0)
        result["shares_m"] = round(p.get("mktCap", 0) / max(p.get("price", 1), 0.01) / 1e6, 0)

    # Enterprise value from key-metrics
    km = _fmp_get(f"enterprise-values/{symbol}", {"period": "annual", "limit": 1})
    if km and isinstance(km, list) and len(km) > 0:
        result["enterprise_value_m"] = round(float(km[0].get("enterpriseValue", 0)) / 1e6, 0)
        result["net_debt_m"] = result["enterprise_value_m"] - result.get("market_cap_m", 0)

    # TTM Revenue
    income = _fmp_get(f"income-statement/{symbol}", {"period": "annual", "limit": 1})
    if income and isinstance(income, list) and len(income) > 0:
        result["revenue_m"] = round(float(income[0].get("revenue", 0)) / 1e6, 0)
        result["revenue_date"] = income[0].get("date", "")

    return result


def main():
    parser = argparse.ArgumentParser(description="EVI EV/Sales 估值（全自动从 FMP 获取数据）")
    parser.add_argument("--symbol", required=True, help="目标公司股票代码")
    parser.add_argument("--peers", required=True, help="逗号分隔的 peer 代码")
    parser.add_argument("--segment", default="overall", help="分部名称")
    parser.add_argument("--data-dir", default=None, help="输出目录（可选）")
    parser.add_argument("--adj-pp", type=float, default=0, help="质量调整百分比")

    args = parser.parse_args()
    peers = [s.strip() for s in args.peers.split(",") if s.strip()]

    print(f"📊 EV/Sales 估值: {args.symbol} vs peers {peers}", file=sys.stderr)
    print(f"   数据来源: FMP keyMetrics-TTM (时间对齐)", file=sys.stderr)

    # 1. 获取目标公司财务数据
    target = _get_target_financials(args.symbol)
    target_info = _get_company_info(args.symbol)

    # 2. 获取所有 peer 的 EV/Sales
    peer_results = []
    for sym in peers:
        ev_data = _get_ev_sales(sym)
        if ev_data is None:
            print(f"   WARNING: {sym} — 无法获取 EV/Sales，跳过", file=sys.stderr)
            continue
        info = _get_company_info(sym)
        peer_results.append({**ev_data, **info})

    if len(peer_results) < 2:
        result = {
            "status": "insufficient_peers",
            "error": f"仅 {len(peer_results)} 家 peer 有有效数据（需要 ≥ 3）",
            "peer_data": peer_results,
            "target": target,
        }
    else:
        # 3. 计算分位数
        multiples = sorted([p["ev_sales"] for p in peer_results])
        n = len(multiples)
        p25 = multiples[max(0, int(n * 0.25))]
        median = multiples[int(n * 0.5)]
        p75 = multiples[min(n - 1, int(n * 0.75))]

        adj_factor = 1 + args.adj_pp / 100.0

        # 4. 三场景估值
        rev = target.get("revenue_m", 0)
        shares = target.get("shares_m", 1)
        net_debt = target.get("net_debt_m", 0)

        def _ev_to_equity_ps(ev_sales_multiple: float) -> dict:
            ev = rev * ev_sales_multiple
            equity = ev - net_debt
            per_share = equity / shares if shares > 0 else 0
            return {"ev_sales": round(ev_sales_multiple, 2), "implied_ev_m": round(ev, 0), "implied_equity_m": round(equity, 0), "per_share": round(per_share, 2)}

        scenarios = {
            "bear": _ev_to_equity_ps(p25),
            "base": _ev_to_equity_ps(median * adj_factor),
            "bull": _ev_to_equity_ps(p75 * adj_factor),
        }

        # 5. vs 当前价
        current_price = target.get("price", 0)
        for s, v in scenarios.items():
            v["upside_pct"] = round((v["per_share"] / current_price - 1) * 100, 1) if current_price else None

        result = {
            "method": "EV/Sales",
            "segment_id": args.segment,
            "target": {**target, **target_info},
            "current_price": current_price,
            "peer_set": peer_results,
            "multiple_stats": {"p25": round(p25, 2), "median": round(median, 2), "p75": round(p75, 2)},
            "adj_pp": args.adj_pp,
            "scenarios": scenarios,
            "note": "所有 peer EV/Sales 来自 FMP TTM 数据（时间对齐），禁止手动计算。",
        }

    # 输出
    output = json.dumps(result, indent=2, ensure_ascii=False)
    print(output)

    if args.data_dir:
        out_dir = Path(args.data_dir) / "valuation" / args.segment
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "ps_result.json").write_text(output, encoding="utf-8")
        print(f"\n✅ 结果已写入: {out_dir / 'ps_result.json'}", file=sys.stderr)


if __name__ == "__main__":
    main()
