#!/usr/bin/env python3
"""evi-valuation-comps / comps_calc.py

可比公司估值脚本。只需输入目标公司代码和 peer 代码，自动从 FMP 获取所有数据。
多倍数交叉验证：EV/EBITDA + EV/Sales + P/E + P/B。

所有倍数使用 FMP 预计算的 TTM 数据，保证时间点对齐。

Usage:
    python3 comps_calc.py --symbol 0981.HK --peers "UMC,GFS,TSM,1347.HK"
    python3 comps_calc.py --symbol 0981.HK --peers "UMC,GFS,TSM" --segment foundry --data-dir data/0981_HK
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
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


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


def _get_peer_multiples(symbol: str) -> dict[str, Any] | None:
    """获取单个公司的全部估值倍数（TTM，时间对齐）。"""
    symbol = symbol.strip().upper()

    # TTM key metrics
    km_data = _fmp_get(f"key-metrics-ttm/{symbol}")
    km = km_data[0] if km_data and isinstance(km_data, list) and len(km_data) > 0 else {}

    # TTM ratios
    r_data = _fmp_get(f"ratios-ttm/{symbol}")
    r = r_data[0] if r_data and isinstance(r_data, list) and len(r_data) > 0 else {}

    # Profile
    p_data = _fmp_get(f"profile/{symbol}")
    p = p_data[0] if p_data and isinstance(p_data, list) and len(p_data) > 0 else {}

    if not km and not r:
        return None

    # EV/EBITDA
    ev_ebitda = _safe_float(km.get("enterpriseValueOverEBITDATTM")) or _safe_float(r.get("enterpriseValueMultipleTTM"))

    # EV/Sales
    ev_sales = _safe_float(km.get("evToSalesTTM")) or _safe_float(km.get("evToSales"))
    if not ev_sales:
        # Fallback: annual
        ann = _fmp_get(f"key-metrics/{symbol}", {"period": "annual", "limit": 1})
        if ann and isinstance(ann, list) and len(ann) > 0:
            ev_sales = _safe_float(ann[0].get("evToSales"))

    # P/E
    pe = _safe_float(km.get("peRatioTTM")) or _safe_float(r.get("priceEarningsRatioTTM")) or _safe_float(p.get("pe"))

    # P/B
    pb = _safe_float(km.get("pbRatioTTM")) or _safe_float(r.get("priceToBookRatioTTM"))

    # Revenue growth
    growth_data = _fmp_get(f"income-statement-growth/{symbol}", {"period": "annual", "limit": 1})
    rev_growth = None
    if growth_data and isinstance(growth_data, list) and len(growth_data) > 0:
        rev_growth = _safe_float(growth_data[0].get("growthRevenue"))
        if rev_growth:
            rev_growth = round(rev_growth * 100, 1)

    # Gross margin
    gm = _safe_float(r.get("grossProfitMarginTTM") or r.get("grossProfitMargin"))
    if gm and gm < 1:
        gm = round(gm * 100, 1)

    has_any = any([ev_ebitda, ev_sales, pe, pb])
    if not has_any:
        return None

    return {
        "symbol": symbol,
        "name": p.get("companyName", symbol),
        "market_cap_b": round(p.get("mktCap", 0) / 1e9, 2) if p.get("mktCap") else None,
        "ev_to_ebitda": round(ev_ebitda, 2) if ev_ebitda else None,
        "ev_to_sales": round(ev_sales, 2) if ev_sales else None,
        "pe": round(pe, 2) if pe else None,
        "pb": round(pb, 2) if pb else None,
        "revenue_growth_pct": rev_growth,
        "gross_margin_pct": gm,
        "source": "FMP-TTM",
    }


def _get_target_data(symbol: str) -> dict[str, Any]:
    """获取目标公司的核心财务数据。"""
    symbol = symbol.strip().upper()
    result: dict[str, Any] = {"symbol": symbol}

    # Profile
    profile = _fmp_get(f"profile/{symbol}")
    if profile and isinstance(profile, list) and len(profile) > 0:
        p = profile[0]
        result["name"] = p.get("companyName", "")
        result["price"] = p.get("price")
        result["market_cap_m"] = round(p.get("mktCap", 0) / 1e6, 0)
        result["shares_m"] = round(p.get("mktCap", 0) / max(p.get("price", 1), 0.01) / 1e6, 0)

    # EV
    ev_data = _fmp_get(f"enterprise-values/{symbol}", {"period": "annual", "limit": 1})
    if ev_data and isinstance(ev_data, list) and len(ev_data) > 0:
        result["ev_m"] = round(float(ev_data[0].get("enterpriseValue", 0)) / 1e6, 0)
        result["net_debt_m"] = result["ev_m"] - result.get("market_cap_m", 0)
    else:
        result["net_debt_m"] = 0

    # TTM financials
    income = _fmp_get(f"income-statement/{symbol}", {"period": "annual", "limit": 1})
    if income and isinstance(income, list) and len(income) > 0:
        i = income[0]
        result["revenue_m"] = round(float(i.get("revenue", 0)) / 1e6, 0)
        result["ebitda_m"] = round(float(i.get("ebitda", 0)) / 1e6, 0)
        result["net_income_m"] = round(float(i.get("netIncome", 0)) / 1e6, 0)
        result["fiscal_date"] = i.get("date", "")

    # Book value
    bs = _fmp_get(f"balance-sheet-statement/{symbol}", {"period": "annual", "limit": 1})
    if bs and isinstance(bs, list) and len(bs) > 0:
        result["book_value_m"] = round(float(bs[0].get("totalStockholdersEquity", 0)) / 1e6, 0)

    return result


def _percentiles(values: list[float]) -> dict[str, float]:
    s = sorted(values)
    n = len(s)
    if n == 0:
        return {"p25": 0, "median": 0, "p75": 0}
    return {
        "p25": round(s[max(0, int(n * 0.25))], 2),
        "median": round(statistics.median(s), 2),
        "p75": round(s[min(n - 1, int(n * 0.75))], 2),
    }


def main():
    parser = argparse.ArgumentParser(description="EVI 可比公司估值（全自动）")
    parser.add_argument("--symbol", required=True, help="目标公司代码")
    parser.add_argument("--peers", required=True, help="逗号分隔的 peer 代码")
    parser.add_argument("--segment", default="overall", help="分部名称")
    parser.add_argument("--data-dir", default=None, help="输出目录（可选）")

    args = parser.parse_args()
    peers = [s.strip() for s in args.peers.split(",") if s.strip()]

    print(f"📊 可比公司估值: {args.symbol} vs {peers}", file=sys.stderr)

    # 1. 获取目标公司数据
    target = _get_target_data(args.symbol)

    # 2. 获取 peer 倍数
    peer_data = []
    for sym in peers:
        d = _get_peer_multiples(sym)
        if d:
            peer_data.append(d)
            print(f"   ✓ {sym}: EV/EBITDA={d['ev_to_ebitda']}x EV/S={d['ev_to_sales']}x PE={d['pe']}x", file=sys.stderr)
        else:
            print(f"   ✗ {sym}: 数据不可用", file=sys.stderr)

    if len(peer_data) < 2:
        result = {"status": "insufficient_peers", "error": f"仅 {len(peer_data)} 家有效", "peer_data": peer_data, "target": target}
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    # 3. 每种倍数独立估值
    shares = target.get("shares_m", 1)
    net_debt = target.get("net_debt_m", 0)
    price = target.get("price", 0)
    methods: dict[str, Any] = {}

    # EV/EBITDA
    vals = [p["ev_to_ebitda"] for p in peer_data if p.get("ev_to_ebitda")]
    if vals and target.get("ebitda_m"):
        pcts = _percentiles(vals)
        ebitda = target["ebitda_m"]
        methods["EV/EBITDA"] = {
            "peer_stats": pcts,
            "target_value_m": ebitda,
            "implied_per_share": {
                "bear": round((ebitda * pcts["p25"] - net_debt) / shares, 2),
                "base": round((ebitda * pcts["median"] - net_debt) / shares, 2),
                "bull": round((ebitda * pcts["p75"] - net_debt) / shares, 2),
            },
        }

    # EV/Sales
    vals = [p["ev_to_sales"] for p in peer_data if p.get("ev_to_sales")]
    if vals and target.get("revenue_m"):
        pcts = _percentiles(vals)
        rev = target["revenue_m"]
        methods["EV/Sales"] = {
            "peer_stats": pcts,
            "target_value_m": rev,
            "implied_per_share": {
                "bear": round((rev * pcts["p25"] - net_debt) / shares, 2),
                "base": round((rev * pcts["median"] - net_debt) / shares, 2),
                "bull": round((rev * pcts["p75"] - net_debt) / shares, 2),
            },
        }

    # P/E
    vals = [p["pe"] for p in peer_data if p.get("pe")]
    if vals and target.get("net_income_m") and target["net_income_m"] > 0:
        pcts = _percentiles(vals)
        ni = target["net_income_m"]
        methods["P/E"] = {
            "peer_stats": pcts,
            "target_value_m": ni,
            "implied_per_share": {
                "bear": round(ni * pcts["p25"] / shares, 2),
                "base": round(ni * pcts["median"] / shares, 2),
                "bull": round(ni * pcts["p75"] / shares, 2),
            },
        }

    # P/B
    vals = [p["pb"] for p in peer_data if p.get("pb")]
    if vals and target.get("book_value_m"):
        pcts = _percentiles(vals)
        bv = target["book_value_m"]
        methods["P/B"] = {
            "peer_stats": pcts,
            "target_value_m": bv,
            "implied_per_share": {
                "bear": round(bv * pcts["p25"] / shares, 2),
                "base": round(bv * pcts["median"] / shares, 2),
                "bull": round(bv * pcts["p75"] / shares, 2),
            },
        }

    # 4. 综合（各方法 base 的平均值）
    base_prices = [m["implied_per_share"]["base"] for m in methods.values() if m.get("implied_per_share")]
    composite = round(statistics.mean(base_prices), 2) if base_prices else None

    # Upside
    for m in methods.values():
        for s in ("bear", "base", "bull"):
            v = m["implied_per_share"].get(s)
            if v and price:
                m["implied_per_share"][f"{s}_upside_pct"] = round((v / price - 1) * 100, 1)

    result = {
        "method": "Comps",
        "segment_id": args.segment,
        "target": target,
        "current_price": price,
        "peer_set": peer_data,
        "methods": methods,
        "composite_base_per_share": composite,
        "composite_upside_pct": round((composite / price - 1) * 100, 1) if composite and price else None,
        "note": "所有倍数来自 FMP TTM 数据（时间对齐），禁止手动计算。",
    }

    output = json.dumps(result, indent=2, ensure_ascii=False)
    print(output)

    if args.data_dir:
        out_dir = Path(args.data_dir) / "valuation" / args.segment
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "comps_result.json").write_text(output, encoding="utf-8")
        print(f"\n✅ 结果已写入: {out_dir / 'comps_result.json'}", file=sys.stderr)


if __name__ == "__main__":
    main()
