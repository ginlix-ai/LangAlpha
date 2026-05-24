#!/usr/bin/env python3
"""evi-valuation-dcf / dcf_calc.py

Deterministic 3-scenario DCF for ONE business segment. Reads the assumption
files prepared by evi-assumption-builder; writes:

    data/{symbol_dir}/valuation/{segment}/dcf_result.json

Inputs (relative to --data-dir):
    valuation/{segment}/assumption_ledger.json
    valuation/{segment}/growth_bridge.json
    valuation/{segment}/margin_bridge.json
    valuation/{segment}/risk_adjustment.json
    valuation/group/assumption_ledger.json     (company WACC inputs)

Conventions:
    revenue          → from growth_bridge.rows[year].revenue (number or {bear,base,bull})
    ebit_margin      → from margin_bridge.rows[year].ebit_margin (% or {bear,base,bull})
    tax_rate         → group assumption_ledger.tax_rate_pct  (default 22%)
    capex_to_rev     → margin_bridge.rows[year].capex_to_rev_pct (default 5%)
    da_to_rev        → margin_bridge.rows[year].da_to_rev_pct    (default 4%)
    nwc_to_rev_pct   → margin_bridge.rows[year].nwc_change_pct   (default 1%)
    wacc             → group.wacc_pct + risk_adjustment.wacc_premium_bps/100
    terminal_growth  → CLI --terminal-growth or risk_adjustment.terminal_growth_pct

Usage:
    python3 dcf_calc.py --data-dir data/0700_HK --segment cloud
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


SCENARIOS = ("bear", "base", "bull")


def _read_json(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _scenarios(v: Any, fallback: float | None = None) -> dict[str, float]:
    """Normalize a scalar or {bear,base,bull} dict into a 3-scenario dict."""
    if isinstance(v, dict):
        out = {}
        for s in SCENARIOS:
            if s in v:
                out[s] = float(v[s])
            elif fallback is not None:
                out[s] = float(fallback)
        return out
    if v is None:
        return {s: float(fallback) for s in SCENARIOS} if fallback is not None else {}
    return {s: float(v) for s in SCENARIOS}


def _row_year(row: dict) -> str:
    return str(row.get("year") or row.get("period") or "")


def _ledger_var(ledger: dict | None, name: str) -> float | None:
    if not ledger:
        return None
    for a in ledger.get("assumptions") or []:
        if a.get("variable") == name and a.get("value") is not None:
            try:
                return float(a["value"])
            except Exception:
                return None
    return None


def _present_value(cashflows: list[float], wacc: float) -> float:
    pv = 0.0
    for t, cf in enumerate(cashflows, start=1):
        pv += cf / ((1 + wacc) ** t)
    return pv


def calc(
    data_dir: Path, segment: str, years: int, cli_terminal_growth: float | None
) -> dict[str, Any]:
    seg_dir = data_dir / "valuation" / segment
    group_dir = data_dir / "valuation" / "group"

    growth = _read_json(seg_dir / "growth_bridge.json") or {}
    margin = _read_json(seg_dir / "margin_bridge.json") or {}
    risk   = _read_json(seg_dir / "risk_adjustment.json") or {}
    ledger = _read_json(seg_dir / "assumption_ledger.json") or {}
    group  = _read_json(group_dir / "assumption_ledger.json") or {}

    # --- WACC ---
    base_wacc = _ledger_var(group, "wacc_pct") or _ledger_var(group, "wacc") or 9.0  # %
    premium_bps = float(risk.get("wacc_premium_bps") or 0)
    wacc_pct = base_wacc + premium_bps / 100.0
    wacc = wacc_pct / 100.0

    tax_rate = (_ledger_var(group, "tax_rate_pct") or 22.0) / 100.0
    terminal_g = (
        cli_terminal_growth
        if cli_terminal_growth is not None
        else (risk.get("terminal_growth_pct") or 2.5)
    ) / 100.0
    exec_risk = float(risk.get("execution_risk_factor") or 1.0)

    # --- year-by-year forecast (max `years` years from growth_bridge.rows) ---
    growth_rows = growth.get("rows") or []
    margin_rows = margin.get("rows") or []
    margin_by_year = {_row_year(r): r for r in margin_rows}
    growth_by_year = {_row_year(r): r for r in growth_rows}

    # determine forecast years: those rows whose year ends with E (estimate) or are after first 'A'
    forecast_years: list[str] = []
    for r in growth_rows:
        y = _row_year(r)
        if y.endswith("E"):
            forecast_years.append(y)
    forecast_years = forecast_years[:years]

    if not forecast_years:
        return {
            "method": "DCF", "segment_id": segment,
            "status": "missing_inputs",
            "missing": ["growth_bridge.rows with year ending in 'E'"],
        }

    fcf_paths: dict[str, list[float]] = {s: [] for s in SCENARIOS}
    for y in forecast_years:
        g = growth_by_year.get(y) or {}
        m = margin_by_year.get(y) or {}
        rev = _scenarios(g.get("revenue"))
        ebit_m = _scenarios(m.get("ebit_margin"), fallback=15.0)
        capex_r = _scenarios(m.get("capex_to_rev_pct"), fallback=5.0)
        da_r = _scenarios(m.get("da_to_rev_pct"), fallback=4.0)
        nwc_r = _scenarios(m.get("nwc_change_pct"), fallback=1.0)
        for s in SCENARIOS:
            r = rev.get(s)
            if r is None:
                continue
            ebit = r * ebit_m[s] / 100.0
            nopat = ebit * (1 - tax_rate)
            da = r * da_r[s] / 100.0
            capex = r * capex_r[s] / 100.0
            nwc = r * nwc_r[s] / 100.0
            fcf = nopat + da - capex - nwc
            fcf_paths[s].append(fcf)

    # require all 3 scenarios non-empty
    missing = [s for s, lst in fcf_paths.items() if not lst]
    if missing:
        return {
            "method": "DCF", "segment_id": segment,
            "status": "missing_inputs",
            "missing": [f"scenario={s} has no forecast cashflows" for s in missing],
        }

    values: dict[str, float] = {}
    for s in SCENARIOS:
        cf = fcf_paths[s]
        last_fcf = cf[-1]
        terminal = last_fcf * (1 + terminal_g) / max(wacc - terminal_g, 1e-6)
        # discount terminal at year N
        n = len(cf)
        pv_terminal = terminal / ((1 + wacc) ** n)
        pv = _present_value(cf, wacc) + pv_terminal
        values[s] = round(pv * exec_risk, 2)

    # --- sensitivity (only on base) ---
    def _scenario_value(wacc_override: float, term_override: float, margin_delta_pp: float) -> float:
        cf = []
        for y in forecast_years:
            g = growth_by_year.get(y) or {}
            m = margin_by_year.get(y) or {}
            rev = _scenarios(g.get("revenue")).get("base")
            if rev is None:
                continue
            base_em = _scenarios(m.get("ebit_margin"), fallback=15.0)["base"] + margin_delta_pp
            capex = rev * _scenarios(m.get("capex_to_rev_pct"), fallback=5.0)["base"] / 100.0
            da = rev * _scenarios(m.get("da_to_rev_pct"), fallback=4.0)["base"] / 100.0
            nwc = rev * _scenarios(m.get("nwc_change_pct"), fallback=1.0)["base"] / 100.0
            ebit = rev * base_em / 100.0
            nopat = ebit * (1 - tax_rate)
            cf.append(nopat + da - capex - nwc)
        last_fcf = cf[-1]
        terminal = last_fcf * (1 + term_override) / max(wacc_override - term_override, 1e-6)
        n = len(cf)
        pv = _present_value(cf, wacc_override) + terminal / ((1 + wacc_override) ** n)
        return pv * exec_risk

    base_value = values["base"]
    sens = []
    for label, w_off, t_off, em_off in [
        ("WACC +100bps",       wacc + 0.01, terminal_g, 0.0),
        ("WACC -100bps",       wacc - 0.01, terminal_g, 0.0),
        ("Terminal +50bps",    wacc, terminal_g + 0.005, 0.0),
        ("Terminal -50bps",    wacc, terminal_g - 0.005, 0.0),
        ("EBIT margin +100bps",wacc, terminal_g, 1.0),
        ("EBIT margin -100bps",wacc, terminal_g, -1.0),
    ]:
        v = _scenario_value(w_off, t_off, em_off)
        sens.append({
            "variable": label,
            "value":    round(v, 2),
            "value_pct_change": round((v - base_value) / base_value * 100, 2),
        })

    return {
        "method": "DCF",
        "segment_id": segment,
        "status": "ok",
        "values": values,
        "currency": ledger.get("currency") or "unspecified",
        "key_assumptions": [a.get("assumption_id") for a in (ledger.get("assumptions") or []) if a.get("assumption_id")],
        "wacc_used": round(wacc_pct, 2),
        "terminal_growth": round(terminal_g * 100, 2),
        "tax_rate":  round(tax_rate * 100, 2),
        "execution_risk_factor": exec_risk,
        "forecast_years": forecast_years,
        "sensitivity": sens,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--segment", required=True)
    p.add_argument("--years", type=int, default=10)
    p.add_argument("--terminal-growth", type=float, default=None,
                   help="In percent, e.g. 2.5. Overrides risk_adjustment if given.")
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    res = calc(data_dir, args.segment, args.years, args.terminal_growth)

    out_path = data_dir / "valuation" / args.segment / "dcf_result.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[dcf_calc] {args.segment} → {out_path}")
    print(f"[dcf_calc] status={res.get('status')}")
    if res.get("status") == "ok":
        v = res["values"]
        print(f"[dcf_calc] values bear={v['bear']:,.0f} base={v['base']:,.0f} bull={v['bull']:,.0f}")
    return 0 if res.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
