#!/usr/bin/env python3
"""evi-valuation-orchestrator / aggregate.py

Combine multiple {method}_result.json files for ONE segment into
final_segment_valuation.json with default weights.

Default weights:
  - primary       : 0.5
  - cross_check(s): split remaining 0.5 evenly
  - if (cross_check.values.base - primary.values.base) / primary.values.base > 30%:
        halve that cross_check's weight; renormalize.
  - method whose confidence < 0.4 → weight halved.

The "primary" method is determined from valuation_method_matrix.json; if
absent, falls back to "DCF" if present, else first method.

Usage:
    python3 aggregate.py --data-dir data/0700_HK --segment cloud
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


METHODS = ("DCF", "PS", "PEG", "Comps", "DDM")
SCENARIOS = ("bear", "base", "bull")


def _read_json(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _segment_role_map(matrix: dict | None, segment: str) -> dict[str, str]:
    """method → role from valuation_method_matrix.json."""
    out: dict[str, str] = {}
    if not isinstance(matrix, dict):
        return out
    for row in matrix.get("matrix") or []:
        if row.get("segment_id") == segment:
            for m in row.get("methods") or []:
                if m.get("method") and m.get("role"):
                    out[m["method"]] = m["role"]
    return out


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    s = sum(weights.values())
    if s <= 0:
        return weights
    return {k: round(v / s, 4) for k, v in weights.items()}


def aggregate(data_dir: Path, segment: str) -> dict[str, Any]:
    seg_dir = data_dir / "valuation" / segment
    matrix = _read_json(data_dir / "valuation_method_matrix.json")
    role_map = _segment_role_map(matrix, segment)

    results: list[dict[str, Any]] = []
    for m in METHODS:
        r = _read_json(seg_dir / f"{m.lower()}_result.json")
        if not r or r.get("status") and r["status"] != "ok":
            continue
        if not isinstance(r.get("values"), dict):
            continue
        results.append({
            "method": m,
            "role":   role_map.get(m, "cross_check"),
            "values": r["values"],
            "confidence": float(r.get("confidence") or 0.5),
            "currency":   r.get("currency"),
        })

    if not results:
        return {
            "segment_id": segment,
            "status": "no_method_results",
            "method_results": [],
            "final_values": {},
        }

    # Pick primary
    primary = next((r for r in results if r["role"] == "primary"), None)
    if primary is None:
        primary = next((r for r in results if r["method"] == "DCF"), results[0])
        primary["role"] = "primary"

    # Initial weights
    weights: dict[str, float] = {}
    cross = [r for r in results if r is not primary]
    weights[primary["method"]] = 0.5
    if cross:
        each = 0.5 / len(cross)
        for r in cross:
            weights[r["method"]] = each
    else:
        weights[primary["method"]] = 1.0

    # Penalize cross_check far from primary
    primary_base = float(primary["values"].get("base") or 0.0)
    if primary_base != 0:
        for r in cross:
            cb = float(r["values"].get("base") or 0.0)
            if cb == 0:
                continue
            dev = abs(cb - primary_base) / abs(primary_base)
            if dev > 0.30:
                weights[r["method"]] *= 0.5

    # Penalize low confidence
    for r in results:
        if r["confidence"] < 0.4:
            weights[r["method"]] *= 0.5

    weights = _normalize_weights(weights)

    # Combined values
    final_values: dict[str, float] = {}
    for s in SCENARIOS:
        v = 0.0
        wsum = 0.0
        for r in results:
            val = r["values"].get(s)
            if val is None:
                continue
            w = weights.get(r["method"], 0.0)
            v += float(val) * w
            wsum += w
        if wsum > 0:
            final_values[s] = round(v / wsum, 2)

    # Consistency: 1 - relative stdev of base values
    base_vals = [float(r["values"].get("base")) for r in results if r["values"].get("base") is not None]
    consistency = 1.0
    if len(base_vals) >= 2 and sum(base_vals) > 0:
        mean = sum(base_vals) / len(base_vals)
        var = sum((x - mean) ** 2 for x in base_vals) / len(base_vals)
        sd = math.sqrt(var)
        consistency = round(max(0.0, 1.0 - sd / abs(mean)), 3)

    currency = next((r["currency"] for r in results if r.get("currency")), None)

    return {
        "schema_version": 1,
        "segment_id": segment,
        "status": "ok",
        "method_results": [
            {
                "method": r["method"],
                "role":   r["role"],
                "weight": weights.get(r["method"], 0.0),
                "values": r["values"],
                "confidence": r["confidence"],
            }
            for r in results
        ],
        "final_values": final_values,
        "currency":     currency,
        "consistency_score": consistency,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--segment", required=True)
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    res = aggregate(data_dir, args.segment)

    out_path = data_dir / "valuation" / args.segment / "final_segment_valuation.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[aggregate] {args.segment} → {out_path}")
    print(f"[aggregate] status={res.get('status')} methods="
          f"{[m['method'] for m in res.get('method_results') or []]}")
    if res.get("status") == "ok":
        v = res.get("final_values") or {}
        print(f"[aggregate] final base={v.get('base')} consistency={res['consistency_score']}")
    return 0 if res.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
