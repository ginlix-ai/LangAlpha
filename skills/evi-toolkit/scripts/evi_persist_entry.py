#!/usr/bin/env python3
"""evi-toolkit / evi_persist_entry.py

Persist the final EVI valuation result to the LangAlpha backend.

Reads from the standard EVI project layout:

  data/{symbol_dir}/
    business_segments.json
    valuation_method_matrix.json
    information/indexed_facts.json
    valuation/group/{assumption_ledger,reverse_valuation,final_company_valuation}.json
    valuation/{segment_id}/{assumption_ledger,*_result,final_segment_valuation}.json
    monitor/{revaluation_tasks,trigger_log}.json

Builds the EVI payload (schema documented in evi-toolkit/SKILL.md §3) and
POSTs to:

  POST {LANGALPHA_API_BASE}/api/v1/templates/_internal/entries/{entry_id}/finalize

Usage:
  python3 evi_persist_entry.py --entry-id <uuid> --data-dir data/{symbol_dir}
  python3 evi_persist_entry.py --entry-id <uuid> --data-dir ... --status partial
  python3 evi_persist_entry.py --entry-id <uuid> --status failed --error-message "..."
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------

def _read_json(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[evi_persist] WARN bad json {p}: {e}", file=sys.stderr)
        return None


def _api_base() -> str:
    base = os.environ.get("LANGALPHA_API_BASE")
    return (base or "http://localhost:8000").rstrip("/")


# ---------------------------------------------------------------------------
# Payload construction
# ---------------------------------------------------------------------------

def _collect_segments(data_dir: Path) -> dict[str, dict[str, Any]]:
    """Scan valuation/{segment_id}/ directories (excluding 'group')."""
    val_root = data_dir / "valuation"
    segs: dict[str, dict[str, Any]] = {}
    if not val_root.exists():
        return segs
    for child in sorted(val_root.iterdir()):
        if not child.is_dir() or child.name == "group":
            continue
        seg_id = child.name
        results: dict[str, Any] = {}
        for method in ("dcf", "ps", "peg", "comps", "ddm"):
            r = _read_json(child / f"{method}_result.json")
            if r is not None:
                results[method.upper()] = r
        seg_payload = {
            "name": seg_id,
            "assumption_ledger": _read_json(child / "assumption_ledger.json"),
            "growth_bridge":     _read_json(child / "growth_bridge.json"),
            "margin_bridge":     _read_json(child / "margin_bridge.json"),
            "risk_adjustment":   _read_json(child / "risk_adjustment.json"),
            "results":           results or None,
            "final":             _read_json(child / "final_segment_valuation.json"),
        }
        # Pull a friendlier name from final / assumption_ledger if available.
        for src in (seg_payload.get("final"), seg_payload.get("assumption_ledger")):
            if isinstance(src, dict):
                nm = src.get("segment_name") or src.get("name")
                if nm:
                    seg_payload["name"] = nm
                    break
        segs[seg_id] = seg_payload
    return segs


def _build_payload(data_dir: Path, company: dict[str, Any]) -> dict[str, Any]:
    business_segments = _read_json(data_dir / "business_segments.json")
    matrix = _read_json(data_dir / "valuation_method_matrix.json")
    indexed_facts = _read_json(data_dir / "information/indexed_facts.json") or {}
    facts_list = indexed_facts.get("facts") or []

    by_segment: dict[str, int] = {}
    high = 0
    for f in facts_list:
        sid = f.get("segment_id") or f.get("segment") or "_unassigned"
        by_segment[sid] = by_segment.get(sid, 0) + 1
        rel = (f.get("reliability") or "").lower()
        if rel in ("high", "高"):
            high += 1
    facts_summary = {
        "total": len(facts_list),
        "by_segment": by_segment,
        "high_reliability_pct": (round(high / len(facts_list), 3) if facts_list else 0.0),
    }

    segments = _collect_segments(data_dir)

    group = {
        "assumption_ledger": _read_json(data_dir / "valuation/group/assumption_ledger.json"),
        "reverse_valuation": _read_json(data_dir / "valuation/group/reverse_valuation.json"),
        "final":             _read_json(data_dir / "valuation/group/final_company_valuation.json"),
    }

    monitor_tasks = _read_json(data_dir / "monitor/revaluation_tasks.json") or []
    if isinstance(monitor_tasks, dict):
        monitor_tasks = monitor_tasks.get("tasks") or []
    trigger_log = _read_json(data_dir / "monitor/trigger_log.json") or []
    last_run = trigger_log[-1] if isinstance(trigger_log, list) and trigger_log else None
    monitor = {
        "last_run_id":       (last_run or {}).get("monitor_run_id") if isinstance(last_run, dict) else None,
        "last_checked_at":   (last_run or {}).get("checked_at") if isinstance(last_run, dict) else None,
        "open_tasks":        [t for t in monitor_tasks if (t or {}).get("status") != "done"],
    }

    return {
        "schema_version":            "evi-1.0",
        "company":                   company,
        "business_segments":         business_segments,
        "valuation_method_matrix":   matrix,
        "segments":                  segments,
        "group":                     group,
        "monitor":                   monitor,
        "indexed_facts_summary":     facts_summary,
    }


def _build_summary(payload: dict[str, Any]) -> dict[str, Any]:
    company = payload.get("company") or {}
    group_final = (payload.get("group") or {}).get("final") or {}
    final_values = group_final.get("final_values") or {}
    summary: dict[str, Any] = {
        "company_name":   company.get("display_name") or company.get("symbol"),
        "currency_unit":  group_final.get("currency") or group_final.get("currency_unit"),
        "n_segments":     len(payload.get("segments") or {}),
        "schema_version": "evi-1.0",
    }
    if "base" in final_values:
        summary["fair_value_base"] = final_values["base"]
    if "bear" in final_values:
        summary["fair_value_bear"] = final_values["bear"]
    if "bull" in final_values:
        summary["fair_value_bull"] = final_values["bull"]

    cur = group_final.get("current_price")
    if cur is not None:
        summary["current_price"] = cur
        if "fair_value_base" in summary and isinstance(cur, (int, float)) and cur:
            try:
                summary["upside_pct"] = round((summary["fair_value_base"] - cur) / cur * 100, 2)
            except Exception:
                pass

    judgment = group_final.get("judgment")
    if judgment:
        summary["judgment"] = judgment

    monitor_open = len((payload.get("monitor") or {}).get("open_tasks") or [])
    summary["monitor_open_tasks"] = monitor_open
    return summary


# ---------------------------------------------------------------------------
# Backend POST
# ---------------------------------------------------------------------------

def _post_finalize(
    entry_id: str,
    status: str,
    summary: dict[str, Any] | None,
    payload: dict[str, Any] | None,
    error_message: str | None,
) -> tuple[bool, str]:
    url = f"{_api_base()}/api/v1/templates/_internal/entries/{entry_id}/finalize"
    body = json.dumps({
        "status": status,
        "summary": summary,
        "payload": payload,
        "error_message": error_message,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    tok = os.environ.get("INTERNAL_SERVICE_TOKEN", "")
    if tok:
        req.add_header("X-Internal-Service-Token", tok)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            return resp.status < 400, f"HTTP {resp.status} {text[:300]}"
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", errors="replace") if e.fp else ""
        return False, f"HTTP {e.code} {text[:300]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--entry-id", required=True)
    p.add_argument("--data-dir", help="data/{symbol_dir}")
    p.add_argument("--symbol", help="If --data-dir not given, resolve from symbol")
    p.add_argument("--display-name")
    p.add_argument("--market")
    p.add_argument("--status", default="completed",
                   choices=["completed", "partial", "failed"])
    p.add_argument("--error-message")
    args = p.parse_args()

    if args.status == "failed":
        ok, info = _post_finalize(args.entry_id, "failed", None, None,
                                  args.error_message or "EVI run failed (no message)")
        print(f"[evi_persist] failed-finalize ok={ok} {info}")
        return 0 if ok else 1

    if not args.data_dir:
        print("ERROR: --data-dir required for non-failed status", file=sys.stderr)
        return 2
    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"ERROR: data dir does not exist: {data_dir}", file=sys.stderr)
        return 2

    company = {
        "symbol":        args.symbol or data_dir.name.replace("_", "."),
        "display_name":  args.display_name,
        "market":        args.market,
    }

    payload = _build_payload(data_dir, company)
    summary = _build_summary(payload)

    has_group_final = bool((payload.get("group") or {}).get("final"))
    has_any_segment = any(
        (s or {}).get("final") or (s or {}).get("results")
        for s in (payload.get("segments") or {}).values()
    )
    if not has_group_final and not has_any_segment:
        ok, info = _post_finalize(
            args.entry_id, "failed", None, None,
            "No EVI artifacts found (group/final and segments are both empty)",
        )
        print(f"[evi_persist] empty → failed ok={ok} {info}")
        return 1

    final_status = args.status
    if args.status == "completed" and not has_group_final:
        final_status = "partial"

    print(f"[evi_persist] data_dir={data_dir}")
    print(f"[evi_persist] segments={len(payload['segments'])} "
          f"group_final={'OK' if has_group_final else 'MISSING'} "
          f"facts={payload['indexed_facts_summary']['total']}")
    print(f"[evi_persist] summary keys={list(summary)}")

    ok, info = _post_finalize(args.entry_id, final_status, summary, payload, None)
    print(f"[evi_persist] finalize status={final_status} ok={ok} {info}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
