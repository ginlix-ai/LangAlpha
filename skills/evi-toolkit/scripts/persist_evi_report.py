#!/usr/bin/env python3
"""evi-toolkit / persist_evi_report.py

V2 of evi_persist_entry.py — the **report-first** persistence script.

Philosophy:
  Agents primarily output narrative markdown reports under ``reports/*.md``.
  Numerical / chart data lives in CSVs and a single small ``facets.json``.
  The dashboard reads markdown directly (no nested JSON gymnastics).

What this script does:
  1. Read ``data/{symbol}/reports/*.md``        → embed each as a report.
  2. Read ``data/{symbol}/facets.json``         → looking-glass numbers for the dashboard.
  3. Read ``data/{symbol}/base/CHECKLIST.json`` → data quality scorecard.
  4. Read ``data/{symbol}/information/indexed_facts.json`` → fact index.
  5. (Backward compat) read existing valuation/{group,segments}/*.json
     when present so legacy templates keep rendering.
  6. POST the merged payload to the LangAlpha finalize endpoint.

Outputs:
  POST {LANGALPHA_API_BASE}/api/v1/templates/_internal/entries/{entry_id}/finalize

Usage:
  python3 persist_evi_report.py --entry-id <uuid> --data-dir data/{symbol_dir}
  python3 persist_evi_report.py --entry-id <uuid> --data-dir ... --status partial
  python3 persist_evi_report.py --entry-id <uuid> --status failed --error-message "..."
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
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
        print(f"[persist_evi_report] WARN bad json {p}: {e}", file=sys.stderr)
        return None


def _read_text(p: Path) -> str | None:
    if not p.exists():
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


def _api_base() -> str:
    base = os.environ.get("LANGALPHA_API_BASE")
    return (base or "http://localhost:8000").rstrip("/")


# ---------------------------------------------------------------------------
# Reports + facets
# ---------------------------------------------------------------------------

# 标题与默认顺序（前端 tab 展示用）。Agent 可加任意命名的 report —— 未列在此处的会按字母序追加。
DEFAULT_REPORT_ORDER = (
    # 公司层估值（最重要 — 第一个 tab）
    "final",
    "valuation_summary",
    "valuation",
    "reverse_valuation",
    # 公司层定性分析（v3.5）
    "quality",
    # 公司层调研
    "company_overview",
    "industry_research",
    # 更新记录（独立 Tab）
    "changelog",
    # 数据 / 监控
    "data",
    "data_index",
    "monitor",
    # v2 兼容
    "segments",
    "facts",
    "valuation_router",
    "assumptions",
)

REPORT_TITLES = {
    # v3 总-分结构
    "final":               "最终估值结论",
    "valuation_summary":   "估值汇总（SOTP）",
    "valuation":           "估值分析",
    "reverse_valuation":   "反向估值",
    "quality":             "定性分析（4 维度）",
    "company_overview":    "公司产业调研",
    "industry_research":   "产业调研报告",
    "changelog":           "更新记录",
    "data":                "数据收集报告",
    "data_index":          "数据索引",
    "monitor":             "监控记录",
    # v2 兼容
    "segments":            "业务分部",
    "facts":               "事实索引",
    "valuation_router":    "估值方法路由",
    "assumptions":         "假设账本",
}

# Tab 映射（前端用于把 report 归到哪个 Tab）
REPORT_TAB_MAP = {
    "final":               "valuation",
    "valuation_summary":   "valuation",
    "valuation":           "valuation",
    "reverse_valuation":   "valuation",
    "quality":             "valuation",
    "company_overview":    "company-research",
    "industry_research":   "company-research",
    "changelog":           "changelog",
    "monitor":             "automation",
    "data":                "data",
    "data_index":          "data",
    # v2 兼容
    "segments":            "company-research",
    "facts":               "data",
    "valuation_router":    "valuation",
    "assumptions":         "valuation",
}


def _classify_segment_id(stem: str, path: Path) -> tuple[str | None, str]:
    """
    解析 segment_id 和 doc_type。
    返回 (segment_id, doc_type)
      - doc_type: 'research' | 'valuation' | 'overview' | 'unknown'
    """
    # reports/segments/{seg_id}.md → research
    # reports/segments/{seg_id}_valuation.md → valuation
    if "segments" in path.parts:
        if stem.endswith("_valuation"):
            return stem[:-len("_valuation")], "valuation"
        return stem, "research"
    return None, "unknown"


def _collect_reports(data_dir: Path) -> list[dict[str, Any]]:
    """List markdown reports under reports/ (含子目录 segments/) as in-payload entries."""
    rdir = data_dir / "reports"
    if not rdir.exists():
        return []

    # 1) 顶层 *.md
    found: dict[str, Path] = {}
    for f in sorted(rdir.glob("*.md")):
        found[f.stem] = f

    # 2) segments/ 子目录下的 *.md
    seg_files: list[tuple[Path, str, str]] = []  # (path, segment_id, doc_type)
    seg_dir = rdir / "segments"
    if seg_dir.exists():
        for f in sorted(seg_dir.glob("*.md")):
            seg_id, doc_type = _classify_segment_id(f.stem, f)
            if seg_id:
                seg_files.append((f, seg_id, doc_type))

    out: list[dict[str, Any]] = []

    # 顶层报告先按预定顺序，再按字母序追加
    ordered_keys: list[str] = []
    for k in DEFAULT_REPORT_ORDER:
        if k in found:
            ordered_keys.append(k)
    for k in sorted(found):
        if k not in ordered_keys:
            ordered_keys.append(k)

    for k in ordered_keys:
        f = found[k]
        text = _read_text(f) or ""
        out.append({
            "key":      k,
            "title":    REPORT_TITLES.get(k, k.replace("_", " ").title()),
            "tab":      REPORT_TAB_MAP.get(k, "valuation"),
            "scope":    "company",
            "doc_type": "valuation" if k in ("final","valuation","valuation_summary","reverse_valuation") else
                        "research" if k in ("company_overview","industry_research") else
                        "quality" if k == "quality" else
                        "monitor" if k == "monitor" else
                        "data" if k in ("data","data_index") else "unknown",
            "path":     str(f.relative_to(data_dir)),
            "markdown": text,
            "size_chars": len(text),
            "updated_at": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    # 分部报告（带 segment_id）
    for f, seg_id, doc_type in seg_files:
        text = _read_text(f) or ""
        suffix = "（估值）" if doc_type == "valuation" else "（调研）"
        out.append({
            "key":        f"{seg_id}_{doc_type}",
            "title":      f"{seg_id}{suffix}",
            "tab":        f"segment:{seg_id}",
            "scope":      "segment",
            "segment_id": seg_id,
            "doc_type":   doc_type,
            "path":       str(f.relative_to(data_dir)),
            "markdown":   text,
            "size_chars": len(text),
            "updated_at": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    return out


def _legacy_valuation_payload(data_dir: Path) -> dict[str, Any]:
    """Re-collect the v1 segments / group payload as backward-compat data."""
    val_root = data_dir / "valuation"
    segs: dict[str, dict[str, Any]] = {}
    if val_root.exists():
        for child in sorted(val_root.iterdir()):
            if not child.is_dir() or child.name == "group":
                continue
            seg_id = child.name
            results: dict[str, Any] = {}
            for method in ("dcf", "ps", "peg", "comps", "ddm"):
                r = _read_json(child / f"{method}_result.json")
                if r is not None:
                    results[method.upper()] = r
            segs[seg_id] = {
                "name": seg_id,
                "results": results or None,
                "final": _read_json(child / "final_segment_valuation.json"),
            }

    group = {
        "final":               _read_json(data_dir / "valuation/group/final_company_valuation.json"),
        "reverse_valuation":   _read_json(data_dir / "valuation/group/reverse_valuation.json"),
        "assumption_ledger":   _read_json(data_dir / "valuation/group/assumption_ledger.json"),
    }
    return {"segments": segs, "group": group}


# ---------------------------------------------------------------------------
# Build payload + summary
# ---------------------------------------------------------------------------

def _build_payload(data_dir: Path, company: dict[str, Any]) -> dict[str, Any]:
    facets = _read_json(data_dir / "facets.json") or {}
    checklist = _read_json(data_dir / "base/CHECKLIST.json")
    indexed_facts = _read_json(data_dir / "information/indexed_facts.json") or {}
    facts_list = indexed_facts.get("facts") or []

    by_segment: dict[str, int] = {}
    high = 0
    for f in facts_list:
        sid = f.get("segment_id") or "_unassigned"
        by_segment[sid] = by_segment.get(sid, 0) + 1
        if (f.get("reliability") or "").lower() in ("high", "高"):
            high += 1
    facts_summary = {
        "total": len(facts_list),
        "by_segment": by_segment,
        "high_reliability_pct": (round(high / len(facts_list), 3) if facts_list else 0.0),
    }

    legacy = _legacy_valuation_payload(data_dir)
    reports = _collect_reports(data_dir)

    monitor = {}
    mt = _read_json(data_dir / "monitor/revaluation_tasks.json") or []
    if isinstance(mt, dict):
        mt = mt.get("tasks") or []
    monitor["open_tasks"] = [t for t in mt if (t or {}).get("status") != "done"]
    tl = _read_json(data_dir / "monitor/trigger_log.json") or []
    if isinstance(tl, list) and tl:
        last = tl[-1]
        if isinstance(last, dict):
            monitor["last_run_id"]     = last.get("monitor_run_id")
            monitor["last_checked_at"] = last.get("checked_at")

    return {
        "schema_version": "evi-2.0",
        "company":   company,
        "facets":    facets,
        "checklist": checklist,
        "reports":   reports,
        "indexed_facts_summary": facts_summary,
        # v1 兼容字段：让旧 EviReportPanel 的代码路径仍能拿到数据
        "segments":  legacy["segments"],
        "group":     legacy["group"],
        "monitor":   monitor,
    }


def _build_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Compose the dashboard's compact summary fields.

    Resolution order for valuation numbers:
      1. ``facets.json`` (preferred — explicit, agent-controlled)
      2. group.final.fair_value_per_share / final_values
    """
    facets = payload.get("facets") or {}
    company = payload.get("company") or {}
    group_final = (payload.get("group") or {}).get("final") or {}
    checklist = payload.get("checklist") or {}

    summary: dict[str, Any] = {
        "company_name":   facets.get("company_name") or company.get("display_name") or company.get("symbol"),
        "currency_unit":  facets.get("currency_unit"),
        "schema_version": "evi-2.0",
    }

    fv = facets.get("fair_value") or {}
    if "base" in fv:        summary["fair_value_base"] = fv["base"]
    if "bear" in fv:        summary["fair_value_bear"] = fv["bear"]
    if "bull" in fv:        summary["fair_value_bull"] = fv["bull"]

    cur = facets.get("current_price") or group_final.get("current_price")
    if cur is not None:
        summary["current_price"] = cur

    upside = facets.get("upside_pct")
    if upside is None and isinstance(group_final.get("upside_pct"), (int, float)):
        upside = group_final["upside_pct"]
    if isinstance(upside, dict):
        upside = upside.get("base")
    if upside is not None:
        summary["upside_pct"] = upside

    judgment = facets.get("judgment") or group_final.get("judgment")
    if judgment:
        summary["judgment"] = judgment

    summary["n_segments"]         = facets.get("n_segments") or len(payload.get("segments") or {})
    summary["structure_type"]     = facets.get("structure_type")  # single_segment | multi_segment
    summary["monitor_open_tasks"] = len((payload.get("monitor") or {}).get("open_tasks") or [])
    if checklist:
        s = checklist.get("summary") or {}
        summary["checklist_overall"] = s.get("overall")
        summary["checklist_missing"] = s.get("missing")
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
        "status": status, "summary": summary,
        "payload": payload, "error_message": error_message,
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
    p.add_argument("--data-dir")
    p.add_argument("--symbol")
    p.add_argument("--display-name")
    p.add_argument("--market")
    p.add_argument("--status", default="completed",
                   choices=["completed", "partial", "failed"])
    p.add_argument("--error-message")
    args = p.parse_args()

    if args.status == "failed":
        ok, info = _post_finalize(
            args.entry_id, "failed", None, None,
            args.error_message or "EVI run failed (no message)",
        )
        print(f"[persist_evi_report] failed-finalize ok={ok} {info}")
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

    has_reports = bool(payload.get("reports"))
    has_facets = bool(payload.get("facets"))
    has_legacy = bool((payload.get("group") or {}).get("final"))

    if not has_reports and not has_facets and not has_legacy:
        ok, info = _post_finalize(
            args.entry_id, "failed", None, None,
            "No EVI artifacts found (reports, facets, group/final all empty)",
        )
        print(f"[persist_evi_report] empty → failed ok={ok} {info}")
        return 1

    final_status = args.status
    if args.status == "completed":
        # Need at minimum: 1 report + facets, or legacy group/final.
        if not (has_reports and has_facets) and not has_legacy:
            final_status = "partial"

    print(f"[persist_evi_report] data_dir={data_dir}")
    print(f"[persist_evi_report] reports={len(payload.get('reports') or [])} "
          f"facets={'YES' if has_facets else 'NO'} "
          f"legacy_group_final={'YES' if has_legacy else 'NO'}")
    print(f"[persist_evi_report] summary keys={list(summary)}")

    ok, info = _post_finalize(args.entry_id, final_status, summary, payload, None)
    print(f"[persist_evi_report] finalize status={final_status} ok={ok} {info}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
