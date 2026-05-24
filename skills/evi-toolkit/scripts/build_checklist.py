#!/usr/bin/env python3
"""evi-toolkit / build_checklist.py

Build the **base data quality CHECKLIST** for an EVI project.

Why:
  After ``evi-base-data-builder`` finishes downloading & parsing data, we need
  a deterministic, human-readable scorecard that says — for each piece of data
  we expect — whether it's present, fresh, and complete. The Agent and the
  frontend both consume this.

Inputs (relative to --data-dir):
  base/catalog.json
  base/financials/raw/*.pdf
  base/financials/parsed/*.md
  base/financials/mdna/*.md
  base/financials/segments/segment_data.json
  base/financials/indicators/key_metrics.csv | key_metrics.json
  base/research/raw/*
  base/transcripts/raw/*
  base/fmp/*.json
  base/validation/fmp_reconcile.json

Outputs:
  base/CHECKLIST.md      — human-readable markdown with a status table
  base/CHECKLIST.json    — machine-readable summary the dashboard reads

Status semantics:
  ✅ ok        — required data present and considered fresh / complete
  ⚠️ partial   — present but stale / incomplete (e.g. only 4 fiscal years)
  ❌ missing   — required data not found

The checks are intentionally heuristic — Agents can override / refine items
afterwards by directly editing CHECKLIST.json (the schema lets them).

Usage:
  python3 build_checklist.py --data-dir data/0700_HK \\
      [--required-periods 6] [--max-staleness-days 200]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def _read_json(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _list(dir_: Path, suffixes: tuple[str, ...] = ()) -> list[Path]:
    if not dir_.exists():
        return []
    out = []
    for f in sorted(dir_.iterdir()):
        if f.is_dir():
            continue
        if suffixes and f.suffix.lower() not in suffixes:
            continue
        out.append(f)
    return out


PERIOD_RE = re.compile(
    r"(20\d{2})[-_/]?((Q[1-4])|(H[12])|annual|interim|半年|年报|中期)?",
    re.IGNORECASE,
)


def _detect_periods(files: list[Path]) -> list[str]:
    """Pull a ``YYYY[-Q?]`` token out of each filename, sorted descending."""
    periods: set[str] = set()
    for f in files:
        m = PERIOD_RE.search(f.stem)
        if not m:
            continue
        yr = m.group(1)
        q = (m.group(3) or m.group(4) or "").upper()
        periods.add(f"{yr}-{q}" if q else yr)
    return sorted(periods, reverse=True)


def _file_mtime_age_days(f: Path) -> int:
    try:
        ts = f.stat().st_mtime
        return int((datetime.now().timestamp() - ts) / 86400)
    except Exception:
        return 99999


# ---------------------------------------------------------------------------
# Item check
# ---------------------------------------------------------------------------

def _item(
    key: str, label: str,
    status: str, detail: str = "",
    last_updated: str | None = None,
    severity: str = "blocking",      # blocking / important / nice_to_have
) -> dict[str, Any]:
    return {
        "key": key, "label": label,
        "status": status, "detail": detail,
        "last_updated": last_updated,
        "severity": severity,
    }


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _check_fmp(data_dir: Path) -> list[dict[str, Any]]:
    fmp = data_dir / "base" / "fmp"
    expected = ("profile", "incomeStatement", "balanceSheet", "cashFlow", "keyMetrics", "ratios")
    items = []
    for key in expected:
        f = fmp / f"{key}.json"
        if not f.exists():
            items.append(_item(f"fmp_{key}", f"FMP {key}.json",
                               "missing", f"未找到 {f.name}", severity="blocking"))
            continue
        age = _file_mtime_age_days(f)
        status = "ok" if age <= 14 else ("partial" if age <= 90 else "missing")
        detail = f"{age} 天前更新；建议每周刷新一次"
        items.append(_item(
            f"fmp_{key}", f"FMP {key}.json", status, detail,
            datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).strftime("%Y-%m-%d"),
            severity="blocking",
        ))
    return items


def _check_financials(data_dir: Path, required_periods: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    raw = _list(data_dir / "base/financials/raw", (".pdf", ".html", ".htm"))
    parsed = _list(data_dir / "base/financials/parsed", (".md",))
    mdna = _list(data_dir / "base/financials/mdna", (".md",))

    raw_periods = _detect_periods(raw)
    parsed_periods = _detect_periods(parsed)
    mdna_periods = _detect_periods(mdna)

    items.append(_item(
        "financials_raw",
        f"原始财报 PDF（≥ 最近 {required_periods} 期）",
        ("ok" if len(raw_periods) >= required_periods
            else "partial" if raw_periods else "missing"),
        f"已收集 {len(raw_periods)} 期：{', '.join(raw_periods[:8]) or '—'}",
        severity="blocking",
    ))

    items.append(_item(
        "financials_parsed",
        "财报 PDF → Markdown 解析",
        ("ok" if len(parsed_periods) >= max(3, required_periods // 2)
            else "partial" if parsed_periods else "missing"),
        f"已解析 {len(parsed_periods)} 期；建议至少最近 3 期",
        severity="important",
    ))

    items.append(_item(
        "financials_mdna",
        "MD&A / 业务回顾 抽取",
        ("ok" if len(mdna_periods) >= max(3, required_periods // 2)
            else "partial" if mdna_periods else "missing"),
        f"已抽取 {len(mdna_periods)} 期",
        severity="important",
    ))

    return items


def _check_segments(data_dir: Path) -> dict[str, Any]:
    seg_path = data_dir / "base/financials/segments/segment_data.json"
    seg = _read_json(seg_path)
    if not isinstance(seg, dict):
        return _item("segments", "业务分部数据 (segment_data.json)",
                     "missing", "尚未生成", severity="blocking")
    n_seg = len(seg.get("segments") or [])
    return _item(
        "segments", "业务分部数据 (segment_data.json)",
        "ok" if n_seg > 0 else "missing",
        f"识别到 {n_seg} 个分部",
        severity="blocking",
    )


def _check_indicators(data_dir: Path, required_periods: int) -> list[dict[str, Any]]:
    """Indicators may be JSON or CSV; we accept either."""
    indicators_dir = data_dir / "base/financials/indicators"
    json_p = indicators_dir / "key_metrics.json"
    csv_p = indicators_dir / "key_metrics.csv"
    items: list[dict[str, Any]] = []

    metrics_required = (
        "ROE", "ROIC", "gross_margin", "operating_margin",
        "rd_ratio", "fcf", "debt_to_ebitda", "interest_coverage",
    )

    j = _read_json(json_p)
    if isinstance(j, list):
        present_metrics = {row.get("metric") for row in j if isinstance(row, dict)}
        periods_per_metric: dict[str, set] = {}
        with_reason = 0
        for row in j:
            if not isinstance(row, dict):
                continue
            m = row.get("metric")
            p = row.get("period")
            if m and p:
                periods_per_metric.setdefault(m, set()).add(p)
            if (row.get("change_reason") or {}).get("summary"):
                with_reason += 1

        miss = [m for m in metrics_required if m not in present_metrics]
        thin = [m for m, ps in periods_per_metric.items() if len(ps) < required_periods]

        if miss:
            items.append(_item(
                "indicators_metrics",
                f"关键指标完整性（{len(metrics_required)} 个核心）",
                "partial",
                f"缺失：{', '.join(miss)}",
                severity="blocking",
            ))
        else:
            items.append(_item(
                "indicators_metrics",
                f"关键指标完整性（{len(metrics_required)} 个核心）",
                "ok", f"全部覆盖；变动说明 {with_reason} 条",
                severity="blocking",
            ))

        items.append(_item(
            "indicators_periods",
            f"关键指标 ≥ 最近 {required_periods} 期",
            "ok" if not thin else "partial",
            (f"{len(thin)} 个指标期数不足：{', '.join(thin[:5])}" if thin else "全部达标"),
            severity="important",
        ))
        return items

    if csv_p.exists():
        items.append(_item(
            "indicators_metrics",
            "关键指标 CSV 已生成（建议同时输出 JSON）",
            "partial", "CSV 存在；前端结构化看板需要 JSON 字段",
            severity="important",
        ))
        return items

    items.append(_item(
        "indicators_metrics", "关键指标抽取",
        "missing", "未找到 key_metrics.json 或 key_metrics.csv",
        severity="blocking",
    ))
    return items


def _check_validation(data_dir: Path) -> dict[str, Any]:
    f = data_dir / "base/validation/fmp_reconcile.json"
    j = _read_json(f)
    if not isinstance(j, dict):
        return _item("validation", "FMP 校验 (fmp_reconcile.json)",
                     "missing", "未找到", severity="important")
    rows = j.get("rows") or []
    mismatches = sum(1 for r in rows if (r.get("status") or "").lower() == "mismatch")
    status = "ok" if mismatches == 0 else "partial"
    return _item(
        "validation", "FMP 校验",
        status,
        f"{len(rows)} 项校验，{mismatches} 项不一致",
        severity="important",
    )


def _check_research(data_dir: Path) -> dict[str, Any]:
    files = _list(data_dir / "base/research/raw")
    return _item(
        "research", "卖方/行业研报",
        "ok" if files else "partial" if (data_dir / "base/research/raw").exists() else "missing",
        f"{len(files)} 篇" if files else "尚未下载或可外搜",
        severity="nice_to_have",
    )


def _check_transcripts(data_dir: Path) -> dict[str, Any]:
    files = _list(data_dir / "base/transcripts/raw")
    return _item(
        "transcripts", "电话会纪要",
        "ok" if files else "partial",
        f"{len(files)} 期" if files else "尚未下载",
        severity="important",
    )


def _check_catalog(data_dir: Path) -> dict[str, Any]:
    cat = _read_json(data_dir / "base/catalog.json")
    if not isinstance(cat, dict):
        return _item("catalog", "base/catalog.json",
                     "missing", "请运行 update_catalog.py --rebuild", severity="important")
    n = len(cat.get("items") or [])
    return _item(
        "catalog", "base/catalog.json",
        "ok" if n > 0 else "partial",
        f"{n} 条索引",
        severity="important",
    )


# ---------------------------------------------------------------------------
# Aggregation + rendering
# ---------------------------------------------------------------------------

def build(data_dir: Path, required_periods: int = 6) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    items.extend(_check_fmp(data_dir))
    items.extend(_check_financials(data_dir, required_periods))
    items.append(_check_segments(data_dir))
    items.extend(_check_indicators(data_dir, required_periods))
    items.append(_check_validation(data_dir))
    items.append(_check_research(data_dir))
    items.append(_check_transcripts(data_dir))
    items.append(_check_catalog(data_dir))

    n_total = len(items)
    n_ok = sum(1 for x in items if x["status"] == "ok")
    n_partial = sum(1 for x in items if x["status"] == "partial")
    n_missing = sum(1 for x in items if x["status"] == "missing")
    blocking_missing = [
        x for x in items
        if x["severity"] == "blocking" and x["status"] == "missing"
    ]

    overall = (
        "ok" if n_missing == 0 and n_partial == 0
        else "partial" if not blocking_missing
        else "blocked"
    )

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "required_periods": required_periods,
        "summary": {
            "total": n_total, "ok": n_ok,
            "partial": n_partial, "missing": n_missing,
            "blocking_missing": [x["key"] for x in blocking_missing],
            "overall": overall,
        },
        "items": items,
    }


_STATUS_BADGE = {"ok": "✅", "partial": "⚠️", "missing": "❌"}
_SEVERITY_BADGE = {
    "blocking": "🔴 必需", "important": "🟠 重要", "nice_to_have": "🟡 可选",
}


def render_md(checklist: dict[str, Any]) -> str:
    s = checklist["summary"]
    overall_emoji = {
        "ok": "✅ 全部通过", "partial": "⚠️ 部分缺失", "blocked": "❌ 有 blocking 缺失",
    }[s["overall"]]
    lines = [
        f"# Base Data CHECKLIST",
        "",
        f"_生成时间: {checklist['generated_at']}_",
        f"_要求至少 {checklist['required_periods']} 期历史数据_",
        "",
        f"**整体状态：{overall_emoji}**",
        "",
        f"- ✅ ok: **{s['ok']}**  ⚠️ partial: **{s['partial']}**  ❌ missing: **{s['missing']}**",
    ]
    if s["blocking_missing"]:
        lines.append(f"- 🔴 阻塞性缺失：{', '.join(s['blocking_missing'])}")
    lines += [
        "",
        "| 状态 | 项目 | 重要性 | 详情 | 最后更新 |",
        "|---|---|---|---|---|",
    ]
    for it in checklist["items"]:
        lines.append(
            f"| {_STATUS_BADGE.get(it['status'], '?')} | "
            f"{it['label']} | {_SEVERITY_BADGE.get(it['severity'], it['severity'])} | "
            f"{it['detail'] or '—'} | {it['last_updated'] or '—'} |"
        )
    lines.append("")
    lines.append(
        "> 修复建议：先处理 🔴 必需项的 ❌；其它 ⚠️ 在写完报告后再补齐。"
        "可以让 Agent 直接编辑 `CHECKLIST.json` 标注 status / detail。"
    )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--required-periods", type=int, default=6)
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"ERROR: data dir not found: {data_dir}", file=sys.stderr)
        return 2

    checklist = build(data_dir, args.required_periods)
    out_json = data_dir / "base" / "CHECKLIST.json"
    out_md = data_dir / "base" / "CHECKLIST.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(checklist, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(render_md(checklist), encoding="utf-8")

    s = checklist["summary"]
    print(f"[build_checklist] overall={s['overall']} ok={s['ok']} partial={s['partial']} missing={s['missing']}")
    print(f"[build_checklist] → {out_md}")
    print(f"[build_checklist] → {out_json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
