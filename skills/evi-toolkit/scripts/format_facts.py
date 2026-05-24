#!/usr/bin/env python3
"""evi-toolkit / format_facts.py

Extract the fact index out of EVI agent's markdown reports.

Convention: every ``reports/*.md`` written by an evi-* skill ends with a
section like::

    ## Facts Index

    [1] fact_id=fact_cloud_001 | segment=cloud | reliability=high
        text: 云业务收入恢复增长，财报将其归因为 …
        source: doc_2024_annual#mdna_cloud
        url: https://...

    [2] fact_id=fact_cloud_002 | segment=cloud | reliability=medium
        text: 卖方研报上调 2026 营收预测 ...
        source: research/2024-12-15_goldman.pdf
        url: https://...

This script walks every ``reports/*.md``, parses these blocks, merges them
into ``information/indexed_facts.json``. Existing fact IDs are preserved;
new ones are appended.

Why this exists:
  Agents naturally write reports in markdown. We don't want them context-
  switching to fill JSON schemas at the same time. Instead they put facts
  with a stable shape at the bottom of each report, and this script turns
  that into the index the dashboard reads.

Usage:
  python3 format_facts.py --data-dir data/0700_HK
  python3 format_facts.py --data-dir data/0700_HK --report reports/data.md
  python3 format_facts.py --data-dir data/0700_HK --dry-run
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
# Regexes
# ---------------------------------------------------------------------------

# Section header — first match wins; multiple variants tolerated.
SECTION_RE = re.compile(
    r"^##+\s*(facts?\s*index|事实索引|引用索引)\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# Each fact block starts with [N] ... and contains key=value pairs.
FACT_HEADER_RE = re.compile(
    r"^\[(\d+)\]\s+(.+?)$",
    re.MULTILINE,
)

KV_PAIR_RE = re.compile(r"(\w+)\s*=\s*([^|\s][^|]*?)(?=\s*\||\s*$)")
TEXT_LINE_RE = re.compile(r"^\s*(text|内容)\s*[:：]\s*(.+)$", re.IGNORECASE | re.MULTILINE)
SOURCE_LINE_RE = re.compile(r"^\s*(source|来源)\s*[:：]\s*(.+)$", re.IGNORECASE | re.MULTILINE)
URL_LINE_RE = re.compile(r"^\s*(url|链接)\s*[:：]\s*(\S+)\s*$", re.IGNORECASE | re.MULTILINE)
QUOTE_LINE_RE = re.compile(r"^\s*(quote|原文)\s*[:：]\s*(.+)$", re.IGNORECASE | re.MULTILINE)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _split_into_blocks(section: str) -> list[tuple[int, str, str]]:
    """Yield (display_no, header_kv_string, body_text) for each [N] block."""
    matches = list(FACT_HEADER_RE.finditer(section))
    out: list[tuple[int, str, str]] = []
    for i, m in enumerate(matches):
        no = int(m.group(1))
        header_kv = m.group(2).strip()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(section)
        body = section[start:end].strip()
        out.append((no, header_kv, body))
    return out


def _parse_kv(header: str) -> dict[str, str]:
    """Parse 'fact_id=... | segment=... | reliability=...' into dict."""
    return {m.group(1): m.group(2).strip() for m in KV_PAIR_RE.finditer(header)}


def _parse_block(no: int, header: str, body: str, source_report: str) -> dict[str, Any]:
    kv = _parse_kv(header)
    text = ""
    source = ""
    url = None
    quote = None

    m = TEXT_LINE_RE.search(body)
    if m:
        text = m.group(2).strip()
    else:
        # fallback: first non-empty line of body
        for line in body.splitlines():
            line = line.strip()
            if line:
                text = line
                break

    m = SOURCE_LINE_RE.search(body)
    if m:
        source = m.group(2).strip()

    m = URL_LINE_RE.search(body)
    if m:
        url = m.group(2).strip()

    m = QUOTE_LINE_RE.search(body)
    if m:
        quote = m.group(2).strip()

    return {
        "display_no": no,
        "fact_id": kv.get("fact_id") or f"auto_{source_report}_{no}",
        "segment_id": kv.get("segment") or kv.get("segment_id"),
        "topic": kv.get("topic"),
        "reliability": (kv.get("reliability") or "medium").lower(),
        "text": text,
        "quote": quote,
        "source": {
            "kind": kv.get("kind"),
            "ref": source or kv.get("source"),
            "url": url,
        },
        "valid_for": [m.strip() for m in (kv.get("valid_for") or "").split(",") if m.strip()] or None,
        "tags": [t.strip() for t in (kv.get("tags") or "").split(",") if t.strip()] or None,
        "found_in_report": source_report,
    }


def parse_report(report_path: Path, repo_root: Path) -> list[dict[str, Any]]:
    """Return the list of facts found in a single markdown report."""
    if not report_path.exists():
        return []
    text = report_path.read_text(encoding="utf-8", errors="replace")
    m = SECTION_RE.search(text)
    if not m:
        return []
    section = text[m.end():]
    # cut at next H2 of equal or higher level (defensive)
    next_h = re.search(r"^##\s+\S", section, re.MULTILINE)
    if next_h:
        section = section[: next_h.start()]

    rel = str(report_path.relative_to(repo_root)) if repo_root in report_path.parents else report_path.name
    out = []
    for no, header, body in _split_into_blocks(section):
        out.append(_parse_block(no, header, body, rel))
    return out


# ---------------------------------------------------------------------------
# Index merging
# ---------------------------------------------------------------------------

def merge(existing: dict[str, Any] | None, new_facts: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge new facts into the existing index. Preserve old fact_id, append new."""
    base = existing if isinstance(existing, dict) else {}
    facts = list(base.get("facts") or [])
    by_id = {f.get("fact_id"): f for f in facts if isinstance(f, dict)}

    next_no = max((f.get("display_no") or 0) for f in facts) + 1 if facts else 1
    appended = 0
    updated = 0
    for nf in new_facts:
        fid = nf.get("fact_id")
        if fid and fid in by_id:
            # update fields but preserve original display_no
            old = by_id[fid]
            old.update({k: v for k, v in nf.items() if v not in (None, "", []) and k != "display_no"})
            updated += 1
            continue
        nf = dict(nf)
        if not nf.get("display_no"):
            nf["display_no"] = next_no
        else:
            # re-allocate display_no to be globally unique
            nf["display_no"] = next_no
        next_no += 1
        facts.append(nf)
        appended += 1

    base["schema_version"] = 1
    base["facts"] = facts
    base["next_fact_id"] = (max((f.get("display_no") or 0) for f in facts) + 1) if facts else 1
    base["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    base["_stats"] = {"appended": appended, "updated": updated, "total": len(facts)}
    return base


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--report", default=None,
                   help="Single report path (relative to data-dir or absolute). "
                        "If omitted, walks data-dir/reports/*.md.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print parsed facts but don't write the index.")
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"ERROR: data dir not found: {data_dir}", file=sys.stderr)
        return 2

    if args.report:
        rp = (data_dir / args.report).resolve() if not Path(args.report).is_absolute() else Path(args.report)
        reports = [rp] if rp.exists() else []
    else:
        reports_dir = data_dir / "reports"
        reports = sorted(reports_dir.glob("*.md")) if reports_dir.exists() else []

    if not reports:
        print(f"[format_facts] no reports found under {data_dir}/reports/", file=sys.stderr)
        return 1

    all_new: list[dict[str, Any]] = []
    for r in reports:
        new = parse_report(r, data_dir)
        print(f"[format_facts] {r.name} → {len(new)} facts")
        all_new.extend(new)

    if args.dry_run:
        print(json.dumps(all_new, ensure_ascii=False, indent=2))
        return 0

    idx_path = data_dir / "information" / "indexed_facts.json"
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    existing = None
    if idx_path.exists():
        try:
            existing = json.loads(idx_path.read_text(encoding="utf-8"))
        except Exception:
            existing = None
    merged = merge(existing, all_new)
    idx_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    s = merged["_stats"]
    print(f"[format_facts] {idx_path}: total={s['total']} appended={s['appended']} updated={s['updated']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
