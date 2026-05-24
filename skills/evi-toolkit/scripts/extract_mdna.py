#!/usr/bin/env python3
"""evi-toolkit / extract_mdna.py

Heuristic extractor for the MD&A (Management Discussion & Analysis) section
out of an already-parsed financial markdown.

Approach:
  1. Look for a section header matching one of these patterns
     (case-insensitive, multi-language):
       - "management discussion and analysis"
       - "management's discussion and analysis"
       - "md&a"
       - "管理层讨论与分析"
       - "管理層討論與分析"
       - "业务回顾"          (常见于港股年报)
       - "業務回顧"
       - "董事局报告"        (港股 / chairman's report)
  2. Capture from that header until the next H1/H2 heading at the same level.
  3. If nothing matches → write a stub with `status:unmatched` and a list of
     the top-level headings we did see (so the Agent can hand-pick).

Usage:
    python3 extract_mdna.py --parsed-md <md> --out <out_md>
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PATTERNS = [
    r"management(?:'s)?\s+discussion\s+(?:&|and)\s+analysis",
    r"\bmd&a\b",
    r"management discussion and analysis",
    r"管理层讨论与分析",
    r"管理層討論與分析",
    r"业务回顾",
    r"業務回顧",
    r"董事局报告",
    r"董事會報告",
    r"chairman'?s\s+statement",
]

HEADING_RE = re.compile(r"^(#{1,3})\s+(.+?)\s*$", re.MULTILINE)


def _collect_headings(text: str) -> list[tuple[int, int, str]]:
    """Return list of (offset, level, title)."""
    out: list[tuple[int, int, str]] = []
    for m in HEADING_RE.finditer(text):
        out.append((m.start(), len(m.group(1)), m.group(2).strip()))
    return out


def _find_mdna(text: str) -> tuple[int, int] | None:
    headings = _collect_headings(text)
    if not headings:
        return None
    pat = re.compile("|".join(PATTERNS), re.IGNORECASE)
    for i, (offset, level, title) in enumerate(headings):
        if pat.search(title):
            # find next heading of same OR upper level
            end = len(text)
            for j in range(i + 1, len(headings)):
                _, l2, _ = headings[j]
                if l2 <= level:
                    end = headings[j][0]
                    break
            return (offset, end)
    return None


def extract(parsed_md: Path, out: Path) -> int:
    if not parsed_md.exists():
        print(f"[extract_mdna] ERROR: not found: {parsed_md}", file=sys.stderr)
        return 2

    text = parsed_md.read_text(encoding="utf-8", errors="replace")
    found = _find_mdna(text)
    out.parent.mkdir(parents=True, exist_ok=True)

    if not found:
        candidates = [t for _, _, t in _collect_headings(text)][:30]
        out.write_text(
            f"---\nstatus: unmatched\nsource: {parsed_md.name}\n---\n\n"
            f"# MD&A extraction failed\n\nTop-level headings found:\n\n"
            + "\n".join(f"- {c}" for c in candidates)
            + "\n",
            encoding="utf-8",
        )
        print(f"[extract_mdna] WARN: no MD&A heading matched in {parsed_md.name}", file=sys.stderr)
        return 1

    start, end = found
    chunk = text[start:end].strip()
    out.write_text(
        f"---\nsource: {parsed_md.name}\nrange_chars: {start}-{end}\n"
        f"extracted_by: evi-toolkit/extract_mdna.py\n---\n\n{chunk}\n",
        encoding="utf-8",
    )
    print(f"[extract_mdna] OK {parsed_md.name} → {out} ({end-start:,} chars)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--parsed-md", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    return extract(Path(args.parsed_md).resolve(), Path(args.out).resolve())


if __name__ == "__main__":
    sys.exit(main())
