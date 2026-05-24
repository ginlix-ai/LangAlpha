#!/usr/bin/env python3
"""evi-toolkit / init_project.py

Create the EVI project skeleton for one company:

    data/{symbol_dir}/
      base/{financials/{raw,parsed,mdna,segments,indicators},
            research/{raw,parsed},
            transcripts/{raw,parsed},
            fmp,validation}/
      base/catalog.json
      base/INDEX.md
      information/
      valuation/group/
      monitor/

Idempotent: existing files are NEVER overwritten.

Usage:
    python3 init_project.py --symbol 0700.HK [--market hk]
    python3 init_project.py --data-dir /abs/path/to/data/0700_HK
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent

_in_sandbox = ".agents/skills" in str(SKILL_DIR)
if _in_sandbox:
    DEFAULT_DATA_DIR = SKILL_DIR.parent.parent.parent / "data"
else:
    DEFAULT_DATA_DIR = SKILL_DIR / "data"


def _safe_symbol_dir(symbol: str) -> str:
    out = re.sub(r"[^A-Za-z0-9_]+", "_", symbol or "")
    return out.strip("_")


SUBDIRS = [
    "base/financials/raw",
    "base/financials/parsed",
    "base/financials/mdna",
    "base/financials/segments",
    "base/financials/indicators",
    "base/research/raw",
    "base/research/parsed",
    "base/transcripts/raw",
    "base/transcripts/parsed",
    "base/fmp",
    "base/validation",
    "information",
    "valuation/group",
    "monitor",
]


def _write_if_absent(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def init_project(data_dir: Path, symbol: str | None, market: str | None) -> int:
    data_dir.mkdir(parents=True, exist_ok=True)
    for sub in SUBDIRS:
        (data_dir / sub).mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    catalog = {
        "schema_version": 1,
        "symbol": symbol,
        "market": market,
        "created_at": now,
        "updated_at": now,
        "items": [],
    }
    _write_if_absent(data_dir / "base/catalog.json", json.dumps(catalog, ensure_ascii=False, indent=2))

    _write_if_absent(
        data_dir / "base/INDEX.md",
        f"# Base Catalog — {symbol or '(symbol unset)'}\n\n"
        f"> Auto-managed by `update_catalog.py`. Manual edits will be overwritten.\n\n"
        f"_created: {now}_\n",
    )

    indexed_facts = {
        "schema_version": 1,
        "symbol": symbol,
        "facts": [],
        "next_fact_id": 1,
        "updated_at": now,
    }
    _write_if_absent(data_dir / "information/indexed_facts.json", json.dumps(indexed_facts, ensure_ascii=False, indent=2))

    print(f"[init_project] OK data_dir={data_dir}")
    print(f"[init_project] subdirs created: {len(SUBDIRS)}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", help="Stock symbol like 0700.HK")
    p.add_argument("--market", help="hk / us / cn (optional)")
    p.add_argument("--data-dir", help="Override data dir; otherwise derived from symbol")
    args = p.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir).resolve()
    elif args.symbol:
        data_dir = DEFAULT_DATA_DIR / _safe_symbol_dir(args.symbol)
    else:
        print("ERROR: --symbol or --data-dir is required", file=sys.stderr)
        return 2

    return init_project(data_dir, args.symbol, args.market)


if __name__ == "__main__":
    sys.exit(main())
