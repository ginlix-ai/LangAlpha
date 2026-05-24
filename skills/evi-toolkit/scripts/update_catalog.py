#!/usr/bin/env python3
"""evi-toolkit / update_catalog.py

Maintain ``data/{symbol_dir}/base/catalog.json`` — the searchable index
that other evi-* skills consume.

Modes:
  --rebuild           Walk base/ and rebuild items[] from scratch.
  --add  ITEM_JSON    Append/replace one item (JSON object via stdin or file).
  --remove ITEM_ID    Drop one item by id.
  --list              Pretty-print current catalog (debug).

The catalog item schema is documented in SKILL.md §2.4. Briefly:

    {
      "id":          "<unique slug>",         # e.g. doc_2024_annual
      "kind":        "financials"|"announcements"|"research"|"transcripts"|"other",
      "title":       "<human title>",
      "raw_path":    "base/financials/raw/...",
      "parsed_path": "base/.../*.md"  | null,
      "mdna_path":   "...."           | null,   # only for financials
      "period":      "2024" | "2024-Q3" | null,
      "language":    "zh-Hant" | "en" | "zh-Hans" | null,
      "added_at":    "2026-05-21T..."
    }
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
DEFAULT_DATA_DIR = (
    SKILL_DIR.parent.parent.parent / "data" if _in_sandbox else SKILL_DIR / "data"
)

KIND_DIRS = {
    "financials": "base/financials",
    "research":   "base/research",
    "transcripts": "base/transcripts",
}

PERIOD_RE = re.compile(r"(20\d{2})(?:[-_/](Q[1-4]|H[12]|annual|interim))?", re.IGNORECASE)


def _safe_symbol_dir(symbol: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", symbol or "").strip("_")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _detect_period(name: str) -> str | None:
    m = PERIOD_RE.search(name)
    if not m:
        return None
    yr, q = m.group(1), m.group(2)
    return f"{yr}-{q}" if q else yr


def _walk(data_dir: Path) -> list[dict]:
    """Discover items by scanning the base/ tree."""
    items: list[dict] = []
    for kind, rel in KIND_DIRS.items():
        raw_dir = data_dir / rel / "raw"
        parsed_dir = data_dir / rel / "parsed"
        mdna_dir = data_dir / "base/financials/mdna"
        if not raw_dir.exists():
            continue
        for f in sorted(raw_dir.iterdir()):
            if f.is_dir():
                continue
            if f.suffix.lower() not in {".pdf", ".md", ".txt", ".html", ".htm"}:
                continue
            stem = f.stem
            iid = re.sub(r"[^A-Za-z0-9]+", "_", stem).strip("_").lower()
            iid = f"{kind[:3]}_{iid}"
            parsed = parsed_dir / f"{stem}.md"
            mdna = mdna_dir / f"{stem}-mdna.md" if kind == "financials" else None

            items.append(
                {
                    "id": iid,
                    "kind": kind,
                    "title": stem.replace("_", " ").replace("-", " ").strip(),
                    "raw_path": str(f.relative_to(data_dir)),
                    "parsed_path": (str(parsed.relative_to(data_dir)) if parsed.exists() else None),
                    "mdna_path": (str(mdna.relative_to(data_dir)) if mdna and mdna.exists() else None),
                    "period": _detect_period(stem),
                    "language": None,
                    "added_at": _now(),
                }
            )
    return items


def _load_catalog(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"schema_version": 1, "items": [], "updated_at": _now()}


def _save_catalog(path: Path, cat: dict) -> None:
    cat["updated_at"] = _now()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cat, ensure_ascii=False, indent=2), encoding="utf-8")


def cmd_rebuild(data_dir: Path) -> int:
    cat_path = data_dir / "base/catalog.json"
    cat = _load_catalog(cat_path)
    cat["items"] = _walk(data_dir)
    _save_catalog(cat_path, cat)
    print(f"[update_catalog] rebuilt — {len(cat['items'])} items → {cat_path}")
    return 0


def cmd_add(data_dir: Path, payload: dict) -> int:
    cat_path = data_dir / "base/catalog.json"
    cat = _load_catalog(cat_path)
    if "id" not in payload:
        print("[update_catalog] ERROR: item must have 'id'", file=sys.stderr)
        return 2
    payload.setdefault("added_at", _now())
    cat["items"] = [it for it in cat["items"] if it.get("id") != payload["id"]]
    cat["items"].append(payload)
    _save_catalog(cat_path, cat)
    print(f"[update_catalog] add — id={payload['id']} ({len(cat['items'])} total)")
    return 0


def cmd_remove(data_dir: Path, item_id: str) -> int:
    cat_path = data_dir / "base/catalog.json"
    cat = _load_catalog(cat_path)
    before = len(cat["items"])
    cat["items"] = [it for it in cat["items"] if it.get("id") != item_id]
    _save_catalog(cat_path, cat)
    print(f"[update_catalog] remove — id={item_id} ({before}→{len(cat['items'])})")
    return 0


def cmd_list(data_dir: Path) -> int:
    cat = _load_catalog(data_dir / "base/catalog.json")
    print(json.dumps(cat, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol")
    p.add_argument("--data-dir")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--rebuild", action="store_true")
    g.add_argument("--add", help="JSON file path or '-' for stdin")
    g.add_argument("--remove", metavar="ITEM_ID")
    g.add_argument("--list", action="store_true")
    args = p.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir).resolve()
    elif args.symbol:
        data_dir = DEFAULT_DATA_DIR / _safe_symbol_dir(args.symbol)
    else:
        print("ERROR: --symbol or --data-dir required", file=sys.stderr)
        return 2

    if args.rebuild:
        return cmd_rebuild(data_dir)
    if args.list:
        return cmd_list(data_dir)
    if args.remove:
        return cmd_remove(data_dir, args.remove)
    if args.add:
        if args.add == "-":
            payload = json.load(sys.stdin)
        else:
            payload = json.loads(Path(args.add).read_text(encoding="utf-8"))
        return cmd_add(data_dir, payload)
    return 2


if __name__ == "__main__":
    sys.exit(main())
