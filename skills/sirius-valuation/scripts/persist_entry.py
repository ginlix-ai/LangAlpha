#!/usr/bin/env python3
"""persist_entry.py — Sandbox-side script that uploads Sirius analysis results
to the LangAlpha backend's internal finalize endpoint.

Called by the agent at the end of /sirius-valuation runs. Reads:
  - data/{symbol_dir}/engine_result.json        (quantitative valuation engine)
  - data/{symbol_dir}/financial_context.md      (financial summary; passed through)
  - data/{symbol_dir}/structured/d1.json…d7.json (per-dimension structured results)
  - data/{symbol_dir}/reports/d1_report.md…d7_report.md (analysis reports)

Falls back to legacy paths (d1.json in data root) for backward compatibility.

Builds:
  - summary: 4-6 key dashboard fields (fair value, current price, judgment, ...)
  - payload: full structured result (engine_result + all dN.json + report paths)

Then POSTs to /api/v1/templates/_internal/entries/{entry_id}/finalize.

Usage (from within the sandbox):
  python3 .agents/skills/sirius-valuation/scripts/persist_entry.py \\
      --entry-id <uuid> \\
      --data-dir data/1357_HK

Environment:
  LANGALPHA_API_BASE       Default: http://localhost:8000
  INTERNAL_SERVICE_TOKEN   Optional; included in X-Internal-Service-Token header.
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
# Helpers
# ---------------------------------------------------------------------------


def _read_json(path: Path) -> dict[str, Any] | None:
    """Read a JSON file; return None if missing or unparseable."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[persist_entry] WARN: could not parse {path}: {e}", file=sys.stderr)
        return None


def _read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return None


def _api_base() -> str:
    """Resolve backend API base url. Tries host.docker.internal first (works
    in a real Docker sandbox), then falls back to localhost (LocalProvider /
    same-host runs)."""
    explicit = os.environ.get("LANGALPHA_API_BASE")
    if explicit:
        return explicit.rstrip("/")
    # The sandbox might or might not have host.docker.internal resolution.
    # LocalProvider runs subprocesses on the host directly, so localhost works.
    return "http://localhost:8000"


# ---------------------------------------------------------------------------
# Summary / payload extraction
# ---------------------------------------------------------------------------


def _build_summary(
    engine_result: dict[str, Any] | None,
    d7: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the compact dashboard summary (4-6 numeric / string fields).

    Conservative — if a field is missing, omit it rather than guess.
    """
    summary: dict[str, Any] = {}
    if engine_result:
        cv = engine_result.get("crossValidation") or {}
        if "weighted_avg" in cv:
            summary["fair_value"] = cv["weighted_avg"]
        if "current_price" in cv:
            summary["current_price"] = cv["current_price"]
        if "safety_margin" in cv:
            summary["upside_pct"] = cv["safety_margin"]
        if "judgment" in cv:
            summary["judgment"] = cv["judgment"]  # 低估 / 合理 / 高估
        cls = engine_result.get("classification") or {}
        if "type" in cls:
            summary["company_type"] = cls["type"]
    # D7 is the final qualitative verdict (may override / refine engine view)
    if d7:
        # Try common keys — sirius D7 schema is still evolving
        for k in ("final_recommendation", "recommendation", "rating"):
            v = d7.get(k)
            if v:
                summary["recommendation"] = v
                break
        # Sometimes D7 provides a corrected fair value
        for k in ("adjusted_fair_value", "fair_value_after_review"):
            v = d7.get(k)
            if v is not None:
                summary["fair_value_adjusted"] = v
                break
    return summary


def _build_payload(
    engine_result: dict[str, Any] | None,
    dimension_results: dict[str, dict[str, Any] | None],
    financial_context: str | None,
    reports: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    """Build the full result payload for the detail view."""
    payload: dict[str, Any] = {
        "engine_result": engine_result or {},
        "dimensions": {k: v for k, v in dimension_results.items() if v},
        "financial_context_md": financial_context or "",
        "schema_version": "2.0",
    }
    # Include reports as markdown strings for front-end rendering
    if reports:
        payload["reports"] = {k: v for k, v in reports.items() if v}
    return payload


# ---------------------------------------------------------------------------
# HTTP call to backend
# ---------------------------------------------------------------------------


def _post_finalize(
    entry_id: str,
    status: str,
    summary: dict[str, Any] | None,
    payload: dict[str, Any] | None,
    error_message: str | None,
) -> tuple[bool, str]:
    """POST to /api/v1/templates/_internal/entries/{entry_id}/finalize."""
    url = f"{_api_base()}/api/v1/templates/_internal/entries/{entry_id}/finalize"
    body = json.dumps(
        {
            "status": status,
            "summary": summary,
            "payload": payload,
            "error_message": error_message,
        }
    ).encode("utf-8")

    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")
    if token:
        req.add_header("X-Internal-Service-Token", token)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            return resp.status < 400, f"HTTP {resp.status} {text[:200]}"
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
    p.add_argument("--entry-id", required=True, help="template_entries.entry_id (UUID)")
    p.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing engine_result.json and d1.json..d7.json",
    )
    p.add_argument(
        "--status",
        default="completed",
        choices=["completed", "failed"],
        help="Final status to record (default: completed)",
    )
    p.add_argument(
        "--error-message",
        default=None,
        help="Required when --status=failed",
    )
    args = p.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"[persist_entry] ERROR: data dir does not exist: {data_dir}", file=sys.stderr)
        return 2

    if args.status == "failed":
        ok, info = _post_finalize(
            entry_id=args.entry_id,
            status="failed",
            summary=None,
            payload=None,
            error_message=args.error_message or "Analysis failed (no message)",
        )
        print(f"[persist_entry] failed-finalize: ok={ok} {info}")
        return 0 if ok else 1

    # Read all available artifacts.
    engine_result = _read_json(data_dir / "engine_result.json")
    financial_ctx = _read_text(data_dir / "financial_context.md")

    # v2.0: structured/ subdirectory; fallback to legacy flat d{N}.json
    structured_dir = data_dir / "structured"
    reports_dir = data_dir / "reports"

    dimensions: dict[str, dict[str, Any] | None] = {}
    reports: dict[str, str | None] = {}
    for i in range(1, 8):
        key = f"D{i}"
        # Try v2.0 path first, then legacy
        structured_path = structured_dir / f"d{i}.json"
        legacy_path = data_dir / f"d{i}.json"
        dimensions[key] = _read_json(structured_path) or _read_json(legacy_path)
        # Read report if exists
        report_path = reports_dir / f"d{i}_report.md"
        reports[key] = _read_text(report_path)

    found_dims = [k for k, v in dimensions.items() if v]
    found_reports = [k for k, v in reports.items() if v]
    print(f"[persist_entry] data dir: {data_dir}")
    print(f"[persist_entry] engine_result.json: {'OK' if engine_result else 'MISSING'}")
    print(f"[persist_entry] financial_context.md: {'OK' if financial_ctx else 'MISSING'}")
    print(f"[persist_entry] dimensions found: {found_dims}")
    print(f"[persist_entry] reports found: {found_reports}")

    if not engine_result and not found_dims:
        # Nothing usable — mark failed.
        ok, info = _post_finalize(
            entry_id=args.entry_id,
            status="failed",
            summary=None,
            payload=None,
            error_message="No analysis artifacts found in data dir",
        )
        print(f"[persist_entry] failed-finalize: ok={ok} {info}")
        return 1

    summary = _build_summary(engine_result, dimensions.get("D7"))
    payload = _build_payload(engine_result, dimensions, financial_ctx, reports)
    print(f"[persist_entry] summary keys: {list(summary.keys())}")

    ok, info = _post_finalize(
        entry_id=args.entry_id,
        status="completed",
        summary=summary,
        payload=payload,
        error_message=None,
    )
    print(f"[persist_entry] finalize: ok={ok} {info}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
