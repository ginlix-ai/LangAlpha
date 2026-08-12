#!/usr/bin/env python3
"""Calibrate per-model ``speed`` and ``intelligence`` (1-5) in models.json from
Artificial Analysis's free Data API.

Source of truth for the two genuinely-subjective editorial fields in the
model-detail flyout:
  - ``intelligence`` <- AA ``artificial_analysis_intelligence_index``
  - ``speed``        <- AA ``median_output_tokens_per_second`` (throughput)

AA lists one row PER reasoning-effort per model (xhigh/high/medium/low/
non-reasoning). Two ways to read an "intelligence" number off that:

  --basis served  (default) pick the AA row whose effort matches how WE run the
                  model (from manifest ``parameters``), nearest-effort fallback.
                  Honest: gpt-5.4@medium rates below gpt-5.5, not tied to it.
  --basis ceiling pick the highest-intelligence row. Simpler, but mid-tier
                  "full" models look near-flagship.

The AA index spans the whole AA universe (~5..62 in v4); our manifest is all
frontier models, so a global quantile collapses to 5. We bucket with
FRONTIER-CALIBRATED absolute thresholds (INTEL_BANDS / SPEED_BANDS) so a "5"
always means the same thing and the lineup still spreads across 1-5.

Usage:
  python scripts/ops/calibrate_model_ratings.py                  # dry-run, served basis
  python scripts/ops/calibrate_model_ratings.py --basis ceiling  # dry-run, ceiling
  python scripts/ops/calibrate_model_ratings.py --apply --fields intelligence
  python scripts/ops/calibrate_model_ratings.py --apply --fields intelligence,speed
  python scripts/ops/calibrate_model_ratings.py --no-cache       # force refetch

Key resolution: $ARTIFICIAL_ANALYSIS_API_KEY, else --env-file (defaults to the
repo's .env). Free tier = 100 req/day; the 3-page fetch is cached to
/tmp/aa_models_cache.json.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import urllib.request
from pathlib import Path

AA_BASE = "https://artificialanalysis.ai/api/v2/language/models/free"
CACHE = Path("/tmp/aa_models_cache.json")

REPO = Path(__file__).resolve().parents[2]  # .../langalpha
MANIFEST = REPO / "src/llms/manifest/models.json"
DEFAULT_ENV = REPO / ".env"

# Frontier-calibrated bands. (lower_bound_inclusive, tier) high->low.
INTEL_BANDS = [(56, 5), (49, 4), (42, 3), (35, 2), (0, 1)]   # <- AA intelligence index
SPEED_BANDS = [(150, 5), (110, 4), (75, 3), (50, 2), (0, 1)]  # <- AA output tokens/sec

# Our access/region/serving variants that map onto a base model's AA rating.
VARIANT_SUFFIXES = (
    "-oauth-1m", "-oauth", "-anthropic", "-cn", "-intl",
    "-coding", "-highspeed", "-spark",
)
# Noise tokens dropped during name matching (release qualifiers, not identity).
STOP_TOKENS = {"preview", "experimental", "exp"}

# Models AA's language endpoint doesn't track (or tracks without throughput).
# Hand-grounded from the cited sources; applied to every visible variant of the
# base, and only for the fields present (so AA-derived fields survive otherwise).
MANUAL = {
    # Qwen commercial SKUs <- their OSS siblings on AA (per request).
    "qwen3.5-plus":  {"speed": 2, "intelligence": 3,
                      "ref": "AA Qwen3.5 397B-A17B (Reasoning): II 45, 51.8 tok/s"},
    "qwen3.6-flash": {"speed": 5, "intelligence": 3,
                      "ref": "AA Qwen3.6 35B-A3B (Reasoning): II 43.5, 172 tok/s"},
    # GLM turbo throughput <- OpenRouter/Fireworks (~48-70 tok/s). 'turbo' is
    # cheap/agentic, not high-throughput. Intelligence stays AA-derived.
    "glm-5-turbo":  {"speed": 2, "ref": "OpenRouter ~48 / Fireworks 70 tok/s -> band 2"},
    "glm-5v-turbo": {"speed": 2, "ref": "GLM-turbo family throughput ~48-70 tok/s"},
    # Doubao Seed 2.0 <- LMArena + ByteDance Seed2.0 Model Card (Feb 2026); no
    # AA index yet, no public tok/s, speed left as hand estimate (Pro slow ->
    # Mini fast).
    "doubao-seed-2.0-pro":  {"intelligence": 4,
                             "ref": "LMArena Elo ~1466 (~21st), ~40 below the frontier cluster "
                                    "(GPT-5.5/Opus-4.7/Gemini-3.1-Pro ~1505). ByteDance's model card "
                                    "benches only vs the Feb-2026 frontier (GPT-5.2/Opus-4.5/"
                                    "Gemini-3-Pro) and concedes coding + long-tail gaps: GPQA 88.9 "
                                    "vs 92.4, SimpleQA 36 vs 72, SWE-Verified 76.5 vs Opus-4.5 80.9"},
    "doubao-seed-2.0-lite": {"intelligence": 3,
                             "ref": "ByteDance benches Lite vs GPT-5-mini/Gemini-3-Flash (efficient "
                                    "tier, not flagships): MMLU-Pro 87.7, AIME25 93.0, GPQA 85.1, "
                                    "SWE-Verified 73.5; strong for its class, no independent data"},
    "doubao-seed-2.0-code": {"intelligence": 4,
                             "ref": "self-report SWE-Verified 76.5 / LiveCodeBench-v6 87.8, Pro-equal "
                                    "coding but below Opus-4.5 (80.9). NB: AA's 'Doubao Seed Code' "
                                    "II-34/#10 entry is the older Nov-2025 model, not Seed-2.0-Code"},
    "doubao-seed-2.0-mini": {"intelligence": 3,
                             "ref": "smallest Seed 2.0 SKU: self-report AIME25 87.0, GPQA 79.0, "
                                    "SWE-Verified 67.9, benched vs GPT-5-mini/Gemini-3-Flash"},
}


def band(value: float, bands: list[tuple[float, int]]) -> int:
    for lo, tier in bands:
        if value >= lo:
            return tier
    return 1


def get_key(env_file: Path) -> str:
    key = (os.environ.get("ARTIFICIAL_ANALYSIS_API_KEY") or "").strip()
    if key:
        return key
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("ARTIFICIAL_ANALYSIS_API_KEY="):
                return line.split("=", 1)[1].strip().strip("'\"")
    sys.exit(f"No ARTIFICIAL_ANALYSIS_API_KEY in env or {env_file}")


def fetch_aa(key: str, use_cache: bool) -> list[dict]:
    if use_cache and CACHE.exists():
        return json.loads(CACHE.read_text())
    out: list[dict] = []
    page = 1
    while True:
        req = urllib.request.Request(f"{AA_BASE}?page={page}", headers={"x-api-key": key})
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read())
        out.extend(payload["data"])
        if not payload.get("pagination", {}).get("has_more"):
            break
        page += 1
    CACHE.write_text(json.dumps(out))
    return out


def tokens(s: str) -> frozenset[str]:
    """Order-insensitive token set, parens + release qualifiers dropped:
    'claude-haiku-4-5' ~ 'Claude 4.5 Haiku', 'gemini-3.1-pro' ~ 'Gemini 3.1 Pro Preview'."""
    s = re.sub(r"\(.*?\)", "", s.lower())
    return frozenset(t for t in re.findall(r"[a-z]+|\d+", s) if t and t not in STOP_TOKENS)


def base_name(model_key: str) -> str:
    for suf in VARIANT_SUFFIXES:
        if model_key.endswith(suf):
            return model_key[: -len(suf)]
    return model_key


def collapse_base(model_key: str) -> str:
    """Strip ALL variant suffixes iteratively, for matching against MANUAL keys
    (e.g. 'qwen3.5-plus-intl' -> 'qwen3.5-plus')."""
    changed = True
    while changed:
        changed = False
        for suf in VARIANT_SUFFIXES:
            if model_key.endswith(suf):
                model_key = model_key[: -len(suf)]
                changed = True
    return model_key


def _num(v) -> float | None:
    """Coerce an AA value to a finite float; None for missing/str-garbage/NaN
    so a schema drift can't crash the run or silently band to 1."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def ii_of(row: dict) -> float | None:
    return _num(row.get("evaluations", {}).get("artificial_analysis_intelligence_index"))


def target_effort_rank(cfg: dict) -> int | None:
    """How hard WE run the model, 0 (non-reasoning) .. 4 (xhigh/max)."""
    p = cfg.get("parameters", {}) or {}
    eff = (p.get("reasoning") or {}).get("effort")
    if eff:
        return {"xhigh": 4, "high": 3, "medium": 2, "low": 1, "minimal": 0}.get(eff, 2)
    oc = (p.get("output_config") or {}).get("effort")
    if oc:
        return {"xhigh": 4, "high": 3, "medium": 2, "low": 1}.get(oc, 4)
    th = p.get("thinking") or (cfg.get("extra_body", {}) or {}).get("thinking") or {}
    if th:
        return {"adaptive": 4, "enabled": 3, "disabled": 0}.get(th.get("type"), 3)
    if p.get("thinking_level") == "high" or p.get("include_thoughts"):
        return 3
    return None  # unknown -> ceiling


def aa_effort_rank(name: str) -> int | None:
    n = name.lower()
    if "instant" in n or "non-reasoning" in n or "non reasoning" in n:
        return 0
    if "xhigh" in n or "max effort" in n:
        return 4
    if "high" in n:
        return 3
    if "medium" in n:
        return 2
    if "low" in n:
        return 1
    return None  # no effort qualifier on this row


def pick_row(rows: list[dict], target: int | None, basis: str) -> dict:
    rows = [r for r in rows if ii_of(r) is not None]
    if not rows:
        return {}
    if basis == "ceiling" or target is None:
        return max(rows, key=ii_of)
    ranked = [(aa_effort_rank(r["name"]), r) for r in rows]
    cand = [(rk, r) for rk, r in ranked if rk is not None]
    if not cand:
        return max(rows, key=ii_of)
    # nearest effort to how we serve it; tie-break on higher intelligence
    return min(cand, key=lambda x: (abs(x[0] - target), -(ii_of(x[1]) or -1)))[1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write models.json (default: dry-run)")
    ap.add_argument("--basis", choices=("served", "ceiling"), default="served")
    ap.add_argument("--fields", default="intelligence,speed",
                    help="comma list of fields to write on --apply")
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    args = ap.parse_args()
    write_fields = {f.strip() for f in args.fields.split(",") if f.strip()}

    aa = fetch_aa(get_key(args.env_file), use_cache=not args.no_cache)
    by_tokens: dict[frozenset[str], list[dict]] = {}
    for m in aa:
        for label in (m["slug"], m["name"]):
            by_tokens.setdefault(tokens(label), []).append(m)

    manifest = json.loads(MANIFEST.read_text())
    visible = [k for k, v in manifest.items() if v.get("visible")]

    # MANUAL overrides: applied (on --apply) to every visible variant of each
    # base, for the listed fields only — computed up front so dry-run rows can
    # show the value --apply would actually write.
    overrides = {k: MANUAL[collapse_base(k)] for k in visible if collapse_base(k) in MANUAL}

    matched, unmatched, changes = [], [], 0
    print(f"basis={args.basis}  fields={','.join(sorted(write_fields))}\n")
    print(f"{'model':30s} {'cur s/i':>7}  {'AA II':>6} {'AA tps':>7}  {'new s/i':>7}  AA row (effort matched)")
    print("-" * 100)
    for k in sorted(visible):
        cur = manifest[k]
        cur_s, cur_i = cur.get("speed"), cur.get("intelligence")
        rows = by_tokens.get(tokens(k)) or by_tokens.get(tokens(base_name(k)))
        row = pick_row(rows, target_effort_rank(cur), args.basis) if rows else {}
        if not row:
            ov = overrides.get(k, {})
            new_s = ov["speed"] if ("speed" in ov and "speed" in write_fields) else cur_s
            new_i = ov["intelligence"] if ("intelligence" in ov and "intelligence" in write_fields) else cur_i
            label = "(manual)" if (new_s, new_i) != (cur_s, cur_i) else "(keep)"
            if label == "(manual)":
                changes += 1
            unmatched.append(k)
            print(f"{k:30s} {f'{cur_s}/{cur_i}':>7}  {'—':>6} {'—':>7}  {f'{new_s}/{new_i}' if label == '(manual)' else label:>8}  — no AA match")
            continue
        ii, tps, name = ii_of(row), _num(row.get("performance", {}).get("median_output_tokens_per_second")), row["name"]
        new_i = band(ii, INTEL_BANDS) if ("intelligence" in write_fields and ii is not None) else cur_i
        new_s = band(tps, SPEED_BANDS) if ("speed" in write_fields and tps is not None) else cur_s
        flag = "" if (new_s == cur_s and new_i == cur_i) else "  *"
        if flag:
            changes += 1
        matched.append((k, new_s, new_i))
        ii_s = f"{ii:.1f}" if ii is not None else "—"
        tps_s = f"{tps:.0f}" if tps is not None else "—"
        print(f"{k:30s} {f'{cur_s}/{cur_i}':>7}  {ii_s:>6} {tps_s:>7}  {f'{new_s}/{new_i}':>7}{flag}  {name[:46]}")

    print("-" * 100)
    print(f"matched={len(matched)}  changed={changes}  unmatched(kept as-is)={len(unmatched)}")
    if unmatched:
        print("UNMATCHED (AA has no row; see MANUAL below for any covered): " + ", ".join(unmatched))

    if overrides:
        print("\nMANUAL OVERRIDES (sourced; AA-untracked or no-throughput):")
        for b, ov in MANUAL.items():
            sets = ", ".join(f"{f}={ov[f]}" for f in ("speed", "intelligence") if f in ov)
            print(f"  {b:24s} {sets:24s} <- {ov['ref']}")

    if args.apply:
        for k, s, i in matched:
            if "speed" in write_fields:
                manifest[k]["speed"] = s
            if "intelligence" in write_fields:
                manifest[k]["intelligence"] = i
        for k, ov in overrides.items():
            for f in ("speed", "intelligence"):
                if f in ov and f in write_fields:
                    manifest[k][f] = ov[f]
        # Match the manifest's canonical format exactly so the diff is values-only.
        MANIFEST.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
        print(f"\nWROTE {MANIFEST} — fields={','.join(sorted(write_fields))}; "
              f"{len(matched)} AA models + {len(overrides)} manual-override variants")
    else:
        print("\nDRY-RUN — re-run with --apply to write models.json")


if __name__ == "__main__":
    main()
