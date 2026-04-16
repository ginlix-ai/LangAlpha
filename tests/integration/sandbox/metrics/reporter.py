from __future__ import annotations

import dataclasses
import datetime
import json
import statistics
import sys
from typing import Any

from .collector import MetricsCollector, OperationMetric


def _percentile(values: list[float], pct: float) -> float:
    """Compute the *pct*-th percentile of *values* using linear interpolation.

    Edge cases:
    - Empty list returns 0.0.
    - Single-element list returns that element regardless of *pct*.
    """
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    # Index into the sorted array (0-based).  pct is in [0, 100].
    k = (pct / 100) * (n - 1)
    lo = int(k)
    hi = min(lo + 1, n - 1)
    frac = k - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


# ---------------------------------------------------------------------------
# JSON report
# ---------------------------------------------------------------------------

def _category_stats(ops: list[OperationMetric]) -> dict[str, Any]:
    durations = [o.duration_s for o in ops]
    return {
        "count": len(durations),
        "mean_s": round(statistics.mean(durations), 6),
        "median_s": round(statistics.median(durations), 6),
        "p95_s": round(_percentile(durations, 95), 6),
        "max_s": round(max(durations), 6),
        "min_s": round(min(durations), 6),
    }


def _provider_summary(ops: list[OperationMetric]) -> dict[str, Any]:
    total_duration = sum(o.duration_s for o in ops)
    successes = sum(1 for o in ops if o.success)
    success_rate = successes / len(ops) if ops else 0.0

    by_category: dict[str, list[OperationMetric]] = {}
    for o in ops:
        by_category.setdefault(o.category, []).append(o)

    return {
        "total_operations": len(ops),
        "total_duration_s": round(total_duration, 6),
        "success_rate": round(success_rate, 6),
        "by_category": {
            cat: _category_stats(cat_ops) for cat, cat_ops in sorted(by_category.items())
        },
    }


def _cross_provider_comparison(
    ops_by_provider: dict[str, list[OperationMetric]],
) -> dict[str, Any]:
    """Build comparison for categories present in at least 2 providers."""
    # Collect mean durations per (provider, category).
    provider_cat_means: dict[str, dict[str, float]] = {}
    for provider, ops in sorted(ops_by_provider.items()):
        cat_groups: dict[str, list[float]] = {}
        for o in ops:
            cat_groups.setdefault(o.category, []).append(o.duration_s)
        provider_cat_means[provider] = {
            cat: statistics.mean(durs) for cat, durs in cat_groups.items()
        }

    # Determine categories appearing in >= 2 providers.
    all_categories: dict[str, list[str]] = {}  # category -> list of providers
    for provider, cats in provider_cat_means.items():
        for cat in cats:
            all_categories.setdefault(cat, []).append(provider)

    comparison: dict[str, Any] = {}
    for cat, providers in sorted(all_categories.items()):
        if len(providers) < 2:
            continue
        entry: dict[str, Any] = {}
        for p in sorted(providers):
            entry[f"{p}_mean_s"] = round(provider_cat_means[p][cat], 6)

        # Slowdown factor: every provider compared against the first alphabetically.
        baseline = sorted(providers)[0]
        baseline_mean = provider_cat_means[baseline][cat]
        slowdown: dict[str, float] = {}
        if baseline_mean > 0:
            for p in sorted(providers):
                if p != baseline:
                    slowdown[f"{p}_vs_{baseline}"] = round(
                        provider_cat_means[p][cat] / baseline_mean, 6
                    )
        entry["slowdown_factor"] = slowdown
        comparison[cat] = entry

    return comparison


def write_json_report(collector: MetricsCollector, output_path: str) -> None:
    """Serialize collected metrics to a structured JSON file."""
    ops_by_provider: dict[str, list[OperationMetric]] = {}
    for o in collector.operations:
        ops_by_provider.setdefault(o.provider, []).append(o)

    providers_tested = sorted(ops_by_provider.keys())

    summary = {
        provider: _provider_summary(ops)
        for provider, ops in sorted(ops_by_provider.items())
    }

    report: dict[str, Any] = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "providers_tested": providers_tested,
        "summary": summary,
        "cross_provider_comparison": _cross_provider_comparison(ops_by_provider),
        "operations": [dataclasses.asdict(o) for o in collector.operations],
        "retries": [dataclasses.asdict(r) for r in collector.retries],
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Terminal summary
# ---------------------------------------------------------------------------

_CATEGORIES_FOR_TABLE = ("exec", "code_run", "file_io", "session")


def print_terminal_summary(collector: MetricsCollector) -> None:
    """Print a human-readable summary table to stderr."""
    if not collector.operations:
        return

    ops_by_provider: dict[str, list[OperationMetric]] = {}
    for o in collector.operations:
        ops_by_provider.setdefault(o.provider, []).append(o)

    output_file = "sandbox_metrics.json"

    lines: list[str] = []
    lines.append("")
    lines.append("======================== Sandbox Metrics Summary ========================")
    lines.append("")

    # Header
    lines.append(
        f"{'Provider':<11}| {'Ops':>4} | {'Duration':>8} | {'Success':>7} "
        f"| {'Exec (p50)':>10} | {'CodeRun (p50)':>13} | {'FileIO (p50)':>12}"
        f" | {'Session (p50)':>13}"
    )
    lines.append(
        "-----------+------+----------+---------+------------+---------------+"
        "-------------+---------------"
    )

    for provider in sorted(ops_by_provider.keys()):
        ops = ops_by_provider[provider]
        total_ops = len(ops)
        total_dur = sum(o.duration_s for o in ops)
        successes = sum(1 for o in ops if o.success)
        success_pct = (successes / total_ops * 100) if total_ops else 0.0

        cat_p50: dict[str, str] = {}
        for cat in _CATEGORIES_FOR_TABLE:
            cat_ops = [o for o in ops if o.category == cat]
            if cat_ops:
                durations = [o.duration_s for o in cat_ops]
                cat_p50[cat] = f"{_percentile(durations, 50):.2f}s"
            else:
                cat_p50[cat] = "   -"

        lines.append(
            f"{provider:<11}| {total_ops:>4} | {total_dur:>7.1f}s | {success_pct:>6.1f}% "
            f"| {cat_p50['exec']:>10} | {cat_p50['code_run']:>13} | {cat_p50['file_io']:>12}"
            f" | {cat_p50['session']:>13}"
        )

    lines.append("")
    lines.append(f"Full report: {output_file}")
    lines.append("=========================================================================")
    lines.append("")

    sys.stderr.write("\n".join(lines))
