#!/usr/bin/env python3
"""Measure the outbound requests of get_sec_filing calls.

Runs an async entry point once per case inside ``metered_call`` and prints one
table row per call: wall time, network requests by host, HTTP cache hits,
critical path against summed request time, peak concurrency and bytes.

Usage:
  uv run python scripts/sec_filing_bench.py AAPL:10-K NVDA:8-K --cold --repeat 2
  uv run python scripts/sec_filing_bench.py AAPL:10-Q:2025:Q2:statements --parallel
  uv run python scripts/sec_filing_bench.py --cases-file cases.json --json out.json
  uv run python scripts/sec_filing_bench.py AAPL:10-K --entry pkg.module:async_fn

A case is ``SYMBOL:FORM[:FISCAL_YEAR[:FISCAL_PERIOD[:VIEW]]]`` (empty fields
skip), or an object with those keys in a JSON list. Case keys are passed to
the entry point as keyword arguments; ``form`` is sent as ``form`` when the entry
point has that parameter and as ``filing_type`` otherwise, and a key the entry
point does not accept is dropped with a warning.

--cold points EDGAR_LOCAL_DATA_DIR at a fresh temp dir before edgar loads and
empties it before each case (before the first round with --parallel), so the
first run of a case misses the HTTP cache; its repeats are warm. It also drops the cached
FMP transcripts of the cases' symbols, and no other Redis key. In-process caches (an already-built
Company object, say) survive the wipe, so cold means cold at the HTTP level.

The entry point's own ``metered_call`` joins the case's meter, so a row covers every
request the call makes.

The harness copies the context into ``run_in_executor`` work (an entry point
that still uses a bare thread pool would otherwise go unmetered, see
src/tools/sec/metering.py); --no-context-shim turns that off.
"""

from __future__ import annotations

# Importing src loads .env tracing; a bench run must never trace to LangSmith.
import os

os.environ["LANGSMITH_TRACING"] = "false"
os.environ["LANGCHAIN_TRACING_V2"] = "false"

import argparse  # noqa: E402
import asyncio  # noqa: E402
import contextvars  # noqa: E402
import importlib  # noqa: E402
import inspect  # noqa: E402
import json  # noqa: E402
import shutil  # noqa: E402
import sys  # noqa: E402
import tempfile  # noqa: E402
import time  # noqa: E402
from contextlib import contextmanager  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Callable  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
CASE_FIELDS = ("symbol", "form", "fiscal_year", "fiscal_period", "view")
DEFAULT_ENTRY = "src.tools.sec.tool:get_sec_filing_async"


def parse_case(spec: str) -> dict[str, Any]:
    parts = spec.split(":")
    if len(parts) < 2 or len(parts) > len(CASE_FIELDS):
        raise SystemExit(f"bad case {spec!r}: expected SYMBOL:FORM[:FISCAL_YEAR[:FISCAL_PERIOD[:VIEW]]]")
    case = {k: v for k, v in zip(CASE_FIELDS, parts) if v}
    if "fiscal_year" in case:
        case["fiscal_year"] = int(case["fiscal_year"])
    return case


def case_label(case: dict[str, Any]) -> str:
    return ":".join(str(case[k]) for k in CASE_FIELDS if k in case)


def build_kwargs(fn: Callable, case: dict[str, Any], warned: set[str]) -> dict[str, Any]:
    params = inspect.signature(fn).parameters
    takes_any = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
    kwargs: dict[str, Any] = {}
    for key, value in case.items():
        name = key
        if key == "form" and "form" not in params and "filing_type" in params:
            name = "filing_type"
        if name in params or takes_any:
            kwargs[name] = value
        elif key not in warned:
            warned.add(key)
            print(f"warning: entry point has no {key!r} parameter; dropped", file=sys.stderr)
    return kwargs


def propagate_context(loop: asyncio.AbstractEventLoop) -> None:
    original = loop.run_in_executor

    def run_in_executor(executor, func, *args):
        return original(executor, contextvars.copy_context().run, func, *args)

    loop.run_in_executor = run_in_executor  # type: ignore[method-assign]


def join_case_meter() -> None:
    """Make every loaded module's ``metered_call`` record into the case's meter when one is open.

    The tool opens its own meter, which would leave the case's meter empty.
    """
    from src.tools.sec import metering

    original = metering.metered_call

    @contextmanager
    def joined(**case: Any):
        outer = metering.current_meter.get()
        if outer is not None:
            yield outer
            return
        with original(**case) as meter:
            yield meter

    for module in list(sys.modules.values()):
        if getattr(module, "metered_call", None) is original:
            module.metered_call = joined


def wipe(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for child in directory.iterdir():
        shutil.rmtree(child) if child.is_dir() else child.unlink()


def fmt_hosts(summary: dict[str, Any]) -> str:
    parts = []
    for host, counts in sorted(summary["requests_by_host"].items()):
        n = counts.get("network", 0) + counts.get("revalidated", 0)
        if n:
            parts.append(f"{host.removesuffix('.gov').removesuffix('.com')}:{n}")
    return " ".join(parts) or "-"


def render(rows: list[dict[str, Any]]) -> str:
    header = ["case", "run", "wall s", "network", "by host", "http hits", "reval", "crit/sum s", "conc", "MB in", "limiter ms", "note"]
    lines = [header]
    for row in rows:
        s = row["summary"]
        lines.append(
            [
                row["case"],
                str(row["run"]),
                f"{s['wall_ms'] / 1000:.2f}",
                str(s["network_count"]),
                fmt_hosts(s),
                str(s["http_cache_hits"]),
                str(s["revalidations"]),
                f"{s['critical_path_ms'] / 1000:.2f}/{s['network_ms_sum'] / 1000:.2f}",
                str(s["max_concurrency"]),
                f"{s['bytes_in'] / 1e6:.2f}",
                f"{s['limiter_wait_ms_sum']:.0f}",
                row.get("error") or "",
            ]
        )
    widths = [max(len(line[i]) for line in lines) for i in range(len(header))]
    out = ["| " + " | ".join(c.ljust(w) for c, w in zip(line, widths)) + " |" for line in lines]
    out.insert(1, "|" + "|".join("-" * (w + 2) for w in widths) + "|")
    return "\n".join(out)


async def run_once(entry: Callable, metered_call: Callable, case: dict[str, Any], run: int, warned: set[str]) -> dict[str, Any]:
    error = None
    result_chars = None
    with metered_call(**case, run=run) as meter:
        try:
            result = await entry(**build_kwargs(entry, case, warned))
            first = result[0] if isinstance(result, tuple) else result
            result_chars = len(first) if isinstance(first, str) else len(str(first))
        except Exception as e:
            error = f"{type(e).__name__}: {e}"[:120]
    return {
        "case": case_label(case),
        "run": run,
        "summary": meter.summary(),
        "error": error,
        "result_chars": result_chars,
        "records": [
            {
                "host": r.host,
                "path_class": r.path_class,
                "status": r.status,
                "cache": r.cache,
                "bytes": r.bytes,
                "start_ms": round((r.started - meter.started) * 1000, 1),
                "end_ms": None if r.ended is None else round((r.ended - meter.started) * 1000, 1),
                "limiter_wait_ms": None if r.limiter_wait_ms is None else round(r.limiter_wait_ms, 1),
            }
            for r in meter.records
        ],
    }


async def cold_reset(edgar_dir: Path, cases: list[dict[str, Any]]) -> None:
    """Empty every cache a cold call must miss: edgartools' HTTP cache, the parsed-filing store and the cases' transcripts.
    The store is pointed at the emptied local directory, so a configured object store cannot serve a cold call."""
    from edgar_engine.cache import LocalDirStore

    from src.tools.sec import edgar
    from src.utils.cache.redis_cache import get_cache_client

    wipe(edgar_dir)
    wipe(Path(os.environ["SEC_FILINGS_CACHE_DIR"]))
    edgar._store = edgar._MeteredStore(LocalDirStore(os.environ["SEC_FILINGS_CACHE_DIR"]))
    client = get_cache_client()
    if client.client is None:
        return
    for symbol in {str(case["symbol"]).upper() for case in cases}:
        async for key in client.client.scan_iter(match=f"sec:fmp:transcript:{symbol}:*"):
            await client.client.delete(key)


async def main(args: argparse.Namespace, cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from src.tools.sec.metering import metered_call
    from src.utils.cache.redis_cache import init_cache

    await init_cache()

    module_name, _, attr = args.entry.partition(":")
    entry = getattr(importlib.import_module(module_name), attr)
    join_case_meter()
    if not args.no_context_shim:
        propagate_context(asyncio.get_running_loop())

    warned: set[str] = set()
    rows: list[dict[str, Any]] = []
    cold_dir = Path(os.environ["EDGAR_LOCAL_DATA_DIR"]) if args.cold else None
    if args.parallel:
        if cold_dir:
            await cold_reset(cold_dir, cases)
        for run in range(1, args.repeat + 1):
            rows.extend(await asyncio.gather(*(run_once(entry, metered_call, c, run, warned) for c in cases)))
        return rows
    for case in cases:
        if cold_dir:
            await cold_reset(cold_dir, [case])
        for run in range(1, args.repeat + 1):
            rows.append(await run_once(entry, metered_call, case, run, warned))
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cases", nargs="*", help="SYMBOL:FORM[:FISCAL_YEAR[:FISCAL_PERIOD[:VIEW]]]")
    p.add_argument("--cases-file", help="JSON list of case objects")
    p.add_argument("--entry", default=DEFAULT_ENTRY, help=f"module:async_function (default {DEFAULT_ENTRY})")
    p.add_argument("--parallel", action="store_true", help="run each round's cases concurrently")
    p.add_argument("--cold", action="store_true", help="fresh EDGAR_LOCAL_DATA_DIR, emptied before each case")
    p.add_argument("--repeat", type=int, default=1, help="calls per case; with --cold only the first is cold")
    p.add_argument("--json", dest="json_path", help="also write every summary and request record here")
    p.add_argument("--no-context-shim", action="store_true", help="do not copy the context into run_in_executor work")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cases = [parse_case(c) for c in args.cases]
    if args.cases_file:
        cases += json.loads(Path(args.cases_file).read_text())
    if not cases:
        raise SystemExit("no cases given")
    cold_root = None
    if args.cold:
        cold_root = tempfile.mkdtemp(prefix="sec_bench_edgar_")
        os.environ["EDGAR_LOCAL_DATA_DIR"] = cold_root
        os.environ["SEC_FILINGS_CACHE_DIR"] = str(Path(cold_root) / "store")
    sys.path.insert(0, str(REPO_ROOT))
    started = time.perf_counter()
    try:
        rows = asyncio.run(main(args, cases))
    finally:
        if cold_root:
            shutil.rmtree(cold_root, ignore_errors=True)
    print(render(rows))
    print(f"\n{len(rows)} calls in {time.perf_counter() - started:.1f}s; entry {args.entry}")
    if args.json_path:
        Path(args.json_path).write_text(json.dumps(rows, indent=2))
