"""``uv run python -m scripts.data_probe`` — run / plan / show."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from scripts.data_probe import PROBE_VERSION
from scripts.data_probe.canaries import (
    MARKETS,
    asset_classes_for,
    canaries_for,
    representative,
)
from scripts.data_probe.probe import (
    PROBE_SCHEMAS,
    probe_cell,
    scrub,
)
from scripts.data_probe.report import print_cells, print_ruleset, print_table
from scripts.data_probe.store import (
    credential_fingerprint,
    load_for_merge,
    merge_rulesets,
    save_ruleset,
)
from market_protocol.intervals import to_schema
from market_protocol.routing.ruleset import (
    PROBED_SURFACES,
    Cell,
    CellKey,
    ProviderInfo,
    Ruleset,
    Surface,
    load_ruleset,
)

# Credential each provider is configured by. ginlix-data (and tushare, which
# ginlix-data serves) is addressed by URL, so only its presence is recorded;
# yfinance is keyless.
PROVIDER_ENV: dict[str, Optional[str]] = {
    "fmp": "FMP_API_KEY",
    "tushare": None,
    "ginlix-data": None,
    "yfinance": None,
}

SURFACES = tuple(s.value for s in PROBED_SURFACES)
WAIT_POLL_S = 30
WAIT_CAP_S = 8 * 3600


# ---------------------------------------------------------------------------
# Argument plumbing
# ---------------------------------------------------------------------------


def _csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.data_probe",
        description=(
            "Probe every configured market-data provider against canary baskets "
            "and write the routing ruleset (data_routing.yaml)."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="probe providers and write the ruleset")
    run.add_argument("--market", default="auto",
                     help=f"comma-separated: {','.join(MARKETS)}, or 'auto' (default)")
    run.add_argument("--surface", default=",".join(SURFACES),
                     help=f"comma-separated: {','.join(SURFACES)}")
    run.add_argument("--interval", default="1m,5m",
                     help=f"intraday bar schemas, bare or full: {','.join(PROBE_SCHEMAS)}")
    run.add_argument("--provider", default=None,
                     help="restrict to these providers (default: every configured one)")
    run.add_argument("--wait-for-open", action="store_true",
                     help="sleep until the first requested market opens, so lag is measurable")
    run.add_argument("--out", default=None,
                     help="ruleset path (default: $DATA_ROUTING_RULESET or ./data_routing.yaml)")
    run.add_argument("--dry-run", action="store_true", help="print the table, write nothing")
    run.add_argument("--verbose", "-v", action="store_true", help="log every provider call")

    plan = sub.add_parser("plan", help="what each market can measure right now")
    plan.add_argument("--market", default="auto", help="comma-separated markets, or 'auto'")

    show = sub.add_parser("show", help="print the current ruleset")
    show.add_argument("--path", default=None, help="ruleset path")
    show.add_argument("--verbose", "-v", action="store_true",
                      help="per-cell provider detail with exclusion reasons and notes")
    return parser


def resolve_markets(spec: str) -> list[str]:
    if spec.strip() == "auto":
        return list(MARKETS)
    unknown = [m for m in _csv(spec) if m not in MARKETS]
    if unknown:
        raise SystemExit(f"unknown market(s): {', '.join(unknown)} (known: {', '.join(MARKETS)})")
    return _csv(spec)


def resolve_surfaces(spec: str) -> list[Surface]:
    out = []
    for name in _csv(spec):
        if name not in SURFACES:
            raise SystemExit(f"unknown surface: {name} (known: {', '.join(SURFACES)})")
        out.append(Surface(name))
    return out


def _ruleset_path(arg: str | None) -> Path:
    if arg:
        return Path(arg)
    from src.data_client.registry import default_ruleset_path

    return default_ruleset_path()


def resolve_intervals(spec: str) -> list[str]:
    """Schema ids for the requested widths; ``1m`` and ``ohlcv-1m`` both read."""
    out: list[str] = []
    unknown: list[str] = []
    for token in _csv(spec):
        try:
            schema = to_schema(token)
        except ValueError:
            schema = None
        if schema in PROBE_SCHEMAS:
            out.append(schema)
        else:
            unknown.append(token)
    if unknown:
        raise SystemExit(
            f"unknown interval(s): {', '.join(unknown)} (known: {', '.join(PROBE_SCHEMAS)})"
        )
    return out


# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------


async def build_sources(only: list[str] | None = None) -> dict[str, Any]:
    """Every source whose credentials are present, built directly.

    Deliberately bypasses ``config.yaml`` routing: the probe's job is to find
    out what a token can do, not to re-ask what someone already assumed.
    """
    from src.data_client.registry import _SOURCE_REGISTRY

    wanted = set(only) if only else None
    if wanted:
        unknown = wanted - set(_SOURCE_REGISTRY)
        if unknown:
            raise SystemExit(
                f"unknown provider(s): {', '.join(sorted(unknown))} "
                f"(known: {', '.join(sorted(_SOURCE_REGISTRY))})"
            )
    sources: dict[str, Any] = {}
    for name, (available, build) in _SOURCE_REGISTRY.items():
        if wanted is not None and name not in wanted:
            continue
        try:
            if not available():
                continue
            sources[name] = await build()
        except Exception as exc:
            print(f"  ! {name}: could not build source: {scrub(exc)}", file=sys.stderr)
    return sources


def configured_markets() -> dict[str, set[str]]:
    """Markets ``config.yaml`` lists each provider for, under any capability.

    The chain never routes a provider in a cell outside these, so probing one
    there spends quota on a verdict nothing reads; worse, a CN-only source may
    answer a US canary through another feed and look routable.
    """
    from src.config.settings import get_market_data_providers

    scopes: dict[str, set[str]] = {}
    for cfg in get_market_data_providers():
        scope = scopes.setdefault(cfg["name"], set())
        for field in ("markets", "intraday_markets", "daily_markets", "snapshot_markets"):
            scope.update(cfg.get(field) or ())
    return scopes


def sources_for_market(
    sources: dict[str, Any], scopes: dict[str, set[str]], market: str
) -> dict[str, Any]:
    """The sources a cell in *market* is probed with.

    A provider config does not list at all has no stated markets and is probed
    everywhere; the chain ignores its verdicts until config names it.
    """
    from src.data_client.market_data_provider import _market_matches

    return {
        name: source for name, source in sources.items()
        if name not in scopes or _market_matches(scopes[name], market)
    }


def provider_infos(names: list[str]) -> dict[str, ProviderInfo]:
    infos: dict[str, ProviderInfo] = {}
    for name in names:
        env = PROVIDER_ENV.get(name)
        infos[name] = ProviderInfo(
            configured=True,
            fingerprint=credential_fingerprint(os.environ.get(env)) if env else None,
        )
    return infos


async def close_sources(sources: dict[str, Any]) -> None:
    for source in sources.values():
        try:
            await source.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Clocks
# ---------------------------------------------------------------------------


def _clock(market: str, *, regular_only: bool = False):
    from src.data_client.instrument_clock import clock_for

    symbol = representative(market)
    is_index = symbol is not None and symbol.startswith("^")
    return clock_for(symbol, is_index, regular_only=regular_only)


def seconds_until_open(market: str, now: datetime) -> int:
    """Seconds to the regular open *market* next reads ``open`` at (never 0).

    The clock reads extended hours as closed, so pre-market counts down to
    today's bell and after-hours to tomorrow's; while the session runs it names
    the next session's open.
    """
    clock = _clock(market, regular_only=True)
    return int(clock.seconds_until_next_open(now) or clock.seconds_until_next_session_open(now))


def market_phase(market: str, now: datetime) -> str:
    return _clock(market).market_phase(now)


def market_tz(market: str) -> ZoneInfo:
    return ZoneInfo(str(_clock(market).tz))


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def plan_rows(markets: list[str], now: datetime, local_tz: Any) -> list[list[str]]:
    rows = []
    for market in markets:
        phase = market_phase(market, now)
        secs = seconds_until_open(market, now)
        # +30s so the printed minute rounds rather than always reading one
        # minute early (seconds_until_open floors).
        nxt = now + timedelta(seconds=secs + 30)
        rows.append([
            market,
            phase,
            "yes" if phase == "open" else "no",
            "lag" if phase == "open" else "completeness",
            nxt.astimezone(market_tz(market)).strftime("%Y-%m-%d %H:%M %Z"),
            nxt.astimezone(local_tz).strftime("%Y-%m-%d %H:%M %Z"),
            _humanize(secs),
        ])
    return rows


def _humanize(seconds: int) -> str:
    if seconds <= 0:
        return "now"
    hours, rem = divmod(int(seconds), 3600)
    return f"{hours}h{rem // 60:02d}m"


def cmd_plan(args: argparse.Namespace) -> int:
    markets = resolve_markets(args.market)
    now = datetime.now(timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo
    print_table(
        ("market", "phase", "lag now", "measures",
         "next open (venue)", "next open (local)", "in"),
        plan_rows(markets, now, local_tz),
        "market plan",
    )
    open_markets = [m for m in markets if market_phase(m, now) == "open"]
    closed = [m for m in markets if m not in open_markets]
    print()
    if open_markets:
        print("measure lag now:")
        print(f"  uv run python -m scripts.data_probe run --market {','.join(open_markets)} "
              f"--surface intraday --interval 1m,5m")
    if closed:
        print("measure session completeness now:")
        print(f"  uv run python -m scripts.data_probe run --market {','.join(closed)} "
              f"--surface intraday,daily,snapshot --interval 1m,5m")
        soonest = min(closed, key=lambda m: seconds_until_open(m, now))
        print(f"come back for {soonest} lag with:")
        print(f"  uv run python -m scripts.data_probe run --market {soonest} "
              f"--surface intraday --interval 1m --wait-for-open")
    return 0


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


def cmd_show(args: argparse.Namespace) -> int:
    path = _ruleset_path(args.path)
    ruleset = load_ruleset(path)
    if ruleset is None:
        print(f"no readable ruleset at {path}. Run "
              f"`uv run python -m scripts.data_probe run` first")
        return 1
    print_ruleset(ruleset, verbose=args.verbose)
    return 0


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


async def wait_for_open(
    market: str,
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    poll_s: int = WAIT_POLL_S,
    cap_s: int = WAIT_CAP_S,
    sleeper: Callable[[float], Any] = asyncio.sleep,
    printer: Callable[[str], None] = print,
) -> bool:
    """Block until *market* is open. False when the cap runs out first."""
    now = now_fn()
    if market_phase(market, now) == "open":
        printer(f"{market} is already open, probing now")
        return True
    secs = seconds_until_open(market, now)
    printer(f"{market} opens in {_humanize(secs)} "
            f"({(now + timedelta(seconds=secs + 30)).astimezone(market_tz(market)):%Y-%m-%d %H:%M %Z}) "
            f"- waiting, polling every {poll_s}s")
    waited = 0
    while waited < cap_s:
        await sleeper(poll_s)
        waited += poll_s
        if market_phase(market, now_fn()) == "open":
            printer(f"{market} is open after {_humanize(waited)}, probing now")
            return True
    printer(f"gave up waiting for {market} after {_humanize(cap_s)}")
    return False


def cell_keys(
    markets: list[str], surfaces: list[Surface], intervals: list[str]
) -> list[CellKey]:
    keys: list[CellKey] = []
    for market in markets:
        for asset_class in asset_classes_for(market):
            for surface in surfaces:
                spans = intervals if surface is Surface.INTRADAY else [None]
                for interval in spans:
                    keys.append(CellKey(market=market, asset_class=asset_class,
                                        surface=surface, interval=interval))
    return keys


async def run_probe(args: argparse.Namespace) -> int:
    markets = resolve_markets(args.market)
    surfaces = resolve_surfaces(args.surface)
    intervals = resolve_intervals(args.interval)
    providers = _csv(args.provider) if args.provider else None
    path = _ruleset_path(args.out)
    if not args.dry_run:
        load_for_merge(path)  # refuse before a minutes-long run, not after it

    sources = await build_sources(providers)
    if not sources:
        print("no market-data provider is configured. Set FMP_API_KEY / GINLIX_DATA_URL, "
              "or install yfinance", file=sys.stderr)
        return 1
    print(f"providers: {', '.join(sources)}", flush=True)

    if args.wait_for_open and markets:
        await wait_for_open(markets[0])

    log = (lambda m: print(m, flush=True)) if args.verbose else (lambda _m: None)
    now = datetime.now(timezone.utc)
    scopes = configured_markets()
    cells: list[Cell] = []
    unmeasured: list[str] = []
    try:
        keys = cell_keys(markets, surfaces, intervals)
        for index, key in enumerate(keys, start=1):
            symbols = canaries_for(key.market, key.asset_class)
            cell_sources = sources_for_market(sources, scopes, key.market)
            if not symbols or not cell_sources:
                continue
            # Flushed: the run is minutes long behind a per-minute quota, and a
            # pipe would otherwise show nothing until it finished.
            print(f"[{index}/{len(keys)}] {key.as_str()} ({', '.join(symbols)})", flush=True)
            cell = await probe_cell(key, cell_sources, symbols, log=log)
            cells.append(cell)
            answered = {p.name for p in cell.providers}
            unmeasured += [f"{n} @ {key.as_str()}" for n in cell_sources if n not in answered]
    finally:
        await close_sources(sources)

    print()
    print_cells(cells, "probe results")
    if unmeasured:
        # Each keeps whatever verdict an earlier run gave it; the rest of the
        # run is measured and written all the same.
        print("\nno answer, so no verdict (re-run to measure): " + ", ".join(unmeasured),
              file=sys.stderr)
    status = 1 if unmeasured else 0

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return status

    update = Ruleset(
        generated_at=now,
        probe_version=PROBE_VERSION,
        providers=provider_infos(list(sources)),
        cells=cells,
    )
    merged = merge_rulesets(load_for_merge(path), update)
    save_ruleset(merged, path)
    print(f"\nwrote {len(cells)} cells to {path} ({len(merged.cells)} total)")
    return status


def cmd_run(args: argparse.Namespace) -> int:
    return asyncio.run(run_probe(args))


# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    load_dotenv()
    args = build_parser().parse_args(argv)
    return {"run": cmd_run, "plan": cmd_plan, "show": cmd_show}[args.command](args)
