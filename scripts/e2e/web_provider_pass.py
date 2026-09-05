#!/usr/bin/env python3
"""Web provider pass: agent turns → LangSmith attribution → breaker classification.

Proves the two halves of the web-provider work against a live stack and live
upstreams, driven by the real agent rather than by mocks:

  1. Agent turns (flash and ptc, both by default): each turn is told to run a
     web search and fetch a page, so WebSearch/WebFetch run inside a real
     traced turn.
  2. LangSmith attribution: the runs those turns produced — correlated by
     thread id, since several stacks can share one project — must carry the
     provider that actually served them — ``search_engine``/``search_depth``
     on WebSearch, ``fetch_provider``/``fetch_attempts`` plus a
     ``fetch_provider:<p>`` tag on WebFetch. A chain that fell through must
     record WHY (``firecrawl:budget_exceeded`` → ``inhouse:ok``), not just
     that it did.
  3. Breaker classification (the fix): a URL that fails target-side, driven
     past the failure threshold, must leave every provider breaker CLOSED with
     a zero failure count, and must not poison the next healthy fetch. Runs
     in-process because breaker state is process-local and no endpoint exposes
     it; the network calls are real.
  4. Server-side quiet check: the backend must not have logged a single
     breaker transition while all of that happened — healthy failures are
     silent by design, so any ``Circuit breaker [...]`` line is a regression.

Phase 3 imports ``src`` and fails loudly if it cannot — it is the phase that
proves the fix, so it must never quietly self-skip. Phase 4 needs docker and
does skip with an observation when it is absent.

Usage:
  uv run python scripts/e2e/web_provider_pass.py --user local-dev-user \
      [--workspace <ws_id>] [--base http://localhost:8000] \
      [--agents flash,ptc] [--model <model>] [--turn-timeout 420]

Auth: X-Service-Token from INTERNAL_SERVICE_TOKEN (repo .env or environment),
user id via --user or E2E_USER_ID. Threads are kept for visual inspection.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# A script's sys.path[0] is its own directory, never the cwd, so the repo root
# has to be added explicitly — without it the breaker phase finds no ``src`` to
# import and skips the one assertion that proves the fix.
sys.path[:0] = [str(Path(__file__).resolve().parent), str(REPO_ROOT)]

# The SSE/auth plumbing is identical for every wire pass — reuse it rather than
# growing a second copy that can drift.
from subagent_wire_pass import (  # noqa: E402
    CHECKS,
    Env,
    check,
    iter_frames,
    observe,
    read_env_file,
)

# A reserved TLD (RFC 2606) can never resolve, from any network, forever — the
# only way to make "the target is unreachable" deterministic in a live test.
UNREACHABLE = "https://nonexistent-host-{n}.invalid/"
# Cache-busted per run: a cached URL never reaches the router, so it records no
# provider at all — correct, but it would test nothing. The host has to tolerate
# repeat automated traffic; example.com starts serving 403 after a few rounds,
# which turns a provider-attribution assertion into a flake.
HEALTHY = f"https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern?e2e={int(time.time())}"

SEARCH_QUERY = "circuit breaker pattern in distributed systems"

TURN_PROMPT = (
    f"Do exactly two things, then stop. First, run ONE web search for "
    f"'{SEARCH_QUERY}'. Second, fetch the page {HEALTHY} and quote its main "
    f"heading. Reply in two sentences. Do not write any files or code."
)

ATTEMPT_RE = re.compile(r"^[a-z0-9_]+:[a-z0-9_]+$")


# ---------------------------------------------------------------------------
# Phase 1 — agent turns
# ---------------------------------------------------------------------------


def run_turn(env: Env, agent_mode: str, workspace_id: str, model: str | None) -> str | None:
    """Post one turn and drain it. Returns the thread id (kept for inspection)."""
    body: dict = {
        "agent_mode": agent_mode,
        "workspace_id": workspace_id,
        "messages": [{"role": "user", "content": TURN_PROMPT}],
    }
    if model:
        body["llm_model"] = model

    outcome, thread_id, frames = None, None, 0
    deadline = time.time() + env.turn_timeout
    with env.client(read_timeout=env.turn_timeout) as c:
        with c.stream("POST", "/api/v1/threads/messages", json=body) as resp:
            if resp.status_code != 200:
                resp.read()
                check(f"{agent_mode}: turn accepted", False, f"HTTP {resp.status_code}: {resp.text[:200]}")
                return None
            for fr in iter_frames(resp):
                frames += 1
                thread_id = thread_id or str(fr.data.get("thread_id") or "") or None
                if fr.event == "run_end":
                    outcome = fr.data.get("outcome") or "completed"
                    break
                if fr.event == "error":
                    outcome = "error"
                    break
                if time.time() > deadline:
                    outcome = "timeout"
                    break

    check(
        f"{agent_mode}: turn ran to completion",
        outcome == "completed",
        f"outcome={outcome} frames={frames} thread={thread_id}",
    )
    return thread_id


# ---------------------------------------------------------------------------
# Phase 2 — LangSmith attribution
# ---------------------------------------------------------------------------


def langsmith_checks(project: str, since: datetime, threads: list[str]) -> None:
    try:
        from langsmith import Client
    except ImportError:
        observe("langsmith SDK not installed — attribution checks skipped")
        return
    if not os.environ.get("LANGSMITH_API_KEY"):
        observe("LANGSMITH_API_KEY unset — attribution checks skipped")
        return
    if not check("LangSmith: the turns yielded thread ids to correlate on", bool(threads),
                 "no thread id was captured — nothing to attribute"):
        return

    client = Client()
    by_thread: dict[str, dict[str, list[dict]]] = {}
    # Ingest lags the turn; poll rather than sleeping a flat guess.
    for _ in range(18):
        time.sleep(5)
        by_thread = {t: {"fetch": [], "search": []} for t in threads}
        try:
            for tid in threads:
                # Scope to THIS pass's turns. Several stacks can share one
                # project, and a project-wide time window silently grades
                # another branch's runs as if they were ours.
                runs = client.list_runs(
                    project_name=project,
                    start_time=since,
                    limit=100,
                    filter=f'and(eq(metadata_key,"thread_id"),eq(metadata_value,"{tid}"))',
                )
                for r in runs:
                    # A run still in flight has not flushed its stamps yet;
                    # asserting over one reports an empty stamp as a missing one.
                    if r.end_time is None:
                        continue
                    md = (r.extra or {}).get("metadata") or {}
                    row = {"metadata": md, "tags": list(r.tags or []), "name": r.name}
                    if r.name == "WebFetch":
                        by_thread[tid]["fetch"].append(row)
                    elif r.name == "WebSearch":
                        by_thread[tid]["search"].append(row)
        except Exception as e:  # project may not exist yet on a fresh key
            observe(f"LangSmith query failed ({type(e).__name__}: {e}) — retrying")
            continue
        if all(v["fetch"] and v["search"] for v in by_thread.values()):
            break

    fetches = [row for v in by_thread.values() for row in v["fetch"]]
    searches = [row for v in by_thread.values() for row in v["search"]]
    missing = [t for t, v in by_thread.items() if not (v["fetch"] and v["search"])]

    if not check(
        "LangSmith: every turn produced WebFetch and WebSearch runs",
        not missing,
        f"{len(fetches)} WebFetch, {len(searches)} WebSearch across {len(threads)} thread(s)"
        + (f"; incomplete: {missing}" if missing else ""),
    ):
        return

    # A cache hit never reaches a provider, so it has no provider to name — it
    # is labelled fetch_source=cache instead. Only live fetches must attribute.
    live = [f for f in fetches if f["metadata"].get("fetch_source") == "live"]
    cached = [f for f in fetches if f["metadata"].get("fetch_source") == "cache"]
    if cached:
        observe(f"{len(cached)} WebFetch run(s) served from cache — exempt from provider attribution")

    # A fetch that failed on every provider has no provider to name — it carries
    # fetch_error and the attempt list instead, which is the whole point.
    failed = [f for f in live if f["metadata"].get("fetch_error")]
    served = [f for f in live if not f["metadata"].get("fetch_error")]
    for f in failed:
        observe(
            f"fetch failed on every provider ({f['metadata']['fetch_error']}) — "
            f"attributed by attempts: {f['metadata'].get('fetch_attempts')}"
        )
    check(
        "LangSmith: a failed fetch still records the error and the attempts",
        all(f["metadata"].get("fetch_attempts") for f in failed),
        f"{len(failed)} failed run(s), all carrying attempts" if failed else "no failed fetches this run",
    )

    stamped = [f for f in served if f["metadata"].get("fetch_provider")]
    check(
        "LangSmith: every served WebFetch run names the provider that served it",
        bool(served) and len(stamped) == len(served),
        f"{len(stamped)}/{len(served)} served runs carry fetch_provider "
        f"(e.g. {stamped[0]['metadata'].get('fetch_provider') if stamped else 'n/a'})",
    )

    # The exact tag, not merely some fetch_provider: tag — a run stamped
    # inhouse but tagged firecrawl is the failure this check exists to catch.
    tagged = [f for f in stamped if f"fetch_provider:{f['metadata']['fetch_provider']}" in f["tags"]]
    check(
        "LangSmith: WebFetch runs carry the filterable fetch_provider tag",
        bool(stamped) and len(tagged) == len(stamped),
        f"{len(tagged)}/{len(stamped)} tagged (e.g. "
        f"{next((t for t in tagged[0]['tags'] if t.startswith('fetch_provider:')), '') if tagged else 'n/a'})",
    )

    with_attempts = [f for f in live if f["metadata"].get("fetch_attempts")]
    malformed = [
        a
        for f in with_attempts
        for a in f["metadata"]["fetch_attempts"]
        if not ATTEMPT_RE.match(str(a))
    ]
    check(
        "LangSmith: fetch_attempts are well-formed provider:outcome pairs",
        bool(with_attempts) and not malformed,
        f"{len(with_attempts)} runs carry attempts; malformed={malformed or 'none'}",
    )

    # The point of the attempt list: a fall-through records the reason, so a
    # silent fallback is legible after the fact.
    fellthrough = [
        f["metadata"]["fetch_attempts"]
        for f in with_attempts
        if len(f["metadata"]["fetch_attempts"]) > 1
    ]
    if fellthrough:
        check(
            "LangSmith: a chain fall-through records why the primary lost",
            all(not str(a[0]).endswith(":ok") for a in fellthrough),
            f"e.g. {fellthrough[0]}",
        )
        # A served fetch's chain must end on the provider that served it —
        # otherwise the attempt list and fetch_provider tell different stories.
        served_chains = [
            f["metadata"]["fetch_attempts"]
            for f in served
            if len(f["metadata"].get("fetch_attempts") or []) > 1
        ]
        check(
            "LangSmith: a served fall-through ends on the provider that served it",
            all(str(a[-1]).endswith(":ok") for a in served_chains),
            f"terminal attempts={[str(a[-1]) for a in served_chains] or 'none'}",
        )
    else:
        observe(
            "no multi-provider attempt recorded — the primary served every fetch "
            "(configure a failing primary in fetch_chain to cover fall-through)"
        )

    engines = {s["metadata"].get("search_engine") for s in searches}
    depths = {s["metadata"].get("search_depth") for s in searches}
    check(
        "LangSmith: WebSearch runs name the engine and depth that served them",
        all(e for e in engines) and all(d for d in depths),
        f"engines={engines or 'none'} depths={depths or 'none'}",
    )

    if len(threads) > 1:
        observe(f"attribution covers {len(fetches)} fetch / {len(searches)} search runs "
                f"across {len(threads)} turns")


# ---------------------------------------------------------------------------
# Phase 3 — breaker classification (in-process, live network)
# ---------------------------------------------------------------------------


def breaker_checks() -> None:
    try:
        import asyncio

        from src.tools.web.breaker import CircuitState
        from src.tools.web.fetch import get_fetch_chain
        from src.tools.web.router import FetchRouter
        from src.tools.web.types import FetchRequest
    except ImportError as e:
        # Never an observation: a skipped breaker phase reports all-green while
        # proving nothing, which is exactly the failure this pass exists to catch.
        check("breaker: the in-process phase ran", False,
              f"src not importable ({e}) — no breaker assertion was evaluated")
        return

    async def drive() -> None:
        # Deliberately the in-house provider alone, not the configured chain: a
        # primary that rate-limits US is a genuine provider fault and SHOULD
        # open its breaker, which would mask the thing under test.
        router = FetchRouter(["inhouse"])
        observe(f"breaker phase isolates 'inhouse' (configured chain: {get_fetch_chain()})")
        entries = router._chain
        if not entries:
            # Never an observation, for the same reason the import failure
            # above is not: a skipped breaker phase reports all-green while
            # proving nothing.
            check("breaker: the in-house provider resolved to a breaker", False,
                  "FetchRouter(['inhouse']) produced an empty chain — nothing to exercise")
            return
        threshold = min(e.breaker.failure_threshold for e in entries)
        rounds = threshold + 2

        target_results, labels = [], []
        for n in range(rounds):
            resp = await router.fetch(FetchRequest(urls=[UNREACHABLE.format(n=n)]))
            target_results.extend(resp.results)
            labels.extend(str(a) for a in resp.attempts)

        # Without this the breaker assertions below can pass vacuously: a proxy
        # that answers for .invalid would mean no target-side failure ever ran.
        check(
            "breaker: every unreachable URL failed, and none blamed the provider",
            len(target_results) == rounds
            and all(r.error is not None and not r.error.provider_fault for r in target_results),
            f"{len(target_results)}/{rounds} results; "
            f"e.g. {str(target_results[0].error) if target_results else 'none'}",
        )
        check(
            "breaker: the attempt label names the target-side cause, not 'provider_error'",
            bool(labels) and all(not lb.endswith(":provider_error") for lb in labels),
            f"labels={sorted(set(labels))}",
        )

        opened = [e.adapter.name for e in entries if e.breaker.state is not CircuitState.CLOSED]
        counted = {e.adapter.name: e.breaker.failure_count for e in entries}
        check(
            f"breaker: {rounds} target-side failures open no provider breaker",
            not opened,
            f"threshold={threshold} states="
            f"{ {e.adapter.name: e.breaker.state.name for e in entries} }",
        )
        check(
            "breaker: target-side failures are not counted against any provider",
            all(v == 0 for v in counted.values()),
            f"failure_count={counted}",
        )

        resp = await router.fetch(FetchRequest(urls=[HEALTHY]))
        served = resp.results[0].ok if resp.results else False
        check(
            "breaker: a healthy fetch still succeeds afterwards (chain not poisoned)",
            served,
            f"provider={resp.provider} attempts={[str(a) for a in resp.attempts]}"
            + ("" if served else f" error={resp.results[0].error if resp.results else 'no result'}"),
        )

    asyncio.run(drive())


# ---------------------------------------------------------------------------
# Phase 4 — the backend stayed quiet
# ---------------------------------------------------------------------------


def quiet_log_check(container: str, since: datetime) -> None:
    try:
        out = subprocess.run(
            # Explicit Z: a naive timestamp is read in the daemon's local zone,
            # which is not the container's.
            ["docker", "logs", container, "--since", since.strftime("%Y-%m-%dT%H:%M:%SZ")],
            capture_output=True, text=True, timeout=60,
        )
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        observe(f"docker logs unavailable ({type(e).__name__}) — quiet-log check skipped")
        return
    if out.returncode != 0:
        observe(f"docker logs {container} failed — quiet-log check skipped")
        return

    lines = [ln for ln in (out.stdout + out.stderr).splitlines() if "Circuit breaker [" in ln]
    # Layers that serve every URL: nothing a single target does may open them.
    # A primary provider rate-limiting us is a real provider fault, so its
    # breaker opening is correct behaviour and only worth noting.
    target_driven = [ln for ln in lines if "[fetch:inhouse]" in ln or "[crawler:" in ln]
    for other in [ln for ln in lines if ln not in target_driven]:
        observe(f"provider-fault breaker line (expected, not a failure): {other[:160]}")
    check(
        "server: no shared-layer breaker opened while serving real traffic",
        not target_driven,
        "no [fetch:inhouse] or [crawler:*] transitions"
        if not target_driven
        else f"{len(target_driven)} line(s): {[ln[-160:] for ln in target_driven[:2]]}",
    )


# ---------------------------------------------------------------------------


def pick_workspace(env: Env) -> str:
    with env.client() as c:
        r = c.get("/api/v1/workspaces")
        r.raise_for_status()
        items = r.json().get("workspaces") or []
    for ws in items:
        if ws.get("status") not in ("archived", "error"):
            return str(ws["workspace_id"])
    sys.exit("No usable workspace for this user — pass --workspace explicitly.")


def main() -> int:
    envf = read_env_file()
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--user", default=os.environ.get("E2E_USER_ID"), help="user id (or E2E_USER_ID)")
    ap.add_argument("--base", default=f"http://localhost:{envf.get('BACKEND_PORT', '8000')}")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--agents", default="flash,ptc", help="comma-separated: flash,ptc")
    ap.add_argument("--model", default=None)
    ap.add_argument("--turn-timeout", type=float, default=420.0)
    ap.add_argument("--project", default=os.environ.get("LANGSMITH_PROJECT") or envf.get("LANGSMITH_PROJECT"))
    # Compose names it "<project>-backend-1", and defaults the project to the
    # checkout's directory name when COMPOSE_PROJECT_NAME is unset.
    ap.add_argument("--backend-container",
                    default=f"{envf.get('COMPOSE_PROJECT_NAME') or REPO_ROOT.name}-backend-1")
    ap.add_argument("--skip-turns", action="store_true", help="breaker + log phases only")
    args = ap.parse_args()

    token = os.environ.get("INTERNAL_SERVICE_TOKEN") or envf.get("INTERNAL_SERVICE_TOKEN")
    if not token:
        sys.exit("INTERNAL_SERVICE_TOKEN not found (env or repo .env).")
    if not args.user and not args.skip_turns:
        sys.exit("--user (or E2E_USER_ID) is required.")

    # A minute of slack: container clocks and the LangSmith window both drift.
    started = datetime.now(timezone.utc) - timedelta(seconds=60)
    env = Env(base=args.base, token=token, user_id=args.user or "", turn_timeout=args.turn_timeout)
    agents = [a.strip() for a in args.agents.split(",") if a.strip()]
    threads: list[str] = []

    if not args.skip_turns:
        ws = args.workspace or pick_workspace(env)
        print(f"base={env.base} user={env.user_id} workspace={ws} agents={agents}")
        for mode in agents:
            print(f"\n== turn: {mode} agent — search + fetch ==")
            tid = run_turn(env, mode, ws, args.model)
            if tid:
                threads.append(tid)

        print(f"\n== LangSmith attribution (project={args.project}) ==")
        if args.project:
            langsmith_checks(args.project, started, threads)
        else:
            observe("no LANGSMITH_PROJECT — attribution checks skipped")

    print("\n== breaker classification (live, in-process) ==")
    breaker_checks()

    print(f"\n== server quiet check ({args.backend_container}) ==")
    quiet_log_check(args.backend_container, started)

    failed = [c for c in CHECKS if not c[1]]
    print(f"\n{'=' * 60}\n{len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed"
          + (f"; threads kept: {', '.join(threads)}" if threads else ""))
    for name, ok, _ in CHECKS:
        print(f"  {'✅' if ok else '❌'} {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
