#!/usr/bin/env python3
"""The Drive mount harness. Every suite but ``races`` runs on the real
kernel mount, inside a Linux container as root (``harness.py`` makes it):
the livefs package shipped the way the host ships it, the Drive daemon
started with ``drive-start`` against the fake server (or a real one).

  bench       search and read workloads, mount against local disk
  conc        1, 4 and 8 simultaneous searches, warm and caches dropped
  memory      the daemon's memory and the store's disk use
  writers     every way a program writes, checked against the server's copy
  refusal     saves the server refuses, filed for the tool result
  feed        changes made elsewhere reaching a reader, and how fast
  faults      saves unanswered, late or stalled; names under folding
  small_disk  the store on a disk too small for the text
  races       the daemon's operations in this process, for races the kernel
              cannot hit on cue (no root needed: runs on a laptop too)
  blocks      large files by block: a 5 GB file on a 3 GiB disk, cancelled
              read-ahead, cut fetches, versions held for readers, links
              running out (against the fake and its blob store)
  live        large files against a real server: saves in parts, reads on
              both routes, refusals, a file replaced mid-read, an append
              while a reader holds the old version (``--server``)

    python3 inside.py --suites races [--runtime DIR]

Results go to ``--out`` as JSON, with tables printed at the end.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time

import checks
import fixtures
import suite_bench
import suite_faults  # noqa: F401 - registers its cases
import suite_feed  # noqa: F401
import suite_blocks  # noqa: F401
import suite_live  # noqa: F401
import suite_races  # noqa: F401
import suite_refusal  # noqa: F401
import suite_small_disk  # noqa: F401
import suite_writers  # noqa: F401
from fixtures import Env, FakeServer, Failure, Mount, RealServer, log

ORDER = ["bench", "conc", "memory", "writers", "refusal", "feed", "faults", "races", "small_disk", "blocks", "live"]
REAL = {"writers", "feed", "live"}  # what needs no server-side knobs


def kernel(a, suites: list[str], results: checks.Results, report: dict) -> int:
    if os.geteuid() != 0 or not os.path.exists("/dev/fuse"):
        sys.exit("the kernel suites run as root with /dev/fuse (harness.py)")
    os.makedirs(a.workdir, exist_ok=True)
    corpus, exp = fixtures.ensure_corpus(a.workdir, a.files)
    report.update(files=exp["files"], total_bytes=exp["total_bytes"], text_files=exp["text_files"],
                  env={"uname": os.uname().release, "cpus": len(os.sched_getaffinity(0)),
                       "python": sys.version.split()[0],
                       "rg": fixtures.sh(["rg", "--version"]).stdout.splitlines()[0]})
    log(json.dumps(report["env"]))
    t0 = time.perf_counter()
    if a.server:
        server = RealServer(a.server, a.token, a.rest or f"{a.server}/api/v1/drive", a.rest_token)
        server.clear(suite_writers.BASE, suite_feed.REL)
        mount = Mount(server.base_url, server.token)
    else:
        server = FakeServer("/var/tmp/drive_srv", corpus)
        mount = Mount(server.base_url)
    fixtures.MOUNTS.append(mount)
    report["server_seed_s"] = round(time.perf_counter() - t0, 2)
    env = Env(server, mount, corpus, fixtures.corpus_files(corpus), report)
    report["expected"] = exp
    try:
        with contextlib.suppress(Exception):
            mount.stop()
        if set(suites) & {"bench", "conc", "memory"}:
            log("== start, empty store (fetch all text, then warm)")
            report["start_fresh"] = mount.start(fresh_store=True)
            report["after_start"] = mount.status()
        for suite in suites:
            if suite in ("writers", "refusal", "feed", "faults", "small_disk", "blocks", "live") and not mount.pid():
                report["start_writers"] = mount.start({"warm": False})
            log(f"== {suite}")
            checks.run(suite, results, env, only=a.only)
        report["final_status"] = mount.status()
        if not a.server:
            report["server"] = server.admin("GET", "stats")[1]
        return 0
    except Failure as exc:
        report["failure"] = str(exc)
        log(f"!!!! FAILED: {exc}\n{mount.log_tail()}")
        return 2
    finally:
        with contextlib.suppress(Exception):
            mount.stop()
        server.stop()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=20000)
    ap.add_argument("--workdir", default="/var/tmp/drive_bench")
    ap.add_argument("--suites", default=None)
    ap.add_argument("--only", default="", help="run only the cases whose names hold one of these (comma list)")
    ap.add_argument("--levels", default="1,4,8")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--out", default="/results/drive_mount.json")
    ap.add_argument("--settings", default="{}", help="daemon settings JSON for every start")
    ap.add_argument("--runtime", help="the livefs_runtime the races import (default: the one shipped)")
    ap.add_argument("--blob", help="the blob store's plain and TLS bases, comma-separated")
    ap.add_argument("--server", help="a real server's base URL in place of the fake")
    ap.add_argument("--token", help="a computer's mount token on --server")
    ap.add_argument("--rest", help="the Drive REST API on --server (.../api/v1/drive)")
    ap.add_argument("--rest-token", help="a bearer token for --rest, where it needs one")
    a = ap.parse_args()
    a.only = [o for o in a.only.split(",") if o]
    if a.blob:
        fixtures.trust_blob(*a.blob.split(","))
    fixtures.EXTRA_SETTINGS.update(json.loads(a.settings))
    suite_bench.LEVELS[:] = [int(x) for x in a.levels.split(",")]
    suite_bench.RUNS = a.runs
    wanted = set((a.suites or "bench,conc,writers,refusal,feed,memory").split(","))
    unknown = wanted - set(ORDER)
    if unknown:
        sys.exit(f"no such suite: {', '.join(sorted(unknown))}")
    if a.server:
        if a.suites and wanted - REAL:
            sys.exit(f"suites that need the fake server: {', '.join(sorted(wanted - REAL))}")
        wanted &= REAL
    suites = [s for s in ORDER if s in wanted]
    if a.only and not [c for c in checks.CASES if c.suite in suites and any(o in c.name for o in a.only)]:
        sys.exit(f"no case in {', '.join(suites)} matches --only {','.join(a.only)}")
    results = checks.Results()
    report: dict = {"started": time.strftime("%Y-%m-%dT%H:%M:%S")}
    status = 0
    try:
        on_kernel = [s for s in suites if s != "races"]
        if on_kernel:
            status = kernel(a, on_kernel, results, report)
        if "races" in suites and not status:
            from local import DEFAULT_RUNTIME, Runtime

            src = a.runtime or (fixtures.SRC if os.path.isdir(fixtures.SRC) else DEFAULT_RUNTIME)
            log("== races")
            checks.run("races", results, Runtime.load(src), only=a.only)
    finally:
        report.pop("expected", None)
        report["checks"] = results.checks
        with contextlib.suppress(OSError):
            os.makedirs(os.path.dirname(a.out), exist_ok=True)
            with open(a.out, "w") as f:
                json.dump(report, f, indent=1, ensure_ascii=False)
    suite_bench.print_tables(report)
    failed = results.failed()
    log(f"\nchecks: {len(results.checks) - len(failed)} passed, {len(failed)} failed")
    for c in failed:
        log(f"  FAIL {c['name']}: {c['detail']}")
    log(f"results: {a.out}")
    return status or (1 if failed else 0)


if __name__ == "__main__":
    sys.exit(main())
