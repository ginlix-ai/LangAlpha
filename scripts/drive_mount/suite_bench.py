"""Search and read workloads on the mount against local disk, with the
daemon's own operation counts: cold (no warmer) and warmed (``bench``); 1, 4
and 8 simultaneous searches, warm and with the kernel's caches dropped
before each pass (``conc``); the daemon's memory and the store's disk use
(``memory``). They measure rather than pin, so they add to the results
file, with tables printed at the end."""

from __future__ import annotations

import os
import random
import subprocess
import tempfile
import time

from checks import Results, case
from fixtures import PRIVATE, TOP, Env, Failure, Mount, diff, drop_caches, du, log

WORKLOADS = ["rg_ascii", "rg_common_count", "rg_zh2", "grep_ascii", "find_count",
             "ls_R", "py_read500", "rg_ascii_md"]
RUNS = 3
LEVELS = [1, 4, 8]


def make_workloads(exp: dict, sample: list, files_expected: int) -> dict:
    n = exp["needles"]
    a, w, z2 = n["ascii"]["token"], n["common"]["token"], n["zh2"]["token"]

    def lines(cmd, expect, shell=False):
        def run(cwd):
            r = subprocess.run(cmd, cwd=cwd, shell=shell, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
            if r.returncode not in (0, 1):
                raise Failure(f"{cmd} exited {r.returncode}: {r.stderr[-500:]}")
            out = r.stdout.strip()
            got = int(out) if shell else (len(out.splitlines()) if out else 0)
            return got, expect
        return run

    def devnull(cmd, shell=False):
        def run(cwd):
            r = subprocess.run(cmd, cwd=cwd, shell=shell, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE, text=True)
            if r.returncode not in (0, 1):
                raise Failure(f"{cmd} exited {r.returncode}: {r.stderr[-500:]}")
            return None, None
        return run

    def read500(cwd):
        total = 0
        for rel, _ in sample:
            with open(os.path.join(cwd, rel), "rb") as f:
                while True:
                    b = f.read(1 << 20)
                    if not b:
                        break
                    total += len(b)
        return total, sum(s for _, s in sample)

    return {
        "rg_ascii": lines(["rg", "-l", a], n["ascii"]["files"]),
        "rg_common_count": devnull(["rg", "-c", w]),
        "rg_zh2": lines(["rg", "-l", z2], n["zh2"]["files"]),
        "grep_ascii": lines(["grep", "-rl", a, "."], n["ascii"]["files"]),
        "find_count": lines("find . -type f | wc -l", files_expected, shell=True),
        "ls_R": devnull("ls -R > /dev/null", shell=True),
        "py_read500": read500,
        "rg_ascii_md": lines(["rg", "-l", a, "-g", "*.md"], n["ascii"]["md_files"]),
    }


def run_workloads(target: str, workloads: dict, names: list, runs: int, mount: Mount | None, tag: str) -> dict:
    out = {}
    for wl in names:
        times, ops = [], []
        for i in range(runs):
            before = mount.ops() if mount else None
            t0 = time.perf_counter()
            got, want = workloads[wl](target)
            dt = time.perf_counter() - t0
            if mount:
                ops.append(diff(mount.ops(), before))
            times.append(round(dt, 4))
            if want is not None and got != want:
                raise Failure(f"{tag} {wl} pass {i + 1}: got {got}, expected {want}")
            log(f"  {tag:6} {wl:16} pass {i + 1} {dt:8.3f}s" + ("" if want is None else f"  ({got} ok)"))
        out[wl] = {"runs": times, "first": times[0], "warm": min(times[1:]) if runs > 1 else None}
        if ops:
            out[wl]["ops"] = ops
    return out


def searches(exp: dict, n: int) -> list:
    need = exp["needles"]
    a, w, z2 = need["ascii"]["token"], need["common"]["token"], need["zh2"]["token"]
    base = [
        (["rg", "-l", a], need["ascii"]["files"]),
        (["rg", "-c", w], None),
        (["rg", "-l", z2], need["zh2"]["files"]),
        (["rg", "-l", a, "-g", "*.md"], need["ascii"]["md_files"]),
    ]
    return [base[i % len(base)] for i in range(n)]


def one_pass(target: str, jobs: list) -> dict:
    outs = [tempfile.TemporaryFile(mode="w+") for _ in jobs]
    t0 = time.perf_counter()
    procs = [subprocess.Popen(cmd, cwd=target, stdout=o, stderr=subprocess.PIPE, text=True)
             for (cmd, _), o in zip(jobs, outs, strict=True)]
    ends = {}
    while len(ends) < len(procs):
        for i, p in enumerate(procs):
            if i not in ends and p.poll() is not None:
                ends[i] = time.perf_counter() - t0
        time.sleep(0.002)
    for i, (p, (cmd, expect)) in enumerate(zip(procs, jobs, strict=True)):
        err = p.stderr.read()
        outs[i].seek(0)
        out = outs[i].read()
        outs[i].close()
        if p.returncode not in (0, 1):
            raise Failure(f"{cmd} exited {p.returncode}: {err[-400:]}")
        if expect is not None:
            got = len(out.strip().splitlines()) if out.strip() else 0
            if got != expect:
                raise Failure(f"{cmd} returned {got} files, expected {expect}")
    return {"wall": round(max(ends.values()), 3), "fastest": round(min(ends.values()), 3)}


def run_conc(target: str, exp: dict, levels: list, mount: Mount | None, cold: bool = False) -> dict:
    res = {}
    for n in levels:
        before = mount.ops() if mount else None
        passes = []
        for _ in range(3):
            if cold:
                # Names, attributes and pages all go: every lookup and read
                # reaches the daemon again, as after memory pressure.
                drop_caches()
            passes.append(one_pass(target, searches(exp, n)))
        res[n] = {"first": passes[0]["wall"], "warm": min(p["wall"] for p in passes[1:]),
                  "passes": passes}
        if mount:
            res[n]["ops"] = diff(mount.ops(), before)
        log(f"  x{n}: first {res[n]['first']:.2f}s warm {res[n]['warm']:.2f}s")
    return res


def table(title: str, rows: list, cols: list) -> str:
    out = [title, "%-16s" % "workload" + "".join("%20s" % c for c in cols)]
    out += ["%-16s" % r[0] + "".join("%20s" % v for v in r[1:]) for r in rows]
    return "\n".join(out)


def _workloads(env: Env) -> tuple[dict, dict]:
    sample = random.Random(1234).sample(env.files, min(500, len(env.files)))
    exp = env.report["expected"]
    # The mount carries the generated .ignore beside the corpus.
    return make_workloads(exp, sample, exp["files"] + 1), make_workloads(exp, sample, exp["files"])


@case("bench")
def search_and_read_workloads(r: Results, env: Env) -> None:
    mount, rep = env.mount, env.report
    on_mount, local = _workloads(env)
    log("== L: local disk")
    rep["L"] = run_workloads(env.corpus, local, WORKLOADS, RUNS, None, "L")
    log("== W: Drive mount, warmed by the daemon after start")
    rep["W"] = run_workloads(TOP, on_mount, WORKLOADS, RUNS, mount, "W")
    rep["after_bench"] = mount.status()
    mount.stop()
    log("== C: Drive mount restarted (store full), no warmer: a cold kernel cache")
    rep["restart_nowarm"] = mount.start({"warm": False})
    log(str(rep["restart_nowarm"]))
    rep["C"] = run_workloads(TOP, on_mount, WORKLOADS, RUNS, mount, "C")
    mount.stop()
    rep["restart_warm"] = mount.start()
    log(str(rep["restart_warm"]))


@case("conc")
def simultaneous_searches(r: Results, env: Env) -> None:
    exp, rep = env.report["expected"], env.report
    log("== conc: local disk")
    rep["conc_L"] = run_conc(env.corpus, exp, LEVELS, None)
    log("== conc: Drive mount (warmed)")
    rep["conc_W"] = run_conc(TOP, exp, LEVELS, env.mount)
    log("== conc: local disk, caches dropped before each pass")
    rep["conc_Lcold"] = run_conc(env.corpus, exp, LEVELS, None, cold=True)
    log("== conc: Drive mount, caches dropped before each pass")
    rep["conc_Wcold"] = run_conc(TOP, exp, LEVELS, env.mount, cold=True)


@case("memory")
def daemon_memory(r: Results, env: Env) -> None:
    st = env.mount.status() or {}
    env.report["memory"] = {"daemon": st.get("memory"), "store_bytes": du(os.path.join(PRIVATE, "store")),
                            "store_files": (st.get("store") or {}).get("files"),
                            "entries": st.get("spaces"), "corpus_bytes": env.report["expected"]["total_bytes"]}
    log(str(env.report["memory"]))


def print_tables(rep: dict) -> None:
    if "L" in rep and "W" in rep:
        rows = []
        for wl in WORKLOADS:
            row = [wl]
            for key in ("L", "C", "W"):
                e = rep.get(key, {}).get(wl)
                row.append("-" if not e else f"{e['first']:.3f}/{e['warm']:.3f}")
            ops = rep["W"][wl].get("ops", [{}])[-1]
            row.append(",".join(f"{k}={v}" for k, v in ops.items()) or "0")
            rows.append(row)
        log("\n" + table("seconds first/warm: L local disk, C mount cold (no warmer), W mount warmed",
                         rows, ["L", "C", "W", "daemon ops, warm pass"]))
    if "conc_L" in rep:
        log("\nN simultaneous searches, seconds until the last ends, first/warm")
        for n in rep["conc_L"]:
            lw, mw = rep["conc_L"][n], rep.get("conc_W", {}).get(n, {})
            log(f"  x{n}: local {lw['first']:.2f}/{lw['warm']:.2f}   mount {mw.get('first', 0):.2f}/{mw.get('warm', 0):.2f}")
        if "conc_Wcold" in rep:
            log("caches dropped before each pass, seconds until the last ends, best of the later passes")
            for n in rep["conc_Lcold"]:
                lc, mc = rep["conc_Lcold"][n], rep["conc_Wcold"].get(n, {})
                log(f"  x{n}: local {lc['warm']:.2f}   mount {mc.get('warm', 0):.2f}")
