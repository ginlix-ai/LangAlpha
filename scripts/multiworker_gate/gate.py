#!/usr/bin/env python3
"""Multi-worker fault-injection gate — re-runnable subset of the v4 Gate 2 matrix.

Spins two extra single-uvicorn-worker servers (A :8001, B :8002) INSIDE the
running backend container — multiple processes against one Postgres + Redis is
the multi-worker topology every cell exercises — then drives real turns across
them and asserts terminal state on the run ledger (conversation_responses).

Run it whenever a change touches turn lifecycle, streaming, outbox, or
subagent ownership:

    uv run python scripts/multiworker_gate/gate.py --user <user_id>
    uv run python scripts/multiworker_gate/gate.py --cells 1,7,13   # subset

Requires: the worktree's docker stack up, service-token auth
(INTERNAL_SERVICE_TOKEN in the container env), and a flash-capable LLM key.
Threads created by the gate are kept for inspection (--clean deletes them).
See README.md for the full 17-cell matrix and the manual-only cells.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
import uuid

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

WORKERS = {"A": 8001, "B": 8002}
# "MAIN" is the stack's own server — used only for post-teardown cleanup.
PORTS = {**WORKERS, "MAIN": 8000}
LONG_PROMPT = (
    "Count from 1 to 2000, one number per line. Output only the numbers, "
    "no commentary."
)
QUICK_PROMPT = "Reply with exactly: ok"


def sh(cmd: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


class Stack:
    """Detected containers + auth for the worktree's compose stack."""

    def __init__(self, backend: str, postgres: str, user_id: str, token: str):
        self.backend = backend
        self.postgres = postgres
        self.user_id = user_id
        self.token = token
        self.threads: list[str] = []

    @classmethod
    def detect(cls, user_id: str) -> "Stack":
        out = sh(["docker", "ps", "--format", "{{.Names}}"]).stdout.split()
        backend = None
        for name in out:
            if not name.endswith("-backend-1"):
                continue
            mounts = sh(["docker", "inspect", name, "--format",
                         "{{range .Mounts}}{{.Source}}\n{{end}}"]).stdout
            if f"{REPO_ROOT}/src" in mounts.splitlines():
                backend = name
                break
        if not backend:
            sys.exit(f"No running backend container mounts {REPO_ROOT}/src — "
                     "is this worktree's stack up?")
        postgres = backend.replace("-backend-1", "-postgres-1")
        token = sh(["docker", "exec", backend, "printenv",
                    "INTERNAL_SERVICE_TOKEN"]).stdout.strip()
        if not token:
            sys.exit("INTERNAL_SERVICE_TOKEN not set in the backend container.")
        return cls(backend, postgres, user_id, token)

    # -- in-container process control -------------------------------------

    def launch_worker(self, name: str) -> None:
        port = WORKERS[name]
        # A stale server on the port makes every later launch silently fail
        # to bind while health checks pass against the impostor — and kill
        # cells then signal dead pidfile pids. Refuse to stack.
        code, _ = self.api(name, "GET", "/health", max_time=3)
        if code == 200:
            sys.exit(f"Port {port} already serves /health — stale gate worker "
                     f"{name}; teardown failed, kill it before rerunning.")
        # exec keeps the shell's pid, so the pidfile is the server's pid.
        inner = (
            f"echo $$ > /tmp/gate_{name}.pid; "
            f"exec /app/.venv/bin/python /app/server.py --host 127.0.0.1 "
            f"--port {port} >> /tmp/gate_{name}.log 2>&1"
        )
        sh(["docker", "exec", "-d", "-e", "RECOVERY_SCAN_INTERVAL=5",
            self.backend, "sh", "-c", inner])
        deadline = time.time() + 90
        while time.time() < deadline:
            code, _ = self.api(name, "GET", "/health")
            if code == 200:
                return
            time.sleep(2)
        log = sh(["docker", "exec", self.backend, "tail", "-30",
                  f"/tmp/gate_{name}.log"]).stdout
        sys.exit(f"Worker {name} (:{port}) failed to become healthy.\n{log}")

    def worker_pid(self, name: str) -> str:
        return sh(["docker", "exec", self.backend, "cat",
                   f"/tmp/gate_{name}.pid"]).stdout.strip()

    def signal_worker(self, name: str, sig: str) -> None:
        pid = self.worker_pid(name)
        sh(["docker", "exec", self.backend, "sh", "-c", f"kill -{sig} {pid}"])

    def worker_alive(self, name: str) -> bool:
        pid = self.worker_pid(name)
        r = sh(["docker", "exec", self.backend, "sh", "-c",
                f"kill -0 {pid} 2>/dev/null && echo yes || echo no"])
        return r.stdout.strip() == "yes"

    def teardown_workers(self) -> None:
        # Kill by cmdline pattern, not only the pidfile: an aborted run can
        # overwrite the pidfile while an older server still holds the port.
        for name, port in WORKERS.items():
            script = (
                "for d in /proc/[0-9]*; do "
                "c=$(tr '\\0' ' ' < $d/cmdline 2>/dev/null); "
                f"case \"$c\" in *\"server.py --host 127.0.0.1 --port {port}\"*) "
                "kill -9 $(basename $d) 2>/dev/null;; esac; done; "
                f"rm -f /tmp/gate_{name}.pid"
            )
            sh(["docker", "exec", self.backend, "sh", "-c", script])

    # -- API + DB ----------------------------------------------------------

    def api(self, worker: str, method: str, path: str, body: dict | None = None,
            dispatch: bool = False, max_time: int = 30) -> tuple[int, str]:
        """curl inside the container against worker A/B; returns (status, body).

        Streaming responses are sampled: curl exit 28 (max-time on an open
        SSE stream) is reported as the already-received status code.
        """
        port = PORTS[worker]
        # Unique body file per call: cells fire concurrent requests, and a
        # shared file lets one response clobber another before it's read.
        tmp = f"/tmp/gate_body_{uuid.uuid4().hex}.out"
        cmd = ["docker", "exec", self.backend, "curl", "-s", "-N",
               "--max-time", str(max_time), "-o", tmp,
               "-w", "%{http_code}",
               "-X", method, f"http://127.0.0.1:{port}{path}",
               "-H", f"X-Service-Token: {self.token}",
               "-H", f"X-User-Id: {self.user_id}"]
        if dispatch:
            cmd += ["-H", "X-Dispatch: background"]
        if body is not None:
            cmd += ["-H", "Content-Type: application/json",
                    "-d", json.dumps(body)]
        r = sh(cmd, timeout=max_time + 15)
        code = int(r.stdout.strip() or 0)
        text = sh(["docker", "exec", self.backend, "sh", "-c",
                   f"cat {tmp} 2>/dev/null; rm -f {tmp}"]).stdout
        return code, text

    def pq(self, sql: str) -> str:
        r = sh(["docker", "exec", self.postgres, "psql", "-U", "postgres",
                "-d", "postgres", "-tA", "-c", sql])
        if r.returncode != 0:
            raise RuntimeError(f"psql failed: {r.stderr}")
        return r.stdout.strip()

    # -- turn helpers ------------------------------------------------------

    def dispatch(self, worker: str, prompt: str, thread_id: str | None = None,
                 request_key: str | None = None, llm_model: str | None = None,
                 ) -> tuple[int, dict]:
        body: dict = {"agent_mode": "flash",
                      "messages": [{"role": "user", "content": prompt}]}
        if request_key:
            body["request_key"] = request_key
        if llm_model:
            body["llm_model"] = llm_model
        path = (f"/api/v1/threads/{thread_id}/messages" if thread_id
                else "/api/v1/threads/messages")
        code, text = self.api(worker, "POST", path, body, dispatch=True)
        try:
            resp = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return code, {"raw": text[:400]}
        new_tid = resp.get("thread_id")
        if new_tid and new_tid not in self.threads:
            self.threads.append(new_tid)
        return code, resp

    def run_row(self, run_id: str) -> dict:
        row = self.pq(
            "SELECT status, attempt_no, "
            "COALESCE(metadata->>'recovery',''), "
            "COALESCE(metadata->>'cancelled_by_user','') "
            f"FROM conversation_responses WHERE conversation_response_id='{run_id}'")
        if not row:
            return {}
        status, attempt, recovery, by_user = row.split("|")
        return {"status": status, "attempt_no": int(attempt),
                "recovery": recovery, "cancelled_by_user": by_user}

    def wait_terminal(self, run_id: str, timeout: int = 180) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            row = self.run_row(run_id)
            if row and row["status"] != "in_progress":
                return row
            time.sleep(2)
        return self.run_row(run_id)

    def settled_thread(self, worker: str = "A") -> str:
        """A fresh thread whose turn-1 run has completed (an idle slot)."""
        code, resp = self.dispatch(worker, QUICK_PROMPT)
        assert code in (200, 202), f"seed dispatch failed: {code} {resp}"
        tid, rid = resp["thread_id"], resp["run_id"]
        row = self.wait_terminal(rid)
        assert row.get("status") == "completed", f"seed turn didn't complete: {row}"
        return tid


# --------------------------------------------------------------------------
# Cells. Each returns (passed: bool, evidence: str).
# --------------------------------------------------------------------------

def cell_1_start_race(st: Stack) -> tuple[bool, str]:
    """Two-process START: exactly one wins the in_progress slot."""
    tid = st.settled_thread()
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        futs = [pool.submit(st.dispatch, w, LONG_PROMPT, tid) for w in ("A", "B")]
        results = [f.result() for f in futs]
    codes = sorted(c for c, _ in results)
    open_rows = st.pq("SELECT count(*) FROM conversation_responses WHERE "
                      f"conversation_thread_id='{tid}' AND status='in_progress'")
    winner = next((r for c, r in results if c in (200, 202)), None)
    if winner:
        st.api("A", "POST", f"/api/v1/threads/{tid}/cancel")
        st.wait_terminal(winner["run_id"], timeout=60)
    # The loser is bounded: 409 (admission conflict) or 503 (advisory-lock
    # contention inside acquire_root — writer_capacity, by design). The real
    # invariant is one winner and at most one in_progress row.
    winners = [c for c in codes if c in (200, 202)]
    losers = [c for c in codes if c in (409, 503)]
    ok = len(winners) == 1 and len(losers) == 1 and int(open_rows) <= 1
    return ok, f"codes={codes} concurrent_open_rows={open_rows} thread={tid}"


def cell_2_sigkill_scanner(st: Stack) -> tuple[bool, str]:
    """SIGKILL the owning worker mid-run → scanner finalizes it."""
    code, resp = st.dispatch("A", LONG_PROMPT)
    if code not in (200, 202):
        return False, f"dispatch failed: {code} {resp}"
    tid, rid = resp["thread_id"], resp["run_id"]
    time.sleep(2)
    # Freeze the owner BEFORE the liveness check: a fast model can finish the
    # turn between "row is in_progress" and the kill landing. SIGSTOP makes
    # check-then-SIGKILL atomic regardless of generation speed.
    st.signal_worker("A", "STOP")
    if st.run_row(rid).get("status") != "in_progress":
        st.signal_worker("A", "CONT")
        return False, f"run settled before the freeze (thread {tid})"
    st.signal_worker("A", "9")
    row = st.wait_terminal(rid, timeout=60)  # B scans every 5s
    ok = row.get("status") == "error" and row.get("recovery") == "scanner"
    return ok, f"post-kill row={row} thread={tid} (worker A killed; relaunch follows)"


def cell_7_cancel_races(st: Stack) -> tuple[bool, str]:
    """Cross-worker cancel lands; second cancel is idempotent no_active_run."""
    code, resp = st.dispatch("A", LONG_PROMPT)
    if code not in (200, 202):
        return False, f"dispatch failed: {code} {resp}"
    tid, rid = resp["thread_id"], resp["run_id"]
    time.sleep(2)
    c1, b1 = st.api("B", "POST", f"/api/v1/threads/{tid}/cancel")
    row = st.wait_terminal(rid, timeout=60)
    c2, b2 = st.api("B", "POST", f"/api/v1/threads/{tid}/cancel")
    idempotent = c2 == 200 and "no_active_run" in b2
    ok = (c1 == 200 and row.get("status") == "cancelled"
          and row.get("cancelled_by_user") == "true" and idempotent)
    return ok, (f"cancel1={c1} row={row} cancel2={c2} "
                f"idempotent={'yes' if idempotent else b2[:120]} thread={tid}")


def _guarded_mutation_409(st: Stack, method: str, path_fmt: str,
                          label: str) -> tuple[bool, str]:
    code, resp = st.dispatch("A", LONG_PROMPT)
    if code not in (200, 202):
        return False, f"dispatch failed: {code} {resp}"
    tid, rid = resp["thread_id"], resp["run_id"]
    time.sleep(2)
    mcode, mbody = st.api("B", method, path_fmt.format(tid=tid))
    st.api("A", "POST", f"/api/v1/threads/{tid}/cancel")
    st.wait_terminal(rid, timeout=60)
    ok = mcode == 409
    return ok, f"{label} during live run → {mcode} {mbody[:120]} thread={tid}"


def cell_9_compact_vs_live(st: Stack) -> tuple[bool, str]:
    """Compaction against a live cross-worker run → 409."""
    return _guarded_mutation_409(
        st, "POST", "/api/v1/threads/{tid}/summarize", "summarize")


def cell_10_delete_vs_live(st: Stack) -> tuple[bool, str]:
    """Thread delete against a live cross-worker run → guarded refusal."""
    return _guarded_mutation_409(st, "DELETE", "/api/v1/threads/{tid}", "delete")


def cell_13_request_key_replay(st: Stack) -> tuple[bool, str]:
    """Same request_key retransmitted to the OTHER worker → same run, no dup."""
    tid = st.settled_thread()
    key = str(uuid.uuid4())
    c1, r1 = st.dispatch("A", QUICK_PROMPT, tid, request_key=key)
    if c1 not in (200, 202):
        return False, f"first dispatch failed: {c1} {r1}"
    c2, r2 = st.api("B", "POST", f"/api/v1/threads/{tid}/messages",
                    {"agent_mode": "flash", "request_key": key,
                     "messages": [{"role": "user", "content": QUICK_PROMPT}]},
                    dispatch=True)
    rows = st.pq("SELECT count(*) FROM conversation_responses WHERE "
                 f"request_key='{key}'")
    st.wait_terminal(r1["run_id"], timeout=90)
    ok = c2 == 409 and r1["run_id"] in r2 and int(rows) == 1
    return ok, (f"replay={c2} identity_echoed={r1['run_id'] in r2} "
                f"rows_for_key={rows} thread={tid}")


def cell_14_retry_race(st: Stack) -> tuple[bool, str]:
    """Concurrent /retry of a failed run → exactly one attempt-2 row."""
    # Deterministic failure: a manifest model whose provider key is absent
    # in this stack fails inside the workflow, after START — a retryable
    # `error` terminal. Override with GATE_FAIL_MODEL if your env differs.
    # Turn 1 must COMPLETE first: retry replays from the last checkpoint, and
    # a thread whose only run died at agent init has none (404 no_checkpoints).
    fail_model = os.environ.get("GATE_FAIL_MODEL", "claude-sonnet-5")
    tid = st.settled_thread()
    code, resp = st.dispatch("A", QUICK_PROMPT, tid, llm_model=fail_model)
    if code not in (200, 202):
        return False, f"failing dispatch not accepted: {code} {resp}"
    rid = resp["run_id"]
    row = st.wait_terminal(rid, timeout=90)
    if row.get("status") != "error":
        return False, (f"seed run ended {row.get('status')!r}, need 'error' — "
                       f"set GATE_FAIL_MODEL to a keyless model (thread {tid})")
    ws = st.pq("SELECT workspace_id FROM conversation_threads WHERE "
               f"conversation_thread_id='{tid}'")
    retry_body = {"workspace_id": ws}
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        futs = [pool.submit(st.api, w, "POST", f"/api/v1/threads/{tid}/retry",
                            retry_body, False, 8) for w in ("A", "B")]
        codes = sorted(f.result()[0] for f in futs)
    time.sleep(3)
    attempt2 = st.pq("SELECT count(*) FROM conversation_responses WHERE "
                     f"conversation_thread_id='{tid}' AND attempt_no=2")
    st.api("A", "POST", f"/api/v1/threads/{tid}/cancel")
    return int(attempt2) == 1, f"retry_codes={codes} attempt2_rows={attempt2} thread={tid}"


def cell_17_graceful_sigterm(st: Stack) -> tuple[bool, str]:
    """SIGTERM with an open run → owner finalizes cancelled before exit."""
    code, resp = st.dispatch("B", LONG_PROMPT)
    if code not in (200, 202):
        return False, f"dispatch failed: {code} {resp}"
    tid, rid = resp["thread_id"], resp["run_id"]
    time.sleep(3)
    st.signal_worker("B", "TERM")
    row = st.wait_terminal(rid, timeout=70)
    deadline = time.time() + 30
    exited = False
    while time.time() < deadline:
        if not st.worker_alive("B"):
            exited = True
            break
        time.sleep(2)
    ok = (row.get("status") == "cancelled"
          and row.get("cancelled_by_user") == "false"
          and row.get("recovery") == ""  # owner finalized, not the scanner
          and exited)
    return ok, f"row={row} worker_exited={exited} thread={tid}"


CELLS = {
    "1": ("Two-process START slot exclusivity", cell_1_start_race),
    "7": ("Cancel: cross-worker + idempotent", cell_7_cancel_races),
    "9": ("Compaction vs live run → 409", cell_9_compact_vs_live),
    "10": ("Delete vs live run → 409", cell_10_delete_vs_live),
    "13": ("Request-key replay → one run", cell_13_request_key_replay),
    "14": ("Retry race → one attempt-2", cell_14_retry_race),
    "2": ("SIGKILL mid-run → scanner finalize", cell_2_sigkill_scanner),
    "17": ("Graceful SIGTERM with open run", cell_17_graceful_sigterm),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--user", default=os.environ.get("E2E_USER_ID"),
                    help="user id for service-token auth (or E2E_USER_ID env)")
    ap.add_argument("--cells", default=",".join(CELLS),
                    help="comma-separated cell numbers (declaration order kept)")
    ap.add_argument("--clean", action="store_true",
                    help="delete gate threads afterwards (default: keep)")
    args = ap.parse_args()
    if not args.user:
        ap.error("--user (or E2E_USER_ID) is required")

    st = Stack.detect(args.user)
    print(f"backend={st.backend} postgres={st.postgres} user={st.user_id}")
    wanted = [c.strip() for c in args.cells.split(",") if c.strip()]
    unknown = [c for c in wanted if c not in CELLS]
    if unknown:
        ap.error(f"unknown cells: {unknown} (automated: {list(CELLS)})")

    st.teardown_workers()
    for w in WORKERS:
        st.launch_worker(w)
        print(f"worker {w} healthy on :{WORKERS[w]}")

    results: list[tuple[str, str, bool, str]] = []
    try:
        for num in [c for c in CELLS if c in wanted]:
            title, fn = CELLS[num]
            print(f"\n── cell {num}: {title}")
            try:
                passed, evidence = fn(st)
            except Exception as e:  # a broken cell must not kill the suite
                passed, evidence = False, f"EXCEPTION {type(e).__name__}: {e}"
            print(f"   {'PASS' if passed else 'FAIL'} — {evidence}")
            results.append((num, title, passed, evidence))
            if num == "2":  # cell 2 kills worker A; later cells need it back
                st.launch_worker("A")
                print("   worker A relaunched")
    finally:
        st.teardown_workers()

    print("\n" + "=" * 62)
    failed = [r for r in results if not r[2]]
    for num, title, passed, _ in results:
        print(f"  cell {num:>2}  {'PASS' if passed else 'FAIL'}  {title}")
    print(f"{len(results) - len(failed)}/{len(results)} passed"
          + ("" if not failed else " — FAILURES ABOVE"))
    if args.clean:
        for tid in st.threads:
            code, _ = st.api("MAIN", "DELETE", f"/api/v1/threads/{tid}")
            print(f"  cleaned thread {tid} → {code}")
    elif st.threads:
        print(f"gate threads kept for inspection: {', '.join(st.threads)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
