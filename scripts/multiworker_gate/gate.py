#!/usr/bin/env python3
"""Multi-worker fault-injection gate — re-runnable subset of the v4 Gate 2 matrix.

Spins two extra single-uvicorn-worker servers (A :8001, B :8002) INSIDE the
running backend container — multiple processes against one Postgres + Redis is
the multi-worker topology every cell exercises — then drives real turns across
them and asserts terminal state on the run ledger (conversation_responses).

Run it whenever a change touches turn lifecycle, streaming, outbox, subagent
ownership, or the workspace<->sandbox binding:

    uv run python scripts/multiworker_gate/gate.py --user <user_id>
    uv run python scripts/multiworker_gate/gate.py --cells 1,7,13   # subset

Requires: the worktree's docker stack up, service-token auth
(INTERNAL_SERVICE_TOKEN in the container env), and an LLM key for the user's
default model.
Cell 18 additionally needs a working sandbox provider — it creates, recreates
and deletes a real sandbox; cells 21-22 run analyst turns in a running
workspace of the user (GATE_WORKSPACE overrides the pick). Threads created by the gate are kept for inspection
(--clean deletes them); cell 18 always deletes its own workspace.
See README.md for the full matrix and the manual-only cells.
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
# A turn short enough that a steer can be timed into its last model call.
SHORT_PROMPT = (
    "Count from 1 to 150, one number per line. Output only the numbers, "
    "no commentary."
)
# One long generation with no tool call: a steer accepted mid-generation has
# no later model call in that run to read it, so it is missed by design.
NO_TOOLS_LONG_PROMPT = (
    "Without using any tools, write the numbers from 1 to 1500, one per line. "
    "Output only the numbers, no commentary."
)
TASK_PROMPT = (
    "Launch exactly one background subagent with the Task tool "
    "(subagent_type general-purpose). Its prompt: 'Without using any tools, "
    "write the numbers from 1 to 400, one per line, then the word DONE.' "
    "Do nothing else yourself; when it finishes, reply with its last line."
)
# Steer verdicts poll this long for the outbox drainer, the recovery scanner
# and any turn a missed steer becomes to settle every row.
STEER_SETTLE_S = 150

# Reads a thread's checkpointed messages (main or a task namespace) inside
# the backend container. ``messages`` is a DeltaChannel, so a raw
# ``aget_tuple`` can't materialize it; the history reader's compiled graph can.
CKPT_PROBE = r'''
import asyncio, json, logging, os, sys
sys.path.insert(0, "/app"); sys.path.insert(0, "/app/src")
logging.disable(logging.CRITICAL)
from src.server.utils.checkpointer import get_checkpointer
from src.server.services.history.reader import CheckpointHistoryReader

async def main(tid, ns):
    cp = get_checkpointer(
        "postgres", db_host=os.environ.get("DB_HOST"),
        db_port=os.environ.get("DB_PORT"), db_name=os.environ.get("DB_NAME"),
        db_user=os.environ.get("DB_USER"),
        db_password=os.environ.get("DB_PASSWORD"))
    await cp.conn.open()
    reader = CheckpointHistoryReader(cp)
    st = await reader.graph.aget_state(
        {"configurable": {"thread_id": tid, "checkpoint_ns": ns}})
    out = []
    for m in (st.values or {}).get("messages", []):
        c = m.content if isinstance(m.content, str) else json.dumps(m.content)
        calls = json.dumps(getattr(m, "tool_calls", None) or [])
        out.append({"type": m.type, "content": c, "tool_calls": calls})
    print("CKPT_JSON " + json.dumps(out))

asyncio.run(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else ""))
'''


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
        self._home: str | None = None

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
        # GATE_WORKER_ENV="HOST_MODE=oss OTHER=1" reaches the workers only,
        # so a cell that needs an entitlement the platform stack lacks can
        # run against the same stack without restarting it.
        extra = [a for kv in os.environ.get("GATE_WORKER_ENV", "").split()
                 for a in ("-e", kv)]
        sh(["docker", "exec", "-d", "-e", "RECOVERY_SCAN_INTERVAL=5", *extra,
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
        # Skip $$ — this shell's own cmdline contains the pattern, and the glob
        # sorts /proc lexicographically, so a 4-digit teardown pid is visited
        # before a 3-digit server: without the guard the loop SIGKILLs itself
        # first and silently leaves the worker alive for the next run.
        for name, port in WORKERS.items():
            script = (
                "self=$$; for d in /proc/[0-9]*; do "
                "p=$(basename $d); [ \"$p\" = \"$self\" ] && continue; "
                "c=$(tr '\\0' ' ' < $d/cmdline 2>/dev/null); "
                f"case \"$c\" in *\"server.py --host 127.0.0.1 --port {port}\"*) "
                "kill -9 $p 2>/dev/null;; esac; done; "
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

    def home(self, worker: str) -> str:
        """Home's id. Gate turns run there, as the Chief of Staff."""
        if self._home is None:
            code, text = self.api(worker, "POST", "/api/v1/workspaces/home")
            if code != 200:
                sys.exit(f"POST /api/v1/workspaces/home failed: {code} {text[:200]}")
            self._home = json.loads(text)["workspace_id"]
        return self._home

    def dispatch(self, worker: str, prompt: str, thread_id: str | None = None,
                 request_key: str | None = None, llm_model: str | None = None,
                 workspace_id: str | None = None) -> tuple[int, dict]:
        # A workspace makes it an analyst turn; without one it runs in Home,
        # as the Chief of Staff.
        body: dict = {"workspace_id": workspace_id or self.home(worker),
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

    # -- steer helpers (cells 19-23) -----------------------------------------

    def steer(self, worker: str, thread_id: str, content: str, *,
              workspace_id: str | None = None) -> tuple[bool, str, str]:
        """A user steer into the thread's running turn (``steer_only``: it
        never starts a turn itself). Returns (accepted, request_key, detail)."""
        key = str(uuid.uuid4())
        body: dict = {"workspace_id": workspace_id or self.home(worker),
                      "steer_only": True, "request_key": key,
                      "messages": [{"role": "user", "content": content}]}
        code, text = self.api(worker, "POST",
                              f"/api/v1/threads/{thread_id}/messages", body,
                              max_time=20)
        return ("event: steering_accepted" in text, key,
                f"{code} {' '.join(text.split())}")

    def steer_rows(self, idem_keys: list[str]) -> dict[str, dict]:
        """The steer jobs under these idempotency keys, by key."""
        if not idem_keys:
            return {}
        keys = ",".join(f"'{k}'" for k in idem_keys)
        out = self.pq(
            "SELECT idempotency_key, status, COALESCE(payload->>'outcome',''), "
            "COALESCE(payload->>'steered_run_id',''), "
            "COALESCE(payload->>'dispatched_run_id',''), "
            "COALESCE(payload->>'returned_run_id',''), run_id, attempts, "
            "COALESCE(payload->>'target',''), ordering_key "
            f"FROM hook_outbox WHERE hook_type='steer' AND idempotency_key IN ({keys})")
        rows = {}
        for line in out.splitlines():
            (key, status, outcome, steered, dispatched, returned, run_id,
             attempts, target, okey) = line.split("|")
            rows[key] = {"status": status, "outcome": outcome,
                         "steered_run_id": steered, "dispatched_run_id": dispatched,
                         "returned_run_id": returned, "run_id": run_id,
                         "attempts": int(attempts), "target": target,
                         "ordering_key": okey}
        return rows

    def wait_steers_settled(self, idem_keys: list[str],
                            timeout: int = STEER_SETTLE_S) -> dict[str, dict]:
        """Poll until every row is closed (done/dead) or the timeout lapses."""
        deadline = time.time() + timeout
        rows = self.steer_rows(idem_keys)
        while time.time() < deadline:
            if all(rows.get(k, {}).get("status") in ("done", "dead")
                   for k in idem_keys):
                return rows
            time.sleep(3)
            rows = self.steer_rows(idem_keys)
        return rows

    def wait_thread_quiet(self, thread_id: str, timeout: int = STEER_SETTLE_S,
                          ) -> bool:
        """Until the thread has no in_progress run and no live steer row."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            busy = self.pq(
                "SELECT (SELECT count(*) FROM conversation_responses WHERE "
                f"conversation_thread_id='{thread_id}' AND status='in_progress')"
                " + (SELECT count(*) FROM hook_outbox WHERE hook_type='steer' "
                f"AND conversation_thread_id='{thread_id}' "
                "AND status IN ('pending','claimed'))")
            if busy == "0":
                return True
            time.sleep(3)
        return False

    def thread_runs(self, thread_id: str) -> list[dict]:
        out = self.pq(
            "SELECT conversation_response_id, status, turn_index, "
            "COALESCE(metadata->>'msg_type','') FROM conversation_responses "
            f"WHERE conversation_thread_id='{thread_id}' ORDER BY run_seq")
        runs = []
        for line in out.splitlines():
            rid, status, turn, msg_type = line.split("|")
            runs.append({"run_id": rid, "status": status, "turn": int(turn),
                         "msg_type": msg_type})
        return runs

    def ckpt_messages(self, thread_id: str, ns: str = "") -> list[dict]:
        """The thread's checkpointed messages (``ns`` = ``task:{id}`` for a
        task's namespace), oldest first."""
        sh_in = subprocess.run(
            ["docker", "exec", "-i", self.backend, "sh", "-c",
             "cat > /tmp/gate_ckpt_probe.py"],
            input=CKPT_PROBE, capture_output=True, text=True, timeout=30)
        if sh_in.returncode != 0:
            raise RuntimeError(f"probe install failed: {sh_in.stderr}")
        r = sh(["docker", "exec", "-w", "/app", self.backend,
                "/app/.venv/bin/python", "/tmp/gate_ckpt_probe.py",
                thread_id, ns], timeout=90)
        for line in r.stdout.splitlines():
            if line.startswith("CKPT_JSON "):
                return json.loads(line[len("CKPT_JSON "):])
        raise RuntimeError(f"checkpoint probe failed: {r.stderr[-400:]}")

    @staticmethod
    def token_hits(messages: list[dict], token: str,
                   types: tuple[str, ...] = ("human",)) -> list[int]:
        """Indexes of the messages of ``types`` whose content carries ``token``."""
        return [i for i, m in enumerate(messages)
                if m["type"] in types and token in m["content"]]

    def run_until_terminal(self, run_id: str, timeout: int = 180,
                           step: float = 0.25) -> tuple[dict, float]:
        """Tight-poll a run to its terminal row; returns (row, seconds)."""
        t0 = time.time()
        deadline = t0 + timeout
        row = self.run_row(run_id)
        while time.time() < deadline and row.get("status") == "in_progress":
            time.sleep(step)
            row = self.run_row(run_id)
        return row, time.time() - t0

    def steer_workspace(self) -> str | None:
        """A running analyst workspace of the user (GATE_WORKSPACE wins)."""
        if os.environ.get("GATE_WORKSPACE"):
            return os.environ["GATE_WORKSPACE"]
        # Home sits at a fixed uuid5 of the user, as
        # src/server/database/home_workspace.py derives it.
        home = uuid.uuid5(uuid.UUID("f1a50000-0000-5000-e000-f1a500000000"),
                          self.user_id)
        ws = self.pq(
            "SELECT workspace_id FROM workspaces WHERE "
            f"user_id='{self.user_id}' AND status='running' "
            f"AND workspace_id <> '{home}' "
            "ORDER BY last_activity_at DESC NULLS LAST LIMIT 1")
        return ws or None


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
                    {"workspace_id": st.home("B"), "request_key": key,
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
    fail_model = os.environ.get("GATE_FAIL_MODEL", "claude-sonnet-5-5")
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


def cell_18_sandbox_replacement(st: Stack) -> tuple[bool, str]:
    """Sandbox replaced behind worker B → B re-attaches, never 404s a live file.

    The workspace/sandbox tier's counterpart to the run-ledger cells.
    ``WorkspaceManager._sessions`` is process memory, so B keeps a handle to
    whatever sandbox it last attached to; when A's ``/spec`` recreates the
    sandbox, B must notice from ``workspaces.sandbox_id`` and re-attach. The
    symptom when it doesn't is a 404 "File not found" for a file that exists.

    B is probed CONCURRENTLY with the replacement, not after it: once ``/spec``
    returns the row is already ``running`` on the new id, so a post-hoc read
    proves nothing about the window where DB and cache both name a sandbox that
    is being destroyed. 503 is an acceptable transient verdict inside the
    window but not a terminal one — the cell requires convergence to 200 with
    the exact sentinel bytes.
    """
    sentinel = f"gate-18-{uuid.uuid4().hex}"
    path = "gate_18.txt"
    code, text = st.api("A", "POST", "/api/v1/workspaces",
                        {"name": "gate-18 sandbox replacement"}, max_time=240)
    if code != 201:
        return False, f"workspace create failed: {code} {text[:200]}"
    ws = json.loads(text)["workspace_id"]

    # Everything past the create goes inside the try: a real sandbox exists from
    # here on, and st.pq raises on a psql failure, so a setup line left outside
    # would skip the finally and leak the sandbox to bill on.
    try:
        tier_before = st.pq("SELECT COALESCE(resource_tier,'standard') FROM "
                            f"workspaces WHERE workspace_id='{ws}'")
        target = "performance" if tier_before == "standard" else "standard"
        read_path = (
            f"/api/v1/workspaces/{ws}/files/read?path={path}&unlimited=true"
        )

        # 0. Bring the machine up. Create returns as soon as the row is
        # written: the workspace is bound to the caller's computer, and the
        # machine comes up on the first turn or an explicit start. Without
        # this the write below is refused (409, workspace is stopped) and the
        # cell never reaches the replacement it exists to test.
        acode, atext = st.api("A", "POST", f"/api/v1/workspaces/{ws}/start",
                              max_time=300)
        if acode != 200:
            return False, f"workspace start failed: {acode} {atext[:200]} ws={ws}"

        # 1. Write through A — A must own the session /spec will replace.
        wcode, wtext = st.api("A", "PUT",
                              f"/api/v1/workspaces/{ws}/files/write?path={path}",
                              {"content": sentinel}, max_time=90)
        if wcode != 200:
            return False, f"write failed: {wcode} {wtext[:200]} ws={ws}"

        # 2. Warm B, so B holds a cached handle to the pre-replacement sandbox.
        bcode, btext = st.api("B", "GET", read_path, max_time=90)
        if bcode != 200 or sentinel not in btext:
            return False, f"B warm read failed: {bcode} {btext[:200]} ws={ws}"
        sandbox_before = st.pq(
            f"SELECT sandbox_id FROM workspaces WHERE workspace_id='{ws}'")

        # 3. Replace the sandbox through A while hammering B throughout.
        b_codes: list[int] = []
        stop_probing = False

        def probe_b() -> None:
            while not stop_probing:
                c, _ = st.api("B", "GET", read_path, max_time=20)
                b_codes.append(c)
                time.sleep(1)

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            prober = pool.submit(probe_b)
            spec = pool.submit(st.api, "A", "POST",
                               f"/api/v1/workspaces/{ws}/spec",
                               {"tier": target}, False, 300)
            scode, stext = spec.result()
            stop_probing = True
            prober.result()

        if scode != 200:
            return False, f"/spec failed: {scode} {stext[:200]} ws={ws}"
        sandbox_after = st.pq(
            f"SELECT sandbox_id FROM workspaces WHERE workspace_id='{ws}'")
        if not sandbox_after or sandbox_after == sandbox_before:
            return False, (f"sandbox_id did not change ({sandbox_before!r}) — "
                           f"/spec did not recreate; ws={ws}")

        # 4. B must converge to the real content, not merely stop 404ing.
        converged, last = False, ""
        deadline = time.time() + 120
        while time.time() < deadline:
            c, t = st.api("B", "GET", read_path, max_time=30)
            b_codes.append(c)
            last = f"{c} {t[:160]}"
            if c == 200 and sentinel in t:
                converged = True
                break
            if c == 404:  # the reported bug: absence claimed for a live file
                break
            time.sleep(3)

        # 5. B must have *noticed*, not just gotten lucky on a cold cache.
        # Scoped to THIS workspace: /tmp/gate_B.log is opened append-only and
        # survives teardown, so a bare 'is stale' count is satisfied by any
        # previous run's detection and would keep passing after a regression.
        log = sh(["docker", "exec", st.backend, "sh", "-c",
                  f"grep -c 'Cached session for {ws} is stale' "
                  "/tmp/gate_B.log || true"]).stdout.strip()
        noticed = (log or "0") != "0"
        never_404 = 404 not in b_codes
        ok = converged and never_404 and noticed
        return ok, (f"sandbox {sandbox_before[:8]}→{sandbox_after[:8]} "
                    f"B_codes={sorted(set(b_codes))} converged={converged} "
                    f"stale_detections={log} last={last} ws={ws}")
    finally:
        # Before teardown_workers, or the Daytona sandbox leaks and bills on.
        st.api("A", "DELETE", f"/api/v1/workspaces/{ws}", max_time=180)


# --------------------------------------------------------------------------
# Steer cells (19-23). A steer is a ``hook_outbox`` row (hook_type='steer')
# that belongs to the thread, not to one run. The verdict every cell shares:
# each accepted steer ends status='done' exactly once with an outcome, none
# stays pending/claimed/dead, and the model reads it at most once, counted
# in the checkpoint by a per-steer token, never from process memory.
# --------------------------------------------------------------------------

def _settled_thread_steer(st: Stack, row: dict | None, outcomes: tuple[str, ...],
                          runs: dict[str, dict]) -> str | None:
    """Why a settled main-thread steer row breaks the contract, or None."""
    if not row:
        return "LOST: no row"
    if row["status"] != "done":
        return f"row {row['status']} outcome={row['outcome'] or '-'}"
    if row["outcome"] not in outcomes:
        return f"outcome {row['outcome'] or '-'} not in {outcomes}"
    if row["outcome"] == "turn":
        took = row["dispatched_run_id"]
        if took not in runs:
            return f"outcome turn but dispatched run {took or '-'} missing"
        final = st.wait_terminal(took, timeout=90)
        if final.get("status") != "completed":
            return f"its turn {took[:8]} ended {final.get('status')}"
    if row["outcome"] == "delivered" and row["steered_run_id"] not in runs:
        return f"delivered by unknown run {row['steered_run_id'] or '-'}"
    return None


def cell_19_steer_vs_finalize(st: Stack) -> tuple[bool, str]:
    """User steer on B timed into the end of A's turn → read once, never lost.

    The finalize CAS and the steer's admission meet on the run row: a steer
    that commits first is released by the finalize (or made due by the claim
    safety net) and becomes the next turn; one that loses is refused
    ``not_running`` with nothing written.
    """
    code, resp = st.dispatch("A", SHORT_PROMPT)
    if code not in (200, 202):
        return False, f"calibration dispatch failed: {code} {resp}"
    row, dur = st.run_until_terminal(resp["run_id"])
    if row.get("status") != "completed":
        return False, f"calibration turn ended {row} thread={resp['thread_id']}"
    plans = [(0.6, 0.9), (0.8, 1.0), (0.95, 1.1)]
    failures: list[str] = []
    tally: dict[str, int] = {}
    threads: list[str] = []
    for fracs in plans:
        code, resp = st.dispatch("A", SHORT_PROMPT)
        if code not in (200, 202):
            failures.append(f"dispatch {code}")
            continue
        tid, rid = resp["thread_id"], resp["run_id"]
        threads.append(tid[:8])
        t0 = time.time()

        def fire(frac: float, tid: str = tid, t0: float = t0) -> tuple:
            time.sleep(max(0.0, t0 + frac * dur - time.time()))
            token = f"G19{uuid.uuid4().hex[:10]}"
            ok, key, detail = st.steer(
                "B", tid, f"{token}: also end your reply with the word MANGO.")
            return token, ok, key, detail, round(time.time() - t0, 1)

        with concurrent.futures.ThreadPoolExecutor(len(fracs)) as pool:
            sent = list(pool.map(fire, fracs))
        st.wait_terminal(rid)
        rows = st.wait_steers_settled([f"steer:{k}" for _, ok, k, _, _ in sent if ok])
        quiet = st.wait_thread_quiet(tid)
        rows = st.steer_rows([f"steer:{k}" for _, _, k, _, _ in sent])
        runs = {r["run_id"]: r for r in st.thread_runs(tid)}
        msgs = st.ckpt_messages(tid)
        if not quiet:
            failures.append(f"{tid[:8]} never went quiet")
        for token, ok, key, detail, at in sent:
            row = rows.get(f"steer:{key}")
            if not ok:
                tally["refused"] = tally.get("refused", 0) + 1
                if row:
                    failures.append(f"{token}@{at}s refused yet wrote {row}")
                elif '"code": "not_running"' not in detail:
                    failures.append(f"{token}@{at}s refused oddly: {detail[:200]}")
                continue
            outcome = (row or {}).get("outcome") or "-"
            tally[outcome] = tally.get(outcome, 0) + 1
            why = _settled_thread_steer(st, row, ("delivered", "turn"), runs)
            hits = st.token_hits(msgs, token)
            if why is None and len(hits) != 1:
                why = f"read {len(hits)}x in the checkpoint"
            if why:
                failures.append(f"{token}@{at}s thread={tid[:8]}: {why}")
    accepted = sum(v for k, v in tally.items() if k != "refused")
    if not accepted:
        failures.append("no steer was accepted: nothing raced the finalize")
    runtime = st.thread_runs(resp["thread_id"])[0]["msg_type"] if threads else "-"
    return not failures, (f"{runtime} turn≈{dur:.1f}s outcomes={tally} threads={threads}"
                          + (f" FAILURES={failures}" if failures else ""))


def cell_20_steer_vs_cancel(st: Stack) -> tuple[bool, str]:
    """User steer on B, then a stop → returned to the caller, or delivered
    if read before the stamp; never both, never left pending."""
    plans = [("A", 0.0, 1), ("A", 0.4, 1), ("B", 1.0, 1), ("B", 0.0, 2)]
    failures: list[str] = []
    tally: dict[str, int] = {}
    threads: list[str] = []
    for cancel_on, gap, n in plans:
        code, resp = st.dispatch("A", LONG_PROMPT)
        if code not in (200, 202):
            failures.append(f"dispatch {code}")
            continue
        tid, rid = resp["thread_id"], resp["run_id"]
        threads.append(tid[:8])
        time.sleep(3)
        sent = []
        for _ in range(n):
            token = f"G20{uuid.uuid4().hex[:10]}"
            ok, key, detail = st.steer(
                "B", tid, f"{token}: also end your reply with the word MANGO.")
            sent.append((token, ok, key, detail))
        time.sleep(gap)
        path = f"/api/v1/threads/{tid}/cancel?run_id={rid}"
        c1, b1 = st.api(cancel_on, "POST", path)
        final = st.wait_terminal(rid, timeout=90)
        c2, b2 = st.api("B" if cancel_on == "A" else "A", "POST", path)

        def returned(body: str) -> set[str]:
            try:
                return {e.get("request_key") for e in
                        json.loads(body).get("returned_steers") or []}
            except (ValueError, AttributeError):
                return set()

        first, again = returned(b1), returned(b2)
        rows = st.wait_steers_settled([f"steer:{k}" for _, ok, k, _ in sent if ok])
        st.wait_thread_quiet(tid)
        msgs = st.ckpt_messages(tid)
        if final.get("status") != "cancelled":
            failures.append(f"{tid[:8]} run ended {final.get('status')} (cancel {c1} {b1[:100]})")
        for token, ok, key, detail in sent:
            if not ok:
                failures.append(f"{token} refused before the stop: {detail[:200]}")
                continue
            row = rows.get(f"steer:{key}")
            hits = st.token_hits(msgs, token)
            outcome = (row or {}).get("outcome") or "-"
            label = outcome + ("(re-asked)" if key in again - first else "")
            tally[label] = tally.get(label, 0) + 1
            if not row or row["status"] != "done":
                failures.append(f"{token} thread={tid[:8]}: row {row}")
            elif outcome == "returned":
                if key not in first | again:
                    failures.append(f"{token}: returned but in neither /cancel answer")
                if hits:
                    failures.append(f"{token}: returned AND in the checkpoint at {hits}")
            elif outcome == "delivered":
                if key in first | again:
                    failures.append(f"{token}: delivered AND returned by /cancel")
                if len(hits) != 1:
                    failures.append(f"{token}: delivered but read {len(hits)}x")
            else:
                failures.append(f"{token} thread={tid[:8]}: outcome {outcome} after a user stop")
    runtime = st.thread_runs(tid)[0]["msg_type"] if threads else "-"
    return not failures, (f"{runtime} outcomes={tally} threads={threads}"
                          + (f" FAILURES={failures}" if failures else ""))


def cell_21_cos_steer(st: Stack) -> tuple[bool, str]:
    """Chief of Staff hand-off on B into the analyst's live turn on A →
    ``steered``, one row read once, and the run reports under its gen.

    The POST is the one ``src/tools/secretary/dispatch.py`` sends: dispatched,
    service-token auth, ``origin`` agent plus the report-back wiring
    (``origin_home_thread_id`` / ``origin_dispatch_gen``). The Home thread is
    a fresh Home thread of the gate's own, so a report-back it may get
    never lands in the user's real Home conversation.
    """
    ws = st.steer_workspace()
    if not ws:
        return False, "no running analyst workspace (set GATE_WORKSPACE)"
    home_tid = st.settled_thread("A")
    agent = "chief_of_staff"
    # "late" times the hand-off at half a calibration turn on the same (warm)
    # thread: inside the analyst's final answer, which then misses it.
    plans = [("mid", NO_TOOLS_LONG_PROMPT, 8.0), ("late", SHORT_PROMPT, None)]
    failures: list[str] = []
    notes: list[str] = []
    for label, prompt, delay in plans:
        warm = None
        if delay is None:
            code, resp = st.dispatch("A", prompt, workspace_id=ws)
            if code not in (200, 202):
                failures.append(f"{label}: calibration dispatch {code} {resp}")
                continue
            _, d = st.run_until_terminal(resp["run_id"])
            warm, delay = resp["thread_id"], 0.5 * d
        code, resp = st.dispatch("A", prompt, warm, workspace_id=ws)
        if code not in (200, 202):
            failures.append(f"{label}: analyst dispatch {code} {resp}")
            continue
        tid, rid = resp["thread_id"], resp["run_id"]
        time.sleep(delay)
        live = st.run_row(rid).get("status")
        gen = str(uuid.uuid4())
        token = f"G21{uuid.uuid4().hex[:10]}"
        body = {
            "messages": [{"role": "user", "content":
                          f"{token}: when you finish, also state the word MANGO."}],
            "workspace_id": ws,
            "origin": {"type": "agent", "id": home_tid, "agent": agent},
            "origin_home_thread_id": home_tid,
            "origin_dispatch_gen": gen,
        }
        hcode, htext = st.api("B", "POST", f"/api/v1/threads/{tid}/messages",
                              body, dispatch=True, max_time=60)
        try:
            hresp = json.loads(htext)
        except ValueError:
            hresp = {"raw": htext[:200]}
        status = hresp.get("status")
        key = f"steer:dispatch:{gen}"
        st.wait_terminal(rid, timeout=300)
        rows = st.wait_steers_settled([key]) if status == "steered" else st.steer_rows([key])
        st.wait_thread_quiet(tid, timeout=240)
        runs = {r["run_id"]: r for r in st.thread_runs(tid)}
        msgs = st.ckpt_messages(tid)
        hits = st.token_hits(msgs, token)
        row = rows.get(key)
        tag = f"{label} thread={tid[:8]} run_was={live}"
        if hcode != 200 or status not in ("steered", "dispatched"):
            failures.append(f"{tag}: hand-off {hcode} {str(hresp)[:160]}")
            continue
        if status == "steered":
            gens = st.pq(
                "SELECT COALESCE(metadata->'dispatch_gens' ? "
                f"'{gen}', false) FROM conversation_responses "
                f"WHERE conversation_response_id='{rid}'")
            why = None
            if hresp.get("run_id") != rid:
                why = f"steered into {hresp.get('run_id')} not the live run {rid[:8]}"
            why = why or _settled_thread_steer(st, row, ("delivered", "turn"), runs)
            if why is None and gens != "t":
                why = "the live run's dispatch_gens lacks the gen"
            if why is None and row["outcome"] == "turn":
                took_gen = st.pq(
                    "SELECT COALESCE(metadata->>'origin_dispatch_gen','') FROM "
                    "conversation_responses WHERE conversation_response_id="
                    f"'{row['dispatched_run_id']}'")
                if took_gen != gen:
                    why = f"its turn carries gen {took_gen or '-'}"
            if why is None and len(hits) != 1:
                why = f"read {len(hits)}x in the checkpoint"
            notes.append(f"{label}@{delay:.1f}s:steered→{(row or {}).get('outcome') or '-'}")
            if why:
                failures.append(f"{tag}: {why}")
        else:
            # The turn had ended: the hand-off is a fresh dispatch, no steer.
            new = hresp.get("run_id", "")
            took_gen = st.pq(
                "SELECT COALESCE(metadata->>'origin_dispatch_gen','') FROM "
                f"conversation_responses WHERE conversation_response_id='{new}'")
            notes.append(f"{label}@{delay:.1f}s:dispatched")
            if row:
                failures.append(f"{tag}: dispatched yet a steer row exists {row}")
            if took_gen != gen or len(hits) != 1:
                failures.append(f"{tag}: dispatched run gen={took_gen or '-'} reads={len(hits)}")
    return not failures, (f"agent={agent} home={home_tid[:8]} ws={ws[:8]} {notes}"
                          + (f" FAILURES={failures}" if failures else ""))


def cell_22_task_steer_vs_finish(st: Stack) -> tuple[bool, str]:
    """Task steer on B timed at the background task's finish on A → the row
    ends delivered, resumed (a cause='resume' run reads it) or returned
    (with ``replay_facts.steering_returned``), exactly one of them.

    Needs a real sandbox: a PTC turn in a running workspace launches the task.
    Rep 1 steers mid-generation (the task's one model call is under way, so it
    misses it); rep 2 times the steer at rep 1's measured task duration.
    """
    ws = st.steer_workspace()
    if not ws:
        return False, "no running analyst workspace (set GATE_WORKSPACE)"
    failures: list[str] = []
    tally: dict[str, int] = {}
    notes: list[str] = []
    task_dur: float | None = None
    for rep in range(2):
        code, resp = st.dispatch("A", TASK_PROMPT, workspace_id=ws)
        if code not in (200, 202):
            failures.append(f"rep{rep}: dispatch {code} {resp}")
            continue
        tid, rid = resp["thread_id"], resp["run_id"]
        task = ""
        deadline = time.time() + 240
        while time.time() < deadline and not task:
            task = st.pq(
                "SELECT task_id || '|' || task_run_id || '|' || "
                "extract(epoch from started_at) FROM subagent_runs WHERE "
                f"thread_id='{tid}' ORDER BY started_at LIMIT 1")
            if not task:
                if st.run_row(rid).get("status") != "in_progress":
                    break
                time.sleep(0.5)
        if not task:
            failures.append(f"rep{rep} thread={tid[:8]}: the turn launched no Task")
            st.wait_terminal(rid, timeout=120)
            continue
        task_id, task_run_id, started = task.split("|")
        offset = 3.0 if rep == 0 or task_dur is None else max(1.0, task_dur - 0.3)
        time.sleep(max(0.0, float(started) + offset - time.time()))
        token = f"G22{uuid.uuid4().hex[:10]}"
        input_id = f"g22{uuid.uuid4().hex[:12]}"
        scode, stext = st.api("B", "POST",
                              f"/api/v1/threads/{tid}/tasks/{task_id}/messages",
                              {"content": f"{token}: also end with the word MANGO.",
                               "input_id": input_id})
        at = round(time.time() - float(started), 1)
        key = f"steer:{tid}:{task_id}:{input_id}"
        st.wait_terminal(rid, timeout=300)
        rows = st.wait_steers_settled([key], timeout=240) if scode == 200 else st.steer_rows([key])
        st.wait_thread_quiet(tid, timeout=240)
        if task_dur is None:
            d = st.pq(
                "SELECT extract(epoch from finalized_at - started_at) FROM "
                f"subagent_runs WHERE task_run_id='{task_run_id}'")
            task_dur = float(d) if d else None
        row = rows.get(key)
        tag = f"rep{rep} thread={tid[:8]} task={task_id} steer@{at}s"
        if scode != 200:
            tally["refused"] = tally.get("refused", 0) + 1
            notes.append(f"{tag}: refused {scode} {stext[:80]}")
            if row:
                failures.append(f"{tag}: refused yet wrote {row}")
            continue
        outcome = (row or {}).get("outcome") or "-"
        tally[outcome] = tally.get(outcome, 0) + 1
        returned_n = int(st.pq(
            "SELECT count(*) FROM subagent_runs r, jsonb_array_elements("
            "COALESCE(r.replay_facts->'steering_returned','[]'::jsonb)) e "
            f"WHERE r.thread_id='{tid}' AND e->>'input_id'='{input_id}'"))
        resumes = int(st.pq(
            "SELECT count(*) FROM subagent_runs WHERE "
            f"thread_id='{tid}' AND task_id='{task_id}' AND cause='resume'"))
        task_reads = len(st.token_hits(st.ckpt_messages(tid, f"task:{task_id}"), token))
        notes.append(f"{tag}: {outcome} returned_facts={returned_n} "
                     f"resume_runs={resumes} task_reads={task_reads}")
        if not row or row["status"] != "done":
            failures.append(f"{tag}: row {row}")
        elif outcome not in ("delivered", "resumed", "returned"):
            failures.append(f"{tag}: outcome {outcome}")
        elif outcome == "returned" and (returned_n != 1 or task_reads):
            failures.append(f"{tag}: returned with facts={returned_n} reads={task_reads}")
        elif outcome != "returned" and (returned_n or task_reads != 1):
            failures.append(f"{tag}: {outcome} with facts={returned_n} reads={task_reads}")
        elif outcome == "resumed" and not resumes:
            failures.append(f"{tag}: resumed but no cause='resume' run")
    if not sum(v for k, v in tally.items() if k != "refused"):
        failures.append("no task steer was accepted: nothing raced the finish")
    return not failures, (f"outcomes={tally} {notes}"
                          + (f" FAILURES={failures}" if failures else ""))


def cell_23_missed_steer_vs_send(st: Stack) -> tuple[bool, str]:
    """A steer missed by A's one long generation, then a new send the moment
    that run ends → the steer is read once and before the new message."""
    failures: list[str] = []
    notes: list[str] = []
    for send_on in ("B", "A", "B"):
        code, resp = st.dispatch("A", LONG_PROMPT)
        if code not in (200, 202):
            failures.append(f"dispatch {code}")
            continue
        tid, rid = resp["thread_id"], resp["run_id"]
        time.sleep(3)
        s_tok = f"G23S{uuid.uuid4().hex[:10]}"
        ok, skey, detail = st.steer(
            "B", tid, f"{s_tok}: also end your reply with the word MANGO.")
        if not ok:
            failures.append(f"thread={tid[:8]}: mid-generation steer refused {detail[:200]}")
            st.wait_terminal(rid)
            continue
        final, _ = st.run_until_terminal(rid, step=0.15)
        n_tok = f"G23N{uuid.uuid4().hex[:10]}"
        ncode, ntext = st.api(send_on, "POST", f"/api/v1/threads/{tid}/messages",
                              {"workspace_id": st.home(send_on),
                               "request_key": str(uuid.uuid4()),
                               "messages": [{"role": "user", "content":
                                             f"{n_tok}: reply with the single word KIWI."}]},
                              max_time=15)
        rows = st.wait_steers_settled([f"steer:{skey}"])
        st.wait_thread_quiet(tid)
        runs = {r["run_id"]: r for r in st.thread_runs(tid)}
        msgs = st.ckpt_messages(tid)
        s_hits, n_hits = st.token_hits(msgs, s_tok), st.token_hits(msgs, n_tok)
        row = rows.get(f"steer:{skey}")
        tag = f"send_on={send_on} thread={tid[:8]} {next(iter(runs.values()))['msg_type']}"
        notes.append(f"{tag}: run={final.get('status')} steer→"
                     f"{(row or {}).get('outcome') or '-'} order={s_hits}<{n_hits}")
        why = _settled_thread_steer(st, row, ("turn", "delivered"), runs)
        if why is None and (len(s_hits) != 1 or len(n_hits) != 1):
            why = (f"reads steer={len(s_hits)} new={len(n_hits)} "
                   f"(send {ncode} {' '.join(ntext.split())[:100]})")
        if why is None and s_hits[0] > n_hits[0]:
            why = "the new message was read before the steer"
        if why:
            failures.append(f"{tag}: {why}")
    return not failures, (f"{notes}" + (f" FAILURES={failures}" if failures else ""))


CELLS = {
    "1": ("Two-process START slot exclusivity", cell_1_start_race),
    "7": ("Cancel: cross-worker + idempotent", cell_7_cancel_races),
    "9": ("Compaction vs live run → 409", cell_9_compact_vs_live),
    "10": ("Delete vs live run → 409", cell_10_delete_vs_live),
    "13": ("Request-key replay → one run", cell_13_request_key_replay),
    "14": ("Retry race → one attempt-2", cell_14_retry_race),
    "18": ("Sandbox replaced behind a worker → re-attach", cell_18_sandbox_replacement),
    "19": ("Steer vs finalize → read once, never lost", cell_19_steer_vs_finalize),
    "20": ("Steer vs cancel → returned xor delivered", cell_20_steer_vs_cancel),
    "21": ("Chief of Staff hand-off steered into a live turn", cell_21_cos_steer),
    "22": ("Task steer vs task finish → one settlement", cell_22_task_steer_vs_finish),
    "23": ("Missed steer vs new send → steer read first", cell_23_missed_steer_vs_send),
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
