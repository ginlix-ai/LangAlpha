#!/usr/bin/env python3
"""Frontend-mock wire pass: dispatch → refresh → dispatch → steer → settle.

Scripts the manual browser visual pass against the raw backend API so backend
correctness can be verified without a frontend in the loop. Flow (mirrors the
manual pass):

  1. Turn 1: dispatch a background subagent (AMD dip research), main turn
     exits without waiting for the result (tail mode).
  2. "Refresh": /status × N (worker-coherence), full replay (each turn exactly
     once, terminal turns stamped with run_id), mux v2 probe (channel lanes
     cover active tasks).
  3. Turn 2: second subagent (semiconductor sector) — this stream is DROPPED
     mid-turn and resumed via Last-Event-ID to mock a network blip.
  4. Refresh again.
  5. Turn 3: steer — update all running subagents to keep responses short.
  6. Settle watch: poll /status while tasks finish; every time a report-back
     run is named, cross-check the fix-3 window invariant (a named run whose
     turn is already replayable MUST appear in recent_report_back_run_ids)
     and the fix-1 stamp (its user_message carries run_id). Duplicate-bubble
     checks run on every replay: no turn ever renders twice.

Every check prints ✅/❌ with evidence; exit code 1 if any hard check failed.
Threads are kept for visual inspection (never deleted).

Usage:
  uv run python scripts/e2e/subagent_wire_pass.py --user <user_id> \
      [--workspace <ws_id>] [--base http://localhost:8020] [--model <model>] \
      [--settle-timeout 600] [--turn-timeout 420]

Auth: X-Service-Token from INTERNAL_SERVICE_TOKEN (repo .env or environment),
user id via --user or E2E_USER_ID. The workspace defaults to the user's most
recently updated non-archived workspace (PTC turns need one).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]

CHECKS: list[tuple[str, bool, str]] = []
OBSERVATIONS: list[str] = []


def check(name: str, ok: bool, evidence: str) -> bool:
    CHECKS.append((name, ok, evidence))
    print(f"  {'✅' if ok else '❌'} {name} — {evidence}")
    return ok


def observe(msg: str) -> None:
    OBSERVATIONS.append(msg)
    print(f"  ℹ️  {msg}")


def read_env_file() -> dict[str, str]:
    out: dict[str, str] = {}
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


@dataclass
class Env:
    base: str
    token: str
    user_id: str
    turn_timeout: float

    @property
    def headers(self) -> dict[str, str]:
        return {"X-Service-Token": self.token, "X-User-Id": self.user_id}

    def client(self, read_timeout: float | None = None) -> httpx.Client:
        return httpx.Client(
            base_url=self.base,
            headers=self.headers,
            timeout=httpx.Timeout(10.0, read=read_timeout or 30.0),
        )


@dataclass
class Frame:
    event: str
    data: dict
    id: int | None


def iter_frames(resp: httpx.Response):
    """Parse an SSE byte stream into frames."""
    event, data_lines, fid = "message", [], None
    for line in resp.iter_lines():
        if line == "":
            if data_lines:
                raw = "\n".join(data_lines)
                try:
                    data = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    data = {"_raw": raw}
                yield Frame(event, data if isinstance(data, dict) else {"_raw": raw}, fid)
            event, data_lines, fid = "message", [], None
            continue
        if line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
        elif line.startswith("id:"):
            try:
                fid = int(line[3:].strip())
            except ValueError:
                fid = None


@dataclass
class TurnResult:
    thread_id: str
    run_id: str
    frames: list[Frame] = field(default_factory=list)
    task_inits: list[str] = field(default_factory=list)
    task_updates: list[str] = field(default_factory=list)
    outcome: str | None = None
    last_event_id: int | None = None

    def absorb(self, fr: Frame) -> None:
        self.frames.append(fr)
        if fr.id is not None:
            self.last_event_id = fr.id
        if fr.event == "artifact" and fr.data.get("artifact_type") == "task":
            payload = fr.data.get("payload") or {}
            tid, action = payload.get("task_id"), payload.get("action")
            if tid and action == "init" and tid not in self.task_inits:
                self.task_inits.append(tid)
            elif tid and action == "update" and tid not in self.task_updates:
                self.task_updates.append(tid)
        if fr.event == "run_end":
            self.outcome = fr.data.get("outcome") or "completed"
        if fr.event == "error":
            self.outcome = self.outcome or "error"


def post_turn(
    env: Env,
    prompt: str,
    thread_id: str | None,
    workspace_id: str,
    model: str | None,
    drop_after: int | None = None,
) -> TurnResult:
    """POST a message and stream the main run. With ``drop_after``, abort the
    stream after that many frames and resume via Last-Event-ID — the network
    blip half of the frontend mock (ids must continue past the drop point,
    strictly increasing)."""
    body: dict = {
        "agent_mode": "ptc",
        "workspace_id": workspace_id,
        "messages": [{"role": "user", "content": prompt}],
    }
    if model:
        body["llm_model"] = model
    path = f"/api/v1/threads/{thread_id}/messages" if thread_id else "/api/v1/threads/messages"

    result: TurnResult | None = None
    dropped = False
    deadline = time.time() + env.turn_timeout
    with env.client(read_timeout=env.turn_timeout) as c:
        with c.stream("POST", path, json=body) as resp:
            if resp.status_code != 200:
                resp.read()
                sys.exit(f"POST {path} -> {resp.status_code}: {resp.text[:300]}")
            loc = resp.headers.get("content-location", "")
            run_id = loc.split("run_id=")[-1] if "run_id=" in loc else ""
            for fr in iter_frames(resp):
                if result is None:
                    tid = thread_id or str(fr.data.get("thread_id") or "")
                    result = TurnResult(thread_id=tid, run_id=run_id)
                if not result.thread_id and fr.data.get("thread_id"):
                    result.thread_id = str(fr.data["thread_id"])
                result.absorb(fr)
                if result.outcome:
                    break
                if drop_after is not None and len(result.frames) >= drop_after:
                    dropped = True
                    break
                if time.time() > deadline:
                    break
    if result is None:
        sys.exit(f"POST {path}: stream yielded no frames")
    if not result.run_id:
        sys.exit(f"POST {path}: no run_id in Content-Location ({loc!r})")

    if dropped:
        seen_before = result.last_event_id or 0
        n_before = len(result.frames)
        with env.client(read_timeout=env.turn_timeout) as c:
            with c.stream(
                "GET",
                f"/api/v1/threads/{result.thread_id}/messages/stream",
                params={"run_id": result.run_id},
                headers={"Last-Event-ID": str(seen_before)},
            ) as resp:
                ok_resume = resp.status_code == 200
                monotonic = True
                prev = seen_before
                if ok_resume:
                    for fr in iter_frames(resp):
                        if fr.id is not None:
                            if fr.id <= prev:
                                monotonic = False
                            prev = fr.id
                        result.absorb(fr)
                        if result.outcome or time.time() > deadline:
                            break
        check(
            "mid-stream drop + Last-Event-ID resume",
            ok_resume and monotonic and len(result.frames) > n_before,
            f"dropped at id {seen_before} ({n_before} frames), resumed to id "
            f"{result.last_event_id} ({len(result.frames)} frames), "
            f"monotonic={monotonic}",
        )
    return result


# ---------------------------------------------------------------------------
# Read-model snapshots
# ---------------------------------------------------------------------------


def get_status(env: Env, tid: str) -> dict:
    with env.client() as c:
        r = c.get(f"/api/v1/threads/{tid}/status")
        r.raise_for_status()
        return r.json()


@dataclass
class ReplaySnapshot:
    # A turn legitimately replays several user_message items (the query row
    # plus checkpointed mid-turn steering bubbles) — the duplicate-transcript
    # invariant is therefore on (turn_index, content), not turn_index alone.
    bubbles: dict[tuple[int, str], int] = field(default_factory=dict)
    turns: set[int] = field(default_factory=set)
    stamped: dict[int, str] = field(default_factory=dict)  # turn_index -> run_id
    task_status: dict[str, str] = field(default_factory=dict)  # task_id -> stamped status
    events: int = 0

    @property
    def stamped_run_ids(self) -> set[str]:
        return set(self.stamped.values())

    @property
    def duplicate_bubbles(self) -> dict[tuple[int, str], int]:
        return {k: n for k, n in self.bubbles.items() if n != 1}


def replay_snapshot(env: Env, tid: str) -> ReplaySnapshot:
    snap = ReplaySnapshot()
    with env.client(read_timeout=60.0) as c:
        with c.stream("GET", f"/api/v1/threads/{tid}/messages/replay") as resp:
            resp.raise_for_status()
            for fr in iter_frames(resp):
                snap.events += 1
                if fr.event == "user_message":
                    ti = fr.data.get("turn_index")
                    if isinstance(ti, int):
                        snap.turns.add(ti)
                        content = json.dumps(fr.data.get("content"), sort_keys=True, default=str)
                        key = (ti, content[:400])
                        snap.bubbles[key] = snap.bubbles.get(key, 0) + 1
                        rid = fr.data.get("run_id")
                        if isinstance(rid, str) and rid:
                            snap.stamped[ti] = rid
                if fr.event == "artifact" and fr.data.get("artifact_type") == "task":
                    payload = fr.data.get("payload") or {}
                    task_id = payload.get("task_id")
                    status = fr.data.get("status") or payload.get("status")
                    if task_id and status:
                        snap.task_status[str(task_id)] = str(status)
                if fr.event == "replay_done":
                    break
    return snap


def _seq_tuple(seq: str) -> tuple[int, int]:
    """Redis stream entry id 'major-minor' → sortable tuple."""
    major, _, minor = str(seq).partition("-")
    try:
        return (int(major), int(minor or 0))
    except ValueError:
        return (0, 0)


@dataclass
class MuxCapture:
    seqs: dict[str, list[str]] = field(default_factory=dict)  # run_id -> seqs (order)
    lanes: dict[str, str] = field(default_factory=dict)  # run_id -> lane
    open_lanes: set[str] = field(default_factory=set)
    closes: dict[str, str] = field(default_factory=dict)  # run_id -> terminal outcome


def mux_capture(
    env: Env, tid: str, cursors: dict[str, str] | None, window_s: float
) -> MuxCapture:
    """Connect the v2 mux for a window; optionally resume from per-run cursors
    (exclusive, like the frontend's reconnect)."""
    cap = MuxCapture()
    params: dict[str, str] = {"contract": "v2", "since_age_s": "2"}
    if cursors:
        params["cursors"] = ",".join(f"run:{r}#{s}" for r, s in cursors.items())
    deadline = time.time() + window_s
    try:
        with env.client(read_timeout=window_s + 2) as c:
            with c.stream(
                "GET", f"/api/v1/threads/{tid}/stream", params=params
            ) as resp:
                if resp.status_code != 200:
                    return cap
                for fr in iter_frames(resp):
                    if fr.event == "chan_open":
                        lane = fr.data.get("lane")
                        chan = str(fr.data.get("chan") or "")
                        if isinstance(lane, str):
                            cap.open_lanes.add(lane)
                            if chan.startswith("run:"):
                                cap.lanes[chan[4:]] = lane
                    elif fr.event == "chan_close":
                        chan = str(fr.data.get("chan") or "")
                        if fr.data.get("reason") == "terminal" and chan.startswith("run:"):
                            cap.closes[chan[4:]] = str(fr.data.get("outcome") or "")
                    else:
                        rid, seq = fr.data.get("run_id"), fr.data.get("seq")
                        if isinstance(rid, str) and isinstance(seq, str):
                            cap.seqs.setdefault(rid, []).append(seq)
                            lane = fr.data.get("lane")
                            if isinstance(lane, str):
                                cap.lanes.setdefault(rid, lane)
                    if time.time() > deadline:
                        break
    except httpx.TimeoutException:
        pass
    return cap


def mux_probe(env: Env, tid: str, window_s: float = 3.0) -> tuple[set[str], dict[str, str]]:
    """Lane discovery only: (open lanes, run->close outcome)."""
    cap = mux_capture(env, tid, None, window_s)
    return cap.open_lanes, cap.closes


def mux_resume_check(env: Env, tid: str, label: str, window_s: float = 5.0) -> None:
    """The subagent half of the frontend mock: attach the mux, record each run
    channel's cursor, DROP, reconnect with those cursors — resumed delivery
    must be strictly after the cursor (exclusive) on every channel, and live
    channels must keep making progress (frames or a terminal close)."""
    a = mux_capture(env, tid, None, window_s)
    cursors = {r: seqs[-1] for r, seqs in a.seqs.items() if seqs}
    task_runs = [r for r in cursors if str(a.lanes.get(r, "")).startswith("task:")]
    if not task_runs:
        observe(f"[{label}] no subagent frames captured — resume check skipped")
        return
    b = mux_capture(env, tid, cursors, window_s)
    redelivered = {
        r[:8]: dup
        for r, cur in cursors.items()
        if (dup := [s for s in b.seqs.get(r, []) if _seq_tuple(s) <= _seq_tuple(cur)])
    }
    check(
        f"[{label}] subagent mux resume is exclusive (nothing at/below cursor)",
        not redelivered,
        f"channels={ {a.lanes[r]: cursors[r] for r in task_runs} } "
        f"redelivered={redelivered or 'none'}",
    )
    progressed = [
        r for r in task_runs if b.seqs.get(r) or r in b.closes or a.lanes.get(r, "") in b.open_lanes
    ]
    check(
        f"[{label}] subagent channels progress past the cursor after resume",
        len(progressed) == len(task_runs),
        f"{len(progressed)}/{len(task_runs)} channels progressed "
        f"(new frames: { {r[:8]: len(b.seqs.get(r, [])) for r in task_runs} }, "
        f"closes={ {k[:8]: v for k, v in b.closes.items()} or '—'})",
    )


# ---------------------------------------------------------------------------
# The composite "refresh" mock (what a browser reload exercises)
# ---------------------------------------------------------------------------


def refresh_mock(env: Env, tid: str, label: str, status_reads: int = 6) -> dict:
    print(f"\n== refresh mock: {label} ==")
    slices = []
    for _ in range(status_reads):
        s = get_status(env, tid)
        slices.append(
            (
                s.get("status"),
                s.get("run_id"),
                tuple(sorted(s.get("active_tasks") or [])),
                s.get("pending_report_back"),
                s.get("report_back_run_id"),
                tuple(s.get("recent_report_back_run_ids") or []),
            )
        )
        time.sleep(0.15)
    distinct = sorted(set(slices), key=slices.index)
    check(
        f"[{label}] /status coherent across {status_reads} reads (any worker)",
        len(distinct) <= 2,
        f"{len(distinct)} distinct slice(s); first={distinct[0]}"
        + (f" then={distinct[1]}" if len(distinct) > 1 else ""),
    )

    snap = replay_snapshot(env, tid)
    dupes = snap.duplicate_bubbles
    check(
        f"[{label}] replay: every bubble exactly once",
        not dupes,
        f"turns={sorted(snap.turns)} dupes={ {k[0]: n for k, n in dupes.items()} or 'none'}",
    )

    status = get_status(env, tid)
    live_run = status.get("run_id")
    bad_stamp = [rid for rid in snap.stamped_run_ids if live_run and rid == live_run]
    check(
        f"[{label}] replay stamps only terminal runs",
        not bad_stamp,
        f"stamped={len(snap.stamped_run_ids)} run_ids; live run {live_run or '—'} not stamped",
    )

    window_invariant(env, status, snap, label)

    active = set(status.get("active_tasks") or [])
    if active:
        lanes, closes = mux_probe(env, tid)
        task_lanes = {ln.split("task:", 1)[1] for ln in lanes if ln.startswith("task:")}
        check(
            f"[{label}] mux lanes cover active tasks",
            active <= task_lanes,
            f"active={sorted(active)} lanes={sorted(task_lanes)} closes={closes or '—'}",
        )
    else:
        observe(f"[{label}] no active tasks — mux lane check skipped")
    return status


def window_invariant(env: Env, status: dict, snap: ReplaySnapshot, label: str) -> None:
    """Fix-3/fix-1 cross-check: a named report-back run whose turn already
    replays MUST be stamped in that replay AND listed in recents — otherwise a
    refreshing client re-attaches it (the duplicate bubble)."""
    named = status.get("report_back_run_id")
    if not named:
        return
    recents = status.get("recent_report_back_run_ids") or []
    if named in snap.stamped_run_ids:
        check(
            f"[{label}] window invariant: named terminal run {named[:8]}… in recents",
            named in recents,
            f"replay stamps it (persisted) → recents={[r[:8] for r in recents]}",
        )
    else:
        observe(f"[{label}] named run {named[:8]}… not yet replayable (live) — window not open")


# ---------------------------------------------------------------------------
# Settle watch
# ---------------------------------------------------------------------------


def settle_watch(env: Env, tid: str, expected_tasks: set[str], timeout: float) -> None:
    print(f"\n== settle watch (≤{int(timeout)}s) ==")
    deadline = time.time() + timeout
    window_checked = 0
    last_named = None
    while time.time() < deadline:
        status = get_status(env, tid)
        active = set(status.get("active_tasks") or [])
        pending = status.get("pending_report_back")
        named = status.get("report_back_run_id")
        if named and named != last_named:
            last_named = named
            snap = replay_snapshot(env, tid)
            window_invariant(env, status, snap, "settle")
            if named in snap.stamped_run_ids:
                window_checked += 1
        if not active and pending is False and status.get("status") in ("idle", "completed"):
            break
        time.sleep(1.0)

    status = get_status(env, tid)
    snap = replay_snapshot(env, tid)
    dupes = snap.duplicate_bubbles
    check(
        "[final] replay: every bubble exactly once",
        not dupes,
        f"turns={sorted(snap.turns)} dupes={ {k[0]: n for k, n in dupes.items()} or 'none'}",
    )
    unstamped = [
        ti for ti in snap.turns if ti not in snap.stamped and not status.get("run_id")
    ]
    check(
        "[final] all settled turns stamped with run_id",
        not unstamped,
        f"stamped {len(snap.stamped)}/{len(snap.turns)} turns"
        + (f"; missing turns {unstamped}" if unstamped else ""),
    )
    non_terminal = {t: s for t, s in snap.task_status.items() if s == "running"}
    settled = not (status.get("active_tasks") or [])
    check(
        "[final] task artifacts stamped terminal once settled",
        not settled or not non_terminal,
        f"active_tasks={status.get('active_tasks') or []} artifact_status={snap.task_status or '—'}",
    )
    recents = status.get("recent_report_back_run_ids") or []
    observe(
        f"[final] status={status.get('status')} pending_rb={status.get('pending_report_back')} "
        f"recents={len(recents)} window_hits_checked={window_checked} "
        f"tasks_seen={sorted(expected_tasks)}"
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
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    envf = read_env_file()
    ap.add_argument("--user", default=os.environ.get("E2E_USER_ID"), help="user id (or E2E_USER_ID)")
    ap.add_argument("--base", default=f"http://localhost:{envf.get('BACKEND_PORT', '8000')}")
    ap.add_argument("--workspace", default=None)
    ap.add_argument("--model", default=None, help="llm_model override for all turns")
    ap.add_argument("--turn-timeout", type=float, default=420.0)
    ap.add_argument("--settle-timeout", type=float, default=600.0)
    args = ap.parse_args()

    token = os.environ.get("INTERNAL_SERVICE_TOKEN") or envf.get("INTERNAL_SERVICE_TOKEN")
    if not token:
        sys.exit("INTERNAL_SERVICE_TOKEN not found (env or repo .env).")
    if not args.user:
        sys.exit("--user (or E2E_USER_ID) is required.")

    env = Env(base=args.base, token=token, user_id=args.user, turn_timeout=args.turn_timeout)
    ws = args.workspace or pick_workspace(env)
    print(f"base={env.base} user={env.user_id} workspace={ws}")

    # -- Turn 1: dispatch AMD research, exit without waiting ----------------
    print("\n== turn 1: dispatch subagent (AMD dip) ==")
    t1 = post_turn(
        env,
        "Use the Task tool to dispatch ONE background research subagent to "
        "investigate why AMD stock dipped hard recently. Dispatch it in the "
        "background and end your turn immediately after dispatching — do NOT "
        "wait for its result.",
        thread_id=None,
        workspace_id=ws,
        model=args.model,
    )
    tid = t1.thread_id
    print(f"thread={tid}")
    check(
        "turn 1: main run terminal in tail mode with a task dispatched",
        t1.outcome == "completed" and len(t1.task_inits) >= 1,
        f"outcome={t1.outcome} tasks={t1.task_inits}",
    )

    refresh_mock(env, tid, "after turn 1")

    # -- Turn 2: second dispatch, with a mid-stream drop + resume -----------
    print("\n== turn 2: dispatch subagent (semiconductor sector), with drop ==")
    t2 = post_turn(
        env,
        "Use the Task tool to dispatch ONE MORE background research subagent "
        "to research the semiconductor sector in general (current dynamics, "
        "not company-specific). Dispatch and end your turn immediately — do "
        "NOT wait for the result.",
        thread_id=tid,
        workspace_id=ws,
        model=args.model,
        drop_after=8,
    )
    check(
        "turn 2: main run terminal with a second task dispatched",
        t2.outcome == "completed" and len(t2.task_inits) >= 1,
        f"outcome={t2.outcome} tasks={t2.task_inits}",
    )

    refresh_mock(env, tid, "after turn 2")

    print("\n== subagent stream resume mock (drop + cursor reconnect) ==")
    mux_resume_check(env, tid, "after turn 2")

    # -- Turn 3: steer the running subagents --------------------------------
    print("\n== turn 3: steer — keep responses short ==")
    t3 = post_turn(
        env,
        "Update ALL currently running subagent tasks (Task tool, action "
        "'update'): tell them to keep their responses SHORT — a few bullet "
        "points only. Then end your turn.",
        thread_id=tid,
        workspace_id=ws,
        model=args.model,
    )
    check(
        "turn 3: steer turn terminal",
        t3.outcome == "completed",
        f"outcome={t3.outcome} updates={t3.task_updates or 'none observed'}",
    )
    if not t3.task_updates:
        observe("turn 3: no task-update artifacts observed (tasks may have settled first)")

    refresh_mock(env, tid, "after turn 3")

    print("\n== subagent stream resume mock (post-steer) ==")
    mux_resume_check(env, tid, "after turn 3")

    # -- Settle: report-backs drain; window invariant probed on every naming
    all_tasks = set(t1.task_inits) | set(t2.task_inits)
    settle_watch(env, tid, all_tasks, args.settle_timeout)

    # -- Verdict ------------------------------------------------------------
    failed = [c for c in CHECKS if not c[1]]
    print(f"\n{'=' * 60}\n{len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed; "
          f"thread kept: {tid}")
    for name, ok, ev in CHECKS:
        print(f"  {'✅' if ok else '❌'} {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
