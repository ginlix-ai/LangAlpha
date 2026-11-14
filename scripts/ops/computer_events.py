#!/usr/bin/env python3
"""Pair the computer events into starts and stops, and the tunnel events into
online sessions.

The events (``src/server/services/computer_usage/events.py``) are written
one per status write and report, with no writer reading the last one back,
so the pairing happens here, over the whole record at once.

    python scripts/ops/computer_events.py <EVENT_LOG_DIR>            # summary
    python scripts/ops/computer_events.py <EVENT_LOG_DIR> --csv      # one row per transition
    python scripts/ops/computer_events.py <EVENT_LOG_DIR> --tunnels  # machine sessions

A start opens at ``starting`` and ends at ``running`` (``ok``, or
``released`` when a claim on a running computer handed it back without a
new sandbox) or at a revert to ``stopped`` (``failed`` when it reported a
failure, else ``released``). A stop opens at ``stopping`` and ends at
``stopped`` (``ok``) or back at ``running`` (``aborted``). ``error`` and
``deleted`` end either; a new transition ends a leftover one as
``superseded``. A provider start no claim saw is an ``untracked`` start.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

OPENS = {"starting": "start", "stopping": "stop"}
ENDS = {
    "running": {"start": "ok", "stop": "aborted"},
    "stopped": {"start": "released", "stop": "ok"},
    "error": {"start": "error", "stop": "error"},
    "deleted": {"start": "deleted", "stop": "deleted"},
}
FIELDS = [
    "computer_id", "user_id", "kind", "tier", "action", "cause", "client",
    "detail", "tracked", "path", "from_status", "asked_at", "ended_at",
    "outcome", "failure_code", "duration_ms", "provider_ms", "backup_ms", "idle_s",
]


def read_events(directory: Path, stream: str) -> list[dict]:
    events = []
    for path in sorted(directory.glob(f"{stream}-*.jsonl")):
        for line in path.read_text().splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # a line cut off by a crash mid-write
    events.sort(key=lambda e: e["ts"])
    return events


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


class _Pairing:
    """One pass over the computer events, one handler per event kind."""

    def __init__(self) -> None:
        self.done: list[dict] = []
        self.open: dict[str, dict] = {}
        self.last_status: dict[str, str] = {}
        self.meta: dict[str, dict] = {}
        self.by_sandbox: dict[str, str] = {}

    def _open_start(self, cid: str):
        t = self.open.get(cid)
        return t if t and t["action"] == "start" else None

    def _end(self, cid: str, outcome: str, ts: str) -> None:
        t = self.open.pop(cid)
        if t["action"] == "start" and outcome == "released" and t.get("failure_code"):
            outcome = "failed"
        if (t["action"] == "start" and outcome == "ok"
                and t["from_status"] == "running" and not t.get("path")):
            outcome = "released"
        t.update(outcome=outcome, ended_at=ts)
        t["duration_ms"] = int((_ts(ts) - _ts(t["asked_at"])).total_seconds() * 1000)
        self.done.append(t)

    def status(self, cid: str, e: dict) -> None:
        if e.get("sandbox"):
            self.by_sandbox[e["sandbox"]] = cid
        self.meta[cid] = {k: e.get(k) for k in ("user_id", "kind", "tier")}
        status, t = e["status"], self.open.get(cid)
        action = OPENS.get(status)
        if action:
            if t and t["action"] == action:
                return  # the same claim written again
            if t:
                self._end(cid, "superseded", e["ts"])
            self.open[cid] = {
                **self.meta[cid], "computer_id": cid, "action": action,
                "cause": (action == "stop" and e.get("reason")) or e.get("cause", "unknown"),
                "client": e.get("client"), "detail": e.get("detail"), "tracked": True,
                "from_status": self.last_status.get(cid),
                "asked_at": min(e.get("asked_at") or e["ts"], e["ts"], key=_ts),
                "idle_s": e.get("idle_s"),
            }
        elif t and status in ENDS:
            self._end(cid, ENDS[status][t["action"]], e["ts"])
        self.last_status[cid] = status

    def start_path(self, cid: str, e: dict) -> None:
        if t := self._open_start(cid):
            t.setdefault("path", e.get("path"))

    def start_failure(self, cid: str, e: dict) -> None:
        if t := self._open_start(cid):
            t["failure_code"] = e.get("code")
            return
        # The revert can land first: mark the start it just released.
        past = next((d for d in reversed(self.done)
                     if d["computer_id"] == cid and d["action"] == "start"), None)
        if (past and past["outcome"] == "released"
                and _ts(e["ts"]) - _ts(past["ended_at"]) < timedelta(minutes=2)):
            past.update(outcome="failed", failure_code=e.get("code"))

    def stop_phases(self, cid: str, e: dict) -> None:
        t = self.open.get(cid)
        if t and t["action"] == "stop":
            t.update(backup_ms=e.get("backup_ms"), provider_ms=e.get("provider_ms"))

    def runtime_start(self, e: dict) -> None:
        cid = self.by_sandbox.get(e.get("sandbox"))
        if cid is None:
            return
        t = self._open_start(cid)
        if t and not t.get("path"):
            t.update(path=e.get("path"), provider_ms=e.get("provider_ms"))
            return
        self.done.append({
            **self.meta.get(cid, {}), "computer_id": cid, "action": "start",
            "cause": e.get("cause", "unknown"), "client": e.get("client"),
            "tracked": False, "path": e.get("path"), "outcome": "ok",
            "asked_at": e["ts"], "ended_at": e["ts"], "duration_ms": e.get("provider_ms"),
            "provider_ms": e.get("provider_ms"),
        })


def pair_transitions(events: list[dict]) -> list[dict]:
    pairing = _Pairing()
    by_computer = {
        "status": pairing.status,
        "start_path": pairing.start_path,
        "start_failure": pairing.start_failure,
        "stop_phases": pairing.stop_phases,
    }
    for e in events:
        if e["event"] == "runtime_start":
            pairing.runtime_start(e)
        elif (handle := by_computer.get(e["event"])) and (cid := e.get("computer_id")):
            handle(cid, e)
    return pairing.done


def tunnel_sessions(events: list[dict]) -> dict[str, list[tuple[str, str, str]]]:
    """Per tunnel, its (connected, ended, how) sessions."""
    online: dict[str, str] = {}
    sessions: dict[str, list] = defaultdict(list)
    for e in events:
        change, tid = e["event"], e.get("tunnel_id")
        if change in ("gateway_start", "gateway_stop"):
            for t, since in online.items():
                sessions[t].append((since, e["ts"], change))
            online.clear()
        elif change == "connected":
            online.setdefault(tid, e["ts"])  # a second connection continues the session
        elif tid in online:
            sessions[tid].append((online.pop(tid), e["ts"], change))
    return sessions


def _p(values: list[int], q: float) -> str:
    if not values:
        return "-"
    values = sorted(values)
    return f"{values[min(len(values) - 1, int(q * len(values)))] / 1000:.1f}s"


def summarize(transitions: list[dict]) -> None:
    groups: dict[tuple, list[int]] = defaultdict(list)
    for t in transitions:
        key = (t["action"], t.get("kind"), t.get("path") or "-", t.get("cause"), t["outcome"])
        groups[key].append(t.get("duration_ms") or 0)
    print(f"{'action':6} {'kind':8} {'path':10} {'cause':14} {'outcome':10} {'n':>5} {'p50':>8} {'p90':>8}")
    for key in sorted(groups, key=lambda k: (k[0], -len(groups[k]))):
        ms = groups[key]
        print(f"{key[0]:6} {str(key[1]):8} {key[2]:10} {str(key[3]):14} {key[4]:10} "
              f"{len(ms):5} {_p(ms, 0.5):>8} {_p(ms, 0.9):>8}")
    idle = [t["idle_s"] for t in transitions if t["action"] == "stop" and t.get("idle_s") is not None]
    if idle:
        print(f"\nstops: median idle before stop {statistics.median(idle) / 60:.0f} min (n={len(idle)})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", type=Path)
    parser.add_argument("--csv", action="store_true", help="one row per transition")
    parser.add_argument("--tunnels", action="store_true", help="machine online sessions")
    args = parser.parse_args()
    if args.tunnels:
        for tid, rows in tunnel_sessions(read_events(args.directory, "tunnel-events")).items():
            hours = sum((_ts(b) - _ts(a)).total_seconds() for a, b, _ in rows) / 3600
            print(f"{tid}  sessions={len(rows)}  online={hours:.1f}h")
        return 0
    transitions = pair_transitions(read_events(args.directory, "computer-events"))
    if args.csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(transitions)
    else:
        summarize(transitions)
    return 0


if __name__ == "__main__":
    sys.exit(main())
