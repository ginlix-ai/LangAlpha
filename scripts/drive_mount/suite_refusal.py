"""Saves the server refuses: filed under the call id for the tool result
once ``drive-settle`` returns, never failing a close."""

from __future__ import annotations

import json
import os
import subprocess
import time

from checks import Results, case
from fixtures import TOP, USER, Env, as_user, knobs, notes, outcomes, read_file, settle, wait_for

REL = "rtest"
D = f"{TOP}/{REL}"

#: A program that writes ``body`` to ``name`` and says how its close went.
CLOSE = (
    "import errno\n"
    "f = open('{name}', 'w'); f.write({body!r})\n"
    "try:\n"
    "    f.close(); print('closed')\n"
    "except OSError as e:\n"
    "    print('close', errno.errorcode[e.errno])\n"
)


def write_and_close(name: str, body: str, call: str) -> subprocess.CompletedProcess:
    return as_user(f"mkdir -p {D} && cd {D} && python3 -c \"{CLOSE.format(name=name, body=body)}\"",
                   env={"LIVEFS_CALL_ID": call}, check=False, settle=False)


@case("refusal")
def over_quota(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    call = "call-refusal-0001"
    usage = server.admin("GET", "stats")[1]["usage"]
    with knobs(server, quota=usage["bytes"]):
        p = write_and_close("full.txt", "x" * 5000, call)
        t0 = time.perf_counter()
        settled = settle(mount, call)
        took = time.perf_counter() - t0
        filed = outcomes(server, call)
    refused = [o for o in filed if not o["ok"]]
    r.pin("over quota: close() succeeds (the result reports the save)", "closed" in p.stdout,
          p.stdout.strip() + p.stderr[-200:])
    r.pin("over quota: filed under the call id as NOT SAVED by the time drive-settle returns",
          bool(refused) and refused[0]["code"] == "over_quota",
          f"settle {took:.2f}s {settled}; NOT SAVED (the exit status does not show this):\n" + "\n".join(
              f"- {o['path']}: {o.get('error')}" for o in refused), outcomes=filed)
    r.pin("over quota: the refused new file does not stay on this computer",
          bool(wait_for(lambda: not os.path.exists(f"{D}/full.txt"), 3)))


@case("refusal")
def busy_server_is_sent_again_within_the_call(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    call = "call-refusal-busy-1"
    with knobs(server, busy_writes=2):
        p = write_and_close("busy.txt", "later", call)
        settled = settle(mount, call)
        filed = outcomes(server, call)
    r.pin("busy server: the save is sent again and lands by drive-settle, with nothing filed "
          "as NOT SAVED", "closed" in p.stdout and server.content(f"{REL}/busy.txt") == b"later"
          and not [o for o in filed if not o["ok"]] and settled.get("left") == 0,
          p.stdout.strip() + p.stderr[-200:] + f" {settled}", outcomes=filed)


@case("refusal")
def busy_past_the_in_call_retries_lands_later(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    call = "call-refusal-busy-2"
    with knobs(server, busy_writes=6):
        p = write_and_close("busy2.txt", "later", call)
        settled = settle(mount, call)
        told = [n for n in settled.get("notices") or [] if n.get("call") == call and n.get("kind") == "unsaved"]
        told = told or notes(server, call, "unsaved")
        landed = wait_for(lambda: server.content(f"{REL}/busy2.txt") == b"later", 15)
    filed = outcomes(server, call)
    r.pin("busy server past the in-call retries: close() succeeds, drive-settle tells the agent it "
          "is not saved yet, nothing is filed as refused, and it lands later",
          "closed" in p.stdout and bool(told) and bool(landed)
          and not [o for o in filed if not o["ok"]],
          p.stdout.strip() + p.stderr[-200:] + f" {settled}", outcomes=filed)


@case("refusal")
def save_outlasting_the_settle_wait_is_told_then_saved(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    call = "call-refusal-slow"
    with knobs(server, stall_writes=7):
        as_user(f"mkdir -p {D} && cd {D} && echo slow > slow.txt", env={"LIVEFS_CALL_ID": call},
                check=False, settle=False)
        settled = settle(mount, call, "--wait", "1")
        told = [n for n in settled.get("notices") or [] if n.get("call") == call and n.get("kind") == "unsaved"]
    saved = wait_for(lambda: notes(server, call, "saved"), 15)
    r.pin("a save still unanswered past drive-settle's wait is reported not saved yet, and later saved",
          settled.get("left") == 1 and bool(told) and bool(saved)
          and server.content(f"{REL}/slow.txt") == b"slow\n", f"{settled}, saved {bool(saved)}")


@case("refusal")
def save_over_a_head_changed_elsewhere(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    as_user(f"mkdir -p {D} && echo base > {D}/c.txt")
    wait_for(lambda: server.content(f"{REL}/c.txt") == b"base\n", 3)
    call = "call-refusal-0002"
    script = (
        "import errno, sys, time\n"
        "f = open('c.txt', 'r+'); f.seek(0); f.write('mine'); f.flush()\n"
        "sys.stdout.write('ready\\n'); sys.stdout.flush()\n"
        "sys.stdin.readline()\n"
        "try:\n"
        "    f.close(); print('closed')\n"
        "except OSError as e:\n"
        "    print('close', errno.errorcode[e.errno])\n"
    )
    proc = subprocess.Popen(["runuser", "-u", USER, "--", "python3", "-c", script], cwd=D,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                            env={**os.environ, "LIVEFS_CALL_ID": call})
    proc.stdout.readline()
    server.write(f"{REL}/c.txt", b"theirs\n")
    out, _ = proc.communicate("go\n", timeout=30)
    settle(mount, call)
    filed = outcomes(server, call)
    r.pin("conflict: close() succeeds (the result reports the save)", "closed" in out, out.strip())
    r.pin("conflict: filed under the call id by the time drive-settle returns",
          any(o.get("code") == "changed" for o in filed), json.dumps(filed))
    r.pin("conflict: the other save is kept", server.content(f"{REL}/c.txt") == b"theirs\n")
    r.pin("conflict: this computer shows the kept save, not the refused bytes",
          bool(wait_for(lambda: read_file(f"{D}/c.txt") == b"theirs\n", 3)), repr(read_file(f"{D}/c.txt")))


@case("refusal")
def write_past_the_file_cap_fails_at_once(r: Results, env: Env) -> None:
    server, mount = env.server, env.mount
    as_user(f"mkdir -p {D}")
    # The daemon reads the cap at its start.
    try:
        with knobs(server, file_cap=1 << 20, upload_cap=1 << 20):
            mount.stop()
            mount.start({"warm": False}, wait_ready=True)
            p = as_user(
                f"cd {D} && python3 -c \"\n"
                "import errno\n"
                "f = open('huge.bin', 'wb')\n"
                "try:\n"
                "    f.write(b'x' * (2 << 20)); f.flush(); print('wrote')\n"
                "except OSError as e:\n"
                "    print('write', errno.errorcode[e.errno])\n"
                "try:\n"
                "    f.close()\n"
                "except OSError as e:\n"
                "    print('close', errno.errorcode[e.errno])\n\"", check=False)
            time.sleep(1.0)
    finally:
        mount.stop()
        mount.start({"warm": False}, wait_ready=True)
    r.pin("over the largest file a save carries: the write fails with EFBIG, nothing saved",
          "write EFBIG" in p.stdout and server.entry(f"{REL}/huge.bin") is None, p.stdout.strip())
