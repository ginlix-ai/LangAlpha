"""The mount's large-file path against a real server, with a throwaway
computer's mount token (``harness.py --server ... --token ... --suites live
--only <cases>``). The token's computer kind
decides the read route: one that pulls from the store gets ``link``, one
that does not gets ``api``.

Files go under ``mtlive/``; the command call ids below are opened on the
server before the run, and their outcomes collected after it, since only the
server can say what the tool result shows.

Data in the large volume's ``data``: ``live.parquet`` (about 2 GB, columns
including ``g``) and ``v1.bin`` / ``v2.bin`` (256 MiB of random bytes), each
with a ``.sha256`` beside it.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import subprocess
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from checks import Results, case
from fixtures import PRIVATE, TOP, USER, Env, Failure, as_user, drop_caches, log, sh, wait_for

DATA = "/var/tmp/drive_large/data"
PYLIB = "/var/tmp/drive_large/pylib"
DIR = "mtlive"
MiB = 1 << 20
#: Opened on the server before the run, per computer.
CALLS = {"save": "call-mtlive-save", "over": "call-mtlive-over", "room": "call-mtlive-room",
         "append": "call-mtlive-append", "held": "call-mtlive-held", "codeless": "call-mtlive-codeless",
         "full": "call-mtlive-full"}
PARQUET = "live.parquet"
#: What DuckDB answers over ``live.parquet``: count(*), sum(g).
PARQUET_ANSWER = [84000000, 41958000000]


def _sha(name: str) -> str:
    with open(f"{DATA}/{name}.sha256") as f:
        return f.read().split()[0]


def _hwm_mb(pid: int | None) -> float:
    """The daemon's peak resident memory since it started."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024
    except (OSError, TypeError):
        pass
    return -1.0


def _cpu_s(pid: int | None) -> float:
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, TypeError, IndexError):
        return 0.0


def _ops(env: Env) -> dict:
    return (env.mount.status() or {}).get("ops") or {}


def _restart(env: Env, settings: dict | None = None) -> None:
    """A daemon with an empty store, so reads go to the network."""
    env.mount.stop()
    env.mount.start({"warm": False, **(settings or {})}, fresh_store=True, wait_ready=True)


def _route(ops: dict) -> str:
    return "link" if ops.get("link_bytes") else "api" if ops.get("api_bytes") else "none"


def _upload(env: Env, rel: str, local: str) -> dict:
    """A file put on the Drive from elsewhere, through the person's uploads
    routes on the relay: the entry it became."""
    size = os.path.getsize(local)
    sha = _sha(os.path.basename(local))
    status, data = env.server._rest("POST", "uploads", json_body={
        "path": rel, "size": size, "sha256": sha, "route": "relay"})
    begun = json.loads(data or b"{}")
    if status != 200:
        raise AssertionError(f"begin {rel}: {status} {begun}")
    if begun["mode"] == "held":
        return begun["entry"]
    upload, part = begun["upload_id"], begun["part_size"]
    held = {int(p["part_number"]) for p in begun.get("parts_held") or ()}
    with open(local, "rb") as f:
        for n in range(1, begun["part_count"] + 1):
            if n in held:
                continue
            f.seek((n - 1) * part)
            body = f.read(part)
            for _ in range(5):
                status, data = env.server._rest("PUT", f"uploads/{upload}/parts/{n}", body=body)
                if status == 200:
                    break
                time.sleep(2)
            else:
                raise AssertionError(f"part {n} of {rel}: {status} {data[:200]!r}")
    env.server._rest("POST", f"uploads/{upload}/complete")
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        status, data = env.server._rest("GET", f"uploads/{upload}")
        state = json.loads(data or b"{}")
        if state.get("state") == "done":
            return state["entry"]
        if state.get("state") in ("failed", "aborted"):
            raise AssertionError(f"upload {rel}: {state}")
        time.sleep(1.5)
    raise AssertionError(f"upload {rel} never ended")


def _server_sha(env: Env, rel: str) -> str | None:
    return (env.server.entry(rel) or {}).get("sha256")


def _shown(rel: str, size: int, timeout: float = 120) -> bool:
    path = f"{TOP}/{rel}"
    return bool(wait_for(lambda: os.path.exists(path) and os.path.getsize(path) == size, timeout, 0.5))


def _user_py(code: str, env: dict | None = None) -> subprocess.Popen:
    return subprocess.Popen(["runuser", "-u", USER, "--", "env", f"PYTHONPATH={PYLIB}", "HOME=/tmp",
                             *[f"{k}={v}" for k, v in (env or {}).items()], "python3", "-c", code],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def _hash_as_user(path: str, delay: float = 0.0, chunk: int = 4 * MiB) -> subprocess.Popen:
    return _user_py(
        "import hashlib,time\n"
        "h=hashlib.sha256()\n"
        "try:\n"
        f"  with open({path!r},'rb') as f:\n"
        f"    while b:=f.read({chunk}):\n"
        f"      h.update(b); time.sleep({delay})\n"
        "  print(h.hexdigest())\n"
        "except OSError as e:\n"
        "  print('error', e.errno)\n")


def _ranges(path: str, local: str, count: int, size: int, seed: int, lo: int = 0) -> subprocess.Popen:
    """``count`` reads of ``size`` bytes at random offsets from ``lo``, each
    checked against the same range of the local copy: prints the mismatches."""
    return _user_py(
        "import os,random\n"
        f"r=random.Random({seed}); total=os.path.getsize({local!r}); bad=0\n"
        f"a=os.open({path!r},os.O_RDONLY); b=os.open({local!r},os.O_RDONLY)\n"
        f"for _ in range({count}):\n"
        f"  at=r.randrange({lo}, total-{size})\n"
        f"  bad+=os.pread(a,{size},at)!=os.pread(b,{size},at)\n"
        "print(bad)\n")


# --- saves ---------------------------------------------------------------------


@case("live")
def save_in_parts(r: Results, env: Env) -> None:
    """A 2 GB file copied in: saved in parts from the journal, a daemon
    restart partway sending only the parts the store lacks, the server's
    hash the file's own, the daemon's memory small throughout."""
    local, rel = f"{DATA}/{PARQUET}", f"{DIR}/{PARQUET}"
    as_user(f"mkdir -p {TOP}/{DIR}")
    _restart(env)
    t0 = time.monotonic()
    p = as_user(f"cp {local} {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["save"]}, settle=False, check=False)
    copied = time.monotonic() - t0
    r.pin("the 2 GB copy into the mount ends without error", p.returncode == 0, p.stderr[-200:])
    want = _sha(PARQUET)
    peak = {"mb": 0.0}
    done = threading.Event()

    def watch() -> None:
        while not done.wait(1.0):
            peak["mb"] = max(peak["mb"], _hwm_mb(env.mount.pid()))

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    landed = wait_for(lambda: _server_sha(env, rel) == want, 2400, 3.0)
    done.set()
    watcher.join()
    took = time.monotonic() - t0
    ops = _ops(env)
    r.pin("the server holds the file under its own sha256", bool(landed), f"{_server_sha(env, rel)}")
    r.pin("the daemon's memory stayed small while it saved", 0 < peak["mb"] < 400, f"peak {peak['mb']:.0f} MB")
    size = os.path.getsize(local)
    env.report.setdefault("live", {})["save"] = {
        "copy_s": round(copied, 1), "saved_s": round(took, 1), "mb_s": round(size / took / 1e6, 1),
        "daemon_hwm_mb": round(peak["mb"]),
        "ops": {k: v for k, v in ops.items() if k.startswith("upload")}}
    log(f"  save: {json.dumps(env.report['live']['save'])}")


@case("live")
def resume_after_a_crash(r: Results, env: Env) -> None:
    """The daemon killed partway through a save in parts: the next one takes
    the file up from the journal, begins again with the same sha256, and
    sends only the parts the store does not hold."""
    rel = f"{DIR}/resume.bin"
    local = "/var/tmp/resume.bin"
    sh(f"head -c {400 * MiB} /dev/urandom > {local}; chmod 644 {local}", check=True)
    with open(local, "rb") as f:
        want = hashlib.file_digest(f, "sha256").hexdigest()
    _restart(env)
    as_user(f"cp {local} {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["save"]}, settle=False, check=False)
    sent = wait_for(lambda: _ops(env).get("upload_relay_bytes", 0) + _ops(env).get("upload_direct_bytes", 0)
                    >= 128 * MiB, 600, 0.5)
    # A crash, not a stop: a stopped daemon finishes the saves under way.
    sh(["kill", "-9", str(env.mount.pid())])
    time.sleep(1)
    env.mount.stop()
    env.mount.start({"warm": False}, wait_ready=True)
    landed = wait_for(lambda: _server_sha(env, rel) == want, 1200, 2.0)
    ops = _ops(env)
    r.pin("a save cut off by a crash lands after the restart", bool(sent) and bool(landed),
          f"sent before {bool(sent)}, server {_server_sha(env, rel)}")
    r.pin("the restarted save sent only the parts not held", ops.get("upload_parts_held", 0) >= 8,
          f"held {ops.get('upload_parts_held')}, sent {ops.get('upload_relay_bytes', 0) + ops.get('upload_direct_bytes', 0)}")
    os.unlink(local)


@case("live")
def over_allowance_not_saved(r: Results, env: Env) -> None:
    """A 4 GiB file in an allowance already holding 2 GB: the begin is
    refused, filed for the command, nothing kept on the server."""
    rel = f"{DIR}/over.bin"
    p = as_user(f"truncate -s 4G {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["over"]}, check=False)
    settled = env.mount.run("drive-settle", "--call", CALLS["over"])
    # Refused, the file goes back to what the server holds: nothing.
    gone = wait_for(lambda: not os.path.exists(f"{TOP}/{rel}"), 120, 1.0)
    r.pin("a save over the allowance is refused and not kept", p.returncode == 0 and bool(gone) and _server_sha(env, rel) is None,
          f"settled {json.dumps(settled)[:300]}, refused {gone}")
    env.report.setdefault("live", {})["over"] = {"settled": settled, "ops": {
        k: v for k, v in _ops(env).items() if "refus" in k or k.startswith("upload")}}


@case("live")
def no_room_to_keep_it(r: Results, env: Env) -> None:
    """The journal's disk too small for a file written in: the write fails
    with ENOSPC, the disk is not filled, and the command is told how much
    room it needed."""
    journal = f"{PRIVATE}/unsaved"
    env.mount.stop()
    os.makedirs(journal, exist_ok=True)
    sh(["mount", "-t", "tmpfs", "-o", "size=600m", "tmpfs", journal], check=True)
    try:
        env.mount.start({"warm": False}, wait_ready=True)
        peak = {"used": 0}
        stop = threading.Event()

        def watch() -> None:
            while not stop.wait(0.05):
                st = os.statvfs(journal)
                peak["used"] = max(peak["used"], (st.f_blocks - st.f_bfree) * st.f_frsize)

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        p = as_user(f"dd if=/dev/zero of={TOP}/{DIR}/room.bin bs=1M count=1024 status=none",
                    env={"LIVEFS_CALL_ID": CALLS["room"]}, check=False)
        stop.set()
        watcher.join()
        settled = env.mount.run("drive-settle", "--call", CALLS["room"])
        r.pin("the write fails with no space left", p.returncode != 0 and "No space" in p.stderr, p.stderr[-200:])
        r.pin("the journal's disk was never filled", peak["used"] < 600 * MiB - 16 * MiB,
              f"peak {peak['used'] / 1e6:.0f} MB of {600 * MiB / 1e6:.0f}")
        # The reason goes to the server as a notice and reaches the tool
        # result from there (collected on the host after the run).
        told = sh(["grep", "-c", "MB more free", f"{PRIVATE}/notices.jsonl"]).stdout.strip()
        r.pin("the command is told how much room it needed", told not in ("", "0") or "MB more free" in json.dumps(settled),
              f"notices kept {told}; collect {CALLS['room']} on the server")
        env.report.setdefault("live", {})["no_room"] = {"settled": settled, "peak": peak["used"]}
    finally:
        env.mount.stop()
        sh(["umount", "-l", journal])
        env.mount.start({"warm": False}, wait_ready=True)


@case("live")
def append_to_a_large_file(r: Results, env: Env) -> None:
    """``>>`` to a 256 MiB file on the Drive: the old bytes go to disk, not
    memory, and the save carries old bytes and new."""
    rel = f"{DIR}/app.bin"
    _upload(env, rel, f"{DATA}/v1.bin")
    _restart(env)  # a fresh peak
    r.pin("the file shows on the mount", _shown(rel, os.path.getsize(f"{DATA}/v1.bin")))
    with open(f"{DATA}/v1.bin", "rb") as f:
        want = hashlib.file_digest(f, "sha256")
    want.update(b"tail\n")
    p = as_user(f"echo tail >> {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["append"]}, settle=False, check=False)
    landed = wait_for(lambda: _server_sha(env, rel) == want.hexdigest(), 900, 2.0)
    hwm = _hwm_mb(env.mount.pid())
    r.pin("an append to a large file saves old bytes and new", p.returncode == 0 and bool(landed),
          f"{p.stderr[-200:]} server {_server_sha(env, rel)}")
    r.pin("opening it for writing did not pull it into memory", 0 < hwm < 200, f"peak {hwm:.0f} MB")
    env.report.setdefault("live", {})["append_hwm_mb"] = round(hwm)


# --- reads ---------------------------------------------------------------------


def _read_checks(r: Results, env: Env, label: str) -> None:
    """The 2 GB file by random ranges and whole, then DuckDB over it."""
    local, path = f"{DATA}/{PARQUET}", f"{TOP}/{DIR}/{PARQUET}"
    pid = env.mount.pid()
    cpu0 = _cpu_s(pid)
    t0 = time.monotonic()
    bad = _ranges(path, local, 200, MiB, seed=11).communicate(timeout=1800)[0].strip()
    ranged = time.monotonic() - t0
    r.pin(f"{label}: 200 random 1 MiB ranges match the file", bad == "0", bad[-200:])
    drop_caches()
    t0 = time.monotonic()
    out = _hash_as_user(path).communicate(timeout=3600)[0].strip()
    whole = time.monotonic() - t0
    size = os.path.getsize(local)
    r.pin(f"{label}: the 2 GB file read whole is right", out == _sha(PARQUET), f"{out[:24]} in {whole:.0f}s")
    drop_caches()
    t0 = time.monotonic()
    duck = _user_py(
        "import duckdb,json\n"
        f"print(json.dumps(list(duckdb.connect().execute(\"SELECT count(*), sum(g) FROM read_parquet('{path}')\")"
        ".fetchone())))\n").communicate(timeout=1800)[0].strip()
    ducked = time.monotonic() - t0
    try:
        answer = json.loads(duck.splitlines()[-1])
    except (ValueError, IndexError):
        answer = duck[-300:]
    r.pin(f"{label}: DuckDB over the file answers right", answer == PARQUET_ANSWER, str(answer))
    ops = _ops(env)
    r.pin(f"{label}: the bytes came by the {label} route", _route(ops) == label, f"route {_route(ops)}")
    env.report.setdefault("live", {})[label] = {
        "route": _route(ops), "ranges_s": round(ranged, 1), "whole_s": round(whole, 1),
        "whole_mb_s": round(size / whole / 1e6, 1), "duckdb_s": round(ducked, 1),
        "daemon_cpu_s": round(_cpu_s(pid) - cpu0, 1), "daemon_hwm_mb": round(_hwm_mb(pid)),
        "ops": {k: v for k, v in ops.items() if k.startswith(("link", "api", "source", "fetch_", "block_"))}}
    log(f"  {label}: {json.dumps(env.report['live'][label])}")


@case("live")
def api_reads(r: Results, env: Env) -> None:
    """On a computer the server answers ``api``: the read checks."""
    _restart(env)
    _read_checks(r, env, "api")


@case("live")
def replaced_while_reading(r: Results, env: Env) -> None:
    """A large file replaced from elsewhere while a reader is partway: it,
    and a reader starting meanwhile, get the old version whole; the new one
    shows once they are done."""
    rel = f"{DIR}/ver.bin"
    _upload(env, f"{DIR}/v2-stage.bin", f"{DATA}/v2.bin")  # so the replace is held: instant
    _upload(env, rel, f"{DATA}/v1.bin")
    _upload(env, f"{DIR}/v1keep.bin", f"{DATA}/v1.bin")  # for the link run's late reader
    _restart(env)
    path = f"{TOP}/{rel}"
    r.pin("v1 shows on the mount", _shown(rel, os.path.getsize(f"{DATA}/v1.bin")))
    drop_caches()
    slow = _hash_as_user(path, delay=0.4)  # 64 chunks: about 26 s
    time.sleep(3)
    replaced_at = time.monotonic()
    _upload(env, rel, f"{DATA}/v2.bin")
    r.pin("the server took v2 at once", _server_sha(env, rel) == _sha("v2.bin"),
          f"{time.monotonic() - replaced_at:.1f}s")
    seen = wait_for(lambda: _ops(env).get("pin_held_read", 0) > 0, 30, 0.5)
    joined = _hash_as_user(path)
    first, second = slow.communicate(timeout=600)[0].strip(), joined.communicate(timeout=600)[0].strip()
    r.pin("the reader partway through got v1 whole", first == _sha("v1.bin"), first[:40])
    r.pin("a reader starting meanwhile got v1 too", second == _sha("v1.bin"), f"{second[:40]} held {bool(seen)}")
    t0 = time.monotonic()
    shown = wait_for(lambda: _hash_as_user(path).communicate()[0].strip() == _sha("v2.bin"), 400, 5.0)
    r.pin("once they were done v2 shows", bool(shown), f"after {time.monotonic() - t0:.0f}s; "
          + json.dumps({k: v for k, v in _ops(env).items() if k.startswith("pin_")}))


@case("live")
def append_while_a_reader_holds_the_old_version(r: Results, env: Env) -> None:
    """A reader partway through v1 when v2 arrives, and another process
    appending meanwhile: the append edits the version the path shows (v1),
    so its save is refused as changed and v2 on the server stays whole."""
    rel = f"{DIR}/held.bin"
    _upload(env, rel, f"{DATA}/v1.bin")
    _restart(env)
    path = f"{TOP}/{rel}"
    r.pin("held: v1 shows on the mount", _shown(rel, os.path.getsize(f"{DATA}/v1.bin")))
    drop_caches()
    slow = _hash_as_user(path, delay=0.4)
    time.sleep(3)
    _upload(env, rel, f"{DATA}/v2.bin")
    held = wait_for(lambda: _ops(env).get("pin_held_read", 0) > 0, 30, 0.5)
    p = as_user(f"echo tail >> {path}", env={"LIVEFS_CALL_ID": CALLS["held"]}, check=False)
    settled = env.mount.run("drive-settle", "--call", CALLS["held"])
    first = slow.communicate(timeout=600)[0].strip()
    r.pin("held: the reader partway through got v1 whole", first == _sha("v1.bin") and bool(held), first[:40])
    r.pin("held: the server still holds v2, not v1 with the append", _server_sha(env, rel) == _sha("v2.bin"),
          f"server {_server_sha(env, rel)}; append {p.returncode} {p.stderr[-200:]}")
    env.report.setdefault("live", {})["held_append"] = {"settled": settled, "append_rc": p.returncode}
    log(f"  held: collect {CALLS['held']} on the server; settled {json.dumps(settled)[:300]}")


@case("live")
def link_reads(r: Results, env: Env) -> None:
    """On a computer the server answers ``link``: the read checks."""
    _restart(env)
    _read_checks(r, env, "link")
    log("  live: done")


# --- round 4: what changed -------------------------------------------------------


def _random_file(path: str, mib: int) -> str:
    sh(f"head -c {mib * MiB} /dev/urandom > {path}; chmod 644 {path}", check=True)
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


@case("live")
def a_fresh_2gb_save(r: Results, env: Env) -> None:
    """2 GiB of bytes the Drive has never held, copied in: every part sent,
    the server's hash the file's own, the daemon's memory small."""
    local, rel = "/var/tmp/fresh.bin", f"{DIR}/fresh.bin"
    want = _random_file(local, 2048)
    _restart(env)
    t0 = time.monotonic()
    as_user(f"cp {local} {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["save"]}, settle=False, check=False)
    peak = {"mb": 0.0}
    done = threading.Event()

    def watch() -> None:
        while not done.wait(1.0):
            peak["mb"] = max(peak["mb"], _hwm_mb(env.mount.pid()))

    threading.Thread(target=watch, daemon=True).start()
    landed = wait_for(lambda: _server_sha(env, rel) == want, 2400, 3.0)
    done.set()
    took = time.monotonic() - t0
    ops = _ops(env)
    sent = ops.get("upload_relay_bytes", 0) + ops.get("upload_direct_bytes", 0)
    r.pin("fresh: the server holds the 2 GiB file under its own sha256", bool(landed))
    r.pin("fresh: every part was sent, and memory stayed small", sent >= 2048 * MiB and 0 < peak["mb"] < 400,
          f"sent {sent / MiB:.0f} MiB in {took:.0f}s, peak {peak['mb']:.0f} MB")
    env.report.setdefault("live", {})["fresh"] = {"saved_s": round(took, 1), "sent_mib": round(sent / MiB),
                                                  "daemon_hwm_mb": round(peak["mb"]),
                                                  "ops": {k: v for k, v in ops.items() if k.startswith("upload")}}
    log(f"  fresh: {json.dumps(env.report['live']['fresh'])}")
    os.unlink(local)


@case("live")
def a_take_back_keeps_a_newer_edit(r: Results, env: Env) -> None:
    """A new file saved in parts is renamed while the server reads it back,
    then the old name is edited from elsewhere: the edit survives whatever
    the take-back does, and the file is saved at its new name."""
    local, rel, moved = "/var/tmp/m1.bin", f"{DIR}/m1.bin", f"{DIR}/m1moved.bin"
    want = _random_file(local, 400)
    _restart(env)
    as_user(f"cp {local} {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["save"]}, settle=False, check=False)
    completed = wait_for(lambda: _ops(env).get("upload_completed", 0) >= 1, 900, 0.2)
    mv = as_user(f"mv {TOP}/{rel} {TOP}/{moved}", settle=False, check=False)
    landed = wait_for(lambda: _server_sha(env, rel) == want, 120, 0.1)
    edit = b"the person's edit\n"
    env.server.write(rel, edit)
    at_new = wait_for(lambda: _server_sha(env, moved) == want, 900, 2.0)
    time.sleep(5)
    ops = _ops(env)
    r.pin("m1: the rename during the read-back answers", bool(completed) and mv.returncode == 0, mv.stderr[-200:])
    r.pin("m1: the edit at the old name survives", _server_sha(env, rel) == hashlib.sha256(edit).hexdigest(),
          f"landed first {bool(landed)}, server {_server_sha(env, rel)}")
    r.pin("m1: the file is saved at its new name", bool(at_new), str(_server_sha(env, moved)))
    env.report.setdefault("live", {})["m1"] = {"landed_before_edit": bool(landed), "ops": {
        k: v for k, v in ops.items() if k.startswith(("save_parts", "upload_"))}}
    os.unlink(local)


@case("live")
def a_reader_keeps_the_file_it_opened(r: Results, env: Env) -> None:
    """A reader partway through a large file while it is replaced by a small
    one, or deleted and made again: it reads what it began on."""
    v1 = _sha("v1.bin")
    for how in ("small", "recreated"):
        rel = f"{DIR}/m2-{how}.bin"
        _upload(env, rel, f"{DATA}/v1.bin")
        _restart(env)
        path = f"{TOP}/{rel}"
        r.pin(f"m2 {how}: v1 shows", _shown(rel, os.path.getsize(f"{DATA}/v1.bin")))
        drop_caches()
        slow = _hash_as_user(path, delay=0.3)
        time.sleep(3)
        if how == "recreated":
            env.server._rest("POST", "trash", json_body={"path": rel})
        env.server.write(rel, f"a small {how} file\n".encode())
        got = slow.communicate(timeout=900)[0].strip()
        r.pin(f"m2 {how}: the reader got v1 whole", got == v1, got[:40])


@case("live")
def a_short_write_on_a_full_disk(r: Results, env: Env) -> None:
    """The journal's disk fills from outside while a program writes: each
    write reports what reached the disk, the next fails with ENOSPC, and the
    file holds exactly the bytes the program was told were written."""
    journal = f"{PRIVATE}/unsaved"
    src = "/var/tmp/m3src.bin"
    _random_file(src, 64)
    env.mount.stop()
    os.makedirs(journal, exist_ok=True)
    sh(["mount", "-t", "tmpfs", "-o", "size=600m", "tmpfs", journal], check=True)
    go = "/tmp/m3go"
    if os.path.exists(go):
        os.unlink(go)
    rel = f"{DIR}/m3.bin"
    try:
        env.mount.start({"warm": False}, wait_ready=True)
        writer = _user_py(
            "import os,time\n"
            f"src=open({src!r},'rb').read(); fd=os.open({TOP + '/' + rel!r}, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o644)\n"
            "done=0; err=None\n"
            "while done < len(src):\n"
            f"  if done == 3*{MiB}:\n"
            f"    while not os.path.exists({go!r}): time.sleep(0.05)\n"
            "  try:\n"
            f"    n=os.write(fd, src[done:done+{MiB}])\n"
            "  except OSError as e:\n"
            "    err=e.errno; break\n"
            "  done+=n\n"
            "print(done, err)\n",
            env={"LIVEFS_CALL_ID": CALLS["full"]})
        wait_for(lambda: os.path.exists(f"{TOP}/{rel}") and os.path.getsize(f"{TOP}/{rel}") >= 3 * MiB, 60, 0.05)
        st = os.statvfs(journal)
        free = st.f_bavail * st.f_frsize
        fill = free - (2 * MiB + MiB // 2 + 777)
        sh(["fallocate", "-l", str(fill), f"{journal}/filler"], check=True)
        open(go, "w").close()
        out = writer.communicate(timeout=120)[0].strip().split()
        done, err = int(out[0]), out[1]
        with open(f"{TOP}/{rel}", "rb") as f:
            held = f.read()
        with open(src, "rb") as f:
            want = f.read()[:done]
        r.pin("m3: the program saw ENOSPC after a short count", err == "28" and done % MiB != 0,
              f"told {done} bytes written, then errno {err}")
        r.pin("m3: the file holds exactly the bytes reported written", held == want,
              f"file {len(held)} bytes, reported {done}, same {held[: len(want)] == want}")
        os.unlink(f"{journal}/filler")
        settled = env.mount.run("drive-settle", "--call", CALLS["full"])
        env.report.setdefault("live", {})["m3"] = {"done": done, "err": err, "settled": settled}
    finally:
        env.mount.stop()
        sh(["umount", "-l", journal])
        env.mount.start({"warm": False}, wait_ready=True)
        os.unlink(src)


class _Codeless(BaseHTTPRequestHandler):
    """Passes every request to the server but the mount's saves, which it
    answers 404 with no Drive code while ``refuse`` is set (a proxy, or a
    route not there)."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _pass(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        if self.server.refuse and self.command == "PUT" and urllib.parse.urlsplit(self.path).path.endswith("/write"):
            self.server.refused += 1
            page = b"<html>not found</html>"
            self.send_response(404)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return
        up = http.client.HTTPConnection(self.server.upstream[0], self.server.upstream[1], timeout=300)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection")}
        up.request(self.command, self.path, body=body, headers=headers)
        resp = up.getresponse()
        data = resp.read()
        up.close()
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_PUT = do_POST = do_DELETE = _pass


@case("live")
def a_save_answered_by_no_drive_handler(r: Results, env: Env) -> None:
    """Saves answered 404 with no Drive code: the bytes are kept and the
    command is told NOT SAVED YET with that reason; once the server answers,
    the file is saved."""
    up = urllib.parse.urlsplit(env.mount.base_url)
    proxy = ThreadingHTTPServer(("127.0.0.1", 0), _Codeless)
    proxy.upstream, proxy.refuse, proxy.refused = (up.hostname, up.port), True, 0
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    real = env.mount.base_url
    rel, body = f"{DIR}/m4.txt", b"kept through a codeless 404"
    try:
        env.mount.stop()
        env.mount.base_url = f"http://127.0.0.1:{proxy.server_address[1]}"
        env.mount.start({"warm": False}, wait_ready=True)
        p = as_user(f"printf %s '{body.decode()}' > {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["codeless"]},
                    settle=False, check=False)
        settled = env.mount.run("drive-settle", "--call", CALLS["codeless"])
        with open(f"{TOP}/{rel}", "rb") as f:
            shown = f.read()
        told = json.dumps(settled)
        r.pin("m4: the write ends and the bytes are kept", p.returncode == 0 and shown == body and proxy.refused > 0,
              f"refused {proxy.refused}, shown {shown[:40]!r}")
        r.pin("m4: the command is told NOT SAVED YET with the reason",
              settled.get("left", 0) >= 1 and "404" in told, told[:400])
        proxy.refuse = False
        landed = wait_for(lambda: _server_sha(env, rel) == hashlib.sha256(body).hexdigest(), 180, 1.0)
        r.pin("m4: once the server answers, the file is saved", bool(landed))
        env.report.setdefault("live", {})["m4"] = {"refused": proxy.refused, "settled": settled}
    finally:
        env.mount.stop()
        env.mount.base_url = real
        proxy.shutdown()
        env.mount.start({"warm": False}, wait_ready=True)


@case("live")
def descriptors_while_many_large_files_are_read(r: Results, env: Env) -> None:
    """The descriptor limit the daemon runs with, how many large files can
    be partway read at once, and what a read meets past that (measured under
    a lowered limit, as the real one is out of reach)."""
    _restart(env)
    pid = env.mount.pid()
    limits = open(f"/proc/{pid}/limits").read()
    soft = next(line.split()[3] for line in limits.splitlines() if line.startswith("Max open files"))
    base = len(os.listdir(f"/proc/{pid}/fd"))
    count = 48
    for i in range(count):
        env.server.write(f"{DIR}/fd/f{i:03d}.bin", os.urandom(5 * MiB))
    shown = wait_for(lambda: os.path.exists(f"{TOP}/{DIR}/fd/f{count - 1:03d}.bin"), 120, 0.5)
    lowered = base + 24
    sh(["prlimit", f"--pid={pid}", f"--nofile={lowered}:{lowered}"], check=True)
    reader = _user_py(
        "import os,time,json\n"
        "out=[]; fds=[]\n"
        f"for i in range({count}):\n"
        f"  fd=os.open('{TOP}/{DIR}/fd/f%03d.bin' % i, os.O_RDONLY); fds.append(fd)\n"
        "  try:\n"
        f"    out.append(len(os.pread(fd, {MiB}, 0)))\n"
        "  except OSError as e:\n"
        "    out.append(-e.errno)\n"
        "print(json.dumps(out))\n"
        "time.sleep(3)\n")
    got = json.loads(reader.communicate(timeout=600)[0].strip().splitlines()[-1])
    during = len(os.listdir(f"/proc/{pid}/fd"))
    alive = os.path.exists(f"/proc/{pid}")
    sh(["prlimit", f"--pid={pid}", f"--nofile={soft}:{soft}"])
    again = _hash_as_user(f"{TOP}/{DIR}/fd/f000.bin").communicate(timeout=300)[0].strip()
    ok = sum(1 for n in got if n == MiB)
    errors = sorted({n for n in got if n < 0})
    r.pin("fd: the daemon's limit is read", soft.isdigit() or soft == "unlimited", soft)
    r.pin("fd: past the lowered limit a read gets an error or a whole MiB, and the daemon lives",
          bool(shown) and alive and all(n in (MiB,) or n < 0 for n in got), f"{ok} read, errors {errors}")
    r.pin("fd: once the limit is back, the files read whole", len(again) == 64, again[:40])
    env.report.setdefault("live", {})["fd"] = {"soft_limit": soft, "baseline_fds": base, "lowered": lowered,
                                               "read_ok": ok, "errors": errors, "fds_during": during}
    log(f"  fd: {json.dumps(env.report['live']['fd'])}")


@case("live")
def a_reader_served_from_the_page_cache_past_the_idle_time(r: Results, env: Env) -> None:
    """A reader whose reads the kernel answers from its cache for longer
    than a hold's idle time, the file replaced meanwhile: when its reads
    reach the daemon again they still get the version it began on."""
    rel = f"{DIR}/pc.bin"
    _upload(env, rel, f"{DATA}/v1.bin")
    _restart(env)
    path = f"{TOP}/{rel}"
    r.pin("pc: v1 shows", _shown(rel, os.path.getsize(f"{DATA}/v1.bin")))
    drop_caches()
    half = 128 * MiB
    reader = _user_py(
        "import hashlib,os,time,json\n"
        f"fd=os.open({path!r}, os.O_RDONLY); h=hashlib.sha256(); at=0\n"
        f"while at < {half}:\n"
        f"  b=os.pread(fd, 4<<20, at); h.update(b); at+=len(b)\n"
        "print('half', flush=True)\n"
        "t=time.monotonic(); n=0\n"
        "while time.monotonic()-t < 150:\n"
        f"  os.pread(fd, 4<<20, (n*(4<<20)) % {half}); n+=1; time.sleep(0.01)\n"
        "while True:\n"
        "  b=os.pread(fd, 4<<20, at)\n"
        "  if not b: break\n"
        "  h.update(b); at+=len(b)\n"
        "print(h.hexdigest(), at)\n")
    first = reader.stdout.readline().strip()
    ops0 = _ops(env).get("read", 0)
    _upload(env, rel, f"{DATA}/v2.bin")
    time.sleep(140)
    quiet = _ops(env).get("read", 0) - ops0
    out = reader.communicate(timeout=900)[0].strip().split()
    r.pin("pc: the reader read half, then went on from the kernel's cache", first == "half" and quiet < 8,
          f"{quiet} reads reached the daemon in 140 s")
    r.pin("pc: once its reads reach the daemon again they get v1", out and out[0] == _sha("v1.bin"),
          " ".join(out)[:80] + "; " + json.dumps({k: v for k, v in _ops(env).items() if k.startswith("pin_")}))


@case("live")
def one_dropped_put_does_not_send_the_rest_through_the_server(r: Results, env: Env) -> None:
    """A save straight to the store with its connections to the store cut
    once partway: the parts cut go again, and the rest still goes straight
    to the store."""
    local, rel = "/var/tmp/blip.bin", f"{DIR}/blip.bin"
    want = _random_file(local, 600)
    _restart(env)
    as_user(f"cp {local} {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["save"]}, settle=False, check=False)
    started = wait_for(lambda: _ops(env).get("upload_direct_bytes", 0) >= 128 * MiB, 600, 0.2)
    if not started:
        raise Failure(f"no part went straight to the store: {_ops(env)}")
    # Every connection to the store reset for a moment: the PUTs under way fail once.
    rule = ["OUTPUT", "-p", "tcp", "--dport", "443", "-j", "REJECT", "--reject-with", "tcp-reset"]
    cut = sh(["iptables", "-I", *rule])
    time.sleep(1.5)
    sh(["iptables", "-D", *rule])
    landed = wait_for(lambda: _server_sha(env, rel) == want, 1200, 2.0)
    ops = _ops(env)
    relay, direct = ops.get("upload_relay_bytes", 0), ops.get("upload_direct_bytes", 0)
    r.pin("blip: the file lands", bool(landed))
    failed = sum(v for k, v in ops.items() if k.startswith("upload_put_"))
    r.pin("blip: the cut failed PUTs under way", cut.returncode == 0 and failed > 0,
          f"iptables {cut.returncode} {cut.stderr[-120:]}; failed PUTs {failed}")
    r.pin("blip: at most the parts cut went through the server", relay <= 4 * 16 * MiB,
          f"direct {direct / MiB:.0f} MiB, relay {relay / MiB:.0f} MiB")
    env.report.setdefault("live", {})["blip"] = {"direct_mib": round(direct / MiB), "relay_mib": round(relay / MiB),
                                                 "ops": {k: v for k, v in ops.items() if k.startswith("upload_")}}
    os.unlink(local)


@case("live")
def nine_large_saves_beside_the_browsers_uploads(r: Results, env: Env) -> None:
    """Nine files too large for one request copied in at once while the
    person's browser holds five uploads open: of the person's eight, the
    daemon gets three, and none of the nine is dropped."""
    held = []
    for i in range(5):
        status, data = env.server._rest("POST", "uploads", json_body={
            "path": f"{DIR}/browser-{i}.bin", "size": 64 * MiB, "route": "relay"})
        held.append(json.loads(data).get("upload_id"))
    files = {}
    for i in range(9):
        local = f"/var/tmp/nine-{i}.bin"
        files[f"{DIR}/nine-{i}.bin"] = (local, _random_file(local, 60))
    _restart(env)
    as_user(f"mkdir -p {TOP}/{DIR}")
    for rel, (local, _) in files.items():
        as_user(f"cp {local} {TOP}/{rel}", env={"LIVEFS_CALL_ID": CALLS["save"]}, settle=False, check=False)
    landed = wait_for(lambda: all(_server_sha(env, rel) == want for rel, (_, want) in files.items()), 1200, 3.0)
    ops = _ops(env)
    missing = [rel for rel, (_, want) in files.items() if _server_sha(env, rel) != want]
    for upload in held:
        env.server._rest("DELETE", f"uploads/{upload}")
    r.pin("nine: every file lands beside the browser's five uploads", bool(landed), f"missing {missing}")
    r.pin("nine: the server said too many uploads and the saves waited, none dropped",
          ops.get("save_parts_waiting", 0) > 0 and not missing,
          json.dumps({k: v for k, v in ops.items() if k.startswith(("save_parts", "upload_"))}))
    for local, _ in files.values():
        os.unlink(local)
