"""The block cache's guarantees, against the fake server and blob store
(``harness.py --suites blocks``, which starts the blob store beside it):

- a 5 GB file read end to end through a store on a 3 GiB disk with a budget
  near 500 MB: right bytes, the disk never filled, the budget never passed;
- a read ahead cancelled when its reader stops, its claim given back;
- a fetch cut off part way leaves nothing behind;
- a file replaced while a reader is partway through: that reader, and one
  starting meanwhile, see only the old version; once they are done the new
  one shows; a version the server no longer holds fails, never turns into
  another;
- a link running out mid-read is asked for again; the server's own route
  serves the same bytes.

Data, random bytes with a ``.sha256`` beside each, in the large volume's
``data``: ``big.bin`` (5 GiB), ``v1.bin`` and ``v2.bin`` (256 MiB each)::

    for f in big.bin:5120 v1.bin:256 v2.bin:256; do n=${f%%:*}
      head -c ${f#*:}M /dev/urandom > $n; sha256sum $n | cut -c1-64 > $n.sha256; done
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import time

from checks import Results, case
from fixtures import BLOB, PRIVATE, TOP, USER, Env, drop_caches, log, sh, wait_for
from fixtures import blob as _blob

DATA = "/var/tmp/drive_large/data"
GiB = 1 << 30
MiB = 1 << 20


def _sha(name: str) -> str:
    with open(f"{DATA}/{name}.sha256") as f:
        return f.read().split()[0]


def _hash_as_user(path: str, delay: float = 0.0, chunk: int = 4 * MiB) -> subprocess.Popen:
    """A reader hashing ``path`` as the computer's user, ``delay`` seconds
    between chunks; prints the digest, or the error."""
    code = (
        "import hashlib,sys,time\n"
        f"h=hashlib.sha256()\n"
        "try:\n"
        f"  with open({path!r},'rb') as f:\n"
        f"    while b:=f.read({chunk}):\n"
        f"      h.update(b); time.sleep({delay})\n"
        "  print(h.hexdigest())\n"
        "except OSError as e:\n"
        "  print('error', e.errno)\n"
    )
    return subprocess.Popen(["runuser", "-u", USER, "--", "python3", "-c", code],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def _allocated(folder: str) -> int:
    """Bytes the disk gives a folder's files (a sparse file's holes are free)."""
    return int(sh(["du", "-sk", folder], check=True).stdout.split()[0]) * 1024


def _binaries_allocated(objects: str) -> int:
    """Bytes the disk gives the large files' blocks: every partial file, and
    whole files past what a text can be. The thousands of small texts the
    start fetches are left out, each rounded up to a disk block."""
    total = 0
    for top, _, names in os.walk(objects):
        partial = os.path.basename(top) == "partial"
        for name in names:
            st = os.stat(os.path.join(top, name))
            if partial or st.st_size > MiB:
                total += st.st_blocks * 512
    return total


def _restart(env: Env, settings: dict) -> None:
    env.mount.stop()
    sh(["sh", "-c", f"rm -rf {PRIVATE}/store/*"])
    env.mount.start({"warm": False, **settings}, wait_ready=True)


def _ops(env: Env) -> dict:
    return (env.mount.status() or {}).get("ops") or {}


def _blocks(env: Env) -> dict:
    """The cache's binaries held (``held``, what the budget counts), all it
    holds with text (``bytes``, what its disk should show) and claimed, as
    its status says."""
    store = (env.mount.status() or {}).get("store") or {}
    return {"held": store.get("binaries"), "bytes": store.get("bytes"), "claimed": store.get("claimed"),
            "budget": store.get("budget")}


def _import(env: Env, path: str, name: str) -> None:
    env.server.admin("GET", "import", {"path": path, "file": f"{DATA}/{name}"})


def _links(env: Env, on: bool, life: int = 900, told: int = 0) -> None:
    """Reads by link on the blob store (``on``), else through the server."""
    env.server.admin("POST", "limits", body=json.dumps(
        {"blob_base": BLOB["plain"] if on else "", "link_life": life, "link_told": told}).encode())


@case("blocks")
def big_file_small_disk(r: Results, env: Env) -> None:
    """5 GiB through a 3 GiB store disk with a 500 MB budget."""
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0})
    _links(env, True)
    _import(env, "blocks/big.bin", "big.bin")
    store = f"{PRIVATE}/store"
    env.mount.stop()
    sh(["sh", "-c", f"rm -rf {store}; mkdir -p {store}"])
    sh(["mount", "-t", "tmpfs", "-o", "size=3g", "tmpfs", store], check=True)
    try:
        env.mount.start({"warm": False, "binary_budget": 500 * 1000 * 1000}, wait_ready=True)
        wait_for(lambda: os.path.exists(f"{TOP}/blocks/big.bin"), 30, 0.2)
        drop_caches()
        peak = {"used": 0, "held": 0, "claimed": 0, "counted": 0}
        stop = threading.Event()

        def watch() -> None:
            while not stop.wait(0.2):
                st = os.statvfs(store)
                peak["used"] = max(peak["used"], (st.f_blocks - st.f_bfree) * st.f_frsize)
                b = _blocks(env)
                peak["held"] = max(peak["held"], b.get("held") or 0)
                peak["claimed"] = max(peak["claimed"], b.get("claimed") or 0)
                peak["counted"] = max(peak["counted"], (b.get("held") or 0) + (b.get("claimed") or 0))

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        t0 = time.monotonic()
        reader = _hash_as_user(f"{TOP}/blocks/big.bin")
        out = reader.communicate(timeout=1800)[0].strip()
        took = time.monotonic() - t0
        stop.set()
        watcher.join()
        ops = _ops(env)
        r.pin("5 GiB read whole through a 3 GiB disk", out == _sha("big.bin"),
              f"{out[:16]} in {took:.0f}s ({5 * GiB / took / 1e6:.0f} MB/s)")
        r.pin("the store disk never filled", peak["used"] < 3 * GiB - 64 * MiB,
              f"peak {peak['used'] / 1e6:.0f} MB of {3 * GiB / 1e6:.0f}")
        # A fetch's bytes count as held a moment before its claim is given
        # back, so a status read may see one request (8 MiB) twice.
        r.pin("held and claimed together kept to the budget", peak["counted"] <= 500 * 1000 * 1000 + 8 * MiB,
              f"peak {peak['counted'] / 1e6:.0f} MB (held {peak['held'] / 1e6:.0f}, claimed {peak['claimed'] / 1e6:.0f})")
        r.pin("blocks were evicted to make room", ops.get("block_evicted", 0) > 0,
              f"{ops.get('block_evicted')} evicted")
        b = _blocks(env)
        r.pin("no claim left over", b.get("claimed") == 0, json.dumps(b))
        env.report.setdefault("blocks", {})["big"] = {"seconds": took, **peak, "ops": {
            k: v for k, v in ops.items() if k.startswith(("block_", "fetch_", "link", "api", "source"))}}
    finally:
        env.mount.stop()
        sh(["umount", "-l", store])
        env.mount.start({"warm": False}, wait_ready=True)


@case("blocks")
def ahead_cancelled(r: Results, env: Env) -> None:
    """A reader that stops early: what was fetched ahead of it and not
    started is cancelled, a fetch under way stops, and claims go back."""
    # Slow enough that a fetch ahead (8 MiB) is still under way when the
    # reader stops: 3 MB/s a connection.
    _blob("POST", "/admin/net", {"ttfb": 0.03, "rate": 3e6, "connect": 0.0})
    _links(env, True)
    _import(env, "blocks/big.bin", "big.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    wait_for(lambda: os.path.exists(f"{TOP}/blocks/big.bin"), 30, 0.2)
    drop_caches()
    sh(["runuser", "-u", USER, "--", "sh", "-c", f"head -c {64 * MiB} {TOP}/blocks/big.bin > /dev/null"])
    time.sleep(6)
    ops = _ops(env)
    b = _blocks(env)
    r.pin("read-ahead cancelled when its reader stopped", ops.get("fetch_cancelled", 0) > 0,
          f"{ops.get('fetch_cancelled')} cancelled, {ops.get('fetch_ahead')} ahead")
    r.pin("claims all given back", b.get("claimed") == 0, json.dumps(b))
    held = b.get("held") or 0
    used = _binaries_allocated(f"{PRIVATE}/store/objects")
    r.pin("what the disk holds is what is counted", abs(used - held) <= 8 * MiB,
          f"disk {used / 1e6:.0f} MB, counted {held / 1e6:.0f} MB")
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0})


@case("blocks")
def cut_fetch_leaves_nothing(r: Results, env: Env) -> None:
    """Fetches cut off part way: their partial bytes punched out, the read
    sent again, the right bytes in the end."""
    _links(env, True)
    _import(env, "blocks/v1.bin", "v1.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    wait_for(lambda: os.path.exists(f"{TOP}/blocks/v1.bin"), 30, 0.2)
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0, "cut_after": 3 * MiB, "cuts": 6})
    drop_caches()
    out = _hash_as_user(f"{TOP}/blocks/v1.bin").communicate(timeout=600)[0].strip()
    stats = _blob("GET", "/admin/stats")
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0, "cut_after": 0, "cuts": 0})
    again = _hash_as_user(f"{TOP}/blocks/v1.bin").communicate(timeout=600)[0].strip()
    b = _blocks(env)
    used = _binaries_allocated(f"{PRIVATE}/store/objects")
    r.pin("fetches were cut", stats.get("cut", 0) > 0, f"{stats.get('cut')} cut")
    r.pin("the file read right after cuts", out == _sha("v1.bin") or out.startswith("error"),
          out[:40])
    r.pin("read again with the fault cleared, the file is whole", again == _sha("v1.bin"), again[:40])
    r.pin("nothing of a cut fetch is held", abs(used - (b.get("held") or 0)) <= 8 * MiB and b.get("claimed") == 0,
          f"disk {used / 1e6:.0f} MB, counted {(b.get('held') or 0) / 1e6:.0f} MB, claimed {b.get('claimed')}")


@case("blocks")
def version_held_for_readers(r: Results, env: Env) -> None:
    """Replaced while a reader is partway: it, and a reader starting
    meanwhile, get the old version whole; the new one shows after."""
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0})
    _links(env, True)
    _import(env, "blocks/ver.bin", "v1.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    path = f"{TOP}/blocks/ver.bin"
    wait_for(lambda: os.path.exists(path), 30, 0.2)
    drop_caches()
    slow = _hash_as_user(path, delay=0.1)  # 64 chunks of 4 MiB: about 7 s
    time.sleep(1.5)
    _import(env, "blocks/ver.bin", "v2.bin")
    time.sleep(1.5)
    joined = _hash_as_user(path)
    first, second = slow.communicate(timeout=300)[0].strip(), joined.communicate(timeout=300)[0].strip()
    r.pin("the reader partway through got the old version whole", first == _sha("v1.bin"), first[:40])
    r.pin("a reader starting meanwhile got the old version too", second == _sha("v1.bin"), second[:40])
    shown = wait_for(lambda: os.path.getsize(path) == os.path.getsize(f"{DATA}/v2.bin")
                     and _hash_as_user(path).communicate()[0].strip() == _sha("v2.bin"), 200, 2.0)
    r.pin("once the readers were done the new version shows", bool(shown),
          json.dumps({k: v for k, v in _ops(env).items() if k.startswith("pin_")}))


def _pause_reader(path: str, flag: str, pause: float) -> subprocess.Popen:
    """A reader that reads half of ``path``, says so in ``flag``, keeps the
    file open with no read for ``pause`` seconds, then reads the rest:
    prints the digest, or the error."""
    code = (
        "import hashlib,os,time\n"
        "h=hashlib.sha256()\n"
        "try:\n"
        f"  fd=os.open({path!r}, os.O_RDONLY); size=os.fstat(fd).st_size; at=0\n"
        "  while at < size // 2:\n"
        f"    b=os.pread(fd, {4 * MiB}, at); h.update(b); at+=len(b)\n"
        f"  open({flag!r}, 'w').close(); time.sleep({pause})\n"
        "  while b:=os.pread(fd, 4<<20, at):\n"
        "    h.update(b); at+=len(b)\n"
        "  print(h.hexdigest())\n"
        "except OSError as e:\n"
        "  print('error', e.errno)\n"
    )
    return subprocess.Popen(["runuser", "-u", USER, "--", "python3", "-c", code],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


@case("blocks")
def a_quiet_reader_of_a_removed_or_moved_file(r: Results, env: Env) -> None:
    """Readers halfway through a large file, which is then removed, or moved
    and replaced by another at its old name; they keep it open with no read
    for longer than a hold's idle time, then read the rest. The moved one
    gets the file it opened; the removed one gets it, or an error where the
    page cache no longer has it (no path names it), never other bytes."""
    _links(env, True)
    _import(env, "blocks/rmq.bin", "v1.bin")
    _import(env, "blocks/mvq.bin", "v1.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    for name in ("rmq.bin", "mvq.bin"):
        wait_for(lambda name=name: os.path.exists(f"{TOP}/blocks/{name}"), 30, 0.2)
    drop_caches()
    flags = {n: f"/tmp/half-{n}" for n in ("rmq.bin", "mvq.bin")}
    for flag in flags.values():
        if os.path.exists(flag):
            os.unlink(flag)
    readers = {n: _pause_reader(f"{TOP}/blocks/{n}", flags[n], 135) for n in flags}
    if not wait_for(lambda: all(os.path.exists(f) for f in flags.values()), 120, 0.2):
        for p in readers.values():
            p.kill()
        r.pin("both readers read halfway before the files changed", False,
              json.dumps({n: p.communicate(timeout=30)[0][-200:] for n, p in readers.items()}))
        return
    rm = sh(["runuser", "-u", USER, "--", "rm", f"{TOP}/blocks/rmq.bin"])
    mv = sh(["runuser", "-u", USER, "--", "mv", f"{TOP}/blocks/mvq.bin", f"{TOP}/blocks/mvq-moved.bin"])
    _import(env, "blocks/mvq.bin", "v2.bin")
    out = {n: p.communicate(timeout=600)[0].strip() for n, p in readers.items()}
    ops = _ops(env)
    detail = json.dumps({k: v for k, v in ops.items() if k.startswith("pin_")})
    r.pin("removed partway, then quiet past the idle time: the file whole or an error",
          rm.returncode == 0 and (out["rmq.bin"] == _sha("v1.bin") or out["rmq.bin"].startswith("error")),
          f"{out['rmq.bin'][:40]}; {detail}")
    r.pin("moved and replaced partway, then quiet: the reader gets the file it opened",
          mv.returncode == 0 and out["mvq.bin"] == _sha("v1.bin"), f"{out['mvq.bin'][:40]}; {detail}")


@case("blocks")
def a_removed_file_never_reads_another_files_bytes(r: Results, env: Env) -> None:
    """One process has two files open: small A, not read yet, and large B,
    read partway and then replaced from elsewhere. A is removed and read:
    an error, never B's bytes or B's size."""
    _links(env, True)
    _import(env, "blocks/hold-b.bin", "v1.bin")
    with open("/tmp/small-a.src", "w") as f:
        f.write("small file a")
    with open("/tmp/small-a.src.sha256", "w") as f:
        f.write(hashlib.sha256(b"small file a").hexdigest())
    env.server.admin("GET", "import", {"path": "blocks/small-a.txt", "file": "/tmp/small-a.src"})
    _restart(env, {"binary_budget": 2 * GiB})
    a, b = f"{TOP}/blocks/small-a.txt", f"{TOP}/blocks/hold-b.bin"
    wait_for(lambda: os.path.exists(a) and os.path.exists(b), 30, 0.2)
    drop_caches()
    flag = "/tmp/two-open"
    if os.path.exists(flag):
        os.unlink(flag)
    code = (
        "import os,time\n"
        f"fa=os.open({a!r}, os.O_RDONLY); fb=os.open({b!r}, os.O_RDONLY)\n"
        f"os.pread(fb, {4 * MiB}, 0)\n"
        f"open({flag!r}, 'w').close()\n"
        f"while os.path.exists({flag!r}): time.sleep(0.2)\n"
        "try:\n"
        "  print('size', os.fstat(fa).st_size)\n"
        "except OSError as e:\n"
        "  print('size error', e.errno)\n"
        "try:\n"
        "  print('read', os.pread(fa, 64, 0)[:32].hex())\n"
        "except OSError as e:\n"
        "  print('read error', e.errno)\n"
    )
    reader = subprocess.Popen(["runuser", "-u", USER, "--", "python3", "-c", code],
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if not wait_for(lambda: os.path.exists(flag), 120, 0.2):
        reader.kill()
        r.pin("the reader opened both files", False, reader.communicate(timeout=30)[0][-300:])
        return
    _import(env, "blocks/hold-b.bin", "v2.bin")
    time.sleep(1.5)
    rm = sh(["runuser", "-u", USER, "--", "rm", a])
    os.unlink(flag)
    out = reader.communicate(timeout=120)[0]
    with open(f"{DATA}/v1.bin", "rb") as f:
        b_head = f.read(32).hex()
    big = str(os.path.getsize(f"{DATA}/v1.bin"))
    r.pin("removed A's read is an error or A's own bytes, never B's",
          rm.returncode == 0 and b_head not in out and ("read error" in out or "736d616c6c" in out), out.strip()[:200])
    r.pin("removed A's size is never B's", f"size {big}" not in out, out.strip()[:200])


@case("blocks")
def gone_version_fails(r: Results, env: Env) -> None:
    """A version the server no longer holds, mid-read: an error, never the
    other version's bytes."""
    _links(env, False)  # through the server's route, which knows what it holds
    _import(env, "blocks/gone.bin", "v1.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    path = f"{TOP}/blocks/gone.bin"
    wait_for(lambda: os.path.exists(path), 30, 0.2)
    drop_caches()
    slow = _hash_as_user(path, delay=0.15)
    time.sleep(1.5)
    _import(env, "blocks/gone.bin", "v2.bin")
    env.server.admin("GET", "purge", {"sha256": _sha("v1.bin")})
    out = slow.communicate(timeout=300)[0].strip()
    r.pin("a purged version's read fails", out.startswith("error"), out[:40])
    r.pin("it never read the other version", out not in (_sha("v2.bin"),), out[:40])
    _import(env, "blocks/gone.bin", "v1.bin")  # put the bytes back for later cases


@case("blocks")
def link_runs_out(r: Results, env: Env) -> None:
    """Links that last 3 s, a read that takes longer: asked for again, the
    bytes right. Then the same file through the server's route."""
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 20e6, "connect": 0.0})
    _links(env, True, life=3)
    _import(env, "blocks/link.bin", "v1.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    path = f"{TOP}/blocks/link.bin"
    wait_for(lambda: os.path.exists(path), 30, 0.2)
    drop_caches()
    out = _hash_as_user(path).communicate(timeout=600)[0].strip()
    ops = _ops(env)
    r.pin("a read across expiring links is right", out == _sha("v1.bin"), out[:40])
    r.pin("links were asked for again", (ops.get("source") or 0) > 1,
          f"source {ops.get('source')}, refused 403 {ops.get('link_403', 0)}")
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0})
    _links(env, False)
    _restart(env, {"binary_budget": 2 * GiB})
    wait_for(lambda: os.path.exists(path), 30, 0.2)
    drop_caches()
    out = _hash_as_user(path).communicate(timeout=600)[0].strip()
    ops = _ops(env)
    r.pin("the server's route serves the same bytes", out == _sha("v1.bin") and (ops.get("api_bytes") or 0) > 0,
          f"{out[:16]} api {ops.get('api_bytes')}")
    _links(env, True, life=900)
    log("  blocks: done")


@case("blocks")
def link_runs_out_while_a_reader_waits(r: Results, env: Env) -> None:
    """A reader that waits past its link's life before reading on: the old
    link is refused, a fresh one asked for, the bytes right."""
    _blob("POST", "/admin/net", {"ttfb": 0.0, "rate": 0, "connect": 0.0})
    # The store refuses a link after 20 s, though the server named 900.
    _links(env, True, life=20, told=900)
    _import(env, "blocks/wait.bin", "v1.bin")
    _restart(env, {"binary_budget": 2 * GiB})
    path = f"{TOP}/blocks/wait.bin"
    wait_for(lambda: os.path.exists(path), 30, 0.2)
    drop_caches()
    code = (
        "import os,random,time\n"
        f"a=os.open({path!r},os.O_RDONLY); b=os.open({DATA + '/v1.bin'!r},os.O_RDONLY)\n"
        "first=os.pread(a,1<<20,0)==os.pread(b,1<<20,0)\n"
        "time.sleep(25)\n"
        "r=random.Random(5); bad=0\n"
        "for _ in range(64):\n"
        "  at=r.randrange(128<<20, (256<<20)-(1<<20))\n"
        "  bad+=os.pread(a,1<<20,at)!=os.pread(b,1<<20,at)\n"
        "print(first, bad)\n")
    p = subprocess.run(["runuser", "-u", USER, "--", "python3", "-c", code], capture_output=True, text=True)
    out = (p.stdout + p.stderr).strip()
    ops = _ops(env)
    refused = {k: v for k, v in ops.items() if k.startswith("link_") and k[5:].isdigit()}
    r.pin("a read after the link ran out is right", out == "True 0", out[-300:] + "\n" + env.mount.log_tail()[-1500:])
    r.pin("the old link was refused and a fresh one asked for", bool(refused) and ops.get("source", 0) >= 2,
          f"refused {refused}, source {ops.get('source')}")
    _links(env, True, life=900)
