"""Changes made elsewhere reaching a reader, and how fast."""

from __future__ import annotations

import contextlib
import os
import time

from checks import Results, case
from fixtures import TOP, Env, as_user, read_file, wait_for

REL = "ftest"


def folder(name: str) -> tuple[str, str]:
    """A folder of this case's own, made here: (its Drive path, its path on the mount)."""
    rel = f"{REL}/{name}"
    as_user(f"mkdir -p {TOP}/{rel}")
    return rel, f"{TOP}/{rel}"


def visible(env: Env, name: str, pred, timeout: float = 30.0) -> float | None:
    """Seconds until ``pred`` holds (None: it never did), kept for the report in ms."""
    t0 = time.perf_counter()
    ok = wait_for(pred, timeout, 0.002)
    took = time.perf_counter() - t0
    env.report.setdefault("feed", {}).setdefault("visible_ms", {})[name] = round(took * 1000, 1) if ok else None
    return took if ok else None


def ms(took: float | None) -> str:
    return f"{round(took * 1000, 1) if took is not None else None} ms"


@case("feed")
def change_reaches_a_reader_holding_old_bytes(r: Results, env: Env) -> None:
    rel, d = folder("modify")
    as_user(f"echo v1 > {d}/doc.md")
    read_file(f"{d}/doc.md")  # cached, so the change has to invalidate it
    lat = []
    for i in range(5):
        body = f"v{i + 2} changed elsewhere\n".encode()
        env.server.write(f"{rel}/doc.md", body)
        took = visible(env, f"modify #{i + 1}", lambda b=body: read_file(f"{d}/doc.md") == b)
        if took is not None:
            lat.append(took)
    r.pin("a change elsewhere reaches a reader holding the old bytes in cache",
          len(lat) == 5, f"{[round(x * 1000, 1) for x in lat]} ms")


@case("feed")
def file_made_then_deleted_elsewhere(r: Results, env: Env) -> None:
    rel, d = folder("create")
    with contextlib.suppress(OSError):
        os.stat(f"{d}/new.md")  # a cached absence
    os.listdir(d)
    env.server.write(f"{rel}/new.md", b"brand new\n")
    took = visible(env, "create", lambda: read_file(f"{d}/new.md") == b"brand new\n")
    r.pin("a file made elsewhere shows up (cached absence dropped)",
          took is not None and "new.md" in os.listdir(d), ms(took))

    env.server.admin("POST", "trash", {"path": f"{rel}/new.md"})
    took = visible(env, "delete", lambda: not os.path.exists(f"{d}/new.md") and "new.md" not in os.listdir(d))
    r.pin("a file deleted elsewhere goes", took is not None, ms(took))


@case("feed")
def rename_elsewhere_moves_the_name(r: Results, env: Env) -> None:
    rel, d = folder("rename")
    as_user(f"echo v1 > {d}/doc.md")
    read_file(f"{d}/doc.md")
    os.listdir(d)
    env.server.admin("POST", "move", {"from": f"{rel}/doc.md", "to": f"{rel}/renamed.md"})
    took = visible(env, "rename", lambda: not os.path.exists(f"{d}/doc.md")
                   and read_file(f"{d}/renamed.md") is not None)
    r.pin("a rename elsewhere moves the name", took is not None, ms(took))


@case("feed")
def folder_renamed_then_deleted_elsewhere(r: Results, env: Env) -> None:
    rel, d = folder("folder")
    as_user(f"mkdir -p {d}/sub && echo inner > {d}/sub/inner.md")
    os.listdir(f"{d}/sub")
    env.server.admin("POST", "move", {"from": f"{rel}/sub", "to": f"{rel}/sub2"})
    took = visible(env, "folder rename",
                   lambda: not os.path.exists(f"{d}/sub") and read_file(f"{d}/sub2/inner.md") == b"inner\n")
    r.pin("a folder renamed elsewhere moves with its contents", took is not None, ms(took))

    env.server.admin("POST", "trash", {"path": f"{rel}/sub2"})
    visible(env, "folder delete", lambda: not os.path.exists(f"{d}/sub2"))
    p = as_user(f"mkdir {d}/sub2 && echo again > {d}/sub2/x.md", check=False)
    r.pin("a folder deleted elsewhere can be made again here (no stale name)",
          p.returncode == 0 and env.server.content(f"{rel}/sub2/x.md") == b"again\n", p.stderr.strip())


@case("feed")
def binary_added_elsewhere_joins_the_ignore_file(r: Results, env: Env) -> None:
    rel, _ = folder("ignore")
    before = read_file(f"{TOP}/.ignore") or b""
    # An extension of this run's own: a binary the Drive already holds under
    # a common one would have put it in the ignore file before.
    ext = "b" + os.urandom(3).hex()
    rule = f"*.{ext}".encode()
    env.server.write(f"{rel}/blob.{ext}", os.urandom(4096))
    took = visible(env, "ignore file", lambda: rule in (read_file(f"{TOP}/.ignore") or b""))
    r.pin("a binary added elsewhere joins the ignore file", took is not None and rule not in before, ms(took))


@case("feed")
def cursor_ahead_of_a_recreated_space_resyncs(r: Results, env: Env) -> None:
    if not env.fake:
        return
    server, mount = env.server, env.mount
    rel, d = folder("recreate")
    as_user(f"echo kept > {d}/kept.md")
    # The tables made again under a running mount: its cursor is ahead of
    # the space's sequence, and every id has changed.
    before = (mount.status() or {}).get("feed", {}).get("resyncs", 0)
    server.admin("POST", "recreate")
    visible(env, "resynced", lambda: (mount.status() or {}).get("feed", {}).get("resyncs", 0) > before)
    server.write(f"{rel}/after-recreate.md", b"fresh\n")
    took = visible(env, "recreated", lambda: read_file(f"{d}/after-recreate.md") == b"fresh\n")
    resyncs = (mount.status() or {}).get("feed", {}).get("resyncs", 0)
    p = as_user(f"echo edit >> {d}/kept.md && cat {d}/kept.md", check=False)
    r.pin("a cursor ahead of a re-created space resyncs, and saves go on",
          took is not None and resyncs > before and p.returncode == 0
          and (server.content(f"{rel}/kept.md") or b"").endswith(b"edit\n"),
          f"{ms(took)}, resyncs {before}->{resyncs} {p.stderr.strip()}")


@case("feed")
def feed_report(r: Results, env: Env) -> None:
    status = env.mount.status() or {}
    env.report.setdefault("feed", {}).update(feed=status.get("feed"), invalidations=status.get("invalidations"))
