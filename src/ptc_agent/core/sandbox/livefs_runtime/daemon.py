"""Mount the user's server-held files (memory, profile, automations,
workflows, memos, transcripts) in the sandbox.

Runs as root, from the copy ``boot`` checked; every read and save goes to the
server's livefs endpoint with the computer's mount token, and nothing is kept
on disk.

    python3 -m livefs start --root R --base-url U   (a new token in LIVEFS_CONFIG)
    python3 -m livefs link --root R [--link SRC:DST ...]
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import threading
from collections.abc import Callable, Iterator

from .lifecycle import (
    Paths,
    has_libfuse,
    healthy,
    install_config,
    install_libfuse,
    locked,
    read_state,
    rebase,
    start,
    unsupported,
    use_libfuse3,
    write_state,
)
from .links import finish_set_aside, link, unlink
from .ops import LiveFS
from .protocol import CALL_ENV, CONFIG_ENV, MountError
from .remote import Remote
from .views import CONTENT_BYTES

PROBE_TIMEOUT_S = 5

_CALL_ENV = CALL_ENV.encode() + b"="


def _probe(paths: Paths) -> str | None:
    """Why the server cannot be reached with the installed config, if it
    cannot. A mount that cannot reach it would hold every file operation for
    the request timeout, where no mount refuses at once."""
    try:
        status, _, _ = Remote(paths.config, PROBE_TIMEOUT_S).request(
            "GET", "list", {"path": ""}, attempts=1
        )
    except OSError:
        with open(paths.config) as f:
            return f"no answer from {json.load(f).get('base_url')}"
    return None if status == 200 else f"the server answered {status}"


def _probing(paths: Paths) -> Callable[[], str | None]:
    """Start ``_probe`` beside the rest of ``start``; the call returned waits
    for its answer. Its request is mostly waiting on the network, which a
    new daemon's start fills."""
    answer: list[str | None] = []

    def run() -> None:
        try:
            answer.append(_probe(paths))
        except Exception as exc:  # the config went unreadable mid-probe
            answer.append(f"the probe failed: {exc}")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def result() -> str | None:
        thread.join()
        return answer[0]

    return result


def _ready(args, paths: Paths) -> tuple[dict | None, Callable[[], str | None] | None]:
    """Everything before the lock: the answer when the mount cannot serve
    yet, else the probe a start waits on (None when there is nothing to
    probe). A new token goes in first, so it reaches the sandbox whatever
    else holds the mount back."""
    reason = unsupported()
    if reason:
        return {"ok": False, "error": MountError.UNSUPPORTED, "reason": reason}, None
    os.makedirs(paths.private, mode=0o700, exist_ok=True)
    if args.config:
        # Under the lock, as the rebase below is: a rebase that read the
        # config before a new token landed would write the old one back.
        with locked(paths):
            problem = install_config(args.config, paths)
        if problem:
            return {"ok": False, "error": MountError.BAD_CONFIG, "reason": problem}, None
    if not os.path.exists(paths.config):
        return {"ok": False, "error": MountError.NO_CONFIG}, None
    if not has_libfuse():
        install_libfuse(paths)
        return {
            "ok": False,
            "error": MountError.INSTALLING,
            "reason": "installing libfuse",
        }, None
    with locked(paths):
        rebased = rebase(args.base_url, paths)
    changed = bool(args.config) or rebased or not healthy(paths.mount)
    probe = _probing(paths) if changed else None
    return None, probe


def _serving(
    args, paths: Paths, state: dict, probe: Callable[[], str | None] | None
) -> tuple[bool, dict | None]:
    """Under the lock: start the daemon if it is not serving current code,
    and record what the probe answered. Whether one was started, and why the
    mount does not serve if it does not."""
    if probe is None and state.get("unreachable"):
        # A probe that failed is asked again until one answers, whichever
        # host asks: a daemon up since then serves nothing it can save. Read
        # under the lock, so a start that waited on the one that failed asks.
        probe = _probing(paths)
    try:
        started, problem = start(args, state, paths, gate=probe)
    except OSError as exc:
        started, problem = True, str(exc)
    unreachable = probe() if probe else None
    if unreachable:
        state["unreachable"] = unreachable
    elif probe:
        state.pop("unreachable", None)
    if unreachable:
        return started, {"ok": False, "error": MountError.UNREACHABLE, "reason": unreachable}
    if problem:
        return started, {"ok": False, "error": MountError.START_FAILED, "reason": problem}
    return started, None


@contextlib.contextmanager
def _as_owner(owner: os.stat_result) -> Iterator[Callable[[], contextlib.AbstractContextManager]]:
    """Act with the rights of the user who owns the folders. The links go
    where that user writes, so as root a symlink they planted on the way
    would aim each mkdir, rename and link at any path in the sandbox.
    Yields a context that is root again, for writes to root's own files."""
    if os.geteuid() != 0 or owner.st_uid == 0:
        yield contextlib.nullcontext
        return
    egid, groups = os.getegid(), os.getgroups()

    def drop() -> None:
        os.setgroups([])
        os.setegid(owner.st_gid)
        os.seteuid(owner.st_uid)

    def regain() -> None:
        os.seteuid(0)
        os.setegid(egid)
        os.setgroups(groups)

    @contextlib.contextmanager
    def as_root() -> Iterator[None]:
        regain()
        try:
            yield
        finally:
            drop()

    drop()
    try:
        yield as_root
    finally:
        regain()


def link_folders(args, paths: Paths) -> dict:
    """Link exactly ``--link``.

    Links an earlier call made that this one does not ask for are removed, so
    the caller's list (every live folder on this computer) is the whole truth
    and a folder that left needs no separate teardown. Laid whether or not the
    daemon serves: each points through the mount link, which a start swaps
    over, so they stay right across a restart.
    """
    os.makedirs(paths.private, mode=0o700, exist_ok=True)
    with locked(paths):
        state = read_state(paths)
        wanted = {}
        for spec in args.link or ():
            source, _, target = spec.partition(":")
            wanted[os.path.abspath(target)] = source
        failed, moved = {}, {}
        copied = state.get("copied")
        copied = copied if isinstance(copied, dict) else {}
        with _as_owner(os.stat(args.root)) as as_root:

            def keep() -> None:
                # A copy aside, saved before the delete after it: a link
                # killed partway through would copy what is left again.
                with as_root():
                    write_state(paths, {**state, "copied": copied})

            for target in state.get("links", ()):
                if target not in wanted:
                    unlink(target, paths.mount)
            for target in [t for t in copied if t not in wanted]:
                # Kept for the next pass when it fails, as for a link.
                with contextlib.suppress(OSError):
                    finish_set_aside(target, copied)
            for target, source in wanted.items():
                try:
                    aside = link(os.path.join(paths.mount, source), target, copied, keep)
                except OSError as exc:
                    failed[target] = str(exc)
                else:
                    if aside:
                        moved[target] = aside
        state["links"] = sorted(wanted)
        # Written here, as root: the owner the links are made as cannot.
        state["copied"] = {
            t: c for t, c in copied.items() if t in failed or t not in wanted
        }
        # How the daemon tells a new layout from a rewrite that kept it.
        state["sources"] = sorted(set(wanted.values()))
        write_state(paths, state)
    return {
        "ok": not failed,
        "error": MountError.LINK_FAILED if failed else None,
        "failed": failed or None,
        "set_aside": moved or None,
    }


def start_mount(args, paths: Paths) -> dict:
    """Install a new token and make sure the mount serves current code. It needs
    no folder list, so the host runs it beside the asset sync. Beside a sync
    still replacing the code, ``boot`` answers ``stale_code`` and the host
    asks again once the sync has landed."""
    refused, probe = _ready(args, paths)
    if refused:
        return refused
    with locked(paths):
        state = read_state(paths)
        started, refused = _serving(args, paths, state, probe)
        write_state(paths, state)
    return refused or {"ok": True, "error": None, "started": started or None}


def call_in_environ(pid: int, proc: str = "/proc") -> str | None:
    """The call id a process started with. Its starting environment, not its
    current one: children inherit it, and the shell's own redirections are
    requests of the shell's pid."""
    try:
        with open(f"{proc}/{pid}/environ", "rb") as f:
            for item in f.read().split(b"\0"):
                if item.startswith(_CALL_ENV):
                    return item[len(_CALL_ENV):].decode(errors="replace") or None
    except OSError:
        pass
    return None


def content_budget(default: int, cgroup: str = "/sys/fs/cgroup") -> int:
    """``default``, or a sixteenth of the sandbox's memory limit when smaller."""
    for name in ("memory.max", "memory/memory.limit_in_bytes"):
        try:
            with open(os.path.join(cgroup, name)) as f:
                limit = f.read().strip()
        except OSError:
            continue
        return min(default, int(limit) // 16) if limit.isdigit() else default
    return default


def _truncate_fuse_3(fuse, path, length, fip):
    """libfuse 3 hands truncate the open file when ftruncate(2) made the call,
    and the vendored binding drops it. Without it the truncate goes through a
    handle of its own, and the open one, still holding the old bytes, saves
    over it: ``open('r+')``, ``truncate()``, ``write()`` keeps the old tail."""
    fh = (fip.contents if fuse.raw_fi else fip.contents.fh) if fip else None
    name = None if path is None else path.decode(fuse.encoding, fuse.errors)
    return fuse.operations.truncate(name, length, fh)


def serve(args, paths: Paths) -> None:
    # mfusepy is imported only here: ``start`` runs before libfuse may be
    # installed.
    use_libfuse3()
    from . import mfusepy as fuse

    owner = os.stat(args.root)
    operations = LiveFS(
        Remote(paths.config),
        owner.st_uid,
        owner.st_gid,
        caller=lambda: fuse.fuse_get_context()[2],
        call_of=call_in_environ,
        layout=paths.state,
        content_bytes=content_budget(CONTENT_BYTES),
    )
    # Loaded while ``start`` probes the server; mounted only once it answered.
    if getattr(args, "gated", False) and sys.stdin.readline().strip() != "go":
        return

    class FUSE(fuse.FUSE):
        truncate_fuse_3 = _truncate_fuse_3

    FUSE(
        operations,
        args.mount,
        foreground=True,
        allow_other=True,
        default_permissions=True,
        attr_timeout=0,
        entry_timeout=0,
        negative_timeout=0,
        fsname="livefs",
    )


def main() -> int:
    parser = argparse.ArgumentParser(prog="livefs")
    parser.add_argument("command", choices=("serve", "start", "link"))
    parser.add_argument("--root", required=True)
    parser.add_argument("--mount")
    parser.add_argument("--base-url")
    parser.add_argument("--link", action="append")
    parser.add_argument("--gated", action="store_true")
    args = parser.parse_args()
    # Taken out, so the daemon a start launches does not inherit the token.
    args.config = os.environ.pop(CONFIG_ENV, None)
    paths = Paths()
    if args.command == "serve":
        serve(args, paths)
        return 0
    commands = {"start": start_mount, "link": link_folders}
    result = commands[args.command](args, paths)
    print(json.dumps(result))
    return 0 if result.get("ok") else 1
