"""Drive the sandbox-side file transfer runtime.

The runtime (``wsfiles_transfer_runtime.py``, shipped to
``_internal/src/wsfiles_transfer.py``) walks, hashes and moves workspace
files inside the sandbox. This module hands it a JSON spec, runs it, and
reads the JSON it writes back. The server never holds file bytes on this
path; it holds paths, digests and presigned URLs.

The two JSON files travel through ``_internal/`` with the raw runtime calls
rather than the agent-facing upload helpers: ``_internal`` is on the agent's
denylist by design, and this exchange is the server's, not the agent's.
"""

from __future__ import annotations

import base64
import json
import logging
import shlex
import time
import uuid
from dataclasses import dataclass
from typing import Any

from ptc_agent.core.paths import (
    ALWAYS_HIDDEN_DIR_NAMES,
    BACKUP_EXCLUDE_AGENT_SUBDIRS,
    BACKUP_EXCLUDE_DIRS,
    HIDDEN_DIR_NAMES,
)
from ptc_agent.core.sandbox._shared import (
    _TRANSFER_RUNTIME_SOURCE,
    TRANSFER_RUNTIME_SANDBOX_NAME,
)
from ptc_agent.core.sandbox.wsfiles_transfer_runtime import RESULT_MARKER
from ptc_agent.core.sandbox.retry import RetryPolicy

logger = logging.getLogger(__name__)

# Directory names pruned at any depth. ``__pycache__`` was only ever filtered
# by its contents' names before.
EXCLUDE_DIR_NAMES: frozenset[str] = (
    BACKUP_EXCLUDE_DIRS | HIDDEN_DIR_NAMES | ALWAYS_HIDDEN_DIR_NAMES | {"__pycache__"}
)
# The skill reconciler's scratch space, which must never be restored on top
# of a live reconcile. Matched by workspace-relative path: the same names
# anywhere else (``work/model/.staging``) are the user's own directories.
SKILLS_DIR = ".agents/skills"
EXCLUDE_REL_DIRS: tuple[str, ...] = (
    *BACKUP_EXCLUDE_AGENT_SUBDIRS,
    f"{SKILLS_DIR}/.staging",
)
EXCLUDE_REL_DIR_PREFIXES: tuple[str, ...] = (f"{SKILLS_DIR}/.trash-",)
# The reconciler's lock file, at its one path; a user's own
# ``results/.skills-sync.flock`` is a file like any other.
EXCLUDE_REL_FILES: tuple[str, ...] = (f"{SKILLS_DIR}/.skills-sync.flock",)
EXCLUDE_SUFFIXES: frozenset[str] = frozenset({".pyc", ".pyo", ".so", ".dylib", ".o"})
EXCLUDE_BASENAMES: frozenset[str] = frozenset({".DS_Store", "Thumbs.db", "__init__.py"})

SYNC_MARKER_NAME = ".file_sync_marker"

SCAN_TIMEOUT_S = 300
# Transfer timeouts scale with bytes at a floor bandwidth so a large workspace
# on a slow link is not cut off, while an idle exchange still ends.
TRANSFER_FLOOR_BYTES_PER_S = 1024 * 1024
TRANSFER_MIN_TIMEOUT_S = 300
TRANSFER_MAX_TIMEOUT_S = 3600

# The sandbox runs on a one-CPU quota, and the runtime's CPU per item grows
# with its thread count (a 300-file pull: 1.0 s of CPU at 16 threads, 3.5 s
# at 64) while the store answers in ~80 ms per GET and ~210 ms per PUT.
# These are the measured knees; wider pools throttle, and the tail grows.
PUSH_CONCURRENCY = 16
PULL_CONCURRENCY = 32

# Files at or below the cutoff travel as members of a pack: one object per
# chunk of the workspace instead of one per file. The transfer cost is per
# object, and the small files are most of the objects while being almost
# none of the bytes. The chunk cap bounds what one small edit re-uploads and
# what a restore holds in flight; chunks are written under PACK_DIR, which the
# scan already excludes, and are removed once pushed.
PACK_CUTOFF = 256 * 1024
PACK_MAX_BYTES = 32 * 1024 * 1024
PACK_DIR = "_internal/packs"


class TransferRuntimeError(Exception):
    """The runtime itself could not run or answer; not a per-item failure."""


@dataclass(slots=True)
class ScanEntry:
    path: str
    kind: str
    size: int
    mtime_ns: int
    mode: int
    sha256: str | None
    symlink_target: str | None
    is_binary: bool | None


@dataclass(slots=True)
class ScanResult:
    entries: list[ScanEntry]
    oversized: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    hashed: int
    reused: int


def transfer_timeout_s(total_bytes: int) -> int:
    scaled = TRANSFER_MIN_TIMEOUT_S + total_bytes // TRANSFER_FLOOR_BYTES_PER_S
    return int(min(max(scaled, TRANSFER_MIN_TIMEOUT_S), TRANSFER_MAX_TIMEOUT_S))


def exclusion_spec(max_file_bytes: int) -> dict[str, Any]:
    return {
        "exclude_dir_names": sorted(EXCLUDE_DIR_NAMES),
        "exclude_rel_dirs": list(EXCLUDE_REL_DIRS),
        "exclude_rel_dir_prefixes": list(EXCLUDE_REL_DIR_PREFIXES),
        "exclude_rel_files": list(EXCLUDE_REL_FILES),
        "exclude_basenames": sorted(EXCLUDE_BASENAMES),
        # The sync marker and every transient file the runtime or the relay
        # writes sit at the root; anywhere deeper the name is the user's own,
        # and a basename exclusion would drop ``results/.file_sync_marker``
        # from the scan and prune its row. The marker is one exact name: a
        # root ``.file_sync_marker.bak`` is a user file too.
        "exclude_root_basenames": [SYNC_MARKER_NAME],
        "exclude_root_basename_prefixes": [".wsfiles-"],
        "exclude_suffixes": sorted(EXCLUDE_SUFFIXES),
        "max_file_bytes": max_file_bytes,
    }


def _script_path(sandbox: Any) -> str:
    return f"{sandbox.working_dir}/_internal/src/{TRANSFER_RUNTIME_SANDBOX_NAME}"


async def _upload_runtime(sandbox: Any) -> None:
    """Ship the runtime to a sandbox that lacks it or runs a stale copy.

    The asset sync delivers it on every fresh sandbox and on the first sync
    after a deploy; a warm sandbox between those two moments still needs it.
    """
    script = _script_path(sandbox)
    source = _TRANSFER_RUNTIME_SOURCE.read_bytes()
    await sandbox._runtime_call(
        sandbox.runtime.upload_file, source, script, retry_policy=RetryPolicy.SAFE
    )
    logger.info(f"Uploaded the file transfer runtime to {script}")


# A single argv string tops out at 128 KiB on Linux; below this the spec rides
# on the command line and the op is one round trip to the sandbox.
INLINE_SPEC_LIMIT = 96 * 1024


def _parse_result(stdout: str) -> dict[str, Any] | None:
    """The runtime's result line, or None when no result was printed."""
    for line in reversed((stdout or "").splitlines()):
        if line.startswith(RESULT_MARKER):
            return json.loads(line[len(RESULT_MARKER):])
    return None


def _slowest(out: dict[str, Any]) -> str:
    """Suffix with the per-item latency spread, for the transfer timing line."""
    results = out.get("results")
    if not isinstance(results, dict) or not results:
        return ""
    timed = sorted(
        ((r or {}).get("ms") or 0, key) for key, r in results.items() if (r or {}).get("ms")
    )
    if not timed:
        return ""
    q = lambda f: timed[min(len(timed) - 1, int(f * len(timed)))][0]  # noqa: E731
    ms, key = timed[-1]
    return f", per item p50={q(0.5)} p90={q(0.9)} p99={q(0.99)} max={ms} ms ({key})"


async def run_transfer_op(
    sandbox: Any, op: str, spec: dict[str, Any], *, timeout_s: int
) -> dict[str, Any]:
    """Run one runtime subcommand and return its output document.

    One exec when the spec fits an argument, two when it has to be uploaded.
    A sandbox without the runtime, or with one that predates this exchange,
    exits 2 without a result line; the runtime is uploaded and the op rerun.
    """
    script = _script_path(sandbox)
    runtime = sandbox.runtime
    payload = json.dumps(spec, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(payload).decode("ascii")
    started = time.monotonic()
    # One path for every attempt: the runtime removes the spec file once it
    # has read it, and a rerun after a runtime upload reuses the name so a
    # copy a stale runtime never read is taken by the one that replaces it.
    in_path = f"{sandbox.working_dir}/_internal/.wsfiles/{op}-{uuid.uuid4().hex}.json"

    async def _run() -> Any:
        if len(encoded) <= INLINE_SPEC_LIMIT:
            cmd = f"python3 {shlex.quote(script)} {op} --spec-b64 {encoded}"
        else:
            await sandbox._runtime_call(
                runtime.upload_file, payload, in_path, retry_policy=RetryPolicy.SAFE
            )
            cmd = f"python3 {shlex.quote(script)} {op} {shlex.quote(in_path)}"
        return await sandbox._runtime_call(
            runtime.exec, cmd, timeout_s, retry_policy=RetryPolicy.SAFE
        )

    res = await _run()
    out = _parse_result(res.stdout)
    if out is None and res.exit_code == 2:
        await _upload_runtime(sandbox)
        res = await _run()
        out = _parse_result(res.stdout)
    if out is None:
        tail = (res.stdout or "")[-2000:]
        raise TransferRuntimeError(
            f"wsfiles_transfer {op} exited {res.exit_code} without a result: {tail}"
        )
    if not isinstance(out, dict):
        raise TransferRuntimeError(f"wsfiles_transfer {op} wrote a non-object")
    if out.get("error"):
        # The runtime caught its own crash and reported it instead of
        # a result set; nothing below is trustworthy.
        raise TransferRuntimeError(f"wsfiles_transfer {op} failed: {out['error']}")
    hs = out.get("handshakes") or {}
    hs_note = (
        f", tls full={hs.get('full', 0)} resumed={hs.get('resumed', 0)}"
        if isinstance(hs, dict) and hs
        else ""
    )
    logger.info(
        f"wsfiles_transfer {op}: {len(spec.get('items') or [])} item(s) in "
        f"{time.monotonic() - started:.1f}s{_slowest(out)}{hs_note}"
    )
    return out


async def scan_workspace(
    sandbox: Any,
    prior: dict[str, tuple[int, int, str]],
    *,
    max_file_bytes: int,
) -> ScanResult:
    """Walk and hash the workspace. ``prior`` lets unchanged files skip hashing."""
    spec = exclusion_spec(max_file_bytes)
    spec["root"] = sandbox.working_dir
    spec["prior"] = {p: list(v) for p, v in prior.items()}
    out = await run_transfer_op(sandbox, "scan", spec, timeout_s=SCAN_TIMEOUT_S)
    # A runtime that predates the exact-name key reports the marker as a
    # file; a manifest row for it would restore a "populated" claim into a
    # sandbox before its files arrive, so the server drops it as well.
    entries = [
        ScanEntry(
            path=e["path"],
            kind=e["kind"],
            size=int(e.get("size") or 0),
            mtime_ns=int(e.get("mtime_ns") or 0),
            mode=int(e.get("mode") or 0),
            sha256=e.get("sha256"),
            symlink_target=e.get("symlink_target"),
            is_binary=e.get("is_binary"),
        )
        for e in out.get("entries", [])
        if e["path"] != SYNC_MARKER_NAME
    ]
    return ScanResult(
        entries=entries,
        oversized=out.get("oversized", []),
        errors=out.get("errors", []),
        hashed=int(out.get("hashed") or 0),
        reused=int(out.get("reused") or 0),
    )


async def push_direct(
    sandbox: Any, items: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Upload ``items`` (path, sha256, size, url, headers) from the sandbox.

    Returns per-digest results: ``ok``, ``changed``, ``failed``, ``unreachable``.
    """
    if not items:
        return {}
    total = sum(int(i["size"]) for i in items)
    spec = {
        "root": sandbox.working_dir,
        "concurrency": PUSH_CONCURRENCY,
        "timeout_s": TRANSFER_MIN_TIMEOUT_S,
        "items": items,
    }
    out = await run_transfer_op(
        sandbox, "push", spec, timeout_s=transfer_timeout_s(total)
    )
    return out.get("results", {})


async def pull_direct(
    sandbox: Any, items: list[dict[str, Any]], *, defer_dir_modes: bool = False
) -> dict[str, dict[str, Any]]:
    """Materialize ``items`` in the sandbox. Returns per-path results.

    ``defer_dir_modes`` leaves directories writable because a later op will
    place more files under them; that op carries the directory items again
    and applies their modes and mtimes once everything is in.
    """
    if not items:
        return {}
    total = sum(int(i.get("size") or 0) for i in items)
    spec = {
        "root": sandbox.working_dir,
        "concurrency": PULL_CONCURRENCY,
        "timeout_s": TRANSFER_MIN_TIMEOUT_S,
        "items": items,
    }
    if defer_dir_modes:
        spec["defer_dir_modes"] = True
    out = await run_transfer_op(
        sandbox, "pull", spec, timeout_s=transfer_timeout_s(total)
    )
    return out.get("results", {})


async def pack_direct(
    sandbox: Any, members: list[dict[str, Any]]
) -> dict[str, Any]:
    """Concatenate ``members`` (path, sha256, size) into chunk files in the sandbox.

    Returns ``{"chunks": [{path, sha256, size, members: [{path, offset, size,
    sha256}]}], "changed": [path, ...]}``. The chunks are then pushed like any
    other file; ``changed`` lists members whose bytes no longer matched.
    """
    if not members:
        return {"chunks": [], "changed": []}
    total = sum(int(m.get("size") or 0) for m in members)
    spec = {
        "root": sandbox.working_dir,
        "out_dir": PACK_DIR,
        "max_bytes": PACK_MAX_BYTES,
        "members": members,
    }
    out = await run_transfer_op(
        sandbox, "pack", spec, timeout_s=transfer_timeout_s(total)
    )
    return {"chunks": out.get("chunks") or [], "changed": out.get("changed") or []}


async def unlink_direct(sandbox: Any, paths: list[str]) -> int:
    """Remove files under the working dir; returns how many were removed."""
    if not paths:
        return 0
    out = await run_transfer_op(
        sandbox, "unlink", {"root": sandbox.working_dir, "paths": paths}, timeout_s=60
    )
    return int(out.get("removed") or 0)


def all_unreachable(results: dict[str, dict[str, Any]]) -> bool:
    """True when every item failed at the connection level and none got an HTTP answer.

    That signature means the sandbox has no route to the store, which the
    relay path can still serve. Any 4xx or 5xx means the store was reached,
    and falling back would only repeat a real rejection.
    """
    if not results:
        return False
    return all(r.get("status") == "unreachable" for r in results.values())
