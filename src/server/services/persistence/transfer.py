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

import asyncio
import base64
import hashlib
import json
import logging
import shlex
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from ptc_agent.core.paths import (
    ALWAYS_HIDDEN_DIR_NAMES,
    BACKUP_EXCLUDE_AGENT_SUBDIRS,
    BACKUP_EXCLUDE_DIRS,
    HIDDEN_DIR_NAMES,
    THREAD_DIRS_SET_ASIDE,
    SandboxLayout,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox._shared import (
    _TRANSFER_RUNTIME_SOURCE,
    TRANSFER_RUNTIME_SANDBOX_NAME,
)
from ptc_agent.core.sandbox.wsfiles_transfer_runtime import RESULT_MARKER
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.database.blob_keys import INLINE_MAX_BYTES, RELAY_MAX_BYTES
from src.utils.storage import get_blob_transfer_mode

logger = logging.getLogger(__name__)

# Directory names pruned at any depth, because that is what these names mean
# wherever they appear: a dependency tree, a build output or an interpreter
# cache, each re-derivable by re-running the install that made it. ``.git``
# stays on this list deliberately rather than by inheritance: a repository's
# object store is routinely larger than the working tree it belongs to, the
# per-workspace mirror is capped at 1 GiB with a 100 MiB ceiling per file that
# a single packfile clears on its own, and restoring half a repository is worse
# than restoring none. The cost is stated in the release note: a cloned repo
# comes back as a plain working tree, and ``git clone`` puts the history back.
EXCLUDE_DIR_NAMES: frozenset[str] = ALWAYS_HIDDEN_DIR_NAMES | {"__pycache__"}
# Ours, and reserved only where we put them. These are ordinary words, and
# matching them at any depth silently dropped a user's own ``work/model/tools/``
# or ``work/mcp_servers/`` from every scan and then pruned their rows.
EXCLUDE_ROOT_DIRS: tuple[str, ...] = tuple(
    sorted(BACKUP_EXCLUDE_DIRS | HIDDEN_DIR_NAMES)
)
# The skill reconciler's scratch space, which must never be restored on top
# of a live reconcile. Matched by workspace-relative path: the same names
# anywhere else (``work/model/.staging``) are the user's own directories.
SKILLS_DIR = SandboxLayout.SKILLS_DIR
# What the deferred restore passes placed or found in place, one file of
# NUL-ended paths per write (see DEFERRED_RESTORE_DIRS). It speaks for the sandbox it was written
# on: a copy restored onto the next would name paths that sandbox never received.
DEFERRED_LEDGER = f"{WorkspaceLayout.LARGE_TOOL_RESULTS_DIR}/.restored.d"
EXCLUDE_REL_DIRS: tuple[str, ...] = (
    *EXCLUDE_ROOT_DIRS,
    *BACKUP_EXCLUDE_AGENT_SUBDIRS,
    f"{SKILLS_DIR}/.staging",
    DEFERRED_LEDGER,
)
# The skill reconciler's trash and what a prune set aside: both on their way
# out, and a copy of either would come back with a restore.
EXCLUDE_REL_DIR_PREFIXES: tuple[str, ...] = (f"{SKILLS_DIR}/.trash-", THREAD_DIRS_SET_ASIDE)
# The reconciler's lock file, at its one path; a user's own
# ``results/.skills-sync.flock`` is a file like any other.
EXCLUDE_REL_FILES: tuple[str, ...] = (f"{SKILLS_DIR}/.skills-sync.flock",)
EXCLUDE_SUFFIXES: frozenset[str] = frozenset({".pyc", ".pyo", ".so", ".dylib", ".o"})
# ``__init__.py`` is NOT here: it is the file that makes a directory a Python
# package, and dropping it by basename cost the user their own packages on
# every sandbox rebuild.
EXCLUDE_BASENAMES: frozenset[str] = frozenset({".DS_Store", "Thumbs.db"})

SYNC_MARKER_NAME = ".file_sync_marker"

# Evicted tool results are read back rarely, and with the agent's scratchpads
# they are most of a long thread's bytes, so a restore brings both after the
# rest of the folder rather than before the first turn. The notes are the
# exception: a resumed turn reads them first, so they come with the folder and
# are not deferred at all (see is_deferred); once that pass is done, a missing
# one is one the turn deleted.
# The turn can still write to a scratchpad before the second pass reaches it,
# so a path the pass finds holding anything is left alone: it was written
# after the backup. The sandbox makes the same check as it places each entry,
# in one step with the placement, so one written while its backup copy
# downloads is kept as well. A path a pass placed or found there goes in
# DEFERRED_LEDGER, so a later look, by this pass, another worker's or a retry,
# reads its absence as the turn deleting it rather than as a file still to send. Their rows are pruned
# only once the sandbox holds DEFERRED_MARKER, which that pass writes when
# every one came back: until then a missing file is one still on its way, not
# one deleted. The scan has to list the marker to see it, so no sandbox-side
# exclusion may cover it; the server drops it from the manifest instead.
DEFERRED_RESTORE_DIRS: tuple[str, ...] = (
    WorkspaceLayout.LARGE_TOOL_RESULTS_DIR,
    WorkspaceLayout.SCRATCHPAD_DIR,
)
# Where the first deferred dir always had it, so a sandbox restored before
# the scratchpads were deferred still reads as done.
DEFERRED_MARKER = f"{WorkspaceLayout.LARGE_TOOL_RESULTS_DIR}/.restored"


def scratchpad_note_thread(path: str) -> str | None:
    """The thread whose checkpoint notes hold ``path``, or None for a path
    outside every thread's notes."""
    base = WorkspaceLayout.SCRATCHPAD_DIR + "/"
    thread, _, rest = path.removeprefix(base).partition("/")
    if path.startswith(base) and rest.split("/", 1)[0] == WorkspaceLayout.SCRATCHPAD_NOTE_DIR:
        return thread
    return None


def is_deferred(path: str) -> bool:
    """Whether the second restore pass brings ``path`` back."""
    return scratchpad_note_thread(path) is None and any(
        path == d or path.startswith(d + "/") for d in DEFERRED_RESTORE_DIRS
    )


# Bounded by the disk rather than the workspace's history: a scan hashes only
# what changed, the sandbox hashes at ~1.5 GB/s, and a tier's writable layer
# is the most it can ever hold, so even a cold tenth of that rate fits.
SCAN_TIMEOUT_S = 300
# A sweep reads metadata only (~185k entries/s on a one-CPU sandbox, measured),
# so this is a stuck exec, not a big computer.
SWEEP_TIMEOUT_S = 30
# How far before a scan's start its mark sits. An inode nobody has stat-ed
# lately takes its change time from the coarse clock, at most one tick behind
# the wall (10 ms at HZ=100; 1.6 ms measured on kernel 6.17 overlayfs), so a
# write just after the mark could read as older. Ten ticks of headroom. Wider
# is not safer, only costlier: a write inside the margin makes the project
# sync once more on the next pass, and turns often end with a write.
SCAN_MARK_MARGIN_NS = 100_000_000
# Transfer timeouts scale with bytes at a floor bandwidth so a large workspace
# on a slow link is not cut off, while an idle exchange still ends. The floor
# is deliberately pessimistic against a measured ~210 ms per PUT, so the
# ceiling is what an exchange this side has stopped believing in rather than
# what a legitimate transfer could need: at the floor it covers 7 GiB, and at
# the bandwidth actually seen, far more than any workspace holds.
TRANSFER_FLOOR_BYTES_PER_S = 1024 * 1024
TRANSFER_MIN_TIMEOUT_S = 300
TRANSFER_MAX_TIMEOUT_S = 7200

# The sandbox runs on a one-CPU quota, and the runtime's CPU per item grows
# with its thread count (a 300-file pull: 1.0 s of CPU at 16 threads, 3.5 s
# at 64) while the store answers in ~80 ms per GET and ~210 ms per PUT.
# These are the measured knees; wider pools throttle, and the tail grows.
PUSH_CONCURRENCY = 16
PULL_CONCURRENCY = 32
# What a pull may hold in temp files at once inside the sandbox. Every
# download lands beside its target and is renamed in only once it verifies,
# so a restore's transient disk cost is what is in flight, not what it
# finally places. The count alone bounded that at concurrency x file size,
# which no longer bounds anything now that the file size does not.
PULL_MAX_INFLIGHT_BYTES = 512 * 1024 * 1024

# Files at or below the cutoff travel as members of a pack: one object per
# chunk of the workspace instead of one per file. The transfer cost is per
# object, and the small files are most of the objects while being almost
# none of the bytes. The chunk cap bounds what one small edit re-uploads and
# what a restore holds in flight; chunks are written under PACK_DIR, which the
# scan already excludes, and are removed once pushed.
PACK_CUTOFF = 256 * 1024
#: Must stay under MULTIPART_THRESHOLD_BYTES below. A chunk is unlinked in the
#: sandbox the moment the store has it, and only a whole PUT is stored that
#: early: raising this past the threshold would drop the sandbox's only copy
#: of a chunk whose parts the server has yet to assemble.
PACK_MAX_BYTES = 32 * 1024 * 1024
#: What a backup stages on the sandbox's disk at once: the pack set is packed
#: and pushed a run of whole chunks at a time, each run's chunks gone before
#: the next is written. Staged whole, a backup needed free disk equal to every
#: small file in the workspace, so a nearly full disk could never back up.
PACK_STAGE_MAX_BYTES = 2 * PACK_MAX_BYTES
PACK_DIR = SandboxLayout.PACKS_DIR

# What one transfer may hold in memory at once on the paths that move bytes
# through this process. The direct path caps no file, so a count alone bounds
# nothing that matters: eight files is eight files whether they are 4 KiB or
# 256 MiB apiece. Each backup or restore takes its own budget, so a worker
# running several at once may hold a multiple of this.
INPROCESS_MAX_INFLIGHT_BYTES = 512 * 1024 * 1024

# A file at or above this is uploaded in parts rather than as one PUT.
# Nothing here is about what the store can hold in one object: a single PUT
# has no resume, so an interrupted one starts again from zero, and the larger
# the file the more likely it is interrupted. The store picks the part size
# against its own limits; this is only the point where splitting starts to
# pay for itself.
MULTIPART_THRESHOLD_BYTES = 100 * 1024 * 1024
# The S3 API's ceiling on one PUT; above it only a multipart upload stores.
SINGLE_PUT_MAX_BYTES = 5 * 1024**3


def transfer_mode(sandbox: Any) -> str:
    # PTCSandbox holds the whole CoreConfig; the provider name is on its
    # sandbox section. Anything else (a mock, a foreign runtime) reads as
    # an unknown provider and relays.
    config = getattr(sandbox, "config", None)
    section = getattr(config, "sandbox", None)
    provider = getattr(section, "provider", None)
    if not isinstance(provider, str):
        provider = None
    return get_blob_transfer_mode(provider)


def scan_cap_bytes(sandbox: Any, *, blobs_on: bool) -> int | None:
    """Largest file this deployment could store, or ``None`` when nothing bounds it.

    The limit belongs to the route the bytes take, not to the file: direct
    streams sandbox-to-store and is bounded only by the workspace disk, while
    the two paths that materialize the file in this process are bounded by
    whatever holds it. A scan cap is therefore a statement about the current
    deployment, and a file it rejects can never be stored *here*, which is
    what separates it from a push that merely failed this pass.
    """
    if not blobs_on:
        return INLINE_MAX_BYTES
    if transfer_mode(sandbox) == "direct":
        return None
    return RELAY_MAX_BYTES


class ByteBudget:
    """Admission for files held whole in this process, by weight and by count.

    Both bind and the tighter one wins: weight alone would admit thousands of
    tiny files and exhaust everything that is per-request rather than
    per-byte, while count alone is the bound that let an uncapped file
    through. A file too large for the whole budget runs alone rather than
    deadlocking behind a budget it can never fit.

    The sandbox runtime carries a twin of this by hand, since it ships as a
    stdlib-only script and cannot import it.
    """

    def __init__(self, max_bytes: int, max_files: int) -> None:
        self._max_bytes = max(1, int(max_bytes))
        self._max_files = max(1, int(max_files))
        self._bytes = 0
        self._files = 0
        # The count bound queues in FIFO order ahead of the byte check, so at
        # most ``max_files`` holders ever wait on the condition. Callers gather
        # every item at once, and waking all of them on each release made a
        # large transfer quadratic in its file count.
        self._slots = asyncio.Semaphore(self._max_files)
        self._cv = asyncio.Condition()

    @asynccontextmanager
    async def hold(self, size: int | None) -> AsyncIterator[None]:
        # An unknown weight is charged the whole budget, not nothing: a bound
        # that admits freely whenever it cannot measure an item is not a
        # bound. A measured zero still costs zero.
        want = (
            self._max_bytes
            if size is None
            else min(max(int(size), 0), self._max_bytes)
        )
        async with self._slots:
            async with self._cv:
                while self._files and self._bytes + want > self._max_bytes:
                    await self._cv.wait()
                self._files += 1
                self._bytes += want
            try:
                yield
            finally:
                # Released before any await: a cancelled holder (a client
                # that disconnected) can be cancelled again at the lock, and
                # a release lost there leaks its weight for the process's life.
                self._files -= 1
                self._bytes -= want
                await asyncio.shield(self._wake())

    async def _wake(self) -> None:
        async with self._cv:
            self._cv.notify_all()


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
    # The sandbox clock when the walk began, and the boot it belongs to:
    # the raw material of the project's scan mark.
    started_ns: int | None = None
    boot_id: str | None = None
    # Wall clock minus boot clock at the start: lets a sweep see a clock
    # stepped back since (see ScanMark).
    clock_offset_ns: int | None = None
    # The sandbox holds DEFERRED_MARKER: its deferred dirs are complete.
    deferred_restored: bool = False


@dataclass(slots=True)
class SweepResult:
    changed: list[str]
    unchanged: list[str]
    missing: list[str]
    visited: int
    walk_ms: int


def _transfer_roots(layout: WorkspaceLayout) -> dict[str, str]:
    """The walk root and the machine scratch root every transfer op needs.

    Packs are the machine's scratch and live under the computer's ``_internal``,
    which is why the second root is the computer and not the project folder.
    """
    return {"root": layout.workspace, "pack_root": layout.root}


def transfer_timeout_s(total_bytes: int) -> int:
    scaled = TRANSFER_MIN_TIMEOUT_S + total_bytes // TRANSFER_FLOOR_BYTES_PER_S
    return int(min(max(scaled, TRANSFER_MIN_TIMEOUT_S), TRANSFER_MAX_TIMEOUT_S))


def exclusion_spec(max_file_bytes: int | None) -> dict[str, Any]:
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


@lru_cache(maxsize=1)
def _runtime_digest() -> str:
    return hashlib.sha256(_TRANSFER_RUNTIME_SOURCE.read_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class ScanRules:
    """What a scan could see and store, as one fingerprint a mark can carry.

    A mark vouches that nothing changed since its scan, but only among the
    files that scan admitted. Narrow an exclusion or raise the cap (object
    storage switched on, a relay deployment moved to direct) and a file that
    was skipped becomes eligible with no change time moving, so a sweep
    would skip it for the sandbox's life. The runtime's own source is the
    third input: it holds the walk and the sweep's comparison, and hashing
    it retires every mark on a runtime change without a version constant
    someone has to remember to bump. Each retired mark costs one full sync,
    which is what every turn paid before the sweep.
    """

    max_file_bytes: int | None
    fingerprint: str

    @classmethod
    def of(cls, max_file_bytes: int | None) -> ScanRules:
        """The rules a scan under ``max_file_bytes`` runs under on this build."""
        canonical = json.dumps(
            {"exclusions": exclusion_spec(max_file_bytes), "runtime": _runtime_digest()},
            sort_keys=True,
            separators=(",", ":"),
        )
        return cls(max_file_bytes, hashlib.sha256(canonical.encode()).hexdigest()[:16])


def _script_path(sandbox: Any) -> str:
    layout = SandboxLayout(sandbox.working_dir)
    return f"{layout.internal_src}/{TRANSFER_RUNTIME_SANDBOX_NAME}"


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
    if runtime is None:
        # A lazy start names its sandbox before the runtime that reaches it is
        # attached. Typed, so a caller that did not wait for readiness meets a
        # sandbox condition rather than an AttributeError the funnel calls a bug.
        raise SandboxTransientError("Sandbox runtime is not attached yet")
    payload = json.dumps(spec, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(payload).decode("ascii")
    started = time.monotonic()
    # One path for every attempt: the runtime removes the spec file once it
    # has read it, and a rerun after a runtime upload reuses the name so a
    # copy a stale runtime never read is taken by the one that replaces it.
    layout = SandboxLayout(sandbox.working_dir)
    in_path = f"{layout.wsfiles}/{op}-{uuid.uuid4().hex}.json"

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
    max_file_bytes: int | None,
    layout: WorkspaceLayout,
    hash_files: bool = True,
) -> ScanResult:
    """Walk and hash one project folder. ``prior`` lets unchanged files skip hashing.

    ``hash_files=False`` lists without reading contents: a changed file then
    carries no digest, which suits only a caller that never stores the entry.

    The walk root is the project's folder rather than the machine: several
    projects share the root, each syncs under its own advisory lock, and a walk
    from the root would have each of them claim the others' files and prune its
    own manifest.
    """
    spec = exclusion_spec(max_file_bytes)
    spec["root"] = layout.workspace
    spec["prior"] = {p: list(v) for p, v in prior.items()}
    if not hash_files:
        spec["hash"] = False
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
        if e["path"] not in (SYNC_MARKER_NAME, DEFERRED_MARKER)
    ]
    return ScanResult(
        entries=entries,
        oversized=out.get("oversized", []),
        errors=out.get("errors", []),
        hashed=int(out.get("hashed") or 0),
        reused=int(out.get("reused") or 0),
        started_ns=out.get("started_ns") if isinstance(out.get("started_ns"), int) else None,
        boot_id=out.get("boot_id") or None,
        clock_offset_ns=(
            out.get("clock_offset_ns")
            if isinstance(out.get("clock_offset_ns"), int)
            else None
        ),
        deferred_restored=any(
            e["path"] == DEFERRED_MARKER for e in out.get("entries", [])
        ),
    )


@dataclass(frozen=True, slots=True)
class ScanMark:
    """What a clean pass vouches for: nothing in the project changed before ``ns``.

    ``boot_id`` and ``sandbox_id`` scope that claim. A change time means
    nothing across a reboot, and a container shares its host's boot id, so a
    recreated sandbox holding a restore can share the boot of the one that
    wrote the mark; only the sandbox id tells them apart. ``rules`` scopes it
    to the files the scan admitted (see ``ScanRules``). ``offset_ns`` is the
    wall clock's distance from the boot clock when the mark was taken: a
    change time is only comparable to ``ns`` while the wall clock has not
    been stepped back since, and the sweep checks exactly that. The runtime
    never sees the sandbox id or the rules: both are settled here, so the
    wire carries only the time, the boot and the offset.
    """

    ns: int
    boot_id: str
    sandbox_id: str
    rules: str
    offset_ns: int

    @classmethod
    def of(cls, scan: ScanResult, sandbox: Any, rules: ScanRules) -> ScanMark | None:
        """The mark a clean pass over ``scan`` records, or None when it cannot vouch.

        A runtime that predates the sweep reports no start time, and a sandbox
        without an id could be any sandbox; either way no mark is better than
        one a later sweep would trust wrongly. ``rules`` are the ones the scan
        ran under, not the current ones: the two only differ mid-deploy, and
        the mark must describe the pass that was made.
        """
        sandbox_id = getattr(sandbox, "sandbox_id", None)
        if (
            scan.started_ns is None
            or scan.clock_offset_ns is None
            or not scan.boot_id
            or not sandbox_id
        ):
            return None
        return cls(
            scan.started_ns - SCAN_MARK_MARGIN_NS,
            scan.boot_id,
            str(sandbox_id),
            rules.fingerprint,
            scan.clock_offset_ns,
        )

    @classmethod
    def trusted(
        cls, stored: dict[str, Any] | None, sandbox_id: str | None, rules: ScanRules
    ) -> ScanMark | None:
        """The stored mark if it vouches for ``sandbox_id``'s files under ``rules``.

        A mark from other rules, or from before marks carried rules or a
        clock offset, is not trusted: the project syncs in full once, and the
        new mark takes over.
        """
        if not stored or not sandbox_id or stored.get("sandbox_id") != sandbox_id:
            return None
        if stored.get("rules") != rules.fingerprint:
            return None
        ns, boot_id = stored.get("ns"), stored.get("boot_id")
        offset_ns = stored.get("offset_ns")
        if not isinstance(ns, int) or not boot_id or not isinstance(offset_ns, int):
            return None
        return cls(ns, boot_id, sandbox_id, rules.fingerprint, offset_ns)

    def as_json(self) -> dict[str, Any]:
        return {
            "ns": self.ns,
            "boot_id": self.boot_id,
            "sandbox_id": self.sandbox_id,
            "rules": self.rules,
            "offset_ns": self.offset_ns,
        }


@dataclass(frozen=True, slots=True)
class SweepTarget:
    """One project folder for a sweep; no mark means changed without a walk."""

    key: str
    root: str
    mark: ScanMark | None


async def sweep_projects(sandbox: Any, targets: list[SweepTarget]) -> SweepResult:
    """Which of ``targets`` changed since their marks, in one exec.

    List the likeliest-changed first: the walk of a changed project stops at
    its first newer entry. The walk honours the backup's own exclusions, so an
    entry the scan would skip never makes a project look changed.
    """
    spec = exclusion_spec(None)
    spec["projects"] = [
        {
            "key": t.key,
            "root": t.root,
            "mark": (
                {"ns": t.mark.ns, "boot_id": t.mark.boot_id, "offset_ns": t.mark.offset_ns}
                if t.mark
                else None
            ),
        }
        for t in targets
    ]
    out = await run_transfer_op(sandbox, "sweep", spec, timeout_s=SWEEP_TIMEOUT_S)
    if "changed" not in out:
        raise TransferRuntimeError(f"sweep returned no result: {out.get('error')}")
    return SweepResult(
        changed=list(out.get("changed") or ()),
        unchanged=list(out.get("unchanged") or ()),
        missing=list(out.get("missing") or ()),
        visited=int(out.get("visited") or 0),
        walk_ms=int(out.get("walk_ms") or 0),
    )


async def hash_one_file(
    sandbox: Any,
    path: str,
    *,
    prior: tuple[int, int, str] | None,
    layout: WorkspaceLayout,
) -> ScanEntry | None:
    """Stat and hash one file under the project folder; None when it is not a file.

    ``path`` is relative to ``layout.workspace``. ``prior`` is the manifest's
    (size, mtime_ns, sha256) and lets an unchanged file skip the read.
    """
    spec: dict[str, Any] = {**_transfer_roots(layout), "path": path}
    if prior is not None:
        spec["prior"] = list(prior)
    out = await run_transfer_op(sandbox, "hash", spec, timeout_s=SCAN_TIMEOUT_S)
    if out.get("status") != "ok" or not out.get("sha256"):
        return None
    return ScanEntry(
        path=path,
        kind="file",
        size=int(out.get("size") or 0),
        mtime_ns=int(out.get("mtime_ns") or 0),
        mode=int(out.get("mode") or 0),
        sha256=out["sha256"],
        symlink_target=None,
        is_binary=out.get("is_binary"),
    )


async def push_direct(
    sandbox: Any, items: list[dict[str, Any]], *, layout: WorkspaceLayout
) -> dict[str, dict[str, Any]]:
    """Upload ``items`` (path, sha256, size, url, headers) from the sandbox.

    Returns per-digest results: ``ok``, ``changed``, ``failed``, ``unreachable``.
    """
    if not items:
        return {}
    total = sum(int(i["size"]) for i in items)
    spec = {
        **_transfer_roots(layout),
        "concurrency": PUSH_CONCURRENCY,
        "timeout_s": TRANSFER_MIN_TIMEOUT_S,
        "items": items,
    }
    out = await run_transfer_op(
        sandbox, "push", spec, timeout_s=transfer_timeout_s(total)
    )
    return out.get("results", {})


async def pull_direct(
    sandbox: Any,
    items: list[dict[str, Any]],
    *,
    layout: WorkspaceLayout,
    defer_dir_modes: bool = False,
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
        **_transfer_roots(layout),
        "concurrency": PULL_CONCURRENCY,
        "max_inflight_bytes": PULL_MAX_INFLIGHT_BYTES,
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
    sandbox: Any,
    members: list[dict[str, Any]],
    *,
    layout: WorkspaceLayout,
    release: list[str] | None = None,
) -> dict[str, Any]:
    """Concatenate ``members`` (path, sha256, size) into chunk files in the sandbox.

    Returns ``{"chunks": [{path, sha256, size, members: [{path, offset, size,
    sha256}]}], "changed": [path, ...]}``. The chunks are then pushed like any
    other file; ``changed`` lists members whose bytes no longer matched.
    ``release`` names chunks that must be off the disk first; the op removes
    them and fails rather than pack beside one it could not.
    """
    if not members:
        return {"chunks": [], "changed": []}
    total = sum(int(m.get("size") or 0) for m in members)
    spec = {
        **_transfer_roots(layout),
        # ``out_dir`` rides along for a runtime that predates ``pack_root``.
        "out_dir": PACK_DIR,
        "max_bytes": PACK_MAX_BYTES,
        "members": members,
        "release": release or [],
    }
    out = await run_transfer_op(
        sandbox, "pack", spec, timeout_s=transfer_timeout_s(total)
    )
    return {"chunks": out.get("chunks") or [], "changed": out.get("changed") or []}


async def unlink_direct(
    sandbox: Any, paths: list[str], *, layout: WorkspaceLayout
) -> int:
    """Remove files under the project folder; returns how many were removed."""
    if not paths:
        return 0
    out = await run_transfer_op(
        sandbox,
        "unlink",
        {**_transfer_roots(layout), "paths": paths},
        timeout_s=60,
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
