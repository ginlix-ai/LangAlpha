"""The second half of an upload sent through object storage: assembling the
parts, pulling the object into the workspace folder, and cleaning up after.

The request that completes an upload starts its pull, and any worker's sweep
takes over one whose worker died, once its lease lapses. The row in
``workspace_file_uploads`` is the only record of where an upload stands; the
set of pulls running here only bounds how many this worker takes on.
"""

from __future__ import annotations

import asyncio
import logging
import random
import shlex
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import anyio
from fastapi import HTTPException

from ptc_agent.core.paths import TRANSIENT_ROOT_PREFIX
from ptc_agent.core.sandbox.retry import RetryPolicy
from src.config.settings import get_conversation_pool_max
from src.server.database import workspace_file_uploads as db
from src.server.database.workspace_file import WorkspaceSyncBusy, workspace_sync_lock
from src.server.models.workspace import served_from_mirror
from src.server.services.background_starts import schedule_start
from src.server.services.computer_disk import (
    CRITICAL_FREE_BYTES,
    disk_command,
    parse_df,
)
from src.server.services.persistence.transfer import pull_direct, transfer_timeout_s
from src.server.services.workspace_files import (
    acquire_sandbox_to_change,
    contained_target,
    not_running_for_upload,
    owned_workspace_on_computer,
    owner_layout,
    previous_dir_names_of,
    record_fs_bytes,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.error_sanitization import single_line
from src.utils.concurrency import cancel_and_join
from src.utils.storage import (
    abort_multipart_upload,
    complete_multipart_upload,
    delete_object,
    get_signed_url,
    is_storage_enabled,
    list_uploaded_parts,
    object_size,
)

logger = logging.getLogger(__name__)

_MB = 1024 * 1024

UPLOAD_FAILED = "Upload failed"
STORE_UNREACHABLE = "Could not reach file storage. Try again."

# Long enough to list the parts and assemble two gigabytes, which the store
# does without moving the bytes.
COMPLETE_LEASE = timedelta(minutes=5)
# Renewed every _HEARTBEAT_S while the pull runs, so only a dead worker lets
# it lapse.
PULL_LEASE = timedelta(minutes=2)
_HEARTBEAT_S = 30.0
# How often an upload in progress counts as use of its computer, so the idle
# sweep does not stop the machine under it.
KEEP_ALIVE_EVERY = timedelta(seconds=60)
# Well inside a heartbeat, so a slow provider call cannot hold the renew back
# until the lease lapses.
_KEEP_ALIVE_TIMEOUT_S = 20.0
MAX_PULL_ATTEMPTS = 5
_RETRY_STEP = timedelta(seconds=30)
_CLEANUP_HOLD = timedelta(minutes=5)
# Long after any client stopped polling the row.
_PRUNE_AFTER = timedelta(days=7)
# Each pull holds a pooled connection for the workspace's sync lock while it
# runs, so one worker takes on only a few; the rest wait for a sweep.
_LOCAL_PULLS_MAX = 4
# (upload id, attempt) of each pull running here.
_running_pulls: set[tuple[str, int]] = set()
# Slots promised to claims still waiting on the database, so claims made at
# once cannot together take on more than the worker has room for.
_reserved_slots = 0


def _local_pulls_max() -> int:
    """At most half the pool, which the lease heartbeat and every other
    request on the worker draw on too, and never none: a pull runs somewhere."""
    return min(_LOCAL_PULLS_MAX, max(1, get_conversation_pool_max() // 2))


# The runtime's answers (``wsfiles_transfer_runtime.py``) that no retry can
# change: the path names a folder with files in it, or leads out of the folder.
_POPULATED_DIRECTORY = "target is a populated directory"
_PATH_ESCAPES = "path escapes root"
_REFUSED_IN_SANDBOX = (_POPULATED_DIRECTORY, _PATH_ESCAPES)
# Where a pull fetches the file before placing it: at the folder's root, under
# the prefix a scan skips there and the next pull sweeps away.
_STAGING_PREFIX = f"{TRANSIENT_ROOT_PREFIX}upload-"


def _reserve_slots(wanted: int) -> int:
    """Promise up to ``wanted`` of this worker's free pull slots; how many it got."""
    global _reserved_slots
    granted = max(0, min(wanted, _local_pulls_max() - len(_running_pulls) - _reserved_slots))
    _reserved_slots += granted
    return granted


def _release_slots(count: int) -> None:
    global _reserved_slots
    _reserved_slots -= count


def not_enough_disk(free_bytes: int) -> str:
    return (
        "Not enough free disk space in this workspace for this file "
        f"({max(free_bytes, 0) // _MB} MB free)."
    )


async def free_disk_bytes(sandbox: Any, work_dir: str) -> int | None:
    """Free bytes on the filesystem holding ``work_dir``; None when unreadable.

    Unreadable is not a refusal: the pull writes to a temporary name and
    removes it when the disk fills, so the check only spares a doomed upload.
    """
    command, timeout = disk_command(work_dir, breakdown=False)
    try:
        result = await sandbox._runtime_call(
            sandbox.runtime.exec, command, timeout, retry_policy=RetryPolicy.SAFE
        )
    except Exception as e:
        logger.warning(f"Disk reading for an upload failed: {single_line(str(e))}")
        return None
    reading = parse_df(result.stdout or "")
    return reading.free_bytes if reading else None


def has_room(free_bytes: int, size: int) -> bool:
    return free_bytes - CRITICAL_FREE_BYTES >= size


def part_size_of(row: dict[str, Any], number: int) -> int:
    """Every part but the last is ``part_size``; the last is what remains."""
    if number < row["part_count"]:
        return int(row["part_size"])
    return int(row["size"]) - int(row["part_size"]) * (int(row["part_count"]) - 1)


# --- assembling ----------------------------------------------------------------


@dataclass
class Assembly:
    """How assembling went: ``ok``, or the parts still owed, or a store failure
    worth retrying (``unreachable``), or an object that is not what was sent
    (``mismatch``)."""

    ok: bool = False
    missing: list[int] = field(default_factory=list)
    unreachable: bool = False
    mismatch: bool = False


def _assemble(row: dict[str, Any]) -> Assembly:
    """Assemble the parts the store holds, checked against the row, not the client.

    The parts and their sizes come from the store's own listing, so a client
    cannot finish an upload short, long, or with parts it was never granted.
    An upload the store no longer knows may be one an earlier attempt
    assembled before its answer was lost, so the object's size settles it.
    """
    key, store_upload_id, size = row["object_key"], row["store_upload_id"], int(row["size"])
    parts = list_uploaded_parts(key, store_upload_id)
    if parts is None:
        held = object_size(key)
        if held is None:
            return Assembly(unreachable=True)
        return Assembly(ok=held == size, mismatch=held != size)
    by_number = {p["part_number"]: p for p in parts}
    missing = [
        n
        for n in range(1, int(row["part_count"]) + 1)
        if n not in by_number or by_number[n]["size"] != part_size_of(row, n)
    ]
    if missing:
        return Assembly(missing=missing)
    numbered = [(n, by_number[n]["etag"]) for n in range(1, int(row["part_count"]) + 1)]
    completed = complete_multipart_upload(key, store_upload_id, numbered)
    held = object_size(key)
    if held is None:
        return Assembly(unreachable=True)
    if held != size:
        return Assembly(mismatch=True) if completed else Assembly(unreachable=True)
    return Assembly(ok=True)


async def assemble_and_pull(row: dict[str, Any]) -> tuple[dict[str, Any] | None, Assembly]:
    """Assemble a ``completing`` row and start its pull; the row after, and how
    assembling went.

    Parts still owed or a store that did not answer hand the row back to its
    uploader, who can send them and complete again. The pull runs here when
    this worker has room for it, and otherwise is left for a sweep to claim.
    """
    upload_id = row["upload_id"]
    try:
        assembly = await asyncio.to_thread(_assemble, row)
    except BaseException:
        with anyio.CancelScope(shield=True):
            await db.reopen_upload(upload_id)
        raise
    if assembly.missing or assembly.unreachable:
        return await db.reopen_upload(upload_id), assembly
    if assembly.mismatch:
        logger.warning(f"Upload {upload_id} assembled to a size other than its own")
        ended = await db.end_upload(
            upload_id, from_states=("completing",), state="failed", error=UPLOAD_FAILED
        )
        if ended is not None:
            await clean_up(ended)
        return ended, assembly
    slot = _reserve_slots(1)
    try:
        started = await db.start_pull(upload_id, PULL_LEASE if slot else None)
    finally:
        _release_slots(slot)
    # Launched on the claim the row records, not the one asked for: the
    # database leaves the row due while its user has another pull running.
    if started is not None and started["lease_until"] is not None:
        launch_pull(started)
    return started, assembly


# --- pulling -------------------------------------------------------------------


class _Refused(Exception):
    """A pull no retry can rescue; ``message`` is what the uploader is shown."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class _ClaimLost(Exception):
    """Another attempt holds the row now; this one records nothing."""


def launch_pull(row: dict[str, Any]) -> None:
    # Keyed by the attempt: a claim this worker's sweep takes over from a pull
    # of its own whose renewals failed has to start, not be deduplicated
    # against that pull, which stops at its next heartbeat.
    claim = (row["upload_id"], int(row["attempts"]))
    _running_pulls.add(claim)
    task = schedule_start(f"workspace-upload:{claim[0]}:{claim[1]}", lambda: pull_upload(row))
    task.add_done_callback(lambda _t, c=claim: _running_pulls.discard(c))


async def count_as_use(workspace_id: str, sandbox_id: str) -> None:
    try:
        await WorkspaceManager.get_instance().keep_sandbox_alive(workspace_id, sandbox_id)
    except Exception as e:
        logger.debug(f"Upload did not count as activity: {e}")


async def _heartbeat(
    row: dict[str, Any], attempt: int, held: dict[str, Any], pull: asyncio.Task
) -> None:
    """Keep the claim, and count the pull as use of the computer while it runs.

    A claim lost to another worker stops ``pull``: the row is that worker's to
    pull now, and this one would only hold the folder's lock against it.
    """
    upload_id = row["upload_id"]
    while True:
        await asyncio.sleep(_HEARTBEAT_S)
        try:
            if not await db.renew_pull(upload_id, attempt, PULL_LEASE):
                logger.warning(f"Upload {upload_id} pull {attempt} lost its claim; stopping it")
                pull.cancel()
                return
            sandbox = held.get("sandbox")
            if sandbox is not None and await db.claim_touch(upload_id, KEEP_ALIVE_EVERY):
                await asyncio.wait_for(
                    WorkspaceManager.get_instance().keep_sandbox_alive(
                        row["workspace_id"], sandbox.sandbox_id, runtime=sandbox.runtime
                    ),
                    _KEEP_ALIVE_TIMEOUT_S,
                )
        except TimeoutError:
            logger.info(f"Upload {upload_id} keep-alive took over {_KEEP_ALIVE_TIMEOUT_S:.0f}s")
        except Exception as e:
            logger.debug(f"Upload {upload_id} heartbeat failed: {e}")


async def pull_upload(row: dict[str, Any]) -> None:
    """Run one claimed pull attempt to its end and record it.

    A shutdown, or a folder busy syncing, hands the attempt back uncounted for
    another worker to run; a failure counts against the row's attempts and
    waits a little longer each time before a sweep tries again. An attempt
    whose claim was lost stops where it is and records nothing.
    """
    upload_id, attempt = row["upload_id"], int(row["attempts"])
    held: dict[str, Any] = {}
    pull = asyncio.create_task(_pull_once(row, held))
    beat = asyncio.create_task(_heartbeat(row, attempt, held, pull))
    try:
        await pull
    except asyncio.CancelledError:
        caller = asyncio.current_task()
        if caller is not None and caller.cancelling() == 0:
            # Only the pull was cancelled, by the heartbeat: the row is no
            # longer this attempt's to record.
            return
        with anyio.CancelScope(shield=True):
            await db.release_pull(
                upload_id, attempt, retry_in=timedelta(0), refund=True
            )
        raise
    except _ClaimLost:
        logger.warning(f"Upload {upload_id} pull {attempt} lost its claim before placing")
        return
    except _Refused as e:
        logger.warning(f"Upload {upload_id} refused at pull: {e.message}")
        await _end_pull(upload_id, attempt, state="failed", error=e.message)
        return
    except WorkspaceSyncBusy:
        # A backup or a restore held the folder past the wait: nothing was
        # tried, so nothing is counted.
        logger.info(f"Upload {upload_id} pull {attempt} waited out a busy folder; trying later")
        await db.release_pull(upload_id, attempt, retry_in=_RETRY_STEP, refund=True)
        return
    except Exception as e:
        logger.warning(
            f"Upload {upload_id} pull {attempt}/{MAX_PULL_ATTEMPTS} failed: "
            f"{type(e).__name__}: {single_line(str(e))}"
        )
        # One that began placing earns the row one more attempt, which fetches
        # nothing: it settles by the folder what this one could not record.
        # The sweep's claims count the same.
        placing = held.get("placing") or row.get("placing_inode") is not None
        if attempt >= MAX_PULL_ATTEMPTS + (1 if placing else 0):
            await _end_pull(upload_id, attempt, state="failed", error=UPLOAD_FAILED)
        else:
            await db.release_pull(
                upload_id, attempt, retry_in=_RETRY_STEP * attempt, refund=False
            )
        return
    finally:
        await cancel_and_join(beat)
    if await _end_pull(upload_id, attempt, state="done"):
        record_fs_bytes("upload", int(row["size"]))
        logger.info(f"Upload {upload_id} placed in workspace {row['workspace_id']}")


async def _end_pull(
    upload_id: str, attempt: int, *, state: str, error: str | None = None
) -> bool:
    ended = await db.finish_pull(upload_id, attempt, state=state, error=error)
    if ended is None:
        return False
    await clean_up(ended)
    return True


async def _pull_once(row: dict[str, Any], held: dict[str, Any]) -> None:
    workspace_id, user_id = row["workspace_id"], row["user_id"]
    size = int(row["size"])
    try:
        current = await owned_workspace_on_computer(workspace_id, user_id)
    except HTTPException as e:
        # Deleted, or no longer this user's: nothing will take the file.
        if e.status_code in (400, 403, 404):
            raise _Refused(UPLOAD_FAILED) from None
        raise
    # Stopped since the upload began: it fails as the one-request path would
    # refuse it, rather than starting the computer again under its owner.
    if served_from_mirror(current.get("status")):
        raise _Refused(not_running_for_upload(current).detail)
    async with acquire_sandbox_to_change(workspace_id, user_id) as (sandbox, workspace):
        held["sandbox"] = sandbox
        layout = owner_layout(workspace)
        work_dir = layout.workspace
        # Resolved again rather than trusted from the start: the folder may
        # have been renamed since, or a link made on the way to the file.
        try:
            target = await contained_target(
                sandbox, row["path"], work_dir, previous_dir_names_of(workspace)
            )
        except HTTPException:
            raise _Refused(UPLOAD_FAILED) from None
        rel = target[len(work_dir.rstrip("/")) + 1 :]
        staged = f"{_STAGING_PREFIX}{row['upload_id']}"
        item: dict[str, Any] = {"kind": "file", "size": size, "mode": 0o644}
        if row.get("sha256"):
            item["sha256"] = row["sha256"]
        # Restores and backups pull into the same staging names at the folder's
        # root and each sweeps away the ones it did not make, so pulls into one
        # workspace take turns with them.
        async with workspace_sync_lock(workspace_id):
            # An earlier attempt began moving the file into place and its end
            # was never recorded: the folder settles it, since placing the file
            # again would write over whatever was done to it since.
            placed_inode = row.get("placing_inode")
            resuming = placed_inode is not None
            found = await _clear_staging(
                sandbox,
                work_dir,
                keep=staged if resuming else None,
                target=target,
                placed_inode=placed_inode,
            )
            if resuming and found == "placed":
                return
            if resuming and found != "staged":
                raise _Refused(UPLOAD_FAILED)
            if not resuming:
                # Measured after the sweep, which an earlier attempt's partial
                # or unplaced copy would otherwise count against.
                free = await free_disk_bytes(sandbox, work_dir)
                if free is not None and not has_room(free, size):
                    raise _Refused(not_enough_disk(free))
                url = await asyncio.to_thread(
                    get_signed_url, row["object_key"], transfer_timeout_s(size) + 300
                )
                if url is None:
                    raise RuntimeError("could not sign the object's download")
                # Fetched to a name of its own without the path's write lock,
                # whose lease is sized for one write, then placed under it: the
                # file tools hold it from an edit's read to its write, and a
                # rename landing in between would be written over with the
                # edit's stale content.
                fetched = await pull_direct(
                    sandbox, [{**item, "path": staged, "url": url}], layout=layout
                )
                _raise_unless_ok(row, fetched.get(staged))
            inode = await _inode_of(sandbox, f"{work_dir.rstrip('/')}/{staged}")
            if not await db.mark_placing(row["upload_id"], int(row["attempts"]), inode):
                raise _ClaimLost
            held["placing"] = True
            async with sandbox.path_write_lock(sandbox.normalize_path(target)):
                placed = await pull_direct(
                    sandbox, [{**item, "path": rel, "file": staged}], layout=layout
                )
            result = placed.get(rel) or {}
            if result.get("status") != "ok" and _refused_in_sandbox(result):
                # The row ends here, so nothing would place the fetched copy;
                # one a retry can still place stays for the next attempt.
                await _remove(sandbox, f"{work_dir.rstrip('/')}/{staged}")
    _raise_unless_ok(row, result)


async def _clear_staging(
    sandbox: Any,
    work_dir: str,
    *,
    keep: str | None,
    target: str,
    placed_inode: int | None = None,
) -> str:
    """Remove what earlier transfers left at the folder's root, all but
    ``keep``; then whether ``keep`` is there (``staged``), or else the file
    at ``target`` is ``placed_inode`` (``placed``), or neither (``none``).

    Under the sync lock every transient name at the root is a leftover, as
    the transfer runtime's own sweep holds. Only the inode tells a placed
    copy from the file it replaced, whose staged copy a sweep may have taken.
    """
    root = shlex.quote(work_dir)
    kept = shlex.quote(keep or "")
    dest = shlex.quote(target)
    inode = shlex.quote("" if placed_inode is None else str(int(placed_inode)))
    command = (
        f"cd -- {root} || exit 1; "
        f"for f in {TRANSIENT_ROOT_PREFIX}*; do "
        f'{{ [ -e "$f" ] || [ -L "$f" ]; }} || continue; '
        f'[ "$f" = {kept} ] && continue; rm -rf -- "$f"; done; '
        f"if [ -n {kept} ] && [ -f {kept} ] && [ ! -L {kept} ]; then echo staged; "
        f"elif [ -n {inode} ] && [ -f {dest} ] && [ ! -L {dest} ] "
        f"&& [ \"$(ls -di -- {dest} | awk '{{print $1}}')\" = {inode} ]; then echo placed; "
        f"else echo none; fi"
    )
    result = await sandbox._runtime_call(
        sandbox.runtime.exec, command, 60, retry_policy=RetryPolicy.SAFE
    )
    lines = (result.stdout or "").split()
    if result.exit_code != 0 or not lines or lines[-1] not in ("staged", "placed", "none"):
        raise RuntimeError(f"clearing staging failed: exit {result.exit_code}")
    return lines[-1]


async def _inode_of(sandbox: Any, path: str) -> int:
    result = await sandbox._runtime_call(
        sandbox.runtime.exec, f"ls -di -- {shlex.quote(path)}", 30, retry_policy=RetryPolicy.SAFE
    )
    first = (result.stdout or "").split()[:1]
    if result.exit_code != 0 or not first or not first[0].isdigit():
        raise RuntimeError(f"reading the fetched copy failed: exit {result.exit_code}")
    return int(first[0])


def _refused_in_sandbox(result: dict[str, Any]) -> bool:
    return result.get("status") == "mismatch" or result.get("error") in _REFUSED_IN_SANDBOX


def _raise_unless_ok(row: dict[str, Any], result: dict[str, Any] | None) -> None:
    result = result or {}
    status = result.get("status")
    if status == "ok":
        return
    if status == "mismatch":
        # The store holds other bytes than the upload declared; another pull
        # fetches the same ones.
        logger.warning(f"Upload {row['upload_id']} pulled other bytes: {result.get('error')}")
        raise _Refused(UPLOAD_FAILED)
    if result.get("error") in _REFUSED_IN_SANDBOX:
        logger.warning(f"Upload {row['upload_id']} refused in the sandbox: {result.get('error')}")
        raise _Refused(UPLOAD_FAILED)
    raise RuntimeError(f"pull {status}: {result.get('error')}")


async def _remove(sandbox: Any, path: str) -> None:
    try:
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"rm -f -- {shlex.quote(path)}",
            30,
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        logger.warning(f"Removing an upload's fetched copy failed: {single_line(str(e))}")


# --- cleaning up ---------------------------------------------------------------


def _clean_store(row: dict[str, Any]) -> bool:
    """Remove what the upload left in the store; whether nothing is left.

    An upload never assembled holds parts under its open upload, which only
    an abort removes, so an abort the store did not confirm leaves the row
    for the sweep to try again. The object itself goes either way, since an
    attempt may have assembled it unrecorded.
    """
    key = row["object_key"]
    if row["attempts"] == 0 and not abort_multipart_upload(key, row["store_upload_id"]):
        return False
    return delete_object(key)


async def clean_up(row: dict[str, Any]) -> None:
    """Clean an ended row's store bytes now; the sweep retries one that fails."""
    try:
        cleaned = await asyncio.to_thread(_clean_store, row)
    except Exception as e:
        logger.warning(f"Upload {row['upload_id']} cleanup failed: {e}")
        return
    if cleaned:
        await db.mark_cleaned(row["upload_id"])


# --- sweep ---------------------------------------------------------------------

SWEEP_INTERVAL_S = 60.0
SWEEP_BATCH_LIMIT = 25
STOP_GRACE = 30.0


async def sweep_once() -> None:
    """One pass over uploads a request will not finish.

    Every worker runs it; each step claims its rows in one conditional
    update, so a row is acted on by one worker at a time. A row whose step
    raises is logged and left for a later pass, never in the way of the rest.
    """
    for row in await db.expire_idle(SWEEP_BATCH_LIMIT):
        logger.info(f"Upload {row['upload_id']} expired while uploading")
    for row in await db.claim_lapsed(
        "completing", lease=COMPLETE_LEASE, limit=SWEEP_BATCH_LIMIT
    ):
        # A worker died assembling it: finish the job, or hand it back.
        await _sweep_step(row, "assembling", assemble_and_pull)
    for row in await db.fail_exhausted_pulls(
        MAX_PULL_ATTEMPTS, UPLOAD_FAILED, SWEEP_BATCH_LIMIT
    ):
        logger.warning(f"Upload {row['upload_id']} failed after {row['attempts']} pulls")
        await _sweep_step(row, "cleanup", clean_up)
    room = _reserve_slots(_local_pulls_max())
    try:
        lapsed = (
            await db.claim_lapsed(
                "pulling", lease=PULL_LEASE, limit=room, max_attempts=MAX_PULL_ATTEMPTS
            )
            if room
            else []
        )
    finally:
        _release_slots(room)
    for row in lapsed:
        try:
            launch_pull(row)
        except Exception:
            logger.error(f"Upload {row['upload_id']} pull did not start", exc_info=True)
    for row in await db.claim_waiting_touches(KEEP_ALIVE_EVERY, SWEEP_BATCH_LIMIT):
        schedule_start(
            f"workspace-upload-touch:{row['upload_id']}",
            lambda r=row: count_as_use(r["workspace_id"], r["sandbox_id"]),
        )
    for row in await db.claim_cleanups(_CLEANUP_HOLD, SWEEP_BATCH_LIMIT):
        await _sweep_step(row, "cleanup", clean_up)
    await db.prune_cleaned(_PRUNE_AFTER, SWEEP_BATCH_LIMIT * 4)


async def _sweep_step(row: dict[str, Any], what: str, step) -> None:
    try:
        await step(row)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.error(f"Upload {row['upload_id']} {what} failed in the sweep", exc_info=True)


class UploadSweeper:
    _instance: UploadSweeper | None = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> UploadSweeper:
        if cls._instance is None:
            cls._instance = UploadSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    def start(self) -> None:
        """Start the sweep loop; inert without object storage, where no upload goes through it."""
        if not is_storage_enabled():
            return
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._loop(), name="upload-sweeper")
        logger.info(f"[UploadSweeper] started (interval={self._interval:.0f}s)")

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning("[UploadSweeper] loop ended with an error at shutdown", exc_info=True)
        self._loop_task = None
        self._stop_event = asyncio.Event()

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            # Jitter first, so workers started by one deploy do not all claim
            # at once.
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except TimeoutError:
                pass
            try:
                await sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[UploadSweeper] sweep failed", exc_info=True)
