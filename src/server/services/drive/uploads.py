"""A Drive file sent in parts, so its bytes go between the client and object
storage and never sit whole in a worker.

Parts are kept under the upload's own key, never under a digest's name: the
client's word for the content's hash is not taken, and an object under a
digest's key is trusted by every reader of the user's blobs. Once every part
is in, the server reads the assembled object back, files it under the
digest it found, and records the version. That read takes as long as the
file is large, so it runs in the background behind a lease, and the client
polls. Each step may reach any worker: ``drive_uploads`` and the store are
the upload's only state.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import dataclasses
import hashlib
import logging
import re
import threading
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

from src.server.database.blob_keys import BlobError, upload_key
from src.server.database.drive import reads
from src.server.database.drive import uploads as rows
from src.server.database.drive.keys import normalize, path_key
from src.server.database.drive.writes import ContentGone, ContentRef
from src.server.database.workspace_file_blobs import (
    BlobUploadError,
    blob_size,
    release_blob,
    store_blob_by_copy,
)
from src.server.services import drive
from src.server.services.background_starts import schedule_start
from src.server.models.drive import UploadIn
from src.server.services.drive import content_bytes as _bytes
from src.server.services.drive.data_files import is_data_file
from src.server.services.livefs import outcomes
from src.server.services.store_multipart import (
    DIRECT_UPLOAD_MIN_BYTES,
    MAX_PARTS_PER_SIGN,
    PART_URL_TTL_S,
    PARTS_MISSING,
    UPLOAD_PART_BYTES,
    MultipartUpload,
    StoreUnreachable,
    assemble_parts,
    discard_upload,
    opened_upload,
    sign_parts,
)
from src.utils.storage import (
    is_storage_enabled,
    list_uploaded_parts,
    multipart_enabled,
    sha256_object,
    upload_part,
)

logger = logging.getLogger(__name__)

#: Uploads a person may have open at once; each holds parts in the store.
MAX_OPEN = 8
#: The store's ceiling on parts in one upload.
_MAX_PARTS = 10_000
_MIB = 1024 * 1024
#: The largest part a worker holds whole while relaying it: a relayed file
#: is at most this many times ``_MAX_PARTS`` (625 GiB).
RELAY_PART_MAX_BYTES = 64 * _MIB
#: Renewed every ``_HEARTBEAT_S`` while the object is read back, so only a
#: dead worker lets it lapse.
VERIFY_LEASE = timedelta(minutes=2)
_HEARTBEAT_S = 30.0
#: Tries at reading back and filing an assembled upload before it fails.
MAX_ATTEMPTS = 3
#: Read-backs one worker runs at once; each streams a whole file through it.
_VERIFIES = asyncio.Semaphore(2)
#: The slowest read-back taken for a live store, before it is called stalled.
_READ_FLOOR_BYTES_S = 4 * _MIB

_UNSTORED = "The file could not be stored now"
_UNREAD = "The stored file could not be read back"


class PartsMissing(drive.DriveError):
    """Completing found parts the store does not hold at their size."""

    def __init__(self, missing: list[int]) -> None:
        super().__init__("parts_missing", PARTS_MISSING, {"missing_parts": missing})


def part_size_for(size: int) -> int:
    """16 MiB, or the whole MiB that keeps the file within the store's parts."""
    least = -(-size // _MAX_PARTS)
    return max(UPLOAD_PART_BYTES, -(-least // _MIB) * _MIB)


async def _parts_held(row: dict[str, Any]) -> list[dict[str, Any]]:
    held = await asyncio.to_thread(list_uploaded_parts, row["object_key"], row["store_upload_id"])
    if held is None:
        raise drive.DriveError("unavailable", _UNSTORED)
    return [{"part_number": p["part_number"], "size": p["size"], "md5": _md5_of(p["etag"])} for p in held]


_MD5_HEX = re.compile(r"[0-9a-f]{32}")


def _md5_of(etag: str) -> str | None:
    """A held part's MD5, hex, when its ETag has an MD5's shape (32 hex
    characters); None otherwise, which the client takes as "cannot prove"
    and sends that part again. The shape is all that is checked: an ETag of
    another shape (a multipart or some S3-compatible stores' form) is caught,
    but an encrypted bucket's 32-hex ETag that is not the MD5 is not, so a
    client treats a part whose MD5 does not match as one to send again,
    never as another file."""
    tag = etag.strip('"').lower()
    return tag if _MD5_HEX.fullmatch(tag) else None


async def status(row: dict[str, Any], *, parts: bool = True) -> dict[str, Any]:
    state = row["state"]
    if state == "uploading" and row["expires_at"] <= datetime.now(row["expires_at"].tzinfo):
        # Past its deadline and not swept yet: a sign or a completion already
        # refuses it as ended, so it is not offered as one to continue.
        state = "aborted"
    out: dict[str, Any] = {
        "upload_id": row["upload_id"],
        "state": state,
        "mode": row["mode"],
        "path": row["path"],
        "size": row["size"],
        "part_size": row["part_size"],
        "part_count": row["part_count"],
        "expires_at": row["expires_at"].isoformat(),
    }
    if state == "uploading" and parts:
        out["parts_held"] = await _parts_held(row)
    if state == "done":
        # Null once the file it saved has been deleted or purged since.
        out["entry"] = row["entry_id"] and await reads.live_entry(
            row["space_id"], entry_id=row["entry_id"]
        )
    if state == "failed" and row["error"]:
        out["error"] = row["error"]
    return out


async def owned(space: drive.Space, upload_id: str) -> dict[str, Any]:
    """The upload ``upload_id`` names in ``space``; ``not_found`` otherwise,
    as for any id the caller cannot see."""
    try:
        upload_id = str(uuid.UUID(upload_id))
    except ValueError:
        raise drive.DriveError("not_found", "No such upload") from None
    row = await rows.get(upload_id)
    if row is None or row["space_id"] != space.space_id:
        raise drive.DriveError("not_found", "No such upload")
    return row


def _ended(state: str | None = None) -> drive.DriveError:
    return drive.DriveError(
        "upload_ended",
        "This upload has ended. Begin it again.",
        None if state is None else {"state": state},
    )


async def _open_row(space: drive.Space, upload_id: str) -> dict[str, Any]:
    row = await owned(space, upload_id)
    touched = await rows.touch(row["upload_id"])
    if touched is None:
        raise _ended()
    return touched


# --- begin -------------------------------------------------------------------


async def _held(space: drive.Space, sha: str, size: int) -> tuple[bool, str | None] | None:
    """Whether the person already holds ``sha`` at ``size``: ``(is_text,
    blob owner)`` when they do, the owner None for bytes the space holds."""
    found = await reads.content_by_sha(space.space_id, sha)
    if found is not None:
        return (found.is_text, None) if found.size == size else None
    if not is_storage_enabled() or size <= drive.TEXT_MAX_BYTES:
        # A small blob might be text, which only its bytes can tell.
        return None
    owner = space.user_id
    assert owner is not None
    if not await _bytes.registered(owner, sha):
        return None
    return (False, owner) if await blob_size(owner, sha) == size else None


def _kind_holds(name: str, is_text: bool, size: int) -> bool:
    """Whether a write of held bytes under ``name`` would keep their text
    flag (``kept_as_text``). With object storage a data file is never text,
    so text held under another name is not, and held non-text bytes small
    enough to be text may have been text kept as data: neither is reused,
    and the bytes are sent again for the name's own rule."""
    if not is_storage_enabled():
        return True
    if is_data_file(name):
        return not is_text
    return is_text or size > drive.TEXT_MAX_BYTES


async def begin(
    space: drive.Space,
    actor: drive.Actor,
    req: UploadIn,
    *,
    direct: bool,
    report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Answer how the file is to be sent, or record it when nothing need be.

    ``direct`` is whether the caller can reach the store. ``report`` names
    where the save's outcome is filed once it ends (``computer_id`` and
    ``call_id``, with the ``path`` the command sees).
    """
    bounds = await drive.limits(space)
    path = normalize(req.path)
    bounds.too_large(req.size)
    name = path.rsplit("/", 1)[-1]
    sha = (req.sha256 or "").lower() or None
    if sha is not None:
        held = await _held(space, sha, req.size)
        if held is not None and _kind_holds(name, held[0], req.size):
            is_text, owner = held
            ref = ContentRef(sha, req.size, is_text, _bytes.content_type(name, is_text, req.content_type))
            try:
                entry = await drive.save(
                    space,
                    path,
                    ref,
                    actor,
                    blob_user_id=owner,
                    if_match=req.if_match,
                    if_none_match=req.if_none_match,
                    note=req.note,
                    bounds=bounds,
                )
            except ContentGone:
                pass  # freed since it was found: the bytes are sent after all
            else:
                return {"mode": "held", "entry": entry}

    content_type = _bytes.content_type(name, False, req.content_type)
    # Named by a digest when one is given, so bytes the space holds already
    # count as they will; otherwise by a name no digest has.
    await drive.room_for(
        space,
        path,
        ContentRef(sha or f"upload:{uuid.uuid4()}", req.size, False, content_type),
        actor,
        if_match=req.if_match,
        if_none_match=req.if_none_match,
        bounds=bounds,
    )
    if not (is_storage_enabled() and multipart_enabled()) or req.size < DIRECT_UPLOAD_MIN_BYTES:
        bounds.too_large(req.size, bounds.max_request_bytes)
        return {"mode": "single", "max_bytes": bounds.max_request_bytes}

    relay = req.route == "relay" or not direct
    if sha is not None:
        found = await rows.find_open(
            space.space_id,
            path_key(path),
            req.size,
            sha,
            actor.user_id,
            content_type=content_type,
            if_match=req.if_match,
            if_none_match=req.if_none_match,
            actor=dataclasses.asdict(actor),
            note=req.note,
            report=report,
            relay=relay,
        )
        if found is not None:
            return {**await status(found), "resumed": True}

    owner = space.user_id
    assert owner is not None
    upload_id = str(uuid.uuid4())
    try:
        key = upload_key(owner, upload_id)
    except BlobError as e:
        raise drive.DriveError("unavailable", _UNSTORED) from e
    part_size = part_size_for(req.size)
    if relay:
        bounds.too_large(req.size, RELAY_PART_MAX_BYTES * _MAX_PARTS)
    try:
        async with opened_upload(key, content_type="application/octet-stream") as store_id:
            row = await rows.create(
                {
                    "upload_id": upload_id,
                    "space_id": space.space_id,
                    "user_id": actor.user_id,
                    "path": path,
                    "path_key": path_key(path),
                    "size": req.size,
                    "part_size": part_size,
                    "part_count": max(1, -(-req.size // part_size)),
                    "sha256": sha,
                    "content_type": content_type,
                    "note": req.note,
                    "if_match": req.if_match,
                    "if_none_match": req.if_none_match,
                    "actor": dataclasses.asdict(actor),
                    "mode": "relay" if relay else "direct",
                    "object_key": key,
                    "store_upload_id": store_id,
                    "report": report,
                },
                max_open=MAX_OPEN,
            )
    except StoreUnreachable:
        raise drive.DriveError("unavailable", _UNSTORED) from None
    except rows.TooManyUploads:
        raise drive.DriveError(
            "too_many_uploads", "Too many uploads in progress. Wait for one to finish."
        ) from None
    logger.info(
        "[Drive] upload %s begun: %d bytes in %d parts (%s)",
        upload_id,
        req.size,
        row["part_count"],
        row["mode"],
    )
    return {**await status(row, parts=False), "parts_held": [], "resumed": False}


# --- parts ---------------------------------------------------------------------


def _md5_b64(value: str | None) -> str | None:
    """A part's MD5 as sent (base64), checked for shape; None for none."""
    if not value:
        return None
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raw = b""
    if len(raw) != 16:
        raise drive.DriveError("bad_digest", "A part's MD5 is 16 bytes, in base64")
    return value


def _number(row: dict[str, Any], number: int) -> int:
    if not 1 <= number <= row["part_count"]:
        raise drive.DriveError("bad_part", f"Parts are numbered 1 to {row['part_count']}")
    return number


async def sign(
    space: drive.Space, upload_id: str, parts: list[dict[str, Any]], *, for_browser: bool
) -> dict[str, Any]:
    """Signed PUT addresses for parts sent straight to the store, signed for
    the browser's endpoint when ``for_browser``."""
    if not 1 <= len(parts) <= MAX_PARTS_PER_SIGN:
        raise drive.DriveError("bad_part", f"Sign 1 to {MAX_PARTS_PER_SIGN} parts at a time")
    row = await _open_row(space, upload_id)
    md5s = {_number(row, int(p["part_number"])): _md5_b64(p.get("md5")) for p in parts}
    found = await asyncio.to_thread(
        sign_parts, MultipartUpload.of_row(row), list(md5s), for_browser=for_browser, md5s=md5s
    )
    if found is None:
        raise drive.DriveError("unavailable", _UNSTORED)
    return {"parts": found, "expires_in": PART_URL_TTL_S}


async def _until_returned[T](fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """``fn`` in a thread, awaited until it returns even when the caller is
    cancelled: a thread cannot be stopped, so what it holds (a relayed part,
    under the held-bytes budget) is let go only once it is done with it."""
    call = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
    try:
        return await asyncio.shield(call)
    except asyncio.CancelledError:
        await asyncio.wait({call})
        if not call.cancelled():
            call.exception()  # seen, so an error it ended with is not logged as lost
        raise


async def relay_part(
    space: drive.Space,
    upload_id: str,
    number: int,
    md5: str | None,
    read: Callable[[int], Awaitable[bytes]],
) -> dict[str, Any]:
    """Pass one part sent to the server into the store's upload.

    ``read(cap)`` reads the request body, stopping past ``cap``. The part is
    held whole, under the worker's held-bytes budget, for as long as the
    store takes to accept it: one part, never the file.
    """
    row = await _open_row(space, upload_id)
    number = _number(row, number)
    md5 = _md5_b64(md5)
    size = MultipartUpload.of_row(row).part_size_of(number)
    if size > RELAY_PART_MAX_BYTES:
        raise drive.DriveError(
            "too_large", f"A part sent through the server is at most {RELAY_PART_MAX_BYTES // _MIB} MB"
        )
    async with drive.holding(size):
        data = await read(size)
        if len(data) != size:
            raise drive.DriveError("bad_part", f"Part {number} is {size} bytes, not {len(data)}")
        digest = await asyncio.to_thread(lambda: hashlib.md5(data).digest())
        if md5 is not None and base64.b64decode(md5) != digest:
            raise drive.DriveError("bad_digest", f"Part {number} does not match its Content-MD5")
        etag = await _until_returned(
            upload_part,
            row["object_key"],
            row["store_upload_id"],
            part_number=number,
            data=data,
            content_md5=base64.b64encode(digest).decode("ascii"),
        )
    if etag is None:
        raise drive.DriveError("unavailable", _UNSTORED)
    return {"part_number": number, "size": size, "md5": digest.hex()}


async def cancel(space: drive.Space, upload_id: str) -> dict[str, Any]:
    """End an upload that has not recorded its version, while it is sent or
    read back: the worker reading it loses its lease, so the version it was
    about to record is never committed. The status, ``aborted``; refused
    ``upload_ended`` (with its ``state``) once it has ended, done or not."""
    row = await owned(space, upload_id)
    ended = await rows.end(row["upload_id"], "aborted", from_states=("uploading", "verifying"))
    if ended is None:
        again = await rows.get(row["upload_id"]) or row
        raise _ended(again["state"])
    await _clean(ended)
    return await status(ended)


# --- completing ------------------------------------------------------------------


async def complete(space: drive.Space, upload_id: str) -> dict[str, Any]:
    """Assemble the parts the store holds and start filing the file; the
    upload's status. Parts still owed hand it back to its sender."""
    row = await owned(space, upload_id)
    if row["state"] != "uploading":
        return await poll(row)
    claimed = await rows.claim_verify(row["upload_id"], VERIFY_LEASE)
    if claimed is None:
        again = await rows.get(row["upload_id"]) or row
        if again["state"] == "uploading":
            raise _ended(again["state"])
        return await poll(again)
    lease = _Lease(claimed)
    try:
        async with lease.kept():
            assembled = await _assemble(lease, reopen_on_error=True)
    except BaseException:
        await rows.reopen(lease.upload_id, lease.token)
        raise
    if assembled:
        _start({**claimed, "assembled": True})
    return await status(await rows.get(lease.upload_id) or claimed)


async def poll(row: dict[str, Any]) -> dict[str, Any]:
    """An upload's status; a ``verifying`` one whose worker died is taken over."""
    if row["state"] == "verifying":
        await _take_over(row["upload_id"])
    return await status(row)


def _start(row: dict[str, Any]) -> None:
    schedule_start(f"drive-upload:{row['upload_id']}", lambda: _finish(row))


async def _take_over(upload_id: str) -> None:
    """Take over a ``verifying`` upload whose lease lapsed; past its tries
    (each worker that died reading it counts) it fails instead."""
    taken = await rows.reclaim(upload_id, VERIFY_LEASE)
    if taken is None:
        return
    if taken["attempts"] > MAX_ATTEMPTS:
        await _fail(taken, "unavailable", _UNREAD)
        return
    logger.info(
        "[Drive] upload %s taken over from a lapsed lease (%s)",
        upload_id,
        "assembled" if taken["assembled"] else "not assembled",
    )
    _start(taken)


async def _fail(row: dict[str, Any], code: str, message: str) -> None:
    """End a ``verifying`` upload failed, while ``row``'s lease holds it."""
    ended = await rows.end(
        row["upload_id"],
        "failed",
        from_states=("verifying",),
        error={"type": code, "message": message},
        token=row["lease_token"],
    )
    if ended is not None:
        logger.info("[Drive] upload %s failed: %s", row["upload_id"], code)
        # Only the request that ended it gets here, so it is filed once.
        await _report(ended, {"ok": False, "code": code, "error": message})
        await _clean(ended)


async def _report(row: dict[str, Any], outcome: dict[str, Any]) -> None:
    """File how a save in parts ended for the command that began it, if one
    did: its tool result says so even when nobody reads the upload again."""
    report = rows.report_of(row)
    if not report or not report.get("computer_id"):
        return
    try:
        await outcomes.record(
            report["computer_id"],
            report.get("call_id"),
            {"op": "write", "path": report.get("path"), **outcome},
        )
    except Exception:  # noqa: BLE001 - the save stands; only its notice is lost
        logger.warning("[Drive] upload %s outcome not filed", row["upload_id"], exc_info=True)


async def _retry_or_fail(row: dict[str, Any], message: str) -> None:
    """A store that did not answer is tried again on the next poll or sweep,
    up to ``MAX_ATTEMPTS``: the parts are in, and resending them would cost
    the client the whole file."""
    if row["attempts"] >= MAX_ATTEMPTS:
        await _fail(row, "unavailable", message)
        return
    if await rows.release(row["upload_id"], row["lease_token"]):
        logger.info("[Drive] upload %s will be tried again: %s", row["upload_id"], message)
    else:
        logger.info("[Drive] upload %s lost its lease; nothing recorded", row["upload_id"])


async def _clean(row: dict[str, Any]) -> None:
    """Remove what an ended upload left in the store; the sweep retries."""
    try:
        cleaned = await asyncio.to_thread(
            discard_upload, row["object_key"], row["store_upload_id"], assembled=row["assembled"]
        )
    except Exception as e:  # noqa: BLE001 - the sweep tries again
        logger.warning("[Drive] upload %s cleanup failed: %s", row["upload_id"], e)
        return
    if cleaned:
        await rows.mark_cleaned(row["upload_id"])


class _LeaseLost(Exception):
    """The lease on an upload went to another worker, or the upload was
    cancelled: whatever this worker was about to record is not recorded."""


class _Lease:
    """This worker's hold on a ``verifying`` upload. A heartbeat renews it;
    a renewal that finds it gone marks it lost, and the steps check that
    before each change, so a lost lease stops the work where it can and
    the token in every write stops it where it cannot."""

    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row
        self.upload_id: str = row["upload_id"]
        self.token: str = row["lease_token"]
        self.lost = asyncio.Event()

    async def _beat(self) -> None:
        """A renewal that fails (the database) is tried again at the next
        beat; once the last proof that this worker holds the lease is older
        than the lease less a beat, another may hold it, so it is lost."""
        loop = asyncio.get_running_loop()
        proven = loop.time()
        while True:
            await asyncio.sleep(_HEARTBEAT_S)
            try:
                held = await rows.renew(self.upload_id, self.token, VERIFY_LEASE)
            except Exception:  # noqa: BLE001 - tried again at the next beat
                logger.warning("[Drive] upload %s lease not renewed", self.upload_id, exc_info=True)
                if loop.time() - proven >= VERIFY_LEASE.total_seconds() - _HEARTBEAT_S:
                    self.lost.set()
                    return
                continue
            if not held:
                self.lost.set()
                return
            proven = loop.time()

    @contextlib.asynccontextmanager
    async def kept(self) -> AsyncIterator[None]:
        beat = asyncio.create_task(self._beat())
        try:
            yield
        finally:
            beat.cancel()

    def check(self) -> None:
        if self.lost.is_set():
            raise _LeaseLost()

    async def unless_lost(self, work: Awaitable[Any]) -> Any:
        """``work``'s answer, or ``_LeaseLost`` as soon as the lease goes;
        for reads, which are safe to leave running."""
        task = asyncio.ensure_future(work)
        lost = asyncio.ensure_future(self.lost.wait())
        try:
            await asyncio.wait({task, lost}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            lost.cancel()
        if not task.done():
            task.cancel()
            raise _LeaseLost()
        return task.result()


async def _assemble(lease: _Lease, *, reopen_on_error: bool) -> bool:
    """Assemble the parts the store holds, if no earlier attempt recorded
    that it did; whether the upload goes on to be read back. Parts still
    owed hand it back to its sender. ``reopen_on_error``: the request that
    completes it answers a store that did not answer by handing it back;
    a takeover tries again later instead."""
    if lease.row["assembled"]:
        return True
    row = lease.row
    assembly = await asyncio.to_thread(assemble_parts, MultipartUpload.of_row(row))
    if assembly.missing:
        await rows.reopen(lease.upload_id, lease.token)
        raise PartsMissing(assembly.missing)
    if assembly.unreachable:
        if reopen_on_error:
            await rows.reopen(lease.upload_id, lease.token)
            raise drive.DriveError("unavailable", _UNSTORED)
        await _retry_or_fail(row, _UNSTORED)
        return False
    if assembly.mismatch:
        await _fail(row, "unavailable", "The parts did not make the file's size")
        return False
    return await rows.assembled(lease.upload_id, lease.token)


def _read_deadline(size: int) -> float:
    return 120 + size / _READ_FLOOR_BYTES_S


async def _file_under_digest(owner: str, row: dict[str, Any], sha: str) -> datetime | None:
    """Put the assembled object under ``sha``'s key unless the user holds
    it; when this put it there, the registration's stamp, which lets a save
    refused after it let the registration go (``release_blob``)."""
    if await _bytes.registered(owner, sha):
        return None
    try:
        return await store_blob_by_copy(owner, sha, row["object_key"], row["size"])
    except BlobUploadError:
        raise drive.DriveError("unavailable", _UNSTORED) from None


async def _finish(row: dict[str, Any]) -> None:
    """Assemble the parts if that step is unfinished, read the object back,
    file it by its digest and record the version, all under the row's lease.
    The version and the upload's end commit together, and only while the
    lease holds: a worker that lost it records nothing."""
    lease = _Lease(row)
    copied: list[tuple[str, str, datetime]] = []
    try:
        async with lease.kept():
            if not await _assemble(lease, reopen_on_error=False):
                return
            lease.check()
            async with _VERIFIES:
                lease.check()
                stop = threading.Event()
                reading = asyncio.ensure_future(
                    asyncio.to_thread(
                        sha256_object, row["object_key"], _read_deadline, stop
                    )
                )
                try:
                    sha = await lease.unless_lost(asyncio.shield(reading))
                finally:
                    # The slot is the thread's until it stops: told to, it
                    # stops at its next chunk.
                    stop.set()
                    await asyncio.wait({reading})
            if sha is None:
                await _retry_or_fail(row, _UNREAD)
                return
            if row["sha256"] is not None and row["sha256"] != sha:
                await _fail(row, "mismatch", "The file that arrived is not the one begun")
                return
            space = await reads.get_space(row["space_id"])
            if space is None or space.user_id is None:
                await _fail(row, "not_found", "No such space")
                return
            await _record(lease, space, sha, copied)
    except _LeaseLost:
        logger.info("[Drive] upload %s lost its lease; nothing recorded", row["upload_id"])
        await _unfile(copied)
    except PartsMissing:
        logger.info("[Drive] upload %s handed back for missing parts", row["upload_id"])
    except _Refused:
        await _unfile(copied)
    except drive.DriveError as e:
        await _unfile(copied)
        await _retry_or_fail(row, e.message)
    except Exception:
        # Anything else (the database, a bug) counts as a try too, so a file
        # that always fails ends failed instead of being read back forever.
        logger.warning("[Drive] upload %s could not be filed", row["upload_id"], exc_info=True)
        await _unfile(copied)
        await _retry_or_fail(row, _UNSTORED)


class _Refused(Exception):
    """The save was refused and the upload ended for it."""


async def _record(
    lease: _Lease, space: drive.Space, sha: str, copied: list[tuple[str, str, datetime]]
) -> None:
    """Record the version; ``copied`` gains the registration this made, for
    the caller to take back if the version is not recorded."""
    row = lease.row
    owner = space.user_id
    assert owner is not None
    ref = ContentRef(sha, row["size"], False, row["content_type"])
    actor = drive.Actor(**rows.actor_of(row))

    async def done(tx: Any, entry: drive.Entry) -> None:
        if not await rows.mark_done(tx.cur, lease.upload_id, lease.token, entry.entry_id):
            raise _LeaseLost()  # rolls the version back with the transaction

    try:
        # The commit's own checks, before the bytes are copied under their
        # digest, so a save bound to be refused copies nothing.
        await drive.room_for(
            space,
            row["path"],
            ref,
            actor,
            if_match=row["if_match"],
            if_none_match=row["if_none_match"],
            bounds=await drive.limits(space),
        )
        lease.check()
        stamp = await _file_under_digest(owner, row, sha)
        if stamp is not None:
            copied.append((owner, sha, stamp))
        lease.check()
        entry = await drive.save(
            space,
            row["path"],
            ref,
            actor,
            blob_user_id=owner,
            if_match=row["if_match"],
            if_none_match=row["if_none_match"],
            note=row["note"],
            then=done,
        )
    except drive.DriveError as e:
        if e.code == "unavailable":
            raise
        await _fail(row, e.code, e.message)
        raise _Refused() from None
    logger.info("[Drive] upload %s recorded as %s", lease.upload_id, entry.entry_id)
    # Marked done in the version's own transaction: one worker gets here.
    ended = await rows.get(lease.upload_id)
    if ended is not None:
        await _report(ended, {"ok": True, "size": row["size"]})
        await _clean(ended)


async def _unfile(copied: list[tuple[str, str, datetime]]) -> None:
    """Let go of a registration this upload made for a version that was
    never recorded, unless something has used it since."""
    for owner, sha, stamp in copied:
        try:
            await release_blob(owner, sha, stamp)
        except Exception:  # noqa: BLE001 - the blob sweep collects it after its grace
            logger.warning("[Drive] blob %s not released", sha, exc_info=True)


# --- sweep -------------------------------------------------------------------

_SWEEP_LIMIT = 100
_PRUNE_AFTER = timedelta(days=7)


async def sweep_once() -> None:
    """End uploads past their deadline, take over ones whose worker died,
    and remove what ended ones left in the store."""
    for row in await rows.expired(_SWEEP_LIMIT):
        await _clean(row)
    for upload_id in await rows.lapsed(_SWEEP_LIMIT):
        await _take_over(upload_id)
    for row in await rows.uncleaned(_SWEEP_LIMIT):
        await _clean(row)
    await rows.prune(_PRUNE_AFTER)

