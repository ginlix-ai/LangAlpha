"""Chat composer attachments: uploaded when added, placed when the message is sent.

An attachment needs no workspace and no running computer, so the composer can
upload a file before it knows where the message goes, or while the computer
is stopped; the turn that sends the message places the file in its
workspace. The server decides how the bytes travel and where they wait: in
the store, sent there by the browser in parts (``direct``) or through this
process in one request (``relay``), or, on a deployment without object
storage, in Postgres until the turns that use them are past retrying.

Each step may reach any worker, so the row in ``chat_attachments`` is an
attachment's only state, and the sweep every worker runs settles what no
request will. Every step here raises an :class:`AttachmentRefused` for the
router to answer; nothing here speaks HTTP.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any, Literal
from uuid import uuid4

from src.server.database import chat_attachments as db
from src.server.database.blob_keys import RELAY_MAX_BYTES, attachment_key
from src.server.database.chat_attachments import ChatAttachmentRow
from src.server.models.chat_attachment import declared_type, served_type
from src.server.database.workspace import get_workspace
from src.server.services.persistence.transfer import held_bytes_budget, kind_pulls_from_store
from src.server.services.store_multipart import (
    DIRECT_UPLOAD_MIN_BYTES,
    IDLE_TTL,
    MAX_DIRECT_UPLOAD_BYTES,
    PARTS_MISSING,
    UPLOAD_PART_BYTES,
    MultipartUpload,
    StoreUnreachable,
    assemble_parts,
    discard_upload,
    opened_upload,
    part_count_for,
    shown_state,
    sign_parts,
)
from src.server.services.sweep_loop import JitteredSweeper
from src.server.utils.uploads import _too_large
from src.utils.storage import (
    browser_uploads_enabled,
    delete_object,
    is_storage_enabled,
    upload_bytes,
)

logger = logging.getLogger(__name__)

_MB = 1024 * 1024

# Without object storage the bytes wait in a Postgres BYTEA, which every
# backup and replica carries until the row goes, so the cap is well under the
# relay's.
POSTGRES_ATTACHMENT_MAX_BYTES = 50 * _MB
# All of one user's attachment bytes Postgres holds at once, sent or not.
POSTGRES_HELD_MAX_BYTES_PER_USER = 512 * _MB
# A sent attachment's bytes stay in Postgres this long, for a retry of the
# turn that used it; they count against the cap above until they go.
POSTGRES_SENT_CONTENT_RETENTION = timedelta(days=1)
# All of one user's bytes in the store that no message has used yet: the
# cap on uploads in progress bounds how many go up at once, not how many
# pile up waiting, each kept a day.
STAGED_MAX_BYTES_PER_USER = 8 * 1024 * _MB
# Each holds a multipart upload open in the store, or a request's worth of
# bytes in flight, until it ends.
MAX_OPEN_ATTACHMENTS_PER_USER = 16

# A relay upload's bytes all arrive before its request reaches the handler,
# so nothing moves its deadline while they do: it starts with time for the
# whole file at this rate, on top of the idle allowance. A direct upload
# moves its own deadline each time it signs parts.
RELAY_FLOOR_BYTES_PER_S = 32 * 1024

# Ready but never sent: the message was discarded, or the tab closed.
UNSENT_TTL = timedelta(hours=24)
# A sent attachment's row outlasts the retries of the turn that used it by
# this long. The store object stays: it is the copy the thread's history shows.
SENT_RETENTION = timedelta(days=7)
# An aborted row answers a client still asking about it for this long.
_ABORTED_PRUNE_AFTER = timedelta(days=1)
_CLEANUP_HOLD = timedelta(minutes=5)

ATTACHMENT_ENDED = "This upload has ended. Attach the file again."
TOO_MANY_ATTACHMENTS = "Too many files are uploading. Wait for one to finish."
TOO_MANY_HELD_BYTES = (
    "Attached files already fill the space kept for them. Remove some, or try again later."
)
TOO_MANY_STAGED_BYTES = (
    "Too many attached files are waiting to be sent. Send or remove some first."
)
SIZE_CHANGED = "The file sent is not the size given when its upload began."
NOT_DIRECT = "This upload takes the file in one request."
NOT_RELAY = "This upload takes the file in parts."
ARRIVED_DAMAGED = "The file did not arrive intact. Attach it again."
_EXPIRED_UPLOADING = "The upload stopped before the whole file arrived."
_EXPIRED_UNSENT = "The file was not sent with a message in time."

Mode = Literal["relay", "direct"]



# --- refusals ----------------------------------------------------------------------


class AttachmentRefused(Exception):
    """A step the attachment refuses; ``detail`` is what the uploader is shown."""

    def __init__(self, detail: Any) -> None:
        super().__init__(detail)
        self.detail = detail


class AttachmentNotFound(AttachmentRefused):
    def __init__(self) -> None:
        super().__init__("Attachment not found")


class AttachmentEnded(AttachmentRefused):
    def __init__(self) -> None:
        super().__init__(ATTACHMENT_ENDED)


class WrongPath(AttachmentRefused):
    """The step belongs to the other way of sending the file."""


class PartOutOfRange(AttachmentRefused):
    def __init__(self) -> None:
        super().__init__("Part number out of range")


class PartsMissing(AttachmentRefused):
    def __init__(self, missing: list[int]) -> None:
        super().__init__({"message": PARTS_MISSING, "missing_parts": missing})


class TooLarge(AttachmentRefused):
    pass


class SizeChanged(AttachmentRefused):
    """Fewer bytes arrived than the upload began with."""

    def __init__(self) -> None:
        super().__init__(SIZE_CHANGED)


class SentTooMuch(AttachmentRefused):
    """More bytes arrived than the upload began with."""

    def __init__(self) -> None:
        super().__init__(SIZE_CHANGED)


class TooMany(AttachmentRefused):
    """The user holds as many attachments, or as many bytes, as one may."""


# --- rules ---------------------------------------------------------------------


async def computer_takes_large(user_id: str, workspace_id: str) -> bool:
    """Whether the workspace's computer can receive a file larger than the
    server copies: only one that fetches it from the store can.

    Read from its row, so nothing is woken. A workspace not known to be the
    user's, or not yet on a computer, gets the benefit of the doubt: the turn
    tells the agent if the file could not be placed.
    """
    try:
        workspace = await get_workspace(workspace_id)
    except Exception:
        return True
    if workspace is None or workspace.get("user_id") != user_id:
        return True
    kind = workspace.get("computer_kind")
    return kind is None or kind_pulls_from_store(kind)


def _initial_ttl(mode: Mode, size: int) -> timedelta:
    if mode == "direct":
        return IDLE_TTL
    return IDLE_TTL + timedelta(seconds=size / RELAY_FLOOR_BYTES_PER_S)


async def check_fits(attachment_id: str, user_id: str, workspace_id: str) -> dict[str, Any]:
    """Whether a staged file can go to ``workspace_id``, for a composer whose
    target changed, or became known, after the file began: refused as
    :func:`begin` would have refused it."""
    row = await owned(attachment_id, user_id)
    if row["size"] > RELAY_MAX_BYTES and not await computer_takes_large(user_id, workspace_id):
        raise TooLarge(_too_large_for_computer(row["size"]))
    return status(row)


def _too_large_for_computer(size: int) -> str:
    return (
        f"File is too large for this workspace's computer ({size // _MB} MB). "
        f"It takes files up to {RELAY_MAX_BYTES // _MB} MB."
    )


def plan_upload(size: int) -> tuple[Mode, int]:
    """How a file of ``size`` bytes travels and the most that path takes.

    Raises:
        TooLarge: Past the most any path takes on this deployment, which is
            what the refusal names.
    """
    if not is_storage_enabled():
        largest = POSTGRES_ATTACHMENT_MAX_BYTES
        plan: tuple[Mode, int] = ("relay", largest)
    elif browser_uploads_enabled():
        largest = MAX_DIRECT_UPLOAD_BYTES
        plan = ("direct", largest) if size >= DIRECT_UPLOAD_MIN_BYTES else ("relay", RELAY_MAX_BYTES)
    else:
        largest = RELAY_MAX_BYTES
        plan = ("relay", largest)
    if size > largest:
        raise TooLarge(_too_large(largest, size))
    return plan


def status(row: ChatAttachmentRow) -> dict[str, Any]:
    return {
        "attachment_id": row["attachment_id"],
        "state": shown_state(row["state"], row["expires_at"]),
        "name": row["name"],
        "size": row["size"],
        "mime_type": row["mime_type"],
        "error": row["error"],
    }


def is_open(row: ChatAttachmentRow) -> bool:
    """Whether the upload can still take bytes."""
    return shown_state(row["state"], row["expires_at"]) == "uploading"


def _multipart(row: ChatAttachmentRow) -> MultipartUpload:
    assert row["object_key"] and row["store_upload_id"] and row["part_size"] and row["part_count"]
    return MultipartUpload(
        key=row["object_key"],
        upload_id=row["store_upload_id"],
        size=row["size"],
        part_size=row["part_size"],
        part_count=row["part_count"],
    )


# --- steps ---------------------------------------------------------------------


async def owned(attachment_id: str, user_id: str) -> ChatAttachmentRow:
    row = await db.get_attachment(attachment_id)
    if row is None or row["user_id"] != user_id:
        raise AttachmentNotFound
    return row


async def begin(
    user_id: str,
    *,
    name: str,
    size: int,
    mime_type: str,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Start an attachment's upload: ``relay`` for the file in one request,
    or ``direct`` with its parts' layout."""
    mode, max_bytes = plan_upload(size)
    if size > RELAY_MAX_BYTES and workspace_id and not await computer_takes_large(
        user_id, workspace_id
    ):
        raise TooLarge(_too_large_for_computer(size))
    attachment_id = str(uuid4())
    name = name.strip() or "file"
    mime_type = declared_type(mime_type, name)
    key = attachment_key(attachment_id, name) if is_storage_enabled() else None

    async def create(store_upload_id: str | None) -> ChatAttachmentRow:
        try:
            return await db.create_attachment(
                attachment_id=attachment_id,
                user_id=user_id,
                name=name,
                mime_type=mime_type,
                size=size,
                mode=mode,
                object_key=key,
                store_upload_id=store_upload_id,
                part_size=UPLOAD_PART_BYTES if store_upload_id else None,
                part_count=part_count_for(size) if store_upload_id else None,
                max_open=MAX_OPEN_ATTACHMENTS_PER_USER,
                max_held_bytes=POSTGRES_HELD_MAX_BYTES_PER_USER,
                max_staged_bytes=STAGED_MAX_BYTES_PER_USER,
                ttl=_initial_ttl(mode, size),
            )
        except db.TooManyOpenAttachments:
            raise TooMany(TOO_MANY_ATTACHMENTS) from None
        except db.TooManyHeldBytes:
            raise TooMany(TOO_MANY_HELD_BYTES) from None
        except db.TooManyStagedBytes:
            raise TooMany(TOO_MANY_STAGED_BYTES) from None

    if mode == "relay":
        await create(None)
        return {"attachment_id": attachment_id, "mode": mode, "max_bytes": max_bytes}
    assert key is not None
    async with opened_upload(key, content_type=served_type(mime_type)) as store_upload_id:
        row = await create(store_upload_id)
    return {
        "attachment_id": attachment_id,
        "mode": mode,
        "max_bytes": max_bytes,
        "part_size": row["part_size"],
        "part_count": row["part_count"],
        "expires_at": row["expires_at"].isoformat(),
    }


async def receive(
    attachment_id: str, user_id: str, read: Callable[[int], Awaitable[bytes]]
) -> dict[str, Any]:
    """Take a ``relay`` attachment's whole file, which ``read`` gives up to the
    size begun with, raising :class:`SentTooMuch` past it. Sending again
    answers where it stands."""
    row = await owned(attachment_id, user_id)
    if row["state"] == "ready":
        return status(row)
    if not is_open(row):
        raise AttachmentEnded
    if row["mode"] != "relay":
        raise WrongPath(NOT_RELAY)
    size = row["size"]
    held = AsyncExitStack()
    await held.enter_async_context(held_bytes_budget().hold(size))
    try:
        data = await read(size)
        if len(data) != size:
            raise SizeChanged
    except BaseException:
        await held.aclose()
        raise
    # A write already in its thread goes on when the request is cancelled,
    # so what follows it runs to the end too: an object it puts after a
    # discard's cleanup is only removed by settling the lost race. The bytes
    # stay held until then, so the reservation goes with the write.
    settling = asyncio.ensure_future(_store_relay_holding(held, row, data))
    settling.add_done_callback(lambda t: t.cancelled() or t.exception())
    return await asyncio.shield(settling)


async def _store_relay_holding(
    held: AsyncExitStack, row: ChatAttachmentRow, data: bytes
) -> dict[str, Any]:
    async with held:
        return await _store_relay(row, data)


async def _store_relay(row: ChatAttachmentRow, data: bytes) -> dict[str, Any]:
    attachment_id, key = row["attachment_id"], row["object_key"]
    if key:
        stored = await asyncio.to_thread(
            upload_bytes,
            key,
            data,
            content_type=served_type(row["mime_type"]),
            max_size=row["size"],
        )
        if not stored:
            raise StoreUnreachable
        ready = await db.mark_ready(attachment_id)
    else:
        ready = await db.mark_ready(attachment_id, content=data)
    if ready is None:
        return await _settle_lost_race(row, wrote_object=bool(key))
    return status(ready)


async def sign(attachment_id: str, user_id: str, part_numbers: list[int]) -> list[dict[str, Any]]:
    """Signed PUT URLs for the named parts of a ``direct`` attachment. Asking
    for parts is what keeps an upload alive."""
    row = await owned(attachment_id, user_id)
    if row["mode"] != "direct":
        raise WrongPath(NOT_DIRECT)
    numbers = sorted(set(part_numbers))
    if numbers[0] < 1 or numbers[-1] > (row["part_count"] or 0):
        raise PartOutOfRange
    extended = await db.extend_upload(attachment_id)
    if extended is None:
        raise AttachmentEnded
    parts = await asyncio.to_thread(sign_parts, _multipart(extended), numbers)
    if parts is None:
        raise StoreUnreachable
    return parts


async def complete(attachment_id: str, user_id: str) -> dict[str, Any]:
    """Assemble a ``direct`` attachment's parts in the store. Completing again
    answers where it stands."""
    row = await owned(attachment_id, user_id)
    if row["state"] == "ready":
        return status(row)
    if row["mode"] != "direct":
        raise WrongPath(NOT_DIRECT)
    if not is_open(row):
        raise AttachmentEnded
    assembly = await asyncio.to_thread(assemble_parts, _multipart(row))
    if assembly.missing:
        raise PartsMissing(assembly.missing)
    if assembly.unreachable:
        raise StoreUnreachable
    if assembly.mismatch:
        logger.warning(f"Attachment {attachment_id} assembled to a size other than its own")
        ended = await db.abort_unsent(attachment_id, error=ARRIVED_DAMAGED)
        if ended is not None:
            await clean_up(ended)
        raise AttachmentEnded
    ready = await db.mark_ready(attachment_id)
    if ready is None:
        return await _settle_lost_race(row, wrote_object=True)
    return status(ready)


async def discard(attachment_id: str, user_id: str) -> None:
    """Discard an attachment no message has used. One a message used stays:
    the thread's history shows it."""
    row = await owned(attachment_id, user_id)
    ended = await db.abort_unsent(row["attachment_id"])
    if ended is not None:
        logger.info(f"Attachment {attachment_id} discarded")
        await clean_up(ended)


async def _settle_lost_race(row: ChatAttachmentRow, *, wrote_object: bool) -> dict[str, Any]:
    """The answer for a step that found the row no longer uploading.

    Another request may have finished it, which answers the same; one that
    ended it is refused, and an object this step wrote after the end's
    cleanup ran is removed here.
    """
    current = await db.get_attachment(row["attachment_id"]) or row
    if current["state"] == "ready":
        return status(current)
    if wrote_object and row["object_key"]:
        try:
            deleted = await asyncio.to_thread(delete_object, row["object_key"])
        except Exception as e:
            logger.warning(f"Removing an ended attachment's object failed: {e}")
            deleted = False
        if not deleted:
            # The end's cleanup already ran, so only the sweep can take it now.
            await db.reopen_cleanup(row["attachment_id"])
    raise AttachmentEnded


# --- cleaning up -----------------------------------------------------------------


async def clean_up(row: ChatAttachmentRow) -> None:
    """Clean an aborted row's store bytes now; the sweep retries one that fails."""
    key = row["object_key"]
    try:
        cleaned = key is None or await asyncio.to_thread(
            discard_upload, key, row["store_upload_id"], assembled=row["ready_at"] is not None
        )
    except Exception as e:
        logger.warning(f"Attachment {row['attachment_id']} cleanup failed: {e}")
        return
    if cleaned:
        await db.mark_cleaned(row["attachment_id"], seen=row["updated_at"])


# --- sweep -----------------------------------------------------------------------

SWEEP_BATCH_LIMIT = 25


async def sweep_once() -> None:
    """One pass over attachments no request will settle.

    Every worker runs it; each step claims its rows in one conditional
    update, so a row is acted on by one worker at a time. A row whose cleanup
    fails is left for a later pass, never in the way of the rest.
    """
    for row in await db.expire_uploads(SWEEP_BATCH_LIMIT, _EXPIRED_UPLOADING):
        logger.info(f"Attachment {row['attachment_id']} expired while uploading")
        await clean_up(row)
    for row in await db.expire_unsent(UNSENT_TTL, SWEEP_BATCH_LIMIT, _EXPIRED_UNSENT):
        logger.info(f"Attachment {row['attachment_id']} expired unsent")
        await clean_up(row)
    for row in await db.claim_cleanups(_CLEANUP_HOLD, SWEEP_BATCH_LIMIT):
        await clean_up(row)
    await db.drop_sent_content(POSTGRES_SENT_CONTENT_RETENTION, SWEEP_BATCH_LIMIT)
    await db.prune_sent(SENT_RETENTION, SWEEP_BATCH_LIMIT * 4)
    await db.prune_aborted(_ABORTED_PRUNE_AFTER, SWEEP_BATCH_LIMIT * 4)


class AttachmentSweeper(JitteredSweeper):
    """Runs with or without object storage: without it, the bytes it drops
    are in Postgres."""

    name = "AttachmentSweeper"
    interval = 60.0

    async def sweep_once(self) -> None:
        await sweep_once()
