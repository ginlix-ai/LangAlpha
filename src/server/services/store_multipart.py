"""The store half of an upload a browser sends to object storage in parts.

Shared by uploads into a workspace folder and by chat attachments: both open
the store's multipart upload as their row is written, sign parts one batch at
a time, take the parts' sizes from the store's own listing rather than from
the client, assemble the object there, and clean up the same way. Each keeps
its own table; what happens to the object afterwards is each caller's own
business.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from src.utils.storage import (
    abort_multipart_upload,
    complete_multipart_upload,
    delete_object,
    list_uploaded_parts,
    object_size,
    open_multipart_upload,
    sign_upload_part,
)

_MB = 1024 * 1024

MAX_DIRECT_UPLOAD_BYTES = 2 * 1024 * _MB
# Below this the one-request path is quicker than a round of parts, and stays
# under the request-body limit an edge proxy commonly sets.
DIRECT_UPLOAD_MIN_BYTES = 32 * _MB
# What a failed part costs to send again. Fixed, so a 2 GB file is 128 parts.
UPLOAD_PART_BYTES = 16 * _MB
PART_URL_TTL_S = 900
MAX_PARTS_PER_SIGN = 32

# An upload nobody signs parts for in this long is given up. Each batch of
# parts moves it, so it bounds a stalled upload, not a slow one.
IDLE_TTL = timedelta(hours=1)
# However often it signs, an upload ends by this age.
MAX_AGE = timedelta(hours=24)

STORE_UNREACHABLE = "Could not reach file storage. Try again."
PARTS_MISSING = "Some parts of the file did not arrive."


class StoreUnreachable(Exception):
    """The store did not answer a step worth retrying."""


def part_count_for(size: int) -> int:
    return -(-size // UPLOAD_PART_BYTES)


@dataclass(frozen=True, slots=True)
class MultipartUpload:
    """One upload in parts as the store sees it, built by each caller from its row."""

    key: str
    upload_id: str
    size: int
    part_size: int
    part_count: int

    def part_size_of(self, number: int) -> int:
        """Every part but the last is ``part_size``; the last is what remains."""
        if number < self.part_count:
            return self.part_size
        return self.size - self.part_size * (self.part_count - 1)


def shown_state(state: str, expires_at: datetime) -> str:
    """An upload past its deadline reads as aborted before the sweep ends it:
    nothing can send to it or complete it any more."""
    if state == "uploading" and expires_at <= datetime.now(UTC):
        return "aborted"
    return state


def extend_sql(table: str, id_column: str, returning: str) -> str:
    """Move an uploading row's idle deadline out, up to its age limit; the row
    back, or none once it has no deadline left. Takes ``%(id)s`` and
    :data:`EXTEND_PARAMS`."""
    return f"""
        UPDATE {table}
        SET expires_at = LEAST(NOW() + %(idle_ttl)s, created_at + %(max_age)s),
            updated_at = NOW()
        WHERE {id_column} = %(id)s AND state = 'uploading'
          AND expires_at > NOW() AND created_at > NOW() - %(max_age)s
        RETURNING {returning}
    """


EXTEND_PARAMS = {"idle_ttl": IDLE_TTL, "max_age": MAX_AGE}


@asynccontextmanager
async def opened_upload(key: str, *, content_type: str) -> AsyncIterator[str]:
    """Open the store's multipart upload for ``key`` and yield its id; abort it
    when the body raises, since nothing was sent to it yet and one left open
    would only wait for the bucket's lifecycle rule.

    Raises:
        StoreUnreachable: The store would not open one.
    """
    store_upload_id = await asyncio.to_thread(
        open_multipart_upload, key, content_type=content_type
    )
    if store_upload_id is None:
        raise StoreUnreachable
    try:
        yield store_upload_id
    except BaseException:
        await asyncio.to_thread(abort_multipart_upload, key, store_upload_id)
        raise


def sign_parts(upload: MultipartUpload, numbers: list[int]) -> list[dict[str, Any]] | None:
    """Signed PUT URLs for ``numbers``, each bound to its part's length; None
    when the store would not sign one.

    Run off the event loop: signing makes no request, but the first one in a
    process builds the store's client, which takes a while.
    """
    parts = []
    for number in numbers:
        size = upload.part_size_of(number)
        url = sign_upload_part(
            upload.key,
            upload.upload_id,
            part_number=number,
            size=size,
            expires_in=PART_URL_TTL_S,
            for_browser=True,
        )
        if url is None:
            return None
        parts.append(
            {
                "part_number": number,
                "offset": (number - 1) * upload.part_size,
                "size": size,
                "url": url,
            }
        )
    return parts


@dataclass
class Assembly:
    """How assembling went: ``ok``, or the parts still owed, or a store failure
    worth retrying (``unreachable``), or an object that is not what was sent
    (``mismatch``)."""

    ok: bool = False
    missing: list[int] = field(default_factory=list)
    unreachable: bool = False
    mismatch: bool = False


def assemble_parts(upload: MultipartUpload) -> Assembly:
    """Assemble the parts the store holds, checked against the row, not the client.

    The parts and their sizes come from the store's own listing, so a client
    cannot finish an upload short, long, or with parts it was never granted.
    An upload the store no longer knows may be one an earlier attempt
    assembled before its answer was lost, so the object's size settles it.
    Blocking; callers run it in a thread.
    """
    parts = list_uploaded_parts(upload.key, upload.upload_id)
    if parts is None:
        held = object_size(upload.key)
        if held is None:
            return Assembly(unreachable=True)
        return Assembly(ok=held == upload.size, mismatch=held != upload.size)
    by_number = {p["part_number"]: p for p in parts}
    numbers = range(1, upload.part_count + 1)
    missing = [
        n
        for n in numbers
        if n not in by_number or by_number[n]["size"] != upload.part_size_of(n)
    ]
    if missing:
        return Assembly(missing=missing)
    completed = complete_multipart_upload(
        upload.key, upload.upload_id, [(n, by_number[n]["etag"]) for n in numbers]
    )
    held = object_size(upload.key)
    if held is None:
        return Assembly(unreachable=True)
    if held != upload.size:
        return Assembly(mismatch=True) if completed else Assembly(unreachable=True)
    return Assembly(ok=True)


def discard_upload(key: str, store_upload_id: str | None, *, assembled: bool) -> bool:
    """Remove what an ended upload left in the store; whether nothing is left.

    One never assembled holds parts under its open upload, which only an
    abort removes, so an abort the store did not confirm leaves the row for
    the sweep to try again. The object goes either way: an attempt may have
    assembled it unrecorded, or a relay written it as the row ended.
    Blocking; callers run it in a thread.
    """
    if store_upload_id and not assembled and not abort_multipart_upload(key, store_upload_id):
        return False
    return delete_object(key)
