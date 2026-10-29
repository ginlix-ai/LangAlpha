"""Uploads a browser sends to object storage, for the sandbox to pull into the folder.

``POST /files/upload`` carries a file through this process, so it is capped
at what a worker can hold. A larger file goes from the browser to the store
in parts on URLs signed here, and the sandbox pulls the assembled object into
place. Starting an upload answers which of the two paths the client takes;
the client never decides it. Each step may reach any worker, so the row in
``workspace_file_uploads`` is the upload's only state.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, Response
from pydantic import BaseModel, Field

from src.server.database import workspace_file_uploads as db
from src.server.database.blob_keys import upload_key
from src.server.models.workspace import served_from_mirror
from src.server.services.background_starts import schedule_start
from src.server.services.computer_disk import CRITICAL_FREE_BYTES
from src.server.services.persistence.transfer import scan_cap_bytes
from src.server.services.workspace_file_containment import normalize_requested_path
from src.server.services.workspace_files import (
    acquire_sandbox_to_change,
    contained_target,
    not_running_for_upload,
    owned_workspace_on_computer,
    previous_dir_names_of,
)
from src.server.services.workspace_upload_pull import (
    COMPLETE_LEASE,
    KEEP_ALIVE_EVERY,
    STORE_UNREACHABLE,
    assemble_and_pull,
    clean_up,
    count_as_use,
    free_disk_bytes,
    not_enough_disk,
    part_size_of,
)
from src.server.utils.api import CurrentUserId
from src.server.utils.uploads import _too_large
from src.utils.storage import (
    abort_multipart_upload,
    browser_uploads_enabled,
    is_storage_enabled,
    open_multipart_upload,
    sign_upload_part,
)

from ._shared import MAX_UPLOAD_BYTES, _virtual_file, owner_work_dir

logger = logging.getLogger(__name__)

# No prefix of its own: mounted on the file routes' router, which carries it.
router = APIRouter(tags=["Workspace Files"])

_MB = 1024 * 1024

MAX_DIRECT_UPLOAD_BYTES = 2 * 1024 * _MB
# Below this the one-request path is quicker than a round of parts, and stays
# under the request-body limit an edge proxy commonly sets.
DIRECT_UPLOAD_MIN_BYTES = 32 * _MB
# What a failed part costs to send again. Fixed, so a 2 GB file is 128 parts.
UPLOAD_PART_BYTES = 16 * _MB
PART_URL_TTL_S = 900
MAX_PARTS_PER_SIGN = 32
# Each holds up to MAX_DIRECT_UPLOAD_BYTES in the store until it ends.
MAX_OPEN_UPLOADS_PER_USER = 8

UPLOAD_ENDED = "This upload has ended. Start it again."
TOO_MANY_UPLOADS = "Too many uploads in progress. Wait for one to finish."
ALREADY_SAVING = "This upload is already being saved to the workspace."
PARTS_MISSING = "Some parts of the file did not arrive."


class BeginUploadRequest(BaseModel):
    path: str = Field(
        ..., min_length=1, max_length=4096, description="Destination path (virtual or absolute)."
    )
    size: int = Field(..., ge=0, description="The file's length in bytes.")
    sha256: str | None = Field(
        None,
        pattern=r"^[0-9a-f]{64}$",
        description="The file's SHA-256, checked in the sandbox before the file is placed.",
    )


class SignPartsRequest(BaseModel):
    part_numbers: list[int] = Field(..., min_length=1, max_length=MAX_PARTS_PER_SIGN)


def direct_available(sandbox: Any) -> bool:
    """Whether this sandbox can take an upload through the store.

    That needs a store the browser may send parts to, and a sandbox that
    pulls from the store itself, which is what the direct transfer mode says.
    A machine its owner brought keeps the one-request path.
    """
    if not browser_uploads_enabled() or scan_cap_bytes(sandbox, blobs_on=True) is not None:
        return False
    provider = getattr(getattr(getattr(sandbox, "config", None), "sandbox", None), "provider", None)
    return provider != "ssh"


def _status(row: dict[str, Any]) -> dict[str, Any]:
    state = row["state"]
    if state == "uploading" and row["expires_at"] <= datetime.now(UTC):
        state = "aborted"
    return {
        "upload_id": row["upload_id"],
        "state": state,
        "path": row["path"],
        "size": row["size"],
        "error": row["error"],
    }


async def _owned_upload(workspace_id: str, upload_id: UUID, user_id: str) -> dict[str, Any]:
    row = await db.get_upload(str(upload_id))
    if row is None or row["user_id"] != user_id or row["workspace_id"] != workspace_id:
        raise HTTPException(status_code=404, detail="Upload not found")
    return row


@router.post("/{workspace_id}/files/uploads")
async def begin_workspace_upload(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: BeginUploadRequest = Body(...),
) -> dict[str, Any]:
    """Start an upload: ``relay`` to send the file to ``/files/upload``, or
    ``direct`` with the upload's id and its parts' layout."""
    if body.size > MAX_DIRECT_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413, detail=_too_large(MAX_DIRECT_UPLOAD_BYTES, body.size)
        )
    workspace = await owned_workspace_on_computer(workspace_id, x_user_id)
    if served_from_mirror(workspace.get("status")):
        raise not_running_for_upload(workspace)
    if body.size < DIRECT_UPLOAD_MIN_BYTES:
        return {"mode": "relay"}

    async with acquire_sandbox_to_change(workspace_id, x_user_id) as (sandbox, workspace):
        if not direct_available(sandbox):
            # The cap the relay route itself applies, said before any bytes move.
            scan_cap = scan_cap_bytes(sandbox, blobs_on=is_storage_enabled())
            relay_cap = MAX_UPLOAD_BYTES if scan_cap is None else min(MAX_UPLOAD_BYTES, scan_cap)
            if body.size > relay_cap:
                raise HTTPException(status_code=413, detail=_too_large(relay_cap, body.size))
            return {"mode": "relay"}

        work_dir = owner_work_dir(workspace)
        previous_dirs = previous_dir_names_of(workspace)
        # The checks the relay route makes, made before the bytes are sent.
        virtual = _virtual_file(normalize_requested_path(body.path, work_dir, previous_dirs))
        if virtual:
            raise HTTPException(status_code=400, detail=virtual.read_only)
        target = await contained_target(sandbox, body.path, work_dir, previous_dirs)
        rel = target[len(work_dir.rstrip("/")) + 1 :]
        if not rel:
            raise HTTPException(status_code=400, detail="Destination path is required")

        # Checked against the uploads already due on the computer where the
        # row is written, under the user's lock, so two starts cannot both fit.
        free = await free_disk_bytes(sandbox, work_dir)

        upload_id = str(uuid.uuid4())
        key = upload_key(x_user_id, upload_id)
        store_upload_id = await asyncio.to_thread(
            open_multipart_upload, key, content_type="application/octet-stream"
        )
        if store_upload_id is None:
            raise HTTPException(status_code=503, detail=STORE_UNREACHABLE)
        try:
            row = await db.create_upload(
                upload_id=upload_id,
                user_id=x_user_id,
                workspace_id=workspace_id,
                path=rel,
                size=body.size,
                part_size=UPLOAD_PART_BYTES,
                part_count=-(-body.size // UPLOAD_PART_BYTES),
                sha256=body.sha256,
                object_key=key,
                store_upload_id=store_upload_id,
                sandbox_id=getattr(sandbox, "sandbox_id", None),
                max_open=MAX_OPEN_UPLOADS_PER_USER,
                free_bytes=free,
                keep_free_bytes=CRITICAL_FREE_BYTES,
            )
        except BaseException as e:
            # Nothing was sent yet; an upload left open would only wait for
            # the bucket's lifecycle rule.
            await asyncio.to_thread(abort_multipart_upload, key, store_upload_id)
            if isinstance(e, db.TooManyOpenUploads):
                raise HTTPException(status_code=429, detail=TOO_MANY_UPLOADS) from None
            if isinstance(e, db.NotEnoughRoom):
                raise HTTPException(
                    status_code=507, detail=not_enough_disk(e.free_bytes)
                ) from None
            raise

    logger.info(
        f"Upload {upload_id} started into workspace {workspace_id}: "
        f"{body.size} bytes in {row['part_count']} parts"
    )
    return {
        "mode": "direct",
        "upload_id": upload_id,
        "path": rel,
        "part_size": row["part_size"],
        "part_count": row["part_count"],
        "expires_at": row["expires_at"].isoformat(),
    }


def _sign_parts(row: dict[str, Any], numbers: list[int]) -> list[dict[str, Any]] | None:
    """Off the event loop: signing makes no request, but the first one in a
    process builds the store's client, which takes a while."""
    parts = []
    for number in numbers:
        size = part_size_of(row, number)
        url = sign_upload_part(
            row["object_key"],
            row["store_upload_id"],
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
                "offset": (number - 1) * row["part_size"],
                "size": size,
                "url": url,
            }
        )
    return parts


@router.post("/{workspace_id}/files/uploads/{upload_id}/parts")
async def sign_workspace_upload_parts(
    workspace_id: str,
    upload_id: UUID,
    x_user_id: CurrentUserId,
    body: SignPartsRequest = Body(...),
) -> dict[str, Any]:
    """Signed PUT URLs for the named parts, each bound to its length.

    Asking for parts is what keeps an upload alive, and what counts it as use
    of the computer it is going to.
    """
    row = await _owned_upload(workspace_id, upload_id, x_user_id)
    numbers = sorted(set(body.part_numbers))
    if numbers[0] < 1 or numbers[-1] > row["part_count"]:
        raise HTTPException(status_code=422, detail="Part number out of range")
    row = await db.extend_upload(row["upload_id"])
    if row is None:
        raise HTTPException(status_code=409, detail=UPLOAD_ENDED)

    parts = await asyncio.to_thread(_sign_parts, row, numbers)
    if parts is None:
        raise HTTPException(status_code=503, detail=STORE_UNREACHABLE)

    sandbox_id = row["sandbox_id"]
    if sandbox_id and await db.claim_touch(row["upload_id"], KEEP_ALIVE_EVERY):
        schedule_start(
            f"workspace-upload-touch:{row['upload_id']}",
            lambda: count_as_use(workspace_id, sandbox_id),
        )
    return {"parts": parts, "expires_in": PART_URL_TTL_S}


@router.post("/{workspace_id}/files/uploads/{upload_id}/complete", status_code=202)
async def complete_workspace_upload(
    workspace_id: str, upload_id: UUID, x_user_id: CurrentUserId
) -> dict[str, Any]:
    """Assemble the sent parts and start the pull into the folder; poll the
    upload for its end. Completing again answers where it stands."""
    row = await _owned_upload(workspace_id, upload_id, x_user_id)
    if row["state"] != "uploading":
        return _status(row)
    claimed = await db.claim_complete(row["upload_id"], COMPLETE_LEASE)
    if claimed is None:
        row = await db.get_upload(row["upload_id"]) or row
        if row["state"] == "uploading":
            raise HTTPException(status_code=409, detail=UPLOAD_ENDED)
        return _status(row)

    after, assembly = await assemble_and_pull(claimed)
    if assembly.missing:
        raise HTTPException(
            status_code=409,
            detail={"message": PARTS_MISSING, "missing_parts": assembly.missing},
        )
    if assembly.unreachable:
        raise HTTPException(status_code=503, detail=STORE_UNREACHABLE)
    return _status(after or claimed)


@router.get("/{workspace_id}/files/uploads/{upload_id}")
async def get_workspace_upload(
    workspace_id: str, upload_id: UUID, x_user_id: CurrentUserId
) -> dict[str, Any]:
    """Where the upload stands: ``uploading``, ``completing`` and ``pulling``
    while it runs, then ``done``, ``failed`` (with ``error``) or ``aborted``."""
    return _status(await _owned_upload(workspace_id, upload_id, x_user_id))


@router.delete("/{workspace_id}/files/uploads/{upload_id}", status_code=204)
async def cancel_workspace_upload(
    workspace_id: str, upload_id: UUID, x_user_id: CurrentUserId
) -> Response:
    """Cancel an upload still sending parts and discard them.

    Refused once the parts are assembled: the pull that follows replaces the
    file in one rename and cannot be stopped short of it.
    """
    row = await _owned_upload(workspace_id, upload_id, x_user_id)
    if row["state"] == "uploading":
        ended = await db.end_upload(
            row["upload_id"], from_states=("uploading",), state="aborted"
        )
        if ended is not None:
            logger.info(f"Upload {ended['upload_id']} cancelled")
            await clean_up(ended)
            return Response(status_code=204)
        row = await db.get_upload(row["upload_id"]) or row
    if row["state"] in ("completing", "pulling"):
        raise HTTPException(status_code=409, detail=ALREADY_SAVING)
    return Response(status_code=204)
