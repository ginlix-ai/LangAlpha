"""Chat composer attachments, uploaded before the message that carries them.

Starting an upload answers how the file travels; the client never decides it.
A file the browser sends in parts goes to the store on URLs signed here, any
other comes through ``/content`` in one request. The message then names the
attachment by id, and its turn places the file in the workspace
(``handlers/chat/attachments.py``). The steps are ``services/chat_attachments``;
this module only answers them over HTTP.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Body, File, HTTPException, Response, UploadFile
from pydantic import BaseModel, Field

from src.server.services import chat_attachments as attachments
from src.server.services.store_multipart import (
    MAX_PARTS_PER_SIGN,
    PART_URL_TTL_S,
    STORE_UNREACHABLE,
    StoreUnreachable,
)
from src.server.utils.api import CurrentUserId
from src.server.utils.uploads import read_capped

router = APIRouter(prefix="/api/v1/attachments", tags=["Attachments"])


class BeginAttachmentRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="The file's name.")
    size: int = Field(..., ge=0, description="The file's length in bytes.")
    mime_type: str = Field("", max_length=255, description="The file's MIME type.")
    workspace_id: str | None = Field(
        None,
        max_length=64,
        description=(
            "The workspace the message is going to, when known: a file its "
            "computer cannot receive is refused before it is sent."
        ),
    )


class CheckAttachmentRequest(BaseModel):
    workspace_id: str = Field(
        ..., min_length=1, max_length=64, description="The workspace the message is going to now."
    )


class SignPartsRequest(BaseModel):
    part_numbers: list[int] = Field(..., min_length=1, max_length=MAX_PARTS_PER_SIGN)


_STATUS: dict[type[attachments.AttachmentRefused], int] = {
    attachments.AttachmentNotFound: 404,
    attachments.AttachmentEnded: 409,
    attachments.WrongPath: 409,
    attachments.PartsMissing: 409,
    attachments.PartOutOfRange: 422,
    attachments.TooLarge: 413,
    attachments.SizeChanged: 400,
    attachments.SentTooMuch: 413,
    attachments.TooMany: 429,
}


@contextmanager
def _answered() -> Iterator[None]:
    try:
        yield
    except attachments.AttachmentRefused as e:
        raise HTTPException(status_code=_STATUS[type(e)], detail=e.detail) from None
    except StoreUnreachable:
        raise HTTPException(status_code=503, detail=STORE_UNREACHABLE) from None


async def _read_up_to(file: UploadFile, size: int) -> bytes:
    try:
        return await read_capped(file, size)
    except HTTPException as e:
        if e.status_code == 413:
            raise attachments.SentTooMuch from None
        raise


@router.post("", status_code=201)
async def begin_attachment(
    x_user_id: CurrentUserId, body: BeginAttachmentRequest = Body(...)
) -> dict[str, Any]:
    """Start an attachment's upload: ``relay`` to send the file to its
    ``/content``, or ``direct`` with its parts' layout."""
    with _answered():
        return await attachments.begin(
            x_user_id,
            name=body.name,
            size=body.size,
            mime_type=body.mime_type,
            workspace_id=body.workspace_id,
        )


@router.post("/{attachment_id}/content")
async def send_attachment_content(
    attachment_id: UUID, x_user_id: CurrentUserId, file: UploadFile = File(...)
) -> dict[str, Any]:
    """The whole file of a ``relay`` attachment, in one request; a body longer
    than the size begun with is refused as it is read, before it is held whole."""
    with _answered():
        return await attachments.receive(
            str(attachment_id), x_user_id, lambda size: _read_up_to(file, size)
        )


@router.post("/{attachment_id}/parts")
async def sign_attachment_parts(
    attachment_id: UUID, x_user_id: CurrentUserId, body: SignPartsRequest = Body(...)
) -> dict[str, Any]:
    """Signed PUT URLs for the named parts, each bound to its length. Asking
    for parts is what keeps an upload alive."""
    with _answered():
        parts = await attachments.sign(str(attachment_id), x_user_id, body.part_numbers)
    return {"parts": parts, "expires_in": PART_URL_TTL_S}


@router.post("/{attachment_id}/complete")
async def complete_attachment(
    attachment_id: UUID, x_user_id: CurrentUserId
) -> dict[str, Any]:
    """Assemble a ``direct`` attachment's parts in the store. Completing again
    answers where it stands."""
    with _answered():
        return await attachments.complete(str(attachment_id), x_user_id)


@router.post("/{attachment_id}/check")
async def check_attachment(
    attachment_id: UUID, x_user_id: CurrentUserId, body: CheckAttachmentRequest = Body(...)
) -> dict[str, Any]:
    """Whether the file can go to ``workspace_id``: its status, or 413 when
    that workspace's computer cannot receive a file this large."""
    with _answered():
        return await attachments.check_fits(str(attachment_id), x_user_id, body.workspace_id)


@router.get("/{attachment_id}")
async def get_attachment(attachment_id: UUID, x_user_id: CurrentUserId) -> dict[str, Any]:
    """Where the attachment stands: ``uploading``, then ``ready``, or
    ``aborted`` (with ``error``)."""
    with _answered():
        return attachments.status(await attachments.owned(str(attachment_id), x_user_id))


@router.delete("/{attachment_id}", status_code=204)
async def delete_attachment(attachment_id: UUID, x_user_id: CurrentUserId) -> Response:
    """Discard an attachment no message has used. One a message used stays:
    the thread's history shows it."""
    with _answered():
        await attachments.discard(str(attachment_id), x_user_id)
    return Response(status_code=204)
