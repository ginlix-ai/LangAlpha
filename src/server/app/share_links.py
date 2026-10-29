"""The owner's side of share links: mint, review the file list, share, stop.

A top-level module rather than a file under ``workspace_files``: the manifest
reads files through that package and judges them with ``share_access``, which
``workspace_files`` cannot import back without a cycle.

Endpoints, all owner-only:
- POST  /api/v1/workspaces/{id}/file-grant                 - a path grant for the owner's viewer
- POST  /api/v1/workspaces/{id}/share-links                - get or create an item's link
- GET   /api/v1/workspaces/{id}/share-links                - the shared links here
- GET   /api/v1/workspaces/{id}/share-links/{code}/files   - the current file list
- PATCH /api/v1/workspaces/{id}/share-links/{code}         - share with a reviewed list, or stop
- GET   /api/v1/share-links/{code}                         - one of the owner's links, any workspace
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.server.app.share_access import ShareScope, shared_path_visible
from src.server.app.workspace_files._shared import owner_work_dir
from src.server.database.share_codes import share_path
from src.server.database.share_links import (
    KIND_FILE,
    KIND_APP,
    ShareLink,
    ensure_app_link,
    ensure_file_link,
    get_link,
    list_shared,
    set_private,
    set_shared,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services.file_grants import grant_prefix, mint_file_grant, seconds_left
from src.server.services.workspace_file_containment import contained_relative_path
from src.server.services.workspace_files import (
    owned_workspace_on_computer,
    previous_dir_names_of,
)
from src.server.app.share_manifest import (
    ManifestEntry,
    ManifestEntryMissing,
    ManifestTooLarge,
    build_manifest,
    manifest_drift,
)
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1/workspaces", tags=["Share Links"])
owner_router = APIRouter(prefix="/api/v1/share-links", tags=["Share Links"])


class ShareLinkCreate(BaseModel):
    kind: Literal["file", "app"]
    path: str | None = None
    port: int | None = Field(None, ge=3000, le=9999)


class ShareLinkUpdate(BaseModel):
    shared: bool
    files: list[str] | None = None


class ShareLinkResponse(BaseModel):
    code: str
    workspace_id: str
    url: str
    kind: str
    path: str | None
    port: int | None
    title: str | None
    shared: bool
    shared_at: datetime | None
    shared_files: list[str] | None
    created_at: datetime


class FileGrantResponse(BaseModel):
    prefix: str
    expires_in: int


def _to_model(link: ShareLink) -> ShareLinkResponse:
    return ShareLinkResponse(
        code=link.code,
        workspace_id=link.workspace_id,
        url=share_path(link.code),
        kind=link.kind,
        path=link.path,
        port=link.port,
        title=link.display_title,
        shared=link.shared,
        shared_at=link.shared_at,
        shared_files=list(link.files) if link.shared and link.files else None,
        created_at=link.created_at,
    )


async def _owned_workspace(workspace_id: str, user_id: str) -> tuple[dict[str, Any], str]:
    workspace = await owned_workspace_on_computer(workspace_id, user_id)
    return workspace, owner_work_dir(workspace)


async def _owned_link(workspace: dict[str, Any], code: str) -> ShareLink:
    link = await get_link(code)
    if link is None or link.workspace_id != str(workspace["workspace_id"]):
        raise HTTPException(status_code=404, detail="Share link not found")
    return link


async def _current_files(
    workspace: dict[str, Any], link: ShareLink, user_id: str, work_dir: str
) -> list[ManifestEntry]:
    """The manifest as an HTTP answer: the caps are a 422 the dialog explains."""
    try:
        return await build_manifest(
            workspace, link.path or "", user_id=user_id, work_dir=work_dir
        )
    except ManifestEntryMissing:
        raise HTTPException(status_code=404, detail="File not found") from None
    except ManifestTooLarge as e:
        raise HTTPException(
            status_code=422, detail={"code": e.code, "limit": e.limit}
        ) from None


@router.post("/{workspace_id}/file-grant", response_model=FileGrantResponse)
async def mint_workspace_file_grant(
    workspace_id: str, x_user_id: CurrentUserId
) -> FileGrantResponse:
    """A path grant for the owner's own viewers; see ``services/file_grants``."""
    workspace, _ = await _owned_workspace(workspace_id, x_user_id)
    grant = await mint_file_grant(str(workspace["workspace_id"]))
    return FileGrantResponse(
        prefix=grant_prefix(grant), expires_in=seconds_left(grant.expires_at)
    )


@router.post("/{workspace_id}/share-links", response_model=ShareLinkResponse)
async def get_or_create_share_link(
    workspace_id: str, body: ShareLinkCreate, x_user_id: CurrentUserId
) -> ShareLinkResponse:
    """The item's one link, private on first sight. Cheap: no file list here."""
    workspace, work_dir = await _owned_workspace(workspace_id, x_user_id)
    ws_id = str(workspace["workspace_id"])

    if body.kind == KIND_APP:
        if body.port is None:
            raise HTTPException(status_code=422, detail="A port is required")
        return _to_model(await ensure_app_link(ws_id, body.port))

    path = contained_relative_path(
        body.path or "", work_dir, previous_dir_names_of(workspace)
    )
    if path is None or not shared_path_visible(ShareScope(ws_id, ""), path):
        raise HTTPException(status_code=400, detail="This file cannot be shared")
    return _to_model(await ensure_file_link(ws_id, path))


@router.get("/{workspace_id}/share-links")
async def list_share_links(
    workspace_id: str, x_user_id: CurrentUserId
) -> dict[str, list[ShareLinkResponse]]:
    """Only shared links: a private one is found through its item."""
    workspace, _ = await _owned_workspace(workspace_id, x_user_id)
    links = await list_shared(str(workspace["workspace_id"]))
    return {"links": [_to_model(link) for link in links]}


@router.get("/{workspace_id}/share-links/{code}/files")
async def share_link_files(
    workspace_id: str, code: str, x_user_id: CurrentUserId
) -> dict[str, Any]:
    """The current file list, and how it differs from the list that is shared."""
    workspace, work_dir = await _owned_workspace(workspace_id, x_user_id)
    link = await _owned_link(workspace, code)
    if link.kind != KIND_FILE:
        raise HTTPException(status_code=400, detail="An app link has no file list")
    entries = await _current_files(workspace, link, x_user_id, work_dir)
    stored = list(link.files or ()) if link.shared else None
    return {
        "files": [e.as_dict() for e in entries],
        "total_size": sum(e.size for e in entries),
        "drift": manifest_drift(entries, stored),
    }


@router.patch("/{workspace_id}/share-links/{code}", response_model=ShareLinkResponse)
async def update_share_link(
    workspace_id: str, code: str, body: ShareLinkUpdate, x_user_id: CurrentUserId
) -> ShareLinkResponse:
    """Share with the list the owner reviewed, or make the link private again.

    The list is recomputed and must equal what the owner saw: a file added
    between the dialog opening and the switch flipping is a 409 carrying the
    new list, never a silent widening of the share. A Stop that lands while
    the list is being built is a 409 too, never undone by this write.
    """
    workspace, work_dir = await _owned_workspace(workspace_id, x_user_id)
    link = await _owned_link(workspace, code)

    if not body.shared:
        return _to_model(await set_private(code) or link)

    if link.kind != KIND_FILE:
        raise HTTPException(status_code=400, detail="An app link cannot be shared")
    if not body.files:
        raise HTTPException(
            status_code=422, detail="The reviewed file list is required to share"
        )
    entries = await _current_files(workspace, link, x_user_id, work_dir)
    current = [e.path for e in entries]
    if set(current) != set(body.files):
        raise HTTPException(
            status_code=409,
            detail={"code": "files_changed", "files": [e.as_dict() for e in entries]},
        )
    shared = await set_shared(code, current, expected_revision=link.revision)
    if shared is None:
        raise HTTPException(status_code=409, detail={"code": "link_changed"})
    return _to_model(shared)


@owner_router.get("/{code}", response_model=ShareLinkResponse)
async def get_share_link(code: str, x_user_id: CurrentUserId) -> ShareLinkResponse:
    """What one of the owner's links points at, so their app can open it in place.

    The workspace list holds shared links only, and an app link is never shared,
    so an ``/a/`` link in a reply is mapped back to its port or file through
    this. Any of the owner's workspaces answers, since a report-back carries an
    analyst's links into Home; the link names the workspace to open it in.
    Another user's code is not found rather than forbidden.
    """
    link = await get_link(code)
    workspace = await db_get_workspace(link.workspace_id) if link else None
    if link is None or workspace is None or workspace.get("user_id") != x_user_id:
        raise HTTPException(status_code=404, detail="Share link not found")
    return _to_model(link)
