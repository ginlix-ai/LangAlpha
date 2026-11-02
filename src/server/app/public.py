"""
Public Share Router — Unauthenticated endpoints for shared thread access.

All endpoints use an opaque share_token instead of thread/workspace IDs.
No auth required. workspace_id is resolved server-side and never exposed.
A token resolves to a (workspace, path) scope; the file endpoints serve that
path and its subtree and nothing else, whatever path the URL asks for.

The metadata route also answers for a file or app share link, which is what
the ``/a/`` page dispatches on. It takes optional auth for that: a private
link opens for its signed-in owner and for nobody else.

This module carries the thread itself: the metadata a viewer opens and the SSE
replay, with the owner-only marks stripped out of every event by
``services/share_redaction``. What the token authorizes lives in
``share_access``, the file routes in ``share_files``, and the branded failure
page in ``share_pages``.

Endpoints:
- GET /api/v1/public/shared/{share_token}          - Thread, file or app metadata
- GET /api/v1/public/shared/{share_token}/replay    — SSE conversation replay
- GET /api/v1/public/shared/{share_token}/files     — File listing (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/read     — Read file content (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/serve/{path} — Serve file inline with sandboxed CSP (requires allow_files)
- GET /api/v1/public/shared/{share_token}/files/download — Download raw file (requires allow_download)
"""

import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from src.observability import observe_replay_stream

from src.server.app.share_access import (
    LINK_NOT_FOUND,
    LinkAccess,
    get_permissions,
    get_shared_thread,
    resolve_link,
)
from src.server.app.share_files import share_files_router
from src.server.app.workspace_sandbox import owner_preview_url, with_preview_path
from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import Artifact, Version
from src.server.database.conversation.replay_rows import get_replay_thread_data
from src.server.database.pool import DATABASE_UNAVAILABLE
from src.server.database.share_links import KIND_APP
from src.server.services.artifacts import grants as artifact_grants
from src.server.services.file_grants import grant_prefix, mint_file_grant, seconds_left
from src.server.services.artifacts.snapshot import (
    LegacyRefused,
    SnapshotUnavailable,
    legacy_snapshot,
)
from src.server.services.preview_base import preview_base
from src.server.services.preview_links import signed_url_expires_at
from src.server.utils.api import PageViewer, Viewer, database_unavailable
from src.server.utils.error_sanitization import single_line
from src.server.services.share_redaction import ShareRedaction

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/public", tags=["Public Sharing"])
# The file routes live next door and mount here, so the token prefix and the
# tag stay in one place.
router.include_router(share_files_router)

# =============================================================================
# METADATA
# =============================================================================


async def _latest_shared_version(artifact: Artifact | None) -> tuple[Artifact, Version]:
    """The record behind a shared file link and the version its visitors see.

    A link shared before versions has none until this first asks, which cuts
    v1 from the list the share froze, from a warm session or the stored copy,
    never waking a computer. A 404 when the link is no longer shared, its file
    is gone, or its files are past the caps (asked once, then remembered), a
    503 when that first version cannot be read now.
    """
    if artifact is None or not artifact.shared:
        raise HTTPException(status_code=404, detail=LINK_NOT_FOUND)
    if artifact.latest_seq == 0:
        try:
            artifact = await legacy_snapshot(artifact)
        except LegacyRefused:
            raise HTTPException(status_code=404, detail=LINK_NOT_FOUND) from None
        except SnapshotUnavailable as e:
            logger.warning(
                f"First version of shared link {artifact.code} unavailable: "
                f"{single_line(str(e))}"
            )
            raise HTTPException(status_code=503, detail="File unavailable") from None
        if not artifact.shared:
            raise HTTPException(status_code=404, detail=LINK_NOT_FOUND)
    # The revision and the seq come from one read of the row, so the grant
    # minted from them names a version the row named at that revision.
    latest = await artifacts_db.get_version(artifact.artifact_id, artifact.latest_seq)
    if latest is None:
        raise HTTPException(status_code=503, detail="File unavailable")
    return artifact, latest


async def _file_metadata(access: LinkAccess) -> dict[str, Any]:
    """A file link: the live file to its owner, the latest version to anyone
    else.

    The owner reads and comments on the file as it is now, framed through the
    workspace file grant. A visitor gets a grant for one version at the link's
    current revision, which each of its files is checked against, so an
    unshare or the next Update ends it and the page asks here again.
    """
    link = access.link
    if access.owner:
        grant = await mint_file_grant(link.workspace_id)
        return {
            "kind": "file",
            "access": "owner",
            "artifact_id": link.artifact_id,
            "workspace_id": link.workspace_id,
            "name": link.display_title,
            "path": link.path,
            "shared": link.shared,
            "latest_seq": link.artifact.latest_seq if link.artifact else 0,
            "frame_base": grant_prefix(grant),
            "expires_in": seconds_left(grant.expires_at),
        }

    artifact, latest = await _latest_shared_version(link.artifact)
    seq = latest.seq
    visitor = await artifact_grants.mint_artifact_share_grant(
        link.code, artifact.artifact_id, revision=artifact.revision, seq=seq
    )
    base = preview_base()
    entry = latest.entry_path
    return {
        "kind": "file",
        "access": "public",
        "artifact_id": artifact.artifact_id,
        "name": entry.rsplit("/", 1)[-1],
        "path": entry,
        "title": latest.title,
        "seq": seq,
        "frame_base": (
            base.artifact_url(artifact.artifact_id, visitor.token)
            if base
            else artifact_grants.grant_prefix(visitor)
        ),
        "isolated": base is not None,
        "expires_in": seconds_left(visitor.expires_at),
    }


async def _link_metadata(
    access: LinkAccess, user_id: str | None, path: str | None
) -> dict[str, Any]:
    """What the ``/a/`` page renders for a link ``resolve_link`` admitted.

    ``expires_in`` is how many seconds the credential in ``url`` or
    ``frame_base`` has left, so the page renews it just before rather than on
    a timer.
    """
    link = access.link
    if link.kind == KIND_APP:
        # An app link is never shared, so only its owner is ever here.
        url = await owner_preview_url(link.workspace_id, user_id, link.port)
        return {
            "kind": "app",
            "title": link.display_title,
            "url": with_preview_path(url, path or link.path),
            "expires_in": seconds_left(await signed_url_expires_at(url)),
        }
    return await _file_metadata(access)


@router.get("/shared/{share_token}")
async def get_shared_thread_metadata(
    share_token: str,
    page_viewer: PageViewer,
    response: Response,
    view_as: str | None = Query(None, alias="as"),
    path: str | None = Query(None),
):
    """Metadata for a shared thread, file or app. Auth is optional.

    ``?as=visitor`` drops the viewer, so the owner sees exactly what a visitor
    sees, a private link included. ``?path=`` opens an app at a page other
    than its entry, which is how an old preview URL keeps its suffix. A token
    that is not a link is a thread token and answers as it always has.
    """
    # The answer depends on the bearer and can carry the owner's grant, so no
    # cache between here and the browser may hand it to the next visitor.
    response.headers["Cache-Control"] = "no-store"
    viewer = Viewer(None) if view_as == "visitor" else page_viewer
    try:
        access = await resolve_link(share_token, user_id=viewer.user_id)
        thread = None if access is not None else await get_shared_thread(share_token)
    except HTTPException as e:
        # Not found is said only to a viewer the keys could check. Any other
        # may be the owner, and a private link and an unknown code must still
        # answer alike, so both wait for the keys.
        if e.status_code == 404 and viewer.unconfirmed is not None:
            raise viewer.unconfirmed from None
        raise
    if access is not None:
        return await _link_metadata(access, viewer.user_id, path)

    perms = get_permissions(thread)
    allow_files = perms.get("allow_files", False)

    return {
        "kind": "thread",
        "thread_id": str(thread["conversation_thread_id"]),
        "title": thread.get("title"),
        "msg_type": thread.get("msg_type"),
        "created_at": thread.get("created_at"),
        "updated_at": thread.get("updated_at"),
        "workspace_name": thread.get("workspace_name"),
        # The transcript's absolute paths are spelled with these, so the viewer
        # reads them as workspace paths the way the owner does. A turn from
        # before a move spells them under a root the workspace has left, which
        # only opens a file, and which this thread may never show, so those
        # roots go only with the files. Former folder names stay out: they are
        # old workspace names the share never showed, and the file routes
        # still fold them.
        "root_dir": thread.get("root_dir"),
        "previous_root_dirs": (
            list(thread.get("previous_root_dirs") or ()) if allow_files else []
        ),
        "dir_name": thread.get("dir_name"),
        "permissions": {
            "allow_files": allow_files,
            "allow_download": perms.get("allow_download", False),
        },
    }


# =============================================================================
# REPLAY
# =============================================================================


@router.get("/shared/{share_token}/replay")
async def replay_shared_thread(share_token: str):
    """Replay a shared thread as SSE. No auth required.

    The same assembly as the owner's replay, so a viewer sees the turns the
    owner sees; the thread resolves through the share token and every event
    passes ``ShareRedaction`` on the way out.
    """
    from src.server.services.history.replay import finish_lines, read_replay_page

    try:
        thread = await get_shared_thread(share_token)
        thread_id = str(thread["conversation_thread_id"])
        rows = await get_replay_thread_data(thread_id)
        if rows is None:
            raise HTTPException(status_code=404, detail="Shared thread not found")
        page, _ = await read_replay_page(rows)
        # Public replay has no /status reconciliation at all, so without the
        # stamp its task cards would be stuck "running" forever (see
        # history/task_status.py). Only the whitelisted status value is added,
        # which redaction leaves as it is.
        await finish_lines(thread_id, page.lines, status_only=True)
    except DATABASE_UNAVAILABLE as e:
        # As the owner's replay answers it.
        raise database_unavailable("replaying a shared thread", logger, e)
    items = [line.item() for line in page.lines]
    redaction = ShareRedaction(items)

    async def event_generator():
        seq = 0
        for item in items:
            event_type, data = item["event"], item["data"]
            if event_type == "user_message":
                replay_data = _shared_user_message(redaction, data)
            else:
                replay_data = redaction.event(event_type, data)
                if replay_data is None:
                    continue
                replay_data.setdefault("thread_id", thread_id)
            seq += 1
            yield (
                f"id: {seq}\n"
                f"event: {event_type}\n"
                f"data: {json.dumps(replay_data, ensure_ascii=False, default=str)}\n\n"
            )

        seq += 1
        yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps({'thread_id': thread_id}, default=str)}\n\n"

    return StreamingResponse(
        observe_replay_stream(event_generator(), source="public"),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )


def _shared_user_message(redaction: ShareRedaction, data: dict[str, Any]) -> dict[str, Any]:
    """A turn's user message as a viewer may read it.

    The run ids stay out: they exist for the owner's report-back catch-up and
    report rows, neither of which a public viewer has.
    """
    content, metadata = redaction.query(
        {
            "type": data.get("query_type"),
            "content": data.get("content"),
            "metadata": data.get("metadata"),
        }
    )
    return {
        **{k: v for k, v in data.items() if k not in ("run_id", "report_run_id")},
        "content": content,
        "metadata": metadata,
    }
