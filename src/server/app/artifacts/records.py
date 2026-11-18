"""The owner's routes on a file's record: find or make it, where its comments
go, publishing and versions.

A shared workspace's members use them as its owner does: the record is the
holder's, read and written as the holder's rows (``Access.owner_id``), while
who acted is the member, and who a link may open to is judged by the
holder's organization, as the link's visitors are. Publishing, unsharing,
choosing the audience and keeping a shared file updated go as far as the
workspace's managers allow (``shared_workspaces.share_out``)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from src.server.app.artifacts.models import (
    ArtifactGrantResponse,
    ArtifactLookupResponse,
    ArtifactPathRequest,
    ArtifactResponse,
    ArtifactThreadResponse,
    AudienceRequest,
    ArtifactVersionResponse,
    BindRequest,
    DefaultDestination,
    DestinationsResponse,
    DriftResponse,
    KeepUpdatedRequest,
    KeptRequest,
    LeftOutRequest,
    PublishRequest,
    PublishResponse,
    ShareCapResponse,
    ShareFileEntryResponse,
    StatusRequest,
    StatusResponse,
)
from src.server.services.workspace_files import owner_work_dir
from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import Artifact
from src.server.database.conversation import thread_reach
from src.server.database.share_codes import share_path
from src.server.services.artifacts import snapshot
from src.server.services.artifacts.events import publish_artifact_change
from src.server.services.artifacts.grants import (
    grant_prefix,
    mint_artifact_grant,
    mint_artifact_member_grant,
)
from src.server.services.file_grants import seconds_left
from src.server.services import activity
from src.server.services.activity import Action
from src.server.services.share_audience import (
    ORGANIZATION,
    PUBLIC,
    link_audience,
    offered,
    refuse_public_link_of,
    require_members_audience,
)
from src.server.services.preview_base import artifact_base
from src.server.services.served_paths import may_be_artifact
from src.server.services.workspace_file_containment import contained_relative_path
from src.server.services.account import AccountSuspended
from src.server.services.shared_workspaces.access import Access, NoAccess, workspace_access
from src.server.services.shared_workspaces.share_out import refuse_link_change
from src.server.services.workspace_files import (
    previous_dir_names_of,
    reached_workspace_on_computer,
)
from src.server.utils.api import CurrentUserId
from src.server.utils.pg_sanitize import normalize_uuid

router = APIRouter()

#: As many as a version may hold, the longest list an owner unticks from.
_MAX_LEFT_OUT = snapshot.MAX_VERSION_FILES
workspace_router = APIRouter(prefix="/api/v1/workspaces", tags=["Artifacts"])


# --- helpers -----------------------------------------------------------------


def not_found(what: str = "Artifact") -> HTTPException:
    return HTTPException(status_code=404, detail=f"{what} not found")


def conflict(code: str, **extra: Any) -> HTTPException:
    return HTTPException(status_code=409, detail={"code": code, **extra})


def path_id(value: str, what: str = "Artifact") -> str:
    """An id from the path, or the 404 a missing row gets."""
    normalized = normalize_uuid(value)
    if normalized is None:
        raise not_found(what)
    return normalized


@contextmanager
def db_errors() -> Iterator[None]:
    """The database's refusals as every owner route answers them."""
    try:
        yield
    except artifacts_db.ArtifactNotFound:
        raise not_found() from None
    except artifacts_db.CommentNotFound:
        raise not_found("Comment") from None
    except artifacts_db.DestinationUnavailable:
        raise conflict("destination_unavailable") from None
    except artifacts_db.SharingChanged:
        raise conflict("sharing_changed") from None
    except artifacts_db.VersionMoved:
        raise conflict("version_moved") from None
    except artifacts_db.CommentConflict as e:
        raise conflict(e.code, message=e.message) from None


@contextmanager
def _snapshot_errors() -> Iterator[None]:
    try:
        yield
    except snapshot.SnapshotRefused as e:
        if e.code == "file_missing":
            raise HTTPException(
                status_code=404, detail={"code": "file_missing"}
            ) from None
        if e.code == "files_changed":
            raise conflict(
                "files_changed", files=[entry.as_dict() for entry in e.files or ()]
            ) from None
        if e.code == "needs_computer":
            raise conflict("needs_computer") from None
        raise HTTPException(
            status_code=422, detail={"code": e.code, "limit": e.limit}
        ) from None
    except snapshot.SnapshotUnavailable:
        raise HTTPException(
            status_code=503,
            detail="The file cannot be read right now. Try again shortly.",
        ) from None


async def owned(artifact_id: str, user_id: str) -> Artifact:
    return (await reached(artifact_id, user_id))[0]


async def reached(artifact_id: str, caller: str) -> tuple[Artifact, Access]:
    """The record and the caller's access to its workspace: its owner's, or
    a shared workspace member's. Anyone else's, or a deleted workspace's, is
    not found, as every owner route answers."""
    artifact_id = path_id(artifact_id)
    artifact = await artifacts_db.get_artifact(artifact_id, user_id=caller)
    if artifact is not None:
        return artifact, Access(caller)
    from src.server.services.membership import teams_door_open

    # Only a shared workspace's records are reached by anyone but their
    # owner, and with the door shut there are none.
    if not teams_door_open():
        raise not_found()
    workspace_id = await artifacts_db.artifact_workspace_id(artifact_id)
    if workspace_id is None:
        raise not_found()
    from src.server.database.workspace import get_workspace

    try:
        access = await workspace_access(await get_workspace(workspace_id), caller)
    except (NoAccess, AccountSuspended):
        raise not_found() from None
    artifact = await artifacts_db.get_artifact(artifact_id, user_id=access.owner_id)
    if artifact is None:
        raise not_found()
    return artifact, access


async def _response(artifact: Artifact, user_id: str, viewer: str) -> ArtifactResponse:
    """``user_id`` is the record's owner: the holder's for a shared
    workspace, whose comments and links they are. A thread it is bound to
    that ``viewer`` does not reach is left out, as is everything of it."""
    counts = await artifacts_db.comment_counts(artifact.artifact_id, user_id=user_id)
    bound = artifact.bound_thread_id
    if bound and viewer != user_id and await thread_reach.hidden([bound], viewer):
        bound = None
    return ArtifactResponse(
        artifact_id=artifact.artifact_id,
        code=artifact.code,
        url=share_path(artifact.code),
        workspace_id=artifact.workspace_id,
        path=artifact.path,
        name=artifact.name,
        shared=artifact.shared,
        shared_at=artifact.shared_at,
        keep_updated=artifact.keep_updated,
        kept=artifact.kept,
        latest_seq=artifact.latest_seq,
        shared_seq=artifact.shared_seq,
        bound_thread_id=bound,
        open_comments=counts.open,
        unread=counts.unread,
        created_at=artifact.created_at,
        legacy_refused=artifact.legacy_refused,
        audience=link_audience(artifact.audience),
        audiences=await offered(user_id, link_audience(artifact.audience)),
    )


async def _changed(user_id: str, artifact: Artifact, viewer: str) -> ArtifactResponse:
    await publish_artifact_change(user_id, artifact)
    return await _response(artifact, user_id, viewer)


def artifact_path(workspace: dict[str, Any], path: str) -> tuple[str, str]:
    """The workspace's id and ``path`` as the canonical path of a file that
    may have a record, or 400. Any spelling the file routes accept."""
    relative = contained_relative_path(
        path, owner_work_dir(workspace), previous_dir_names_of(workspace)
    )
    if relative is None or not may_be_artifact(relative):
        raise HTTPException(status_code=400, detail="This file cannot be an artifact")
    return str(workspace["workspace_id"]), relative


async def _workspace_path(
    workspace_id: str, path: str, user_id: str
) -> tuple[str, str, str]:
    """``artifact_path`` in a workspace the caller reaches, and whose rows
    its records are. Another user's workspace, or a deleted one, is not
    found, as every owner route answers."""
    try:
        workspace, _ = await reached_workspace_on_computer(workspace_id, user_id)
    except HTTPException as e:
        if e.status_code == 403:
            raise not_found("Workspace") from None
        raise
    if workspace.get("status") == "deleted":
        raise not_found("Workspace")
    return (*artifact_path(workspace, path), str(workspace["user_id"]))


# --- records -----------------------------------------------------------------


@workspace_router.get(
    "/{workspace_id}/artifacts", response_model=ArtifactLookupResponse
)
async def find_artifact(
    workspace_id: str,
    x_user_id: CurrentUserId,
    path: str = Query(..., min_length=1, max_length=4096),
) -> ArtifactLookupResponse:
    """The file's record, or null for a file never recorded. Never makes one."""
    ws_id, relative, owner_id = await _workspace_path(workspace_id, path, x_user_id)
    artifact = await artifacts_db.find_artifact(ws_id, relative)
    return ArtifactLookupResponse(
        artifact=await _response(artifact, owner_id, x_user_id) if artifact else None
    )


@workspace_router.post("/{workspace_id}/artifacts", response_model=ArtifactResponse)
async def ensure_artifact(
    workspace_id: str, body: ArtifactPathRequest, x_user_id: CurrentUserId
) -> ArtifactResponse:
    """The file's record, made private if new."""
    ws_id, relative, owner_id = await _workspace_path(workspace_id, body.path, x_user_id)
    artifact = await artifacts_db.ensure_artifact(ws_id, relative)
    return await _response(artifact, owner_id, x_user_id)


@router.get("/{artifact_id}", response_model=ArtifactResponse)
async def get_artifact(artifact_id: str, x_user_id: CurrentUserId) -> ArtifactResponse:
    artifact, access = await reached(artifact_id, x_user_id)
    return await _response(artifact, access.owner_id, x_user_id)


@router.patch("/{artifact_id}", response_model=ArtifactResponse)
async def update_artifact(
    artifact_id: str, body: KeepUpdatedRequest, x_user_id: CurrentUserId
) -> ArtifactResponse:
    current, access = await reached(artifact_id, x_user_id)
    if current.shared:
        # Each later version a kept-updated link cuts reaches its visitors.
        await refuse_link_change(access)
    with db_errors():
        artifact = await artifacts_db.set_keep_updated(
            user_id=access.owner_id,
            artifact_id=path_id(artifact_id),
            keep_updated=body.keep_updated,
        )
    return await _changed(access.owner_id, artifact, x_user_id)


@router.put("/{artifact_id}/kept", response_model=ArtifactResponse)
async def set_artifact_kept(
    artifact_id: str, body: KeptRequest, x_user_id: CurrentUserId
) -> ArtifactResponse:
    """Keep the file for the owner without sharing it, which is how a page
    becomes their dashboard: the server holds a version of it and the files it
    reads, so it opens while its computer is off, and the end of each turn in
    its workspace cuts another when they changed. The first version is cut
    now, from the files as they are (a stopped computer is started); the same
    409s and 503s as a publish's read. A shared file's visitors stay on their
    version (``shared_seq``). Stopping keeps the versions cut so far. A keep
    refused leaves the record as it was.
    """
    artifact, access = await reached(artifact_id, x_user_id)
    with db_errors(), _snapshot_errors():
        if body.kept:
            artifact = await snapshot.keep(
                artifact,
                user_id=access.owner_id,
                loaded=tuple(body.loaded),
                earlier=tuple(body.earlier),
                actor=x_user_id,
            )
        else:
            artifact = await artifacts_db.stop_keeping(
                user_id=access.owner_id, artifact_id=artifact.artifact_id
            )
    return await _changed(access.owner_id, artifact, x_user_id)


@router.post("/{artifact_id}/left-out", response_model=ArtifactResponse)
async def set_file_left_out(
    artifact_id: str, body: LeftOutRequest, x_user_id: CurrentUserId
) -> ArtifactResponse:
    """Take a file the page does not need to draw off what a publish carries, or
    put it back. A file the page loads goes along regardless, so naming one here
    changes nothing."""
    artifact, access = await reached(artifact_id, x_user_id)
    with db_errors():
        artifact = await artifacts_db.set_left_out(
            user_id=access.owner_id,
            artifact_id=artifact.artifact_id,
            path=body.path,
            left_out=body.left_out,
            cap=_MAX_LEFT_OUT,
        )
    if body.left_out and body.path not in artifact.left_out_files:
        raise HTTPException(status_code=422, detail="too_many_left_out")
    return await _changed(access.owner_id, artifact, x_user_id)


# --- where comments go -------------------------------------------------------


@router.put("/{artifact_id}/binding", response_model=ArtifactResponse)
async def bind_artifact(
    artifact_id: str, body: BindRequest, x_user_id: CurrentUserId
) -> ArtifactResponse:
    """Send the file's comments to a thread until unbound: one in the file's
    workspace, or a Chief of Staff thread."""
    _, access = await reached(artifact_id, x_user_id)
    with db_errors():
        artifact = await artifacts_db.bind(
            user_id=access.owner_id,
            artifact_id=path_id(artifact_id),
            thread_id=body.thread_id,
            caller=x_user_id,
        )
    return await _changed(access.owner_id, artifact, x_user_id)


@router.delete("/{artifact_id}/binding", response_model=ArtifactResponse)
async def unbind_artifact(
    artifact_id: str, x_user_id: CurrentUserId
) -> ArtifactResponse:
    _, access = await reached(artifact_id, x_user_id)
    with db_errors():
        artifact = await artifacts_db.unbind(
            user_id=access.owner_id, artifact_id=path_id(artifact_id)
        )
    return await _changed(access.owner_id, artifact, x_user_id)


def _thread(
    thread: artifacts_db.DestinationThread | None,
) -> ArtifactThreadResponse | None:
    return ArtifactThreadResponse.model_validate(thread) if thread else None


@router.get("/{artifact_id}/destinations", response_model=DestinationsResponse)
async def get_destinations(
    artifact_id: str,
    x_user_id: CurrentUserId,
    beside: str | None = Query(None, description="The thread beside the panel."),
) -> DestinationsResponse:
    """The thread a comment goes to unless the owner picks another, and the
    candidates the composer's menu offers."""
    _, access = await reached(artifact_id, x_user_id)
    with db_errors():
        found = await artifacts_db.destinations(
            user_id=access.owner_id,
            artifact_id=path_id(artifact_id),
            beside_thread_id=normalize_uuid(beside) if beside else None,
            caller=x_user_id,
        )
    default, rule = found.default
    return DestinationsResponse(
        default=DefaultDestination(
            rule=rule, thread=_thread(default), workspace_id=found.artifact.workspace_id
        ),
        bound=_thread(found.bound),
        beside=_thread(found.beside),
        creator=_thread(found.creator),
    )


# --- publish and versions ----------------------------------------------------


@router.post("/{artifact_id}/status", response_model=StatusResponse)
async def get_status(
    artifact_id: str, body: StatusRequest, x_user_id: CurrentUserId
) -> StatusResponse:
    """The live file against the shared version (the latest when not shared),
    and the files a publish now would carry, what the live page reported
    loading among them. Reads the
    computer, or the stored copy when it is stopped, which holds no library
    folder: a page loading from one is a 409 ``needs_computer``. Past a cap,
    the files listed before the walk stopped, with ``over_cap``, so the owner
    can leave out what puts it over; a publish is still refused. A POST
    because the report can outgrow a URL."""
    return await _status(
        artifact_id, x_user_id, tuple(body.loaded), tuple(body.earlier)
    )


# A tab still on the build before the POST reads status this way until it reloads.
@router.get("/{artifact_id}/status", response_model=StatusResponse)
async def get_status_unreported(
    artifact_id: str, x_user_id: CurrentUserId
) -> StatusResponse:
    return await _status(artifact_id, x_user_id, (), ())


async def _status(
    artifact_id: str, user_id: str, loaded: tuple[str, ...], earlier: tuple[str, ...]
) -> StatusResponse:
    artifact, access = await reached(artifact_id, user_id)
    with _snapshot_errors():
        found = await snapshot.status(
            artifact,
            user_id=access.owner_id,
            loaded=loaded,
            earlier=earlier,
            actor=user_id,
        )
    over, sealed = found.over_cap, found.snapshot
    entries = over.entries if over else sealed.entries
    left_out = over.left_out if over else sealed.left_out
    return StatusResponse(
        latest_seq=found.latest_seq,
        live_sha256=None if over else sealed.entry_sha256,
        page_sha256=None if over else sealed.page_sha256,
        files=[ShareFileEntryResponse.model_validate(e) for e in entries],
        left_out=[ShareFileEntryResponse.model_validate(e) for e in left_out],
        total_size=sum(e.size or 0 for e in entries),
        changed=found.changed,
        drift=DriftResponse.model_validate(found.drift) if found.drift else None,
        over_cap=ShareCapResponse(code=over.code, limit=over.limit) if over else None,
        root_refs=[] if over else sealed.root_refs,
    )


@router.post("/{artifact_id}/publish", response_model=PublishResponse)
async def publish_artifact(
    artifact_id: str, body: PublishRequest, x_user_id: CurrentUserId
) -> PublishResponse:
    """Share the file and cut a version from the list the owner reviewed.

    The list is found again and must match, never widening on its own (409
    ``files_changed`` with the list as it is now, as for a file left out or
    put back during the read). A file equal to its latest version cuts nothing
    and is shared at that version. A version cut during the read is a 409
    ``version_moved``: publishing again reads the file anew. A stopped
    computer's page that loads from a library folder is a 409
    ``needs_computer``, as for status.
    """
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    await refuse_link_change(access)
    if link_audience(artifact.audience) == ORGANIZATION:
        await require_members_audience(owner_id)
    elif access.is_shared:
        # A shared workspace's link opens to members only, so its first
        # publish takes that audience rather than being refused the default.
        await require_members_audience(owner_id)
        with db_errors():
            artifact = await artifacts_db.set_audience(
                user_id=owner_id,
                artifact_id=artifact.artifact_id,
                audience=ORGANIZATION,
            )
    else:
        await refuse_public_link_of(owner_id)
    with db_errors(), _snapshot_errors():
        result = await snapshot.publish(
            artifact,
            user_id=owner_id,
            actor=x_user_id,
            reviewed=body.files,
            label=body.label,
            keep_updated=body.keep_updated,
            loaded=tuple(body.loaded),
            earlier=tuple(body.earlier),
            reviewed_page=body.page_sha256,
        )
    version = await artifacts_db.get_version(artifact.artifact_id, result.seq)
    if version is None:
        raise not_found()
    return PublishResponse(
        artifact=await _response(result.artifact, owner_id, x_user_id),
        version=ArtifactVersionResponse.model_validate(version),
        cut=result.cut,
    )


@router.put("/{artifact_id}/audience", response_model=ArtifactResponse)
async def set_audience(
    artifact_id: str, body: AudienceRequest, x_user_id: CurrentUserId
) -> ArtifactResponse:
    """Who the link opens to. Members of the owner's organization only, which
    the owner must be an active member of to choose, or anyone, which on a
    shared link is turning on a public link (``public_links``). Holds on every
    visitor's next request; a member's grant opens nothing once the link opens
    to anyone, and an anonymous one nothing once it opens to members."""
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    await refuse_link_change(access)
    before = link_audience(artifact.audience)
    if body.audience == ORGANIZATION:
        await require_members_audience(owner_id)
    elif artifact.shared and before != PUBLIC:
        await refuse_public_link_of(owner_id)
    if body.audience == before:
        return await _response(artifact, owner_id, x_user_id)
    with db_errors():
        artifact = await artifacts_db.set_audience(
            user_id=owner_id,
            artifact_id=artifact.artifact_id,
            audience=ORGANIZATION if body.audience == ORGANIZATION else None,
        )
    activity.record(
        x_user_id,
        Action.FILE_AUDIENCE_CHANGED,
        resource=("artifact", artifact.artifact_id),
        workspace_id=artifact.workspace_id,
        detail={
            "path": artifact.path,
            "audience": body.audience,
            "shared": artifact.shared,
        },
    )
    return await _changed(owner_id, artifact, x_user_id)


@router.post("/{artifact_id}/unshare", response_model=ArtifactResponse)
async def unshare_artifact(
    artifact_id: str, x_user_id: CurrentUserId
) -> ArtifactResponse:
    """Stop sharing; the versions stay, and publishing again resumes at the
    same link."""
    _, access = await reached(artifact_id, x_user_id)
    await refuse_link_change(access)
    with db_errors():
        artifact = await artifacts_db.unshare(
            user_id=access.owner_id, artifact_id=path_id(artifact_id)
        )
    snapshot.record_unshared(x_user_id, artifact)
    return await _changed(access.owner_id, artifact, x_user_id)


@router.get("/{artifact_id}/versions")
async def list_versions(
    artifact_id: str, x_user_id: CurrentUserId
) -> dict[str, list[ArtifactVersionResponse]]:
    artifact = await owned(artifact_id, x_user_id)
    versions = await artifacts_db.list_versions(artifact.artifact_id)
    return {"versions": [ArtifactVersionResponse.model_validate(v) for v in versions]}


@router.post("/{artifact_id}/grant", response_model=ArtifactGrantResponse)
async def mint_grant(
    artifact_id: str, x_user_id: CurrentUserId
) -> ArtifactGrantResponse:
    """Where the owner's viewer frames a published version from, read only;
    see ``services/artifacts/grants``. A shared workspace's member gets a
    grant bound to them, which ends when they stop reaching the workspace."""
    artifact, access = await reached(artifact_id, x_user_id)
    if access.is_shared:
        grant = await mint_artifact_member_grant(artifact.artifact_id, x_user_id)
    else:
        grant = await mint_artifact_grant(artifact.artifact_id)
    base = artifact_base()
    return ArtifactGrantResponse(
        base=base.artifact_url(grant.artifact_id, grant.token)
        if base
        else grant_prefix(grant),
        expires_in=seconds_left(grant.expires_at),
        isolated=base is not None,
    )
