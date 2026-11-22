"""Shared workspaces: create one, list yours, and manage who is in it.

A shared workspace belongs to an organization. Its people (those listed on
it, and those in the one team it may name, or everyone in the organization)
each run their own turns in it, under their own account, and see each
other's shared threads and files. Every route answers 404 unless the ``shared_workspaces`` flag resolves
on for the caller, which needs ``teams`` and the organization's grant, and
503 while the caller's account cannot be read. Who may create one is the
``shared_workspaces.create`` rule, which the auth service sends per account
(owners and admins always allowed).

The workspace itself, its threads and its files are reached through the
ordinary ``/workspaces`` and ``/threads`` routes, which accept a member where
they accept the owner.

The organization's owners and admins list, hand over and delete its shared
workspaces from the console, through the service routes at the end, which
the auth service calls with its service token and no user.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from src.server.app.workspaces import workspace_response
from src.server.database import shared_workspaces_console as console_db
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.dependencies.principal import ReadPrincipal, WorkspaceWritePrincipal
from src.server.models.shared_workspace import (
    ConsoleMember,
    ConsoleSharedWorkspace,
    ConsoleSharedWorkspacePage,
    MemberAdd,
    MembersView,
    MemberUpdate,
    SharedStanding,
    SharedWorkspace,
    SharedWorkspaceCreate,
    SharedWorkspaceList,
    SharingUpdate,
    TeamUsage,
    TeamView,
)
from src.server.services.policy import SHARED_WORKSPACES_CREATE
from src.server.services.account import require_policy
from src.server.services.shared_workspaces import service
from src.server.services.shared_workspaces.access import (
    SharedAccess,
    workspace_access,
    standing,
)
from src.server.utils.api import ServiceCaller

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Shared workspaces"])


@router.post(
    "/api/v1/shared-workspaces", response_model=SharedWorkspace, status_code=201
)
async def create_shared_workspace(
    body: SharedWorkspaceCreate, principal: WorkspaceWritePrincipal
) -> SharedWorkspace:
    """Create a shared workspace in the caller's organization, on a cloud
    computer of its own, with the caller as its first manager and the
    people named as members."""
    _no_computer_token(principal)
    from src.server.database.workspace_names import (
        WorkspaceNameInvalid,
        WorkspaceNameTaken,
    )

    try:
        workspace, shared = await service.create(
            principal.user_id,
            name=body.name,
            description=body.description,
            everyone=body.everyone,
            member_emails=body.members,
            team_id=body.team_id,
        )
    except (WorkspaceNameInvalid, WorkspaceNameTaken) as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    # Read again: the binding made after the row was created may move it.
    full = await db_get_workspace(str(workspace["workspace_id"]))
    return SharedWorkspace(
        workspace=workspace_response(full), shared=await service.shared_block(shared)
    )


@router.get("/api/v1/shared-workspaces", response_model=SharedWorkspaceList)
async def list_shared_workspaces(principal: ReadPrincipal) -> SharedWorkspaceList:
    """The caller's shared workspaces: those they are listed on, those
    naming a team they are in, and those open to everyone in their
    organization."""
    _no_computer_token(principal)
    await service.require_seat(principal.user_id)
    policy = await require_policy(principal.user_id)
    found = await service.list_for(principal.user_id)
    return SharedWorkspaceList(
        workspaces=[
            SharedWorkspace(workspace=workspace_response(w), shared=b, has_unread=unread)
            for w, b, unread in found
        ],
        can_create=policy.allows(SHARED_WORKSPACES_CREATE),
    )


@router.get(
    "/api/v1/shared-workspaces/{workspace_id}/standing", response_model=SharedStanding
)
async def get_standing(workspace_id: str, principal: ReadPrincipal) -> SharedStanding:
    """Where a shared workspace the caller's page held stands now, so a page
    that lost it while away can say whether it was deleted or the caller is
    no longer in it. Someone removed before it was deleted is told ``gone``,
    as for any workspace they never reached."""
    _no_computer_token(principal)
    await service.require_seat(principal.user_id)
    return SharedStanding(state=await standing(workspace_id, principal.user_id))


async def _shared(workspace_id: str, caller: str, *, manage: bool = False) -> SharedAccess:
    # Absent, as a route that does not exist is, for a caller who cannot
    # have shared workspaces and for a workspace that is not one.
    await service.require_seat(caller)
    access = await workspace_access(
        await db_get_workspace(workspace_id), caller, manage=manage
    )
    if access.shared is None:
        raise service.not_available()
    return access.shared


def _no_computer_token(principal: Any) -> None:
    # A computer's token acts for its own computer, never on who may use a
    # shared one.
    if principal.computer_id is not None:
        raise HTTPException(
            status_code=403,
            detail={
                "type": "forbidden_for_token",
                "message": "A computer's token cannot manage shared workspaces",
            },
        )


@router.get("/api/v1/workspaces/{workspace_id}/members", response_model=MembersView)
async def list_members(workspace_id: str, principal: ReadPrincipal) -> MembersView:
    return await service.members_view(await _shared(workspace_id, principal.user_id))


@router.get("/api/v1/workspaces/{workspace_id}/team", response_model=TeamView)
async def get_team(workspace_id: str, principal: ReadPrincipal) -> TeamView:
    """The team the workspace names and its people, as the auth service
    says now, for anyone who reaches the workspace. 404 when it names none;
    503 when the auth service cannot say."""
    return await service.team_view(await _shared(workspace_id, principal.user_id))


@router.post(
    "/api/v1/workspaces/{workspace_id}/members",
    response_model=MembersView,
    status_code=201,
)
async def add_member(
    workspace_id: str, body: MemberAdd, principal: WorkspaceWritePrincipal
) -> MembersView:
    """Add an active member of the organization, by email address (manager)."""
    shared = await _shared(workspace_id, principal.user_id, manage=True)
    await service.add_member(shared, body.email, body.role)
    return await service.members_view(shared)


@router.patch("/api/v1/workspaces/{workspace_id}/members", response_model=MembersView)
async def update_sharing(
    workspace_id: str, body: SharingUpdate, principal: WorkspaceWritePrincipal
) -> MembersView:
    """Open the workspace to everyone in the organization, or only to the
    listed people and its team; name a team the caller is in, or none;
    choose who may share its work outside it; and keep members' own context
    out of it or let it follow the organization's grant (manager). Naming a
    team closes it to everyone, and opening it clears the team. Whoever
    reached it only through what was taken away has their turns there
    stopped."""
    if body.member_context is not None:
        # Letting members' own context in is the managers' own act: a
        # computer's token, which an agent's commands carry, never makes it.
        _no_computer_token(principal)
    shared = await _shared(workspace_id, principal.user_id, manage=True)
    if body.changes_settings:
        await service.update_settings(
            shared, sharing=body.sharing, member_context=body.member_context
        )
    if body.everyone is not None or body.changes_team:
        await service.set_audience(
            shared, everyone=body.everyone, team_id=body.team_id, names_team=body.changes_team
        )
    return await service.members_view(await _shared(workspace_id, principal.user_id))


@router.patch(
    "/api/v1/workspaces/{workspace_id}/members/{user_id}", response_model=MembersView
)
async def update_member(
    workspace_id: str, user_id: str, body: MemberUpdate, principal: WorkspaceWritePrincipal
) -> MembersView:
    """Change a member's role (manager). The last active manager cannot be
    made a member while others remain."""
    shared = await _shared(workspace_id, principal.user_id, manage=True)
    await service.set_role(shared, user_id, body.role)
    return await service.members_view(await _shared(workspace_id, principal.user_id))


@router.delete("/api/v1/workspaces/{workspace_id}/members/{user_id}", status_code=204)
async def remove_member(
    workspace_id: str, user_id: str, principal: WorkspaceWritePrincipal
) -> None:
    """Remove a member (manager), or leave (anyone, naming themselves).
    Their access ends at once and a turn of theirs running here is stopped."""
    leaving = user_id == principal.user_id
    shared = await _shared(workspace_id, principal.user_id, manage=not leaving)
    await service.remove_member(shared, user_id)


# --- the console -------------------------------------------------------------


def _console_entry(row: dict[str, Any]) -> ConsoleSharedWorkspace:
    members = [ConsoleMember(**m) for m in row.get("members", [])]
    return ConsoleSharedWorkspace(
        workspace_id=row["workspace_id"],
        name=row["name"],
        description=row.get("description"),
        created_by=row["created_by"],
        created_by_name=row.get("created_by_name"),
        created_at=row["created_at"],
        everyone=bool(row["everyone"]),
        team_id=row.get("team_id"),
        sharing=row["share_out"],
        members=members,
        managers=[m.user_id for m in members if m.role == "manager"],
        last_activity_at=row.get("last_activity_at"),
        computer_status=row.get("computer_status"),
        turn_running=bool(row.get("turn_running")),
    )


@router.get(
    "/api/v1/shared-workspaces/service",
    response_model=ConsoleSharedWorkspacePage,
    dependencies=[ServiceCaller],
)
async def list_for_console(
    org_id: str = Query(..., min_length=1),
    cursor: Optional[str] = None,
    limit: int = Query(default=25, ge=1, le=100),
) -> ConsoleSharedWorkspacePage:
    """Every live shared workspace of the organization, newest first, for its
    owners and admins. Read whatever the flags say now: the workspaces are
    the organization's."""
    try:
        rows, next_cursor = await console_db.list_for_org(org_id, cursor=cursor, limit=limit)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid cursor") from None
    return ConsoleSharedWorkspacePage(
        workspaces=[_console_entry(r) for r in rows], next_cursor=next_cursor
    )


@router.get(
    "/api/v1/shared-workspaces/service/teams/{team_id}",
    response_model=TeamUsage,
    dependencies=[ServiceCaller],
)
async def team_usage(team_id: str, org_id: str = Query(..., min_length=1)) -> TeamUsage:
    """How many of the organization's live shared workspaces name the team,
    so a team still named is not removed from under them. Read whatever the
    flags say now: the workspaces are the organization's."""
    return TeamUsage(workspaces=await service.count_naming_team(org_id, team_id))


@router.get(
    "/api/v1/shared-workspaces/service/{workspace_id}",
    response_model=ConsoleSharedWorkspace,
    dependencies=[ServiceCaller],
)
async def get_for_console(
    workspace_id: str, org_id: str = Query(..., min_length=1)
) -> ConsoleSharedWorkspace:
    """One of the organization's live shared workspaces, as the list gives
    it, for the console to read before or after a change. 404 for another
    organization's workspace, a deleted one, or none."""
    return await _console_entry_of(workspace_id, org_id)


async def _console_entry_of(workspace_id: str, org_id: str) -> ConsoleSharedWorkspace:
    rows, _ = await console_db.list_for_org(org_id, workspace_id=workspace_id)
    if not rows:
        raise HTTPException(status_code=404, detail="Not found")
    return _console_entry(rows[0])


@router.put(
    "/api/v1/shared-workspaces/service/{workspace_id}/managers/{user_id}",
    response_model=ConsoleSharedWorkspace,
    dependencies=[ServiceCaller],
)
async def make_manager_for_console(
    workspace_id: str,
    user_id: str,
    org_id: str = Query(..., min_length=1),
    actor_id: str = Query(..., min_length=1, max_length=255),
) -> ConsoleSharedWorkspace:
    """Make someone a manager of one of the organization's shared workspaces,
    as the owner or admin ``actor_id`` asked in the console; someone not
    listed is added, once they are an active member of the organization.
    404 for another organization's workspace."""
    await service.make_manager_for_console(workspace_id, org_id, actor_id, user_id)
    return await _console_entry_of(workspace_id, org_id)


@router.delete(
    "/api/v1/shared-workspaces/service/{workspace_id}",
    status_code=204,
    dependencies=[ServiceCaller],
)
async def delete_for_console(
    workspace_id: str,
    org_id: str = Query(..., min_length=1),
    actor_id: str = Query(..., min_length=1, max_length=255),
) -> None:
    """Delete one of the organization's shared workspaces with its computer,
    as the owner or admin ``actor_id`` asked in the console. 404 for another
    organization's workspace."""
    from src.server.database.workspace import WorkspaceBusyError

    try:
        await service.delete_for_console(workspace_id, org_id, actor_id)
    except WorkspaceBusyError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
