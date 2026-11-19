"""Creating a shared workspace, its members, listing and deleting it.

A shared workspace is made in one transaction: its holder, a cloud computer
of the holder's (never an own machine, never primary, never always-on), the
workspace on it, its ``shared_workspaces`` row and its member rows. The
machine is brought up by the first turn, as for any new workspace.

Who is in it: its listed people, and the people of the one team it may name
(``teams``), or everyone in its organization instead of a team. Someone
reaches it if any of those holds, so taking a person off the list keeps
them in through the team, and the other way round.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import HTTPException
from psycopg.rows import tuple_row

from ptc_agent.core.sandbox.providers import OWN_MACHINE_KINDS
from src.config import settings
from src.server.database import pool
from src.server.database import shared_workspaces as db
from src.server.database import user as users_db
from src.server.database import workspace as workspace_db
from src.server.database.computer import create_computer
from src.server.database.workspace_names import checked_workspace_name
from src.server.models.shared_workspace import (
    MAX_MEMBERS_AT_CREATE,
    Member,
    MembersView,
    SharedBlock,
    TeamPerson,
    TeamRef,
    TeamView,
    You,
)
from src.server.services import account, activity, platform_client, workspace_manager
from src.server.services.activity import Action
from src.server.services.computer_errors import ComputerRemovalRefused
from src.server.services.policy import SHARED_WORKSPACES_CREATE
from src.server.services.account import require_policy
from src.server.services.shared_workspaces import share_out, teams
from src.server.services.shared_workspaces.access import (
    SharedAccess,
    acts_as_manager,
    caller_seat,
    has_active_manager,
)
from src.server.services.shared_workspaces.feed import (
    publish_members_changed,
    publish_workspace_changed,
)
from src.server.database.shared_workspaces import is_holder, new_holder_id
from src.server.services.shared_workspaces.turns import (
    end_access,
    end_access_of_unreached,
    lost,
)

logger = logging.getLogger(__name__)


def not_available() -> HTTPException:
    """What a caller who cannot have shared workspaces sees: the answer to a
    route that does not exist, word for word."""
    return HTTPException(status_code=404, detail="Not Found")


def _refusal(status: int, kind: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(
        status_code=status, detail={"type": kind, "message": message, **extra}
    )


async def require_seat(caller: str):
    seat = await caller_seat(caller)
    if seat is None:
        raise not_available()
    return seat


async def _resolve_person(email: str, org_id: str) -> dict[str, Any]:
    """The account an address names, once it is an active member of
    ``org_id``. No roster crosses: the address is looked up here and the
    account is read the way any account is."""
    person = await users_db.user_by_email(email)
    active = await account.active_in(person["user_id"], org_id) if person is not None else False
    if active is None:
        raise account.AccountUnknown()
    if not active:
        raise _refusal(
            400, "member_not_found",
            "No account with that email address is in your organization.",
            email=email,
        )
    return person


def _deployment_kind() -> str:
    config = workspace_manager.WorkspaceManager.get_instance().config
    kind = config.sandbox.provider
    if kind in OWN_MACHINE_KINDS or kind not in config.sandbox.creatable_providers():
        # A shared workspace runs only on a computer the deployment provides.
        raise _refusal(
            409, "no_cloud_computer",
            "This deployment has no cloud computer a shared workspace can run on.",
        )
    return kind


async def create(
    caller: str,
    *,
    name: str,
    description: Optional[str],
    everyone: bool,
    member_emails: list[str],
    team_id: Optional[str] = None,
) -> tuple[dict[str, Any], SharedAccess]:
    """The workspace as created, and the caller's access to it as its first
    manager."""
    seat = await require_seat(caller)
    org_id = seat.org_id
    (await require_policy(caller)).require(SHARED_WORKSPACES_CREATE)
    if team_id is not None:
        await teams.require_nameable(seat.teams, team_id, org_id)
    name = checked_workspace_name(name)
    emails = list(dict.fromkeys(e.strip() for e in member_emails if e and e.strip()))
    if len(emails) > MAX_MEMBERS_AT_CREATE:
        raise _refusal(400, "too_many_members", "Add at most 50 people at once.")
    people = [await _resolve_person(e, org_id) for e in emails]
    members: list[tuple[str, db.Role]] = [(caller, "manager")] + [
        (p["user_id"], "member") for p in people if p["user_id"] != caller
    ]
    kind = _deployment_kind()
    manager = workspace_manager.WorkspaceManager.get_instance()
    cap = settings.get_infrastructure_config().shared_workspaces.max_per_organization
    holder_id = new_holder_id()

    async with pool.get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=tuple_row) as cur:
            await db.lock_org(cur, org_id)
            if await db.count_live_for_org(cur, org_id) >= cap:
                raise _refusal(
                    409, "shared_workspace_limit",
                    f"Your organization has the most shared workspaces it can have ({cap}).",
                    limit=cap,
                )
            await db.insert_holder(cur, holder_id, name)
        computer = await create_computer(
            holder_id,
            kind=kind,
            name=name,
            is_primary=False,
            status="stopped",
            resource_tier="standard",
            root_dir=manager.config.filesystem.working_directory,
            conn=conn,
        )
        workspace = await workspace_db.create_workspace_on_computer(
            holder_id,
            name,
            str(computer["computer_id"]),
            description=description,
            conn=conn,
        )
        if workspace is None:
            raise RuntimeError("shared workspace's computer was gone at creation")
        workspace_id = str(workspace["workspace_id"])
        async with conn.cursor(row_factory=tuple_row) as cur:
            row = await db.insert_shared(
                cur,
                workspace_id=workspace_id,
                holder_id=holder_id,
                org_id=org_id,
                everyone=everyone,
                created_by=caller,
                members=members,
                team_id=team_id,
            )

    await manager.resolve_binding(workspace_id, workspace=workspace)
    detail: dict[str, Any] = {"members": len(members), "everyone": everyone}
    if team_id is not None:
        detail["team_id"] = team_id
    activity.record(
        caller, Action.SHARED_WORKSPACE_CREATED, resource=("workspace", workspace_id),
        workspace_id=workspace_id, detail=detail,
    )
    for user_id, _ in members[1:]:
        activity.record(
            caller, Action.SHARED_WORKSPACE_MEMBER_ADDED,
            resource=("workspace", workspace_id), workspace_id=workspace_id,
            detail={"member_id": user_id, "role": "member"},
        )
    logger.info(
        "Created shared workspace %s (holder %s, org %s) for %s with %d members",
        workspace_id, holder_id, org_id, caller, len(members),
    )
    await publish_members_changed(org_id, workspace_id)
    return workspace, seat.access(row, caller, "manager")


async def shared_block(shared: SharedAccess) -> SharedBlock:
    """What a client is told of a shared workspace beside its row. Whether
    the caller manages is the decision every manager's act is gated on
    (``acts_as_manager``), so a member acting for absent managers is shown
    the controls those acts answer."""
    manager = await acts_as_manager(shared)
    rule = share_out.in_force(shared.row)
    return SharedBlock(
        role=shared.role,
        can_manage=manager,
        everyone=shared.row.everyone,
        created_by=shared.row.created_by,
        sharing=shared.row.share_out,
        can_share_out=share_out.may_share_out(rule, manager),
        can_change_links=share_out.may_change_links(rule, manager),
        listed=shared.listed,
        team=_team_ref(shared),
        team_admin=shared.team_admin,
    )


def _team_ref(shared: SharedAccess) -> Optional[TeamRef]:
    team_id = shared.row.team_id
    if team_id is None:
        return None
    return TeamRef(id=team_id, name=shared.team.name if shared.team is not None else None)


async def list_for(caller: str) -> list[tuple[dict[str, Any], SharedBlock, bool]]:
    """The caller's shared workspaces, each as its workspace row, the shared
    block and whether the caller has a thread there unread. Empty with the
    feature off or no organization."""
    seat = await caller_seat(caller)
    if seat is None:
        return []
    listed = await db.list_with_unread(caller, seat.org_id, seat.team_ids)
    workspaces = await workspace_db.get_workspaces([row.workspace_id for row, _, _ in listed])
    found = []
    for row, role, unread in listed:
        if row.workspace_id in workspaces:
            block = await shared_block(seat.access(row, caller, role))
            found.append((workspaces[row.workspace_id], block, unread))
    return found


async def members_view(shared: SharedAccess) -> MembersView:
    """The member list a client shows, each with whether their access through
    the organization has ended (suspended or removed)."""
    rows = await db.list_members(shared.workspace_id)
    active = await asyncio.gather(
        *(account.active_in(row["user_id"], shared.org_id) for row in rows)
    )
    block = await shared_block(shared)
    return MembersView(
        workspace_id=shared.workspace_id,
        everyone=block.everyone,
        sharing=block.sharing,
        members=[Member(**row, active=a) for row, a in zip(rows, active)],
        you=You.model_validate(block, from_attributes=True),
        team=block.team,
    )


async def team_view(shared: SharedAccess) -> TeamView:
    """The team the workspace names and its people, read from the auth
    service now, each marked when they are also on the workspace's own
    list. 404 when it names none; the 503 when the roster cannot be read,
    which a client shows without losing the rest of the people view."""
    team_id = shared.row.team_id
    if team_id is None:
        raise HTTPException(status_code=404, detail="No team")
    try:
        roster = await teams.read_roster(team_id, shared.org_id)
    except teams.TeamNotFound:
        raise _refusal(404, teams.TEAM_NOT_FOUND, "Team not found.") from None
    except teams.RosterUnavailable:
        raise platform_client.quota_service_unavailable() from None
    listed = {m["user_id"] for m in await db.list_members(shared.workspace_id)}
    avatars = await users_db.avatars_of([m.user_id for m in roster.members])
    return TeamView(
        id=roster.id,
        name=roster.name,
        people=[
            TeamPerson(
                user_id=m.user_id, name=m.name, email=m.email,
                avatar_url=avatars.get(m.user_id), role=m.role, active=m.active,
                listed=m.user_id in listed,
            )
            for m in roster.members
        ],
    )


async def add_member(shared: SharedAccess, email: str, role: db.Role) -> dict[str, Any]:
    workspace_id = shared.workspace_id
    person = await _resolve_person(email, shared.org_id)
    if not await db.add_member(workspace_id, person["user_id"], role, shared.caller):
        raise _refusal(409, "already_member", "That person is already a member.")
    activity.record(
        shared.caller, Action.SHARED_WORKSPACE_MEMBER_ADDED,
        resource=("workspace", workspace_id),
        workspace_id=workspace_id, detail={"member_id": person["user_id"], "role": role},
    )
    await publish_members_changed(shared.org_id, workspace_id)
    return person


async def _would_leave_no_manager(shared: SharedAccess, leaving: str) -> bool:
    """Whether taking ``leaving``'s manager role leaves no active manager
    while other people remain listed."""
    others = [m for m in await db.list_members(shared.workspace_id) if m["user_id"] != leaving]
    return bool(others) and not await has_active_manager(
        shared.workspace_id, shared.org_id, excluding=leaving
    )


async def set_role(shared: SharedAccess, user_id: str, role: db.Role) -> None:
    workspace_id = shared.workspace_id
    if role == "member" and await _would_leave_no_manager(shared, user_id):
        raise _refusal(
            409, "last_manager",
            "Make someone else a manager first: a shared workspace needs one.",
        )
    before = await db.set_role(workspace_id, user_id, role)
    if before is None:
        raise HTTPException(status_code=404, detail="Member not found")
    if before != role:
        activity.record(
            shared.caller, Action.SHARED_WORKSPACE_ROLE_CHANGED,
            resource=("workspace", workspace_id), workspace_id=workspace_id,
            detail={"member_id": user_id, "role": role, "was": before},
        )
        await publish_members_changed(shared.org_id, workspace_id)


async def remove_member(shared: SharedAccess, user_id: str) -> None:
    """Remove ``user_id`` (a manager's act, or anyone leaving). Unless they
    still reach the workspace another way, their access ends at their next
    request; a turn of theirs running in the workspace is stopped, as a
    suspension stops one, and previews on its computer are revoked so a
    capability they hold stops working."""
    workspace_id = shared.workspace_id
    role = await db.get_member_role(workspace_id, user_id)
    if role is None:
        raise HTTPException(status_code=404, detail="Member not found")
    if role == "manager" and await _would_leave_no_manager(shared, user_id):
        raise _refusal(
            409, "last_manager",
            "Make someone else a manager first: a shared workspace needs one.",
        )
    await db.remove_member(workspace_id, user_id)
    activity.record(
        shared.caller, Action.SHARED_WORKSPACE_MEMBER_REMOVED,
        resource=("workspace", workspace_id), workspace_id=workspace_id,
        detail={"member_id": user_id, "left": shared.caller == user_id},
    )
    await publish_members_changed(shared.org_id, workspace_id)
    # Someone also in its team, or in a workspace open to everyone, keeps it.
    if await lost(shared.row, user_id):
        await end_access(workspace_id, shared.holder_id, user_id)


async def set_audience(
    shared: SharedAccess, *, everyone: Optional[bool], team_id: Optional[str], names_team: bool
) -> None:
    """Open the workspace to everyone in the organization or close it, and
    name a team the caller is in or none (a manager's act). Whoever reached
    it only through what was taken away loses it as a removed member does."""
    workspace_id = shared.workspace_id
    if names_team and team_id is not None and team_id != shared.row.team_id:
        seat = await require_seat(shared.caller)
        await teams.require_nameable(seat.teams, team_id, shared.org_id)
    try:
        before, after = await db.set_audience(
            workspace_id, everyone=everyone, team_id=team_id, names_team=names_team
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Workspace not found") from None
    if before == after:
        return
    detail: dict[str, Any] = {}
    if before.everyone != after.everyone:
        detail["everyone"] = after.everyone
    if before.team_id != after.team_id:
        detail |= {"team_id": after.team_id, "was_team_id": before.team_id}
    activity.record(
        shared.caller, Action.SHARED_WORKSPACE_ROLE_CHANGED,
        resource=("workspace", workspace_id), workspace_id=workspace_id,
        detail=detail,
    )
    await publish_members_changed(shared.org_id, workspace_id)
    if not after.everyone and (before.everyone or before.team_id not in (None, after.team_id)):
        await _end_unreached(workspace_id)


async def _end_unreached(workspace_id: str) -> None:
    row = await db.get_shared_workspace(workspace_id)
    if row is not None:
        await end_access_of_unreached(row)


async def set_share_out(shared: SharedAccess, value: share_out.ShareOut) -> None:
    """Change who may share the workspace's work outside it (a manager's
    act). Every open session of its members reads the workspace again; a
    link already made opens to whom the new choice allows from its next
    visit on."""
    workspace_id = shared.workspace_id
    if shared.row.share_out == value:
        return
    before = await db.set_share_out(workspace_id, value)
    if before is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    activity.record(
        shared.caller, Action.SHARED_WORKSPACE_SHARING_CHANGED,
        resource=("workspace", workspace_id), workspace_id=workspace_id,
        detail={"sharing": value, "was": before},
    )
    await publish_workspace_changed(shared.org_id, workspace_id, "updated")


async def delete(workspace_id: str, caller: str, *, from_console: bool = False) -> None:
    """Delete the workspace with its computer and sandbox, through the
    computer's removal; the holder row stays as an inert owner of record.
    ``from_console`` marks an organization admin's act in the console."""
    workspace = await workspace_db.get_workspace(workspace_id)
    if workspace is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    shared = await db.get_shared_workspace(workspace_id)
    computer_id = workspace.get("computer_id")
    manager = workspace_manager.WorkspaceManager.get_instance()
    if computer_id:
        try:
            await manager.remove_computer_with_workspaces(
                str(computer_id), [workspace_id], delete_files=False
            )
        except ComputerRemovalRefused as e:
            # The computer is this workspace's alone, so what holds its
            # removal holds the delete, and is answered as a workspace's is.
            raise workspace_db.WorkspaceBusyError(
                await _delete_refusal(workspace_id, str(e))
            ) from None
    else:
        await manager.delete_workspace(workspace_id)
    activity.record(
        caller, Action.SHARED_WORKSPACE_DELETED, resource=("workspace", workspace_id),
        workspace_id=workspace_id,
        detail={"from_console": True} if from_console else None,
    )
    if shared is not None:
        await publish_workspace_changed(shared.org_id, workspace_id, "deleted")


async def _delete_refusal(workspace_id: str, removal: str) -> str:
    if await workspace_db.workspace_is_moving(workspace_id):
        return workspace_db.MOVING_DELETE_REFUSAL
    if await workspace_db.workspace_has_active_work(workspace_id):
        return workspace_db.ACTIVE_WORK_DELETE_REFUSAL
    return removal


# --- the console -------------------------------------------------------------


async def count_naming_team(org_id: str, team_id: str) -> int:
    return await db.count_naming_team(org_id, team_id)


async def make_manager_for_console(
    workspace_id: str, org_id: str, actor_id: str, user_id: str
) -> None:
    """Make ``user_id`` a manager of one of ``org_id``'s shared workspaces,
    as the owner or admin ``actor_id`` asked in the console. Someone not
    listed is added as a manager, once they are an active member of the
    organization; a listed member's standing is the workspace's business
    and is not re-read."""
    if await db.get_live_in_org(workspace_id, org_id) is None:
        raise HTTPException(status_code=404, detail="Not found")
    before = await db.get_member_role(workspace_id, user_id)
    if before == "manager":
        return
    if before is None:
        known = not is_holder(user_id) and bool(await users_db.names_of([user_id]))
        active = await account.active_in(user_id, org_id) if known else False
        if active is None:
            raise account.AccountUnknown()
        if not active:
            raise _refusal(
                400, "member_not_found",
                "That person is not an active member of the organization.",
            )
    before = await db.make_manager(workspace_id, user_id, actor_id)
    if before != "manager":
        await publish_members_changed(org_id, workspace_id)
    if before is None:
        activity.record(
            actor_id, Action.SHARED_WORKSPACE_MEMBER_ADDED,
            resource=("workspace", workspace_id), workspace_id=workspace_id,
            detail={"member_id": user_id, "role": "manager", "from_console": True},
        )
    elif before != "manager":
        activity.record(
            actor_id, Action.SHARED_WORKSPACE_ROLE_CHANGED,
            resource=("workspace", workspace_id), workspace_id=workspace_id,
            detail={
                "member_id": user_id, "role": "manager", "was": before,
                "from_console": True,
            },
        )


async def delete_for_console(workspace_id: str, org_id: str, actor_id: str) -> None:
    """Delete one of ``org_id``'s shared workspaces, as the owner or admin
    ``actor_id`` asked in the console. 404 for another organization's."""
    if await db.get_live_in_org(workspace_id, org_id) is None:
        raise HTTPException(status_code=404, detail="Not found")
    await delete(workspace_id, actor_id, from_console=True)
