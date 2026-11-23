"""Shared workspaces as clients and the console see them."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, StrictBool, model_validator

from src.server.database.shared_workspaces import Role, ShareOut
from src.server.models.workspace import WorkspaceResponse

MAX_MEMBERS_AT_CREATE = 50
#: Longer than any id the auth service makes; a bound, not a format.
TEAM_ID_MAX = 100

Standing = Literal["active", "deleted", "gone"]


class TeamRef(BaseModel):
    """The team a shared workspace names."""

    id: str
    #: Its name, when the caller is in it; the roster read gives it to
    #: anyone else who reaches the workspace.
    name: Optional[str] = None


class SharedBlock(BaseModel):
    #: The caller's role: ``manager`` or ``member``.
    role: str
    can_manage: bool
    #: Everyone in the organization may use it, beside the listed people.
    everyone: bool
    created_by: Optional[str] = None
    #: Who may share its work outside it: ``managers``, ``members`` or
    #: ``none``. Kept, and not applied, while it is open to everyone.
    sharing: ShareOut = "managers"
    #: The caller may share its work with the organization and publish it
    #: to the library.
    can_share_out: bool = True
    #: The caller may turn a thread's or file's link on or off.
    can_change_links: bool = True
    #: The caller is on its own list of people; false for someone who
    #: reaches it only through its team or everyone, who cannot leave it.
    listed: bool = True
    #: The team it names, beside its listed people; None for none.
    team: Optional[TeamRef] = None
    #: The caller is an admin of that team, so manages it without being
    #: listed as a manager.
    team_admin: bool = False


class SharedWorkspace(BaseModel):
    workspace: WorkspaceResponse
    shared: SharedBlock
    #: In the caller's list: a thread there whose latest turn ended after
    #: the caller last read it. False on the answer to a create.
    has_unread: bool = False


class SharedWorkspaceList(BaseModel):
    workspaces: list[SharedWorkspace]
    #: Whether the caller may create one (the ``shared_workspaces.create`` rule).
    can_create: bool


class SharedWorkspaceCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    everyone: bool = False
    #: Email addresses of active members of the caller's organization.
    members: list[str] = Field(default_factory=list, max_length=MAX_MEMBERS_AT_CREATE)
    #: A team the caller is in, whose people reach it beside the listed
    #: ones. Not with ``everyone``.
    team_id: Optional[str] = Field(None, min_length=1, max_length=TEAM_ID_MAX)

    @model_validator(mode="after")
    def _team_or_everyone(self) -> "SharedWorkspaceCreate":
        if self.everyone and self.team_id is not None:
            raise ValueError("a workspace open to everyone names no team")
        return self


class SharedStanding(BaseModel):
    #: ``active`` while the caller reaches it; ``deleted`` when it was deleted
    #: while they did; ``gone`` otherwise, which says nothing more.
    state: Standing


class Member(BaseModel):
    user_id: str
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    role: Role
    #: False when the person's access through the organization has ended
    #: (suspended or removed); None while their account cannot be read.
    active: Optional[bool] = None
    added_at: datetime


class You(BaseModel):
    role: str
    can_manage: bool
    can_share_out: bool
    can_change_links: bool
    listed: bool = True


class MemberContextView(BaseModel):
    """Whether a member's own answer preferences and personal memory reach
    the agent on their messages here: only while both are true."""

    #: The caller's organization grants it, and this deployment lets it
    #: (the ``shared_workspaces_member_context`` feature).
    organization: bool
    #: The workspace's managers have not turned it off here.
    workspace: bool
    #: This deployment offers it, so a manager may keep it out whatever
    #: their own organization's grant: a member may hold one of their own.
    offered: bool


class MembersView(BaseModel):
    workspace_id: str
    everyone: bool
    #: Who may share its work outside it (``SharedBlock.sharing``).
    sharing: ShareOut
    members: list[Member]
    you: You
    #: The team it names (``SharedBlock.team``); its people come from
    #: ``GET .../team``.
    team: Optional[TeamRef] = None
    member_context: MemberContextView


class TeamPerson(BaseModel):
    user_id: str
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    #: Their role in the team: its admins manage the workspace.
    role: Literal["admin", "member"]
    #: An active member of the organization now.
    active: bool
    #: Also on the workspace's own list, so leaving the team keeps them in.
    listed: bool


class TeamView(BaseModel):
    """The team a shared workspace names, as the auth service says now."""

    id: str
    name: str
    people: list[TeamPerson]


class MemberAdd(BaseModel):
    email: str = Field(..., min_length=3, max_length=320)
    role: Role = "member"


class MemberUpdate(BaseModel):
    role: Role


class SharingUpdate(BaseModel):
    everyone: Optional[bool] = None
    #: Who may share the workspace's work outside it.
    sharing: Optional[ShareOut] = None
    #: The team it names: one the caller is in, or null for none. Left out,
    #: unchanged. Naming one closes it to everyone; opening it to everyone
    #: clears it.
    team_id: Optional[str] = Field(None, min_length=1, max_length=TEAM_ID_MAX)
    #: False keeps members' own context out of this workspace; true follows
    #: the organization's grant.
    member_context: Optional[StrictBool] = None

    @property
    def changes_team(self) -> bool:
        return "team_id" in self.model_fields_set

    @property
    def changes_settings(self) -> bool:
        return self.sharing is not None or self.member_context is not None

    @model_validator(mode="after")
    def _something(self) -> "SharingUpdate":
        if self.everyone is None and not (self.changes_settings or self.changes_team):
            raise ValueError("send everyone, sharing, team_id or member_context")
        if self.everyone and self.team_id is not None:
            raise ValueError("a workspace open to everyone names no team")
        return self


class ConsoleMember(BaseModel):
    user_id: str
    name: Optional[str] = None
    email: Optional[str] = None
    role: Role
    added_at: datetime


class ConsoleSharedWorkspace(BaseModel):
    workspace_id: str
    name: str
    description: Optional[str] = None
    created_by: str
    created_by_name: Optional[str] = None
    created_at: datetime
    #: Open to everyone in the organization, beside the listed people.
    everyone: bool
    #: The team it names, beside the listed people, as the auth service ids
    #: it; None for none.
    team_id: Optional[str] = None
    #: Who its managers let share its work outside it: ``managers``,
    #: ``members`` or ``none``; applied only while ``everyone`` is false.
    sharing: ShareOut
    members: list[ConsoleMember]
    #: The user ids of its managers, in the order they were added.
    managers: list[str]
    #: The latest of the workspace's, its threads' and its computer's activity.
    last_activity_at: Optional[datetime] = None
    #: Its computer's state (``running``, ``stopped``, ...); None with no computer.
    computer_status: Optional[str] = None
    #: A run is under way in one of its threads now, a subagent's included.
    #: A turn paused on a question or approval runs nothing, so it is false.
    turn_running: bool = False


class TeamUsage(BaseModel):
    #: The organization's live shared workspaces that name the team.
    workspaces: int


class ConsoleSharedWorkspacePage(BaseModel):
    workspaces: list[ConsoleSharedWorkspace]
    next_cursor: Optional[str]
