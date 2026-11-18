"""Who may use a workspace or a thread, and as whose rows.

A personal workspace answers exactly as the owner check always has: its owner
and nobody else, the rows read as the caller's. A shared workspace (its row
owned by a holder) answers for an active member of its organization who has
the ``shared_workspaces`` feature and reaches it (``db.reaches``: listed on
it, in the team it names, or anyone in the organization when it is open to
everyone); its rows are the holder's. Its managers are its listed managers
and the admins of its team. Checked on every request against the caller's
current account, whose teams ride the same cached read as its organization,
so a removal from the workspace or the team, or a suspension, takes effect
at the next request on every worker.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable
from dataclasses import dataclass, field, replace
from typing import Any, Literal, Optional

from fastapi import HTTPException

from src.server.database import conversation as conversation_db
from src.server.database.conversation import thread_reach
from src.server.database import shared_workspaces as db
from src.server.database import workspace as workspace_db
from src.server.database.shared_workspaces import Role as MemberRole
from src.server.database.shared_workspaces import SharedRow
from src.server.models.shared_workspace import Standing
from src.server.database.shared_workspaces import is_holder
from src.server.services import account, features, membership
from src.server.services.account_contract import Team

logger = logging.getLogger(__name__)

Role = Literal["owner"] | MemberRole

FEATURE = "shared_workspaces"
#: The ``type`` of a refusal to a member of what only a manager may do.
MANAGER_ONLY = "shared_workspace_manager_only"


@dataclass(frozen=True)
class SharedAccess:
    """A caller's access to a shared workspace they reach."""

    row: SharedRow
    caller: str
    #: The caller's listed role; ``member`` for someone not listed, who
    #: reaches it through its team or everyone.
    role: MemberRole
    #: A member who may manage because no listed manager is active.
    acting_manager: bool = False
    #: The caller is on the workspace's own list (``role`` is theirs there).
    listed: bool = True
    #: The caller's place in the team the workspace names; None when it
    #: names none or they are not in it. Its admins manage the workspace
    #: without being listed as managers.
    team: Optional[Team] = None

    @property
    def team_admin(self) -> bool:
        return self.team is not None and self.team.role == "admin"

    @property
    def workspace_id(self) -> str:
        return self.row.workspace_id

    @property
    def holder_id(self) -> str:
        return self.row.holder_id

    @property
    def org_id(self) -> str:
        return self.row.org_id

    @property
    def may_manage(self) -> bool:
        return self.role == "manager" or self.team_admin or self.acting_manager


@dataclass(frozen=True)
class Access:
    """What a caller may do in a workspace, and whose rows it is."""

    #: The workspace row's owner: the caller for a personal workspace, the
    #: holder for a shared one. Reads of the machine and the workspace's own
    #: resources take this id; reads of the person take the caller's.
    owner_id: str
    shared: Optional[SharedAccess] = None

    @property
    def is_shared(self) -> bool:
        return self.shared is not None

    @property
    def org_id(self) -> Optional[str]:
        return self.shared.org_id if self.shared is not None else None

    @property
    def may_manage(self) -> bool:
        return self.shared is None or self.shared.may_manage


class NoAccess(HTTPException):
    """A refusal of the workspace itself: no such row, not the caller's, or
    a manager's act asked of a member. A route answers it as raised; any
    other path asks :func:`verdict`."""


def _forbidden() -> NoAccess:
    # The owner check's answer for a row that is not the caller's.
    return NoAccess(status_code=403, detail="Forbidden")


def _not_found(noun: str) -> NoAccess:
    return NoAccess(status_code=404, detail=f"{noun} not found")


def manager_only() -> NoAccess:
    return NoAccess(
        status_code=403,
        detail={
            "type": MANAGER_ONLY,
            "message": "Only a manager of this shared workspace can do that.",
        },
    )


Verdict = Literal["member", "not", "unknown"]


async def verdict(check: Awaitable[Access]) -> Verdict:
    """An access check (:func:`shared_access`, :func:`workspace_access`,
    :func:`thread_access`) for a path that acts for a member of a shared
    workspace rather than answering them: ``member`` while they reach it,
    ``not`` once they do not (or it is not shared), and ``unknown`` while
    their account cannot be read, which each caller answers its own way."""
    try:
        access = await check
    except (NoAccess, account.AccountSuspended):
        return "not"
    except account.AccountUnknown:
        return "unknown"
    return "member" if access.is_shared else "not"


async def member_or_404(check: Awaitable[Access], detail: str = "Not found") -> Access:
    """:func:`verdict` for a route that opens something of a shared
    workspace's to a member: the access, or the uniform 404 that says
    nothing of why. An unknown account is the 503 as raised."""
    try:
        access = await check
    except (NoAccess, account.AccountSuspended):
        access = None
    if access is None or not access.is_shared:
        raise HTTPException(status_code=404, detail=detail)
    return access


@dataclass(frozen=True)
class Seat:
    """Someone who may use their organization's shared workspaces now, with
    the teams the auth service last said they are in there."""

    org_id: str
    teams: tuple[Team, ...] = ()

    @property
    def team_ids(self) -> frozenset[str]:
        return frozenset(t.id for t in self.teams)

    def reaches(self, row: SharedRow, role: Optional[MemberRole]) -> bool:
        return row.org_id == self.org_id and db.reaches(row, role, self.team_ids)

    def team_of(self, row: SharedRow) -> Optional[Team]:
        """Their place in the team ``row`` names, None when not in it."""
        return next((t for t in self.teams if t.id == row.team_id), None)

    def access(self, row: SharedRow, caller: str, role: Optional[MemberRole]) -> SharedAccess:
        """The access of someone this seat says reaches ``row``."""
        return SharedAccess(
            row, caller, role or "member", listed=role is not None, team=self.team_of(row)
        )


async def caller_seat(caller: str) -> Optional[Seat]:
    """``caller``'s :class:`Seat`: an active member with the feature on.
    None otherwise, reading nothing more with the ``teams`` door shut. A
    suspended caller is refused as every route refuses one; an unknown
    account is the 503, so a path acting without a request refuses too."""
    if not membership.teams_door_open():
        return None
    (await account.read_standing(caller)).refuse_if_suspended()
    member = await features.org_member(caller, FEATURE)
    if member is None:
        return None
    return Seat(member.org.id, member.account.teams)


async def shared_access(
    workspace_id: str, holder_id: str, caller: str, *, manage: bool = False,
    noun: str = "Workspace",
) -> Access:
    """The caller's access to a shared workspace they reach. One they do not
    reach answers as no ``noun`` at all, so its id says nothing of whether
    it exists."""
    row = await db.get_shared_workspace(workspace_id)
    if row is None or row.holder_id != holder_id:
        raise _not_found(noun)
    seat = await caller_seat(caller)
    if seat is None or seat.org_id != row.org_id:
        raise _not_found(noun)
    role = await db.get_member_role(workspace_id, caller)
    if not seat.reaches(row, role):
        raise _not_found(noun)
    shared = seat.access(row, caller, role)
    return Access(holder_id, await _as_manager(shared) if manage else shared)


async def _as_manager(shared: SharedAccess) -> SharedAccess:
    """``shared`` acting as a manager, or the refusal where only one may act."""
    if shared.may_manage:
        return shared
    if not await acts_as_manager(shared):
        raise manager_only()
    return replace(shared, acting_manager=True)


async def acts_as_manager(shared: SharedAccess) -> bool:
    """Whether the caller may do what a manager does: a manager, or any
    member while no listed manager is active, so a workspace is never left
    with no one to run it."""
    if shared.may_manage:
        return True
    return not await has_active_manager(shared.workspace_id, shared.org_id)


async def has_active_manager(
    workspace_id: str, org_id: str, *, excluding: Optional[str] = None
) -> bool:
    """Whether a listed manager other than ``excluding`` is an active member
    of the organization. Unknown counts as active, so an outage never hands
    a member a manager's rights."""
    for manager in await db.manager_ids(workspace_id):
        if manager != excluding and await account.active_in(manager, org_id) is not False:
            return True
    return False


async def workspace_access(
    workspace: Optional[dict[str, Any]], caller: str, *, manage: bool = False
) -> Access:
    """The caller's access to ``workspace`` (a row as ``get_workspace``
    reads it): 404 for no row and for a shared workspace the caller does not
    reach, 403 for someone else's own workspace, and for ``manage`` a 403 to
    a member where only a manager may act."""
    if not workspace:
        raise _not_found("Workspace")
    owner = workspace.get("user_id")
    if owner == caller:
        return Access(caller)
    if not is_holder(owner):
        raise _forbidden()
    return await shared_access(
        str(workspace["workspace_id"]), owner, caller, manage=manage
    )


async def thread_access(
    thread_id: str,
    caller: str,
    *,
    manage: bool = False,
    meta: Optional[dict[str, Any]] = None,
) -> Access:
    """:func:`workspace_access` for the workspace a thread is in, and in a
    shared workspace the thread's own rule: shared with the workspace, or
    started by the caller (``thread_reach``). A manager reaches no further
    into a thread than anyone. A shared workspace's thread the caller does
    not reach answers as no thread at all, so its id says nothing. ``meta``
    is a ``get_thread_auth_meta`` read the caller already holds."""
    if meta is None:
        meta = await conversation_db.get_thread_auth_meta(thread_id)
    if meta is None:
        raise _not_found("Thread")
    owner = meta["user_id"]
    if owner == caller:
        return Access(caller)
    if not is_holder(owner):
        raise _forbidden()
    try:
        access = await shared_access(str(meta["workspace_id"]), owner, caller)
    except NoAccess:
        raise _not_found("Thread") from None
    if await thread_reach.hidden([thread_id], caller):
        raise _not_found("Thread")
    # Asked only once the thread is known to be theirs to see, so a refusal
    # to a member says nothing of a thread they do not reach.
    return Access(owner, await _as_manager(access.shared)) if manage else access


async def shared_org_of(workspace_id: Optional[str]) -> Optional[str]:
    """The organization of a shared workspace, or None for any other. Reads
    nothing while the ``teams`` door is shut, where no shared turn is
    admitted."""
    if not workspace_id or not membership.teams_door_open():
        return None
    shared = await db.get_shared_workspace(str(workspace_id))
    return shared.org_id if shared else None


#: How often a shared thread's stream asks again whether its viewer may read
#: the workspace, as the stream mux's rescan does.
STREAM_RECHECK_S = 30.0
#: How often it asks again whether the thread's own rule still lets them in:
#: its starter may make it private while a colleague watches.
THREAD_RECHECK_S = 2.0


@dataclass
class ThreadWatch:
    """The access check a long-lived stream on a shared thread repeats between
    frames, read from Postgres as every worker sees it: the thread's rule
    every ``THREAD_RECHECK_S``, so a stream ends within seconds of the thread
    going private, and the whole check every ``STREAM_RECHECK_S`` or when
    forced. An outage keeps the stream, as it stops nothing elsewhere; the
    next request asks again."""

    thread_id: str
    caller: str
    _checked: float = field(default_factory=time.monotonic)
    _rule_checked: float = field(default_factory=time.monotonic)

    @classmethod
    def of(cls, thread_id: str, caller: str, access: Access) -> Optional[ThreadWatch]:
        """None for a thread only its owner reads, which costs a stream nothing."""
        return cls(thread_id, caller) if access.is_shared else None

    async def ok(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        try:
            if force or now - self._checked >= STREAM_RECHECK_S:
                self._checked = self._rule_checked = now
                return await verdict(thread_access(self.thread_id, self.caller)) != "not"
            if now - self._rule_checked >= THREAD_RECHECK_S:
                self._rule_checked = now
                return not await thread_reach.hidden([self.thread_id], self.caller)
        except Exception:
            logger.warning(
                "[shared] stream access check failed thread=%s", self.thread_id, exc_info=True
            )
        return True


async def computer_access(
    computer: Optional[dict[str, Any]], caller: str, *, manage: bool = False
) -> Access:
    """:func:`workspace_access` for a computer: its owner, or for a shared
    workspace's computer, a member of that workspace (a manager for
    ``manage``). The owner check's 404 and 403 otherwise."""
    if not computer:
        raise _not_found("Computer")
    owner = computer.get("user_id")
    if owner == caller:
        return Access(caller)
    if not is_holder(owner):
        raise _forbidden()
    shared = await db.shared_of_holder(owner)
    if shared is None:
        raise _not_found("Computer")
    return await shared_access(
        shared.workspace_id, owner, caller, manage=manage, noun="Computer"
    )


async def standing(workspace_id: str, caller: str) -> Standing:
    """Where a shared workspace stands for ``caller``, for a page that lost
    it while away: ``active`` while they reach it, ``deleted`` when it was
    deleted while they still did (they still reach its row, and are in its
    organization), and ``gone`` for anything else. Someone removed, or never
    in it, learns nothing about whether it still exists: every such case is
    ``gone``."""
    shared = await db.get_shared_workspace(workspace_id)
    if shared is None:
        return "gone"
    seat = await caller_seat(caller)
    if seat is None or not seat.reaches(
        shared, await db.get_member_role(shared.workspace_id, caller)
    ):
        return "gone"
    status = await workspace_db.workspace_status(shared.workspace_id)
    if status is None:
        return "gone"
    return "deleted" if status == "deleted" else "active"
