"""Shared workspaces on their members' live feeds.

What happens in a shared workspace (a turn starts or ends, a thread is
renamed, archived or deleted, the members or the workspace change) is
published once, on its organization's channel
(``thread_lifecycle_feed.shared_events_channel``). Every feed connection of a
member of that organization with the feature listens there beside the user's
own channel, and passes an event on only when its account reaches the
workspace at that moment: the reach is read again from Postgres for each event
and at each reconcile, so a removal, a suspension or a workspace closed to
everyone stops the next event on every worker, with nothing held in process
memory between connections. An event about a thread its starter keeps
private names them when published, and goes to them alone. Redis carries the
events; the snapshot each connection reads every 30 seconds is the truth a
missed one falls back to.

A connection whose account stops reaching a workspace says so once
(``shared_access_ended``), naming only that workspace, which the account
could see until then; a deleted workspace is told as deleted to those who
reached it, whether its event or a reach read finds it gone first.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from src.server.database import shared_workspaces as db
from src.server.services import account
from src.server.services.shared_workspaces import access
from src.server.services.shared_workspaces.viewer import as_seen_by
from src.server.services.thread_lifecycle_feed import (
    EVENT_VERSION,
    PRIVATE_TO_KEY,
    SENDER_KEY,
    publish_user_event,
    shared_events_channel,
)

logger = logging.getLogger(__name__)

#: The members of a workspace changed: someone added or removed, a role, its
#: team, or the workspace opened to everyone in the organization or closed.
MEMBERS_EVENT = "shared_members"
#: The workspace itself changed: ``change`` is ``updated`` or ``deleted``.
WORKSPACE_EVENT = "shared_workspace"
#: Said to one connection: its account no longer reaches the workspace.
ACCESS_ENDED_EVENT = "shared_access_ended"
#: A file's comments changed (``services.artifacts.events``), naming the
#: thread whose queue the change touched.
COMMENT_EVENT = "artifact_comment"
#: Said on one person's own channel when the auth service tells of a change
#: to their organization or teams: each of their feed connections reads its
#: reach again at once, says what it lost, and passes the event on so the
#: client reads its shared workspaces again (what it gained, a role in a
#: team that now manages one).
REACH_EVENT = "shared_reach"


@dataclass(frozen=True)
class Reach:
    org_id: str
    workspace_ids: frozenset[str]


async def reach_of(user_id: str, *, unknown_raises: bool = False) -> Optional[Reach]:
    """The shared workspaces ``user_id`` reaches now, or None when they reach
    none because none can be theirs (the ``teams`` door shut, no organization,
    the feature off, suspended), which reads nothing more than the access
    check would.

    An account that cannot be read reaches none as well, so an outage shows
    a removed member nothing. ``unknown_raises`` is for a caller that holds
    an earlier answer and must not take the outage for a loss."""
    try:
        seat = await access.caller_seat(user_id)
    except account.AccountSuspended:
        return None
    except account.AccountUnknown:
        if unknown_raises:
            raise
        return None
    if seat is None:
        return None
    rows = await db.list_for_member(user_id, seat.org_id, seat.team_ids)
    return Reach(seat.org_id, frozenset(shared.workspace_id for shared, _ in rows))


async def _deleted(workspace_id: str, user_id: str) -> bool:
    """Whether a workspace this account lost was deleted while they still
    reached it, by the rule the standing route answers with: someone taken
    out of it first learns only that they lost it, never whether it still
    exists. Unknown counts as not deleted, which says the plainer thing."""
    try:
        return await access.standing(workspace_id, user_id) == "deleted"
    except Exception:
        logger.warning("[shared-feed] workspace read failed ws=%s", workspace_id, exc_info=True)
        return False


def _event(type_: str, workspace_id: str, **extra: Any) -> dict[str, Any]:
    return {"v": EVENT_VERSION, "type": type_, "workspace_id": workspace_id, **extra}


async def publish_members_changed(org_id: str, workspace_id: str) -> None:
    """Sent after the change has committed: each member's connection reads
    its reach again, so the people added see it and the people removed stop
    seeing it."""
    await publish_user_event(
        None, _event(MEMBERS_EVENT, workspace_id), shared_org=org_id
    )


async def publish_reach_changed(user_id: str) -> None:
    await publish_user_event(user_id, {"v": EVENT_VERSION, "type": REACH_EVENT})


async def publish_workspace_changed(
    org_id: str, workspace_id: str, change: str
) -> None:
    await publish_user_event(
        None, _event(WORKSPACE_EVENT, workspace_id, change=change), shared_org=org_id
    )


class SharedFeed:
    """One feed connection's view of its account's shared workspaces."""

    def __init__(self, user_id: str, reach: Reach) -> None:
        self.user_id = user_id
        self.reach = reach
        self.channel = shared_events_channel(reach.org_id)
        #: The account's reach in another organization, once it has moved
        #: there: this feed listens to the one it was opened in, so the
        #: connection replaces it (``moved_on``).
        self._moved: Optional[Reach] = None

    def moved_on(self) -> Optional["SharedFeed"]:
        """The feed to listen with from now on, when the account has moved
        to another organization since this one opened; None otherwise."""
        return SharedFeed(self.user_id, self._moved) if self._moved is not None else None

    @classmethod
    async def open(cls, user_id: str) -> Optional["SharedFeed"]:
        """None for an account that can reach no shared workspace now, whose
        connection then runs exactly as it always has. Once open, a later loss
        of the organization or the feature empties the reach rather than
        ending the connection."""
        try:
            reach = await reach_of(user_id)
        except Exception:
            logger.warning("[shared-feed] reach read failed user=%s", user_id, exc_info=True)
            return None
        return cls(user_id, reach) if reach is not None else None

    @property
    def workspace_ids(self) -> list[str]:
        return sorted(self.reach.workspace_ids)

    async def _refresh(self) -> Optional[frozenset[str]]:
        """The reach read again, or None when the read failed (nothing is
        passed on until one succeeds). An account that cannot be read just
        now is such a failure: taken for a loss, an outage would tell every
        open page its access had ended. Another organization than the one
        this connection listens to reaches nothing on it."""
        try:
            reach = await reach_of(self.user_id, unknown_raises=True)
        except Exception:
            logger.warning("[shared-feed] reach read failed user=%s", self.user_id, exc_info=True)
            return None
        if reach is None:
            return frozenset()
        if reach.org_id != self.reach.org_id:
            self._moved = reach
            return frozenset()
        return reach.workspace_ids

    async def _settle(
        self, now: frozenset[str], *, told: frozenset[str] = frozenset()
    ) -> list[dict]:
        """Take ``now`` as the reach; the workspaces it lost, as frames. A
        lost workspace that was deleted is told as deleted: the reach can be
        read between the delete and its event, which then finds nothing left
        to tell."""
        lost = self.reach.workspace_ids - now - told
        self.reach = Reach(self.reach.org_id, now)
        frames = []
        for w in sorted(lost):
            if await _deleted(w, self.user_id):
                frames.append(_event(WORKSPACE_EVENT, w, change="deleted"))
            else:
                frames.append(_event(ACCESS_ENDED_EVENT, w))
        return frames

    async def reconcile(self) -> list[dict]:
        now = await self._refresh()
        return [] if now is None else await self._settle(now)

    def _thread_view(self, event: dict) -> Optional[dict]:
        """The event as this account may have it, or None: a private thread's
        events go to the starter it names, and a file's comment event names
        no thread the account may not see."""
        if event.get(PRIVATE_TO_KEY) in (None, self.user_id):
            return event
        return {**event, "thread_id": None} if event.get("type") == COMMENT_EVENT else None

    def _for_viewer(self, event: dict) -> dict:
        out = dict(event)
        out.pop(PRIVATE_TO_KEY, None)
        sender = out.pop(SENDER_KEY, None)
        status, reason = as_seen_by(
            out.get("status"), out.get("interrupt_reason"), sender, self.user_id
        )
        if status != out.get("status"):
            out.update(status=status, interrupt_reason=reason)
        return out

    async def on_event(self, event: dict) -> list[dict]:
        """The frames an event on the organization's channel gives this
        connection: none unless its account reaches the workspace now."""
        workspace_id = event.get("workspace_id")
        if not isinstance(workspace_id, str):
            return []
        before = self.reach.workspace_ids
        now = await self._refresh()
        if now is None:
            return []
        if event.get("type") == WORKSPACE_EVENT and event.get("change") == "deleted":
            # Deleted: no one reaches it now, so those who did until then
            # are told it was deleted rather than that they lost it. The
            # reach held here can be older than a removal, so the account's
            # standing decides, not the reach.
            told = frozenset({workspace_id}) & before
            if told and not await _deleted(workspace_id, self.user_id):
                told = frozenset()
            frames = [self._for_viewer(event)] if told else []
            return frames + await self._settle(now, told=told)
        frames = await self._settle(now)
        shown = self._thread_view(event) if workspace_id in now else None
        if shown is not None:
            frames.append(self._for_viewer(shown))
        return frames
