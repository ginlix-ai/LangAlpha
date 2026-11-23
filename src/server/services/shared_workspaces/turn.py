"""Who a turn is for and whose workspace it runs in, decided once a turn."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from fastapi import HTTPException

from ptc_agent.agent.transcript.sender import MemberSender
from src.server.contracts.turn_kinds import posted_by_server
from src.server.database import shared_workspaces as db
from src.server.database.conversation.senders import stamp_thread_creator
from src.server.database.runs.hook_jobs import SHARED_ORG_META
from src.server.database.shared_workspaces import SharedRow, is_holder
from src.server.database.user import get_user_profile_for_prompt
from src.server.services.shared_workspaces.holder import shared_profile

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest


def choose_recorded_as(
    sender: Optional[str], owner: Optional[str], org_id: Optional[str]
) -> Optional[str]:
    """The account a turn's usage is recorded against and its admission and
    live gate are asked for: the one place it is chosen, for a turn in any
    workspace. Every turn is recorded as its sender. Recording a workspace's
    turns as another account, one its organization or team names, is the auth
    service's answer to give, and this is where it would be read."""
    return sender


def carried_recorded_as(row: Mapping[str, Any]) -> Optional[str]:
    """The account recorded for a run started later on a turn's behalf, read
    off the row or payload that carries it: an automation, or a run's own row
    for usage it settles afterwards. It is the key that names the user,
    because the two have always been one account; a run recorded as anyone
    else would carry its own key, written where the user is."""
    value = row.get("user_id")
    return str(value) if value else None


@dataclass(frozen=True)
class TurnIdentity:
    """The turn's sender, the workspace's owner, and whether the build may
    use the sender's own data: in their own workspace all of it, in a shared
    one none, since other members read what the agent says and steer it."""

    sender: Optional[str]
    #: Whose machine, workspace, memory and connectors the turn reads
    #: (``TurnRoute.owner_id``).
    owner: Optional[str]
    #: The account the turn's usage rows, admission and live gate name
    #: (:func:`choose_recorded_as`). Everything about the person acting
    #: (credentials, model settings, names, read state) reads ``sender``.
    recorded_as: Optional[str]
    #: A shared workspace's row as of the turn, read once by :meth:`of` for
    #: everything the turn asks of it; None in anyone's own workspace and
    #: once the row is gone.
    shared_row: Optional[SharedRow] = field(default=None, repr=False)

    @classmethod
    async def of(
        cls, sender: Optional[str], owner: Optional[str], *, carried: Optional[str] = None
    ) -> TurnIdentity:
        """Reads nothing for a turn in anyone's own workspace. ``carried`` is
        the account of the turn a later run was started for, which it keeps."""
        row = await db.shared_of_holder(owner) if is_holder(owner) else None
        recorded_as = carried or choose_recorded_as(sender, owner, row.org_id if row else None)
        return cls(sender, owner, recorded_as, row)

    @property
    def shared(self) -> bool:
        return is_holder(self.owner)

    @property
    def org_id(self) -> Optional[str]:
        """A shared workspace's organization, which hears of its runs."""
        return self.shared_row.org_id if self.shared_row else None

    async def profile(self) -> Optional[dict[str, Any]]:
        """The sender's profile as the turn's prompt and context state it."""
        return self.stated(await self.own_profile())

    async def own_profile(self) -> Optional[dict[str, Any]]:
        """The sender's whole profile, read once a turn; :meth:`stated` is
        what of it the turn's prompt may carry."""
        return await get_user_profile_for_prompt(self.sender) if self.sender else None

    def stated(self, profile: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        return shared_profile(profile) if self.shared else profile

    async def begin(self, thread_id: str) -> None:
        if self.shared:
            await stamp_thread_creator(thread_id, self.sender)

    def stamp(self, caller: Optional[dict[str, Any]]) -> dict[str, Any]:
        """The caller's START stamp, with a shared workspace's organization
        in place of any the caller named: the key names who hears of the
        run, so its start and its settle reach the workspace's members."""
        stamp = {k: v for k, v in (caller or {}).items() if k != SHARED_ORG_META}
        if self.org_id:
            stamp[SHARED_ORG_META] = self.org_id
        return stamp

    def refuse_other_roles(self, request: ChatRequest) -> None:
        """Refuse a shared turn's message in any role but ``user``, the one
        that records its writer (``MemberSender.stamp``): every member reads
        the thread, and one in another role (``human`` among them) would
        reach the others' calls untagged, as another member, the agent or
        the harness."""
        if self.shared and any(m.role != "user" for m in request.messages or ()):
            raise HTTPException(
                status_code=422,
                detail="Messages in a shared workspace are sent with the user role.",
            )

    async def message_sender(self, request: ChatRequest) -> Optional[MemberSender]:
        """The member who wrote this turn's message, recorded on it and on a
        steer it becomes, as of sending; None for a turn no member wrote
        (:func:`member_wrote`), and for every turn in anyone's own
        workspace, whose messages record nothing."""
        if not self.shared or not self.sender or not member_wrote(request):
            return None
        return await member_sender(self.sender)

    def announced(self, profile: Optional[dict[str, Any]]) -> dict[str, Any]:
        """The sender a shared run's start frame names, for a member
        attaching live to draw it as theirs."""
        if not self.shared:
            return {}
        return {"sender": {"user_id": self.sender, "name": (profile or {}).get("name")}}


async def member_sender(user_id: str) -> MemberSender:
    """``user_id`` as the model reads a member, by their name as of now: on a
    message of theirs, and on a steer they send a task."""
    profile = await get_user_profile_for_prompt(user_id)
    return _member_sender(user_id, (profile or {}).get("name"))


def member_wrote(request: ChatRequest) -> bool:
    """Whether a member wrote this shared turn's message, which then names
    them (:meth:`TurnIdentity.message_sender`): any turn but one the server
    posts (``posted_by_server``: a report-back or a wake). A member's
    automation run is theirs and keeps their name, as anything else they send
    does.

    Their own context rides only a narrower set, the messages they typed
    themselves (:func:`member_typed`)."""
    return not posted_by_server(request.query_type, request.platform)


def member_typed(request: ChatRequest) -> bool:
    """Whether the member typed this turn's message as it was sent: one they
    wrote (:func:`member_wrote`) that started no other way, so not an
    automation's run nor work another agent sent (``ThreadOrigin``: a turn a
    person starts carries no origin). Only such a turn may carry that
    member's own context (``member_context.py``)."""
    return member_wrote(request) and request.origin is None


def _member_sender(user_id: str, name: Optional[str]) -> MemberSender:
    """The member under the label the model reads them by: their name, then
    a short id that is theirs alone, so two members are never read as one
    whatever names they go by or take later. The id is a digest of theirs,
    the same in every thread and on every message."""
    short = short_id(user_id)
    # One line and bounded: the label rides every call of every member.
    shown = " ".join((name or "").split())[:NAME_MAX] or "Member"
    return MemberSender(user_id, f"{shown} ({short})")


#: The longest name a sender tag spells out.
NAME_MAX = 64


def short_id(user_id: str) -> str:
    """A member's short id: a digest of theirs, the same wherever it names
    them (their sender tag, their memory's folder), revealing nothing of the
    account it is taken from."""
    return hashlib.sha256(user_id.encode()).hexdigest()[:6]

