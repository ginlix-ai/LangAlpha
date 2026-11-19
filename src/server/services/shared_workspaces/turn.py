"""Who a turn is for and whose workspace it runs in, decided once a turn."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional

from src.server.database import shared_workspaces as db
from src.server.database.conversation.senders import stamp_thread_creator
from src.server.database.runs.hook_jobs import SHARED_ORG_META
from src.server.database.shared_workspaces import is_holder
from src.server.database.user import get_user_profile_for_prompt
from src.server.services.shared_workspaces.holder import shared_profile


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
    #: A shared workspace's organization, which hears of its runs. Read only
    #: by :meth:`of`, for a turn; None once the workspace's row is gone.
    org_id: Optional[str] = None

    @classmethod
    async def of(
        cls, sender: Optional[str], owner: Optional[str], *, carried: Optional[str] = None
    ) -> TurnIdentity:
        """Reads nothing for a turn in anyone's own workspace. ``carried`` is
        the account of the turn a later run was started for, which it keeps."""
        org_id = None
        if is_holder(owner):
            row = await db.shared_of_holder(owner)
            org_id = row.org_id if row else None
        recorded_as = carried or choose_recorded_as(sender, owner, org_id)
        return cls(sender, owner, recorded_as, org_id)

    @property
    def shared(self) -> bool:
        return is_holder(self.owner)

    async def profile(self) -> Optional[dict[str, Any]]:
        """The sender's profile as the turn's prompt and context state it."""
        profile = await get_user_profile_for_prompt(self.sender) if self.sender else None
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

    def announced(self, profile: Optional[dict[str, Any]]) -> dict[str, Any]:
        """The sender a shared run's start frame names, for a member
        attaching live to draw it as theirs."""
        if not self.shared:
            return {}
        return {"sender": {"user_id": self.sender, "name": (profile or {}).get("name")}}
