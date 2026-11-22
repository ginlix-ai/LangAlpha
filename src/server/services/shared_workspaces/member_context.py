"""Whether a member's own context reaches the agent on their turn in a
shared workspace.

A shared workspace's build carries nothing of any one member: other members
read everything the agent says there and steer it. An organization may grant
an exception (the ``shared_workspaces_member_context`` feature, which a
deployment can switch off like any other): on a member's own messages the
agent also uses that member's answer preferences and may read their personal
memory, read only. The workspace's managers may keep it out of their
workspace (``SharedRow.member_context``), and each member may keep their own
out of every shared workspace (``OWN_SWITCH``). Each turn asks again, so a
change holds from the next one.

Nothing here is remembered between turns but the alias key, which a
migration writes once: every answer is read from the workspace's row and the
sender's account and preferences as of the turn.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Optional

from ptc_agent.agent.middleware.member_context import MemberContext
from src.config.features import is_feature_enabled_system
from ptc_agent.agent.transcript.sender import MemberSender
from src.server.database.conversation.senders import (
    resumed_turn_typed,
    run_turn_typed,
    turn_typed,
)
from src.server.database.server_keys import get_server_key
from src.server.database.shared_workspaces import SharedRow
from src.server.database.user import get_user_preferences
from src.server.models.shared_workspace import MemberContextView
from src.server.services import features
from src.server.services.shared_workspaces.turn import TurnIdentity, member_typed, short_id

if TYPE_CHECKING:
    from src.server.models.chat import ChatRequest

logger = logging.getLogger(__name__)

FEATURE = "shared_workspaces_member_context"
#: The member's own switch in their ``other_preference``; absent means on.
OWN_SWITCH = "own_context_in_shared_workspaces"
#: The ``server_keys`` row a member's memory aliases are made with
#: (migration 149).
ALIAS_KEY_NAME = "member_memory_alias"

# The one process-level cache a request path may consult: the key is written
# once by a migration and never changes, so every worker reads the same bytes
# and a stale copy cannot exist.
_alias_key: bytes | None = None


async def alias_key() -> bytes:
    global _alias_key
    if _alias_key is None:
        _alias_key = await get_server_key(ALIAS_KEY_NAME)
    return _alias_key


async def organization_allows(user_id: str, org_id: str, *, recheck: bool = False) -> bool:
    """Whether ``user_id`` is an active member of ``org_id`` with the feature
    on for them. An account that cannot be read grants nothing."""
    try:
        member = await features.org_member(user_id, FEATURE, recheck=recheck)
    except Exception:  # noqa: BLE001 - a failed read grants nothing
        logger.warning("[member_context] account read failed for %s", user_id, exc_info=True)
        return False
    return member is not None and member.org.id == org_id


async def own_switch_on(user_id: str) -> bool:
    """The member's own switch (``OWN_SWITCH`` in their ``other_preference``):
    on unless they turned it off. Read from the row, since a member turning it
    off is keeping their context out of a workspace others read, and that
    holds from the next turn; a read that fails keeps it out."""
    try:
        prefs = await get_user_preferences(user_id, cached=False)
    except Exception:  # noqa: BLE001 - a failed read grants nothing
        logger.warning("[member_context] preferences read failed for %s", user_id, exc_info=True)
        return False
    return ((prefs or {}).get("other_preference") or {}).get(OWN_SWITCH) is not False


async def organization_grants(user_id: str, org_id: str) -> bool:
    """Whether ``user_id``'s organization grants it. A cached yes is asked
    again of the auth service, so a grant withdrawn reaches the next turn even
    before its change notice does, and while it cannot be asked the grant is
    withheld; a no is taken as read."""
    if not await organization_allows(user_id, org_id):
        return False
    return await organization_allows(user_id, org_id, recheck=True)


async def grant(identity: TurnIdentity) -> bool:
    """Whether this shared turn's sender's own context may reach the agent:
    while the workspace's managers leave it on (``SharedRow.member_context``),
    the sender's organization, whose workspace this is, grants it, and the
    sender has not turned it off themselves. Reads nothing past the
    workspace's row unless the managers left it on."""
    sender, row = identity.sender, identity.shared_row
    if not sender or row is None or not row.member_context:
        return False
    granted, own = await asyncio.gather(
        organization_grants(sender, row.org_id), own_switch_on(sender)
    )
    return granted and own


async def for_turn(
    identity: TurnIdentity,
    request: ChatRequest,
    sender: Optional[MemberSender],
    profile: Optional[Mapping[str, Any]],
    *,
    thread_id: Optional[str] = None,
    replaced_typed: Optional[bool] = None,
) -> Optional[MemberContext]:
    """The member whose own context this turn carries, or None. ``sender``
    is what the turn's messages record of their writer
    (``TurnIdentity.message_sender``), ``profile`` the sender's whole
    profile as the turn read it. Only a message the member typed carries it
    (``member_typed``): never a turn the server posted, an automation's run
    or another agent's work, nor a retry of one, whose request comes back
    without the origin the turn was sent with, nor a resume or a regenerate
    of one, which come back the same way (``thread_id`` names the thread;
    ``replaced_typed`` is :func:`replaced_turn_typed`, read before START)."""
    if sender is None or not member_typed(request):
        return None
    row = identity.shared_row
    if row is None or not row.member_context:
        return None  # the managers' switch, read before anything costs a read
    if not is_feature_enabled_system(FEATURE):
        return None  # nor the deployment's, so no provenance read can fail it
    if request.retry_of_run_id and not await run_turn_typed(request.retry_of_run_id):
        return None
    if request.hitl_response and not (thread_id and await resumed_turn_typed(thread_id)):
        return None
    if not _fork_typed(request, replaced_typed):
        return None
    if not await grant(identity):
        return None
    try:
        key = await alias_key()
    except Exception:  # noqa: BLE001 - a key that cannot be read grants nothing
        logger.warning("[member_context] alias key read failed", exc_info=True)
        return None
    preference = (profile or {}).get("agent_preference")
    return MemberContext(
        user_id=sender.user_id,
        name=sender.name,
        handle=short_id(sender.user_id),
        workspace_id=identity.shared_row.workspace_id,
        alias_key=key,
        agent_preference=dict(preference) if isinstance(preference, Mapping) else {},
    )


async def replaced_turn_typed(
    identity: TurnIdentity, request: ChatRequest, thread_id: str
) -> Optional[bool]:
    """Whether a person typed the turn an edit or a regenerate replaces, read
    before START: START writes that turn's query row anew for an edit, and
    for a regenerate of a turn that never ran, which comes back as the resend
    of its stored message, without the origin the turn was sent with. None
    where the request replaces no turn, or the deployment or the workspace's
    managers keep the context out."""
    row = identity.shared_row
    if request.fork_from_turn is None or row is None or not row.member_context:
        return None
    if not is_feature_enabled_system(FEATURE):
        return None
    return await turn_typed(thread_id, request.fork_from_turn)


def _fork_typed(request: ChatRequest, replaced_typed: Optional[bool]) -> bool:
    """Whether a run from an earlier checkpoint may carry it: one replacing
    a turn only where a person typed that turn (``replaced_typed``), since a
    regenerate comes back without the turn's origin, and an edit reaches here
    looking like a regenerate's resend; a retry's replay is told by the run
    it retries (``run_turn_typed``, asked before this); any other replay
    names no turn, tells nothing and is withheld."""
    if request.fork_from_turn is not None:
        return bool(replaced_typed)
    return not (request.checkpoint_id and not request.messages and not request.retry_of_run_id)


async def view_for(caller: str, row: SharedRow) -> MemberContextView:
    """What the people view says of it: whether ``caller``'s organization
    grants it, read from their account as cached, whether the
    workspace's managers left it on, and whether this deployment offers it."""
    return MemberContextView(
        organization=await organization_allows(caller, row.org_id),
        workspace=row.member_context,
        offered=is_feature_enabled_system(FEATURE),
    )
