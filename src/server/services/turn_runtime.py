"""Where a turn runs, and in which role.

Every entry that starts a turn asks here, so a thread's type and the runs it
carries cannot disagree. A turn in a workspace runs that workspace's analyst;
work outside one runs the Chief of Staff in the user's Home. A client asks for
Home by naming Home's id (``POST /workspaces/home`` returns it), the same way
it names any workspace.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Dict, Optional

from fastapi import HTTPException

from src.server.database.home_workspace import (
    get_home_workspace_id,
    is_home_id,
    is_home_row,
    is_home_stand_in_row,
)
from src.server.services.shared_workspaces.access import workspace_access

if TYPE_CHECKING:
    from ptc_agent.agent.agent import AgentRole
    from ptc_agent.agent.middleware.runtime_context import HomeAway


@dataclass(frozen=True, slots=True)
class TurnRoute:
    """The workspace a turn runs in, the role the agent plays there, and
    whose machine and workspace it is.

    A turn has two accounts. The sender, which every entry carries as its
    ``user_id``, is the person: models, rules, profile and every row that
    records who acted. Its admission and usage name the account
    ``TurnIdentity.recorded_as`` chooses. ``owner_id`` is the account that owns
    the workspace row, and so the computer it is on: what the computer is
    given (vault, connectors, skills, tokens, the file mount) and the
    workspace's memory are read for it. Resolved here, from the row the turn
    names, and never from the sender, so a read of the machine cannot fall
    back on whoever happened to send the turn.

    ``workspace_id`` is the thread's: its turns, memory and records. Once
    bound, ``away`` is set for a Home turn in Home's stand-in while the
    machine Home lives on can't be reached, the one case whose computer and
    folder (``runs_in``) are another workspace's.
    """

    workspace_id: str
    role: AgentRole
    owner_id: str
    away: Optional[HomeAway] = None

    @property
    def runs_in(self) -> str:
        """The workspace whose computer and folder the turn runs in."""
        return self.away.workspace_id if self.away is not None else self.workspace_id

    async def bind(
        self,
        user_id: str,
        workspace: Optional[Dict[str, Any]],
        *,
        place: bool = True,
        follows: Optional[str] = None,
    ) -> TurnRoute:
        """Put Home on the user's computer when the turn runs there, and say
        which computer the turn runs on.

        The caller's last step before the turn, once its own gates pass, so a
        refused turn leaves the computer as it was. An analyst's workspace is
        already on one. The choice is read from Postgres here, at admission,
        so every worker makes the same one (``home_placement``). ``place`` is
        off for a door that only makes the thread and runs nothing;
        ``follows`` is the workspace a command the turn reports on ran in.
        """
        if self.role != "chief_of_staff":
            return self
        await ensure_home(user_id, workspace)
        return await self.placed(user_id, follows=follows) if place else self

    async def placed(self, user_id: str, *, follows: Optional[str] = None) -> TurnRoute:
        """This route with where its Home turn runs read again, as for a turn
        that waited for its thread after its bind."""
        if self.role != "chief_of_staff":
            return self
        from src.server.services.home_placement import place_home_turn

        return replace(self, away=await place_home_turn(user_id, follows=follows))


def turn_route(user_id: str, workspace: Optional[Dict[str, Any]]) -> TurnRoute:
    """Where a turn runs, decided by the row it names alone.

    ``workspace`` is the caller's read of that row, None when the turn names
    none. Nothing is bound here: the caller binds the route
    (:meth:`TurnRoute.bind`) once its own gates pass, so a turn refused there
    leaves the user's computer as it was.
    """
    if workspace is not None and not is_home_row(workspace):
        return TurnRoute(
            str(workspace["workspace_id"]), "analyst", str(workspace["user_id"])
        )
    # Home is the sender's own by derivation, whichever Home row was named.
    return TurnRoute(get_home_workspace_id(user_id), "chief_of_staff", user_id)


def named_workspace_id(
    workspace_id: Optional[str], thread: Optional[Dict[str, Any]]
) -> Optional[str]:
    """The workspace a turn's request names: its own, else its thread's
    (``thread`` the caller's read of that row). None for a new thread naming
    none, which ``requested_workspace`` refuses."""
    if workspace_id:
        return workspace_id
    return str(thread["workspace_id"]) if thread else None


async def requested_workspace(
    user_id: str, workspace_id: Optional[str]
) -> Optional[Dict[str, Any]]:
    """The workspace a client's turn names, once it is the caller's.

    Checked before the credit gate, as a malformed or foreign request is the
    client's error whatever its balance, and before any route is taken, so a
    turn that is refused never binds Home. A new thread has nowhere to run
    until it names a place, so naming none is refused rather than guessed.
    Home's id is the caller's by derivation, so it routes to Home before its
    row exists. A workspace on a machine the policy keeps work off is refused.
    """
    from src.server.database.workspace import get_workspace

    if not workspace_id:
        # Clients match the opening clause, so it stays as is.
        raise HTTPException(
            status_code=400,
            detail=(
                "workspace_id is required to start a thread. Name a "
                "workspace, or Home by the id POST /api/v1/workspaces/home "
                "returns."
            ),
        )
    workspace = await get_workspace(workspace_id)
    if workspace is None and is_home_id(user_id, workspace_id):
        return None
    # The stand-in is Home's to run in, never a client's to open a thread in:
    # it would run as an analyst workspace with no away mark, so its files
    # would never be owed to Home. Its files stay readable by workspace routes.
    if is_home_stand_in_row(workspace):
        raise HTTPException(status_code=404, detail="Workspace not found")
    # A fresh thread id must not run inside another user's workspace, unless
    # it is a shared workspace the sender is a member of. The report-back
    # dispatch sets X-User-Id to the sender it posts for, so it passes.
    await workspace_access(workspace, user_id)
    # Refused here, before a send's stream opens or either door writes a
    # thread row, as a 4xx a client shows rather than a stream error it would
    # offer to retry. The acquire still asks, for every other path.
    from src.server.services.account import refuse_machine_use

    await refuse_machine_use(workspace.get("computer_kind"), user_id)
    return workspace


async def ensure_home(
    user_id: str, home_workspace: Optional[Dict[str, Any]] = None
) -> str:
    """The id of the user's Home, its row created and put on their computer if need be.

    ``home_workspace`` is a read of the user's Home row the caller already
    holds, which spares the upsert. A former Home at another id does not: the
    upsert is what folds it into Home.
    """
    from src.server.database.workspace import (
        HomeWorkspaceTaken,
        get_or_create_home_workspace,
    )
    from src.server.services.workspace_manager import WorkspaceManager

    home_id = get_home_workspace_id(user_id)
    if home_workspace is None or not is_home_id(user_id, home_workspace.get("workspace_id")):
        try:
            await get_or_create_home_workspace(user_id)
        except HomeWorkspaceTaken:
            # The id is another account's. That is the owner check's answer
            # for any row another account holds, and no retry can make this
            # one the caller's.
            raise HTTPException(status_code=403, detail="Forbidden") from None
    await WorkspaceManager.get_instance().ensure_home_bound(user_id)
    return home_id
