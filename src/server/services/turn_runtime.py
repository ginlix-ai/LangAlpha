"""Where a turn runs, and in which role.

Every entry that starts a turn asks here, so a thread's type and the runs it
carries cannot disagree. A turn in a workspace runs that workspace's analyst;
work outside one runs the Chief of Staff in the user's Home. A client asks for
Home by naming Home's id (``POST /workspaces/home`` returns it), the same way
it names any workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

from fastapi import HTTPException

from src.server.database.home_workspace import get_home_workspace_id, is_home_id, is_home_row

if TYPE_CHECKING:
    from ptc_agent.agent.agent import AgentRole


@dataclass(frozen=True, slots=True)
class TurnRoute:
    """The workspace a turn runs in and the role the agent plays there."""

    workspace_id: str
    role: AgentRole

    async def bind(self, user_id: str, workspace: Optional[Dict[str, Any]]) -> None:
        """Put Home on the user's computer when the turn runs there.

        The caller's last step before the turn, once its own gates pass, so a
        refused turn leaves the computer as it was. An analyst's workspace is
        already on one.
        """
        if self.role == "chief_of_staff":
            await ensure_home(user_id, workspace)


def turn_route(user_id: str, workspace: Optional[Dict[str, Any]]) -> TurnRoute:
    """Where a turn runs, decided by the row it names alone.

    ``workspace`` is the caller's read of that row, None when the turn names
    none. Nothing is bound here: the caller binds the route
    (:meth:`TurnRoute.bind`) once its own gates pass, so a turn refused there
    leaves the user's computer as it was.
    """
    if workspace is not None and not is_home_row(workspace):
        return TurnRoute(str(workspace["workspace_id"]), "analyst")
    return TurnRoute(get_home_workspace_id(user_id), "chief_of_staff")


async def requested_workspace(
    user_id: str, workspace_id: Optional[str]
) -> Optional[Dict[str, Any]]:
    """The workspace a client's turn names, once it is the caller's.

    Checked before the credit gate, as a malformed or foreign request is the
    client's error whatever its balance, and before any route is taken, so a
    turn that is refused never binds Home. A new thread has nowhere to run
    until it names a place, so naming none is refused rather than guessed.
    Home's id is the caller's by derivation, so it routes to Home before its
    row exists.
    """
    from src.server.database.workspace import get_workspace
    from src.server.utils.api import require_workspace_owner

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
    # A fresh thread id must not run inside another user's workspace. The
    # report-back dispatch sets X-User-Id to the owner, so it passes.
    require_workspace_owner(workspace, user_id=user_id)
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
