"""Home routes: the user's Home, where it is, and what its stand-in handed back.

Under ``/api/v1/workspaces`` beside the workspace routes, since Home is a
workspace to every client; registered ahead of them so ``/home`` is never read
as a workspace id.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Response

from src.server.app.workspaces import _workspace_to_response
from src.server.database.home_away import list_handbacks, mark_handback_seen
from src.server.database.home_workspace import get_home_stand_in_id
from src.server.database.workspace import (
    HomeWorkspaceTaken,
    get_or_create_home_workspace,
)
from src.server.dependencies.principal import ReadPrincipal, ReadUserId
from src.server.models.computer import ComputerStatus
from src.server.services.background_starts import schedule_start
from src.server.services.computer_usage.causes import computer_cause
from src.server.models.home import (
    HomeHandbackOut,
    HomeHandbacksResponse,
    HomeHandbackSeenRequest,
    HomePlacementResponse,
)
from src.server.models.workspace import WorkspaceResponse
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspaces"])


@router.post("/home", response_model=WorkspaceResponse)
async def get_home_workspace(principal: ReadPrincipal):
    """Get or create the user's Home, where work in no particular workspace runs.

    Its id derives from the user id, so it never changes and a client may keep
    it. Idempotent, and safe to call on every app load. The row is made here;
    Home joins the user's computer on its first start or turn.
    """
    x_user_id = principal.user_id
    try:
        workspace = await get_or_create_home_workspace(x_user_id)
    except HomeWorkspaceTaken:
        # The owner check's answer for a row another account holds; no retry
        # makes this one the caller's.
        raise HTTPException(status_code=403, detail="Forbidden") from None
    except Exception as e:
        logger.exception(f"Error ensuring Home: {e}")
        raise HTTPException(status_code=500, detail="Failed to ensure Home")
    if principal.unlimited:
        # The app's own load only: a token that may not add computers adds none.
        schedule_start(f"stand-in-host:{x_user_id}", lambda: _make_stand_in_host(x_user_id))
    return _workspace_to_response(workspace)


async def _make_stand_in_host(user_id: str) -> None:
    """While Home lives on the user's own machine and the plan grants a cloud
    computer, the next app load keeps one for Home's stand-in: a cloud
    computer they have is marked, or one is made in the background and its
    first build started. Best effort: the next load tries again."""
    from src.server.app.computers import schedule_computer_start
    from src.server.services.computer_manager import ComputerManager

    try:
        computer = await ComputerManager.get_instance().ensure_stand_in_host(user_id)
    except Exception:
        logger.warning(
            "Could not make Home's stand-in a computer for user %s", user_id, exc_info=True
        )
        return
    if computer is not None and computer["status"] == ComputerStatus.CREATING:
        with computer_cause("create"):
            await schedule_computer_start(str(computer["computer_id"]))


@router.get("/home/placement", response_model=HomePlacementResponse)
async def get_home_placement(x_user_id: ReadUserId):
    """Where Home is and where its next turn would run, for the client.

    A read: polling it never places a stand-in or starts a computer.
    """
    from src.server.services.home_placement import home_placement_view

    return HomePlacementResponse.model_validate(await home_placement_view(x_user_id))


@router.get("/home/handbacks", response_model=HomeHandbacksResponse)
async def list_home_handbacks(
    x_user_id: ReadUserId,
    since: datetime = Query(
        description="The earliest placement (`home_fallback.at`) of the turns asked about"
    ),
):
    """Where each file a Home turn wrote in the stand-in went in Home.

    A turn's file card names the path in the stand-in's folder; the first
    hand-back after that turn whose ``files`` hold it put the copy under its
    ``folder`` in Home, and one whose ``kept`` holds it found it unchanged
    since the earlier hand-back whose folder it names.
    """
    return HomeHandbacksResponse(
        stand_in_workspace_id=get_home_stand_in_id(x_user_id),
        handbacks=[
            HomeHandbackOut.model_validate(row)
            for row in await list_handbacks(x_user_id, since)
        ],
    )


@router.post("/home/placement/seen", status_code=204)
async def mark_home_handback_seen(
    x_user_id: CurrentUserId, request: Optional[HomeHandbackSeenRequest] = None
):
    """The user dismissed the notice of what came back from the cloud."""
    await mark_handback_seen(x_user_id, request.handback_id if request else None)
    return Response(status_code=204)
