"""
Sessions endpoint — active session stats.

Moved from /api/v1/chat/sessions to /api/v1/sessions.
"""

from fastapi import APIRouter

from src.server.database.computer import get_computers_for_user
from src.server.services.computer_manager import ComputerManager
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1/sessions", tags=["Sessions"])


@router.get("")
async def get_sessions(x_user_id: CurrentUserId):
    """
    Get information about the caller's active PTC sessions on this worker.

    A workspace id is the credential for the public file route, so the
    listing is scoped to machines the caller owns.
    """
    manager = ComputerManager.current()
    if manager is None:
        # Answers before the manager is constructed, and after a reset.
        return {
            "active_sessions": 0,
            "message": "Computer Manager not initialized",
        }
    owned = {str(c["computer_id"]) for c in await get_computers_for_user(x_user_id)}
    sessions = [
        s for s in manager.live_session_stats() if str(s.get("computer_id")) in owned
    ]
    return {
        "active_sessions": len(sessions),
        "idle_timeout": manager.idle_timeout,
        "cleanup_interval": manager.cleanup_interval,
        "workspaces": sessions,
    }
