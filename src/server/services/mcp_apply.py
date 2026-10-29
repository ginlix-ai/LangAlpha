"""Bringing a workspace's running sandbox up to a just-saved MCP config.

A config write only bumps versions, which a session picks up on its next
acquire. These schedule that acquire now, in the background, so a change is
live before the user's next turn; any failure falls back to that next turn.
Every door that changes what a workspace's servers are (a catalog write, a
vault change, a sign-in) schedules through here.
"""

from __future__ import annotations

import asyncio

# Strong refs to in-flight proactive-apply tasks so they aren't GC'd mid-run.
_proactive_apply_tasks: set[asyncio.Task] = set()
_proactive_apply_pending: dict[str, asyncio.Task] = {}
_PROACTIVE_APPLY_SETTLE_S = 1.5


def schedule_proactive_apply(workspace_id: str, user_id: str) -> None:
    """Front-load verifying + applying a just-saved MCP config.

    Fire-and-forget so it never blocks (or fails) the mutation response. It
    drives a background session acquire that brings the applied config up to the
    new version, warming (cold-starting) the sandbox if it isn't running yet,
    so the change is discovered and live before the user's next turn (no
    surprise). Best-effort: any failure falls back to the next-message apply.

    Mutations within the settle window coalesce into one apply: a newer
    mutation cancels a still-waiting sleeper, never an in-flight apply.
    """
    # At call time: the workspace manager loads the sign-in package, which
    # schedules through here.
    from src.server.services.workspace_manager import WorkspaceManager

    try:
        wm = WorkspaceManager.get_instance()
    except Exception:
        return

    pending = _proactive_apply_pending.get(workspace_id)
    if pending is not None and not pending.done():
        pending.cancel()

    async def _settle_then_apply() -> None:
        await asyncio.sleep(_PROACTIVE_APPLY_SETTLE_S)
        # Past the settle window: deregister so newer mutations schedule a
        # fresh apply instead of cancelling this one mid-flight.
        if _proactive_apply_pending.get(workspace_id) is asyncio.current_task():
            _proactive_apply_pending.pop(workspace_id, None)
        await wm.proactively_apply_mcp_config(workspace_id, user_id)

    task = asyncio.create_task(_settle_then_apply())
    _proactive_apply_pending[workspace_id] = task
    _proactive_apply_tasks.add(task)

    def _cleanup(t: asyncio.Task) -> None:
        _proactive_apply_tasks.discard(t)
        if _proactive_apply_pending.get(workspace_id) is t:
            _proactive_apply_pending.pop(workspace_id, None)

    task.add_done_callback(_cleanup)


def schedule_session_mcp_refresh(workspace_id: str, user_id: str) -> None:
    """Background composite rebuild after an out-of-band schema-cache update.

    Unlike ``schedule_proactive_apply`` there is no version bump to apply, so
    this goes through ``refresh_session_mcp`` (which busts the session's cached
    version first). Undebounced: probes are explicit single user actions.
    """
    from src.server.services.workspace_manager import WorkspaceManager

    try:
        wm = WorkspaceManager.get_instance()
    except Exception:
        return
    task = asyncio.create_task(wm.refresh_session_mcp(workspace_id, user_id))
    _proactive_apply_tasks.add(task)
    task.add_done_callback(_proactive_apply_tasks.discard)
