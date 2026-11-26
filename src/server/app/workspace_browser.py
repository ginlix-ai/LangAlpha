"""The computer's browser, as the panel's Browser tab sees and holds it.

Endpoints:
- POST /api/v1/workspaces/{workspace_id}/sandbox/browser-view
- POST /api/v1/workspaces/{workspace_id}/sandbox/browser-control
- POST /api/v1/workspaces/{workspace_id}/sandbox/browser-navigate
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from ptc_agent.core.sandbox.browser_runtime import protocol
from ptc_agent.core.sandbox.browser_runtime.protocol import (
    HoldAction,
    NavigateAction,
    NavigateRefusal,
)

from src.server.services.browser_view import (
    ViewState,
    browser_control,
    browser_navigate,
    browser_view,
)
from src.server.services.preview_capabilities import (
    PreviewProxyNotConfigured,
    PreviewProxyUnavailable,
)
from src.server.services.features import user_feature_enabled
from src.server.services.preview_links import proxy_failure
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.workspace_files import (
    acquire_sandbox,
    preview_viewer,
    reached_workspace_on_computer,
    sandbox_if_running,
)
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Sandbox"])


class _NoBrowser(Exception):
    """Nothing for a browser route to reach: a kind of computer that never has
    a browser, or one not running for a route that does not start it."""

    def __init__(self, state: Literal["unavailable", "stopped"]) -> None:
        super().__init__(state)
        self.state: Literal["unavailable", "stopped"] = state


async def _browser_sandbox(
    workspace_id: str, user_id: str, *, start: bool = False
) -> tuple[Any, str | None, str | None]:
    """The sandbox whose browser a route acts on, the member its stream is
    minted for, and the computer it runs: a workspace the caller reaches as
    its files' routes do, for a caller who turned the browser on, on a kind of
    computer with a browser, while it runs, or started with *start*.
    Raises ``_NoBrowser`` where there is none to reach."""
    workspace, _ = await reached_workspace_on_computer(workspace_id, user_id)
    viewer = preview_viewer(workspace, user_id)
    # Each person's own switch: in a shared workspace the owner's says nothing
    # about whether a member turned the browser on.
    if not await user_feature_enabled(user_id, "browser"):
        raise _NoBrowser("unavailable")
    if start:
        # The row as the acquisition left it, which a move may have changed.
        sandbox, acquired = await acquire_sandbox(workspace_id, user_id, "browser_view")
        return sandbox, viewer, _computer_of(acquired)
    sandbox = await sandbox_if_running(workspace_id, user_id)
    if sandbox is None:
        raise _NoBrowser("stopped")
    return sandbox, viewer, _computer_of(workspace)


def _computer_of(workspace: dict[str, Any]) -> str | None:
    computer_id = workspace.get("computer_id")
    return str(computer_id) if computer_id else None


class BrowserViewRequest(BaseModel):
    # Opens the browser if it is not up, and starts a stopped computer to do it.
    start: bool = False
    # On a machine of the person's own with no Chrome-family browser, starts
    # downloading Chrome for Testing to it.
    get_chrome: bool = False


class BrowserViewResponse(BaseModel):
    state: ViewState
    url: str | None = None
    expires_in: int | None = None
    # The same stream from an app on the machine itself, past the server.
    local_url: str | None = None


class BrowserControlRequest(BaseModel):
    # Each viewer names itself, so one viewer's release or late renewal never
    # ends or takes back another's hold.
    holder: str = Field(pattern=protocol.HOLDER_PATTERN)
    action: HoldAction
    # On a hand-back, what the person still had down that their stream could
    # not deliver, which the machine lets go of before the hold ends.
    releases: list[dict[str, Any]] = Field(default_factory=list, max_length=protocol.MAX_RELEASES)

    @field_validator("releases")
    @classmethod
    def _engine_can_release(cls, releases: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if protocol.release_commands(releases) is None:
            raise ValueError("not a key or mouse button to let go of")
        return releases


class BrowserControlResponse(BaseModel):
    # Whether this holder holds the page now.
    held: bool


@router.post("/{workspace_id}/sandbox/browser-control")
async def set_browser_control(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: BrowserControlRequest,
) -> BrowserControlResponse:
    """Take, renew or hand back a viewer's hold on the page.

    A page any viewer holds refuses the agent's commands that would act on it.
    ``take`` starts or extends this holder's hold, ``renew`` extends only one it
    still has, and ``release`` ends only its own. One that stops renewing loses
    it. A stopped computer has nothing to hold and is not started for it.
    """
    try:
        sandbox, _viewer, _computer = await _browser_sandbox(workspace_id, x_user_id)
    except _NoBrowser:
        return BrowserControlResponse(held=False)
    held = await browser_control(
        sandbox, holder=body.holder, action=body.action, releases=body.releases
    )
    if held is None:
        # Not "not held": a hand-back that went unanswered may still have the
        # person's keys down on the page, and the viewer asks again.
        raise HTTPException(status_code=503, detail="The computer did not answer")
    if held:
        await _note_use(workspace_id, sandbox)
    return BrowserControlResponse(held=held)


async def _note_use(workspace_id: str, sandbox: Any) -> None:
    """A person holding the page is using the computer, which neither a turn nor
    the provider shows: a stream straight from the provider passes no proxy that
    would count it. The hold's renewals are the heartbeat, as an open preview's
    health poll is; one that lands nowhere costs the hold nothing."""
    try:
        await WorkspaceManager.get_instance().keep_sandbox_alive(
            workspace_id, sandbox.sandbox_id, runtime=getattr(sandbox, "runtime", None)
        )
    except Exception as e:
        logger.debug("Browser use did not reach workspace %s: %s", workspace_id, type(e).__name__)


class BrowserNavigateRequest(BaseModel):
    holder: str = Field(pattern=protocol.HOLDER_PATTERN)
    action: NavigateAction
    # What the person typed, for ``open``; read as an address bar reads it.
    url: str | None = Field(default=None, max_length=protocol.MAX_ADDRESS_LENGTH)


class BrowserNavigateResponse(BaseModel):
    done: bool
    # Why nothing was done.
    reason: NavigateRefusal | None = None


@router.post("/{workspace_id}/sandbox/browser-navigate")
async def navigate_browser(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: BrowserNavigateRequest,
) -> BrowserNavigateResponse:
    """Take the page to an address, or back or forward, for the viewer holding it.

    Only a viewer's own live hold lets it through, as the page is the agent's
    otherwise. A stopped computer is not started for it.
    """
    try:
        sandbox, _viewer, _computer = await _browser_sandbox(workspace_id, x_user_id)
    except _NoBrowser:
        return BrowserNavigateResponse(done=False, reason="not_running")
    reason = await browser_navigate(
        sandbox, holder=body.holder, action=body.action, address=body.url
    )
    return BrowserNavigateResponse(done=reason is None, reason=reason)


@router.post("/{workspace_id}/sandbox/browser-view")
async def get_browser_view(
    workspace_id: str,
    x_user_id: CurrentUserId,
    body: BrowserViewRequest,
) -> BrowserViewResponse:
    """The state of the computer's browser, and the URL its live stream opens on.

    Asking without ``start`` changes nothing, so a tab that comes back from a
    reload never starts a browser or a computer.
    """
    try:
        sandbox, viewer, computer_id = await _browser_sandbox(
            workspace_id, x_user_id, start=body.start
        )
        view = await browser_view(
            sandbox,
            workspace_id,
            computer_id,
            start=body.start,
            get_chrome=body.get_chrome,
            viewer_id=viewer,
        )
    except _NoBrowser as e:
        return BrowserViewResponse(state=e.state)
    except HTTPException:
        raise
    except (PreviewProxyNotConfigured, PreviewProxyUnavailable) as e:
        raise proxy_failure(e, workspace_id) from None
    except Exception:
        logger.exception("Failed to read the browser view of workspace %s", workspace_id)
        raise HTTPException(status_code=500, detail="Failed to read the browser") from None
    return BrowserViewResponse(
        state=view.state, url=view.url, expires_in=view.expires_in, local_url=view.local_url
    )
