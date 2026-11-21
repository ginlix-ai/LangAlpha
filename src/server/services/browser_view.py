"""Whether a computer's browser can be watched, and where.

The machine is the truth, asked on every call, so any worker can answer and a
browser the agent closed or a daemon that idled out is never reported as open.
Only a call that says so starts anything: a tab restored on a reload or left open
reads the state, and reading must not undo a stop.
"""

from __future__ import annotations

import json
import logging
import shlex
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

from ptc_agent.core.sandbox.browser_runtime import protocol
from ptc_agent.core.sandbox.browser_runtime.protocol import (
    HoldAction,
    NavigateAction,
    NavigateRefusal,
)
from ptc_agent.core.sandbox.providers import provider_class
from ptc_agent.core.sandbox.runtime import RuntimeState
from src.server.database.computer import get_computer_for_workspace
from src.server.services.preview_base import preview_base
from src.server.services.preview_capabilities import proxied, publish

logger = logging.getLogger(__name__)

__all__ = [
    "BrowserView",
    "ViewState",
    "ask_runtime",
    "browser_available",
    "browser_control",
    "browser_navigate",
    "browser_view",
]

# How long a view's URL opens a new socket; one already open outlives it, and a
# viewer asks again when it reconnects.
VIEW_LIFETIME_S = 3600
# Chrome's first start on a one-CPU machine, plus the bridge coming up.
_START_WAIT_S = 30
_STATUS_TIMEOUT_S = 15
# The machine's own wait for a page to load, plus the call there and back.
_NAVIGATE_TIMEOUT_S = 45

ViewState = Literal["running", "idle", "low_memory", "stopped", "outdated", "unavailable"]


@dataclass(frozen=True)
class BrowserView:
    """``running`` carries the URL its stream opens on and how long that URL opens
    a socket. ``idle`` is a computer with no browser open, ``low_memory`` one
    whose browser a start did not open because memory is short, ``stopped`` a computer
    that is not running, ``outdated`` one built before the browser shipped, and
    ``unavailable`` a kind of computer that never has one."""

    state: ViewState
    url: str | None = None
    expires_in: int | None = None


async def browser_available(workspace_id: str) -> bool:
    """Whether *workspace_id*'s kind of computer has a browser at all, read off
    its row so a computer that never has one is not reached for. A workspace not
    yet on a computer is placed on the deployment's own kind when it starts."""
    computer = await get_computer_for_workspace(workspace_id)
    return computer is None or provider_class(computer["kind"]).browser_use


async def browser_view(
    sandbox: Any, workspace_id: str, *, start: bool = False, viewer_id: str | None = None
) -> BrowserView:
    """The view of the browser on *sandbox*, opening it first with *start*.
    *viewer_id* is the shared workspace's member the stream is minted for. Where
    the preview proxy runs, their stream ends once they may no longer use the
    workspace; without it, a stream they already opened lasts until it drops."""
    ask = ("start", str(_START_WAIT_S)) if start else ("status",)
    try:
        answer = await _ask(
            sandbox, *ask, timeout=_STATUS_TIMEOUT_S + (_START_WAIT_S if start else 0)
        )
    except Exception as e:
        # A computer that stopped since the route found it running cannot
        # answer, and that is a stopped computer; one still running that did
        # not answer in time is an error, which the viewer asks again about
        # rather than offering to start a computer that is up.
        if start or not await _stopped(sandbox):
            raise
        logger.debug("Browser status did not answer: %s", type(e).__name__)
        return BrowserView("stopped")
    status = protocol.Status.read(answer)
    if not status.installed:
        return BrowserView("outdated")
    if status.low_memory:
        return BrowserView("low_memory")
    if not status.running or status.token is None:
        return BrowserView("idle")
    try:
        url = await _stream_url(
            sandbox, workspace_id, status.token, wake=start, viewer_id=viewer_id
        )
    except Exception:
        # A computer that stopped since it answered fails the link a read must
        # not wake it for, and that is a stopped computer, not an error.
        if not await _stopped(sandbox):
            raise
        return BrowserView("stopped")
    return BrowserView("running", url, VIEW_LIFETIME_S)


async def _stopped(sandbox: Any) -> bool:
    """Whether the computer has stopped, as its provider says now, which reads
    it without waking it. Unknown is not stopped."""
    try:
        return await sandbox.runtime.refresh_state() is not RuntimeState.RUNNING
    except Exception:
        return False


async def browser_control(
    sandbox: Any, *, holder: str, action: HoldAction, releases: list[dict[str, Any]] | None = None
) -> bool | None:
    """Take, renew or hand back *holder*'s hold on the page; whether it holds it
    now, or None when the machine did not answer. A hand-back lets go of
    *releases* first, so one that went unanswered may have let go of nothing."""
    command = ["control", holder, action]
    if action == "release" and releases:
        command.append(json.dumps(releases, separators=(",", ":")))
    try:
        answer = await _ask(sandbox, *command, timeout=_STATUS_TIMEOUT_S)
    except Exception as e:
        logger.debug("Browser control did not answer: %s", type(e).__name__)
        return None
    return isinstance(answer, dict) and answer.get("held") is True


async def browser_navigate(
    sandbox: Any, *, holder: str, action: NavigateAction, address: str | None
) -> NavigateRefusal | None:
    """Take the page where *holder*, who holds it, asked; why nothing was done,
    or None once it was."""
    if action == "open":
        address = protocol.page_address(address or "")
        if address is None:
            return "bad_address"
    command = ("navigate", holder, action, *([address] if action == "open" else []))
    try:
        answer = await _ask(sandbox, *command, timeout=_NAVIGATE_TIMEOUT_S)
    except Exception as e:
        logger.debug("Browser navigation did not answer: %s", type(e).__name__)
        return "failed"
    if isinstance(answer, dict) and answer.get("done") is True:
        return None
    reason = answer.get("reason") if isinstance(answer, dict) else None
    return reason if reason in protocol.NAVIGATE_REFUSALS else "failed"


async def ask_runtime(runtime: Any, internal_src: str, *command: str, timeout: float) -> object:
    """What the machine's browser runtime under *internal_src* answers *command*
    with: the JSON on the last line it prints, or None for output that holds
    none, as from a machine with no runtime. Raises for a machine that does not
    answer.

    On the runtime itself: the sandbox's own call path reconnects, which would
    start a computer that stopped on its idle timer.
    """
    package = f"{internal_src}/{protocol.PACKAGE_NAME}"
    result = await runtime.exec(
        f"python3 -I {shlex.quote(package)} {shlex.join(command)}", timeout=timeout
    )
    lines = (result.stdout or "").strip().splitlines()
    try:
        return json.loads(lines[-1])
    except (IndexError, ValueError):
        return None


async def _ask(sandbox: Any, *command: str, timeout: float) -> object:
    return await ask_runtime(
        sandbox.runtime, sandbox.layout.internal_src, *command, timeout=timeout
    )


async def _stream_url(
    sandbox: Any, workspace_id: str, token: str, *, wake: bool, viewer_id: str | None
) -> str:
    # A read must not start a computer that stopped since it answered.
    info = await sandbox.get_preview_url(protocol.BRIDGE_PORT, VIEW_LIFETIME_S, wake=wake)
    if (
        info.browser_ready
        and (viewer_id is not None or not info.token)
        and preview_base() is not None
    ):
        # A URL the browser dials itself keeps its socket open past a revocation:
        # one with no credential of its own (a published Docker port), and a
        # shared workspace's member's on any computer, whose stream should end once
        # they may no longer use the workspace. Where the proxy runs those go
        # through it, which re-reads both; without it, an open socket outlives
        # the revocation until it drops, and only a new stream is refused.
        info = proxied(await sandbox.get_preview_link(protocol.BRIDGE_PORT, wake=wake))
    url = await publish(
        info,
        sandbox_id=sandbox.sandbox_id,
        port=protocol.BRIDGE_PORT,
        workspace_id=workspace_id,
        expires_in=VIEW_LIFETIME_S,
        viewer_id=viewer_id,
    )
    # The provider's own query, a signed URL's token, stays on it.
    return urlunsplit(urlsplit(url)._replace(path=protocol.PATH_PREFIX + token, fragment=""))
