"""Whether a computer's browser can be watched, and where.

The machine is the truth, asked on every call, so any worker can answer and a
browser the agent closed or a daemon that idled out is never reported as open.
Only a call that says so starts anything: a tab restored on a reload or left open
reads the state, and reading must not undo a stop.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal
from urllib.parse import urlsplit, urlunsplit

from ptc_agent.core.sandbox.browser_runtime import protocol
from ptc_agent.core.sandbox.browser_runtime.protocol import (
    HoldAction,
    InstallRefusal,
    NavigateAction,
    NavigateRefusal,
)
from ptc_agent.core.sandbox.runtime import RuntimeState
from src.server.services import data_token_sweep, toolset
from src.server.services.preview_base import preview_base
from src.server.services.preview_capabilities import proxied, publish

logger = logging.getLogger(__name__)

__all__ = [
    "NOT_RUNNING",
    "BrowserView",
    "ViewState",
    "ask_running",
    "ask_runtime",
    "browser_control",
    "browser_navigate",
    "browser_view",
    "install",
    "install_soon",
]

# How long a view's URL opens a new socket; one already open outlives it, and a
# viewer asks again when it reconnects.
VIEW_LIFETIME_S = 3600
# Chrome's first start on a one-CPU machine, plus the bridge coming up.
_START_WAIT_S = 30
_STATUS_TIMEOUT_S = 15
# The machine's own wait for a page to load, plus the call there and back.
_NAVIGATE_TIMEOUT_S = 45
# The machine's own waits for a person's navigation still running, then the
# agent's command, then the engine to close its browser, plus ending what is
# left: inside the browser's wait for the request.
_CLOSE_TIMEOUT_S = 70
# A first install downloads the engine, about 18 MB.
_INSTALL_TIMEOUT_S = 180
# The one install refusal the person can act on: by installing a Chrome-family
# browser, or asking for Chrome for Testing.
_NO_CHROME: Final[InstallRefusal] = "no_chrome"
# Setups that failed on what can pass, not on the machine itself: an
# unsupported platform or no Chrome stay so until the person acts.
_RETRIED: Final[frozenset[InstallRefusal]] = frozenset({"engine", "ports"})

# Marks a sandbox object whose machine an install was started on.
_INSTALL_STARTED = "_browser_install_started"
# Held so a running install is not collected before it ends.
_INSTALLS: set[asyncio.Task[None]] = set()


class _NotRunning(enum.Enum):
    NOT_RUNNING = "not_running"


#: What :func:`ask_running` answers for a computer it did not reach, which a
#: machine's own answer, JSON or None, never is.
NOT_RUNNING: Final = _NotRunning.NOT_RUNNING

ViewState = Literal[
    "running", "idle", "low_memory", "stopped", "outdated", "no_chrome", "installing", "unavailable"
]


@dataclass(frozen=True)
class BrowserView:
    """``running`` carries the URL its stream opens on and how long that URL opens
    a socket. ``idle`` is a computer with no browser open, ``low_memory`` one
    whose browser a start did not open because memory is short, ``stopped`` a computer
    that is not running, ``outdated`` one built before the browser shipped,
    ``no_chrome`` a machine of the person's own with no Chrome-family browser,
    ``installing`` one getting Chrome for Testing, and ``unavailable`` a kind
    of computer that never has one, or a machine the browser cannot run on.

    ``local_url`` is where an app running on the machine itself opens the same
    stream without the round trip through the server: on a machine of the
    person's own, whose bridge listens on its loopback, for its owner only."""

    state: ViewState
    url: str | None = None
    expires_in: int | None = None
    local_url: str | None = None


async def browser_view(
    sandbox: Any,
    workspace_id: str,
    computer_id: str | None,
    *,
    start: bool = False,
    get_chrome: bool = False,
    viewer_id: str | None = None,
) -> BrowserView:
    """The view of the browser on *sandbox*, opening it first with *start*.
    *computer_id* is the computer it runs, whose row a tool update it starts
    stamps; None, from a row read before the workspace was bound, starts none.
    *viewer_id* is the shared workspace's member the stream is minted for.
    Where the preview proxy runs, their stream ends once they may no longer
    use the workspace; without it, a stream they already opened lasts until it
    drops.

    A machine of the person's own with no browser yet is set up here while they
    wait, and with *get_chrome* starts downloading Chrome for Testing, which
    only they may ask for: it is several hundred MB on their machine.
    """
    try:
        status = await _status(sandbox, start=start)
        if not status.installed and sandbox.installs_browser:
            if get_chrome:
                # Even behind a setup under way, which may be one an acquire
                # began and downloads nothing: the download waits its turn.
                await install(sandbox.runtime, sandbox.layout.internal_src, "chrome")
                return BrowserView("installing")
        if not status.installed and sandbox.installs_browser and not status.installing:
            setup = await install(sandbox.runtime, sandbox.layout.internal_src)
            if setup is None or setup.get("done") is not True:
                reason = setup.get("reason") if setup else None
                return BrowserView("no_chrome" if reason == _NO_CHROME else "unavailable")
            status = await _status(sandbox, start=start)
    except Exception as e:
        # A computer that stopped since the route found it running cannot
        # answer, and that is a stopped computer; one still running that did
        # not answer in time is an error, which the viewer asks again about
        # rather than offering to start a computer that is up.
        if start or not await _stopped(sandbox):
            raise
        logger.debug("Browser status did not answer: %s", type(e).__name__)
        return BrowserView("stopped")
    if status.installing:
        return BrowserView("installing")
    if not status.installed:
        if sandbox.installs_browser:
            return BrowserView("unavailable")
        return BrowserView(
            "installing" if await _updating(sandbox, computer_id, status.toolset) else "outdated"
        )
    if status.low_memory:
        return BrowserView("low_memory")
    if not status.running or status.token is None:
        return BrowserView("idle")
    # Only a machine that sets its browser up itself picks its ports. A cloud
    # image's are fixed, and a port its agent reports otherwise is not one a
    # preview link, which members may hold, should open.
    port = status.port if sandbox.installs_browser else protocol.BRIDGE_PORT
    try:
        url = await _stream_url(sandbox, workspace_id, status.token, port, wake=start, viewer_id=viewer_id)
    except Exception:
        # A computer that stopped since it answered fails the link a read must
        # not wake it for, and that is a stopped computer, not an error.
        if not await _stopped(sandbox):
            raise
        return BrowserView("stopped")
    local = (
        _local_url(port, status.token)
        if sandbox.installs_browser and viewer_id is None
        else None
    )
    return BrowserView("running", url, VIEW_LIFETIME_S, local)


async def _updating(
    sandbox: Any, computer_id: str | None, seen: protocol.Toolset | None
) -> bool:
    """Whether a cloud computer with no browser is getting it from a tool
    update: one under way, or one this starts because its tools are behind the
    server's. Current and still without one, its image predates the browser.
    A read starts only the update the computer's next acquire would start.

    *seen* is what the browser's own status read of the toolset; a runtime
    synced before it reported that says nothing, and root is asked instead.
    """
    if seen is None:
        try:
            answer = await toolset.ask_status(sandbox.runtime, sandbox.layout.internal_src)
        except Exception as e:
            logger.debug("Tool status did not answer: %s", type(e).__name__)
            return False
        if answer is None:
            return False
        seen = protocol.Toolset(answer.version, answer.converging)
    if seen.converging:
        return True
    return computer_id is not None and toolset.converge_soon(sandbox, computer_id, seen.version)


def _local_url(port: int, token: str) -> str:
    """The stream on the machine's own loopback, where its bridge listens."""
    return f"http://127.0.0.1:{port}{protocol.PATH_PREFIX}{token}"


async def install(runtime: Any, internal_src: str, *command: str) -> dict[str, Any] | None:
    """What a machine that sets the browser up itself answers ``install``
    with, or None from one that did not."""
    answer = await ask_runtime(runtime, internal_src, "install", *command, timeout=_INSTALL_TIMEOUT_S)
    return answer if isinstance(answer, dict) else None


def install_soon(sandbox: Any) -> None:
    """Start setting the browser up on *sandbox*'s machine in the background,
    unless its kind has the browser from the image or this sandbox object
    already started it.

    An acquire calls this, so the agent's first browser command finds the
    browser ready without the turn waiting on a download. It is cheap once
    done, and safe from several workers at once: the machine runs one install
    at a time.
    """
    if sandbox is None or not sandbox.installs_browser or getattr(sandbox, _INSTALL_STARTED, False):
        return
    setattr(sandbox, _INSTALL_STARTED, True)
    task = asyncio.create_task(_install_in_background(sandbox))
    _INSTALLS.add(task)
    task.add_done_callback(_INSTALLS.discard)


async def _install_in_background(sandbox: Any) -> None:
    try:
        answer = await install(sandbox.runtime, sandbox.layout.internal_src)
        if answer is None or (answer.get("done") is not True and answer.get("reason") in _RETRIED):
            # A download or the ports may do next time; the next acquire tries.
            setattr(sandbox, _INSTALL_STARTED, False)
            logger.warning(
                f"[browser] setting the browser up on sandbox {getattr(sandbox, 'sandbox_id', None)} failed: {answer!r}"
            )
    except Exception:
        # The Browser tab, or this sandbox's next object, tries again.
        setattr(sandbox, _INSTALL_STARTED, False)
        logger.warning(
            f"[browser] could not set the browser up on sandbox {getattr(sandbox, 'sandbox_id', None)}",
            exc_info=True,
        )


async def _status(sandbox: Any, *, start: bool) -> protocol.Status:
    ask = ("start", str(_START_WAIT_S)) if start else ("status",)
    answer = await _ask(sandbox, *ask, timeout=_STATUS_TIMEOUT_S + (_START_WAIT_S if start else 0))
    return protocol.Status.read(answer)


async def _stopped(sandbox: Any) -> bool:
    """Whether the computer has stopped, as its provider says now, which reads
    it without waking it. Unknown is not stopped."""
    try:
        return await sandbox.runtime.refresh_state() is not RuntimeState.RUNNING
    except Exception:
        return False


async def browser_control(
    sandbox: Any,
    *,
    holder: str,
    action: HoldAction,
    releases: list[dict[str, Any]] | None = None,
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


async def browser_close(sandbox: Any, *, viewer: str) -> protocol.CloseRefusal | None:
    """Close the browser for *viewer*; why it is still open, or None once it is
    closed. The machine refuses while someone else holds the page
    (``browser_runtime.machine.close``); one that does not answer may have
    closed it or not, which the next look tells."""
    try:
        answer = await _ask(sandbox, "close", viewer, timeout=_CLOSE_TIMEOUT_S)
    except Exception as e:
        logger.debug("Browser close did not answer: %s", type(e).__name__)
        return "failed"
    if isinstance(answer, dict) and answer.get("done") is True:
        return None
    reason = answer.get("reason") if isinstance(answer, dict) else None
    return reason if reason in protocol.CLOSE_REFUSALS else "failed"


async def browser_fit(
    sandbox: Any, *, viewer: str, width: int, height: int
) -> tuple[int, int] | Literal["kept"] | None:
    """Size the page to *viewer*'s view of *width* by *height* page pixels; the
    size it took, ``"kept"`` for a page that keeps its own until its browser
    closes, or None for one left as it is for now. The machine decides how large
    and whether at all (``browser_runtime.fit``); a machine whose runtime
    predates the command, or that does not answer, leaves the page as it is."""
    try:
        answer = await _ask(sandbox, "fit", viewer, str(width), str(height), timeout=_STATUS_TIMEOUT_S)
    except Exception as e:
        logger.debug("Browser fit did not answer: %s", type(e).__name__)
        return None
    if not isinstance(answer, dict):
        return None
    if answer.get("kept") is True:
        return "kept"
    taken = answer.get("size")
    if isinstance(taken, list) and len(taken) == 2 and all(type(n) is int for n in taken):
        return taken[0], taken[1]
    return None


async def browser_navigate(
    sandbox: Any, *, holder: str, action: NavigateAction, address: str | None
) -> NavigateRefusal | None:
    """Take the page where *holder*, who holds it, asked; why nothing was done,
    or None once it was."""
    if action == "open":
        address = protocol.typed_address(address or "")
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


async def ask_running(computer: Mapping[str, Any], *command: str, timeout: float) -> object:
    """:func:`ask_runtime` on *computer*'s machine if it is running, or
    :data:`NOT_RUNNING`. A computer that is not running is not started for
    it. Raises for a machine that does not answer. Detached from any session,
    so any worker can ask.
    """
    if not computer.get("provider_ref") or computer.get("status") != "running":
        return NOT_RUNNING
    async with data_token_sweep.running_machine(dict(computer)) as machine:
        if machine is None:
            return NOT_RUNNING
        runtime, internal_src = machine
        return await ask_runtime(runtime, internal_src, *command, timeout=timeout)


async def _ask(sandbox: Any, *command: str, timeout: float) -> object:
    return await ask_runtime(
        sandbox.runtime, sandbox.layout.internal_src, *command, timeout=timeout
    )


async def _stream_url(
    sandbox: Any, workspace_id: str, token: str, port: int, *, wake: bool, viewer_id: str | None
) -> str:
    # A read must not start a computer that stopped since it answered.
    info = await sandbox.get_preview_url(port, VIEW_LIFETIME_S, wake=wake)
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
        info = proxied(await sandbox.get_preview_link(port, wake=wake))
    url = await publish(
        info,
        sandbox_id=sandbox.sandbox_id,
        port=port,
        workspace_id=workspace_id,
        expires_in=VIEW_LIFETIME_S,
        viewer_id=viewer_id,
    )
    # The provider's own query, a signed URL's token, stays on it.
    return urlunsplit(urlsplit(url)._replace(path=protocol.PATH_PREFIX + token, fragment=""))
