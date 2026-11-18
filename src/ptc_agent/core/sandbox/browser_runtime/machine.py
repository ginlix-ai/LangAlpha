"""What the server asks of the machine's browser: ``status`` and ``start`` print
a ``Status``, ``control`` takes, renews or hands back a viewer's hold,
``navigate`` takes the page somewhere for the viewer holding it, ``quality``
keeps the computer's choice of live view quality, ``serve`` runs the bridge and
``proxy`` the filter the browser's requests go through. Each that answers does
so with one line of JSON."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from . import care, daemon, hold, launch, proxy, quality, relay, wrapper
from .protocol import (
    ENGINE_BIN,
    HOLD_ACTIONS,
    NAVIGATE_ACTIONS,
    STREAM_PORT,
    TOKEN_FILE,
    NavigateRefusal,
    Status,
    page_address,
    release_commands,
    state_file,
)

# A read: it starts the browser on its blank page and takes it nowhere, so one
# that lands after the agent's own first command leaves the agent's page alone.
_ENGINE_START_ARGS = ("tab", "list")
# The engine's own wait for a page to load, with room to answer.
_NAVIGATE_WAIT_S = 30
# Letting go of what a person had down, all of it inside the server's wait for
# the hand-back.
_RELEASES_WAIT_S = 8.0
# What the bridge takes to come up after ``ensure`` starts it.
_BRIDGE_UP_S = 2.0
_POLL_S = 0.05


def status(wait_s: float = 0.0) -> Status:
    """Whether the browser is up, and the token that opens its stream.

    A stream with no bridge in front of it is a browser opened some way that
    did not run ``ensure``, so asking starts the bridge rather than reporting a
    browser no one can watch.
    """
    deadline = time.monotonic() + wait_s
    running = _browser_up(deadline) and relay.ensure(
        max(deadline - time.monotonic(), _BRIDGE_UP_S)
    )
    token = None
    if running:
        try:
            token = state_file(TOKEN_FILE).read_text().strip() or None
        except OSError:
            token = None
    return Status(os.access(ENGINE_BIN, os.X_OK), token is not None, token)


def start(wait_s: float) -> Status:
    """Bring the browser up if it is not, and say where it stands.

    Only then, and one the agent's own command is still starting is waited for
    rather than started twice. With memory short the wrapper would refuse the
    open, so it is not tried and the answer says why.
    """
    if os.access(ENGINE_BIN, os.X_OK) and not care.browser_up() and not care.engine_busy():
        if care.memory_full():
            return Status(installed=True, low_memory=True)
        # No browser is up, so a hold is a closed one's: a take that raced its
        # close would otherwise refuse this open until the lease ran out.
        hold.forget()
        hold.mark_seen()
        # A start that loses to the agent's own is harmless: both reach one daemon.
        daemon.detach("agent-browser", *_ENGINE_START_ARGS)
    return status(wait_s)


def _browser_up(deadline: float) -> bool:
    """Whether Chrome is up behind the engine's stream, waiting until *deadline*.

    The stream is the engine's daemon, which outlives a Chrome the OOM killer
    took, so a listening port alone would show a person a frozen page and keep
    their start from bringing a browser back.
    """
    while not (care.browser_up() and daemon.listening(STREAM_PORT)):
        if time.monotonic() >= deadline:
            return False
        time.sleep(_POLL_S)
    return True


def navigate(holder: str, action: str, address: str = "") -> dict[str, object]:
    """Take the page to *address*, back, forward or to itself again, for the person holding it.

    Only for a holder, since the agent's commands are refused for as long as one
    holds the page, and only on an open browser, run with the launch it already
    has, so this neither starts one outside the wrapper's terms nor restarts it.
    The proxy checks the address as it checks any of the browser's requests.
    """
    if not hold.holds(holder):
        return _refused("not_held")
    if not (care.browser_up() and daemon.listening(STREAM_PORT)):
        # A daemon whose Chrome died would launch another on this command,
        # outside the wrapper's memory check.
        return _refused("not_running")
    args = [action]
    if action == "open":
        url = page_address(address)
        if url is None:
            return _refused("bad_address")
        args.append(url)
    launched = launch.current()
    if launched is None:
        # A browser no command of the wrapper's opened, whose launch is unknown,
        # so any command could restart it.
        return _refused("failed")
    env = wrapper.engine_env(os.environ, launched)
    try:
        with hold.navigating():
            # Another viewer's navigation may have held the lock past this
            # viewer's hand-back, or past its lease.
            if not hold.holds(holder):
                return _refused("not_held")
            # Nor past the browser's own end, after which this would open another.
            if not (care.browser_up() and daemon.listening(STREAM_PORT)):
                return _refused("not_running")
            try:
                moved = _engine(env, *args)
            finally:
                # However the engine ended, the page may have changed after the
                # person handed it back, which the take that began their hold has
                # not told the agent.
                hold.moved()
            if action != "open":
                # The engine goes back, forward or reloads by running script in the
                # page, which fails when the page leaves under it, so its exit says
                # nothing. Reading the address is what does, and the tab list the
                # engine sends after a command carries the page's new address only
                # once the next one has run, so the panel's address follows it too.
                moved = _engine(env, "get", "url")
    except subprocess.TimeoutExpired:
        # A page slow to finish loading is still the page the browser is on.
        return {"done": True}
    except OSError:
        return _refused("failed")
    return {"done": True} if moved else _refused("failed")


def control(holder: str, action: str, releases: list[list[str]] | None = None) -> bool:
    """Take, renew or hand back *holder*'s hold; whether it holds the page now.

    A hold is on a page, so none is taken or kept on a browser that has closed:
    it would only keep the agent from opening the next one. A hand-back first
    lets go of *releases*, the engine commands for what the person still had
    down, while the hold still keeps the agent's commands off the page. One that
    could not is kept, so the agent waits while the viewer asks again rather
    than acting on a page with the key still down.
    """
    if action != "release" and not care.browser_up():
        action = "release"
    if action == "release" and releases and not _let_go(holder, releases):
        return hold.holds(holder)
    return hold.control(holder, action)


def _let_go(holder: str, releases: list[list[str]]) -> bool:
    """Whether nothing of *releases* is left down: each let go of, or no page
    of this holder's to let go of it on."""
    launched = launch.current()
    if launched is None:
        return True
    env = wrapper.engine_env(os.environ, launched)
    deadline = time.monotonic() + _RELEASES_WAIT_S
    # Taken as a navigation is: an agent command waits for it, so it never runs
    # on a page that still has the person's key down.
    with hold.navigating():
        # Only into this holder's page: one whose hold lapsed is the agent's
        # again, and a browser that ended would start another for the engine.
        if not (hold.holds(holder) and care.browser_up() and daemon.listening(STREAM_PORT)):
            return True
        for args in releases:
            left = deadline - time.monotonic()
            try:
                if left <= 0 or not _engine(env, *args, timeout=left):
                    return False
            except (subprocess.TimeoutExpired, OSError):
                return False
    return True


def _refused(reason: NavigateRefusal) -> dict[str, object]:
    return {"done": False, "reason": reason}


def _engine(env: dict[str, str], *args: str, timeout: float = _NAVIGATE_WAIT_S) -> bool:
    result = subprocess.run(
        [ENGINE_BIN, *args],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd="/",
        timeout=timeout,
    )
    return result.returncode == 0


def main(argv: list[str]) -> int:
    command, args = (argv[0], argv[1:]) if argv else ("", [])
    if command in ("status", "start"):
        wait_s = float(args[0]) if args else 0.0
        print(json.dumps((status if command == "status" else start)(wait_s).to_wire()))
        return 0
    if command == "control" and len(args) in (2, 3) and hold.valid_holder(args[0]) and args[1] in HOLD_ACTIONS:
        releases = _releases(args[2]) if len(args) == 3 and args[1] == "release" else None
        if len(args) == 2 or releases is not None:
            print(json.dumps({"held": control(args[0], args[1], releases)}))
            return 0
    if (
        command == "navigate"
        and len(args) in (2, 3)
        and hold.valid_holder(args[0])
        and args[1] in NAVIGATE_ACTIONS
    ):
        print(json.dumps(navigate(args[0], args[1], args[2] if len(args) == 3 else "")))
        return 0
    if command == "quality" and len(args) == 1:
        print(json.dumps({"done": quality.choose(args[0])}))
        return 0
    if command == "serve":
        return relay.serve()
    if command == "proxy":
        return proxy.serve()
    return _usage()


def _releases(text: str) -> list[list[str]] | None:
    try:
        return release_commands(json.loads(text))
    except ValueError:
        return None


def _usage() -> int:
    print(
        f"usage: python3 -I {Path(__file__).resolve().parent.name} agent-browser [argument ...]"
        " | status [wait seconds] | start [wait seconds] | control <holder> take|renew|release [releases]"
        " | navigate <holder> open <address>|back|forward|reload"
        " | quality light|standard|sharp|retina|default | serve | proxy",
        file=sys.stderr,
    )
    return 2
