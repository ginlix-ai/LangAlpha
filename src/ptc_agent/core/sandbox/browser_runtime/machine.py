"""What the server asks of the machine's browser: ``status`` and ``start`` print
a ``Status``, ``control`` takes, renews or hands back a viewer's hold,
``navigate`` takes the page somewhere for the viewer holding it, ``fit`` sizes
the page to a viewer's view, ``close`` closes the browser for a viewer, ``choices``
keeps the computer's choices for the browsers it starts (the live view's
quality, whether one shows as a window on a Mac's screen, whether sign-ins are
kept), ``install`` sets the browser up on a person's own machine, ``everyday``
lists its everyday browsers and brings one's sign-ins into the kept profile,
``serve`` runs the bridge and ``proxy`` the filter the browser's requests go
through. Each that answers does so with one line of JSON."""

from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

from . import care, choices, daemon, everyday, fit, gate, hold, host, install, launch, proxy, relay, signin
from .protocol import (
    FOR_PERSON_ENV,
    HOLD_ACTIONS,
    NAVIGATE_ACTIONS,
    TOKEN_FILE,
    Choices,
    CloseRefusal,
    NavigateRefusal,
    Status,
    installing,
    page_address,
    release_commands,
    state_file,
    toolset_seen,
)

# A read: it starts the browser on its blank page and takes it nowhere, so one
# that lands after the agent's own first command leaves the agent's page alone.
_ENGINE_START_ARGS = ("tab", "list")
# The engine's own wait for a page to load, with room to answer: one for all of
# a navigation's commands, since a close waits for them (``hold.navigating``)
# inside the server's wait.
_NAVIGATE_WAIT_S = 30
# What the engine is given to close its browser before what is left is ended.
_CLOSE_WAIT_S = 20.0
# How long a close waits for the agent's command still running to end, so it
# lands between the agent's commands, and how often it looks; past that it
# closes the browser under the command. Inside the server's wait with the above.
_CLOSE_COMMAND_WAIT_S = 10.0
_CLOSE_COMMAND_POLL_S = 0.25
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
    launched = launch.current() if token else None
    h = host.current()
    ready = h.ready()
    return Status(
        ready,
        token is not None,
        token,
        kept=bool(launched and launched.kept),
        port=h.bridge_port,
        installing=not ready and installing(),
        # Only where the server would ask next: a cloud computer with no
        # browser may be getting one from a toolset update. A person's own
        # machine sets its browser up itself and has no toolset to report.
        toolset=toolset_seen() if not (ready or h.own) else None,
    )


def start(wait_s: float) -> Status:
    """Bring the browser up if it is not, and say where it stands.

    Only then, and one the agent's own command is still starting is waited for
    rather than started twice. With memory short the wrapper would refuse the
    open, so it is not tried and the answer says why.
    """
    # Before anything is read: a close the person asks for from here on finds
    # no browser yet, and is what keeps this one from opening (``gate.check``).
    closes = gate.person_closes()
    seen = care.look()
    if seen.host.ready() and not seen.browser_up() and not seen.busy():
        if care.memory_full():
            return Status(installed=True, low_memory=True)
        # No browser is up, so a hold is a closed one's: a take that raced its
        # close would otherwise refuse this open until the lease ran out.
        hold.forget()
        hold.mark_seen()
        # A start that loses to the agent's own is harmless: both reach one daemon.
        daemon.detach("agent-browser", *_ENGINE_START_ARGS, env={FOR_PERSON_ENV: str(closes)})
    return status(wait_s)


def _browser_up(deadline: float) -> bool:
    """Whether the browser is live (``Look.live``), waiting until *deadline*."""
    while not care.look().live():
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
    seen = care.look()
    if not seen.live():
        return _refused("not_running")
    args = [action]
    if action == "open":
        url = page_address(address)
        if url is None:
            return _refused("bad_address")
        args.append(url)
    try:
        with hold.navigating():
            # Another viewer's navigation may have held the lock past this
            # viewer's hand-back, or past its lease.
            if not hold.holds(holder):
                return _refused("not_held")
            # Nor past the browser's own end, after which this would open another.
            seen = care.look()
            if not seen.live():
                return _refused("not_running")
            # Read under the lock, so a save that ended the browser meanwhile
            # (``choose``) leaves this no launch of the closed one's to run. On
            # its daemon's engine when it pins none, or the host's new one
            # would restart it under the person.
            launched = seen.pin_engine(launch.current())
            if launched is None:
                # A browser no command of the wrapper's opened, whose launch is
                # unknown, so any command could restart it.
                return _refused("failed")
            deadline = time.monotonic() + _NAVIGATE_WAIT_S
            try:
                moved = launched.run(seen.host, *args, timeout=_NAVIGATE_WAIT_S) is not None
            finally:
                # However the engine ended, the page may have changed after the
                # person handed it back, which the take that began their hold has
                # not told the agent.
                hold.moved()
                care.note_use()
            if action != "open":
                # The engine goes back, forward or reloads by running script in the
                # page, which fails when the page leaves under it, so its exit says
                # nothing. Reading the address is what does, and the tab list the
                # engine sends after a command carries the page's new address only
                # once the next one has run, so the panel's address follows it too.
                left = deadline - time.monotonic()
                if left <= 0:
                    # Slow to finish loading, as below.
                    return {"done": True}
                moved = launched.run(seen.host, "get", "url", timeout=left) is not None
    except subprocess.TimeoutExpired:
        # A page slow to finish loading is still the page the browser is on.
        return {"done": True}
    except OSError:
        return _refused("failed")
    return {"done": True} if moved else _refused("failed")


def close(viewer: str) -> dict[str, object]:
    """Close the browser for *viewer*, as the agent's ``close`` would, unless
    someone else holds its page; a browser already closed is closed.

    Under the gate's admission, so no command of the agent's starts meanwhile,
    after the one already running ends, a while at most, and under the hold's
    locks in the gate's order, so no take or navigation lands on the browser as
    it goes. The engine's close first, so Chrome writes a kept profile's
    sign-ins back, then whatever is left, as a tool update closes one
    (``gate._move_engine``); what lasted as long as the browser goes with it,
    the viewer's own hold among it.
    """
    with care.admitted() as seen, hold.moving(), hold.navigating():
        if hold.held_by_other(viewer):
            return _close_refused("held")
        gate.note_close_asked()
        deadline = time.monotonic() + _CLOSE_COMMAND_WAIT_S
        while seen.busy() and time.monotonic() < deadline:
            time.sleep(_CLOSE_COMMAND_POLL_S)
            seen = seen.reread()
        was_up = seen.browser_up()
        seen = seen.shut(seen.pin_engine(launch.current()), timeout=_CLOSE_WAIT_S)
        # Any Chrome left, run by its daemon or outliving it (``Look.shut``).
        if seen.browsers():
            return _close_refused("failed")
        # The agent hears of a browser the person closed, not of one already gone.
        if was_up:
            gate.note_person_closed()
    return {"done": True}


def _close_refused(reason: CloseRefusal) -> dict[str, object]:
    return {"done": False, "reason": reason}


def control(holder: str, action: str, releases: list[list[str]] | None = None) -> bool:
    """Take, renew or hand back *holder*'s hold; whether it holds the page now.

    A hold is on a page, so none is taken or kept on a browser that has closed:
    it would only keep the agent from opening the next one. A hand-back first
    lets go of *releases*, the engine commands for what the person still had
    down, while the hold still keeps the agent's commands off the page. One that
    could not is kept, so the agent waits while the viewer asks again rather
    than acting on a page with the key still down.
    """
    if action == "take":
        # Waits out a tool update closing the browser, then finds it closed.
        with hold.moving():
            return _control(holder, action, releases)
    return _control(holder, action, releases)


def _control(holder: str, action: str, releases: list[list[str]] | None) -> bool:
    if action != "release" and not care.look().browser_up():
        action = "release"
    # The person's use of the page, held or handed back, keeps a tool update
    # from moving the browser under them or just after.
    care.note_use()
    if action == "release" and releases and not _let_go(holder, releases):
        return hold.holds(holder)
    return hold.control(holder, action)


def _let_go(holder: str, releases: list[list[str]]) -> bool:
    """Whether nothing of *releases* is left down: each let go of, or no page
    of this holder's to let go of it on."""
    deadline = time.monotonic() + _RELEASES_WAIT_S
    # Taken as a navigation is: an agent command waits for it, so it never runs
    # on a page that still has the person's key down, and so does a save that
    # would end the browser (``choose``).
    with hold.navigating():
        # Only into this holder's page: one whose hold lapsed is the agent's
        # again, and a browser that ended would start another for the engine.
        seen = care.look()
        launched = seen.pin_engine(launch.current())
        if launched is None or not (hold.holds(holder) and seen.live()):
            return True
        for args in releases:
            left = deadline - time.monotonic()
            try:
                if left <= 0 or launched.run(seen.host, *args, timeout=left) is None:
                    return False
            except (subprocess.TimeoutExpired, OSError):
                return False
    return True


def choose(chosen: Choices) -> bool:
    """Keep *chosen* for the browsers the computer starts from now, unless the
    machine holds a later save; whether it holds this save or a later one.

    Under the admission lock, so no command starts a browser on some of one
    save's choices, or on the kept profile while it is erased. A save whose
    sign-ins revision is newer than the one held erases that profile, ending a
    browser open on it at once, whoever is using it: the person asked for their
    sign-ins gone, and a browser left open would write them back as it closed.
    An erase that fails keeps nothing of the save, so the server hands it again.
    """
    with care.admitted() as seen:
        held = choices.held()
        ours = held is not None and held.computer == chosen.computer
        if ours and chosen.revision < held.revision:
            # It reached the machine after a later save, which stands.
            return True
        if not ours or chosen.signins_revision > held.signins_revision:
            # A person's navigation, or the hand-back's releases, runs the engine
            # outside the gate, and would start a browser on the profile being
            # erased if this ended the open one under it. Taken after admission,
            # in the gate's own order (``hold.refusal``), and the processes read
            # again, since the wait can be long.
            with hold.navigating():
                seen.reread().end_on_profile()
                try:
                    signin.erase()
                except OSError:
                    return False
        choices.keep(chosen)
    return True


def _refused(reason: NavigateRefusal) -> dict[str, object]:
    return {"done": False, "reason": reason}


def _wait_s(args: list[str]) -> float | None:
    if len(args) > 1:
        return None
    try:
        wait_s = float(args[0]) if args else 0.0
    except ValueError:
        return None
    # A wait that never ends, or never compares, would poll for good.
    return wait_s if math.isfinite(wait_s) else None


def _status_command(args: list[str]) -> object | None:
    wait_s = _wait_s(args)
    return None if wait_s is None else status(wait_s).to_wire()


def _start_command(args: list[str]) -> object | None:
    wait_s = _wait_s(args)
    return None if wait_s is None else start(wait_s).to_wire()


def _control_command(args: list[str]) -> object | None:
    if len(args) not in (2, 3) or not hold.valid_holder(args[0]) or args[1] not in HOLD_ACTIONS:
        return None
    releases = None
    if len(args) == 3:
        # Releases ride only a hand-back.
        releases = _releases(args[2]) if args[1] == "release" else None
        if releases is None:
            return None
    return {"held": control(args[0], args[1], releases)}


def _navigate_command(args: list[str]) -> object | None:
    if len(args) not in (2, 3) or not hold.valid_holder(args[0]) or args[1] not in NAVIGATE_ACTIONS:
        return None
    return navigate(args[0], args[1], args[2] if len(args) == 3 else "")


def _close_command(args: list[str]) -> object | None:
    if len(args) != 1 or not hold.valid_holder(args[0]):
        return None
    return close(args[0])


def _fit_command(args: list[str]) -> object | None:
    if len(args) != 3 or not hold.valid_holder(args[0]):
        return None
    try:
        width, height = int(args[1]), int(args[2])
    except ValueError:
        return None
    taken = fit.fit(args[0], width, height)
    if taken == fit.KEPT:
        return {"kept": True}
    return {"size": list(taken) if taken else None}


def _choices_command(args: list[str]) -> object | None:
    if len(args) != 1:
        return None
    # Read whole before any is kept, so a malformed one changes nothing.
    chosen = Choices.from_wire(args[0])
    if chosen is None:
        return {"done": False}
    return {"done": choose(chosen)}


def _install_command(args: list[str]) -> object | None:
    if args == []:
        return install.install()
    if args == ["chrome", "--wait"]:
        return install.install(chrome=True)
    if args == ["chrome"]:
        install.get_chrome()
        return {"done": True}
    return None


def _everyday_command(args: list[str]) -> object | None:
    if args == ["list"]:
        return {"sources": [s.to_wire() for s in everyday.sources()]}
    if len(args) == 3 and args[0] == "bring":
        return everyday.bring_signins(args[1], args[2], start)
    return None


#: Each command that answers: its handler checks its arguments and returns the
#: answer, or None for the usage.
_COMMANDS: dict[str, Callable[[list[str]], object | None]] = {
    "status": _status_command,
    "start": _start_command,
    "control": _control_command,
    "navigate": _navigate_command,
    "fit": _fit_command,
    "close": _close_command,
    "choices": _choices_command,
    "install": _install_command,
    "everyday": _everyday_command,
}


def main(argv: list[str]) -> int:
    command, args = (argv[0], argv[1:]) if argv else ("", [])
    # The bridge and the proxy run as processes of their own, until they end.
    if command == "serve":
        return relay.serve()
    if command == "proxy":
        return proxy.serve()
    handler = _COMMANDS.get(command)
    answer = handler(args) if handler else None
    if answer is None:
        return _usage()
    print(json.dumps(answer))
    return 0


def _releases(text: str) -> list[list[str]] | None:
    try:
        return release_commands(json.loads(text))
    except ValueError:
        return None


def _usage() -> int:
    print(
        f"usage: python3 -I {Path(__file__).resolve().parent.name} agent-browser [argument ...]"
        " | status [wait seconds] | start [wait seconds] | control <holder> take|renew|release [releases]"
        " | navigate <holder> open <address>|back|forward|reload | fit <viewer> <width> <height> | close <viewer>"
        ' | choices \'{"quality": light|standard|sharp|retina|null, "window": bool,'
        ' "signins": bool, "signins_revision": <revision>, "revision": <revision>}\''
        " | install [chrome [--wait]] | everyday list | everyday bring <browser> <profile>"
        " | serve | proxy",
        file=sys.stderr,
    )
    return 2
