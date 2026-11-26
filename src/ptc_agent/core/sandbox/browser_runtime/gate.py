"""What runs ahead of every browser command: it starts the proxy the browser's
requests go through and the bridge if they are not up, tends the browser's
processes, settles the launch the browser runs with, and refuses a command while
the person holds the page or memory is short."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import subprocess
import sys
from collections.abc import Callable
from urllib.parse import urlsplit

from . import care, daemon, engine, hold, launch, proxy, quality, relay, signin, window
from .care import Look
from .host import Host
from .launch import Launch
from .protocol import MEMORY_MESSAGE, REOPEN_MESSAGE, TABS_CLOSED_MESSAGE, USER_AGENT_MESSAGE, page_address

#: How long the page goes unused, by the agent's commands and the person alike,
#: before the next command moves the browser onto the engine a tool update
#: installed since it started. A pause stands in for the end of a turn, which the
#: machine does not see: long enough that a turn waiting on a subagent or a long
#: command keeps its browser, well short of its idle close.
ENGINE_MOVE_AFTER_S = 300.0
#: What the engine is given to close its browser, or to open the page again.
_ENGINE_STEP_S = 20.0


class Refused(Exception):
    """The gate's own refusal, the one failure here that stops a command."""


def check(use: str, user_agent: str = "", *, opens_page: bool = False) -> tuple[Launch, Callable[[], None]]:
    """The launch a command must run with, and what lets the next command in.
    *opens_page* is a command that goes to an address before anything else,
    which a browser moved to a new engine opens instead of the page it showed.

    One command is admitted at a time, so two arriving together cannot each
    wait on the other to end a browser whose Chrome died, and both run on its
    stale stream. The admission holds until the command becomes the engine's
    (``care.Admission``), or until one the wrapper runs beside the engine lets
    it go once the engine's command has started.
    """
    admission = care.admit()
    try:
        # Read before this command is noted, which starts the pause again.
        quiet_s = care.quiet_for()
        care.note_use()
        seen = admission.look()
        record = _pin_running_engine(seen, launch.current())
        seen, moved = _move_engine(seen, record, use, quiet_s)
        # The browser the move closed starts on the new engine with this
        # command, on its user agent unless the command asks for another.
        settled = _settle(seen, use, user_agent or moved.user_agent)
        if moved.closed:
            # Before the command's own output, so the agent reads it first.
            print(TABS_CLOSED_MESSAGE.format(", ".join(moved.closed)), file=sys.stderr)
        if moved.address and not opens_page:
            _open_again(settled, seen.host, moved.address)
        return settled, admission.release
    except BaseException:
        admission.release()
        raise


def _settle(seen: Look, use: str, user_agent: str) -> Launch:
    """``look`` only reads the page, ``act`` changes it, ``grow`` opens another tab
    or runs script that can, ``close`` ends the browser, ``closes`` acts on it and
    then ends it, and ``reopen`` ends it and opens another. An open browser keeps
    the launch it started with; for one this command starts, a user agent asked
    for now or earlier holds until a ``close``, and the quality, whether
    sign-ins are kept and whether it shows a window are the ones chosen now.
    """
    stream = seen.host.stream_port
    # Whether the stream listens, before tending and after: the daemon behind
    # it may be up with no Chrome, which only tending finds and ends.
    streaming = daemon.listening(stream)
    # A start that cannot spawn costs the browser its network or the view its
    # stream, never the checks below: the hold and memory still decide.
    try:
        proxy.ensure()
    except OSError:
        pass
    try:
        relay.ensure()
    except OSError:
        pass
    seen = seen.tend(streaming)
    # Tending ends a daemon whose Chrome died, and its stream with it, so this
    # command may be the one that opens a browser. Probed again either way: one
    # a command already running is starting may have begun listening meanwhile.
    up = daemon.listening(stream)
    pending = launch.current()
    if not up and pending is not None and not pending.closing and not seen.engine_gone():
        # The engine's command the reading showed may have ended since without
        # opening a browser, which this command would otherwise join below,
        # skipping the memory check and reusing its launch. Read after the
        # probe, so one that opened a browser meanwhile still shows, and is joined.
        seen = seen.reread()
    if not up and seen.engine_gone():
        # No browser and no engine that may be starting one: the last browser
        # closed, on its idle timeout or with the computer, and nothing set for
        # it carries over to the next. The bridge says nothing of that, since it
        # outlives the browser by its grace and dies under a live one.
        care.forget_browser()
    record = launch.current()
    # A browser the engine is still starting for a command admitted before this
    # one is not up yet, but this command joins it rather than starting another:
    # the launch on record is the one it starts with, which the engine would
    # restart it to change. One being closed is not joined.
    joins = not up and record is not None and not record.closing and not seen.engine_gone()
    # A reopen's browser starts after its own close, on a launch of its own.
    starts = (not up and not joins) or use == "reopen"
    # A browser that closed as its last command meant took its user agent along.
    held_agent = record.user_agent if record and not (starts and record.closing) else ""
    if not starts and user_agent and user_agent != held_agent:
        raise Refused(USER_AGENT_MESSAGE)
    refused = hold.refusal(use)
    if refused:
        raise Refused(refused)
    # Any command but ``close`` opens a browser that is not up, reading or not.
    if (use == "grow" or (use != "close" and starts)) and care.memory_full():
        raise Refused(MEMORY_MESSAGE)
    # A close runs on the launch it found too, so one that never finishes leaves
    # the open browser what it runs with, and the next command no restart.
    found = record if record and not starts else _chosen(seen.host)
    settled = dataclasses.replace(
        found,
        user_agent=user_agent if use == "reopen" else user_agent or held_agent,
        closing=use in ("close", "closes"),
    )
    if settled.kept and not up and not joins:
        seen.ready_profile()
    if settled != record:
        launch.write(settled)
    return settled


def _pin_running_engine(seen: Look, record: Launch | None) -> Launch | None:
    """*record* pinned to its daemon's engine (``Look.pin_engine``), recorded so
    every later command and the viewer's own find it pinned."""
    pinned = seen.pin_engine(record)
    if pinned is not None and pinned != record:
        launch.write(pinned)
    return pinned


#: What a browser shows with no page in it, which a move loses nothing of.
_BLANK_PAGES = frozenset({"", "about:blank"})
#: Every tab of the open browser, each with its address and whether it shows.
_TABS = ("--json", "tab", "list")


@dataclasses.dataclass(frozen=True)
class _Moved:
    """What a browser closed to move engines carries to the one opened next:
    the page it showed, opened again, and the other tabs it had, which the
    agent is told were closed."""

    user_agent: str = ""
    address: str = ""
    closed: tuple[str, ...] = ()


def _move_engine(seen: Look, record: Launch | None, use: str, quiet_s: float) -> tuple[Look, _Moved]:
    """Close a browser still on an engine a tool update has replaced, once its
    page has gone a while unused, so this command opens it on the new
    one with its user agent and page. Not while the person holds the page or
    another command runs, nor for a command that closes or reopens it anyway,
    nor while memory is short: the browser this command opens would be
    refused, and the one closed for it lost with its page. Nor while the person
    has taken the page somewhere the agent has not read since: closing it
    forgets that, and the agent's next action would run on a page it has not
    seen. The browser then stays on the engine it has, which an update keeps.
    """
    if (
        record is None
        or record.closing
        or use in ("close", "closes", "reopen")
        or not engine.stale(record, seen.host)
        or quiet_s < ENGINE_MOVE_AFTER_S
        or hold.held()
        or hold.unseen()
        or seen.busy()
        or not seen.browser_up()
        or care.memory_full()
    ):
        return seen, _Moved()
    # This command's own use, noted as it was admitted.
    noted = care.used_at()
    tabs = _tabs(record, seen.host)
    address = _reopenable(tabs[0] if tabs else None)
    with hold.moving():
        if (
            address is None
            or hold.held()
            or care.used_at() != noted
            or care.memory_full()
        ):
            # A page it cannot name, or names at an address it could not open
            # again, it would lose, so the browser stays where it is for now;
            # as it does for a person who took or used the page while it was
            # read, and when memory has since filled, which would refuse the
            # browser opened for it.
            return seen, _Moved()
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            # The engine's own close first, so Chrome writes a kept profile's
            # sign-ins back before it goes; whatever is left is ended after.
            record.run(seen.host, "close", timeout=_ENGINE_STEP_S)
        seen = seen.reread()
        seen = seen.close(seen.daemons() + seen.browsers())
        care.forget_browser()
    closed = tuple(url for url in tabs[1] if url not in _BLANK_PAGES)
    return seen, _Moved(record.user_agent, address, closed)


def _tabs(record: Launch, h: Host) -> tuple[str, list[str]] | None:
    """The address the open browser shows and those of its other tabs, or
    None when they cannot be read."""
    try:
        printed = record.run(h, *_TABS, timeout=_ENGINE_STEP_S)
        pages = [tab for tab in json.loads(printed)["data"]["tabs"] if tab.get("type", "page") == "page"]
        shown = next(tab["url"] for tab in pages if tab.get("active"))
        others = [tab["url"] for tab in pages if not tab.get("active")]
    except (OSError, subprocess.SubprocessError, TypeError, ValueError, KeyError, AttributeError, StopIteration):
        return None
    if not isinstance(shown, str) or not all(isinstance(url, str) for url in others):
        return None
    return shown, others


def _reopenable(shown: str | None) -> str | None:
    """The address a moved browser opens again for the page it *shown*, empty
    for a blank one, or None for one it could not: no address read, or one
    that is no web page (``data:``, ``blob:``, ``file:``), which
    ``page_address`` would otherwise read as a host name typed bare."""
    if shown is None:
        return None
    if shown in _BLANK_PAGES:
        return ""
    try:
        scheme = urlsplit(shown).scheme.lower()
    except ValueError:
        return None
    return page_address(shown) if scheme in ("http", "https") else None


def _open_again(settled: Launch, h: Host, address: str) -> None:
    """Open the page the moved browser showed, before the command that finds
    it there. One that does not open refuses the command, which would
    otherwise run on a page the agent never saw, and names the address so the
    agent can open it again; the browser is on the new engine by then, so the
    command repeated runs."""
    try:
        opened = settled.run(h, "open", address, timeout=_ENGINE_STEP_S)
    except (OSError, subprocess.SubprocessError):
        opened = None
    if opened is None:
        raise Refused(REOPEN_MESSAGE.format(address))


def _chosen(h: Host) -> Launch:
    """The launch the computer's choices give a browser starting now, on the
    engine's newest version, which it keeps until it closes."""
    return Launch(
        "",
        quality.chosen(),
        kept=signin.kept(),
        window=window.chosen(),
        engine=engine.pinned(h),
    )
