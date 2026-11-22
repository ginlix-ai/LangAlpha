"""What runs ahead of every browser command: it starts the proxy the browser's
requests go through and the bridge if they are not up, tends the browser's
processes, settles the launch the browser runs with, and refuses a command while
the person holds the page or memory is short."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable

from . import care, daemon, hold, launch, proxy, quality, relay, signin, window
from .care import Look
from .launch import Launch
from .protocol import MEMORY_MESSAGE, USER_AGENT_MESSAGE


class Refused(Exception):
    """The gate's own refusal, the one failure here that stops a command."""


def check(use: str, user_agent: str = "") -> tuple[Launch, Callable[[], None]]:
    """The launch a command must run with, and what lets the next command in.

    One command is admitted at a time, so two arriving together cannot each
    wait on the other to end a browser whose Chrome died, and both run on its
    stale stream. The admission holds until the command becomes the engine's
    (``care.Admission``), or until one the wrapper runs beside the engine lets
    it go once the engine's command has started.
    """
    admission = care.admit()
    try:
        return _settle(admission.look(), use, user_agent), admission.release
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
    found = record if record and not starts else _chosen()
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


def _chosen() -> Launch:
    """The launch the computer's choices give a browser starting now."""
    return Launch("", quality.chosen(), kept=signin.kept(), window=window.chosen())
