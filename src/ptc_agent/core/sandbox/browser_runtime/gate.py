"""What runs ahead of every browser command: it starts the proxy the browser's
requests go through and the bridge if they are not up, tends the browser's
processes, settles the launch the browser runs with, and refuses a command while
the person holds the page or memory is short."""

from __future__ import annotations

import fcntl
from typing import IO

from . import care, daemon, hold, launch, proxy, quality, relay
from .launch import Launch
from .protocol import MEMORY_MESSAGE, STREAM_PORT, USER_AGENT_MESSAGE, state_file
_admission: IO[str] | None = None


class Refused(Exception):
    """The gate's own refusal, the one failure here that stops a command."""


def check(use: str, user_agent: str = "") -> Launch:
    """The launch a command must run with.

    ``look`` only reads the page, ``act`` changes it, ``grow`` opens another tab
    or runs script that can, ``close`` ends the browser, ``closes`` acts on it and
    then ends it, and ``reopen`` ends it and opens another. An open browser keeps
    the launch it started with; for one this command starts, a user agent asked
    for now or earlier holds until a ``close``, and the quality is the one chosen
    now.
    """
    _admit()
    care.end_browserless(admitting=True)
    up = daemon.listening(STREAM_PORT)
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
    if not up and care.engine_gone(admitting=True):
        # No browser and no engine that may be starting one: the last browser
        # closed, on its idle timeout or with the computer, and nothing set for
        # it carries over to the next. The bridge says nothing of that, since it
        # outlives the browser by its grace and dies under a live one.
        hold.forget()
        launch.forget()
    care.tend(up, admitting=True)
    # Tending ends a daemon whose Chrome died since the look above, and its
    # stream with it, so this command may be the one that opens a browser.
    up = up and daemon.listening(STREAM_PORT)
    record = launch.current()
    # A reopen's browser starts after its own close, on a launch of its own.
    starts = not up or use == "reopen"
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
    settled = Launch(
        user_agent if use == "reopen" else user_agent or held_agent,
        quality.chosen() if starts or not record else record.quality,
        closing=use in ("close", "closes"),
    )
    if settled != record:
        launch.write(settled)
    return settled


def _admit() -> None:
    """Admit one command at a time, so two arriving together cannot each wait
    on the other to end a browser whose Chrome died, and both run on its stale
    stream. Held until the command becomes the engine's: the file is not
    inherited, so the exec closes it, and the engine's command counts as busy."""
    global _admission
    if _admission is not None:
        return
    lock = state_file(care.ADMISSION_LOCK)
    lock.parent.mkdir(parents=True, exist_ok=True)
    admission = open(lock, "a")
    fcntl.flock(admission, fcntl.LOCK_EX)
    _admission = admission
