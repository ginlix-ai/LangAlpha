"""Which file the engine runs from.

On a cloud computer the host's engine is a link that a tool update moves to a
new version installed beside the last (``<unit>/<version>/<file>``), while an
open browser keeps the version it started on: a command on another would
restart it under it. On a person's own machine the engine is the file itself,
so nothing is pinned and only that file counts.
"""

from __future__ import annotations

import glob
import os
from typing import TYPE_CHECKING

from . import system

if TYPE_CHECKING:
    from .host import Host
    from .launch import Launch


def pinned(h: Host) -> str:
    """What a browser starting now records as its engine: the version the
    host's engine resolves to when that is a link, else nothing to pin."""
    real = os.path.realpath(h.engine)
    return real if real != h.engine else ""


def held_by(h: Host, daemon_pid: int) -> str:
    """The engine to pin for a browser that started with none pinned: its
    daemon's own link to the file it runs, once that is no longer the host's,
    as when the first release replaces an image's plain file with a link. The
    link runs that very file while the daemon lives, whatever its name holds
    now. Nothing while the daemon runs the host's engine, nor on a person's
    own machine, where only the host's file counts."""
    if h.own:
        return ""
    running = system.process_file(daemon_pid)
    try:
        held = os.stat(running)
    except OSError:
        return ""
    try:
        return "" if os.path.samestat(held, os.stat(h.engine)) else running
    except OSError:
        return running


def for_launch(record: Launch | None, h: Host) -> str:
    """The engine a command on *record* runs: the one its browser started on
    while that is still one of the engine's files, else the host's. An update
    keeps the version before it, so only a browser two updates behind
    restarts. A daemon's link counts only while the kernel names it one of
    them, since once the daemon is gone its number may name any process."""
    pin = record.engine if record is not None else ""
    if pin and os.access(pin, os.X_OK) and (system.file_named(pin) or pin) in files(h, None):
        return pin
    return h.engine


def stale(record: Launch, h: Host) -> bool:
    """Whether *record*'s browser runs a version an update has since replaced."""
    return bool(record.engine) and record.engine != pinned(h)


def files(h: Host, record: Launch | None) -> frozenset[str]:
    """Every file the engine's processes may run from, as the kernel names a
    process's file, by its real path: the host's, the version the open browser
    started on, and every version installed beside the newest, which a command
    started before an update may still run. One run from a daemon's own link
    is named as that daemon is."""
    found = {h.engine, os.path.realpath(h.engine)}
    if record is not None and record.engine:
        found.add(record.engine)
    newest = pinned(h)
    if newest:
        # Each version's folder holds only its engine, under whatever name
        # that version gave it.
        unit = os.path.dirname(os.path.dirname(newest))
        found.update(glob.glob(os.path.join(glob.escape(unit), "*", "*")))
    return frozenset(found)
