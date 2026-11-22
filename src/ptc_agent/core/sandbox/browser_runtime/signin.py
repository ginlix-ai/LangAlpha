"""Whether the browser keeps what sites remember between its starts.

Kept, a browser starts on one profile on the computer's disk, so a sign-in made
in it, by the person in the live view, is there the next time it opens and for
every thread on the computer. Not kept, each browser starts on a profile of its
own that goes when it closes. Like the live view's quality, the choice is the
computer's, and an open browser keeps the profile it started on (``launch``).

Each save of the choice on the server erases what the profile holds, so
turning it off, or on again, leaves nothing from before, and a save made only
to sign out is one more save (``machine.choose``).
"""

from __future__ import annotations

import shutil
from pathlib import Path

from . import choices
from .protocol import state_file

#: The kept profile, beside the machine's other state, outside the runtime's
#: ``src`` that each ship replaces and outside every workspace folder.
_PROFILE = "profile"


def profile() -> str:
    return str(state_file(_PROFILE))


def kept() -> bool:
    """Whether a browser starting now starts on the kept profile."""
    chosen = choices.held()
    return bool(chosen and chosen.signins)


def forget_last_tabs(kept: str) -> None:
    """Clear the tabs the last browser on *kept* left, before the next starts.

    Many sites sign a person in with a cookie that lasts the session, which the
    person's own browser keeps by continuing where it left off, and the agent's
    closes often: when idle, short of memory, or with the computer. So it starts
    continuing where it left off too (``launch``), which would also reopen the
    last browser's tabs, another thread's on a browser every thread shares: they
    go, and it starts on one blank page with the cookies alone. Only while no
    browser runs on the profile, which would write them back as it closed.
    """
    shutil.rmtree(Path(kept) / "Default" / "Sessions", ignore_errors=True)


def erase() -> None:
    """Remove the kept profile, and every sign-in in it, from the disk. Only
    with no browser running on it, which would write it back as it closed.

    A profile only partly removed is a sign-out that did not happen, so
    anything but finding none raises: the server hands the save again.
    """
    try:
        shutil.rmtree(profile())
    except FileNotFoundError:
        pass
