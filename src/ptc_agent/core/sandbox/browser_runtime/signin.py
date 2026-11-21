"""Whether the browser keeps what sites remember between its starts.

Kept, a browser starts on one profile on the computer's disk, so a sign-in made
in it, by the person in the live view, is there the next time it opens and for
every thread on the computer. Not kept, each browser starts on a profile of its
own that goes when it closes. Like the live view's quality, the choice is the
computer's, and an open browser keeps the profile it started on (``launch``).

Each save of the choice on the server comes with a revision that sorts after
every earlier one. A save erases what the profile holds, so turning it off, or
on again, leaves nothing from before, and a save made only to sign out is one
more revision. A save older than the one the machine has is one that reached it
late, and is ignored, so it never erases a sign-in made after the newer one.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass

from .protocol import state_file, write_state

_FILE = "signin"
#: The kept profile, beside the machine's other state, outside the runtime's
#: ``src`` that each ship replaces and outside every workspace folder.
_PROFILE = "profile"
#: A revision: fixed-width lowercase hex, so text order is save order.
REVISION = re.compile(r"[0-9a-f]{20}")


@dataclass(frozen=True)
class Choice:
    keep: bool
    revision: str


def profile() -> str:
    return str(state_file(_PROFILE))


def current() -> Choice | None:
    try:
        stored = json.loads(state_file(_FILE).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(stored, dict):
        return None
    keep, revision = stored.get("keep"), stored.get("revision")
    if not isinstance(keep, bool) or not isinstance(revision, str) or not REVISION.fullmatch(revision):
        return None
    return Choice(keep, revision)


def kept() -> bool:
    """Whether a browser starting now starts on the kept profile."""
    choice = current()
    return bool(choice and choice.keep)


def newer(revision: str) -> bool:
    """Whether a save at *revision* is later than the one the machine has."""
    choice = current()
    return choice is None or revision > choice.revision


def record(choice: Choice) -> None:
    write_state(_FILE, json.dumps({"keep": choice.keep, "revision": choice.revision}))


def erase() -> None:
    """Remove the kept profile, and every sign-in in it, from the disk. Only
    with no browser running on it, which would write it back as it closed."""
    shutil.rmtree(profile(), ignore_errors=True)
