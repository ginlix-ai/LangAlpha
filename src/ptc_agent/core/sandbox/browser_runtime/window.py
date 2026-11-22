"""Whether a browser starting now also shows as a window on the machine's screen.

Only a person's own Mac has a screen it can draw on, and only from a login in
the person's desktop, as the helper's is: a cloud computer has no screen, and a
login over the Mac's own SSH runs outside the desktop. Like the live view's
quality, the choice is the computer's, and an open browser keeps the way it
started (``launch``).
"""

from __future__ import annotations

import subprocess
import sys

from . import choices, host

#: What ``launchctl managername`` says inside the person's desktop.
_DESKTOP = "Aqua"


def chosen() -> bool:
    """Whether a browser starting now shows a window: chosen, and possible here."""
    held = choices.held()
    return bool(held and held.window) and can_show()


def can_show() -> bool:
    return sys.platform == "darwin" and host.current().own and _in_desktop()


def _in_desktop() -> bool:
    try:
        named = subprocess.run(
            ["launchctl", "managername"], capture_output=True, text=True, timeout=5
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return False
    return named == _DESKTOP
