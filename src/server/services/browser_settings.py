"""What a computer's owner chose for its browser, handed to its machine.

Three choices: how sharply the live view is drawn, which follows the computer's
size without one; whether sign-ins are kept between the browser's starts, never
on a computer holding shared workspaces, where a member's agent would browse as
whoever signed in; and, on a Mac under remote control, whether the browser also
shows as a window on its screen.

The machine's browser runtime keeps its own copy, read each time its browser
starts. So the choices are handed over at an acquire that finds this sandbox
without them (a rebuilt machine, or one stopped when a choice changed), and at
once when one changes on a running computer. Every save carries a revision the
database orders under the row's lock, and the machine drops a bundle older
than the one it holds, so the two hand-offs can land in any order, on any
worker, and the machine still ends on the last save.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from ptc_agent.core.sandbox.browser_runtime.protocol import (
    NO_REVISION,
    REVISION_PATTERN,
    STREAM_QUALITIES,
    Choices,
)
from src.server.database import computer as computer_db
from src.server.database.computer import (
    BROWSER_QUALITY_KEY as QUALITY_KEY,
    BROWSER_REVISION_KEY as REVISION_KEY,
    BROWSER_SIGNINS_KEY as SIGNINS_KEY,
    BROWSER_SIGNINS_REVISION_KEY as SIGNINS_REVISION_KEY,
    BROWSER_WINDOW_KEY as WINDOW_KEY,
)
from src.server.database.shared_workspaces import is_holder
from src.server.models.computer import BrowserChoicesUpdate, ComputerBrowser
from src.server.services.browser_view import NOT_RUNNING, ask_running, ask_runtime
from src.server.services.ssh_computers import is_mac_under_remote_control

logger = logging.getLogger(__name__)

__all__ = ["apply_now", "keep", "keeps_signins", "read", "save", "shown"]

#: Where each field of an update sits in the computer's ``config``.
_KEYS = {"quality": QUALITY_KEY, "window": WINDOW_KEY, "signins": SIGNINS_KEY}
_WRITE_TIMEOUT_S = 15
# Which choices this sandbox object last handed its machine, so an acquire that
# finds them unchanged costs nothing. Per sandbox object, so a worker that
# attaches anew, or a machine rebuilt under a new object, writes them again.
_HANDED = "_browser_choices_handed"


def _revision(value: object) -> str | None:
    return value if isinstance(value, str) and REVISION_PATTERN.fullmatch(value) else None


def read(computer: Mapping[str, Any]) -> Choices:
    config = computer.get("config") or {}
    quality = config.get(QUALITY_KEY)
    signins_revision = _revision(config.get(SIGNINS_REVISION_KEY)) or NO_REVISION
    return Choices(
        quality=quality if quality in STREAM_QUALITIES else None,
        window=config.get(WINDOW_KEY) is True and is_mac_under_remote_control(computer),
        signins=keeps_signins(computer),
        signins_revision=signins_revision,
        # A row saved before every save had a revision is at least as new as
        # its sign-ins' last save.
        revision=_revision(config.get(REVISION_KEY)) or signins_revision,
        computer=str(computer["computer_id"]),
    )


def shown(computer: Mapping[str, Any]) -> ComputerBrowser:
    """The choices as the computer's response carries them."""
    choices = read(computer)
    return ComputerBrowser(
        quality=choices.quality,
        signins=choices.signins,
        window=choices.window,
        window_available=is_mac_under_remote_control(computer),
    )


def keeps_signins(computer: Mapping[str, Any]) -> bool:
    """Kept unless its owner turned it off, and never on a holder's computer."""
    return (computer.get("config") or {}).get(SIGNINS_KEY) is not False and not is_holder(
        computer.get("user_id")
    )


async def save(
    computer_id: str, user_id: str, update: BrowserChoicesUpdate
) -> dict[str, Any] | None:
    """Save the choices *update* sets in one write and answer the row as saved,
    or None where the computer is gone or not the user's. A null quality
    follows the computer's size. Every save of the sign-ins starts the kept
    profile empty, so saving them as they were is how a person signs the
    browser out of every site."""
    choices = {_KEYS[name]: getattr(update, name) for name in update.model_fields_set}
    return await computer_db.save_browser_choices(computer_id, user_id, choices)


def _taken(answer: object) -> bool:
    return isinstance(answer, dict) and answer.get("done") is True


async def keep(sandbox: Any, choices: Choices | None) -> None:
    """Hand *choices*, the computer's as its acquire read them, to the machine
    *sandbox* runs, unless this sandbox already handed it those. It never
    fails one: a machine left without them keeps what it had, and the next
    acquire tries again.

    An acquire reads them long before it writes, and a save's own write
    (``apply_now``) can land meanwhile on any worker; the machine drops this
    older bundle then, so nothing here reads the row again.
    """
    if sandbox is None or choices is None or getattr(sandbox, _HANDED, None) == choices:
        return
    try:
        answer = await ask_runtime(
            sandbox.runtime,
            sandbox.layout.internal_src,
            "choices",
            choices.to_wire(),
            timeout=_WRITE_TIMEOUT_S,
        )
    except Exception:
        logger.warning(
            f"[browser] could not reach sandbox {getattr(sandbox, 'sandbox_id', None)} to hand it the browser settings",
            exc_info=True,
        )
        return
    if not _taken(answer):
        logger.warning(
            f"[browser] sandbox {getattr(sandbox, 'sandbox_id', None)} did not take the browser settings: {answer!r}"
        )
        return
    setattr(sandbox, _HANDED, choices)


async def apply_now(computer: Mapping[str, Any]) -> bool:
    """Put *computer*'s saved choices on its machine if it is running, so the
    next browser it opens has them without waiting for an acquire, and say
    whether it has them. A machine not running is left alone and gets them at
    its next acquire, before any browser of its opens, so it counts as having them.
    """
    try:
        answer = await ask_running(computer, "choices", read(computer).to_wire(), timeout=_WRITE_TIMEOUT_S)
    except Exception:
        logger.warning(
            f"[browser] could not apply the browser settings to computer {computer.get('computer_id')}",
            exc_info=True,
        )
        return False
    if answer is NOT_RUNNING or _taken(answer):
        return True
    logger.warning(
        f"[browser] computer {computer.get('computer_id')} did not take the browser settings: {answer!r}"
    )
    return False
