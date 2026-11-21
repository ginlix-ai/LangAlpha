"""What a computer's owner chose for its browser, handed to its machine.

Two choices: how sharply the live view is drawn, which follows the computer's
size without one, and whether sign-ins are kept between the browser's starts,
never on a computer that holds shared workspaces, where a member's agent would
browse as whoever signed in. The machine's browser runtime keeps its own copy of
each, read each time its browser starts. So they are handed to the machine at
an acquire that finds this sandbox without them, as a rebuilt machine, or one
stopped when a choice changed, would be, and at once when one changes on a
running computer.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.browser_runtime.protocol import (
    STREAM_QUALITIES,
    StreamQualityName,
)
from ptc_agent.core.sandbox.providers import provider_class
from src.server.database.shared_workspaces import is_holder
from src.server.services.browser_view import ask_runtime

logger = logging.getLogger(__name__)

__all__ = ["Choices", "apply_now", "keeps_signins", "keep", "read", "saved_quality", "saved_signins"]

#: Where each choice sits in the computer's ``config``. An absent quality
#: follows the computer's size; absent sign-ins are kept.
QUALITY_KEY = "browser_quality"
SIGNINS_KEY = "browser_signins"
#: A token each save of the quality writes beside it. Unlike the choice itself
#: it never repeats, so a change that came back to where it started still reads
#: as one: a write straight to the machine meanwhile may have left it elsewhere.
QUALITY_REVISION_KEY = "browser_quality_rev"
#: The same for sign-ins, sorting after every earlier save: each save erases
#: what the kept profile held, so the machine ignores one that reaches it after
#: a later one rather than erase a sign-in made since (``browser_runtime/signin.py``).
SIGNINS_REVISION_KEY = "browser_signins_rev"
#: A computer whose sign-ins were never saved, before any save.
NO_REVISION = "0" * 20
_WRITE_TIMEOUT_S = 15
# Which choices this sandbox object last handed its machine, so an acquire that
# finds them unchanged costs nothing. Per sandbox object, so a worker that
# attaches anew, or a machine rebuilt under a new object, writes them again.
_HANDED = "_browser_choices_handed"
#: Writes one change makes before leaving the machine to the change after it.
_RECONCILE_TRIES = 3


@dataclass(frozen=True)
class Choices:
    quality: StreamQualityName | None = None
    quality_revision: str | None = None
    signins: bool = True
    signins_revision: str = NO_REVISION

    def commands(self) -> tuple[tuple[str, ...], ...]:
        """What the machine runs to take these choices."""
        return (
            ("quality", self.quality or "default"),
            ("signin", "on" if self.signins else "off", self.signins_revision),
        )


def read(computer: Mapping[str, Any]) -> Choices:
    config = computer.get("config") or {}
    quality = config.get(QUALITY_KEY)
    quality_revision = config.get(QUALITY_REVISION_KEY)
    signins_revision = config.get(SIGNINS_REVISION_KEY)
    return Choices(
        quality=quality if quality in STREAM_QUALITIES else None,
        quality_revision=quality_revision if isinstance(quality_revision, str) else None,
        signins=keeps_signins(computer),
        signins_revision=(
            signins_revision if isinstance(signins_revision, str) else NO_REVISION
        ),
    )


def keeps_signins(computer: Mapping[str, Any]) -> bool:
    """Kept unless its owner turned it off, and never on a holder's computer."""
    return (computer.get("config") or {}).get(SIGNINS_KEY) is not False and not is_holder(
        computer.get("user_id")
    )


def saved_quality(quality: StreamQualityName | None) -> dict[str, str | None]:
    """The ``config`` settings that save *quality* as the choice, None to
    follow the computer's size, written together."""
    return {QUALITY_KEY: quality, QUALITY_REVISION_KEY: uuid.uuid4().hex}


def saved_signins(keep: bool) -> dict[str, object]:
    """The ``config`` settings that save whether sign-ins are kept, written
    together. Every save starts the kept profile empty, so saving it as it was
    is how a person signs the browser out of every site."""
    return {SIGNINS_KEY: keep, SIGNINS_REVISION_KEY: f"{time.time_ns():020x}"}


async def _hand(runtime: Any, internal_src: str, choices: Choices) -> None:
    for command in choices.commands():
        answer = await ask_runtime(runtime, internal_src, *command, timeout=_WRITE_TIMEOUT_S)
        if not (isinstance(answer, dict) and answer.get("done") is True):
            raise RuntimeError(f"the machine did not take {command[0]}: {answer!r}")


async def keep(sandbox: Any, *, kind: str, choices: Choices) -> None:
    """Hand *choices* to the machine *sandbox* runs, on its acquire, unless this
    sandbox already handed it those saves. It never fails one: a machine left
    without them keeps what it had, and the next acquire tries again.

    A save's own write to the machine (``apply_now``) can land on any worker or
    fail, and this sandbox object never hears of it, so it is judged by the
    saves: any later one is handed again, even one back to what it handed.
    """
    if sandbox is None or not provider_class(kind).browser_use:
        return
    if getattr(sandbox, _HANDED, None) == choices:
        return
    try:
        await _hand(sandbox.runtime, sandbox.layout.internal_src, choices)
    except Exception:
        logger.warning(
            f"[browser] could not hand the browser settings to sandbox {getattr(sandbox, 'sandbox_id', None)}",
            exc_info=True,
        )
        return
    setattr(sandbox, _HANDED, choices)


async def apply_now(computer: dict[str, Any]) -> None:
    """Put changed choices on *computer*'s machine if it is running, so the
    next browser it opens has them without waiting for an acquire. A machine
    not running is left alone and gets them at its next acquire.

    Two changes close together can reach the machine in either order, and the
    server runs on several workers, so after each write the stored choices are
    read again and put there if they moved: whichever change finishes last
    leaves the machine on the stored ones.
    """
    from src.server.database.computer import get_computer

    for _ in range(_RECONCILE_TRIES):
        applied = read(computer)
        if not await _apply(computer, applied):
            return
        latest = await get_computer(computer["computer_id"])
        if latest is None or read(latest) == applied:
            return
        computer = latest


async def _apply(computer: dict[str, Any], choices: Choices) -> bool:
    """Whether the choices reached the machine."""
    from ptc_agent.core.sandbox.runtime import RuntimeState
    from src.server.services.data_token_sweep import running_sandbox

    if (
        not provider_class(computer["kind"]).browser_use
        or not computer.get("provider_ref")
        or computer.get("status") != "running"
    ):
        return False
    try:
        async with running_sandbox(computer) as runtime:
            if runtime is None:
                return False
            # E2B resumes a sandbox it attaches to, so one paused meanwhile is
            # not woken for this.
            if await runtime.refresh_state() != RuntimeState.RUNNING:
                return False
            internal_src = SandboxLayout(await runtime.fetch_working_dir()).internal_src
            await _hand(runtime, internal_src, choices)
            return True
    except Exception:
        logger.warning(
            f"[browser] could not apply the browser settings to computer {computer.get('computer_id')}",
            exc_info=True,
        )
        return False
