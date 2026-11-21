"""How sharply a computer's live browser view is drawn, as its owner chose.

The machine's browser runtime keeps its own copy of the choice, read each time
its browser starts, and follows the computer's size without one. So the choice
is handed to the machine at an acquire that finds this sandbox without it, as a
rebuilt machine, or one stopped when the choice changed, would be, and at once
when it changes on a running computer.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from typing import Any

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.browser_runtime.protocol import (
    STREAM_QUALITIES,
    StreamQualityName,
)
from ptc_agent.core.sandbox.providers import provider_class
from src.server.services.browser_view import ask_runtime

logger = logging.getLogger(__name__)

__all__ = ["CONFIG_KEY", "REVISION_KEY", "apply_now", "chosen", "keep", "revision", "saved"]

#: Where the choice sits in the computer's ``config``; absent follows its size.
CONFIG_KEY = "browser_quality"
#: A token each save of the choice writes beside it. Unlike the choice itself it
#: never repeats, so a change that came back to where it started still reads as
#: one: a write straight to the machine meanwhile may have left it elsewhere.
REVISION_KEY = "browser_quality_rev"
_WRITE_TIMEOUT_S = 15
# Which save of the choice this sandbox object last handed its machine, so an
# acquire that finds it unchanged costs nothing. Per sandbox object, so a worker
# that attaches anew, or a machine rebuilt under a new object, writes it again.
_HANDED = "_browser_quality_handed"
_NEVER = object()
#: Writes one change makes before leaving the machine to the change after it.
_RECONCILE_TRIES = 3


def chosen(computer: Mapping[str, Any]) -> StreamQualityName | None:
    value = (computer.get("config") or {}).get(CONFIG_KEY)
    return value if value in STREAM_QUALITIES else None


def revision(computer: Mapping[str, Any]) -> str | None:
    value = (computer.get("config") or {}).get(REVISION_KEY)
    return value if isinstance(value, str) else None


def saved(quality: StreamQualityName | None) -> dict[str, str | None]:
    """The ``config`` settings that save *quality* as the choice, None to
    follow the computer's size, written together."""
    return {CONFIG_KEY: quality, REVISION_KEY: uuid.uuid4().hex}


async def _hand(runtime: Any, internal_src: str, quality: StreamQualityName | None) -> None:
    answer = await ask_runtime(
        runtime, internal_src, "quality", quality or "default", timeout=_WRITE_TIMEOUT_S
    )
    if not (isinstance(answer, dict) and answer.get("done") is True):
        raise RuntimeError(f"the machine did not take the live view quality: {answer!r}")


async def keep(
    sandbox: Any, *, kind: str, quality: StreamQualityName | None, revision: str | None
) -> None:
    """Hand *quality*, saved as *revision*, to the machine *sandbox* runs, on its
    acquire, unless this sandbox already handed it that save. It never fails
    one: a machine left without it follows its size, and the next acquire tries
    again.

    A save's own write to the machine (``apply_now``) can land on any worker or
    fail, and this sandbox object never hears of it, so it is judged by the
    save: any later one is handed again, even one back to what it handed.
    """
    if sandbox is None or not provider_class(kind).browser_use:
        return
    handed = (quality, revision)
    if getattr(sandbox, _HANDED, _NEVER) == handed:
        return
    try:
        await _hand(sandbox.runtime, sandbox.layout.internal_src, quality)
    except Exception:
        logger.warning(
            f"[browser] could not hand the live view quality to sandbox {getattr(sandbox, 'sandbox_id', None)}",
            exc_info=True,
        )
        return
    setattr(sandbox, _HANDED, handed)


async def apply_now(computer: dict[str, Any]) -> None:
    """Put a changed choice on *computer*'s machine if it is running, so the
    next browser it opens has it without waiting for an acquire. A machine not
    running is left alone and gets it at its next acquire.

    Two changes close together can reach the machine in either order, and the
    server runs on several workers, so after each write the stored choice is
    read again and put there if it moved: whichever change finishes last
    leaves the machine on the stored one.
    """
    from src.server.database.computer import get_computer

    for _ in range(_RECONCILE_TRIES):
        applied = chosen(computer)
        if not await _apply(computer):
            return
        latest = await get_computer(computer["computer_id"])
        if latest is None or chosen(latest) == applied:
            return
        computer = latest


async def _apply(computer: dict[str, Any]) -> bool:
    """Whether the choice reached the machine."""
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
            await _hand(runtime, internal_src, chosen(computer))
            return True
    except Exception:
        logger.warning(
            f"[browser] could not apply the live view quality to computer {computer.get('computer_id')}",
            exc_info=True,
        )
        return False
