"""The one mapping from a spec change's failure to what the client is told.

A change is refused before it is accepted (a 409 on the request) or fails
after (an outcome on ``computers.spec_change``); both carry the same shape so
the client renders them with one piece of code. Importable without the
provider stack, because the router reads outcomes on every computer list.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Iterable, List, Optional

from fastapi import HTTPException

from src.server.models.computer import (
    ComputerSpecChange,
    SpecChangeError,
    SpecChangeRefusal,
    SpecRefusalCode,
    UnsavedFileOut,
)
from src.server.services.computer_errors import (
    ComputerBusyError,
    DiskTooSmallError,
    MachineBusyError,
)
from src.server.services.persistence.sync_result import (
    BackupIncomplete,
    UnsavedFile,
)

logger = logging.getLogger(__name__)

# Covers both a shutdown that cancelled the change and a worker that died
# mid-change. Either may have left the tier or the sandbox at the old or the
# new size, so this never claims which.
SPEC_CHANGE_INTERRUPTED = (
    "The spec change was interrupted before it finished. Your files are safe; "
    "check the computer's current spec and try again."
)
SPEC_CHANGE_FAILED = (
    "The spec change failed; the computer stays at its previous spec."
)
SPEC_CHANGE_NOT_ALLOWED = "Your plan does not allow this spec."
SPEC_CHANGE_IN_PROGRESS = "A spec change is already running on this computer."

# Enough to act on; the row is read on every computer list.
_FILES_SHOWN = 20


def unsaved_files_out(unsaved: Iterable[UnsavedFile]) -> List[UnsavedFileOut]:
    return [
        UnsavedFileOut(path=f.path, reason=f.reason, size=f.size)
        for f in list(unsaved)[:_FILES_SHOWN]
    ]


def _http_detail_message(exc: HTTPException) -> str:
    """Relay only a detail the gate shaped for the user.

    The platform's quota refusal is a dict whose ``message`` is written to be
    shown; a bare string is the dependency layer talking to a developer
    (``Requires scope: workspace:spec:max``), so it gets the standard copy.
    """
    detail = exc.detail
    if isinstance(detail, dict) and isinstance(detail.get("message"), str):
        return detail["message"] or SPEC_CHANGE_NOT_ALLOWED
    return SPEC_CHANGE_NOT_ALLOWED


def spec_change_error(exc: BaseException) -> SpecChangeError:
    """Map a failed change to the code the client switches on.

    Operator detail (ids, causes) stays in the log: every message here is one
    the person who asked for the change can read.
    """
    if isinstance(exc, asyncio.CancelledError):
        return SpecChangeError(code="interrupted", message=SPEC_CHANGE_INTERRUPTED)
    if isinstance(exc, MachineBusyError):
        return SpecChangeError(code="turn_active", message=str(exc))
    if isinstance(exc, BackupIncomplete):
        return SpecChangeError(
            code="backup_incomplete",
            message=exc.user_message,
            files=unsaved_files_out(exc.unsaved),
        )
    if isinstance(exc, ComputerBusyError):
        return SpecChangeError(code="busy", message=str(exc))
    if isinstance(exc, DiskTooSmallError):
        return SpecChangeError(code="disk_too_small", message=str(exc))
    if isinstance(exc, HTTPException):
        # The plan re-check under the capacity lock refused it.
        return SpecChangeError(code="not_allowed", message=_http_detail_message(exc))
    return SpecChangeError(code="unknown", message=SPEC_CHANGE_FAILED)


def spec_refusal(
    code: SpecRefusalCode,
    message: str,
    files: Iterable[UnsavedFile] = (),
) -> Dict[str, Any]:
    """The 409 detail for a change refused before it was accepted."""
    return SpecChangeRefusal(
        code=code, message=message, files=unsaved_files_out(files)
    ).model_dump(mode="json")


def spec_change_from_row(computer: Dict[str, Any]) -> Optional[ComputerSpecChange]:
    """The stored outcome, with an abandoned in-progress change read as interrupted.

    Staleness is ``spec_change_stale``, projected by every computers read on
    the database clock, so this answers exactly what a new claim would decide.
    """
    raw = computer.get("spec_change")
    if not raw:
        return None
    try:
        change = ComputerSpecChange.model_validate(raw)
    except ValueError:
        logger.warning(
            "Unreadable spec_change on computer %s", computer.get("computer_id")
        )
        return None
    if change.state == "in_progress" and computer.get("spec_change_stale"):
        change.state = "failed"
        change.error = SpecChangeError(
            code="interrupted", message=SPEC_CHANGE_INTERRUPTED
        )
    return change
