"""Why a computer's start failed, kept on its row for the owner to act on.

A start runs behind a 202, so its failure otherwise reaches the owner only as
a frame on a stream they may not be reading. Every start path records it as
it fails, and the server, not the client, decides what fixes it: an SSH
machine whose folder is taken needs a reinstall, one that lost its pairing a
reconnect, and anything else a retry. Importable without the provider stack,
because the router reads it on every computer list.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.providers.ssh_errors import (
    SSHFolderMarked,
    connect_failure,
)
from src.server.database.computer import record_computer_start_failure
from src.server.models.computer import StartFailure, StartFailureRemedy
from src.server.utils.error_sanitization import sandbox_failure_http

logger = logging.getLogger(__name__)

SSH_KIND = "ssh"
START_FAILED = "start_failed"

# A folder the computer cannot have is fixed by taking it back or choosing
# another; a machine that no longer knows the deployment, by pairing it again.
_REINSTALL_CODES = frozenset({SSHFolderMarked.code_name, "root_dir"})
_RECONNECT_CODES = frozenset({"not_connected", "auth_failed", "host_key_mismatch"})


def remedy_for(code: str, *, kind: str) -> StartFailureRemedy:
    """The repair the client offers for *code*. Only a machine its owner
    brought can be reinstalled or paired again; any other is retried."""
    if kind == SSH_KIND and code in _REINSTALL_CODES:
        return "reinstall"
    if kind == SSH_KIND and code in _RECONNECT_CODES:
        return "reconnect"
    return "retry"


def describe_start_failure(exc: BaseException, *, kind: str) -> Dict[str, Any]:
    """The failure as the row keeps it, without ``at``, which the write stamps.

    An SSH machine's own sentence is the owner's to read; any other failure
    is told in the words a start route answers it with, which never carry
    the provider's text.
    """
    failure = connect_failure(exc)
    owner: Optional[str] = None
    if isinstance(failure, SSHFolderMarked):
        code = SSHFolderMarked.code_name
        owner = failure.owner
        message = str(failure)
    elif failure is not None:
        code = failure.code
        message = failure.for_owner
    else:
        code = START_FAILED
        detail = sandbox_failure_http(exc).detail
        message = detail if isinstance(detail, str) else str(detail)
    return {
        "code": code,
        "message": message,
        "owner": owner,
        "remedy": remedy_for(code, kind=kind),
    }


async def record_start_failure(
    computer_id: Optional[str], exc: BaseException, *, kind: str
) -> None:
    """Record *exc* as the computer's last failed start, best effort.

    Called from a start's failure path, which goes on to raise the start's
    own error, so a failure to write never replaces it.
    """
    if not computer_id:
        return
    try:
        await record_computer_start_failure(
            computer_id, describe_start_failure(exc, kind=kind)
        )
    except Exception:
        logger.exception(
            "Could not record the failed start of computer %s", computer_id
        )


def start_failure_from_row(row: Dict[str, Any]) -> Optional[StartFailure]:
    """The row's last failed start, None when there is none or it does not parse."""
    value = row.get("start_failure")
    if not isinstance(value, dict):
        return None
    try:
        return StartFailure.model_validate(value)
    except ValueError:
        logger.warning(
            "Computer %s has a start_failure that does not parse",
            row.get("computer_id"),
        )
        return None
