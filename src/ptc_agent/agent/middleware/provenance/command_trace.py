"""Which background commands' data sources a model call brings to the chat.

A command that outlived its tool call reports back later, as a notice joined
to a live turn (a steering message whose ``steering_delivered`` entry names
the command in its ``report_source``) or as a turn of its own (whose run
config names the command under :data:`REPORT_COMMAND_KEY`, read with the
turn's input). The MCP calls the command's script made were harvested onto
its ledger row when it ended, so its sources are read from there
(:func:`read_command_trace`) and reach the chat with the notice, as a code
run's reach it with its result. A ThreadStatus look that shows an end first
drops the notice, so it names the command on its own result instead
(:data:`REPORTED_COMMANDS_KEY`).

This module owns the ``src.server.database.runs.command_runs`` import, as
``body_store`` owns the body store's.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, get_args

from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.steering import since_model_response
from ptc_agent.agent.transcript.classify import is_run_boundary_message
from src.server.models.chat import CommandReport

logger = logging.getLogger(__name__)

#: The run-config key naming the command a report-back turn of its own
#: reports on. Server-set from the turn's ``report_source``.
REPORT_COMMAND_KEY = "report_command_id"

#: The ThreadStatus artifact key naming the commands whose end that call
#: showed first, which its stamp kept their notices from bringing.
REPORTED_COMMANDS_KEY = "reported_commands"

#: ``(thread_id, command_id) -> trace``: the stored MCP trace of a command
#: on that thread, ``[]`` when there is none or the command is another
#: thread's.
CommandTraceReader = Callable[[str, str], Awaitable[list[dict[str, Any]]]]

(_COMMAND_REPORT_KIND,) = get_args(CommandReport.model_fields["kind"].annotation)


def _steered_commands(message: Any) -> list[str]:
    kwargs = getattr(message, "additional_kwargs", None) or {}
    delivered = kwargs.get("steering_delivered")
    if not isinstance(delivered, Mapping):
        return []
    found: list[str] = []
    for entry in delivered.get("messages") or []:
        source = entry.get("report_source") if isinstance(entry, Mapping) else None
        if (
            isinstance(source, Mapping)
            and source.get("kind") == _COMMAND_REPORT_KIND
            and source.get("command_id")
        ):
            found.append(str(source["command_id"]))
    return found


def reported_commands(
    messages: Sequence[Any], configurable: Mapping[str, Any] | None
) -> list[str]:
    """The commands whose notices the model reads for the first time in this
    call, in the order it reads them.

    A notice joined to a live turn is read at the call after the steering
    message carrying it; a turn of its own reads its notice with the turn's
    input, at its first call. Later calls of the same turn name neither, so
    each notice's sources go out once per turn.
    """
    window = since_model_response(messages)
    found: list[str] = []
    reported = (configurable or {}).get(REPORT_COMMAND_KEY)
    if reported and any(is_run_boundary_message(m) for m in window):
        found.append(str(reported))
    for message in window:
        if isinstance(message, HumanMessage):
            found.extend(_steered_commands(message))
    return list(dict.fromkeys(found))


def shown_commands(result: Any) -> list[str]:
    """The commands a ThreadStatus result names under
    :data:`REPORTED_COMMANDS_KEY`, in the order it showed them."""
    artifact = getattr(result, "artifact", None)
    ids = artifact.get(REPORTED_COMMANDS_KEY) if isinstance(artifact, Mapping) else None
    return [str(i) for i in ids or ()]


async def read_command_trace(thread_id: str, command_id: str) -> list[dict[str, Any]]:
    """The MCP trace stored on ``command_id``'s row, ``[]`` for a command of
    another thread, one with no trace, or a failed read."""
    try:
        from src.server.database.runs.command_runs import get_thread_command

        row = await get_thread_command(thread_id, command_id, with_trace=True)
    except Exception:
        logger.warning(
            "[PROVENANCE] reading command %s's trace failed", command_id, exc_info=True
        )
        return []
    trace = (row or {}).get("mcp_trace")
    return trace if isinstance(trace, list) else []
