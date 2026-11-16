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

Sources the egress relay held for a code run's or a command's paid calls
stand in for the trace entries those calls left (:func:`collect_relayed`),
keyed by the call id the tool puts on its artifact or the command's job ref.

This module owns the ``src.server.database`` imports for commands' and the
relay's sources, as ``body_store`` owns the body store's.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, get_args

from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.steering import since_model_response
from ptc_agent.agent.provenance.trace import RelayBatch
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
CommandTraceReader = Callable[[str, str], Awaitable["CommandTrace"]]

#: ``call_id -> batch``: the relay's held sources under that call id.
RelayReader = Callable[[str], Awaitable[RelayBatch]]

#: ``call_ids -> None``: returns once the paid calls under those call ids
#: have settled, or one bounded wait for them all is up.
RelaySettleWait = Callable[[Sequence[str]], Awaitable[None]]

# The relay holds a call's sources just before settling it, after the reply's
# last byte went out, so a command can end, and its notice be read, first.
# Settling is a few writes; past this the call is still running (a process
# the command left behind). The same bound a code run's result waits.
_SETTLE_WAIT_S = 3.0
_SETTLE_POLL_S = 0.2

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


@dataclass(frozen=True)
class CommandTrace:
    """A handed-off command's MCP trace, and the call id its paid relay calls
    were filed under (on its job ref), which their held sources are taken by."""

    entries: list[dict[str, Any]] = field(default_factory=list)
    call_id: str | None = None


async def read_command_trace(thread_id: str, command_id: str) -> CommandTrace:
    """What ``command_id``'s row stores; empty for a command of another
    thread, one with no trace, or a failed read."""
    try:
        from src.server.database.runs.command_runs import get_thread_command

        row = await get_thread_command(thread_id, command_id, with_trace=True)
    except Exception:
        logger.warning(
            "[PROVENANCE] reading command %s's trace failed", command_id, exc_info=True
        )
        return CommandTrace()
    row = row or {}
    trace = row.get("mcp_trace")
    job_ref = row.get("job_ref")
    call_id = job_ref.get("call_id") if isinstance(job_ref, Mapping) else None
    return CommandTrace(
        trace if isinstance(trace, list) else [], str(call_id) if call_id else None
    )


async def collect_relayed(user_id: str, call_id: str) -> RelayBatch:
    """The relay's held sources for ``user_id``'s ``call_id``, taken once;
    nothing on a failed read, which leaves the sandbox trace to stand, and no
    read at all where no server is metered, since nothing holds any."""
    try:
        from src.server.database.relayed_sources import collect
        from src.server.services.egress.platform_accounts import platform_accounts_configured

        if not platform_accounts_configured():
            return RelayBatch()
        return await collect(user_id, call_id)
    except Exception:
        logger.warning("[PROVENANCE] collecting relayed sources failed", exc_info=True)
        return RelayBatch()


async def await_relay_settled(user_id: str, call_ids: Sequence[str]) -> None:
    """Wait, boundedly and once for all of ``call_ids``, while any of
    ``user_id``'s paid calls under them may still settle, so a handed-off
    command's last call has its sources held before they are taken: the trace
    entries they stand in for are dropped by server, and a source held after
    the taking would show only at a later read, if any.

    The wait holds up the model call, so one bound covers a notice's every
    command, and a hold older than the relay's wall clock is not waited on:
    no live reply can settle it, and the sweep charges it. A failed read
    waits no longer."""
    if not call_ids:
        return
    try:
        from src.server.database.relayed_sources import settling
        from src.server.services.egress.platform_accounts import platform_accounts_configured
        from src.server.services.egress.relay import WALL_CLOCK_S

        if not platform_accounts_configured():
            return
        pending = list(dict.fromkeys(call_ids))
        # The bound covers the reads too: a read stalled on a busy pool would
        # otherwise hold the model call for as long as it takes.
        async with asyncio.timeout(_SETTLE_WAIT_S):
            while pending := sorted(await settling(user_id, pending, WALL_CLOCK_S)):
                await asyncio.sleep(_SETTLE_POLL_S)
    except TimeoutError:
        # Their sources, held after the taking, miss this read.
        logger.warning(
            "[PROVENANCE] paid calls %s still settling after %ss; reading their sources anyway",
            pending,
            _SETTLE_WAIT_S,
        )
        return
    except Exception:
        logger.warning("[PROVENANCE] reading relayed calls' settling failed", exc_info=True)
