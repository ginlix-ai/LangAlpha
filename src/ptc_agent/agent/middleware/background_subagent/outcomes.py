"""One run's terminal outcome: its status, its reason, and their spelling.

Three writers publish this independently and have to agree: the ledger CAS
writes the durable row, the token forwarder writes the wire frame the frontend
classifies on, and the task's in-memory result feeds the agent-facing
aggregate. Deriving it separately per writer is how a credit stop came to read
"Stopped" on one surface and "Failed" on another, so it is derived once here
and each writer projects the value rather than recomputing it.
"""

import asyncio
from dataclasses import dataclass
from typing import Any

from ptc_agent.agent.middleware.background_subagent.registry import (
    RESTART_CANCEL,
    TIME_LIMIT_CANCEL,
    TransportLostError,
)
from ptc_agent.agent.middleware.credit_gate import CreditStopError
from src.server.contracts.status import CREDIT_STOP_ERROR_TYPE, RESTART_STOP_ERROR_TYPE

# Declared spellings for the two terminals something downstream matches on: the
# retention contract greps "transport_lost", and the credit resume query and the
# pause card both match CREDIT_STOP_ERROR_TYPE. A class name would make either
# of them a rename away from silently never matching.
_TRANSPORT_LOST = "transport_lost"

_TORN_STREAM_MESSAGE = (
    "transport_lost: the task's Redis event stream tore mid-run "
    "(spill failure or quota); the replay archive is incomplete"
)


class SubagentTimeLimitError(Exception):
    """A subagent ran past its own wall clock and was stopped.

    The ask for its answer and the tool stop come well before the end, so this
    is the backstop for a run that kept going anyway, as the turn's own stop is
    for the main agent.
    """


def is_time_limit_cancel(cancel: asyncio.CancelledError) -> bool:
    """A cancel from the stop for running past every clock
    (``time_out_writers_where``), which ends a run as its own clock would."""
    return cancel.args == (TIME_LIMIT_CANCEL,)


@dataclass(frozen=True)
class Outcome:
    """How a run ended, in the one vocabulary every writer publishes."""

    status: str
    error: str
    error_type: str
    # The in-memory terminal where it is finer than the ledger's. A run that
    # outran its own clock is an error to the ledger, so the user hears of it
    # (a cancel never reports back), and a timeout to TaskOutput, whose advice
    # for one is to narrow the task rather than to report a failure.
    local_status: str | None = None

    @classmethod
    def from_exception(cls, e: BaseException) -> "Outcome":
        return cls(
            status=_settle_status(e),
            error=str(e),
            error_type=_error_type(e),
            local_status=(
                "timeout" if isinstance(e, SubagentTimeLimitError) else None
            ),
        )

    @classmethod
    def from_cancel(cls, cancel: asyncio.CancelledError) -> "Outcome | None":
        """How a run one of our own stops cancelled ended: a graceful
        restart's (``stop_writers_for_restart``) or the time limit's. None
        for any other cancel, which the caller lets through."""
        if cancel.args == (RESTART_CANCEL,):
            return cls.restart_stop()
        if is_time_limit_cancel(cancel):
            return cls.from_exception(
                SubagentTimeLimitError("Timed out: still running past its time limit")
            )
        return None

    @classmethod
    def restart_stop(cls) -> "Outcome":
        """A run a graceful restart stopped: neutral like a cancel, and kept
        apart from one by its error type, which offers it back for resume."""
        return cls(
            status="cancelled",
            error="paused by a routine deployment",
            error_type=RESTART_STOP_ERROR_TYPE,
        )

    @classmethod
    def torn_stream(cls) -> "Outcome":
        """A handler that finished while its event stream was already lost.
        An error rather than a stop: the run worked, the record of it did not."""
        return cls(
            status="error", error=_TORN_STREAM_MESSAGE, error_type=_TRANSPORT_LOST
        )

    def as_failure(self) -> dict[str, Any]:
        """The ledger row's failure payload."""
        return {"error": self.error, "error_type": self.error_type}

    def as_result(self) -> dict[str, Any]:
        """The writer's return payload.

        Carries ``status`` so the in-memory outcome lands on the same terminal
        the ledger row gets: ``success: False`` alone reads as a failure, which
        is wrong for a stop that was neutral by design.
        """
        return {
            "success": False,
            **self.as_failure(),
            "status": self.local_status or self.status,
        }


def _error_type(e: BaseException) -> str:
    if isinstance(e, TransportLostError):
        return _TRANSPORT_LOST
    if isinstance(e, CreditStopError):
        return CREDIT_STOP_ERROR_TYPE
    return type(e).__name__


def is_time_limit_stop(result: Any) -> bool:
    """Whether a failed run is a subagent stopped at its own clock.

    The ledger spells it ``error`` so the user hears of it, which leaves the
    error type as the only thing that tells it from a run that broke.
    """
    return (
        isinstance(result, dict)
        and result.get("error_type") == SubagentTimeLimitError.__name__
    )


def is_credit_stop(result: Any) -> bool:
    """Whether a settled task stopped on budget rather than being killed.

    The terminal spelling is a cancel on every surface by design, so the error
    type is the only thing separating a stop whose checkpoint is intact from
    one the user ended deliberately.
    """
    return (
        isinstance(result, dict) and result.get("error_type") == CREDIT_STOP_ERROR_TYPE
    )


def is_restart_stop(result: Any) -> bool:
    """Whether a settled task was stopped by a graceful restart, with its
    checkpoint intact for a resume."""
    return (
        isinstance(result, dict) and result.get("error_type") == RESTART_STOP_ERROR_TYPE
    )


def _settle_status(e: BaseException) -> str:
    """A credit stop settles terminal-neutral, like a cancel and unlike a
    failure: the turn ran out of budget, nothing malfunctioned. Status is the
    only outcome field that reaches every surface, so this is what makes the
    chip, the nav row and the summary line all read "Stopped". It keeps its
    failure payload either way, and the ``credit_stop`` type is what offers the
    task back for resume.
    """
    return "cancelled" if isinstance(e, CreditStopError) else "error"
