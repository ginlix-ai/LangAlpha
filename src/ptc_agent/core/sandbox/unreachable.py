"""What a tool call's sandbox calls last concluded about whether its machine answered.

Sandbox tools hand every failure back to the model as text, so the exception
that says the machine did not answer never leaves the tool. The sandbox notes
each call's ending here instead, into a watch the tool call's middleware holds
(``ComputerUnreachableStopMiddleware``), which reads it when the call returns.
The watch lives in a context variable set for that one call, so it is the
call's execution context and nothing a request path shares.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError, connect_failure


def unreachable_code(exc: BaseException) -> str | None:
    """The connect code when *exc* comes from a machine that did not answer
    (asleep, offline, its tunnel down), else None.

    Only the machine's own silence counts: a machine that answered and
    refused is its owner's to fix, the tunnel gateway down is the service's
    outage and recovers as one, and a cloud host's outage carries no code a
    client could word the failure from.
    """
    failure = connect_failure(exc)
    if failure is None or not failure.machine_silent:
        return None
    return failure.code


def unanswered_code(exc: BaseException) -> str | None:
    """The connect code of a machine that did not answer, the tunnel gateway
    not answering included, else None."""
    failure = connect_failure(exc)
    return failure.code if failure is not None and not failure.answered else None


def unreachable_computer_id(exc: BaseException) -> str | None:
    """The computer a failure names as the one that did not answer, when a
    raise on its way up recorded it (an :class:`SSHConnectError`'s
    ``computer_id``, or the turn's stop's ``unreachable_computer_id``), else
    None: the caller then knows it as the turn's own."""
    seen: BaseException | None = exc
    for _ in range(32):
        if seen is None:
            return None
        computer_id = (
            seen.computer_id
            if isinstance(seen, SSHConnectError)
            else getattr(seen, "unreachable_computer_id", None)
        )
        if isinstance(computer_id, str) and computer_id:
            return computer_id
        seen = seen.__cause__ or (None if seen.__suppress_context__ else seen.__context__)
    return None


class UnreachableWatch:
    """One tool call's view of one sandbox: the last unanswered failure its
    calls ended on, cleared by a later call the machine served, so a blip the
    call rode out does not read as a machine that is gone."""

    __slots__ = ("failure", "sandbox")

    def __init__(self, sandbox: Any) -> None:
        self.sandbox = sandbox
        self.failure: BaseException | None = None


_watch: ContextVar[UnreachableWatch | None] = ContextVar(
    "sandbox_unreachable_watch", default=None
)


@contextmanager
def watch_unreachable(sandbox: Any) -> Iterator[UnreachableWatch]:
    """Watch *sandbox*'s calls for the duration of one tool call.

    Only that sandbox: a call reaching another of the user's computers that
    is asleep fails on its own, and the turn can still work on its own.
    """
    watch = UnreachableWatch(sandbox)
    token = _watch.set(watch)
    try:
        yield watch
    finally:
        _watch.reset(token)


def note_call(sandbox: Any, exc: BaseException | None) -> None:
    """Record how one of *sandbox*'s calls ended: served (``exc`` None) or
    failed. A machine that answered and refused clears an earlier silence as
    a served call does: it is up. A no-op outside a watched tool call."""
    watch = _watch.get()
    if watch is None or watch.sandbox is not sandbox:
        return
    if exc is None:
        watch.failure = None
    elif unreachable_code(exc) is not None:
        watch.failure = exc
    elif (failure := connect_failure(exc)) is not None and failure.answered:
        watch.failure = None
