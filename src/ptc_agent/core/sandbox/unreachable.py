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

from ptc_agent.core.sandbox.providers.ssh_errors import connect_failure


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
    failed. A no-op outside a watched tool call."""
    watch = _watch.get()
    if watch is None or watch.sandbox is not sandbox:
        return
    if exc is None:
        watch.failure = None
    elif unreachable_code(exc) is not None:
        watch.failure = exc
