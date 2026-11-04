"""Fallbacks a provider chain took while serving the enclosing call.

A chain whose covering source failed, or a routed market source that had
nothing, records what answered instead, so the tool that made the call can
name the fallback. Outside :func:`recording` a record goes nowhere.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace

import httpx


@dataclass(frozen=True)
class Fallback:
    """*source* missed *what* for *symbol*; *served_by* answered, or nobody did.

    ``error`` is the failure's class (with the HTTP status when there was one),
    ``None`` when the source answered with nothing.
    """

    symbol: str
    what: str
    source: str
    error: str | None
    served_by: str | None


_taken: ContextVar[list[Fallback] | None] = ContextVar("data_client_fallbacks", default=None)


@contextmanager
def recording() -> Iterator[list[Fallback]]:
    """Collect the fallbacks taken inside the block, tasks it gathers included."""
    taken: list[Fallback] = []
    token = _taken.set(taken)
    try:
        yield taken
    finally:
        _taken.reset(token)


def record(fallback: Fallback) -> None:
    taken = _taken.get()
    if taken is not None and fallback not in taken:
        taken.append(fallback)


def answered(symbol: str, what: str, served_by: str) -> None:
    """Credit *served_by* with a read the enclosing block recorded as unanswered."""
    taken = _taken.get()
    if taken is None:
        return
    taken[:] = [
        replace(f, served_by=served_by)
        if (f.symbol, f.what, f.served_by) == (symbol, what, None) else f
        for f in taken
    ]


@contextmanager
def unrecorded() -> Iterator[None]:
    """Keep the block's fallbacks out of the enclosing record.

    For a side read the tool swallows: its misses are not the answer's.
    """
    token = _taken.set(None)
    try:
        yield
    finally:
        _taken.reset(token)


def taken() -> list[Fallback]:
    """The fallbacks recorded so far in the enclosing block."""
    return list(_taken.get() or ())


def error_label(exc: BaseException) -> str:
    """The failure's class, and its status when there was one; never its message.

    A message can carry a request URL or a user's search text.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{type(exc).__name__} {exc.response.status_code}"
    return type(exc).__name__


class SourceFailed(Exception):
    """A source's failure in words a caller may show: its name, class and status.

    ``status_code`` keeps the status for the callers that map it.
    """

    def __init__(self, source: str, exc: BaseException) -> None:
        super().__init__(f"{source} request failed ({error_label(exc)})")
        self.status_code = (
            exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
        )


def surfaced(source: str, exc: Exception) -> Exception:
    """*exc* as a chain raises it: an httpx error's own message carries the
    request URL, so it becomes a :class:`SourceFailed` caused by it."""
    if not isinstance(exc, httpx.HTTPError):
        return exc
    failed = SourceFailed(source, exc)
    failed.__cause__ = exc
    return failed
