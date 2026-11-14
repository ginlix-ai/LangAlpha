"""Telling the host application that a reconnect found, and maybe started, a
sandbox.

The reconnect is the only code that sees the provider's state before a start
(stopped, archived, paused) and times the provider's own start call, which
is what separates a slow archive restore from a slow setup of ours. The
library knows nothing of the server's records, so it reports to one listener
the server registers at startup. A listener failure is never the reconnect's.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import structlog

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class RuntimeStartReport:
    sandbox_id: str
    provider_kind: Optional[str]
    #: The provider's state when the reconnect read it; an archive in
    #: progress reads ``archiving``.
    state_before: str
    #: Whether this reconnect called the provider's start. False when the
    #: sandbox was running, or another caller's start brought it up: that
    #: caller's reconnect reports it.
    started: bool
    #: From the state read to the runtime up: the provider's start and any
    #: wait for a start or stop already under way. None when not started.
    start_ms: Optional[int]


_listener: Optional[Callable[[RuntimeStartReport], None]] = None


def set_runtime_start_listener(
    listener: Optional[Callable[[RuntimeStartReport], None]],
) -> None:
    global _listener
    _listener = listener


def report_runtime_start(report: RuntimeStartReport) -> None:
    listener = _listener
    if listener is None:
        return
    try:
        listener(report)
    except Exception as e:
        logger.debug("Runtime start listener raised; ignoring", error=str(e))
