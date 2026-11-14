"""Why a computer is starting or stopping, carried with the work that asked.

The status writes that start and stop a computer sit several calls below the
route, turn or sweep that wanted it, and most of them are shared by every
caller. So the asker names itself once, around the work
(:func:`computer_cause`), and the status hook reads it back
(:func:`current_cause`) to record the transition. A context variable is
copied into every task started inside it, so a lazy start finishing in the
background still carries the cause of the request that scheduled it.

The innermost cause wins: a Chief of Staff turn reaching another computer
records the reach, not the turn.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from enum import Enum
from datetime import datetime, timezone
from typing import Iterator, Optional

from src.server.contracts.turn_kinds import RunOrigin
from src.server.models.computer_cause import Cause, Client, StartCause, StopCause
from src.server.utils.request_client import request_client

#: The signals a warm names (``POST .../start?lazy=true&reason=``), recorded
#: as the transition's ``detail``.
WARM_REASONS = frozenset(
    {"input", "workspace_open", "thread_open", "composer", "onboarding", "create"}
)

# The surfaces our own apps draw, whose app the user agent names; any other
# platform is a chat app reached through its gateway.
_APP_SURFACES = frozenset({"web", "market_view"})

_cause: ContextVar[Optional[Cause]] = ContextVar("computer_cause", default=None)


class _FromRequest(Enum):
    FROM_REQUEST = "from_request"


FROM_REQUEST = _FromRequest.FROM_REQUEST


@contextmanager
def computer_cause(
    name: StartCause | StopCause,
    *,
    client: Optional[Client] | _FromRequest = FROM_REQUEST,
    detail: Optional[str] = None,
) -> Iterator[Cause]:
    """Name what any computer start or stop inside the block is for.

    *client* defaults to the app the request came from. An explicit None
    stays None: a wake or a hand-off has no app, even when it runs inside
    someone's request.
    """
    cause = Cause(
        name=name,
        client=request_client() if client is FROM_REQUEST else client,
        detail=detail[:64] if detail else None,
        at=datetime.now(timezone.utc),
    )
    token = _cause.set(cause)
    try:
        yield cause
    finally:
        _cause.reset(token)


def current_cause() -> Optional[Cause]:
    return _cause.get()


def warm_reason(value: Optional[str]) -> Optional[str]:
    """A warm's signal as a client sent it, ``other`` for one we don't know."""
    if not value:
        return None
    return value if value in WARM_REASONS else "other"


def turn_cause(
    origin: Optional[RunOrigin], *, platform: Optional[str], delegated: bool
) -> tuple[StartCause, Optional[Client]]:
    """What a turn's computer start is for, and the app it came from.

    A turn the user sent is ``turn`` from their app (the web and market view
    surfaces), or from the chat app its platform names; a Chief of Staff hand-off is ``delegated``. A wake, an
    automation or a report-back is its origin (``turn_kinds.RunOrigin``),
    from no app: such a turn inherits its thread's platform, which says where
    it answers, not who asked.
    """
    if origin and origin != "turn":
        return origin, None
    if delegated:
        return "delegated", None
    surface = (platform or "").partition(":")[0]
    if surface == "cli":
        return "turn", "cli"
    if surface and surface not in _APP_SURFACES:
        return "turn", "channel"
    return "turn", request_client()
