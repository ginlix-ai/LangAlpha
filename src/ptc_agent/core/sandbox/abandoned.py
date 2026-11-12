"""Sandboxes a create gave up on, handed to whoever can delete them later.

A provider that answers a create only once the sandbox runs can still be
building one when the caller stops waiting, and can refuse to delete it until
it settles. Nothing holds such a sandbox, so the provider names the create
and, when it cannot delete what the name finds, reports the name here. The
server installs a sink in every worker that records it for a sweep to finish
(:func:`set_abandoned_sink`); with none, it is logged and the provider's own
auto-stop is all that bounds it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import structlog

logger = structlog.get_logger(__name__)

AbandonedSink = Callable[[str, str, str], Awaitable[None]]

_sink: AbandonedSink | None = None


def set_abandoned_sink(sink: AbandonedSink | None) -> None:
    """Install the recorder for abandoned sandboxes in this worker."""
    global _sink
    _sink = sink


async def report_abandoned(kind: str, ref: str, reason: str) -> None:
    """Hand *ref*, a sandbox of provider *kind* no one holds, to the sink.

    Never raises: the caller is already failing the create, and a lost
    report costs the sandbox's runtime until auto-stop, not the caller's
    error.
    """
    if _sink is None:
        logger.warning(
            "Abandoned sandbox has no recorder; left to the provider's auto-stop",
            kind=kind,
            sandbox=ref,
            reason=reason,
        )
        return
    try:
        await _sink(kind, ref, reason)
    except Exception:
        logger.error(
            "Could not record an abandoned sandbox; left to the provider's auto-stop",
            kind=kind,
            sandbox=ref,
            reason=reason,
            exc_info=True,
        )
