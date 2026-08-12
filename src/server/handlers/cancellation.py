"""Shared request-cancellation handling.

A user Stop (e.g. ``/cancel`` cancelling an in-flight ``/compact`` or ``/offload``
task) or a client disconnect raises ``asyncio.CancelledError`` deep inside a
handler. Because that is a ``BaseException``, it slips past ``except Exception``
and bubbles to ASGI as a raw 500 with no JSON body — which clients mislabel as a
genuine failure. ``cancellation_as_http`` is the single, reusable mechanism that
converts it to an honest 409, so no handler needs its own per-case clause.
"""

import asyncio
import functools
import logging

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def cancellation_as_http(verb: str):
    """Decorator: convert a request-task cancellation into a clean 409.

    Wrap any stoppable handler with ``@cancellation_as_http("<verb>")``. The
    handler's own ``finally`` still runs during cancellation (releasing guards)
    before the wrapper converts the error. The 409 carries
    ``{"code": "request_cancelled", "verb": ...}`` so the frontend can show a
    "stopped" notice instead of a failure. Not re-raised: the cancelled request
    task is itself the cancel target, so nothing awaits its cancellation.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                logger.info(f"Request cancelled by user stop: {verb}")
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "request_cancelled",
                        "verb": verb,
                        "message": "Request cancelled.",
                    },
                )

        return wrapper

    return decorator
