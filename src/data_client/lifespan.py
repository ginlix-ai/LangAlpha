"""Server lifespan that closes the shared data clients a process owns."""

import logging
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


def closing_lifespan(*closers: Callable[[], Awaitable[None]]):
    """A FastMCP lifespan that awaits every closer on shutdown, each guarded.

    Sequential awaits in one ``finally`` stop at the first failure and leak
    the rest, and nesting ``fmp_lifespan`` would too: its ``yield`` is
    unguarded.
    """

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            for close in closers:
                try:
                    await close()
                except Exception:  # noqa: BLE001 — one failed close must not strand the others
                    logger.exception("shutdown close failed: %s", close.__name__)

    return lifespan
