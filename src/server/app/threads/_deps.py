"""Shared plumbing for the threads package: router, logger, task refs."""

import asyncio
import logging
import os

from fastapi import APIRouter

# Pinned to the pre-split module path so log routing/filtering keys keep working.
logger = logging.getLogger("src.server.app.threads")


# Strong references to background dispatch tasks to prevent GC.
# Tasks remove themselves via done callback.
_background_tasks: set[asyncio.Task] = set()


def _get_service_token() -> str:
    """Read INTERNAL_SERVICE_TOKEN at call time (not import time)."""
    return os.getenv("INTERNAL_SERVICE_TOKEN", "")


def _track_task(task: asyncio.Task) -> None:
    """Hold a strong reference to *task* until it completes."""
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


# Single router for all thread operations
router = APIRouter(prefix="/api/v1/threads", tags=["Threads"])

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}
