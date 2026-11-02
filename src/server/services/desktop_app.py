"""The server built into the open-source desktop app, which the app starts and
stops with itself (``LANGALPHA_DESKTOP_APP=1``, set by the app)."""

from __future__ import annotations

import asyncio
import logging
import os

logger = logging.getLogger(__name__)


def in_desktop_app() -> bool:
    return os.environ.get("LANGALPHA_DESKTOP_APP") == "1"


async def stop_this_computer() -> None:
    """Stop the computers on this machine as the app's server goes, as their
    Stop button would.

    The app tells its user the agent runs while the app is open, so a quit
    leaves none of its computer running: the MCP supervisor and any command
    still going end with it. No backup first, since the files are already on
    this disk and the stop has to finish inside the app's grace for the server.
    """
    if not in_desktop_app():
        return
    from src.server.database.computer import get_computers_by_status
    from src.server.models.computer import ComputerStatus
    from src.server.services.computer_manager import ComputerManager

    running = [
        str(c["computer_id"])
        for c in await get_computers_by_status(ComputerStatus.RUNNING)
        if c.get("kind") == "local"
    ]
    if not running:
        return
    manager = ComputerManager.get_instance()
    results = await asyncio.gather(
        *(manager.stop_computer(cid, back_up=False) for cid in running),
        return_exceptions=True,
    )
    for computer_id, result in zip(running, results):
        if isinstance(result, BaseException):
            logger.warning("computer %s not stopped with the app: %s", computer_id, result)
        else:
            logger.info("computer %s stopped with the app", computer_id)
