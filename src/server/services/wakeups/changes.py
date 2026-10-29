"""Tell the price monitors that a thread's price wakeups changed.

The same hint, on the same channel, as a change to the thread's old price
targets: a monitor re-reads a thread's watched prices whichever kind moved,
so one message names the thread for both. Best effort, as that hint is: the
monitor's timed reload is what any change is guaranteed to reach, and a fire
on a row that is no longer armed is refused by its own guard.
"""

import logging

from src.server.services.market_targets.changes import targets_changed

logger = logging.getLogger(__name__)


async def wakeups_changed(thread_id: str) -> None:
    try:
        await targets_changed(thread_id)
    except Exception:
        logger.debug("[Wakeups] changed hint not sent thread_id=%s", thread_id, exc_info=True)
