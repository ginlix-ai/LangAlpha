"""What the agent is told of the wakeups the user cancelled on its thread.

The agent's cancel notice (``middleware/wakeups.py``) reads through this
port, bound to one thread at the agent build: the middleware names what its
history already holds and writes the rows, and the rows, their words and the
wakeup table stay here.
"""

from __future__ import annotations

from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any, Dict, List

from ptc_agent.agent.middleware.wakeups import CancelGroup
from src.server.database.wakeups import cancel as cancel_db
from src.tools.wakeups.describe import cancel_label
from src.tools.wakeups.wording import (
    CANCEL_ROW_MANY,
    CANCEL_ROW_ONE,
    CANCEL_ROW_SETTING_OFF,
)


def _row_text(cancelled_by: str, labels: List[str]) -> str:
    lines = "\n".join(f"- {label}" for label in labels)
    if cancelled_by == "setting_off":
        return CANCEL_ROW_SETTING_OFF.format(lines=lines)
    if len(labels) == 1:
        return CANCEL_ROW_ONE.format(label=labels[0])
    return CANCEL_ROW_MANY.format(lines=lines)


class ThreadCancelNotices:
    """The user's cancels on one thread, labelled in the turn's zone."""

    def __init__(self, thread_id: str, user_tz: str) -> None:
        self._thread_id = thread_id
        self._user_tz = user_tz

    async def untold(self, told: Collection[str]) -> tuple[CancelGroup, ...]:
        rows = [
            row
            for row in await cancel_db.user_cancels(self._thread_id)
            if str(row["wakeup_id"]) not in told
        ]
        if not rows:
            return ()
        now = datetime.now(UTC)
        # The read keeps only the two cancellers the agent is told of.
        by: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            by.setdefault(str(row["cancelled_by"]), []).append(row)
        return tuple(
            CancelGroup(
                wakeup_ids=tuple(str(row["wakeup_id"]) for row in group),
                cancelled_by=cancelled_by,
                text=_row_text(
                    cancelled_by,
                    [cancel_label(row, self._user_tz, now) for row in group],
                ),
            )
            for cancelled_by, group in by.items()
        )
