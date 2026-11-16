"""A firing that waits for its computer: a machine of the user's own, asleep
or offline, which is not the automation failing.

The wait lives in Postgres, not in a coroutine. ``park_for_computer`` parks
the firing naming the computer, and every worker's scheduler sweep
(``sweep_parked``) starts it again through the ordinary start once that
computer is back, or skips it once a later firing takes the wait over or its
deadline passes. No strike either way.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from src.server.database import automation as auto_db
from src.server.database import automation_executions as exec_db
from src.server.services.automation_settlement import Outcome, runs_once, settle

logger = logging.getLogger(__name__)

# A machine of the user's own that can't be reached may be asleep for the
# night: a firing waits for it, until the automation's next firing comes
# along and at most this long.
COMPUTER_WAIT_CAP_SECONDS = 24 * 60 * 60
# An automation that runs once (``runs_once``) has no next firing to stand in
# for this one, so a late run beats none: it waits as long as a notification
# does for its computer.
ONE_SHOT_WAIT_CAP_SECONDS = 7 * 24 * 60 * 60
# A run that waited for its computer and starts at least this long after it
# was due tells its agent so.
_LATE_AFTER = timedelta(minutes=15)

# Automations a firing may still run under after a wait. Any other status,
# unless the firing started under it (a manual run of a paused automation),
# means it was paused, disabled or finished meanwhile.
_RUNNABLE = frozenset({"active", "executing"})


def stopped_meanwhile(fresh: Optional[Dict[str, Any]], status_then: Optional[str]) -> bool:
    """Whether the automation was paused, disabled, finished or removed while
    a firing of it waited, ``fresh`` being it now (None: removed) and
    ``status_then`` its status when the wait began."""
    return fresh is None or (
        fresh["status"] not in _RUNNABLE and fresh["status"] != status_then
    )


async def late_note(automation: Dict[str, Any], row: Dict[str, Any]) -> Optional[str]:
    """What a run released after waiting for its computer tells its agent,
    when it starts well after it was due: when that was, and that work which
    depends on the hour should say so. None for a run on time, or one that
    never waited for its computer."""
    due = row.get("scheduled_at")
    if row.get("wait_reason") != "computer_unreachable" or due is None:
        return None
    if datetime.now(timezone.utc) - due < _LATE_AFTER:
        return None
    from ptc_agent.agent.prompts import format_current_time
    from src.server.database.computer import get_computer

    computer = None
    if awaited := row.get("awaits_computer"):
        try:
            computer = await get_computer(str(awaited))
        except Exception as e:  # noqa: BLE001 - the note reads without the name
            logger.warning(f"[AUTOMATION_WAIT] Could not read the awaited computer: {e}")
    name = (computer or {}).get("name")
    waited_for = f'computer "{name}"' if name else "its computer"
    return (
        f"This run was due at {format_current_time(due, automation.get('timezone'))}. "
        f"It waited for {waited_for}, which couldn't be reached until this run "
        "started. If the instruction depends on when it runs, such as a morning "
        "briefing or a price move, say the run is late and work from current data."
    )


async def park_for_computer(
    automation: Dict[str, Any], execution_id: str, workspace_id: str
) -> bool:
    """Park the firing as ``waiting`` while the computer its turn runs on
    can't be reached (``computer_away``); True when it parked, or was settled
    meanwhile, and nothing more runs here. A computer that answers costs one
    read and no wait."""
    from src.server.services.ssh_computers import computer_away

    try:
        away = await computer_away(workspace_id)
    except Exception as e:
        # No verdict: the turn finds out at its connect, which skips it all
        # the same.
        logger.warning(
            f"[AUTOMATION_WAIT] Looking at the computer failed, running: "
            f"execution_id={execution_id} error={e}"
        )
        return False
    if away is None:
        return False
    parked = await exec_db.park_execution(
        execution_id,
        awaits_computer=str(away["computer_id"]),
        parked_status=automation["status"],
        wait_seconds=(
            ONE_SHOT_WAIT_CAP_SECONDS if runs_once(automation) else COMPUTER_WAIT_CAP_SECONDS
        ),
    )
    if parked:
        logger.info(
            f"[AUTOMATION_WAIT] Waiting for the computer: "
            f"execution_id={execution_id} computer_id={away['computer_id']}"
        )
    return True


Dispatch = Callable[..., None]


async def sweep_parked(dispatch: Dispatch, *, stopping: Callable[[], bool]) -> None:
    """Start the firings parked for a computer that is back, through the
    ordinary start (``dispatch``), and skip those a later firing took the
    wait over from or that waited out their deadline. A release is one guarded statement, so of the workers whose sweeps
    reach a row exactly one starts it. Nothing is started once ``stopping``
    holds."""
    try:
        rows = await exec_db.list_parked_executions()
    except Exception as e:
        logger.error(f"[AUTOMATION_WAIT] Parked-firing sweep failed: {e}")
        return
    for row in rows:
        execution_id = str(row["automation_execution_id"])
        try:
            await sweep_one(row, dispatch, stopping=stopping)
        except Exception as e:
            logger.error(
                f"[AUTOMATION_WAIT] Sweeping a parked firing failed: "
                f"execution_id={execution_id} error={e}"
            )


async def sweep_one(
    row: Dict[str, Any], dispatch: Dispatch, *, stopping: Callable[[], bool]
) -> None:
    """The sweep's move on one parked firing, by its verdict."""
    execution_id = str(row["automation_execution_id"])
    automation = await auto_db.get_automation(str(row["automation_id"]), row["user_id"])
    if automation is None:
        return
    # Past its deadline it is given up even with the computer back, as when
    # no sweep looked at the deadline itself (a deploy, the scheduler down).
    if row["superseded"] or row["expired"]:
        await settle(
            automation, execution_id, Outcome.SKIPPED,
            skip_reason="computer_unreachable", parked=True,
        )
        return
    if not row["reachable"] or stopping():
        return
    if stopped_meanwhile(automation, row["parked_status"] or automation["status"]):
        await settle(
            automation, execution_id, Outcome.SKIPPED, skip_reason="user", parked=True
        )
        return
    if not await exec_db.release_parked_execution(execution_id):
        return
    logger.info(
        f"[AUTOMATION_WAIT] Starting a firing whose computer is back: "
        f"execution_id={execution_id}"
    )
    dispatch(
        automation,
        execution_id,
        name=f"automation_exec_{str(automation['automation_id'])[:8]}",
    )
