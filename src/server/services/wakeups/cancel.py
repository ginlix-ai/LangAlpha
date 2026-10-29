"""The cancels that do not come from the agent's tool: the user's, the
setting's, archiving a thread, deleting a workspace, and delivery's own on
finding a notice nobody would see.

The user's cancel reaches the agent as a row before its next model call
(``cancelled_by`` of ``user`` or ``setting_off``). Archive and workspace
delete write none, since no turn starts there again; each ends the price
targets from before wakeups in its scope too, so its caller makes one call.
"""

import logging
from typing import Dict, Iterable, List, Optional

from src.server.database import market_targets as targets_db
from src.server.database.wakeups import cancel as cancel_db
from src.server.database.wakeups.cancel import CancelOutcome
from src.server.database.wakeups.contract import CancelledBy, is_uuid
from src.server.services.wakeups.changes import wakeups_changed

logger = logging.getLogger(__name__)

_ENDED = ("cancelled", "cancelled_in_flight")


def _price_ended(outcomes: List[CancelOutcome]) -> bool:
    return any(
        o.outcome in _ENDED and o.row is not None and o.row.get("kind") == "price"
        for o in outcomes
    )


async def _publish_price_changes(
    by_thread: Dict[str, List[CancelOutcome]], legacy_threads: Iterable[str] = ()
) -> None:
    """Hint the price monitors at each thread whose watched prices a cancel
    changed: one where a price wakeup ended, or a legacy target did. No
    other kind is watched, so no other change is sent."""
    threads = {t for t, outcomes in by_thread.items() if _price_ended(outcomes)}
    threads.update(str(t) for t in legacy_threads)
    for thread_id in sorted(threads):
        await wakeups_changed(thread_id)


async def _cancel_legacy(thread_id: str, target_id: Optional[str]) -> List[CancelOutcome]:
    rows = await targets_db.cancel_targets(thread_id, target_id=target_id)
    return [
        CancelOutcome(str(r["target_id"]), str(r["target_id"]), "cancelled", None)
        for r in rows
    ]


async def user_cancel(thread_id: str, ids: Optional[List[str]]) -> List[CancelOutcome]:
    """Cancel by id, or everything open on the thread with None.

    An id no wakeup answers to may be a price target from before wakeups,
    which the panel lists beside them, so it gets one more look there; all
    takes those too. A legacy outcome carries no row: its table is not this
    one, and nothing reads it back for the agent.
    """
    outcomes = await cancel_db.cancel_wakeups(thread_id, ids, by="user")
    legacy_changed = False
    if ids is None:
        legacy = await _cancel_legacy(thread_id, None)
        legacy_changed = bool(legacy)
        outcomes = outcomes + legacy
    else:
        resolved: List[CancelOutcome] = []
        for outcome in outcomes:
            if outcome.row is None and is_uuid(outcome.wakeup_id):
                legacy = await _cancel_legacy(thread_id, outcome.wakeup_id)
                if legacy:
                    legacy_changed = True
                    resolved.extend(legacy)
                    continue
            resolved.append(outcome)
        outcomes = resolved
    await _publish_price_changes(
        {thread_id: outcomes}, [thread_id] if legacy_changed else []
    )
    return outcomes


async def setting_off(user_id: str) -> None:
    """Cancel everything the user still has open, on every thread, once the
    setting resolves off. The next turn has no tool to fire into, and each
    thread's agent is told by a row before its next model call. Registered
    as the setting's off hook; delivery cancels whatever a failure here
    leaves, on finding the setting off."""
    await _publish_price_changes(
        await cancel_db.cancel_open(user_id=user_id, by="setting_off")
    )


async def end_wakeup(thread_id: str, wakeup_id: str, by: CancelledBy) -> None:
    """End a wakeup outright, a notice past recall included, as delivery
    finds it: on an archived thread or in a deleted workspace with every
    other one open on the thread, and with the setting off on its own.

    A failed cancel raises rather than reads as done: the caller closes the
    notice's job next, and with the wakeup still open a series would skip
    every slot but its last. The raise leaves the job to be retried."""
    if by in ("archive", "workspace"):
        by_thread = await cancel_db.cancel_open(thread_id=thread_id, by=by)
    else:
        by_thread = {
            thread_id: await cancel_db.cancel_wakeups(thread_id, [wakeup_id], by=by)
        }
    await _publish_price_changes(by_thread)


async def cancel_on_archive(thread_id: str) -> None:
    """End an archived thread's wakeups and legacy price targets: no turn
    starts there again, so a fire would only be dropped. Best effort after
    the archive committed; one that survives a failure here is dropped at
    delivery, as its thread reads archived."""
    by_thread: Dict[str, List[CancelOutcome]] = {}
    legacy: List[str] = []
    try:
        by_thread = await cancel_db.cancel_open(thread_id=thread_id, by="archive")
    except Exception:
        logger.warning(
            "[Wakeups] cancelling wakeups on archive failed thread_id=%s",
            thread_id,
            exc_info=True,
        )
    try:
        if await targets_db.cancel_targets(thread_id):
            legacy = [thread_id]
    except Exception:
        logger.warning(
            "[MarketWatch] cancelling targets on archive failed thread_id=%s",
            thread_id,
            exc_info=True,
        )
    await _publish_price_changes(by_thread, legacy)


async def cancel_on_workspace_delete(workspace_id: str) -> None:
    """End a deleted workspace's wakeups and legacy price targets, on the
    same terms as archive."""
    by_thread: Dict[str, List[CancelOutcome]] = {}
    legacy: List[str] = []
    try:
        by_thread = await cancel_db.cancel_open(workspace_id=workspace_id, by="workspace")
    except Exception:
        logger.warning(
            "[Wakeups] cancelling wakeups on workspace delete failed workspace_id=%s",
            workspace_id,
            exc_info=True,
        )
    try:
        rows = await targets_db.cancel_workspace_targets(workspace_id)
        legacy = [str(row["conversation_thread_id"]) for row in rows]
    except Exception:
        logger.warning(
            "[MarketTargets] cancelling targets on workspace delete failed "
            "workspace_id=%s",
            workspace_id,
            exc_info=True,
        )
    await _publish_price_changes(by_thread, legacy)
