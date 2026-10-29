"""Utility helpers for background subagent middleware."""

import logging
from collections.abc import Awaitable, Callable

from ptc_agent.agent.transcript.classify import MARKET_ALERT_SENDER, WAKEUP_SENDER

logger = logging.getLogger(__name__)

#: Who sent the steers waiting, empty when none: truthy is "wake up".
MessageChecker = Callable[[], Awaitable[list[str]]]

#: The outbox hook type each notice nobody sent waits as, by the sender a
#: checker names it with.
_ALERT_HOOK_TYPES = {MARKET_ALERT_SENDER: "market_target", WAKEUP_SENDER: "wakeup"}


def config_own_run_id(config: dict | None) -> str | None:
    """The run stamped into this invocation's config metadata (v4 2.4c) —
    the same identity SteeringMiddleware filters deliveries by."""
    return ((config or {}).get("metadata") or {}).get("run_id")


async def build_message_checker(
    thread_id: str | None,
    own_run_id: str | None = None,
    *,
    alert_senders: frozenset[str] = frozenset(),
) -> MessageChecker | None:
    """An async closure: who sent the steers waiting that this run would take
    in, empty when none.

    Mirrors what SteeringMiddleware takes (``pending_steer_senders`` shares its
    predicate): counting a steer this run cannot take would wake a wait or
    re-invoke the agent for a message that never arrives. A failed read
    counts as nothing waiting. None without a thread, so callers skip it.
    For a run whose steering takes reports in, a notice of a sender in
    ``alert_senders`` waiting with no steer counts as that sender: a market
    target's alert held until the tasks finish could arrive after the price
    moved on, and the agent set a wakeup to act on when it fires, not after
    its tasks end.
    """
    if not thread_id:
        return None

    async def checker() -> list[str]:
        from src.server.database.runs import outbox as outbox_db

        try:
            senders = await outbox_db.pending_steer_senders(thread_id, own_run_id)
            asked = {
                sender: hook_type
                for sender, hook_type in _ALERT_HOOK_TYPES.items()
                if sender in alert_senders
            }
            if senders or not asked:
                return senders
            waiting = await outbox_db.steerable_alert_senders(thread_id, asked)
            return [sender for sender in asked if sender in waiting]
        except Exception:
            logger.warning(
                f"[Steering] Checking {thread_id} for steers failed", exc_info=True
            )
            return []

    return checker
