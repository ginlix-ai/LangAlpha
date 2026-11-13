"""What a run tells the user, and the ``user_notification`` outbox executor.

A background run announces its start on the user's feed (``compose_started``,
from START). A settle that might be news gets, from the finalize, a
``user_notification`` job and, when APNs is configured, a ``push`` job
(``services/push/notify``). Each asks ``settle_news`` whether the settle is
news and what it says; this module's executor tells the feed, which the
desktop app shows. Separate jobs, so a slow or failing APNs never holds a
desktop notice back. The wording says which thread and what kind of news,
never what the agent wrote.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

from src.server.contracts.notices import Notice, PushKind, SettleNews
from src.server.contracts.turn_kinds import BACKGROUND_ORIGINS, RunOrigin, run_origin
from src.server.database import push_devices as push_db
from src.server.database.runs.hook_jobs import (
    SURFACE_META,
    USER_NOTIFICATION_HOOK,
    is_unchanged_quiet,
)
from src.server.services.report_back.handoff.keys import ORIGIN_THREAD_META
from src.server.services.thread_lifecycle_feed import (
    build_notification_event,
    publish_user_event_strict,
)

logger = logging.getLogger(__name__)

TITLE_LIMIT = 80
FALLBACK_TITLE = "LangAlpha"

#: The surfaces the web app draws; any other surface a turn ran on is a chat
#: app, which answers the user itself.
APP_SURFACES = frozenset({"web", "market_view"})

_NEEDS_INPUT_BODIES = {
    "approval_required": "Waiting for your approval",
    "user_question": "Has a question for you",
    "credit_pause": "Paused: out of credits",
}
#: What a run that ended well says, by where it came from (``run_origin``).
_DONE_BODIES = {
    "turn": "Finished",
    "report": "Reported back",
    "wakeup": "Checked back in",
    "market_target": "Price alert",
}
#: What a background run says as it starts, by where it came from.
_STARTED_BODIES = {
    "market_target": "Checking a price alert…",
}


def push_title(title: Optional[str]) -> str:
    title = (title or "").strip()
    if not title:
        return FALLBACK_TITLE
    if len(title) > TITLE_LIMIT:
        return title[: TITLE_LIMIT - 1].rstrip() + "…"
    return title


def push_owed(status: str, thread_id: str, run_meta: Mapping[str, Any]) -> bool:
    """Whether a settle is news to the user. Not a quiet check that changed
    nothing. Not a dispatched analyst's ending: the report-back on the
    watching thread is the one that tells, though an analyst waiting on the
    user has no report coming and does tell. Not a turn on a chat app, which
    answers the user there."""
    if status == "completed" and is_unchanged_quiet(run_meta):
        return False
    dispatcher = run_meta.get(ORIGIN_THREAD_META)
    reports_elsewhere = (
        dispatcher
        and dispatcher != thread_id
        and not run_meta.get("report_back_ptc_thread_id")
    )
    if reports_elsewhere and status != "interrupted":
        return False
    surface = run_meta.get(SURFACE_META)
    platform = (surface.get("platform") if isinstance(surface, dict) else None) or ""
    return platform.partition(":")[0] in ("", *APP_SURFACES)


def compose(
    status: Optional[str],
    interrupt_reason: Optional[str],
    origin: RunOrigin,
    title: Optional[str],
    automation_name: Optional[str],
) -> Notice:
    """The kind and wording of a settled run's news."""
    title = push_title(title)
    if status == "interrupted":
        body = _NEEDS_INPUT_BODIES.get(interrupt_reason, "Waiting on you")
        return Notice("needs_input", title, body)
    failed = status == "error"
    if origin == "automation":
        name = (automation_name or "").strip()
        verb = "failed" if failed else "ran"
        return Notice("automation", title, f"{name or 'Automation'} {verb}")
    kind: PushKind = "turn" if origin == "turn" else "wakeup"
    body = "Stopped with an error" if failed else _DONE_BODIES[origin]
    return Notice(kind, title, body)


def compose_started(
    origin: RunOrigin, title: Optional[str], automation_name: Optional[str]
) -> Optional[Notice]:
    """What a background run says as it starts, or None for a run the user
    started, whose own window shows it, and for a wakeup: most of its checks
    find nothing and end quiet, which tells no one, so a wakeup is heard of
    only through a result worth telling."""
    if origin not in BACKGROUND_ORIGINS or origin == "wakeup":
        return None
    if origin == "automation":
        body = f"{(automation_name or '').strip() or 'Automation'} running…"
    else:
        body = _STARTED_BODIES[origin]
    return Notice("started", push_title(title), body)


def settle_job_ids(job: Mapping[str, Any]) -> Optional[tuple[str, str, str]]:
    """A settle job's user, thread and run ids, or None for a malformed row,
    which is logged and dropped."""
    payload = job.get("payload") or {}
    ids = (payload.get("user_id"), payload.get("thread_id"), payload.get("run_id"))
    if not all(ids):
        logger.warning(
            "%s outbox job missing user_id/thread_id/run_id — dropping (job=%s)",
            job.get("hook_type") or "settle news",
            job.get("hook_outbox_id"),
        )
        return None
    return ids


async def settle_news(payload: Mapping[str, Any]) -> Optional[SettleNews]:
    """Whether a settle is news to the user now, and what it says. The
    desktop notice and the push each ask on their own job, at their own time
    and through their own retries, so they share the rules, not one verdict:
    a thread seen between the two asks can stop one and not the other. None
    for a thread gone, archived or not the owner's, a settle ``push_owed``
    passes over, a run the thread has moved past, or one watched during the
    grace delay."""
    user_id, thread_id = payload.get("user_id"), payload.get("thread_id")
    context = await push_db.get_push_context(
        thread_id=thread_id, run_id=payload.get("run_id")
    )
    if context is None:
        return None
    run_meta = context.get("run_metadata") or {}
    status = payload.get("status")
    if (
        not context.get("run_exists")
        or context.get("owner_id") != user_id
        or context.get("workspace_status") == "deleted"
        or context.get("archived_at") is not None
        or not push_owed(status, thread_id, run_meta)
    ):
        return None
    run_seq = payload.get("run_seq")
    if run_seq is not None:
        latest = context.get("latest_run_seq")
        if latest is not None and latest > run_seq:
            # The thread moved on; its newest run tells its own news.
            return None
        if (
            status in ("completed", "error")
            and (context.get("last_seen_run_seq") or 0) >= run_seq
        ):
            # Watched on the web, the desktop or a phone during the grace delay.
            return None
    origin = run_origin(run_meta, context.get("query_type"))
    notice = compose(
        status,
        payload.get("interrupt_reason"),
        origin,
        context.get("title"),
        # The name START stamped, as the start notice said it; the join
        # names a run from before the stamp.
        run_meta.get("automation_name") or context.get("automation_name"),
    )
    return SettleNews(notice=notice, origin=origin)


async def execute_user_notification(job: Dict[str, Any]) -> None:
    """Tell the user's feed of a settle that is news, for the desktop app to
    show. A Redis failure raises, so the outbox retries; a repeat is harmless,
    since the desktop replaces a thread's notice in place."""
    ids = settle_job_ids(job)
    if ids is None:
        return
    user_id, thread_id, run_id = ids
    payload = job.get("payload") or {}
    news = await settle_news(payload)
    if news is None:
        return
    await publish_user_event_strict(
        user_id,
        build_notification_event(
            thread_id=thread_id,
            workspace_id=payload.get("workspace_id"),
            run_id=run_id,
            run_seq=payload.get("run_seq"),
            status=payload.get("status") or "",
            interrupt_reason=payload.get("interrupt_reason"),
            news=news,
        ),
    )


def register_outbox_executors() -> None:
    """Bind the user-notification executor. Must run before the drainer
    starts."""
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(USER_NOTIFICATION_HOOK, execute_user_notification)
