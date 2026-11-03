"""The ``push`` outbox executor: what a settled run tells the user's phones.

The finalize enqueues a push for every settle that might be news; this
executor decides, from the run row and the thread, whether it is and what it
says. Always registered, push configured or not: a row whose type has no
executor nacks toward dead, and a server whose key was removed still has rows
to drain. The push says which thread and what kind of news, never what the
agent wrote.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Literal, Mapping, Optional, get_args

from src.server.database import push_devices as push_db
from src.server.database.conversation.threads_read import count_threads_needing_attention
from src.server.database.runs import outbox as outbox_db
from src.server.database.runs.hook_jobs import SURFACE_META, is_unchanged_quiet
from src.server.services.push.apns import ApnsClient, ApnsOutcome, get_apns_client
from src.server.services.push.config import push_available
from src.server.services.report_back.handoff.keys import ORIGIN_THREAD_META

logger = logging.getLogger(__name__)

PushKind = Literal["turn", "needs_input", "wakeup", "automation"]
PUSH_KINDS: tuple[PushKind, ...] = get_args(PushKind)
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
#: What a run that ended well says, by where it came from (``_origin``).
_DONE_BODIES = {
    "turn": "Finished",
    "report": "Reported back",
    "wakeup": "Checked back in",
    "market_target": "Price alert",
}
#: Payload key listing the devices a retried job already reached.
_SENT_KEY = "sent_device_ids"
#: A job starts no send this long after it starts. Its last request, at most
#: two of ``REQUEST_SECONDS`` with a renewed token and a reconnect, then ends
#: well inside the outbox's 60 s lease, which the job renews before it records
#: what it sent; the devices left over retry.
SEND_SECONDS = 20.0


class PushRetry(Exception):
    """APNs or the network failed for a device; the outbox retries the job."""


@dataclass(frozen=True)
class PushMessage:
    kind: PushKind
    title: str
    body: str


def push_title(title: Optional[str]) -> str:
    title = (title or "").strip()
    if not title:
        return FALLBACK_TITLE
    if len(title) > TITLE_LIMIT:
        return title[: TITLE_LIMIT - 1].rstrip() + "…"
    return title


def push_owed(status: str, thread_id: str, run_meta: Mapping[str, Any]) -> bool:
    """Whether a settle is news to the user's phones. Not a quiet check that
    changed nothing. Not a dispatched analyst's ending: the report-back on the
    watching thread is the one that pushes, though an analyst waiting on the
    user has no report coming and does push. Not a turn on a chat app, which
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


def _origin(run_meta: Mapping[str, Any], query_type: Optional[str]) -> str:
    """Where a run came from: an automation, a report-back, a wake of some
    kind (its query type), or the user's own turn."""
    if run_meta.get("automation_id"):
        return "automation"
    if run_meta.get("report_back_ptc_thread_id") or query_type == "system":
        return "report"
    if query_type in ("wakeup", "market_target"):
        return query_type
    return "turn"


def compose(payload: Mapping[str, Any], context: Mapping[str, Any]) -> PushMessage:
    """The kind and wording of a settled run's push, from its outbox payload
    and the read the executor made."""
    title = push_title(context.get("title"))
    status = payload.get("status")
    if status == "interrupted":
        body = _NEEDS_INPUT_BODIES.get(payload.get("interrupt_reason"), "Waiting on you")
        return PushMessage("needs_input", title, body)
    failed = status == "error"
    origin = _origin(context.get("run_metadata") or {}, context.get("query_type"))
    if origin == "automation":
        name = (context.get("automation_name") or "").strip()
        verb = "failed" if failed else "ran"
        return PushMessage("automation", title, f"{name or 'Automation'} {verb}")
    kind: PushKind = "turn" if origin == "turn" else "wakeup"
    body = "Stopped with an error" if failed else _DONE_BODIES[origin]
    return PushMessage(kind, title, body)


def device_wants(device: Mapping[str, Any], kind: PushKind) -> bool:
    """A kind is on unless the device switched it off."""
    return (device.get("preferences") or {}).get(kind) is not False


def _other_environment(environment: str) -> str:
    return "production" if environment == "sandbox" else "sandbox"


async def _deliver(
    client: ApnsClient,
    device: Mapping[str, Any],
    aps_payload: Mapping[str, Any],
    collapse_id: str,
    deadline: float,
) -> ApnsOutcome:
    """Send to one device and tidy its row by the answer."""
    device_id = str(device["push_device_id"])
    environment = device["environment"]

    async def send(env: str):
        return await client.send(
            token=device["token"],
            bundle_id=device["bundle_id"],
            environment=env,
            payload=aps_payload,
            collapse_id=collapse_id,
            deadline=deadline,
        )

    result = await send(environment)
    if result.outcome is ApnsOutcome.BAD_TOKEN:
        # A development-signed build of the production app holds a sandbox
        # token, and the other way round: the token may be the other host's.
        other = _other_environment(environment)
        result = await send(other)
        if result.outcome is ApnsOutcome.SENT:
            await push_db.set_push_device_environment(device_id, other, device["updated_at"])
    if result.outcome in (ApnsOutcome.BAD_TOKEN, ApnsOutcome.UNREGISTERED):
        # A token neither host takes, or one APNs retired. Any other refusal
        # says nothing about the token, so the device stays.
        await push_db.delete_push_device_by_token(device["token"], device["updated_at"])
        logger.info(
            "[PUSH] pruned device %s: %s %s", device_id, result.status, result.reason
        )
        return ApnsOutcome.UNREGISTERED
    return result.outcome


async def execute_push(job: Dict[str, Any]) -> None:
    # From the claim, so the reads before the sends count against the lease.
    deadline = time.time() + SEND_SECONDS
    payload = job.get("payload") or {}
    user_id, thread_id, run_id = (
        payload.get("user_id"),
        payload.get("thread_id"),
        payload.get("run_id"),
    )
    if not (user_id and thread_id and run_id):
        logger.warning(
            "push outbox job missing user_id/thread_id/run_id — dropping (job=%s)",
            job.get("hook_outbox_id"),
        )
        return
    client = await get_apns_client()
    if client is None:
        if push_available():
            # Configured, but the key would not load: retry, it may be mounted yet.
            raise PushRetry("APNs is configured but its key could not be read")
        return
    devices = await push_db.list_push_devices(user_id)
    if not devices:
        return
    context = await push_db.get_push_context(thread_id=thread_id, run_id=run_id)
    if (
        context is None
        or not context.get("run_exists")
        or context.get("owner_id") != user_id
        or context.get("workspace_status") == "deleted"
        or context.get("archived_at") is not None
        or not push_owed(payload.get("status"), thread_id, context.get("run_metadata") or {})
    ):
        return
    run_seq = payload.get("run_seq")
    if run_seq is not None:
        latest = context.get("latest_run_seq")
        if latest is not None and latest > run_seq:
            # The thread moved on; its newest run tells its own news.
            return
        if (
            payload.get("status") in ("completed", "error")
            and (context.get("last_seen_run_seq") or 0) >= run_seq
        ):
            # Watched on the web or another phone during the grace delay.
            return

    message = compose(payload, context)
    already_sent = set(payload.get(_SENT_KEY) or ())
    targets = [
        d
        for d in devices
        if device_wants(d, message.kind) and str(d["push_device_id"]) not in already_sent
    ]
    if not targets:
        return
    aps_payload = {
        "aps": {
            "alert": {"title": message.title, "body": message.body},
            "sound": "default",
            "badge": await count_threads_needing_attention(user_id),
            "thread-id": thread_id,
        },
        # expo-notifications reads a remote push's data from userInfo.body.
        "body": {
            "thread_id": thread_id,
            "workspace_id": payload.get("workspace_id"),
            "run_id": run_id,
            "kind": message.kind,
        },
    }

    # The devices are sent to at once. A database error on one counts as a
    # miss to retry, so the devices already reached are still recorded.
    outcomes = await asyncio.gather(
        *(_deliver(client, d, aps_payload, thread_id, deadline) for d in targets),
        return_exceptions=True,
    )
    sent = [
        str(d["push_device_id"])
        for d, outcome in zip(targets, outcomes)
        if outcome is ApnsOutcome.SENT
    ]
    failed = 0
    for outcome in outcomes:
        if isinstance(outcome, Exception):
            logger.warning("[PUSH] a device's send failed: %s", type(outcome).__name__)
        if outcome is ApnsOutcome.RETRY or isinstance(outcome, Exception):
            failed += 1
    job_id = job.get("hook_outbox_id")
    if sent and job_id and job.get("attempts") is not None:
        # Fresh runway for the writes below and the ack, so no other worker
        # claims the job and sends again before what this one sent is recorded.
        # A lost fence changes nothing here: recording it still helps.
        from src.server.services.hook_outbox import LEASE_SECONDS

        await outbox_db.extend_job_lease(
            str(job_id), LEASE_SECONDS, attempts=job["attempts"]
        )
    if failed and sent and job_id:
        # A retry reaches only the devices that missed it.
        await outbox_db.add_to_job_payload_list(str(job_id), _SENT_KEY, sent)
    if sent:
        await push_db.stamp_push_devices_sent(sent)
    if failed:
        raise PushRetry(f"{failed} device(s) of {len(targets)} to retry")


def register_outbox_executors() -> None:
    """Bind the push executor. Must run before the drainer starts."""
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("push", execute_push)
