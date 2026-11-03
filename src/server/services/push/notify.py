"""The ``push`` outbox executor: what a settled run tells the user's phones.

Always registered, push configured or not: a row whose type has no executor
nacks toward dead, and a server whose key was removed still has rows to drain.
The push says which thread and what kind of news, never what the agent wrote.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from src.server.database import push_devices as push_db
from src.server.database.conversation.threads_read import get_thread_lifecycle_rows
from src.server.database.runs import outbox as outbox_db
from src.server.services.push.apns import ApnsClient, ApnsOutcome, get_apns_client

logger = logging.getLogger(__name__)

PUSH_KINDS = ("turn", "needs_input", "wakeup", "automation")
TITLE_LIMIT = 80
FALLBACK_TITLE = "LangAlpha"

_NEEDS_INPUT_BODIES = {
    "approval_required": "Waiting for your approval",
    "user_question": "Has a question for you",
    "credit_pause": "Paused: out of credits",
}
_WAKE_BODIES = {
    "wakeup": "Checked back in",
    "market_target": "Price alert",
}
#: Payload key listing the devices a retried job already reached.
_SENT_KEY = "sent_device_ids"


class PushRetry(Exception):
    """APNs or the network failed for a device; the outbox retries the job."""


@dataclass(frozen=True)
class PushMessage:
    kind: str
    title: str
    body: str


def push_title(title: Optional[str]) -> str:
    title = (title or "").strip()
    if not title:
        return FALLBACK_TITLE
    if len(title) > TITLE_LIMIT:
        return title[: TITLE_LIMIT - 1].rstrip() + "…"
    return title


def compose(payload: Mapping[str, Any], context: Mapping[str, Any]) -> PushMessage:
    """The kind and wording of a settled run's push, from its outbox payload
    and the thread read the executor made."""
    title = push_title(context.get("title"))
    status = payload.get("status")
    failed = status == "error"
    if status == "interrupted":
        body = _NEEDS_INPUT_BODIES.get(payload.get("interrupt_reason"), "Waiting on you")
        return PushMessage("needs_input", title, body)
    if payload.get("automation_id"):
        name = (context.get("automation_name") or "").strip()
        verb = "failed" if failed else "ran"
        body = f"{name} {verb}" if name else f"Automation {verb}"
        return PushMessage("automation", title, body)
    query_type = context.get("query_type")
    if failed and (payload.get("report_back") or query_type in ("system", *_WAKE_BODIES)):
        return PushMessage("wakeup", title, "Stopped with an error")
    if payload.get("report_back") or query_type == "system":
        return PushMessage("wakeup", title, "Reported back")
    if query_type in _WAKE_BODIES:
        return PushMessage("wakeup", title, _WAKE_BODIES[query_type])
    return PushMessage("turn", title, "Stopped with an error" if failed else "Finished")


def device_wants(device: Mapping[str, Any], kind: str) -> bool:
    """A kind is on unless the device switched it off."""
    return (device.get("preferences") or {}).get(kind) is not False


def badge_count(rows: List[Dict[str, Any]]) -> int:
    """Threads unseen or waiting on the user, as the thread list counts them."""
    return sum(
        1
        for r in rows
        if r.get("branch") == "unseen"
        or (r.get("branch") == "live" and r.get("latest_run_status") == "interrupted")
    )


def _other_environment(environment: str) -> str:
    return "production" if environment == "sandbox" else "sandbox"


async def _deliver(
    client: ApnsClient,
    device: Mapping[str, Any],
    aps_payload: Mapping[str, Any],
    collapse_id: str,
) -> ApnsOutcome:
    """Send to one device and tidy its row by the answer. Returns SENT, RETRY,
    or what else came of it."""
    device_id = str(device["push_device_id"])
    environment = device["environment"]

    async def send(env: str):
        return await client.send(
            token=device["token"],
            bundle_id=device["bundle_id"],
            environment=env,
            payload=aps_payload,
            collapse_id=collapse_id,
        )

    result = await send(environment)
    if result.outcome is ApnsOutcome.BAD_TOKEN:
        # A development-signed build of the production app holds a sandbox
        # token, and the other way round: the token may be the other host's.
        other = _other_environment(environment)
        retried = await send(other)
        if retried.outcome is ApnsOutcome.SENT:
            await push_db.set_push_device_environment(device_id, other)
            return ApnsOutcome.SENT
        if retried.outcome is ApnsOutcome.RETRY:
            return ApnsOutcome.RETRY
        await push_db.delete_push_device_by_token(device["token"])
        logger.info("[PUSH] pruned device %s: a bad token in both environments", device_id)
        return ApnsOutcome.UNREGISTERED
    if result.outcome is ApnsOutcome.UNREGISTERED:
        await push_db.delete_push_device_by_token(device["token"])
        logger.info(
            "[PUSH] pruned device %s: %s %s", device_id, result.status, result.reason
        )
    return result.outcome


async def execute_push(job: Dict[str, Any]) -> None:
    payload = job.get("payload") or {}
    user_id = payload.get("user_id")
    thread_id = payload.get("thread_id") or job.get("conversation_thread_id")
    run_id = payload.get("run_id") or job.get("run_id")
    if not (user_id and thread_id and run_id):
        logger.warning(
            "push outbox job missing user_id/thread_id/run_id — dropping (job=%s)",
            job.get("hook_outbox_id"),
        )
        return
    thread_id, run_id = str(thread_id), str(run_id)
    client = await get_apns_client()
    if client is None:
        return
    devices = await push_db.list_push_devices(user_id)
    if not devices:
        return
    context = await push_db.get_push_context(
        thread_id=thread_id, run_id=run_id, automation_id=payload.get("automation_id")
    )
    if (
        context is None
        or context.get("owner_id") != user_id
        or context.get("workspace_status") == "deleted"
        or context.get("archived_at") is not None
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
    rows, _ = await get_thread_lifecycle_rows(user_id)
    aps_payload = {
        "aps": {
            "alert": {"title": message.title, "body": message.body},
            "sound": "default",
            "badge": badge_count(rows),
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

    sent: List[str] = []
    failed = 0
    for device in targets:
        outcome = await _deliver(client, device, aps_payload, thread_id)
        if outcome is ApnsOutcome.SENT:
            sent.append(str(device["push_device_id"]))
        elif outcome is ApnsOutcome.RETRY:
            failed += 1
    if sent:
        await push_db.stamp_push_devices_sent(sent)
    if failed:
        if sent and job.get("hook_outbox_id"):
            # A retry reaches only the devices that missed it.
            await outbox_db.merge_job_payload(
                str(job["hook_outbox_id"]), {_SENT_KEY: sorted(already_sent | set(sent))}
            )
        raise PushRetry(f"{failed} device(s) of {len(targets)} to retry")


def register_outbox_executors() -> None:
    """Bind the push executor. Must run before the drainer starts."""
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("push", execute_push)
