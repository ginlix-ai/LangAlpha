"""The ``push`` outbox executor: a settled run's news on the user's phones.

Whether a settle is news and what it says is ``services/notices.settle_news``,
which the desktop's ``user_notification`` job asks too; this executor sends
it through APNs. Always registered, push configured or not: a row whose type
has no executor nacks toward dead, and a server whose key was removed still
has rows to drain.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Mapping

from src.server.contracts.notices import PushKind
from src.server.database import push_devices as push_db
from src.server.database.conversation.threads_read import count_threads_needing_attention
from src.server.database.runs import outbox as outbox_db
from src.server.services.notices import settle_job_ids, settle_news
from src.server.services.push.apns import ApnsClient, ApnsOutcome, get_apns_client
from src.server.services.push.config import push_available

logger = logging.getLogger(__name__)

#: Payload key listing the devices a retried job already reached.
_SENT_KEY = "sent_device_ids"
#: A job starts no send this long after it starts. Its last request, at most
#: two of ``REQUEST_SECONDS`` with a renewed token and a reconnect, then ends
#: well inside the outbox's 60 s lease, which the job renews before it records
#: what it sent; the devices left over retry.
SEND_SECONDS = 20.0


class PushRetry(Exception):
    """APNs or the network failed for a device; the outbox retries the job."""


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
    ids = settle_job_ids(job)
    if ids is None:
        return
    user_id, thread_id, run_id = ids
    payload = job.get("payload") or {}
    client = await get_apns_client()
    if client is None:
        if push_available():
            # Configured, but the key would not load: retry, it may be mounted yet.
            raise PushRetry("APNs is configured but its key could not be read")
        return
    devices = await push_db.list_push_devices(user_id)
    if not devices:
        return
    news = await settle_news(payload)
    if news is None:
        return
    message = news.notice
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
