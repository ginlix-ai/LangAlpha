"""Retry queued bot messages, one batch at a time, on every worker.

A delivery that met a busy target left its row queued; this is what turns
"queued" into "delivered" without a person sending something else. The claim
is a single guarded UPDATE ... FOR UPDATE SKIP LOCKED, so two workers never
take the same row and a crashed attempt is retried after the interval. The
request key on the posted turn is what keeps a retry from being a second turn.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

#: A message is retried after this long; a target busy for a whole turn is
#: still worth another attempt before it counts as failed.
RETRY_AFTER_SECONDS = 30
#: How many attempts before the row is failed rather than retried forever.
MAX_ATTEMPTS = 5
#: A claimed row is nobody else's until this passes (a delivery can wait out
#: a busy target for DELIVER_WAIT_CAP, so the lease outlives that).
DELIVERY_LEASE_SECONDS = 180
#: Rows claimed per sweep.
BATCH = 20
#: How often each worker scans.
SCAN_INTERVAL_SECONDS = 20.0


async def claim_due(limit: int = BATCH) -> list[Dict[str, Any]]:
    """Take up to ``limit`` queued messages due for a retry.

    The claim stamps the attempt before delivery runs, so a worker that dies
    mid-delivery does not hold the row: it is due again after the interval.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    WITH due AS (
                        SELECT message_id FROM bot_messages
                        WHERE status = 'queued'
                          AND attempts < %s
                          AND (
                              delivering_until IS NULL
                              OR delivering_until < NOW()
                          )
                          AND (
                              last_attempt_at IS NULL
                              OR last_attempt_at < NOW() - make_interval(secs => %s)
                          )
                        ORDER BY created_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE bot_messages m
                    SET attempts = m.attempts + 1,
                        last_attempt_at = NOW(),
                        delivering_until = NOW() + make_interval(secs => %s)
                    FROM due
                    WHERE m.message_id = due.message_id
                    RETURNING m.message_id, m.attempts
                    """,
                    (
                        MAX_ATTEMPTS,
                        RETRY_AFTER_SECONDS,
                        max(1, min(limit, 100)),
                        DELIVERY_LEASE_SECONDS,
                    ),
                )
                return [dict(r) for r in await cur.fetchall()]


class BotMessageDelivery:
    """The per-worker loop that retries queued bot messages."""

    _instance: "BotMessageDelivery | None" = None

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stopping = False

    @classmethod
    def get_instance(cls) -> "BotMessageDelivery":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="bot-message-delivery")
        logger.info("BotMessageDelivery started")

    async def stop(self) -> None:
        self._stopping = True
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        # The first sweep waits one interval: a message enqueued by a live
        # send is already being delivered by that tool call.
        while not self._stopping:
            try:
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                if self._stopping:
                    return
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # a sweep must never kill the worker
                logger.warning("[BOT_MESSAGE] sweep failed: %s", exc)

    async def sweep_once(self) -> int:
        from src.server.database import bot_messages as msg_db
        from src.server.services.bot_messages.delivery import deliver_message

        rows = await claim_due()
        for row in rows:
            try:
                await deliver_message(
                    row["message_id"], attempts=int(row.get("attempts") or 0)
                )
            except Exception as exc:
                logger.warning(
                    "[BOT_MESSAGE] retry of %s failed: %s", row["message_id"], exc
                )
        # A worker that died on its final attempt leaves the row queued with
        # its attempts spent and no lease; fail those rather than keep them.
        try:
            failed = await msg_db.fail_exhausted_messages()
            if failed:
                logger.warning(
                    "[BOT_MESSAGE] failed %s queued message(s) out of attempts",
                    failed,
                )
        except Exception as exc:
            logger.warning("[BOT_MESSAGE] exhausted reconciliation failed: %s", exc)
        return len(rows)
