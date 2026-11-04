"""Storage for the phones a user's pushes go to, and the read a push decides by."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import advisory_key
from src.server.utils.pg_sanitize import SafeJson

_COLUMNS = (
    "push_device_id, user_id, token, platform, bundle_id, environment, "
    "device_name, app_version, preferences, created_at, updated_at, last_sent_at"
)

#: The installs one account keeps; registering past it drops the one
#: registered longest ago. A push goes to every device at once, so this also
#: bounds one push's fan-out, and a token APNs rotated without the old one
#: being refused yet ages out here.
MAX_DEVICES_PER_USER = 10


async def upsert_push_device(
    *,
    user_id: str,
    token: str,
    platform: str,
    bundle_id: str,
    environment: str,
    device_name: Optional[str],
    app_version: Optional[str],
) -> Dict[str, Any]:
    """Register a token, or refresh it.

    A token is one app install, so one registered by another user moves to
    the caller: the phone changed hands or accounts, and the previous account
    must stop reaching it. Its switches were that account's, so they reset.
    The account then keeps its ``MAX_DEVICES_PER_USER`` newest registrations,
    under a lock per account so concurrent registrations cannot pass it. A
    token changing hands also locks the account it leaves, whose own trim
    must not count it, after a lock on the token, so the owner read cannot
    change before its lock is held. Accounts lock in one order, so two
    swaps cannot wait on each other. The stamp is read after the locks
    (``clock_timestamp()``, not ``NOW()``, the transaction's start), so the
    registration that took them last is newest.
    """
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("PUSH_TOKEN", token),),
            )
            await cur.execute(
                "SELECT user_id FROM push_devices WHERE token = %s", (token,)
            )
            prior = await cur.fetchone()
            accounts = {user_id} | ({str(prior["user_id"])} if prior else set())
            for account in sorted(accounts):
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (advisory_key("PUSH_DEVICES", account),),
                )
            await cur.execute(
                f"""
                INSERT INTO push_devices (
                    user_id, token, platform, bundle_id, environment,
                    device_name, app_version, updated_at
                )
                VALUES (%(user_id)s, %(token)s, %(platform)s, %(bundle_id)s,
                        %(environment)s, %(device_name)s, %(app_version)s,
                        clock_timestamp())
                ON CONFLICT (token) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    platform = EXCLUDED.platform,
                    bundle_id = EXCLUDED.bundle_id,
                    environment = EXCLUDED.environment,
                    device_name = EXCLUDED.device_name,
                    app_version = EXCLUDED.app_version,
                    preferences = CASE
                        WHEN push_devices.user_id = EXCLUDED.user_id
                            THEN push_devices.preferences
                        ELSE '{{}}'::jsonb
                    END,
                    last_sent_at = CASE
                        WHEN push_devices.user_id = EXCLUDED.user_id
                            THEN push_devices.last_sent_at
                    END,
                    updated_at = EXCLUDED.updated_at
                RETURNING {_COLUMNS}
                """,
                {
                    "user_id": user_id,
                    "token": token,
                    "platform": platform,
                    "bundle_id": bundle_id,
                    "environment": environment,
                    "device_name": device_name,
                    "app_version": app_version,
                },
            )
            row = await cur.fetchone()
            await cur.execute(
                """
                DELETE FROM push_devices
                WHERE user_id = %(user_id)s
                  AND push_device_id NOT IN (
                      SELECT push_device_id FROM push_devices
                      WHERE user_id = %(user_id)s
                      ORDER BY updated_at DESC, push_device_id DESC
                      LIMIT %(keep)s
                  )
                """,
                {"user_id": user_id, "keep": MAX_DEVICES_PER_USER},
            )
            return row


async def list_push_devices(user_id: str) -> List[Dict[str, Any]]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM push_devices WHERE user_id = %s "
                "ORDER BY created_at, push_device_id",
                (user_id,),
            )
            return list(await cur.fetchall())


async def get_push_device(user_id: str, push_device_id: str) -> Optional[Dict[str, Any]]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM push_devices "
                "WHERE push_device_id = %s AND user_id = %s",
                (push_device_id, user_id),
            )
            return await cur.fetchone()


async def update_push_device_preferences(
    user_id: str, push_device_id: str, preferences: Dict[str, bool]
) -> Optional[Dict[str, Any]]:
    """Merge per-kind switches into the caller's device; None when it is not
    theirs. ``updated_at`` stays the registration's stamp: a switch is not a
    registration, so it neither makes the device newest for the trim nor
    voids a send's prune."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE push_devices
                SET preferences = preferences || %s::jsonb
                WHERE push_device_id = %s AND user_id = %s
                RETURNING {_COLUMNS}
                """,
                (SafeJson(preferences), push_device_id, user_id),
            )
            return await cur.fetchone()


async def delete_push_device(user_id: str, push_device_id: str) -> bool:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM push_devices WHERE push_device_id = %s AND user_id = %s",
                (push_device_id, user_id),
            )
            return cur.rowcount > 0


async def delete_push_device_by_token(token: str, registered_at: datetime) -> bool:
    """Prune a token APNs no longer delivers to, only as the send read it
    (``registered_at``): APNs judged the registration that send knew. A row
    written since keeps, whatever its stamp, since ``NOW()`` dates a write by
    when its transaction began, not when it committed."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM push_devices WHERE token = %s AND updated_at = %s",
                (token, registered_at),
            )
            return cur.rowcount > 0


async def set_push_device_environment(
    push_device_id: str, environment: str, registered_at: datetime
) -> None:
    """Move a device to the host its token answered on, only as the send read
    it (``registered_at``), so a registration written since keeps its own."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE push_devices SET environment = %s, updated_at = NOW() "
                "WHERE push_device_id = %s AND updated_at = %s",
                (environment, push_device_id, registered_at),
            )


async def stamp_push_devices_sent(push_device_ids: List[str]) -> None:
    if not push_device_ids:
        return
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE push_devices SET last_sent_at = NOW() "
                "WHERE push_device_id = ANY(%s::uuid[])",
                (push_device_ids,),
            )


async def get_push_context(*, thread_id: str, run_id: str) -> Optional[Dict[str, Any]]:
    """What a settled run's push is decided by, in one read: the thread's
    title and owner, how far it was seen, its newest run, and the run's own
    START metadata, the query type of its turn and its automation's name.
    None for a thread that is gone."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT ct.title,
                       ct.archived_at,
                       COALESCE(ct.last_seen_run_seq, 0) AS last_seen_run_seq,
                       w.user_id AS owner_id,
                       w.status AS workspace_status,
                       (SELECT MAX(lr.run_seq) FROM conversation_responses lr
                        WHERE lr.conversation_thread_id = ct.conversation_thread_id
                       ) AS latest_run_seq,
                       r.conversation_response_id IS NOT NULL AS run_exists,
                       COALESCE(r.metadata, '{}'::jsonb) AS run_metadata,
                       q.type AS query_type,
                       a.name AS automation_name
                FROM conversation_threads ct
                JOIN workspaces w ON w.workspace_id = ct.workspace_id
                LEFT JOIN conversation_responses r
                  ON r.conversation_response_id = %(run_id)s::uuid
                 AND r.conversation_thread_id = ct.conversation_thread_id
                LEFT JOIN conversation_queries q
                  ON q.conversation_thread_id = r.conversation_thread_id
                 AND q.turn_index = r.turn_index
                LEFT JOIN automations a
                  ON a.automation_id::text = r.metadata->>'automation_id'
                WHERE ct.conversation_thread_id = %(thread_id)s::uuid
                """,
                {"thread_id": thread_id, "run_id": run_id},
            )
            return await cur.fetchone()
