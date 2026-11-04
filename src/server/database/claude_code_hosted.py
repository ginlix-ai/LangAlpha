"""Claude Code on a user's cloud computers: which credential the user chose
for it (``claude_code_custody``, migration 105), an operator's block on
storing their setup token, and which of their turns run there now.

Every choice is written by ``write_custody``, which deletes the stored token
in the transaction that records any choice but ``stored``, under the token's
writer lock (``connection_change``), so no moment holds a token without the
choice that allows it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.server.database.encryption import get_encryption_key
from src.server.database.oauth_tokens import CLAUDE_CODE_PROVIDER, connection_change
from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

Mode = Literal["local", "api_key", "stored"]
#: What a turn's run row carries in its metadata while its Claude Code CLI
#: runs in a worker for a cloud computer, which ``live_worker_turns`` counts.
WORKER_TURN = {"claude_code_worker": True}
#: What it carries while its CLI runs on the setup token the user stored,
#: whose turns ``live_stored_turns`` finds when the token goes.
STORED_TURN = {"claude_code_stored": True}


class StoringBlocked(Exception):
    """``stored`` chosen for a user an operator blocked storing a token for."""


@dataclass(frozen=True)
class Custody:
    """The user's choice, and whether an operator blocked storing their token."""

    mode: Mode = "local"
    blocked: bool = False


async def get_custody(user_id: str, *, conn=None) -> Custody:
    """The user's choice, ``local`` while they have made none."""
    async with get_db_connection(conn) as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT mode, blocked_at FROM claude_code_custody WHERE user_id = %s", (user_id,))
        row = await cur.fetchone()
    return Custody(row["mode"], row["blocked_at"] is not None) if row else Custody()


async def write_custody(
    user_id: str, mode: Mode, *, disclosure: str | None = None, block_reason: str | None = None,
) -> str | None:
    """Record the user's choice ``mode``: ``stored`` with the version of the
    consent text they accepted, which a later choice keeps on record, and
    with ``block_reason`` an operator's block, which only ``unblock`` clears, and
    which refuses ``stored`` in the same transaction (``StoringBlocked``). Any
    other mode deletes the stored setup token in the same transaction, and
    returns it, for its check key, or None where it cannot be read: a token
    no longer readable under the current key is deleted all the same."""
    async with connection_change(user_id, CLAUDE_CODE_PROVIDER) as conn, conn.cursor(row_factory=dict_row) as cur:
        token = None
        if mode != "stored":
            try:
                async with conn.transaction():
                    await cur.execute(
                        "SELECT pgp_sym_decrypt(access_token, %s) AS token FROM user_oauth_tokens "
                        "WHERE user_id = %s AND provider = %s",
                        (get_encryption_key(), user_id, CLAUDE_CODE_PROVIDER),
                    )
                    token = (row := await cur.fetchone()) and row["token"]
            except psycopg.Error:
                logger.warning(f"[claude_code] stored token unreadable as it is deleted for user_id={user_id}", exc_info=True)
            await cur.execute(
                "DELETE FROM user_oauth_tokens WHERE user_id = %s AND provider = %s", (user_id, CLAUDE_CODE_PROVIDER),
            )
        await cur.execute(
            """
            INSERT INTO claude_code_custody (user_id, mode, disclosure, blocked_at, blocked_reason)
            VALUES (%s, %s, %s, CASE WHEN %s::text IS NULL THEN NULL ELSE NOW() END, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                mode = EXCLUDED.mode, chosen_at = NOW(),
                disclosure = COALESCE(EXCLUDED.disclosure, claude_code_custody.disclosure),
                blocked_at = COALESCE(EXCLUDED.blocked_at, claude_code_custody.blocked_at),
                blocked_reason = COALESCE(EXCLUDED.blocked_reason, claude_code_custody.blocked_reason)
            WHERE EXCLUDED.mode <> 'stored' OR claude_code_custody.blocked_at IS NULL
            """,
            (user_id, mode, disclosure, block_reason, block_reason),
        )
        if cur.rowcount == 0:
            raise StoringBlocked(user_id)
    return token


async def unblock(user_id: str) -> bool:
    """Lift an operator's block; whether there was one. The choice stays ``local``."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE claude_code_custody SET blocked_at = NULL, blocked_reason = NULL "
            "WHERE user_id = %s AND blocked_at IS NOT NULL",
            (user_id,),
        )
        return cur.rowcount > 0


async def stored_users() -> list[str]:
    """Every user who holds a stored setup token or chose to store one."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT user_id FROM user_oauth_tokens WHERE provider = %s "
            "UNION SELECT user_id FROM claude_code_custody WHERE mode = 'stored'",
            (CLAUDE_CODE_PROVIDER,),
        )
        return [row["user_id"] for row in await cur.fetchall()]


async def live_worker_turns(user_id: str, *, besides: str | None = None) -> int:
    """The user's runs in progress whose CLI runs in a worker (``WORKER_TURN``),
    leaving out thread ``besides``'s, whose next message steers its own run."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT count(*) AS live FROM conversation_responses
            WHERE status = 'in_progress' AND user_id = %s AND metadata @> %s
              AND conversation_thread_id IS DISTINCT FROM %s
            """,
            (user_id, Jsonb(WORKER_TURN), besides),
        )
        return (await cur.fetchone())["live"]


async def live_stored_turns(user_id: str) -> list[tuple[str, str]]:
    """The thread and run of each of the user's runs in progress on their
    stored setup token (``STORED_TURN``)."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT conversation_thread_id, conversation_response_id FROM conversation_responses
            WHERE status = 'in_progress' AND user_id = %s AND metadata @> %s
            """,
            (user_id, Jsonb(STORED_TURN)),
        )
        return [(str(r["conversation_thread_id"]), str(r["conversation_response_id"])) for r in await cur.fetchall()]
