"""Claude Code on a user's cloud computers: which credential the user chose
for it (``claude_code_custody``, migration 105), and how many of their turns
run there now."""

from __future__ import annotations

from typing import Literal

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.server.database.pool import get_db_connection

Mode = Literal["local", "api_key", "stored"]
#: What a turn's run row carries in its metadata while its Claude Code CLI
#: runs in a worker for a cloud computer, which ``live_worker_turns`` counts.
WORKER_TURN = {"claude_code_worker": True}


async def get_custody_mode(user_id: str, *, conn=None) -> Mode:
    """The user's choice, ``local`` while they have made none."""
    async with get_db_connection(conn) as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT mode FROM claude_code_custody WHERE user_id = %s", (user_id,))
        row = await cur.fetchone()
    return row["mode"] if row else "local"


async def set_custody_mode(user_id: str, mode: Mode) -> None:
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO claude_code_custody (user_id, mode) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET mode = EXCLUDED.mode, chosen_at = NOW()
            """,
            (user_id, mode),
        )


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
