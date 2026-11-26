"""What a restart of the server would pause for one user, counted for the
desktop app's update panel, which says so before the person restarts."""

from __future__ import annotations

from psycopg.rows import dict_row

from src.server.database import pool

# Commands on a `local` computer stop with the desktop app's server
# (`desktop_app.stop_this_computer`); every other computer's keep running and
# are picked up again after it. A turn or task counts for the person who
# started it, which in a shared workspace is not the workspace's holder; a row
# that names nobody (one from before senders were recorded) counts for the
# holder.
_COUNTS_SQL = """
SELECT
  (SELECT count(*) FROM conversation_responses cr
     JOIN conversation_threads ct ON ct.conversation_thread_id = cr.conversation_thread_id
     JOIN workspaces w ON w.workspace_id = ct.workspace_id
    WHERE cr.status = 'in_progress' AND COALESCE(cr.user_id, w.user_id) = %(user)s) AS turns,
  (SELECT count(*) FROM subagent_runs sr
     JOIN conversation_threads ct ON ct.conversation_thread_id = sr.thread_id
     JOIN workspaces w ON w.workspace_id = ct.workspace_id
    WHERE sr.status = 'in_progress' AND COALESCE(sr.user_id, w.user_id) = %(user)s) AS background_tasks,
  (SELECT count(*) FROM command_runs c
     JOIN computers k ON k.computer_id = c.computer_id
    WHERE c.user_id = %(user)s AND c.status = 'running' AND k.kind = 'local') AS local_commands
"""


async def count_running(user_id: str) -> dict[str, int]:
    """The user's running turns, background subagent tasks, and commands on a
    computer of this machine's own."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_COUNTS_SQL, {"user": user_id})
            row = await cur.fetchone()
    return {key: int(row[key] or 0) for key in ("turns", "background_tasks", "local_commands")}
