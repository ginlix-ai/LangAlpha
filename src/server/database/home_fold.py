"""Folding an account's former Home rows into the Home its id derives.

A Home's id derives from the user id it was made for, so a row only stops
matching its owner when it changes accounts: an account merge moves an
account's rows onto the account it joins, its Home included, under an id
derived from the merged account. The account would then list two "All
workspaces" entries, one holding the merged account's conversations. The fold
moves what that row holds into Home and retires it, so the history reads as
one.

Only a never-bound row folds: a bound one owns a folder on its computer, which
moving rows cannot carry. A row with a run in progress waits for a later
resolve, by the busy rule a delete uses. Nothing in the workspace memory tier
moves, because an unbound row's agent never had one.
"""

from __future__ import annotations

import logging
import re
from itertools import count

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.runs.lifecycle import workspace_has_live_runs
from src.server.database.user_lock import lock_user_writes

logger = logging.getLogger(__name__)

# Every table keyed by a workspace either moves or stays, and a test reads the
# schema for any it finds in neither, so a new one gets a decision.
MOVES: dict[str, str] = {
    # Before the threads, in this order: a row continues its pinned thread
    # only where both live, so it moves with the thread wherever it was stored.
    "automations": """
        UPDATE automations SET workspace_id = %(home)s
        WHERE workspace_id = %(former)s OR conversation_thread_id IN (
            SELECT conversation_thread_id FROM conversation_threads
            WHERE workspace_id = %(former)s
        )
    """,
    # thread_index is unique per workspace, so a row's threads append after
    # Home's in the order they were started. updated_at stays, so the thread
    # list keeps its order.
    "conversation_threads": """
        WITH base AS (
            SELECT COALESCE(MAX(thread_index), -1) + 1 AS next_index
            FROM conversation_threads WHERE workspace_id = %(home)s
        ), moved AS (
            SELECT conversation_thread_id,
                   (SELECT next_index FROM base)
                     + ROW_NUMBER() OVER (ORDER BY created_at, thread_index) - 1
                     AS thread_index
            FROM conversation_threads WHERE workspace_id = %(former)s
        )
        UPDATE conversation_threads t
        SET workspace_id = %(home)s, thread_index = moved.thread_index
        FROM moved WHERE t.conversation_thread_id = moved.conversation_thread_id
    """,
    # A run's place is its thread's workspace, and a usage row outlives its
    # thread: left on the retired row, a run whose thread is later deleted
    # would read as some other workspace's.
    "conversation_usages": """
        UPDATE conversation_usages SET workspace_id = %(home)s
        WHERE workspace_id = %(former)s
    """,
    # A layer whose id Home's chart already has was renamed first
    # (``_rename_colliding_layers``), so both charts' layers land in Home. Only
    # a legacy annotation that collides, which no read shows, is left on the
    # retired row.
    "chart_annotations": """
        UPDATE chart_annotations a SET workspace_id = %(home)s
        WHERE a.workspace_id = %(former)s AND NOT EXISTS (
            SELECT 1 FROM chart_annotations h
            WHERE h.workspace_id = %(home)s
              AND h.chart_id = a.chart_id AND h.annotation_id = a.annotation_id
        )
    """,
}

#: A layer id's longest (``chartkit.schema.LAYER_ID_PATTERN``), which a
#: renamed one keeps to.
_LAYER_ID_MAX = 48
_LAYER_ID = re.compile(r"^[a-z0-9-]{1,48}$")

_SANDBOX = "sandbox state, and a never-bound row has no sandbox"

STAYS: dict[str, str] = {
    "workspace_files": _SANDBOX,
    "workspace_file_uploads": _SANDBOX,
    "livefs_links": _SANDBOX,
    "share_links": _SANDBOX,
    "sandbox_egress_grants": _SANDBOX,
    "sandbox_egress_grant_claims": _SANDBOX,
    "computers": "origin_workspace_id names the workspace a sandbox was adopted "
    "from, and a never-bound row has no sandbox",
    "workspace_mcp_servers": "Home's own MCP selection wins",
    "workspace_mcp_tool_schemas": "cached for the MCP selection, where Home's wins",
    "workspace_vault_secrets": "Home's own secret selection wins",
    "order_attempts": "a record of where it ran",
    "command_runs": "a command runs on the row's computer, and a never-bound row has none",
    "workspace_moves": "a record of the row's own moves, and a never-bound row has none",
    "user_skills": "nothing offers Home as a skill's workspace, so a former Home holds none",
    "workspace_skill_disables": "Home's own skill selection wins",
    "user_subagents": "its workspace_id is reserved and always NULL, so a former Home holds none",
}

_HAS_FORMER = """
    SELECT EXISTS (
        SELECT 1 FROM workspaces
        WHERE user_id = %s AND status = 'unbound' AND workspace_id <> %s
    ) AS found
"""

# The delete convention: a tombstone, which no longer reads as a Home.
_RETIRE = """
    UPDATE workspaces SET status = 'deleted', is_pinned = FALSE
    WHERE workspace_id = %(former)s
"""


async def fold_former_homes(user_id: str, home_id: str, conn=None) -> list[str]:
    """Move the user's former Home rows into ``home_id`` and retire them.

    Returns the ids retired. Every Home resolve lands here, so a user with no
    former row costs one unlocked read and no transaction.
    """
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            await cur.execute(_HAS_FORMER, (user_id, home_id))
            if not (await cur.fetchone())["found"]:
                return []

        folded: list[str] = []
        moved = dict.fromkeys(MOVES, 0)
        async with owned.transaction(), owned.cursor(row_factory=dict_row) as cur:
            await lock_user_writes(cur, user_id)
            # Locked before the busy check, so the check sees a turn admitted
            # meanwhile: admission holds the row FOR SHARE until its run is
            # recorded.
            await cur.execute(
                """
                SELECT workspace_id FROM workspaces
                WHERE user_id = %s AND status = 'unbound' AND workspace_id <> %s
                ORDER BY created_at
                FOR UPDATE
                """,
                (user_id, home_id),
            )
            # One row at a time, so each sees what the last moved into Home.
            for row in await cur.fetchall():
                former = str(row["workspace_id"])
                if await workspace_has_live_runs(cur, former):
                    continue
                params = {"home": home_id, "former": former}
                await _rename_colliding_layers(cur, params)
                for table, statement in MOVES.items():
                    await cur.execute(statement, params)
                    moved[table] += cur.rowcount
                await cur.execute(_RETIRE, params)
                folded.append(former)

    if folded:
        logger.info(
            f"[home_fold] user={user_id} folded {folded} into {home_id} (moved {moved})"
        )
    return folded


def renamed_layer_id(layer_id: str, taken: set[str]) -> str:
    """``layer_id`` with the first ``-<n>`` suffix (from 2) no id in ``taken``
    has, cut to the layer id's length cap."""
    for n in count(2):
        suffix = f"-{n}"
        candidate = layer_id[: _LAYER_ID_MAX - len(suffix)] + suffix
        if candidate not in taken:
            return candidate
    raise AssertionError("unreachable")


async def _rename_colliding_layers(cur, params: dict[str, str]) -> None:
    """Give each of the former row's layers whose id its chart in Home
    already has a free id of its own (``renamed_layer_id``), in its row and
    its payload, so the fold keeps both. Layer ids are the agent's own, and
    every chart 117 converted holds ``earlier-annotations``, so two Homes'
    charts often share one."""
    await cur.execute(
        """
        SELECT a.chart_id, a.annotation_id,
               ARRAY(
                   SELECT o.annotation_id FROM chart_annotations o
                   WHERE o.chart_id = a.chart_id
                     AND o.workspace_id IN (%(home)s, %(former)s)
               ) AS taken
        FROM chart_annotations a
        JOIN chart_annotations h
          ON h.workspace_id = %(home)s
         AND h.chart_id = a.chart_id AND h.annotation_id = a.annotation_id
        WHERE a.workspace_id = %(former)s
        ORDER BY a.chart_id, a.annotation_id
        """,
        params,
    )
    taken_on: dict[str, set[str]] = {}
    for row in await cur.fetchall():
        old = row["annotation_id"]
        if not _LAYER_ID.match(old):
            # A legacy annotation: no read shows it, so it stays put.
            continue
        taken = taken_on.setdefault(row["chart_id"], set(row["taken"]))
        new = renamed_layer_id(old, taken)
        taken.add(new)
        await cur.execute(
            """
            UPDATE chart_annotations
            SET annotation_id = %(new)s::text,
                payload = CASE WHEN payload ? 'id'
                               THEN jsonb_set(payload, '{id}', to_jsonb(%(new)s::text))
                               ELSE payload END
            WHERE workspace_id = %(former)s AND chart_id = %(chart)s AND annotation_id = %(old)s
            """,
            {**params, "new": new, "old": old, "chart": row["chart_id"]},
        )
