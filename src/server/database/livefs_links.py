"""The folders whose links into a computer's file mount went in, per sandbox.

Every server worker reads them with the token row at a turn's start
(``mount_view``), so a folder another worker linked costs this one no exec,
and one whose workspace left the computer is served as linked by none: the
delete or move that takes a workspace off a computer takes its rows in the
same transaction (``drop_workspace_links``). Each row names the link layout
it was laid under, and only rows of the reader's own count.
"""

from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from src.server.database.livefs_tokens import TOKEN_COLUMNS, TokenRow, token_row
from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_LIVE_WORKSPACE


async def mount_view(
    computer_id: str, sandbox_id: str, layout: str
) -> tuple[TokenRow, frozenset[str | None]]:
    """The token row and the folders linked on ``sandbox_id`` under
    ``layout`` (None: the computer's own), in one read."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # A computer with no token row still answers its links.
            await cur.execute(
                f"""
                SELECT {TOKEN_COLUMNS},
                       ARRAY(
                           SELECT l.workspace_id::text FROM livefs_links l
                           WHERE l.computer_id = %(computer_id)s::uuid
                             AND l.sandbox_id = %(sandbox_id)s::text
                             AND l.layout = %(layout)s::text
                       ) AS linked
                FROM (SELECT 1) AS one
                LEFT JOIN livefs_tokens t ON t.computer_id = %(computer_id)s::uuid
                """,
                {"computer_id": computer_id, "sandbox_id": sandbox_id, "layout": layout},
            )
            row = await cur.fetchone()
    return token_row(row), frozenset(row["linked"] if row else ())


async def save_links(
    computer_id: str, sandbox_id: str, linked: frozenset[str | None], layout: str
) -> frozenset[str | None]:
    """Record ``linked`` as the computer's whole linked set, on
    ``sandbox_id`` under ``layout``, and answer what was recorded.

    A workspace deleted or moved off the computer since the link read its
    folders is left out. The share locks on the workspace rows, taken before
    any link row, order this against that delete or move: one committed
    first is seen here, and one waiting on this takes the row written here.
    The computer's row is share-locked first, as a write to a computer and
    its workspaces takes the computer first, so neither ever holds a
    workspace row the other waits on.
    """
    workspaces = sorted(w for w in linked if w is not None)
    root = None in linked
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT 1 FROM computers WHERE computer_id = %s FOR SHARE",
                (computer_id,),
            )
            await cur.execute(
                f"""
                SELECT workspace_id::text FROM workspaces
                WHERE workspace_id = ANY(%s::uuid[])
                  AND computer_id = %s AND {FENCE_LIVE_WORKSPACE}
                ORDER BY workspace_id
                FOR SHARE
                """,
                (workspaces, computer_id),
            )
            bound = [row["workspace_id"] for row in await cur.fetchall()]
            params = {
                "computer_id": computer_id,
                "sandbox_id": sandbox_id,
                "layout": layout,
                "root": root,
                "bound": bound,
            }
            # The computer has one sandbox, so another's rows are a replaced
            # one's; another layout's were laid by another release.
            await cur.execute(
                """
                DELETE FROM livefs_links
                WHERE computer_id = %(computer_id)s::uuid
                  AND (sandbox_id <> %(sandbox_id)s::text
                       OR layout <> %(layout)s::text
                       OR (workspace_id IS NULL AND NOT %(root)s)
                       OR (workspace_id IS NOT NULL
                           AND workspace_id <> ALL(%(bound)s::uuid[])))
                """,
                params,
            )
            await cur.execute(
                """
                INSERT INTO livefs_links (computer_id, sandbox_id, workspace_id, layout)
                SELECT %(computer_id)s::uuid, %(sandbox_id)s::text, w, %(layout)s::text
                FROM unnest(%(bound)s::uuid[]) AS w
                UNION ALL
                SELECT %(computer_id)s::uuid, %(sandbox_id)s::text, NULL::uuid,
                       %(layout)s::text
                WHERE %(root)s
                ON CONFLICT ON CONSTRAINT livefs_links_folder_key DO NOTHING
                """,
                params,
            )
    return frozenset(bound) | ({None} if root else frozenset())


async def drop_workspace_links(
    cur: Any, workspace_id: str, *, keep_computer_id: str | None = None
) -> None:
    """Inside the transaction that takes ``workspace_id`` off a computer:
    forget every link it had, except on ``keep_computer_id``, the one it is
    bound to now. Its folder there may be gone, or made anew without them."""
    await cur.execute(
        "DELETE FROM livefs_links WHERE workspace_id = %s"
        " AND computer_id IS DISTINCT FROM %s",
        (workspace_id, keep_computer_id),
    )
