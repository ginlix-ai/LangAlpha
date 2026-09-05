"""Account-wide off switches for the things that ship with the app.

Neither a builtin MCP server nor a bundle under plugins/ has a row of its own
to carry a flag, so the state is kept as the exception instead: a row here is
a disable, absence is enabled, and a fresh account costs nothing. Both kinds
share one table because assembling the effective set runs on every turn and
wants one query, and ``kind`` keeps them apart because bundle names and server
names are separate namespaces. The table is ``user_mcp_builtin_disables``,
named in 027 for the only kind that existed then.
"""

import logging
from dataclasses import dataclass
from typing import Literal

from psycopg.rows import dict_row

from src.server.database.mcp_servers import bump_user_versions
from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)

DisableKind = Literal["server", "bundle"]


@dataclass(frozen=True)
class AccountDisables:
    """One user's switched-off names, both kinds, from one read."""

    servers: frozenset[str]
    bundles: frozenset[str]


async def list_account_disables(user_id: str) -> AccountDisables:
    """Everything this account has switched off, split by kind."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT kind, name FROM user_mcp_builtin_disables WHERE user_id = %s",
                (user_id,),
            )
            rows = await cur.fetchall()
    return AccountDisables(
        servers=frozenset(r["name"] for r in rows if r["kind"] == "server"),
        bundles=frozenset(r["name"] for r in rows if r["kind"] == "bundle"),
    )


async def set_account_disable(
    user_id: str, kind: DisableKind, name: str, *, disabled: bool
) -> None:
    """Write or clear one account-wide disable.

    The ``mcp_config_version`` fan-out rides the same transaction, and not as
    a cache hint: a resolver whose version no longer matches throws its whole
    grant set away rather than write a stale one, so the bump has to become
    visible at the instant the disable does.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                if disabled:
                    await cur.execute(
                        """
                        INSERT INTO user_mcp_builtin_disables (user_id, kind, name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id, kind, name) DO NOTHING
                        """,
                        (user_id, kind, name),
                    )
                else:
                    await cur.execute(
                        """
                        DELETE FROM user_mcp_builtin_disables
                        WHERE user_id = %s AND kind = %s AND name = %s
                        """,
                        (user_id, kind, name),
                    )
                await bump_user_versions(cur, user_id)
                logger.info(
                    f"[account_disables] set user_id={user_id} kind={kind} "
                    f"name={name} disabled={disabled}"
                )
