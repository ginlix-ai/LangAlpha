"""Bumps of ``workspaces.mcp_config_version``, the drift signal a session reads.

A session compares the version it resolved under with the stored one on its
next acquire, so a write that changes what a workspace resolves bumps it, in
the write's own transaction wherever it has one. The writers span several
modules (the catalog, the schema cache, account disables, the trading
permission, OAuth), so the bump lives in none of them.
"""

from src.server.database.pool import get_db_connection


async def bump_workspace_version(cur, workspace_id: str) -> None:
    """Atomically increment a workspace's mcp_config_version (same txn).

    A change to Home's selection bumps Home's stand-in too, which runs with
    Home's selection (``home_workspace.acting_for``)."""
    from src.server.database.home_workspace import (
        get_home_stand_in_id,
        get_home_workspace_id,
    )

    await cur.execute(
        "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
        "WHERE workspace_id = %s RETURNING user_id",
        (workspace_id,),
    )
    row = await cur.fetchone()
    user_id = (row.get("user_id") if isinstance(row, dict) else row[0]) if row else None
    if user_id and str(workspace_id).lower() == get_home_workspace_id(str(user_id)):
        await cur.execute(
            "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
            "WHERE workspace_id = %s",
            (get_home_stand_in_id(str(user_id)),),
        )


async def bump_user_versions(cur, user_id: str) -> None:
    """Increment mcp_config_version on every workspace of a user (same txn).

    One statement, unpaginated on purpose: a user-level change must never
    leave a subset of workspaces on the old version.
    """
    await cur.execute(
        "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
        "WHERE user_id = %s",
        (user_id,),
    )


async def bump_user_workspaces_mcp_version(user_id: str) -> int:
    """Bump mcp_config_version on ALL of a user's workspaces (own transaction).

    For out-of-band user-level invalidation (OAuth connect/disconnect, user
    vault changes referenced by live servers). Returns workspaces touched.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await bump_user_versions(cur, user_id)
            return cur.rowcount
