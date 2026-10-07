"""Computer persistence with atomic workspace shadows for rolling deploys.

Old deployment readers and platform capacity accounting still need workspace
shadows; comp must lock first and shadow must depend on its output, using
(SELECT count(*) FROM comp) even when no rows are needed, to avoid deadlocks.
Enforce fences in UPDATE so PostgreSQL rechecks under lock: computer entry uses
provider_ref, workspace entry uses sandbox_id.
NULL computer_id rows, including flash and backfill tie-break losers, retain
the single-table path.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import (
    COMPUTER_COLS as _COMPUTER_COLS,
)
from src.server.database.sql_fences import (
    computer_cols,
    FENCE_BINDABLE,
    advisory_key,
    FENCE_LIVE_WORKSPACE,
    FENCE_NOT_DELETED,
    SHADOWED_IDS,
    shadowed_write,
)
from src.server.utils.pg_sanitize import normalize_uuid

__all__ = [
    "FENCE_BINDABLE",
    "FENCE_LIVE_WORKSPACE",
    "FENCE_NOT_DELETED",
]

logger = logging.getLogger(__name__)

# A user reads the name before anything else about the machine, so it says
# where the machine runs: Daytona is the hosted default, Docker is theirs.
COMPUTER_NAMES_BY_KIND = {"daytona": "Cloud computer", "docker": "Local computer"}


def default_computer_name(kind: str) -> str:
    return COMPUTER_NAMES_BY_KIND.get(kind, COMPUTER_NAMES_BY_KIND["daytona"])


DEFAULT_ROOT_DIR = "/home/workspace"

# Match migration 046's CHECK; flash has no computer.
COMPUTER_STATUSES = (
    "creating",
    "starting",
    "running",
    "stopping",
    "stopped",
    "error",
    "deleted",
)

_SETTABLE_SCALAR_COLUMNS = frozenset({"resource_tier", "is_always_on"})

# layout_version 0 is "no sandbox has reported one", not layout v0: the column's
# only writer is stamp_computer_layout_version, and 046 backfills every machine
# at 0 because workspaces never recorded a layout at all.
LAYOUT_VERSION_UNOBSERVED = 0


def observed_layout_version(computer: Dict[str, Any]) -> Optional[int]:
    """None until a sync reports one, so no reader can compare against a v0."""
    version = computer.get("layout_version")
    if version is None or int(version) == LAYOUT_VERSION_UNOBSERVED:
        return None
    return int(version)


@asynccontextmanager
async def _computer_cursor(conn=None):
    async with get_db_connection(conn) as owned:
        async with owned.cursor(row_factory=dict_row) as cur:
            yield cur


@asynccontextmanager
async def computer_capacity_lock(user_id: str):
    """Keep admission and its committed start claim on one PostgreSQL session."""
    from src.server.database.session_lock import release_session_lock

    key = f"computer-capacity:{user_id}"
    async with get_db_connection() as conn:
        try:
            async with conn.transaction():
                await conn.execute("SET LOCAL lock_timeout = '30s'")
                await conn.execute(
                    "SELECT pg_advisory_lock(hashtextextended(%s, 0))", (key,)
                )
            yield conn
        finally:
            await release_session_lock(conn, key)


def computer_advisory_key(computer_id: str) -> int:
    """Use the C(computer) domain to avoid collisions with bare 32-bit workspace hashtext locks."""
    return advisory_key("C", computer_id)


def bind_provider_ref_statement(
    *,
    authority: str,
    returning: str,
    guard_always_on: bool = False,
    guard_host_outage: bool = False,
) -> str:
    """Share identity and lifecycle fences so computer and workspace binds cannot diverge.

    IS NOT DISTINCT FROM fences the previous ref; lifecycle fencing prevents a
    racing bind from reviving a stopped machine. A bind is a sandbox that
    started, so it ends any host outage the row was timing. A rebuild a host
    outage authorized also fences on that outage still running: the old
    sandbox resuming meanwhile ends it, and then the old one is kept.
    """
    always_on_guard = (
        "\n                  AND c.is_always_on = %(expected_always_on)s"
        if guard_always_on
        else ""
    )
    outage_guard = (
        "\n                  AND c.host_unavailable_since"
        " = %(expected_host_unavailable_since)s"
        if guard_host_outage
        else ""
    )
    common = {
        "computer_set": (
            "status = 'running',\n"
            "                    provider_ref = %(provider_ref)s,\n"
            "                    platform_secret_version = %(platform_secret_version)s,\n"
            "                    host_unavailable_since = NULL"
        ),
        "workspace_set": (
            "status = 'running',\n"
            "                    sandbox_id = %(provider_ref)s,\n"
            "                    platform_secret_version = %(platform_secret_version)s"
        ),
        "computer_fence": FENCE_BINDABLE,
        "workspace_fence": FENCE_BINDABLE,
        "computer_guard": (
            "\n                  AND c.provider_ref IS NOT DISTINCT FROM"
            " %(expected_previous)s"
            f"{always_on_guard}{outage_guard}"
        ),
    }
    if authority == "computer":
        return shadowed_write(
            authority="computer",
            computer_returning=_COMPUTER_COLS,
            workspace_returning="w.workspace_id",
            workspace_guard=(
                "\n                  AND w.sandbox_id IS NOT DISTINCT FROM"
                " %(expected_previous)s"
            ),
            select=f"SELECT {returning}, {SHADOWED_IDS} FROM comp",
            **common,
        )
    if authority == "workspace":
        return shadowed_write(
            authority="workspace",
            computer_returning="c.computer_id",
            workspace_returning=f"{returning}, m.mirrored AS computers_bound",
            # A flash workspace and a backfill tie-break loser have no machine to
            # fence, so the mirror count only has to hold for a bound row.
            workspace_guard=(
                "\n                  AND w.sandbox_id IS NOT DISTINCT FROM"
                " %(expected_previous)s"
                "\n                  AND (w.computer_id IS NULL OR m.mirrored > 0)"
            ),
            select="SELECT * FROM shadow",
            **common,
        )
    raise ValueError(f"Unknown bind authority: {authority!r}")


async def get_computer_for_workspace(
    workspace_id: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Keep workspace-to-machine resolution centralized.

    The join already has the project's row, so ``dir_name`` rides along: the
    binding needs the folder as often as it needs the machine, and a second
    query for it is a second answer that can disagree with this one. None is
    normal for flash workspaces and backfill shared-sandbox tie-break losers.
    """
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None

    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {computer_cols("c")}, w.dir_name
            FROM computers c
            JOIN workspaces w ON w.computer_id = c.computer_id
            WHERE w.workspace_id = %s AND c.{FENCE_NOT_DELETED}
            """,
            (workspace_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_computer(
    computer_id: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return None

    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_COMPUTER_COLS}
            FROM computers
            WHERE computer_id = %s AND {FENCE_NOT_DELETED}
            """,
            (computer_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_computer_by_provider_ref(
    kind: str,
    provider_ref: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Two projects naming one sandbox are two projects on one machine.

    Reads the target of idx_computers_provider_ref, so the edge resolve can
    adopt the existing machine instead of inserting a second row for the
    sandbox and tripping that index.
    """
    if not provider_ref:
        return None
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_COMPUTER_COLS}
            FROM computers
            WHERE kind = %s AND provider_ref = %s AND {FENCE_NOT_DELETED}
            """,
            (kind, provider_ref),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_computers_for_user(
    user_id: str,
    *,
    include_deleted: bool = False,
    conn=None,
) -> List[Dict[str, Any]]:
    status_filter = "" if include_deleted else f"AND {FENCE_NOT_DELETED}"
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_COMPUTER_COLS}
            FROM computers
            WHERE user_id = %s {status_filter}
            ORDER BY is_primary DESC,
                     COALESCE(last_activity_at, updated_at) DESC,
                     computer_id
            """,
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_primary_computer(
    user_id: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Migration 046's partial unique (user_id) WHERE is_primary index guarantees one row."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_COMPUTER_COLS}
            FROM computers
            WHERE user_id = %s AND is_primary AND {FENCE_NOT_DELETED}
            """,
            (user_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_computers_by_status(
    status: str,
    limit: int = 100,
    *,
    offset: int = 0,
    conn=None,
) -> List[Dict[str, Any]]:
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_COMPUTER_COLS}
            FROM computers
            WHERE status = %s
            ORDER BY last_activity_at ASC NULLS FIRST, computer_id
            LIMIT %s OFFSET %s
            """,
            (status, limit, offset),
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_retirement_pending_computers(
    limit: int = 100,
    *,
    conn=None,
) -> List[Dict[str, Any]]:
    """Find every machine whose delete-time retirement decision is unsettled."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            SELECT {_COMPUTER_COLS}
            FROM computers c
            WHERE c.status <> 'deleted'
              AND COALESCE(c.config, '{{}}'::jsonb)
                    @> '{{"retire_when_empty": true}}'::jsonb
            ORDER BY c.updated_at ASC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def mark_computer_retirement_pending(
    computer_id: str,
    *,
    conn=None,
) -> bool:
    """Durably ask cleanup to finish retiring this machine once it is empty."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET config = jsonb_set(
                    COALESCE(config, '{{}}'::jsonb),
                    '{{retire_when_empty}}',
                    'true'::jsonb
                ),
                updated_at = NOW()
            WHERE computer_id = %s
              AND {FENCE_NOT_DELETED}
            """,
            (computer_id,),
        )
        return cur.rowcount > 0


async def clear_computer_retirement_pending(
    computer_id: str,
    *,
    conn=None,
) -> bool:
    """Cancel a retirement request after the locked emptiness check keeps the row."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET config = COALESCE(config, '{{}}'::jsonb)
                    #- '{{retire_when_empty}}',
                updated_at = NOW()
            WHERE computer_id = %s
              AND {FENCE_NOT_DELETED}
              AND COALESCE(config, '{{}}'::jsonb)
                    @> '{{"retire_when_empty": true}}'::jsonb
            """,
            (computer_id,),
        )
        return cur.rowcount > 0


async def create_computer(
    user_id: str,
    *,
    kind: str = "daytona",
    name: Optional[str] = None,
    is_primary: bool = False,
    status: str = "creating",
    resource_tier: str = "standard",
    root_dir: str = DEFAULT_ROOT_DIR,
    provider_config: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    computer_id: Optional[str] = None,
    provider_ref: Optional[str] = None,
    is_always_on: bool = False,
    platform_secret_version: int = 0,
    origin_workspace_id: Optional[str] = None,
    conn=None,
) -> Dict[str, Any]:
    """A lost primary-index race retries as non-primary because another primary now exists.

    provider_ref and origin_workspace_id are for adopting a sandbox that
    predates the computers table; a machine minted for a new project is born
    unprovisioned and gets its ref from try_bind_computer_provider_ref."""
    from psycopg.errors import UniqueViolation

    name = name or default_computer_name(kind)

    async def _insert(want_primary: bool) -> Dict[str, Any]:
        async with _computer_cursor(conn) as cur:
            await cur.execute(
                f"""
                INSERT INTO computers (
                    computer_id, user_id, kind, name, is_primary, status,
                    resource_tier, root_dir, provider_config, config,
                    provider_ref, is_always_on, platform_secret_version,
                    origin_workspace_id
                )
                VALUES (
                    COALESCE(%s::uuid, gen_random_uuid()), %s, %s, %s,
                    %s AND NOT EXISTS (
                        SELECT 1 FROM computers
                        WHERE user_id = %s AND is_primary AND {FENCE_NOT_DELETED}
                    ),
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s::uuid
                )
                RETURNING {_COMPUTER_COLS}
                """,
                (
                    computer_id,
                    user_id,
                    kind,
                    name,
                    want_primary,
                    user_id,
                    status,
                    resource_tier,
                    root_dir,
                    Json(provider_config or {}),
                    Json(config or {}),
                    provider_ref,
                    is_always_on,
                    platform_secret_version,
                    origin_workspace_id,
                ),
            )
            return dict(await cur.fetchone())

    try:
        row = await _insert(is_primary)
    except UniqueViolation:
        if provider_ref is not None:
            # Two workspaces naming one sandbox adopt it concurrently; the
            # provider_ref index admits one row, so the loser joins it.
            existing = await get_computer_by_provider_ref(kind, provider_ref)
            if existing is not None and str(existing["user_id"]) == str(user_id):
                logger.info(
                    f"Sandbox {provider_ref} was adopted concurrently as computer "
                    f"{existing['computer_id']}; joining it"
                )
                return existing
        if not is_primary:
            raise
        logger.info(
            f"Lost the primary-computer race for user {user_id}; "
            "inserting as non-primary"
        )
        row = await _insert(False)

    logger.info(
        f"Created computer {row['computer_id']} (kind={kind}, "
        f"primary={row['is_primary']}) for user {user_id}"
    )
    return row


async def update_computer_status(
    computer_id: str,
    status: str,
    *,
    expected: Optional[str | Sequence[str]] = None,
    expected_always_on: Optional[bool] = None,
    updated_before=None,
    expected_provider_ref: Optional[str] = None,
    require_provider_ref: bool = False,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Only try_bind_computer_provider_ref may change the durable machine binding.

    Skip deleted and flash workspaces so a machine transition cannot revive a
    project or attach a machine-less workspace. A tombstone also surrenders the
    primary flag: every probe fences on the tombstone, so a deleted row holding
    it would wedge ensure_primary_computer against the partial unique index.
    """
    if status not in COMPUTER_STATUSES:
        raise ValueError(f"Not a computer status: {status!r}")

    expected_clause = ""
    if expected is not None:
        expected_clause = "\n                  AND c.status = ANY(%(expected)s)"
    if updated_before is not None:
        expected_clause += "\n                  AND c.updated_at <= %(updated_before)s"
    if expected_always_on is not None:
        expected_clause += (
            "\n                  AND c.is_always_on = %(expected_always_on)s"
        )
    if require_provider_ref:
        expected_clause += (
            "\n                  AND c.provider_ref IS NOT DISTINCT FROM"
            " %(expected_ref)s"
        )
    stopped_clause = ", stopped_at = NOW()" if status == "stopped" else ""
    primary_clause = ", is_primary = FALSE" if status == "deleted" else ""
    # Running is a start that succeeded, which ends any host outage; a revert
    # to stopped after a refused start keeps the clock going.
    running_clause = (
        ", host_unavailable_since = NULL" if status == "running" else ""
    )

    params: Dict[str, Any] = {
        "computer_id": computer_id,
        "status": status,
        "expected": [expected] if isinstance(expected, str) else list(expected or ()),
        "updated_before": updated_before,
        "expected_always_on": expected_always_on,
        "expected_ref": expected_provider_ref,
    }
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            shadowed_write(
                authority="computer",
                computer_set=(
                    f"status = %(status)s{stopped_clause}{primary_clause}"
                    f"{running_clause}"
                ),
                workspace_set=f"status = %(status)s{stopped_clause}",
                computer_guard=expected_clause,
                computer_returning=_COMPUTER_COLS,
                workspace_returning="w.workspace_id",
                select=f"SELECT {_COMPUTER_COLS}, {SHADOWED_IDS} FROM comp",
            ),
            params,
        )
        row = await cur.fetchone()

    if row is None:
        return None
    await _publish_shadowed(row, status)
    return dict(row)


async def try_claim_computer_for_start(
    computer_id: str,
    *,
    from_status: str = "stopped",
    expected_provider_ref: Optional[str] = None,
    require_provider_ref: bool = False,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Commit the claim before publishing so subscribers cannot read the pre-claim row.

    Only the UPDATE winner owns the transition; losers wait for running or error.
    require_provider_ref is separate because None is a valid expected identity for
    a never-provisioned computer.
    """
    if from_status not in COMPUTER_STATUSES:
        raise ValueError(f"Not a computer status: {from_status!r}")

    ref_guard = ""
    if require_provider_ref:
        ref_guard = (
            "\n                  AND c.provider_ref IS NOT DISTINCT FROM"
            " %(expected_ref)s"
        )

    params = {
        "computer_id": computer_id,
        "from_status": from_status,
        "expected_ref": expected_provider_ref,
    }
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            shadowed_write(
                authority="computer",
                computer_set="status = 'starting'",
                workspace_set="status = 'starting'",
                computer_fence="status = %(from_status)s",
                computer_guard=ref_guard,
                # Only the projects that were down move; a sibling left behind in
                # another state is not this claim's to reinterpret.
                workspace_fence="status = %(from_status)s",
                computer_returning=_COMPUTER_COLS,
                workspace_returning="w.workspace_id",
                select=f"SELECT {_COMPUTER_COLS}, {SHADOWED_IDS} FROM comp",
            ),
            params,
        )
        row = await cur.fetchone()

    if row is None:
        return None
    logger.debug(f"Claimed computer {computer_id} for start (was {from_status})")
    await _publish_shadowed(row, "starting")
    return dict(row)


async def try_bind_computer_provider_ref(
    computer_id: str,
    *,
    provider_ref: str,
    expected_previous_provider_ref: Optional[str],
    platform_secret_version: int,
    expected_always_on: Optional[bool] = None,
    expected_host_unavailable_since: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """The sole provider_ref writer uses CAS to prevent orphaned concurrent provisions.

    A losing caller must delete its own machine. Always replace platform_secret_version:
    0 means uncertified and possibly plaintext (021); inheriting the old generation
    would hide that machine from the fleet sweeper.
    """
    async with _computer_cursor() as cur:
        await cur.execute(
            bind_provider_ref_statement(
                authority="computer",
                returning=_COMPUTER_COLS,
                guard_always_on=expected_always_on is not None,
                guard_host_outage=expected_host_unavailable_since is not None,
            ),
            {
                "computer_id": computer_id,
                "provider_ref": provider_ref,
                "expected_previous": expected_previous_provider_ref,
                "platform_secret_version": platform_secret_version,
                "expected_always_on": expected_always_on,
                "expected_host_unavailable_since": expected_host_unavailable_since,
            },
        )
        row = await cur.fetchone()

    if row is None:
        return None
    await _publish_shadowed(row, "running")
    return dict(row)


async def stamp_computer_host_unavailable(
    computer_id: str, sandbox_id: str
) -> Optional[tuple[datetime, timedelta]]:
    """Start the outage clock for the sandbox the row names, and read it.

    The first refused start stamps and every later one keeps that stamp, so the
    answer is when the host started refusing and for how long, whichever worker
    asked. None when the row names another sandbox, or none: a replacement has
    its own clock, and this failure says nothing about it.
    """
    async with _computer_cursor() as cur:
        await cur.execute(
            """
            UPDATE computers
            SET host_unavailable_since = COALESCE(host_unavailable_since, NOW())
            WHERE computer_id = %(computer_id)s
              AND provider_ref = %(sandbox_id)s
              AND status != 'deleted'
            RETURNING host_unavailable_since,
                      NOW() - host_unavailable_since AS unavailable_for
            """,
            {"computer_id": computer_id, "sandbox_id": sandbox_id},
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return row["host_unavailable_since"], row["unavailable_for"]


async def clear_computer_host_unavailable(
    computer_id: str, sandbox_id: str
) -> Optional[str]:
    """End the outage clock: the sandbox the row names came up.

    The sandbox the row names now, *sandbox_id* when it is still this one. A
    rebuild the outage authorized binds only while the clock it started on is
    still running, so ending the clock here refuses that bind, and a bind that
    already landed shows here as another sandbox. The row is written even with
    no clock running, because only the row lock orders this against that bind.
    """
    params = {"computer_id": computer_id, "sandbox_id": sandbox_id}
    async with _computer_cursor() as cur:
        await cur.execute(
            """
            UPDATE computers
            SET host_unavailable_since = NULL
            WHERE computer_id = %(computer_id)s
              AND provider_ref = %(sandbox_id)s
            RETURNING provider_ref
            """,
            params,
        )
        if await cur.fetchone() is not None:
            return sandbox_id
        await cur.execute(
            "SELECT provider_ref FROM computers WHERE computer_id = %(computer_id)s",
            params,
        )
        row = await cur.fetchone()
    return row["provider_ref"] if row else None


async def set_computer_resource_tier(
    computer_id: str,
    tier: str,
    *,
    expected_status: str | None = None,
    expected_provider_ref: str | None = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    if expected_status is not None:
        async with _computer_cursor(conn) as cur:
            await cur.execute(
                shadowed_write(
                    authority="computer",
                    computer_set="resource_tier = %(value)s",
                    workspace_set="resource_tier = %(value)s",
                    computer_guard=(
                        "\n                  AND c.status = %(expected_status)s"
                        "\n                  AND c.provider_ref IS NOT DISTINCT FROM"
                        " %(expected_ref)s"
                    ),
                    computer_returning=_COMPUTER_COLS,
                    workspace_returning="w.workspace_id",
                    select=(
                        f"SELECT {_COMPUTER_COLS},"
                        " (SELECT count(*) FROM shadow) AS shadowed_workspaces"
                        " FROM comp"
                    ),
                ),
                {
                    "computer_id": computer_id,
                    "value": tier,
                    "expected_status": expected_status,
                    "expected_ref": expected_provider_ref,
                },
            )
            row = await cur.fetchone()
        return dict(row) if row else None
    return await _set_computer_scalar(computer_id, "resource_tier", tier, conn=conn)


async def set_computer_always_on(
    computer_id: str,
    enabled: bool,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    return await _set_computer_scalar(computer_id, "is_always_on", enabled, conn=conn)


async def _set_computer_scalar(
    computer_id: str,
    column: str,
    value: Any,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Whitelist interpolated column names with _SETTABLE_SCALAR_COLUMNS.

    Workspace shadows remain required for platform entitlement counts.
    """
    if column not in _SETTABLE_SCALAR_COLUMNS:
        raise ValueError(f"Column not settable via _set_computer_scalar: {column!r}")

    async with _computer_cursor(conn) as cur:
        await cur.execute(
            shadowed_write(
                authority="computer",
                computer_set=f"{column} = %(value)s",
                workspace_set=f"{column} = %(value)s",
                computer_returning=_COMPUTER_COLS,
                workspace_returning="w.workspace_id",
                select=(
                    f"SELECT {_COMPUTER_COLS},"
                    " (SELECT count(*) FROM shadow) AS shadowed_workspaces"
                    " FROM comp"
                ),
            ),
            {"computer_id": computer_id, "value": value},
        )
        row = await cur.fetchone()

    if row is None:
        return None
    logger.info(f"Set computer {computer_id} {column} to: {value}")
    return dict(row)


async def stamp_computer_platform_secret_version(
    version: int,
    *,
    computer_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    expected_provider_ref: Optional[str],
    conn=None,
) -> int:
    """The named entry row owns the CAS; the other table is its shadow.

    A zero count is fatal to the sweeper but may be ignored by best-effort resync.
    """
    if (computer_id is None) == (workspace_id is None):
        raise ValueError("Pass exactly one of computer_id or workspace_id")

    guards = {
        "computer_guard": (
            "\n                  AND c.provider_ref IS NOT DISTINCT FROM"
            " %(expected_ref)s"
        ),
        "workspace_guard": (
            "\n                  AND w.sandbox_id IS NOT DISTINCT FROM %(expected_ref)s"
        ),
        "computer_set": "platform_secret_version = %(version)s",
        "workspace_set": "platform_secret_version = %(version)s",
        "workspace_fence": FENCE_NOT_DELETED,
    }
    if computer_id is not None:
        sql = shadowed_write(
            authority="computer",
            computer_returning="c.computer_id",
            workspace_returning="w.workspace_id",
            select="SELECT count(*) AS stamped FROM comp",
            **guards,
        )
    else:
        sql = shadowed_write(
            authority="workspace",
            computer_returning="c.computer_id",
            workspace_returning="w.workspace_id",
            # Count comp, not shadow: the machine owns the generation, so a
            # refused machine CAS is a failed stamp however the shadow landed.
            select="SELECT count(*) AS stamped FROM comp",
            **guards,
        )

    async with _computer_cursor(conn) as cur:
        await cur.execute(
            sql,
            {
                "version": version,
                "computer_id": computer_id,
                "workspace_id": workspace_id,
                "expected_ref": expected_provider_ref,
            },
        )
        row = await cur.fetchone()
    return int((row or {}).get("stamped") or 0)


async def update_computer_activity(
    computer_id: str,
    *,
    conn=None,
) -> bool:
    """SQL cooldowns hold across workers; only update_workspace_activity knows which project to stamp."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET last_activity_at = NOW(), updated_at = NOW()
            WHERE computer_id = %s
              AND {FENCE_NOT_DELETED}
              AND (last_activity_at IS NULL
                   OR last_activity_at < NOW() - INTERVAL '60 seconds')
            """,
            (computer_id,),
        )
        return cur.rowcount > 0


async def rename_computer(
    computer_id: str,
    name: str,
    *,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """The name has no workspace shadow; nothing but the computer reads it."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET name = %s, updated_at = NOW()
            WHERE computer_id = %s
              AND {FENCE_NOT_DELETED}
            RETURNING {_COMPUTER_COLS}
            """,
            (name, computer_id),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def record_computer_disk(
    computer_id: str,
    *,
    sandbox_id: str,
    observed_at: datetime,
    total_bytes: int,
    used_bytes: int,
    free_bytes: int,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Record a reading of ``sandbox_id`` taken at ``observed_at``; None when it lost.

    Readings race across workers and a breakdown's ``du`` can run long after
    its ``df``, so the newest observation wins, not the last write, and a
    reading of a sandbox the machine no longer has is dropped. Only a running
    or stopping machine takes one: a replacement keeps the ref on the old
    sandbox while it starts, and a reading landing then would undo the clear.
    Leaves updated_at alone: a reading is an observation, not an edit of the row.
    """
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET disk_total_bytes = %(total)s,
                disk_used_bytes = %(used)s,
                disk_free_bytes = %(free)s,
                disk_measured_at = %(observed_at)s,
                disk_sandbox_ref = %(sandbox_id)s
            WHERE computer_id = %(computer_id)s
              AND provider_ref = %(sandbox_id)s
              AND status IN ('running', 'stopping')
              AND (disk_measured_at IS NULL OR disk_measured_at < %(observed_at)s)
              AND {FENCE_NOT_DELETED}
            RETURNING {_COMPUTER_COLS}
            """,
            {
                "total": total_bytes,
                "used": used_bytes,
                "free": free_bytes,
                "observed_at": observed_at,
                "computer_id": computer_id,
                "sandbox_id": sandbox_id,
            },
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def clear_computer_disk(
    computer_id: str,
    *,
    sandbox_id: str,
    conn=None,
) -> bool:
    """Drop the reading taken on ``sandbox_id``, once that sandbox is destroyed.

    For a replacement that leaves ``provider_ref`` naming the destroyed
    sandbox until the next start: the ref fence alone would keep serving that
    sandbox's total and fullness as the machine's. Fenced on the reading's own
    sandbox, so a reading of any other one is never touched.
    """
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            """
            UPDATE computers
            SET disk_total_bytes = NULL,
                disk_used_bytes = NULL,
                disk_free_bytes = NULL,
                disk_measured_at = NULL,
                disk_sandbox_ref = NULL
            WHERE computer_id = %s
              AND disk_sandbox_ref = %s
            """,
            (computer_id, sandbox_id),
        )
        return cur.rowcount > 0


async def touch_computer_starting(
    computer_id: str,
    *,
    conn=None,
) -> bool:
    """Renew the durable start claim while a slow provider restore is alive."""
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET updated_at = NOW()
            WHERE computer_id = %s
              AND status = 'starting'
              AND {FENCE_NOT_DELETED}
            """,
            (computer_id,),
        )
        return cur.rowcount > 0


async def stamp_computer_layout_version(
    computer_id: str,
    version: int,
    *,
    conn=None,
) -> bool:
    """The sandbox manifest is authoritative; 0 means no sync has reported, not layout v0.

    Persist only observed versions so consolidation and reap can inspect stopped machines.
    """
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return False

    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET layout_version = %(version)s, updated_at = NOW()
            WHERE computer_id = %(computer_id)s
              AND {FENCE_NOT_DELETED}
              AND layout_version IS DISTINCT FROM %(version)s
            RETURNING computer_id
            """,
            {"computer_id": computer_id, "version": version},
        )
        row = await cur.fetchone()

    if row is not None:
        logger.info(f"Computer {computer_id} layout version stamped: {version}")
    return row is not None


async def stamp_computer_mcp_config_version(
    computer_id: str,
    *,
    conn=None,
) -> Optional[int]:
    """An observation of the machine's wrapper union, stamped once an asset sync rebuilds it.

    Never a gate: the stamp rides the asset sync, so it lags every config write,
    and a compare-and-swap refusing on it withholds the sync that would advance
    it. A shared machine records 0 because per-workspace writers reach no single
    generation; a sole workspace lends its own until the resolve becomes
    machine-scoped. The supervisor and asset sync compare the machine-wide
    _internal/tools/.union.json generation at every workspace count.
    """
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        return None

    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            WITH live AS (
                SELECT count(*) AS n, min(w.mcp_config_version) AS version
                FROM workspaces w
                WHERE w.computer_id = %(computer_id)s AND w.status <> 'deleted'
            )
            UPDATE computers c
            SET mcp_config_version = CASE
                    WHEN live.n = 1 THEN COALESCE(live.version, 0)
                    ELSE 0
                END,
                updated_at = NOW()
            FROM live
            WHERE c.computer_id = %(computer_id)s
              AND c.{FENCE_NOT_DELETED}
              AND c.mcp_config_version IS DISTINCT FROM CASE
                    WHEN live.n = 1 THEN COALESCE(live.version, 0)
                    ELSE 0
                END
            RETURNING c.mcp_config_version
            """,
            {"computer_id": computer_id},
        )
        row = await cur.fetchone()

    if row is None:
        # Avoid a round trip to distinguish unchanged from gone on the asset-sync path.
        return None
    stamped = int(row["mcp_config_version"] or 0)
    logger.info(f"Computer {computer_id} MCP config version stamped: {stamped}")
    return stamped


async def _publish_shadowed(row: Dict[str, Any], status: str) -> None:
    """Keep workspace-channel fan-out for old deployment readers and rollback.

    The computer channel reaches all machine subscribers; missed wakes fall back to polling.
    """
    from src.server.services.workspace_status_pubsub import (
        publish_computer_status_change,
        publish_status_change,
    )

    computer_id = row.get("computer_id")
    if computer_id:
        await publish_computer_status_change(str(computer_id), status)
    for workspace_id in row.get("shadowed_workspace_ids") or ():
        await publish_status_change(str(workspace_id), status)
