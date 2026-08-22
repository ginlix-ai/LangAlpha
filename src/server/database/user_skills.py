"""Database CRUD for user- and workspace-tier skills (``user_skills``).

One row per skill a user owns, carrying the denormalized SKILL.md frontmatter
so listings and the per-turn agent build never open the archive. The archive
bytes live in object storage (``archive_key``) or inline (``archive_blob``)
when no object storage is configured.

A row's scope is its ``workspace_id``: NULL = user tier (every workspace),
set = that workspace only. Scope-keyed functions take ``workspace_id`` and
match it exactly (``IS NOT DISTINCT FROM``); a workspace row may reuse a
user-tier name and shadows it there, so name lookups are only unique within
one scope. ``workspace_skill_disables`` records per-workspace disables of
skills the workspace merely inherits (platform + user tier), which have no
row in the workspace scope to flag.

``archive_blob`` is excluded from every read except :func:`get_user_skill_archive_blob`
— it is up to half a megabyte per row, and the hot paths (listing, agent build)
need only the metadata.

``plugin_id``/``plugin_skill_dir`` mark a row as owned by an installed plugin
(written only by the plugin install/update path, always at the user tier).
Every user edit clears them in place — re-upload, move, and the reconciler's
content write-back — which is the fork-on-edit semantic: a name that is
un-owned tells a later plugin update to skip the row rather than overwrite
the customization.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)


class SkillNameTaken(ValueError):
    """An ``overwrite=False`` upsert found the scope+name already occupied.

    A ValueError so every existing caller's per-skill isolation still catches
    it; a distinct type so the plugin fan-out can report "exists" rather than
    the trigger-conflict its other ValueErrors mean.
    """


class SkillNotOwned(ValueError):
    """An ``owned_by_plugin`` upsert found the row detached under it.

    The write-side counterpart of the delete's ``owned_by_plugin`` predicate.
    A ValueError for the same reason as its sibling, and distinct so an update
    can report the fork it declined to touch rather than an error.
    """

# Namespace for the per-workspace skill-sync advisory lock (two-arg form).
# The reconciler holds the session-level variant across a whole pass;
# workspace-scoped content mutations (upsert/move/delete) take the xact-level
# variant so they serialize against it. Lock order is always SKILL_SYNC →
# per-user lock; for cross-workspace moves, both workspace locks sorted by id.
_SKILL_SYNC_NS = "SKILL_SYNC"

# Rows of a soft-deleted workspace survive so restoring the workspace brings
# its skills back, but they are not part of the live inventory: no management
# surface can reach them, so counting them against the per-user caps would
# reserve a budget nobody can free.
_LIVE_SCOPE = """(
    user_skills.workspace_id IS NULL
    OR EXISTS (
        SELECT 1 FROM workspaces w
        WHERE w.workspace_id = user_skills.workspace_id
          AND w.status <> 'deleted'
    )
)"""


async def _lock_skill_sync_xact(cur, workspace_id: str) -> None:
    await cur.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s::text))",
        (_SKILL_SYNC_NS, workspace_id),
    )


class SkillSyncLockBusy(Exception):
    """Another worker holds this workspace's sync lock."""


@asynccontextmanager
async def workspace_skill_sync_lock(workspace_id: str):
    """Session-level advisory lock held across one full reconcile pass.

    Pins one pooled connection for the duration; released in ``finally`` and
    by Postgres automatically if the connection dies mid-pass. Acquisition is
    try-only: a pass is periodic, so waiting behind a stuck holder would park
    a second pooled connection for as long as that holder lives, and a queue
    of waiters is how one hung sandbox exhausts the pool.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s::text))",
                (_SKILL_SYNC_NS, workspace_id),
            )
            row = await cur.fetchone()
        if not (row and row[0]):
            raise SkillSyncLockBusy(workspace_id)
        try:
            yield
        finally:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s::text))",
                    (_SKILL_SYNC_NS, workspace_id),
                )

# Hard cap on skills per user, mirroring MAX_CATALOG_SERVERS_PER_USER. Defined
# here (not in services/user_skills/limits.py, which re-exports them) so the
# database layer never imports from services.
MAX_SKILLS_PER_USER = 50

# Summed archive size of one user's skills. Bounds both the object-storage
# footprint and the host cache dir a sync has to materialize.
MAX_SKILL_TOTAL_BYTES_PER_USER = 32 * 1024 * 1024

# Every column except archive_blob. `has_inline_archive` lets a caller tell
# which storage backs the row without paying for the bytes.
_SKILL_COLUMN_NAMES = (
    "user_skill_id", "user_id", "workspace_id", "name", "command",
    "description", "license", "frontmatter", "allowed_tools", "enabled",
    "confirmed", "plugin_id", "plugin_skill_dir", "content_hash",
    "archive_key", "archive_bytes", "file_count", "created_at", "updated_at",
)


def _skill_columns(prefix: str = "") -> str:
    """Render the projection, optionally table-qualified.

    One list, two renderings: a bare RETURNING for writers, and a fully
    qualified one for the reads that JOIN. Deriving the second from the first
    makes a new column one edit instead of two that can drift apart.
    """
    return ", ".join(
        [f"{prefix}{col}" for col in _SKILL_COLUMN_NAMES]
        + [f"({prefix}archive_blob IS NOT NULL) AS has_inline_archive"]
    )


# RETURNING cannot JOIN, but it can carry a correlated subquery, so a writer
# hands back the owner's display fields too rather than a row on which
# plugin_name is None means either "no owner" or "this row came from a
# writer". One shape for every row this module returns is what lets
# _user_row_to_info read the three provenance fields the same way whatever
# produced the row; the alternative was a re-read per writer, which the two
# DELETE writers cannot do at all. Correlated on the target table by name,
# which is why no writer here may alias it.
_PLUGIN_DISPLAY_RETURNING = (
    ", (SELECT p.name FROM user_plugins p "
    "WHERE p.user_plugin_id = user_skills.plugin_id) AS plugin_name"
    ", (SELECT p.enabled FROM user_plugins p "
    "WHERE p.user_plugin_id = user_skills.plugin_id) AS plugin_enabled"
)
_SKILL_COLUMNS = _skill_columns() + _PLUGIN_DISPLAY_RETURNING

# The same list qualified for the plugin LEFT JOIN (full table name, so
# _LIVE_SCOPE's own qualification keeps working). Reads take the owner's
# fields off the join they already pay for; only the writers subselect.
_SKILL_COLUMNS_JOINED = (
    _skill_columns("user_skills.")
    + ", p.name AS plugin_name, p.enabled AS plugin_enabled"
)
_PLUGIN_JOIN = (
    "LEFT JOIN user_plugins p ON p.user_plugin_id = user_skills.plugin_id"
)


def _row_to_dict(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalize a raw row: UUIDs to str, timestamps to ISO 8601."""
    if row is None:
        return None
    out = dict(row)
    for key in ("user_skill_id", "workspace_id", "plugin_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("created_at", "updated_at"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    out["frontmatter"] = out.get("frontmatter") or {}
    out["allowed_tools"] = out.get("allowed_tools") or []
    return out


async def list_user_skills(
    user_id: str, *, workspace_id: str | None = None
) -> list[dict[str, Any]]:
    """Every skill in one scope (user tier or one workspace), ordered by name."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_SKILL_COLUMNS_JOINED} FROM user_skills {_PLUGIN_JOIN} "
                "WHERE user_skills.user_id = %s "
                "AND user_skills.workspace_id IS NOT DISTINCT FROM %s "
                "ORDER BY user_skills.name",
                (user_id, workspace_id),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def list_all_user_skills(user_id: str) -> list[dict[str, Any]]:
    """Every live-scope skill row — the all-scopes management view.

    Rows belonging to a deleted workspace are left out for the same reason
    they don't count against the caps: that scope is not reachable from any
    surface this listing feeds.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_SKILL_COLUMNS_JOINED} FROM user_skills {_PLUGIN_JOIN} "
                f"WHERE user_skills.user_id = %s AND {_LIVE_SCOPE} "
                "ORDER BY user_skills.name",
                (user_id,),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def list_skill_disables_for_user(user_id: str) -> list[dict[str, Any]]:
    """Per-workspace skill disables across ALL of a user's workspaces.

    Feeds the all-scopes view's per-name "active in" checklist; one query
    instead of one per workspace.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT workspace_id, name FROM workspace_skill_disables
                WHERE workspace_id IN
                    (SELECT workspace_id FROM workspaces WHERE user_id = %s)
                """,
                (user_id,),
            )
            return [
                {"workspace_id": str(r["workspace_id"]), "name": r["name"]}
                for r in await cur.fetchall()
            ]


async def list_enabled_user_skills(
    user_id: str, *, workspace_id: str | None = None
) -> list[dict[str, Any]]:
    """The agent build's input: only rows that should reach a turn.

    With a ``workspace_id`` this is the two-scope union (user tier plus that
    workspace's rows) — the caller resolves name shadowing; without one it is
    the user tier alone. This query, and only this one, carries the
    plugin-disable join predicate: plugin-level disable reaches skills
    exclusively through this delivery chokepoint, while the management reads
    keep returning the rows (with the owner's state projected for display).
    """
    scope = (
        "user_skills.workspace_id IS NULL"
        if workspace_id is None
        else "(user_skills.workspace_id IS NULL OR user_skills.workspace_id = %s)"
    )
    params = (user_id,) if workspace_id is None else (user_id, workspace_id)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_SKILL_COLUMNS_JOINED} FROM user_skills {_PLUGIN_JOIN} "
                f"WHERE user_skills.user_id = %s AND user_skills.enabled "
                f"AND (user_skills.plugin_id IS NULL OR p.enabled) "
                f"AND {scope} ORDER BY user_skills.name",
                params,
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def get_user_skill(
    user_id: str, name: str, *, workspace_id: str | None = None, conn=None
) -> dict[str, Any] | None:
    """One skill's metadata by scope and name, or None."""
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_SKILL_COLUMNS_JOINED} FROM user_skills {_PLUGIN_JOIN} "
                "WHERE user_skills.user_id = %s AND user_skills.name = %s "
                "AND user_skills.workspace_id IS NOT DISTINCT FROM %s",
                (user_id, name, workspace_id),
            )
            return _row_to_dict(await cur.fetchone())


async def get_user_skill_archive_blob(
    user_id: str, user_skill_id: str
) -> bytes | None:
    """The inline archive bytes, or None when the row is object-storage backed.

    Keyed by row id, not name — with workspace shadowing, a name no longer
    identifies one row per user.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT archive_blob FROM user_skills "
                "WHERE user_id = %s AND user_skill_id = %s",
                (user_id, user_skill_id),
            )
            row = await cur.fetchone()
            if not row or row[0] is None:
                return None
            return bytes(row[0])


@asynccontextmanager
async def archive_key_unused_guard(archive_key: str, user_id: str):
    """Yield True when no row references this key, holding the write lock.

    Keys are content-addressed per user, so two same-content skills share one
    object and a superseded key can only be deleted once nothing points at it.
    The lock has to span the storage delete, not just the query: it is the same
    per-user lock every write takes, so releasing it early would let an upload
    dedup onto the key and then find its bytes gone. Caller does the delete
    inside the ``with`` block.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
                await cur.execute(
                    "SELECT 1 FROM user_skills WHERE archive_key = %s LIMIT 1",
                    (archive_key,),
                )
                yield await cur.fetchone() is None


async def _check_trigger_clash(
    cur, user_id: str, name: str, workspace_id: str | None
) -> None:
    """A name is also a trigger, so it must not land on a sibling's alias.

    Within one scope nothing breaks the tie between two rows answering to the
    same slash command. Across tiers it is fine: a workspace row deliberately
    wins in its own workspace.
    """
    await cur.execute(
        "SELECT name FROM user_skills WHERE user_id = %s "
        "AND workspace_id IS NOT DISTINCT FROM %s "
        "AND command = %s AND name <> %s LIMIT 1",
        (user_id, workspace_id, name, name),
    )
    clash = await cur.fetchone()
    if clash is not None:
        raise ValueError(
            f"/{name} is already the command of the skill {clash['name']!r}"
        )


async def _platform_override_values(cur, user_id: str) -> set[str]:
    """The user's platform-skill alias values, read from the table under the
    caller's per-user advisory lock. The cached reader
    (``services.features.get_skill_command_overrides``) is fine for the
    friendly pre-checks, but a trigger writer must see what a concurrent
    ``set_platform_alias`` — which holds the same lock across its
    check-and-write — actually committed."""
    await cur.execute(
        "SELECT other_preference #> '{skills,command_overrides}' AS ov "
        "FROM user_preferences WHERE user_id = %s",
        (user_id,),
    )
    row = await cur.fetchone()
    ov = (row or {}).get("ov")
    if not isinstance(ov, dict):
        return set()
    return {str(v) for v in ov.values() if v}


@asynccontextmanager
async def user_trigger_guard(user_id: str):
    """Hold the per-user trigger lock across a cross-tier check-and-write.

    ``set_platform_alias`` reads both tiers, checks, then writes preferences;
    the row writers here check the platform tier symmetrically. Without one
    shared guard the two can each read the other tier pre-commit and both
    conclude a trigger is free. Reads and the preferences write may run on
    other connections — they complete (and commit) before the guard exits,
    which is all the mutual exclusion needs. Never nest inside a
    SKILL_SYNC-holding transaction's caller (lock order is SKILL_SYNC →
    per-user, and this guard takes only the per-user half).
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
            yield


async def _free_command(
    cur, user_id: str, name: str, workspace_id: str | None, command: str | None
) -> str | None:
    """Re-check an alias seed under the lock; None when it is no longer free.

    ``free_seed`` picks the seed outside any lock, and the upload path then
    spends a slow object PUT before it gets here, so a sibling can take the
    trigger in between. Silent drop rather than a conflict, matching the seed
    policy: the skill still installs, triggered by its name.
    """
    if command is None:
        return None
    if command in await _platform_override_values(cur, user_id):
        return None
    await cur.execute(
        "SELECT 1 FROM user_skills WHERE user_id = %s "
        "AND workspace_id IS NOT DISTINCT FROM %s "
        "AND COALESCE(command, name) = %s AND name <> %s LIMIT 1",
        (user_id, workspace_id, command, name),
    )
    return None if await cur.fetchone() is not None else command


async def _check_skill_caps(
    cur, user_id: str, name: str, workspace_id: str | None, archive_bytes: int
) -> None:
    """Per-user count/bytes caps; the exact row being replaced is excluded so
    overwriting an existing skill is always allowed. Caller holds the per-user
    advisory lock."""
    await cur.execute(
        "SELECT COUNT(*) AS cnt, "
        "COALESCE(SUM(archive_bytes), 0) AS total_bytes "
        "FROM user_skills WHERE user_id = %s AND NOT "
        "(name = %s AND workspace_id IS NOT DISTINCT FROM %s) "
        f"AND {_LIVE_SCOPE}",
        (user_id, name, workspace_id),
    )
    stats = await cur.fetchone()
    if stats["cnt"] >= MAX_SKILLS_PER_USER:
        raise ValueError(
            f"Maximum of {MAX_SKILLS_PER_USER} skills per user reached"
        )
    if stats["total_bytes"] + archive_bytes > MAX_SKILL_TOTAL_BYTES_PER_USER:
        raise ValueError(
            "Skill storage limit reached "
            f"({MAX_SKILL_TOTAL_BYTES_PER_USER} bytes per user). "
            "Delete a skill first."
        )


async def upsert_user_skill(
    user_id: str,
    name: str,
    *,
    description: str,
    license: str | None,
    frontmatter: dict[str, Any],
    allowed_tools: list[str],
    confirmed: bool,
    content_hash: str,
    archive_key: str | None,
    archive_blob: bytes | None,
    archive_bytes: int,
    file_count: int,
    enabled: bool = True,
    workspace_id: str | None = None,
    plugin_id: str | None = None,
    plugin_skill_dir: str | None = None,
    command: str | None = None,
    overwrite: bool = True,
    owned_by_plugin: str | None = None,
    conn=None,
) -> tuple[dict[str, Any], str | None]:
    """Insert or replace a skill by scope and name.

    ``overwrite=False`` makes an existing row in the same scope a
    :class:`SkillNameTaken` refusal instead of a replacement — the plugin
    fan-out's collision check, moved under this function's lock where it is
    atomic.

    Returns ``(row, superseded_archive_key)`` — the caller deletes the
    superseded object after the write commits, so a failed upsert can never
    orphan the bytes the surviving row still points at.

    Both caps are per user across every scope, enforced under an advisory
    lock on the user so concurrent uploads can't slip past them. The exact
    row being replaced (scope + name) is excluded from both counts:
    overwriting an existing skill is always allowed.

    On replace, ``enabled`` is preserved (a disabled skill re-uploaded stays
    disabled) while the plugin provenance columns take the caller's values —
    a direct re-upload of a plugin-owned name therefore detaches it, which is
    the fork-on-edit semantic. A detach off a DISABLED plugin additionally
    carries the OFF state onto the row: suppression lived in the delivery
    query's ``plugin_id IS NULL OR p.enabled`` predicate, so clearing the
    provenance would turn a skill the user had switched off at the plugin into
    an unconditionally delivered one. Re-uploading your own copy is not consent
    to start running it. The catalog's fork-on-edit already owes this
    (``services/mcp_catalog.apply_catalog_edit``); this is the same rule on
    the skill half.

    ``owned_by_plugin`` narrows the replace to a row that plugin still owns,
    the write-side counterpart of the delete's predicate. A plugin update
    decides to replace by reading ownership first, then spends a zip
    validation and an object PUT before writing; a Customize landing in that
    window would otherwise be re-adopted under the package's content AND have
    its archive dropped as superseded, which is the one destruction here that
    nothing can undo. Raises ``SkillNotOwned`` instead.

    ``command`` seeds only on insert: the column is
    authoritative after creation, so a re-upload never resets a user's alias.
    The seed is re-checked here under the lock, since the caller chose it
    before spending the object PUT.
    """
    async with get_db_connection(conn) as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                if workspace_id is not None:
                    await _lock_skill_sync_xact(cur, workspace_id)
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
                await _check_skill_caps(cur, user_id, name, workspace_id, archive_bytes)

                await _check_trigger_clash(cur, user_id, name, workspace_id)
                # The router's ensure_free_of_platform ran before the object
                # PUT; re-check under the lock so a platform alias committed
                # since (set_platform_alias holds this same lock) can't end
                # up duplicated by this row's name.
                if name in await _platform_override_values(cur, user_id):
                    raise ValueError(
                        f"/{name} is already in use by another skill"
                    )
                command = await _free_command(
                    cur, user_id, name, workspace_id, command
                )

                # The row being replaced is excluded from the aggregate above,
                # so read its archive_key separately to hand back for cleanup.
                # p.enabled rides along so a detach can carry the plugin's OFF
                # state onto the row. FOR UPDATE OF s: the outer join's
                # nullable side cannot be locked, and this only needs the skill.
                await cur.execute(
                    "SELECT s.archive_key, s.plugin_id, p.enabled AS plugin_enabled "
                    "FROM user_skills s "
                    "LEFT JOIN user_plugins p "
                    "  ON p.user_id = s.user_id AND p.user_plugin_id = s.plugin_id "
                    "WHERE s.user_id = %s AND s.name = %s "
                    "AND s.workspace_id IS NOT DISTINCT FROM %s FOR UPDATE OF s",
                    (user_id, name, workspace_id),
                )
                prior = await cur.fetchone()
                prior_key = prior["archive_key"] if prior else None
                if (
                    owned_by_plugin is not None
                    and prior is not None
                    and str(prior["plugin_id"] or "") != owned_by_plugin
                ):
                    raise SkillNotOwned(
                        f"skill {name!r} is no longer owned by this plugin"
                    )
                if not overwrite and prior is not None:
                    # The caller checked this name was free before spending the
                    # object PUT; here, under the lock and on a locked row, is
                    # the only place that check can be true when it is acted on.
                    # Without it a plugin install races a self-upload of the
                    # same name and the ON CONFLICT arm replaces the user's own
                    # skill with the package's, stamped plugin-owned — so a
                    # later uninstall deletes work the plugin never created.
                    raise SkillNameTaken(
                        f"a skill named {name!r} already exists"
                    )

                if (
                    plugin_id is None
                    and prior is not None
                    and prior["plugin_id"] is not None
                    and prior["plugin_enabled"] is False
                ):
                    # Detaching off a disabled plugin. `enabled` is outside the
                    # DO UPDATE SET list below, so writing it here survives the
                    # upsert — and the row is already locked, so this and the
                    # replace are one atomic step.
                    await cur.execute(
                        "UPDATE user_skills SET enabled = FALSE "
                        "WHERE user_id = %s AND name = %s "
                        "AND workspace_id IS NOT DISTINCT FROM %s",
                        (user_id, name, workspace_id),
                    )

                # Uniqueness is a partial index per scope, so ON CONFLICT must
                # name the matching index's columns + predicate to infer it.
                conflict_target = (
                    "(user_id, name) WHERE workspace_id IS NULL"
                    if workspace_id is None
                    else "(workspace_id, name) WHERE workspace_id IS NOT NULL"
                )
                await cur.execute(
                    f"""
                    INSERT INTO user_skills
                        (user_id, workspace_id, name, command, description,
                         license, frontmatter, allowed_tools, enabled,
                         confirmed, plugin_id, plugin_skill_dir, content_hash,
                         archive_key, archive_blob, archive_bytes, file_count,
                         created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT {conflict_target} DO UPDATE SET
                        description = EXCLUDED.description,
                        license = EXCLUDED.license,
                        frontmatter = EXCLUDED.frontmatter,
                        allowed_tools = EXCLUDED.allowed_tools,
                        confirmed = EXCLUDED.confirmed,
                        plugin_id = EXCLUDED.plugin_id,
                        plugin_skill_dir = EXCLUDED.plugin_skill_dir,
                        content_hash = EXCLUDED.content_hash,
                        archive_key = EXCLUDED.archive_key,
                        archive_blob = EXCLUDED.archive_blob,
                        archive_bytes = EXCLUDED.archive_bytes,
                        file_count = EXCLUDED.file_count,
                        updated_at = NOW()
                    RETURNING {_SKILL_COLUMNS}
                    """,
                    (
                        user_id, workspace_id, name, command, description,
                        license, Json(frontmatter), Json(allowed_tools),
                        enabled, confirmed, plugin_id, plugin_skill_dir,
                        content_hash, archive_key, archive_blob, archive_bytes,
                        file_count,
                    ),
                )
                row = _row_to_dict(await cur.fetchone())
                logger.info(
                    "[user_skills] upsert user_id=%s workspace_id=%s name=%s bytes=%d",
                    user_id, workspace_id, name, archive_bytes,
                )
                # Only a genuine replacement leaves an orphan, and only when the
                # new bytes landed under a different key (content-addressed keys
                # make a no-op re-upload return the same one).
                superseded = prior_key if prior_key and prior_key != archive_key else None
                return row, superseded


async def move_user_skill(
    user_id: str,
    name: str,
    *,
    from_workspace_id: str | None,
    to_workspace_id: str | None,
) -> dict[str, Any] | None:
    """Re-scope a skill row (user tier ↔ one workspace) in place.

    Raises ValueError when the name is taken in the target scope; returns None
    when no row exists in the source scope. Runs under the same per-user
    advisory lock as uploads, so the collision check and the update cannot
    race a concurrent upsert. Any per-workspace disable of this name in the
    two workspaces involved is cleared: the move is an explicit statement
    that the skill is wanted where it now lives (and it was live where it
    just left).

    A plugin-owned row cannot move INTO a workspace; that raises ValueError
    and the route turns it into a 409. Detaching it on the way down was the
    gentler-looking answer and is the wrong one: it drops the row out of the
    plugin's owned set while the manifest still declares the component, so
    the next plugin update re-creates it at the user tier and the plugin's
    copy goes live in every OTHER workspace under the very name the user had
    just scoped down. Refusing also keeps owned rows out of the workspace
    tier — the tier the sandbox reconciler writes back to — by construction,
    which is strictly stronger than clearing a column on the way in.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                for ws in sorted(
                    {w for w in (from_workspace_id, to_workspace_id) if w}
                ):
                    await _lock_skill_sync_xact(cur, ws)
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
                if to_workspace_id is not None:
                    # The JOIN makes this return a row only for an owned one,
                    # so there is no plugin_id null-check to get wrong.
                    await cur.execute(
                        "SELECT p.name FROM user_skills "
                        "JOIN user_plugins p "
                        "ON p.user_plugin_id = user_skills.plugin_id "
                        "WHERE user_skills.user_id = %s "
                        "AND user_skills.name = %s "
                        "AND user_skills.workspace_id IS NOT DISTINCT FROM %s",
                        (user_id, name, from_workspace_id),
                    )
                    owner = await cur.fetchone()
                    if owner is not None:
                        raise ValueError(
                            f"This skill is installed by the plugin "
                            f"{owner['name']!r}, which manages it at the "
                            "account level. Upload your own copy of the skill "
                            "to detach it from the plugin first, then move it. "
                            "Uninstalling the plugin removes the skill instead."
                        )
                await cur.execute(
                    "SELECT 1 FROM user_skills WHERE user_id = %s AND name = %s "
                    "AND workspace_id IS NOT DISTINCT FROM %s",
                    (user_id, name, to_workspace_id),
                )
                if await cur.fetchone() is not None:
                    raise ValueError(
                        f"A skill named {name!r} already exists in the "
                        "destination scope"
                    )
                # The moving row's NAME is reserved at the destination too,
                # even when an alias currently hides it: clearing that alias
                # skips collision checks by design, so the name it falls back
                # to has to still be free when it lands.
                await _check_trigger_clash(cur, user_id, name, to_workspace_id)
                # And its effective trigger (alias, else name) must not
                # collide with a destination row's trigger or name.
                await cur.execute(
                    """
                    WITH src AS (
                        SELECT COALESCE(command, name) AS trig FROM user_skills
                        WHERE user_id = %s AND name = %s
                        AND workspace_id IS NOT DISTINCT FROM %s
                    )
                    SELECT 1 FROM user_skills dest, src
                    WHERE dest.user_id = %s
                    AND dest.workspace_id IS NOT DISTINCT FROM %s
                    AND (dest.name = src.trig
                         OR COALESCE(dest.command, dest.name) = src.trig)
                    LIMIT 1
                    """,
                    (user_id, name, from_workspace_id, user_id, to_workspace_id),
                )
                if await cur.fetchone() is not None:
                    raise ValueError(
                        "The skill's command is already in use in the "
                        "destination scope"
                    )
                await cur.execute(
                    # Only rows moving UP reach the clear now: the guard
                    # above refuses the other direction, and fan-out only ever
                    # writes owned rows at the user tier. Kept for the legacy
                    # rows that predate the guard, which belong to whoever
                    # moved them down.
                    f"UPDATE user_skills SET workspace_id = %s, "
                    "plugin_id = NULL, plugin_skill_dir = NULL, "
                    "updated_at = NOW() "
                    "WHERE user_id = %s AND name = %s "
                    "AND workspace_id IS NOT DISTINCT FROM %s "
                    f"RETURNING {_SKILL_COLUMNS}",
                    (to_workspace_id, user_id, name, from_workspace_id),
                )
                row = _row_to_dict(await cur.fetchone())
                if row is None:
                    return None
                for ws in (from_workspace_id, to_workspace_id):
                    if ws is not None:
                        await cur.execute(
                            "DELETE FROM workspace_skill_disables "
                            "WHERE workspace_id = %s AND name = %s",
                            (ws, name),
                        )
                logger.info(
                    "[user_skills] move user_id=%s name=%s from=%s to=%s",
                    user_id, name, from_workspace_id, to_workspace_id,
                )
                return row


async def set_user_skill_enabled(
    user_id: str, name: str, enabled: bool, *, workspace_id: str | None = None
) -> dict[str, Any] | None:
    """Toggle a skill row in one scope. Returns the row, or None when absent."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                if workspace_id is not None:
                    await _lock_skill_sync_xact(cur, workspace_id)
                await cur.execute(
                    f"UPDATE user_skills SET enabled = %s, updated_at = NOW() "
                    f"WHERE user_id = %s AND name = %s "
                    f"AND workspace_id IS NOT DISTINCT FROM %s "
                    f"RETURNING {_SKILL_COLUMNS}",
                    (enabled, user_id, name, workspace_id),
                )
                return _row_to_dict(await cur.fetchone())


async def set_user_skill_command(
    user_id: str,
    name: str,
    command: str | None,
    *,
    workspace_id: str | None = None,
) -> dict[str, Any] | None:
    """Set (or clear, with None) a skill row's slash-command alias.

    Returns the row, or None when absent. Raises ValueError when the alias
    collides with another row's name or effective trigger — same-scope rows
    always, plus the user tier for a workspace row (a workspace alias must
    not shadow an inherited trigger; the reverse, checked at the API layer,
    is allowed only because workspace rows win in-workspace). Names stay
    reserved even when their own alias hides them: clearing an alias skips
    these checks, so the name it falls back to must never have been given
    away. Charset and reserved-name checks are the caller's job.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                if workspace_id is not None:
                    await _lock_skill_sync_xact(cur, workspace_id)
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
                if command is not None and command in (
                    await _platform_override_values(cur, user_id)
                ):
                    raise ValueError(
                        f"Command /{command} is already in use by another skill"
                    )
                if command is not None:
                    scope_pred = (
                        "workspace_id IS NULL"
                        if workspace_id is None
                        else "(workspace_id IS NULL OR workspace_id = %s)"
                    )
                    params: list[Any] = [user_id]
                    if workspace_id is not None:
                        params.append(workspace_id)
                    params.extend([command, command, name, workspace_id])
                    await cur.execute(
                        f"""
                        SELECT 1 FROM user_skills
                        WHERE user_id = %s AND {scope_pred}
                        AND (name = %s OR COALESCE(command, name) = %s)
                        AND NOT (name = %s
                                 AND workspace_id IS NOT DISTINCT FROM %s)
                        LIMIT 1
                        """,
                        params,
                    )
                    if await cur.fetchone() is not None:
                        raise ValueError(
                            f"Command /{command} is already in use by another skill"
                        )
                await cur.execute(
                    f"UPDATE user_skills SET command = %s, updated_at = NOW() "
                    "WHERE user_id = %s AND name = %s "
                    "AND workspace_id IS NOT DISTINCT FROM %s "
                    f"RETURNING {_SKILL_COLUMNS}",
                    (command, user_id, name, workspace_id),
                )
                row = _row_to_dict(await cur.fetchone())
                if row:
                    logger.info(
                        "[user_skills] command user_id=%s name=%s command=%s",
                        user_id, name, command,
                    )
                return row


async def delete_user_skill(
    user_id: str,
    name: str,
    *,
    workspace_id: str | None = None,
    owned_by_plugin: str | None = None,
    conn=None,
) -> dict[str, Any] | None:
    """Delete a skill row in one scope, returning it so the caller can drop
    its archive object. Returns None when there was nothing to delete.

    ``owned_by_plugin`` narrows the delete to a row that plugin still owns, so
    a Customize that detaches it after the caller read ownership is not
    silently overridden. Same predicate as the catalog's delete.
    """
    async with get_db_connection(conn) as db:
        async with db.transaction():
            async with db.cursor(row_factory=dict_row) as cur:
                if workspace_id is not None:
                    await _lock_skill_sync_xact(cur, workspace_id)
                await cur.execute(
                    f"DELETE FROM user_skills WHERE user_id = %s AND name = %s "
                    f"AND workspace_id IS NOT DISTINCT FROM %s "
                    f"AND (%s::uuid IS NULL OR plugin_id = %s::uuid) "
                    f"RETURNING {_SKILL_COLUMNS}",
                    (
                        user_id, name, workspace_id,
                        owned_by_plugin, owned_by_plugin,
                    ),
                )
                row = _row_to_dict(await cur.fetchone())
                if row:
                    if workspace_id is None:
                        # The deny-list markers describe THIS skill; leaving
                        # them would silently disable a later same-name
                        # upload (a different identity) in those workspaces.
                        # Workspace-scoped deletes keep them: there the
                        # marker points at the inherited skill, which the
                        # delete re-exposes.
                        await cur.execute(
                            "DELETE FROM workspace_skill_disables "
                            "WHERE name = %s AND workspace_id IN ("
                            "SELECT workspace_id FROM workspaces "
                            "WHERE user_id = %s)",
                            (name, user_id),
                        )
                    logger.info(
                        "[user_skills] delete user_id=%s workspace_id=%s name=%s",
                        user_id, workspace_id, name,
                    )
                return row


async def get_user_skill_by_id(
    user_id: str, user_skill_id: str
) -> dict[str, Any] | None:
    """One skill row by UUID, any scope — how the reconciler tells a moved
    row (UUID survives ``move_user_skill``) from a deleted one."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_SKILL_COLUMNS} FROM user_skills "
                "WHERE user_id = %s AND user_skill_id = %s",
                (user_id, user_skill_id),
            )
            return _row_to_dict(await cur.fetchone())


async def create_user_skill(
    user_id: str,
    name: str,
    *,
    workspace_id: str,
    description: str,
    license: str | None,
    frontmatter: dict[str, Any],
    allowed_tools: list[str],
    confirmed: bool,
    content_hash: str,
    archive_key: str | None,
    archive_blob: bytes | None,
    archive_bytes: int,
    file_count: int,
    command: str | None = None,
) -> dict[str, Any] | None:
    """Create-only insert for auto-import: never replaces an existing row.

    Returns None when the name is already taken in the scope (the reconciler
    re-decides via the arbiter); raises ValueError on caps. Takes only the
    per-user cap lock — the caller (the reconciler) already holds the
    workspace's session-level SKILL_SYNC lock, which is what serializes this
    against content mutations.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
                await _check_skill_caps(cur, user_id, name, workspace_id, archive_bytes)
                await _check_trigger_clash(cur, user_id, name, workspace_id)
                command = await _free_command(
                    cur, user_id, name, workspace_id, command
                )
                await cur.execute(
                    f"""
                    INSERT INTO user_skills
                        (user_id, workspace_id, name, command, description,
                         license, frontmatter, allowed_tools, enabled,
                         confirmed, content_hash, archive_key, archive_blob,
                         archive_bytes, file_count, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s,
                            %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (workspace_id, name) WHERE workspace_id IS NOT NULL
                        DO NOTHING
                    RETURNING {_SKILL_COLUMNS}
                    """,
                    (
                        user_id, workspace_id, name, command, description,
                        license, Json(frontmatter), Json(allowed_tools),
                        confirmed, content_hash, archive_key, archive_blob,
                        archive_bytes, file_count,
                    ),
                )
                row = _row_to_dict(await cur.fetchone())
                if row:
                    logger.info(
                        "[user_skills] auto-import user_id=%s workspace_id=%s name=%s",
                        user_id, workspace_id, name,
                    )
                return row


async def update_user_skill_content_cas(
    user_id: str,
    user_skill_id: str,
    expected_content_hash: str,
    *,
    description: str,
    license: str | None,
    frontmatter: dict[str, Any],
    allowed_tools: list[str],
    content_hash: str,
    archive_key: str | None,
    archive_blob: bytes | None,
    archive_bytes: int,
    file_count: int,
) -> tuple[dict[str, Any] | None, str | None]:
    """Pull-up write: replace a row's content only if it still carries the
    hash the reconciler observed. Returns ``(row, superseded_archive_key)``;
    ``(None, None)`` = CAS lost, the caller re-decides next pass.

    A content write-back DETACHES a plugin-owned row: a sandbox edit is an
    edit like any other, and leaving plugin_id in place would let the next
    plugin update overwrite the agent's work while the content had silently
    diverged from the plugin. Plugin skills install at the user tier, which
    the reconciler never links, so this is a belt-and-braces guard rather
    than a hot path.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
                )
                await cur.execute(
                    "SELECT name, workspace_id, archive_key FROM user_skills "
                    "WHERE user_id = %s AND user_skill_id = %s "
                    "AND content_hash = %s FOR UPDATE",
                    (user_id, user_skill_id, expected_content_hash),
                )
                prior = await cur.fetchone()
                if prior is None:
                    return None, None
                await _check_skill_caps(
                    cur, user_id, prior["name"], prior["workspace_id"], archive_bytes
                )
                await cur.execute(
                    f"""
                    UPDATE user_skills SET
                        description = %s, license = %s, frontmatter = %s,
                        allowed_tools = %s, content_hash = %s, archive_key = %s,
                        archive_blob = %s, archive_bytes = %s, file_count = %s,
                        plugin_id = NULL, plugin_skill_dir = NULL,
                        updated_at = NOW()
                    WHERE user_id = %s AND user_skill_id = %s
                    RETURNING {_SKILL_COLUMNS}
                    """,
                    (
                        description, license, Json(frontmatter),
                        Json(allowed_tools), content_hash, archive_key,
                        archive_blob, archive_bytes, file_count,
                        user_id, user_skill_id,
                    ),
                )
                row = _row_to_dict(await cur.fetchone())
                prior_key = prior["archive_key"]
                superseded = (
                    prior_key if prior_key and prior_key != archive_key else None
                )
                logger.info(
                    "[user_skills] pull-up user_id=%s skill_id=%s",
                    user_id, user_skill_id,
                )
                return row, superseded


async def delete_user_skill_cas(
    user_id: str, user_skill_id: str, expected_content_hash: str
) -> dict[str, Any] | None:
    """Deletion propagation: drop a row only if its content is still exactly
    what the ledger last synced (content beats deletion). None = CAS lost."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"DELETE FROM user_skills WHERE user_id = %s "
                "AND user_skill_id = %s AND content_hash = %s "
                f"RETURNING {_SKILL_COLUMNS}",
                (user_id, user_skill_id, expected_content_hash),
            )
            row = _row_to_dict(await cur.fetchone())
            if row:
                logger.info(
                    "[user_skills] sync-delete user_id=%s skill_id=%s name=%s",
                    user_id, user_skill_id, row["name"],
                )
            return row


async def list_workspace_skill_disables(workspace_id: str) -> set[str]:
    """Names of inherited skills this workspace has switched off."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT name FROM workspace_skill_disables WHERE workspace_id = %s",
                (workspace_id,),
            )
            return {r["name"] for r in await cur.fetchall()}


async def set_workspace_skill_disable(
    workspace_id: str, name: str, disabled: bool
) -> None:
    """Record or clear a workspace-level disable of an inherited skill."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            if disabled:
                await cur.execute(
                    "INSERT INTO workspace_skill_disables (workspace_id, name) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (workspace_id, name),
                )
            else:
                await cur.execute(
                    "DELETE FROM workspace_skill_disables "
                    "WHERE workspace_id = %s AND name = %s",
                    (workspace_id, name),
                )
