"""Shared workspaces and their members (migration 141).

``shared_workspaces`` names a shared workspace's holder (the ``users`` row
that owns its workspace and computer), its organization, the one team it
may name (migration 147) and whether members' own context reaches it
(migration 149); ``shared_workspace_members`` lists its people. Every
read here is keyed by a workspace or a holder, so a personal workspace never
reaches one.

Who reaches a shared workspace is one rule, :func:`reach_sql` for lists and
:func:`reaches` for one workspace: listed on it, or it is open to everyone in
its organization, or it names a team the person is in. Membership of the
organization itself, and its feature, are the caller's to check first.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from collections.abc import Collection
from typing import Any, Literal, Optional

from psycopg.rows import dict_row, tuple_row

from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import normalize_uuid

#: A holder's ``users.user_id`` starts with this; no person's does.
HOLDER_PREFIX = "shared-"
#: ``users.auth_provider`` on a holder's row, for a person reading the table.
HOLDER_AUTH_PROVIDER = "shared_workspace"


def is_holder(user_id: object) -> bool:
    return isinstance(user_id, str) and user_id.startswith(HOLDER_PREFIX)


class HolderNotAPerson(RuntimeError):
    """A path treated a holder as a person: asked the auth service about it,
    tried to act as it, or recorded a run against it. A bug at the call site,
    raised rather than sent."""


def refuse_holder(user_id: object, where: str) -> None:
    if is_holder(user_id):
        raise HolderNotAPerson(f"{where}: {user_id} is a shared workspace's holder")


def new_holder_id() -> str:
    return f"{HOLDER_PREFIX}{uuid.uuid4()}"

Role = Literal["manager", "member"]
#: Who may share the workspace's work outside it (migration 144).
ShareOut = Literal["managers", "members", "none"]


@dataclass(frozen=True)
class SharedRow:
    workspace_id: str
    holder_id: str
    org_id: str
    everyone: bool
    created_by: str
    share_out: ShareOut
    #: The team it names, as the auth service ids it; None for none.
    team_id: Optional[str] = None
    #: Members' own context follows the organization's grant here; false
    #: when its managers turned it off for it (migration 149).
    member_context: bool = True


#: :class:`SharedRow`'s columns, in its order.
SHARED_COLS = """
    s.workspace_id::text AS workspace_id, s.holder_id, s.org_id, s.everyone,
    s.created_by, s.share_out, s.team_id, s.member_context
"""


def reaches(row: SharedRow, role: Optional[Role], team_ids: Collection[str]) -> bool:
    """Whether someone in ``row``'s organization, listed on it with ``role``
    (None when not listed) and in the teams ``team_ids``, reaches it."""
    return role is not None or row.everyone or (
        row.team_id is not None and row.team_id in team_ids
    )


def reach_sql(shared: str, listed: str, teams: Optional[str]) -> str:
    """:func:`reaches` in SQL, for ``shared`` (an alias of
    ``shared_workspaces``): ``listed`` is SQL true when the person is listed
    on it, ``teams`` a placeholder binding their team ids, or None when they
    are in no team, which leaves the clause out so a person in none reads
    exactly what they read before teams existed."""
    team = f" OR {shared}.team_id = ANY({teams}::text[])" if teams else ""
    return f"({listed} OR {shared}.everyone{team})"


async def reach_held(
    conn, holder_id: str, user_id: str, org_id: str, team_ids: Collection[str]
) -> bool:
    """:func:`reaches` for a turn's START, on its connection: whether
    ``user_id``, in ``org_id`` and the teams ``team_ids`` (read before the
    transaction), reaches the workspace of ``holder_id``. Locks the rows that
    grant it until START commits: the workspace's row, which a change of its
    audience updates, and the person's member row, which a removal deletes.
    A change that committed first is read here; one still to come waits and
    then finds the run to stop (``turns.end_access``)."""
    listed = (
        "EXISTS (SELECT 1 FROM shared_workspace_members m"
        " WHERE m.workspace_id = s.workspace_id AND m.user_id = %(user)s"
        " FOR KEY SHARE)"
    )
    params: dict[str, Any] = {"holder": holder_id, "user": user_id, "org": org_id}
    if team_ids:
        params["teams"] = sorted(team_ids)
    reach = reach_sql("s", listed, "%(teams)s" if team_ids else None)
    async with get_db_connection(conn) as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            f"SELECT {reach} FROM shared_workspaces s"
            " WHERE s.holder_id = %(holder)s AND s.org_id = %(org)s"
            " FOR SHARE OF s",
            params,
        )
        row = await cur.fetchone()
    return bool(row and row[0])


async def _one(where: str, *params: Any) -> Optional[SharedRow]:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(f"SELECT {SHARED_COLS} FROM shared_workspaces s {where}", params)
        row = await cur.fetchone()
    return SharedRow(*row) if row else None


async def get_shared_workspace(workspace_id: str) -> Optional[SharedRow]:
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None
    return await _one("WHERE s.workspace_id = %s", workspace_id)


async def shared_of_holder(holder_id: str) -> Optional[SharedRow]:
    return await _one("WHERE s.holder_id = %s", holder_id)


async def update_settings(
    workspace_id: str,
    *,
    share_out: Optional[ShareOut] = None,
    member_context: Optional[bool] = None,
) -> Optional[SharedRow]:
    """Set each setting given, leaving the others; the row before the change,
    or None for no such workspace.

    The prior row is read under the row lock, so a manager's concurrent change
    to another setting is kept rather than restored from a stale snapshot."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            f"""
            WITH s AS (
                SELECT * FROM shared_workspaces
                 WHERE workspace_id = %s
                   FOR UPDATE
            )
            UPDATE shared_workspaces w
               SET share_out = COALESCE(%s, w.share_out),
                   member_context = COALESCE(%s, w.member_context)
              FROM s
             WHERE w.workspace_id = s.workspace_id
            RETURNING {SHARED_COLS}
            """,
            (workspace_id, share_out, member_context),
        )
        row = await cur.fetchone()
    return SharedRow(*row) if row else None


async def get_member_role(workspace_id: str, user_id: str) -> Optional[Role]:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "SELECT role FROM shared_workspace_members "
            "WHERE workspace_id = %s AND user_id = %s",
            (workspace_id, user_id),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def list_members(workspace_id: str) -> list[dict[str, Any]]:
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT m.user_id, m.role, m.added_by, m.added_at, u.name, u.email,
                   u.avatar_url
              FROM shared_workspace_members m
              JOIN users u ON u.user_id = m.user_id
             WHERE m.workspace_id = %s
             ORDER BY m.role, m.added_at, m.user_id
            """,
            (workspace_id,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def manager_ids(workspace_id: str) -> list[str]:
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "SELECT user_id FROM shared_workspace_members "
            "WHERE workspace_id = %s AND role = 'manager' ORDER BY added_at",
            (workspace_id,),
        )
        return [r[0] for r in await cur.fetchall()]


async def lock_org(cur, org_id: str) -> None:
    """Serialize creations in one organization, so its cap holds across
    workers: the count and the insert run under this transaction lock."""
    await cur.execute(
        "SELECT pg_advisory_xact_lock(hashtext('shared_workspaces:' || %s))",
        (org_id,),
    )


async def count_live_for_org(cur, org_id: str) -> int:
    await cur.execute(
        """
        SELECT count(*) FROM shared_workspaces s
          JOIN workspaces w ON w.workspace_id = s.workspace_id
         WHERE s.org_id = %s AND w.status <> 'deleted'
        """,
        (org_id,),
    )
    return int((await cur.fetchone())[0])


async def insert_holder(cur, holder_id: str, name: str) -> None:
    await cur.execute(
        """
        INSERT INTO users (user_id, email, name, auth_provider,
                           onboarding_completed, created_at, updated_at)
        VALUES (%s, NULL, %s, %s, FALSE, NOW(), NOW())
        """,
        (holder_id, name, HOLDER_AUTH_PROVIDER),
    )


async def insert_shared(
    cur,
    *,
    workspace_id: str,
    holder_id: str,
    org_id: str,
    everyone: bool,
    created_by: str,
    members: list[tuple[str, Role]],
    team_id: Optional[str] = None,
) -> SharedRow:
    await cur.execute(
        f"""
        INSERT INTO shared_workspaces AS s
            (workspace_id, holder_id, org_id, everyone, created_by, everyone_since,
             team_id, team_since)
        VALUES (%s, %s, %s, %s, %s, CASE WHEN %s THEN now() END,
                %s, CASE WHEN %s::text IS NOT NULL THEN now() END)
        RETURNING {SHARED_COLS}
        """,
        (workspace_id, holder_id, org_id, everyone, created_by, everyone, team_id, team_id),
    )
    row = SharedRow(*await cur.fetchone())
    for user_id, role in members:
        await cur.execute(
            """
            INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (workspace_id, user_id) DO NOTHING
            """,
            (workspace_id, user_id, role, created_by),
        )
    return row


async def add_member(workspace_id: str, user_id: str, role: Role, added_by: str) -> bool:
    """False when the person is already listed."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (workspace_id, user_id) DO NOTHING
            """,
            (workspace_id, user_id, role, added_by),
        )
        return cur.rowcount == 1


async def set_role(workspace_id: str, user_id: str, role: Role) -> Optional[Role]:
    """The role before the change, or None when the person is not listed."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            UPDATE shared_workspace_members m SET role = %s
              FROM shared_workspace_members old
             WHERE m.workspace_id = %s AND m.user_id = %s
               AND old.workspace_id = m.workspace_id AND old.user_id = m.user_id
            RETURNING old.role
            """,
            (role, workspace_id, user_id),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def remove_member(workspace_id: str, user_id: str) -> Optional[Role]:
    """The removed person's role, or None when they were not listed."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            "DELETE FROM shared_workspace_members "
            "WHERE workspace_id = %s AND user_id = %s RETURNING role",
            (workspace_id, user_id),
        )
        row = await cur.fetchone()
    return row[0] if row else None


@dataclass(frozen=True)
class Audience:
    everyone: bool
    team_id: Optional[str]


#: Whether it is open to everyone once changed: as asked, else as it was.
_EVERYONE = "COALESCE(%(everyone)s::boolean, s.everyone)"
#: The team it names once changed: none when open to everyone.
_TEAM = f"CASE WHEN {_EVERYONE} THEN NULL WHEN %(names)s THEN %(team)s::text ELSE s.team_id END"


async def set_audience(
    workspace_id: str, *, everyone: Optional[bool], team_id: Optional[str], names_team: bool
) -> tuple[Audience, Audience]:
    """Open the workspace to everyone or close it (``everyone`` None keeps
    it), and with ``names_team`` name ``team_id`` or none. Opening clears the
    team and naming one closes it. Who it reached before and after, read
    under the row's lock. Raises LookupError for no such workspace."""
    if everyone is None and names_team and team_id is not None:
        everyone = False
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        # Opening again restarts the moment from which people who reach it
        # through everyone have its turns to read (migration 143); naming the
        # team it already names keeps when it was named.
        await cur.execute(
            f"""
            WITH old AS (
                SELECT workspace_id, everyone, team_id FROM shared_workspaces
                 WHERE workspace_id = %(ws)s FOR UPDATE
            )
            UPDATE shared_workspaces s
               SET everyone_since = CASE
                       WHEN NOT {_EVERYONE} THEN NULL
                       WHEN s.everyone THEN s.everyone_since
                       ELSE now() END,
                   everyone = {_EVERYONE},
                   team_since = CASE
                       WHEN ({_TEAM}) IS NULL THEN NULL
                       WHEN s.team_id = ({_TEAM}) THEN s.team_since
                       ELSE now() END,
                   team_id = {_TEAM}
              FROM old
             WHERE s.workspace_id = old.workspace_id
            RETURNING old.everyone, old.team_id, s.everyone, s.team_id
            """,
            {"ws": workspace_id, "everyone": everyone, "names": names_team, "team": team_id},
        )
        row = await cur.fetchone()
    if row is None:
        raise LookupError(workspace_id)
    return Audience(bool(row[0]), row[1]), Audience(bool(row[2]), row[3])


async def count_naming_team(org_id: str, team_id: str) -> int:
    """How many of ``org_id``'s live shared workspaces name ``team_id``."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT count(*) FROM shared_workspaces s
              JOIN workspaces w ON w.workspace_id = s.workspace_id
             WHERE s.org_id = %s AND s.team_id = %s AND w.status <> 'deleted'
            """,
            (org_id, team_id),
        )
        return int((await cur.fetchone())[0])


async def list_for_member(
    user_id: str, org_id: str, team_ids: Collection[str] = ()
) -> list[tuple[SharedRow, Optional[Role]]]:
    """The live shared workspaces of ``org_id`` that ``user_id``, in the
    teams ``team_ids``, reaches (:func:`reach_sql`), with their listed role."""
    rows = await _list_for_member(user_id, org_id, team_ids, with_unread=False)
    return [(row, role) for row, role, _ in rows]


async def list_with_unread(
    user_id: str, org_id: str, team_ids: Collection[str] = ()
) -> list[tuple[SharedRow, Optional[Role], bool]]:
    """``list_for_member``, each with whether ``user_id`` has a thread there
    they have not read."""
    return await _list_for_member(user_id, org_id, team_ids, with_unread=True)


async def _list_for_member(
    user_id: str, org_id: str, team_ids: Collection[str], *, with_unread: bool
) -> list[tuple[SharedRow, Optional[Role], bool]]:
    # Imported here: the conversation readers import this module.
    from src.server.database.conversation.viewer_threads import has_unread_sql

    unread = has_unread_sql("s.workspace_id", "%(user)s") if with_unread else "FALSE"
    params: dict[str, Any] = {"user": user_id, "org": org_id}
    if team_ids:
        params["teams"] = sorted(team_ids)
    reach = reach_sql("s", "m.user_id IS NOT NULL", "%(teams)s" if team_ids else None)
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            f"""
            SELECT {SHARED_COLS}, m.role AS role, {unread} AS has_unread
              FROM shared_workspaces s
              JOIN workspaces w ON w.workspace_id = s.workspace_id
              LEFT JOIN shared_workspace_members m
                ON m.workspace_id = s.workspace_id AND m.user_id = %(user)s
             WHERE s.org_id = %(org)s AND w.status <> 'deleted'
               AND {reach}
             ORDER BY w.updated_at DESC, s.workspace_id
            """,
            params,
        )
        return [(SharedRow(*r[:-2]), r[-2], bool(r[-1])) for r in await cur.fetchall()]


async def unlisted_actors(workspace_id: str, holder_id: str) -> list[str]:
    """The people not listed on the workspace who have anything of theirs
    there that losing it would end: a turn, subagent run or command on
    record, or the computer's market-data token. Read when the workspace
    stops being open to everyone or changes its team, to ask of each whether
    they still reach it; a superset, since asking costs only a read."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT DISTINCT a.user_id FROM (
                SELECT r.user_id FROM conversation_responses r
                  JOIN conversation_threads t
                    ON t.conversation_thread_id = r.conversation_thread_id
                 WHERE t.workspace_id = %(ws)s
                UNION
                SELECT x.user_id FROM subagent_runs x
                  JOIN conversation_threads t ON t.conversation_thread_id = x.thread_id
                 WHERE t.workspace_id = %(ws)s AND x.status = 'in_progress'
                UNION
                SELECT c.user_id FROM command_runs c
                 WHERE c.workspace_id = %(ws)s AND c.stopped_by IS NULL
                UNION
                SELECT k.user_id FROM computer_data_tokens k
                  JOIN computers m USING (computer_id)
                 WHERE m.user_id = %(holder)s
            ) a
             WHERE a.user_id IS NOT NULL AND a.user_id <> %(holder)s
               AND NOT EXISTS (SELECT 1 FROM shared_workspace_members sm
                                WHERE sm.workspace_id = %(ws)s AND sm.user_id = a.user_id)
             ORDER BY a.user_id
            """,
            {"ws": workspace_id, "holder": holder_id},
        )
        return [r[0] for r in await cur.fetchall()]


def former_member_sql(user_col: str, workspace: str, user: Optional[str]) -> str:
    """The predicate that ``user_col`` names someone who lost the workspace
    ``workspace``: the person the placeholder ``user`` binds, or with None
    anyone not listed on it now (people who reached it only through
    everyone)."""
    if user is not None:
        return f"{user_col} = {user}"
    return f"""NOT EXISTS (SELECT 1 FROM shared_workspace_members m
                   WHERE m.workspace_id = {workspace} AND m.user_id = {user_col})"""


async def running_work(
    workspace_id: Optional[str], user_id: Optional[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Turns and subagent runs in progress in a shared workspace's threads,
    each with its workspace and holder: in ``workspace_id``, or with None in
    every shared workspace, whose sender is ``user_id`` or with None anyone
    not listed on it now (for one workspace only)."""
    where = "x.status = 'in_progress' AND " + former_member_sql(
        "x.user_id", "s.workspace_id", "%(user)s" if user_id is not None else None
    )
    if workspace_id is not None:
        where += " AND s.workspace_id = %(ws)s"
    params = {"ws": workspace_id, "user": user_id}
    shared = """
        JOIN conversation_threads t ON t.conversation_thread_id = x.{thread}
        JOIN shared_workspaces s ON s.workspace_id = t.workspace_id
    """
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT x.conversation_response_id, x.conversation_thread_id,
                   s.workspace_id::text AS workspace_id, s.holder_id
              FROM conversation_responses x {shared.format(thread="conversation_thread_id")}
             WHERE {where}
            """,
            params,
        )
        turns = [dict(r) for r in await cur.fetchall()]
        await cur.execute(
            f"""
            SELECT x.thread_id, x.task_id, x.task_run_id,
                   s.workspace_id::text AS workspace_id, s.holder_id
              FROM subagent_runs x {shared.format(thread="thread_id")}
             WHERE {where}
            """,
            params,
        )
        tasks = [dict(r) for r in await cur.fetchall()]
    return turns, tasks


async def workspaces_running_commands_of(user_id: str) -> list[str]:
    """The shared workspaces where ``user_id`` has a background command
    running that nobody has asked to stop."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT DISTINCT s.workspace_id::text
              FROM command_runs c
              JOIN shared_workspaces s ON s.workspace_id = c.workspace_id
             WHERE c.user_id = %s AND c.status = 'running' AND c.stopped_by IS NULL
            """,
            (user_id,),
        )
        return [r[0] for r in await cur.fetchall()]


async def computers_minting_as_former(
    workspace_id: str, holder_id: str, user_id: Optional[str]
) -> list[str]:
    """The shared computer, when its market-data token was last minted as
    ``user_id``, or with ``user_id`` None, as anyone not listed on the
    workspace now. The holder's own token is no one's."""
    who = former_member_sql(
        "t.user_id", "%(ws)s", "%(user)s" if user_id is not None else None
    )
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            f"""
            SELECT t.computer_id::text
              FROM computer_data_tokens t JOIN computers c USING (computer_id)
             WHERE c.user_id = %(holder)s AND t.user_id <> c.user_id AND {who}
            """,
            {"ws": workspace_id, "holder": holder_id, "user": user_id},
        )
        return [r[0] for r in await cur.fetchall()]


async def shared_computers_minting_as(user_id: str) -> list[tuple[str, str, str]]:
    """The shared workspaces (id, holder, computer) whose computer's token
    was last minted as ``user_id``."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            SELECT s.workspace_id::text, s.holder_id, t.computer_id::text
              FROM computer_data_tokens t
              JOIN computers c USING (computer_id)
              JOIN shared_workspaces s ON s.holder_id = c.user_id
             WHERE t.user_id = %(user)s
            """,
            {"user": user_id},
        )
        return [(r[0], r[1], r[2]) for r in await cur.fetchall()]


async def get_live_in_org(workspace_id: str, org_id: str) -> Optional[SharedRow]:
    """The shared workspace, while it is live and ``org_id``'s; None for
    one of another organization, so the console never reaches it."""
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None
    return await _one(
        "JOIN workspaces w ON w.workspace_id = s.workspace_id"
        " WHERE s.workspace_id = %s AND s.org_id = %s AND w.status <> 'deleted'",
        workspace_id, org_id,
    )


async def make_manager(
    workspace_id: str, user_id: str, added_by: str
) -> Optional[Role]:
    """Make ``user_id`` a manager, adding them when not listed. Their role
    before (None when they were not listed)."""
    async with get_db_connection() as conn, conn.cursor(row_factory=tuple_row) as cur:
        await cur.execute(
            """
            WITH old AS (
                SELECT role FROM shared_workspace_members
                 WHERE workspace_id = %(ws)s AND user_id = %(user)s
            )
            INSERT INTO shared_workspace_members (workspace_id, user_id, role, added_by)
            VALUES (%(ws)s, %(user)s, 'manager', %(by)s)
            ON CONFLICT (workspace_id, user_id) DO UPDATE SET role = 'manager'
            RETURNING (SELECT role FROM old)
            """,
            {"ws": workspace_id, "user": user_id, "by": added_by},
        )
        row = await cur.fetchone()
    return row[0] if row else None
