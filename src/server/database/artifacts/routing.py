"""Where a record's comments go: its binding, and the default thread.

A file's may go to a thread in its workspace or the owner's Home. A chart's
only to its own workspace's, since a chart id names a chart in that workspace:
a workspace's chart never goes to Home, and Home's own chart goes to Home's
threads, Home being that chart's workspace. A Drive file's to any of the
owner's own workspaces or Home, since the Drive is at the same path in each
of them; never to a shared one, whose other members would read it, nor to
Home's stand-in, which is no workspace of its own."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from psycopg.rows import dict_row

from src.server.database.artifacts.records import (
    ARTIFACT_COLUMNS,
    COMMENT_RECORDS,
    OWNED,
    Artifact,
    ArtifactNotFound,
    RecordKind,
    artifact_from_row,
    lock_owned,
    set_record,
)
from src.server.database.conversation.thread_reach import (
    personal_workspace_sql,
    visible_sql,
)
from src.server.database.home_workspace import excluding_stand_in, get_home_workspace_id
from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import like_escape, normalize_uuid

#: Which rule picked a comment's default thread: ``worked`` for a Drive file's.
DestinationRule = Literal["bound", "beside", "creator", "worked", "new"]
#: The owner's ``other_preference.artifact_comment_destination``: which thread
#: an unbound file's comments go to.
CommentDestination = Literal["beside", "creator", "new"]
_FALLS_THROUGH: dict[CommentDestination, tuple[DestinationRule, ...]] = {
    "beside": ("beside", "creator"),
    "creator": ("creator", "beside"),
    "new": (),
}
#: A Drive file's, which has no creator: the thread that last worked on it
#: stands where a file's creator does. A thread is beside it only in a chat's
#: Drive tab, never on the Drive page.
_DRIVE_FALLS_THROUGH: dict[CommentDestination, tuple[DestinationRule, ...]] = {
    "beside": ("beside", "worked"),
    "creator": ("worked", "beside"),
    "new": (),
}


class DestinationUnavailable(LookupError):
    """The thread is not one this artifact's comments may go to."""


def _held(user_id: str, caller: str) -> str:
    """For a member writing in a shared workspace (``user_id`` its holder),
    the thread row held shared to the end of the transaction, as a turn's
    START holds it: making the thread private then either sees the words
    this queues or is seen by this read. No rule hides a thread from its
    workspace's owner, so their write takes no lock."""
    return " FOR SHARE OF t" if caller != user_id else ""


async def require_reached(cur, *, user_id: str, thread_id: str, caller: str) -> None:
    """Raise unless ``caller`` may see a thread a comment of ``user_id``'s
    already waits for. A thread gone since lets it wait as before."""
    if caller == user_id:
        return
    await cur.execute(
        f"SELECT {visible_sql('t', '%(user_id)s', '%(caller)s')} AS reached"
        f" FROM conversation_threads t WHERE t.conversation_thread_id = %(thread_id)s"
        f"{_held(user_id, caller)}",
        {"thread_id": thread_id, "user_id": user_id, "caller": caller},
    )
    row = await cur.fetchone()
    if row is not None and not row["reached"]:
        raise DestinationUnavailable(thread_id)


#: The kinds whose comments may go to the owner's Home as well as their own
#: workspace: a file's, since Home's Chief of Staff reaches every workspace's
#: files, and a Drive file's, the Drive being at one path in each workspace.
HOME_KINDS: frozenset[RecordKind] = frozenset({"file", "drive"})
_HOME_KINDS_SQL = ", ".join(f"'{kind}'" for kind in sorted(HOME_KINDS))


def destination_sql(record: str, thread: str, workspace: str) -> str:
    """Whether comments on the record ``record`` may go to the thread
    ``thread``, whose workspace row is ``workspace``, as SQL over those
    aliases with ``%(home_id)s`` the owner's Home: not archived, and in the
    record's workspace, or Home for ``HOME_KINDS``, or for a Drive file any
    personal workspace but Home's stand-in, which is no workspace of its own.
    A shared one is left out even when the owner holds it: its other members
    would read the comment. The caller joins ``workspace`` to the owner."""
    return (
        f"({thread}.archived_at IS NULL"
        f" AND ({thread}.workspace_id = {record}.workspace_id"
        f" OR ({record}.kind IN ({_HOME_KINDS_SQL})"
        f" AND {thread}.workspace_id = %(home_id)s)"
        f" OR ({record}.kind = 'drive' AND {excluding_stand_in(workspace)}"
        f" AND {personal_workspace_sql(f'{workspace}.workspace_id')})))"
    )


async def require_destination(
    cur, *, user_id: str, artifact_id: str, thread_id: str, caller: str
) -> None:
    """Raise unless comments on the record may go to the thread, one
    ``caller`` may see."""
    await cur.execute(
        f"""
        SELECT 1
          FROM conversation_threads t
          JOIN workspaces tw ON tw.workspace_id = t.workspace_id
          JOIN share_links l ON l.artifact_id = %(artifact_id)s
         WHERE t.conversation_thread_id = %(thread_id)s
           AND tw.user_id = %(user_id)s AND tw.status <> 'deleted'
           AND {destination_sql('l', 't', 'tw')}
           AND {visible_sql('t', 'tw.user_id', '%(caller)s')}
        {_held(user_id, caller)}
        """,
        {
            "thread_id": thread_id,
            "user_id": user_id,
            "artifact_id": artifact_id,
            "home_id": get_home_workspace_id(user_id),
            "caller": caller,
        },
    )
    if await cur.fetchone() is None:
        raise DestinationUnavailable(thread_id)


async def bind(*, user_id: str, artifact_id: str, thread_id: str, caller: str) -> Artifact:
    """Send the file's comments to ``thread_id`` until unbound.

    ``DestinationUnavailable`` when the thread is not one they may go to, or
    one ``caller`` may not see.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await lock_owned(
                    cur,
                    user_id=user_id,
                    artifact_id=artifact_id,
                    kinds=COMMENT_RECORDS,
                )
                await require_destination(
                    cur,
                    user_id=user_id,
                    artifact_id=artifact_id,
                    thread_id=thread_id,
                    caller=caller,
                )
                await cur.execute(
                    f"""
                    UPDATE share_links l
                       SET bound_thread_id = %s
                     WHERE l.artifact_id = %s
                    RETURNING {ARTIFACT_COLUMNS}
                    """,
                    (thread_id, artifact_id),
                )
                return artifact_from_row(await cur.fetchone())


async def unbind(*, user_id: str, artifact_id: str) -> Artifact:
    return await set_record(
        user_id=user_id,
        artifact_id=artifact_id,
        assignments="bound_thread_id = NULL",
        params={},
        kinds=COMMENT_RECORDS,
    )


@dataclass(frozen=True)
class DestinationThread:
    thread_id: str
    title: str | None
    workspace_id: str
    #: Whether comments may go there now (``destination_sql``).
    usable: bool
    #: A Chief of Staff thread, in the owner's Home.
    home: bool


@dataclass(frozen=True)
class Destinations:
    artifact: Artifact
    bound: DestinationThread | None
    beside: DestinationThread | None
    creator: DestinationThread | None
    preference: CommentDestination = "beside"
    #: A Drive file's: the usable thread that last worked on it (``worked``).
    worked: DestinationThread | None = None

    @property
    def default(self) -> tuple[DestinationThread | None, DestinationRule]:
        """The thread a comment goes to unless the owner picks another: the
        bound thread, else the others in the order the owner's preference
        names, else a new thread in the workspace (None). A binding comes
        first whatever the preference, being the owner's pick for this one
        file. One that cannot take comments now, archived or moved out of
        reach, falls through to the next."""
        order = _DRIVE_FALLS_THROUGH if self.artifact.kind == "drive" else _FALLS_THROUGH
        for rule in ("bound", *order[self.preference]):
            thread: DestinationThread | None = getattr(self, rule)
            if thread is not None and thread.usable:
                return thread, rule
        return None, "new"


def _comment_destination(value: object) -> CommentDestination:
    """A stored preference no writer should have left reads as the default."""
    return next((known for known in _FALLS_THROUGH if value == known), "beside")


async def destinations(
    *,
    user_id: str,
    artifact_id: str,
    beside_thread_id: str | None,
    caller: str,
) -> Destinations:
    """The threads a comment on the artifact could go to by default.

    A thread that is not the owner's, or is gone, or one ``caller`` may not
    see, comes back as None, so a ``beside_thread_id`` from the client names
    nothing it may not see.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {ARTIFACT_COLUMNS},
                       (SELECT p.other_preference ->> 'artifact_comment_destination'
                          FROM user_preferences p
                         WHERE p.user_id = %(user_id)s) AS comment_destination
                  FROM {OWNED}
                 WHERE l.artifact_id = %(artifact_id)s AND {COMMENT_RECORDS}
                """,
                {"artifact_id": artifact_id, "user_id": user_id},
            )
            row = await cur.fetchone()
            artifact = artifact_from_row(row)
            if artifact is None:
                raise ArtifactNotFound(artifact_id)
            preference = _comment_destination(row["comment_destination"])
            worked = (
                await _worked_candidates(cur, artifact) if artifact.kind == "drive" else []
            )
            ids = [
                i
                for i in (
                    artifact.bound_thread_id,
                    beside_thread_id,
                    artifact.created_thread_id,
                    *worked,
                )
                if i
            ]
            threads: dict[str, DestinationThread] = {}
            if ids:
                await cur.execute(
                    f"""
                    SELECT t.conversation_thread_id::text AS thread_id, t.title,
                           t.workspace_id::text,
                           {destination_sql('l', 't', 'tw')} AS usable,
                           (t.workspace_id = %(home_id)s) AS home
                      FROM conversation_threads t
                      JOIN workspaces tw
                        ON tw.workspace_id = t.workspace_id
                       AND tw.user_id = %(user_id)s AND tw.status <> 'deleted'
                      JOIN share_links l ON l.artifact_id = %(artifact_id)s
                     WHERE t.conversation_thread_id = ANY(%(ids)s::uuid[])
                       AND {visible_sql('t', 'tw.user_id', '%(caller)s')}
                    """,
                    {
                        "caller": caller,
                        "ids": ids,
                        "user_id": user_id,
                        "artifact_id": artifact_id,
                        "home_id": get_home_workspace_id(user_id),
                    },
                )
                for row in await cur.fetchall():
                    threads[row["thread_id"]] = DestinationThread(**row)
    return Destinations(
        artifact=artifact,
        bound=threads.get(artifact.bound_thread_id or ""),
        beside=threads.get(beside_thread_id or ""),
        creator=threads.get(artifact.created_thread_id or ""),
        preference=preference,
        worked=next(
            (
                threads[i]
                for i in worked
                if i in threads and threads[i].usable
            ),
            None,
        ),
    )


async def _worked_candidates(cur, artifact: Artifact) -> list[str]:
    """The threads that last worked on a Drive file, most recent first: the
    one its latest comment waits for or went to, and the one its agent last
    saved it from. A version's thread is free text, so only an id counts.

    That thread is a label a process on the computer files, not an
    authority: it only picks a default among the owner's own threads, which
    the owner sees and can change before sending, and ``destination_sql``
    checks whatever thread a send names. Read it for nothing more."""
    await cur.execute(
        """
        SELECT x.thread_id FROM (
            (SELECT c.thread_id::text AS thread_id, c.updated_at AS at
               FROM artifact_comments c
              WHERE c.artifact_id = %(artifact_id)s AND c.thread_id IS NOT NULL
                AND c.delivery IN ('queued', 'sent')
              ORDER BY c.updated_at DESC
              LIMIT 1)
            UNION ALL
            (SELECT v.thread_id, v.created_at
               FROM drive_versions v
              WHERE v.entry_id = %(entry_id)s::uuid AND v.actor_kind = 'agent'
                AND v.thread_id IS NOT NULL
              ORDER BY v.seq DESC
              LIMIT 1)
        ) x
        ORDER BY x.at DESC
        """,
        {"artifact_id": artifact.artifact_id, "entry_id": artifact.path},
    )
    found = (normalize_uuid(r["thread_id"]) for r in await cur.fetchall())
    return list(dict.fromkeys(i for i in found if i))


@dataclass(frozen=True)
class PickableThread:
    thread_id: str
    title: str | None
    workspace_id: str
    workspace_name: str
    #: A Chief of Staff thread, in the owner's Home.
    home: bool
    updated_at: datetime


async def destination_threads(
    *,
    user_id: str,
    artifact_id: str,
    caller: str,
    query: str | None,
    limit: int,
    thread_id: str | None = None,
) -> list[PickableThread]:
    """The threads comments on the record may go to now (``destination_sql``)
    that ``caller`` may see, most recently active first; with ``query``, only
    those whose title holds it, in any case; with ``thread_id``, only that
    one."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT t.conversation_thread_id::text AS thread_id, t.title,
                       t.workspace_id::text, tw.name AS workspace_name,
                       (t.workspace_id = %(home_id)s) AS home, t.updated_at
                  FROM {OWNED}
                  JOIN workspaces tw
                    ON tw.user_id = %(user_id)s AND tw.status <> 'deleted'
                  JOIN conversation_threads t ON t.workspace_id = tw.workspace_id
                 WHERE l.artifact_id = %(artifact_id)s AND {COMMENT_RECORDS}
                   AND {destination_sql('l', 't', 'tw')}
                   AND {visible_sql('t', 'tw.user_id', '%(caller)s')}
                   AND (%(pattern)s::text IS NULL
                        OR t.title ILIKE %(pattern)s::text ESCAPE '\\')
                   AND (%(thread_id)s::uuid IS NULL
                        OR t.conversation_thread_id = %(thread_id)s::uuid)
                 ORDER BY t.updated_at DESC, t.conversation_thread_id
                 LIMIT %(limit)s
                """,
                {
                    "user_id": user_id,
                    "artifact_id": artifact_id,
                    "caller": caller,
                    "home_id": get_home_workspace_id(user_id),
                    "pattern": f"%{like_escape(query)}%" if query else None,
                    "limit": limit,
                    "thread_id": thread_id,
                },
            )
            return [PickableThread(**row) for row in await cur.fetchall()]
