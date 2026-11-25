"""Where a record's comments go: its binding, and the default thread.

A file's may go to a thread in its workspace or the owner's Home. A chart's
only to its own workspace's, since a chart id names a chart in that workspace:
a workspace's chart never goes to Home, and Home's own chart goes to Home's
threads, Home being that chart's workspace."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from psycopg.rows import dict_row

from src.server.database.artifacts.records import (
    ARTIFACT_COLUMNS,
    COMMENT_RECORDS,
    OWNED,
    Artifact,
    ArtifactNotFound,
    artifact_from_row,
    lock_owned,
    set_record,
)
from src.server.database.conversation.thread_reach import visible_sql
from src.server.database.home_workspace import get_home_workspace_id
from src.server.database.pool import get_db_connection

#: Which rule picked a comment's default thread.
DestinationRule = Literal["bound", "beside", "creator", "new"]
#: The owner's ``other_preference.artifact_comment_destination``: which thread
#: an unbound file's comments go to.
CommentDestination = Literal["beside", "creator", "new"]
_FALLS_THROUGH: dict[CommentDestination, tuple[DestinationRule, ...]] = {
    "beside": ("beside", "creator"),
    "creator": ("creator", "beside"),
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


def destination_sql(record: str, thread: str) -> str:
    """Whether comments on the record ``record`` may go to the thread
    ``thread``, as SQL over those aliases with ``%(home_id)s`` the owner's
    Home: not archived, and in the record's workspace or, for a file, Home.
    The caller joins the thread's workspace to the owner."""
    return (
        f"({thread}.archived_at IS NULL"
        f" AND ({thread}.workspace_id = {record}.workspace_id"
        f" OR ({record}.kind = 'file' AND {thread}.workspace_id = %(home_id)s)))"
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
           AND {destination_sql('l', 't')}
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
    #: Whether comments may go there now: not archived, in the artifact's
    #: workspace or, for a file, the owner's Home.
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

    @property
    def default(self) -> tuple[DestinationThread | None, DestinationRule]:
        """The thread a comment goes to unless the owner picks another: the
        bound thread, else the beside or creator thread in the order the
        owner's preference names, else a new thread in the workspace (None).
        A binding comes first whatever the preference, being the owner's pick
        for this one file. One that cannot take comments now, archived or
        moved out of reach, falls through to the next."""
        for rule in ("bound", *_FALLS_THROUGH[self.preference]):
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
            ids = [
                i
                for i in (
                    artifact.bound_thread_id,
                    beside_thread_id,
                    artifact.created_thread_id,
                )
                if i
            ]
            threads: dict[str, DestinationThread] = {}
            if ids:
                await cur.execute(
                    f"""
                    SELECT t.conversation_thread_id::text AS thread_id, t.title,
                           t.workspace_id::text,
                           {destination_sql('l', 't')} AS usable,
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
    )
