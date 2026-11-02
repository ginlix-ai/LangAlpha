"""Where a file's comments go: its binding, and the default thread."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from psycopg.rows import dict_row

from src.server.database.artifacts.records import (
    ARTIFACT_COLUMNS,
    OWNED,
    Artifact,
    ArtifactNotFound,
    artifact_from_row,
    lock_owned,
    set_record,
)
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


async def require_destination(
    cur, *, user_id: str, workspace_id: str, thread_id: str
) -> None:
    """Raise unless comments on a file in ``workspace_id`` may go to the thread."""
    await cur.execute(
        """
        SELECT 1
          FROM conversation_threads t
          JOIN workspaces tw ON tw.workspace_id = t.workspace_id
         WHERE t.conversation_thread_id = %(thread_id)s
           AND t.archived_at IS NULL
           AND tw.user_id = %(user_id)s AND tw.status <> 'deleted'
           AND t.workspace_id IN (%(workspace_id)s, %(home_id)s)
        """,
        {
            "thread_id": thread_id,
            "user_id": user_id,
            "workspace_id": workspace_id,
            "home_id": get_home_workspace_id(user_id),
        },
    )
    if await cur.fetchone() is None:
        raise DestinationUnavailable(thread_id)


async def bind(*, user_id: str, artifact_id: str, thread_id: str) -> Artifact:
    """Send the file's comments to ``thread_id`` until unbound.

    ``DestinationUnavailable`` when the thread is not one they may go to.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                artifact = await lock_owned(
                    cur, user_id=user_id, artifact_id=artifact_id
                )
                await require_destination(
                    cur,
                    user_id=user_id,
                    workspace_id=artifact.workspace_id,
                    thread_id=thread_id,
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
    )


@dataclass(frozen=True)
class DestinationThread:
    thread_id: str
    title: str | None
    workspace_id: str
    #: Whether comments may go there now: not archived, in the artifact's
    #: workspace or the owner's Home.
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
    *, user_id: str, artifact_id: str, beside_thread_id: str | None = None
) -> Destinations:
    """The threads a comment on the artifact could go to by default.

    A thread that is not the owner's, or is gone, comes back as None, so a
    ``beside_thread_id`` from the client names nothing it may not see.
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
                 WHERE l.artifact_id = %(artifact_id)s AND l.kind = 'file'
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
                    """
                    SELECT t.conversation_thread_id::text AS thread_id, t.title,
                           t.workspace_id::text,
                           (t.archived_at IS NULL
                            AND t.workspace_id IN (%(workspace_id)s, %(home_id)s))
                               AS usable,
                           (t.workspace_id = %(home_id)s) AS home
                      FROM conversation_threads t
                      JOIN workspaces tw
                        ON tw.workspace_id = t.workspace_id
                       AND tw.user_id = %(user_id)s AND tw.status <> 'deleted'
                     WHERE t.conversation_thread_id = ANY(%(ids)s::uuid[])
                    """,
                    {
                        "ids": ids,
                        "user_id": user_id,
                        "workspace_id": artifact.workspace_id,
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
