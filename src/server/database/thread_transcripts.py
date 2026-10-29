"""Stored thread transcripts, which the file mount serves.

Each agent of a thread (its own, and each background task's) is stored on its
own: a header row records what its copy was rendered from, and a file row
each of its files, the manifest among them, by its whole path in the thread's
directory: its bytes when they are small, the manifest's always, or there is
no object storage, else the blob behind them. A save replaces one agent's
copy and leaves the others alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from psycopg.errors import ForeignKeyViolation
from psycopg.rows import dict_row

from ptc_agent.core.paths import THREAD_DIR_NAME
from src.server.database.pool import get_db_connection

# The fingerprint of a copy compaction saved live, from the messages in hand
# mid-turn: no render from a checkpoint matches it.
LIVE_FINGERPRINT = ""


@dataclass(frozen=True)
class TaskRun:
    """A task's latest run as a render read it. The copy's header and
    fingerprint come from it, and it moves without a new checkpoint (the run
    finishing, another starting), which the checkpoint order cannot see."""

    task_id: str
    run_id: Any
    status: str | None
    final_checkpoint_id: str | None


@dataclass
class StoredTranscript:
    """One agent's copy. A load fills the header and the manifest alone; a
    save lists every file, the manifest among them, with what it has in hand
    for each."""

    fingerprint: str
    checkpoint_id: str | None
    manifest: str
    # Transcript-dir-relative path -> (sha256, byte_len).
    files: dict[str, tuple[str, int]] = field(default_factory=dict)
    #: Bytes to keep in the row, by path.
    inline: dict[str, bytes] = field(default_factory=dict)
    #: Paths carried over unrendered from the copy this one replaces. Nothing
    #: here holds their bytes, so their rows must already hold this sha.
    carried: set[str] = field(default_factory=set)
    #: A task's render: the run it read, which must still read so to save.
    run: TaskRun | None = None


class StaleCopy(Exception):
    """A save carried a file over that its row no longer matches: the stored
    copy moved after this one was rendered against it. Render it in full."""


class _RunMoved(Exception):
    """A task's run moved after its render read it, so its copy is a later
    export's to save."""


@dataclass(frozen=True)
class StoredFile:
    #: The blob's namespace.
    user_id: str
    sha256: str
    content: bytes | None


async def stored_fingerprints(thread_ids: list[str]) -> dict[str, dict[str, str]]:
    """Each thread's stored agents: fingerprint by prefix."""
    if not thread_ids:
        return {}
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT conversation_thread_id, prefix, fingerprint "
                "FROM thread_transcripts "
                "WHERE conversation_thread_id = ANY(%s::uuid[])",
                (thread_ids,),
            )
            found: dict[str, dict[str, str]] = {}
            for thread_id, prefix, fingerprint in await cur.fetchall():
                found.setdefault(str(thread_id), {})[prefix] = fingerprint
            return found


async def load_stored(
    thread_ids: list[str], prefix: str | None = None
) -> dict[str, dict[str, StoredTranscript]]:
    """Each thread's stored agents by prefix, or only the one ``prefix``
    names: the header and the manifest alone, which is all a render needs of
    what it replaces."""
    from ptc_agent.agent.transcript.store import MANIFEST, TASK_META

    if not thread_ids:
        return {}
    only = "" if prefix is None else " AND s.prefix = %s"
    params = (MANIFEST, TASK_META, thread_ids) + (() if prefix is None else (prefix,))
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT s.conversation_thread_id, s.prefix, s.fingerprint,
                       s.checkpoint_id, f.content
                FROM thread_transcripts s
                LEFT JOIN thread_transcript_files f
                    ON f.conversation_thread_id = s.conversation_thread_id
                   AND f.path = s.prefix || CASE s.prefix WHEN '' THEN %s ELSE %s END
                """
                f"WHERE s.conversation_thread_id = ANY(%s::uuid[]){only}",
                params,
            )
            stored: dict[str, dict[str, StoredTranscript]] = {}
            for thread_id, agent, fingerprint, checkpoint_id, manifest in (
                await cur.fetchall()
            ):
                stored.setdefault(str(thread_id), {})[agent] = StoredTranscript(
                    fingerprint, checkpoint_id, bytes(manifest or b"").decode()
                )
    return stored


async def workspace_checkpoints(workspace_id: str) -> list[tuple[str, str]]:
    """(thread id, checkpoint id) of each of the workspace's threads that has
    finished a turn, newest first: what its transcripts are rendered from."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT conversation_thread_id, latest_checkpoint_id
                FROM conversation_threads
                WHERE workspace_id = %s AND latest_checkpoint_id IS NOT NULL
                ORDER BY updated_at DESC
                """,
                (workspace_id,),
            )
            return [(str(row[0]), row[1]) for row in await cur.fetchall()]


async def workspace_transcripts(workspace_id: str) -> list[str]:
    """Ids of the workspace's threads that have a stored transcript."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT t.conversation_thread_id FROM conversation_threads t
                WHERE t.workspace_id = %s
                  AND EXISTS (
                      SELECT 1 FROM thread_transcripts s
                      WHERE s.conversation_thread_id = t.conversation_thread_id
                  )
                """,
                (workspace_id,),
            )
            return [str(row[0]) for row in await cur.fetchall()]


# The thread whose transcript directory is a short id, and none when two of
# the workspace's threads with a transcript share the prefix: their segments
# are numbered alike, so serving either there would answer a path written for
# the other with the wrong conversation. The prefix is a uuid range, so the
# primary key finds it.
_THREAD_BY_SHORT_ID = """
    WITH thread AS (
        SELECT (array_agg(t.conversation_thread_id))[1] AS id
        FROM conversation_threads t
        WHERE t.workspace_id = %s
          AND t.conversation_thread_id BETWEEN %s::uuid AND %s::uuid
          AND EXISTS (
              SELECT 1 FROM thread_transcripts s
              WHERE s.conversation_thread_id = t.conversation_thread_id
          )
        HAVING count(*) = 1
    )
"""


def _short_id_range(short_id: str) -> tuple[str, str] | None:
    if not THREAD_DIR_NAME.match(short_id):
        return None
    return (
        f"{short_id}-0000-0000-0000-000000000000",
        f"{short_id}-ffff-ffff-ffff-ffffffffffff",
    )


async def list_transcript(
    workspace_id: str, short_id: str
) -> dict[str, tuple[str, int]] | None:
    """Every file of the transcript directory ``short_id`` names, by its path
    in it, with its sha256 and size; None when there is no such directory."""
    bounds = _short_id_range(short_id)
    if bounds is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _THREAD_BY_SHORT_ID
                + """
                SELECT f.path, f.sha256, f.byte_len
                FROM thread_transcript_files f
                JOIN thread ON f.conversation_thread_id = thread.id
                """,
                (workspace_id, *bounds),
            )
            rows = await cur.fetchall()
    return {path: (sha256, int(byte_len)) for path, sha256, byte_len in rows} or None


@dataclass(frozen=True)
class ListedFile:
    thread_id: str
    short_id: str
    path: str
    sha256: str
    byte_len: int
    #: The blob's namespace.
    user_id: str


async def list_transcript_tree(
    workspace_id: str, short_id: str | None, limit: int, *, newest_first: bool = False
) -> list[ListedFile]:
    """The first ``limit`` files of the workspace's transcript directories,
    or of the one ``short_id`` names, in directory order, each directory
    naming its thread as ``_THREAD_BY_SHORT_ID`` picks it. ``newest_first``
    takes the directories by when their thread last changed instead."""
    if short_id is None:
        bounds = ("00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff")
    else:
        bounds = _short_id_range(short_id)
        if bounds is None:
            return []
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                WITH thread AS (
                    SELECT (array_agg(t.conversation_thread_id))[1] AS id,
                        left(t.conversation_thread_id::text, 8) AS short_id,
                        max(t.updated_at) AS updated_at
                    FROM conversation_threads t
                    WHERE t.workspace_id = %s
                      AND t.conversation_thread_id BETWEEN %s::uuid AND %s::uuid
                      AND EXISTS (
                          SELECT 1 FROM thread_transcripts s
                          WHERE s.conversation_thread_id = t.conversation_thread_id
                      )
                    GROUP BY 2
                    HAVING count(*) = 1
                )
                SELECT thread.id, thread.short_id, f.path, f.sha256, f.byte_len, f.user_id
                FROM thread_transcript_files f
                JOIN thread ON f.conversation_thread_id = thread.id
                ORDER BY CASE WHEN %s THEN thread.updated_at END DESC NULLS LAST, 2, 3
                LIMIT %s
                """,
                (workspace_id, *bounds, newest_first, limit),
            )
            rows = await cur.fetchall()
    return [
        ListedFile(str(thread_id), short, path, sha256, int(byte_len), user_id)
        for thread_id, short, path, sha256, byte_len, user_id in rows
    ]


async def load_transcript_contents(files: list[ListedFile]) -> dict[ListedFile, bytes]:
    """The bytes of each file its row holds, in one round trip. A file whose
    row names other bytes now, saved since it was listed, is left out."""
    if not files:
        return {}
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT w.n, f.content, f.sha256
                FROM unnest(%s::uuid[], %s::text[]) WITH ORDINALITY AS w(id, path, n)
                JOIN thread_transcript_files f
                    ON f.conversation_thread_id = w.id AND f.path = w.path
                """,
                ([f.thread_id for f in files], [f.path for f in files]),
            )
            rows = await cur.fetchall()
    return {
        files[n - 1]: bytes(content)
        for n, content, sha256 in rows
        if content is not None and sha256 == files[n - 1].sha256
    }


async def load_transcript_file(
    workspace_id: str, short_id: str, path: str
) -> StoredFile | None:
    """One file of the transcript directory ``short_id`` names, in one round
    trip."""
    bounds = _short_id_range(short_id)
    if bounds is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                _THREAD_BY_SHORT_ID
                + """
                SELECT f.user_id, f.sha256, f.content FROM thread_transcript_files f
                JOIN thread ON f.conversation_thread_id = thread.id
                WHERE f.path = %s
                """,
                (workspace_id, *bounds, path),
            )
            row = await cur.fetchone()
    if row is None:
        return None
    return StoredFile(row[0], row[1], bytes(row[2]) if row[2] is not None else None)


def _changed_rows(
    thread_id: str,
    prefix: str,
    user_id: str,
    transcript: StoredTranscript,
    held: dict[str, tuple[str, str, bool]],
) -> list[tuple]:
    """The file rows a save has to write: new, moved to another sha, or
    gaining bytes a blob-backed row lacked. ``held`` is what the rows hold,
    (sha256, user_id, has bytes) by path."""
    rows = []
    for path, (sha256, byte_len) in transcript.files.items():
        data = transcript.inline.get(path)
        row = held.get(path)
        if row is not None and row[:2] == (sha256, user_id) and (row[2] or data is None):
            continue
        if path in transcript.carried:
            raise StaleCopy(path)
        rows.append((thread_id, prefix, path, user_id, sha256, byte_len, data))
    return rows


async def save_stored(
    thread_id: str, prefix: str, user_id: str, transcript: StoredTranscript
) -> bool:
    """Replace one agent's stored copy unless a newer render already landed.

    Each copy's checkpoint is its own agent's (the thread's, or the task's
    namespace), and checkpoint ids sort by time, so a save racing a later one
    (another worker's turn end) loses on the header row's lock and leaves the
    newer copy alone. At the same checkpoint a copy saved live holds the
    messages past it, so only another live save replaces it, and a task's
    render lands only while its run reads as the render read it: exports of
    a thread may overlap, and one that read a task running would otherwise
    land over the finished copy at the checkpoint they share. Only the file
    rows that differ are written, read under that lock.
    A file row without bytes names a blob, which must be registered.
    Returns whether this save landed; raises ``StaleCopy`` when a file it
    carried over no longer matches its row.
    """
    try:
        async with get_db_connection() as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        """
                        INSERT INTO thread_transcripts AS s
                            (conversation_thread_id, prefix, fingerprint,
                             checkpoint_id, stored_at)
                        VALUES (%s, %s, %s, %s, clock_timestamp())
                        ON CONFLICT (conversation_thread_id, prefix) DO UPDATE
                            SET fingerprint = EXCLUDED.fingerprint,
                                checkpoint_id = EXCLUDED.checkpoint_id,
                                stored_at = clock_timestamp()
                            WHERE s.checkpoint_id IS NULL
                               OR EXCLUDED.checkpoint_id IS NULL
                               OR s.checkpoint_id < EXCLUDED.checkpoint_id
                               OR (s.checkpoint_id = EXCLUDED.checkpoint_id
                                   AND NOT (s.fingerprint = %s
                                            AND EXCLUDED.fingerprint <> %s))
                        RETURNING 1
                        """,
                        (
                            thread_id,
                            prefix,
                            transcript.fingerprint,
                            transcript.checkpoint_id,
                            LIVE_FINGERPRINT,
                            LIVE_FINGERPRINT,
                        ),
                    )
                    if await cur.fetchone() is None:
                        return False
                    run = transcript.run
                    if run is not None:
                        # Read after the header row's lock: a save it waited
                        # on read the run before committing, so this one sees
                        # the run at least as far along as that one did.
                        await cur.execute(
                            """
                            SELECT 1 FROM subagent_tasks t
                            LEFT JOIN subagent_runs r ON r.task_run_id = t.latest_run_id
                            WHERE t.thread_id = %s AND t.task_id = %s
                              AND r.task_run_id IS NOT DISTINCT FROM %s
                              AND r.status IS NOT DISTINCT FROM %s
                              AND r.final_checkpoint_id IS NOT DISTINCT FROM %s
                            """,
                            (
                                thread_id,
                                run.task_id,
                                run.run_id,
                                run.status,
                                run.final_checkpoint_id,
                            ),
                        )
                        if await cur.fetchone() is None:
                            raise _RunMoved
                    # Every save of this agent takes the header row first, so
                    # the rows read here are the ones this save replaces.
                    await cur.execute(
                        "SELECT path, sha256, user_id, content IS NOT NULL AS has_bytes "
                        "FROM thread_transcript_files "
                        "WHERE conversation_thread_id = %s AND prefix = %s",
                        (thread_id, prefix),
                    )
                    held = {
                        r["path"]: (r["sha256"], r["user_id"], r["has_bytes"])
                        for r in await cur.fetchall()
                    }
                    rows = _changed_rows(thread_id, prefix, user_id, transcript, held)
                    gone = [path for path in held if path not in transcript.files]
                    if gone:
                        await cur.execute(
                            "DELETE FROM thread_transcript_files "
                            "WHERE conversation_thread_id = %s AND path = ANY(%s)",
                            (thread_id, gone),
                        )
                    if rows:
                        await cur.executemany(
                            """
                            INSERT INTO thread_transcript_files AS f
                                (conversation_thread_id, prefix, path, user_id,
                                 sha256, byte_len, content)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (conversation_thread_id, path) DO UPDATE
                                SET user_id = EXCLUDED.user_id,
                                    sha256 = EXCLUDED.sha256,
                                    byte_len = EXCLUDED.byte_len,
                                    content = EXCLUDED.content
                            """,
                            rows,
                        )
    except (ForeignKeyViolation, _RunMoved):
        # The thread was deleted while its render was in flight, or the task's
        # run moved; either way the transaction rolled the header back.
        return False
    return True


async def drop_rewound(thread_id: str, checkpoint_id: str, *, conn) -> None:
    """Drop the thread's own copy when it shows turns past ``checkpoint_id``,
    where a fork pins the thread back: rendered past it, or saved live at it,
    which holds the messages after it. Either shows the turns the fork
    discards, and no save at the older checkpoint may replace it. ``conn`` is
    the fork's transaction, so the copy goes with the turns or not at all."""
    async with conn.cursor() as cur:
        await cur.execute(
            "DELETE FROM thread_transcripts "
            "WHERE conversation_thread_id = %s AND prefix = '' "
            "AND (checkpoint_id > %s OR (checkpoint_id = %s AND fingerprint = %s))",
            (thread_id, checkpoint_id, checkpoint_id, LIVE_FINGERPRINT),
        )


async def drop_own_past(thread_id: str, checkpoint_id: str) -> bool:
    """Drop the thread's own copy when it reads as newer than
    ``checkpoint_id``, while that is still the thread's pointer and no run is
    open on it, and return whether one was dropped.

    Every writer labels a copy at a pointer it read or at the copy it
    replaced, and only a fork moves the pointer back, so a copy past an idle
    pointer shows turns a fork replaced: an export that read them can land
    after the fork's START dropped the copy, and no render from the older
    pointer replaces it. One with no label goes too, since the empty fork root
    renders nothing over it. A run open may have saved one live, and a
    pointer moved since it was read means a turn landed past it.

    The thread row's lock keeps a run from being admitted between the check
    and the delete, since every START updates that row before its run row
    commits. A single statement reads the run rows once, at its start, and
    can then wait on the copy's row while a run is admitted and saves over it
    live.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                """
                SELECT 1 FROM conversation_threads
                WHERE conversation_thread_id = %s AND latest_checkpoint_id = %s
                FOR NO KEY UPDATE
                """,
                (thread_id, checkpoint_id),
            )
            if await cur.fetchone() is None:
                return False
            cur = await conn.execute(
                """
                DELETE FROM thread_transcripts s
                WHERE s.conversation_thread_id = %s AND s.prefix = ''
                  AND (s.checkpoint_id IS NULL OR s.checkpoint_id > %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM conversation_responses r
                      WHERE r.conversation_thread_id = s.conversation_thread_id
                        AND r.status = 'in_progress'
                  )
                """,
                (thread_id, checkpoint_id),
            )
            return cur.rowcount > 0


async def delete_stored(thread_id: str, prefixes: set[str]) -> None:
    """Drop these agents' copies, files and all."""
    async with get_db_connection() as conn:
        await conn.execute(
            "DELETE FROM thread_transcripts "
            "WHERE conversation_thread_id = %s AND prefix = ANY(%s)",
            (thread_id, list(prefixes)),
        )
