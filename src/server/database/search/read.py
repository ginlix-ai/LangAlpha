"""The palette's reads: the user's threads by their shown title, turns by
what was said in them, backed-up files by path, and a turn's texts for the
preview.

Ownership is never stored: a reader joins the thread's workspace for its
user, and leaves out archived threads and deleted workspaces as the thread
listings do. Each search pages by a keyset, and each row carries its ``key``,
which the next page starts after.

A search reads the user's own rows and matches the terms over them, its cost
bounded by what that user holds. The turn and file searches put those rows
behind ``OFFSET 0``, which no condition is pushed through: the planner
otherwise guesses an ``ILIKE`` rare and drives the plan from every user's
matches (measured at 25 to 700 ms against a few on a 200-user copy, flipping
between users and terms). ``MATERIALIZED`` alone does not hold it. No
trigram index could serve a match from behind the fence, nor the thread
search's title, which is a thread's title or its first message, so none is
kept for search.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg.rows import class_row

from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.database.search.sql import Keyset, Terms
from src.server.utils.pg_sanitize import normalize_uuid

# A search reads the user's rows only, but a pathological term set can still
# scan many of them; it is a keystroke-driven read, so it gives up rather
# than holding a pool slot.
_SEARCH_TIMEOUT = "5s"

# The name of the automation that started a thread, read off its row while it
# exists and from the name the thread was stamped with after.
_AUTOMATION_NAME = """
    CASE WHEN t.metadata->'origin'->>'type' = 'automation' THEN COALESCE(
        (SELECT a.name FROM automations a
         WHERE a.user_id = w.user_id AND a.automation_id = CASE
             WHEN t.metadata->'origin'->>'id'
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN (t.metadata->'origin'->>'id')::uuid END),
        NULLIF(t.metadata->'origin'->>'name', '')
    ) END"""


def _owned(
    params: dict[str, Any], user_id: str, workspace_id: str | None, *, thread: bool = True
) -> str:
    """The user's live rows of ``w`` (and its thread ``t``, unless the read
    has none), inside ``workspace_id`` when one is given."""
    params["user_id"] = user_id
    sql = "w.user_id = %(user_id)s AND w.status <> 'deleted'"
    if thread:
        sql += " AND t.archived_at IS NULL"
    if workspace_id is not None:
        params["workspace_id"] = workspace_id
        sql += " AND w.workspace_id = %(workspace_id)s::uuid"
    return sql


async def _fetch[R](row: type[R], sql: str, params: dict[str, Any]) -> list[R]:
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=class_row(row)) as cur:
                await cur.execute(f"SET LOCAL statement_timeout = '{_SEARCH_TIMEOUT}'")
                await cur.execute(sql, params)
                return await cur.fetchall()


# Recency of a thread, never NULL, so it can key a page.
_THREAD_RECENCY = "COALESCE(t.updated_at, t.created_at, 'epoch'::timestamptz)"
_THREAD_PAGE = Keyset(((_THREAD_RECENCY, datetime), ("t.conversation_thread_id", UUID)))


@dataclass(frozen=True)
class ThreadMatch:
    thread_id: str
    workspace_id: str
    workspace_name: str
    #: The thread's title, or its first message when it has none.
    title: str
    automation_name: str | None
    updated_at: datetime

    @property
    def key(self) -> tuple[datetime, str]:
        return (self.updated_at, self.thread_id)


async def search_threads(
    user_id: str,
    terms: Sequence[str],
    *,
    limit: int,
    workspace_id: str | None = None,
    after: Sequence[Any] | None = None,
) -> list[ThreadMatch]:
    """The user's threads whose shown title holds every term, most recently
    updated first. ``BadKey`` for an ``after`` that is no thread's key."""
    match = Terms(terms)
    params: dict[str, Any] = {**match.params, "limit": limit}
    owned = _owned(params, user_id, workspace_id)
    keyset = _THREAD_PAGE.after(after, params)
    # ``TITLE_OR_FIRST_QUERY``, its first message read once, for the match
    # and the title shown alike.
    return await _fetch(
        ThreadMatch,
        f"""
        SELECT t.conversation_thread_id::text AS thread_id,
               t.workspace_id::text AS workspace_id, w.name AS workspace_name,
               COALESCE(NULLIF(t.title, ''), first_query.content, '') AS title,
               {_AUTOMATION_NAME} AS automation_name,
               {_THREAD_RECENCY} AS updated_at
        FROM conversation_threads t
        JOIN workspaces w ON w.workspace_id = t.workspace_id
        LEFT JOIN LATERAL ({_sql.first_query(untitled_only=True)}) first_query ON TRUE
        WHERE {owned}
          AND {match.on("COALESCE(NULLIF(t.title, ''), first_query.content)")}
          {keyset}
        ORDER BY {_THREAD_PAGE.order_by}
        LIMIT %(limit)s
        """,
        params,
    )


_TURN_PAGE = Keyset(
    (("m.turn_at", datetime), ("m.conversation_thread_id", UUID), ("m.turn_index", int))
)


@dataclass(frozen=True)
class TurnMatch:
    thread_id: str
    thread_title: str
    workspace_id: str
    workspace_name: str
    turn_index: int
    turn_at: datetime
    #: Whether the user's message held every term; else the answer did.
    in_user: bool
    #: That side's text.
    text: str

    @property
    def key(self) -> tuple[datetime, str, int]:
        return (self.turn_at, self.thread_id, self.turn_index)


async def search_turns(
    user_id: str,
    terms: Sequence[str],
    *,
    limit: int,
    per_thread: int | None,
    workspace_id: str | None = None,
    thread_id: str | None = None,
    after: Sequence[Any] | None = None,
) -> list[TurnMatch]:
    """The user's turns whose message or answer holds every term, newest
    first, at most ``per_thread`` from any thread (the same on every page)
    unless that is None. ``BadKey`` for an ``after`` that is no turn's key.

    Which of a thread's matches a cap keeps: those holding the query's words
    as typed first, then those whose user side matched, then the newest.
    Otherwise a long thread's later answers, which hold every word somewhere,
    push out the turn the reader remembers asking.

    A side matches when it holds every term, the user's message preferred,
    and only its text is returned. The matches are found among keys alone,
    so the texts are read for the page's rows only.
    """
    match = Terms(terms)
    params: dict[str, Any] = {**match.params, "limit": limit}
    owned = _owned(params, user_id, workspace_id)
    if thread_id is not None:
        params["thread_id"] = thread_id
        owned += " AND t.conversation_thread_id = %(thread_id)s::uuid"
    in_user = match.on("s.user_text")
    in_answer = match.on("s.answer_text")
    rank, capped = "", "TRUE"
    if per_thread is not None:
        params.update(per_thread=per_thread, phrase=match.phrase)
        rank = f""",
            row_number() OVER (
                PARTITION BY s.conversation_thread_id
                ORDER BY
                    (%(phrase)s::text IS NOT NULL
                     AND (s.user_text ILIKE %(phrase)s ESCAPE '\\'
                          OR s.answer_text ILIKE %(phrase)s ESCAPE '\\')) DESC,
                    ({in_user}) DESC,
                    s.turn_at DESC, s.turn_index DESC
            ) AS rank_in_thread"""
        capped = "m.rank_in_thread <= %(per_thread)s"
    keyset = _TURN_PAGE.after(after, params)
    return await _fetch(
        TurnMatch,
        f"""
        WITH owned AS MATERIALIZED (
            SELECT t.conversation_thread_id
            FROM conversation_threads t
            JOIN workspaces w ON w.workspace_id = t.workspace_id
            WHERE {owned}
        ), matched AS (
            SELECT s.conversation_thread_id, s.turn_index, s.turn_at,
                   ({in_user}) AS in_user{rank}
            FROM (
                SELECT s.* FROM owned o
                JOIN thread_search_turns s
                  ON s.conversation_thread_id = o.conversation_thread_id
                OFFSET 0  -- the fence: see the module's docstring
            ) s
            WHERE ({in_user}) OR ({in_answer})
        ), page AS (
            SELECT m.* FROM matched m
            WHERE {capped} {keyset}
            ORDER BY {_TURN_PAGE.order_by}
            LIMIT %(limit)s
        )
        SELECT p.conversation_thread_id::text AS thread_id,
               COALESCE({_sql.TITLE_OR_FIRST_QUERY}, '') AS thread_title,
               t.workspace_id::text AS workspace_id, w.name AS workspace_name,
               p.turn_index, p.turn_at, p.in_user,
               CASE WHEN p.in_user THEN s.user_text ELSE s.answer_text END AS text
        FROM page p
        JOIN thread_search_turns s
          ON s.conversation_thread_id = p.conversation_thread_id
         AND s.turn_index = p.turn_index
        JOIN conversation_threads t
          ON t.conversation_thread_id = p.conversation_thread_id
        JOIN workspaces w ON w.workspace_id = t.workspace_id
        ORDER BY p.turn_at DESC, p.conversation_thread_id DESC, p.turn_index DESC
        """,
        params,
    )


# A file's recency, never NULL, so it can key a page.
_FILE_AT = "COALESCE(f.sandbox_modified_at, f.updated_at)"


@dataclass(frozen=True)
class FileMatch:
    workspace_file_id: str
    workspace_id: str
    workspace_name: str
    path: str
    name: str
    size: int
    modified_at: datetime
    #: Whether the file's name holds every term; else its folders complete
    #: the match.
    name_match: bool

    @property
    def key(self) -> tuple[bool, datetime, str]:
        return (self.name_match, self.modified_at, self.workspace_file_id)


async def search_files(
    user_id: str,
    terms: Sequence[str],
    *,
    limit: int,
    excluded: Sequence[str],
    root_excluded: str,
    workspace_id: str | None = None,
    after: Sequence[Any] | None = None,
) -> list[FileMatch]:
    """The user's backed-up files whose path holds every term, those whose
    name holds them all first, newest first within each. ``BadKey`` for an
    ``after`` that is no file's key.

    ``excluded`` are LIKE patterns over ``'/' || file_path`` and
    ``root_excluded`` one over a root-level path, each leaving a file out
    here, before the page is cut, so a page is only short at the end.

    The match runs on the columns of ``unique_file_per_workspace``
    (workspace and path), so the user's paths are matched from that index
    alone; only the matches are read from the table, whose rows carry the
    files' contents, one lookup by that key each, which keeps the files
    alone and reads the size and times a page shows and sorts by.
    """
    match = Terms(terms)
    params: dict[str, Any] = {
        **match.params,
        "limit": limit,
        "excluded": list(excluded),
        "root_excluded": root_excluded,
    }
    owned = _owned(params, user_id, workspace_id, thread=False)
    in_name = f"({match.on('f.file_name')})"
    page = Keyset(((in_name, bool), (_FILE_AT, datetime), ("f.workspace_file_id", UUID)))
    keyset = page.after(after, params)
    return await _fetch(
        FileMatch,
        f"""
        WITH owned AS MATERIALIZED (
            SELECT w.workspace_id, w.name FROM workspaces w WHERE {owned}
        ), matched AS (
            SELECT f.workspace_id, f.file_path, f.workspace_name
            FROM (
                SELECT f.workspace_id, f.file_path, w.name AS workspace_name
                FROM owned w
                JOIN workspace_files f ON f.workspace_id = w.workspace_id
                OFFSET 0  -- the fence: see the module's docstring
            ) f
            WHERE {match.on("f.file_path")}
              AND NOT (('/' || f.file_path) LIKE ANY (%(excluded)s::text[]))
              AND NOT (f.file_path LIKE %(root_excluded)s
                       AND strpos(f.file_path, '/') = 0)
        )
        SELECT f.workspace_file_id::text AS workspace_file_id,
               f.workspace_id::text AS workspace_id, m.workspace_name,
               f.file_path AS path, f.file_name AS name, f.file_size AS size,
               {_FILE_AT} AS modified_at, {in_name} AS name_match
        FROM matched m
        -- One lookup by key per match. The fence keeps the planner, which
        -- cannot size ``matched``, from hashing it against a scan of every
        -- user's files instead.
        CROSS JOIN LATERAL (
            SELECT x.* FROM workspace_files x
            WHERE x.workspace_id = m.workspace_id AND x.file_path = m.file_path
              AND x.kind = 'file'
            OFFSET 0
        ) f
        WHERE TRUE {keyset}
        ORDER BY {page.order_by}
        LIMIT %(limit)s
        """,
        params,
    )


@dataclass(frozen=True)
class TurnTexts:
    thread_id: str
    title: str
    workspace_id: str
    workspace_name: str
    turn_index: int
    turn_at: datetime
    user_text: str
    answer_text: str


async def get_turn_texts(
    user_id: str, thread_id: str, turn_index: int | None, *, clip: int
) -> TurnTexts | None:
    """A turn's indexed texts, each cut to ``clip`` characters plus one so a
    caller can tell a cut text; the thread's newest indexed turn when
    ``turn_index`` is None. None when the thread is not the user's to search
    or the turn has no row."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None
    params: dict[str, Any] = {
        "thread_id": thread_id,
        "turn_index": turn_index,
        "cut": clip + 1,
    }
    owned = _owned(params, user_id, None)
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=class_row(TurnTexts)) as cur:
            await cur.execute(
                f"""
                SELECT t.conversation_thread_id::text AS thread_id,
                       COALESCE({_sql.TITLE_OR_FIRST_QUERY}, '') AS title,
                       t.workspace_id::text AS workspace_id,
                       w.name AS workspace_name,
                       s.turn_index, s.turn_at,
                       left(s.user_text, %(cut)s) AS user_text,
                       left(s.answer_text, %(cut)s) AS answer_text
                FROM conversation_threads t
                JOIN workspaces w ON w.workspace_id = t.workspace_id
                JOIN thread_search_turns s
                  ON s.conversation_thread_id = t.conversation_thread_id
                WHERE t.conversation_thread_id = %(thread_id)s
                  AND {owned}
                  AND (%(turn_index)s::int IS NULL OR s.turn_index = %(turn_index)s)
                ORDER BY s.turn_index DESC
                LIMIT 1
                """,
                params,
            )
            return await cur.fetchone()
