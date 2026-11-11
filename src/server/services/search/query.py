"""The command palette's reads: threads by title, turns by what was said,
and files by path.

A query is split into terms, each matched anywhere and case-insensitively,
all of them required: in the thread's shown title, within one side of a turn,
or in a file's path, so a folder's name finds its files. Threads and turns
rank by recency. A thread contributes at most ``TURNS_PER_THREAD`` turns, its
newest matches, so one long conversation cannot fill the group; a search
inside one thread lists every match. Files whose name holds every term come
before those only their folders matched, newest first within each.

Files are read from the workspaces' backups (``workspace_files``), which is
what covers a stopped workspace; a file a running computer has written since
its last backup is not found until the next one. The panel's rules for which
backed-up files it lists leave files out in the SQL itself, so a page is cut
from what is shown.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection, Sequence
from typing import Any, Protocol

import psycopg

from src.server.database.search import read as db
from src.server.database.search.sql import BadKey
from src.server.models.search import (
    FileHit,
    SearchCursors,
    SearchResponse,
    ThreadHit,
    TurnHit,
    TurnPreview,
)
from src.server.services.served_paths import (
    STORED_FILE_EXCLUDED_LIKES,
    STORED_FILE_ROOT_EXCLUDED_LIKE,
)
from src.server.services.search.text import (
    Cursor,
    Kind,
    decode_cursor,
    encode_cursor,
    match_ranges,
    snippet,
    split_terms,
)

TURNS_PER_THREAD = 3
PREVIEW_CHARS = 4000


class BadSearch(ValueError):
    """A request the search cannot answer as asked."""


class SearchTimedOut(Exception):
    """Every kind the search asked for ran past its statement timeout."""


class _Keyed(Protocol):
    @property
    def key(self) -> tuple[Any, ...]: ...


def _page[R: _Keyed, H](
    kind: Kind, rows: Sequence[R], limit: int, to_hit: Callable[[R], H]
) -> tuple[list[H], str | None]:
    """The first ``limit`` of ``rows`` (read one past it) as hits, and the
    cursor to the next page when there is one."""
    page = rows[:limit]
    more = len(rows) > limit
    return [to_hit(row) for row in page], (
        encode_cursor(Cursor(kind, page[-1].key)) if more else None
    )


async def search(
    user_id: str,
    q: str,
    *,
    kinds: Collection[Kind],
    limit: int,
    cursor: str | None = None,
    workspace_id: str | None = None,
    thread_id: str | None = None,
) -> SearchResponse:
    """Each group of ``kinds`` for ``q``. Inside one thread there are no
    threads or files to list. A kind whose read runs past its timeout comes
    back empty and named in ``failed``, so one slow group never costs the
    others; ``SearchTimedOut`` when every kind asked for did."""
    terms = split_terms(q)
    if thread_id is not None:
        kinds = [k for k in kinds if k == "turns"]
    after: Sequence[Any] | None = None
    if cursor is not None:
        try:
            decoded = decode_cursor(cursor)
        except ValueError as e:
            raise BadSearch("cursor is not one this search handed out") from e
        if set(kinds) != {decoded.kind}:
            raise BadSearch(f"a {decoded.kind} cursor pages kinds={decoded.kind} alone")
        after = decoded.key

    async def threads() -> tuple[list[ThreadHit], str | None]:
        if "threads" not in kinds:
            return [], None
        rows = await db.search_threads(
            user_id, terms, limit=limit + 1, workspace_id=workspace_id, after=after
        )
        return _page("threads", rows, limit, lambda row: _thread(row, terms))

    async def turns() -> tuple[list[TurnHit], str | None]:
        if "turns" not in kinds:
            return [], None
        rows = await db.search_turns(
            user_id,
            terms,
            limit=limit + 1,
            per_thread=None if thread_id is not None else TURNS_PER_THREAD,
            workspace_id=workspace_id,
            thread_id=thread_id,
            after=after,
        )
        return _page("turns", rows, limit, lambda row: _turn(row, terms))

    async def files() -> tuple[list[FileHit], str | None]:
        if "files" not in kinds:
            return [], None
        rows = await db.search_files(
            user_id,
            terms,
            limit=limit + 1,
            excluded=STORED_FILE_EXCLUDED_LIKES,
            root_excluded=STORED_FILE_ROOT_EXCLUDED_LIKE,
            workspace_id=workspace_id,
            after=after,
        )
        return _page("files", rows, limit, lambda row: _file(row, terms))

    failed: list[Kind] = []

    async def kind[H](
        name: Kind, read: Callable[[], Awaitable[tuple[list[H], str | None]]]
    ) -> tuple[list[H], str | None]:
        try:
            return await read()
        except psycopg.errors.QueryCanceled:
            failed.append(name)
            return [], None

    try:
        (
            (thread_hits, threads_next),
            (turn_hits, turns_next),
            (file_hits, files_next),
        ) = await asyncio.gather(
            kind("threads", threads), kind("turns", turns), kind("files", files)
        )
    except BadKey as e:  # a cursor's key that is not its kind's
        raise BadSearch("cursor is not one this search handed out") from e
    if failed and set(failed) >= set(kinds):
        raise SearchTimedOut(", ".join(failed))
    return SearchResponse(
        threads=thread_hits,
        turns=turn_hits,
        files=file_hits,
        next_cursor=SearchCursors(
            threads=threads_next, turns=turns_next, files=files_next
        ),
        failed=[k for k in ("threads", "turns", "files") if k in failed],
    )


def _thread(row: db.ThreadMatch, terms: list[str]) -> ThreadHit:
    return ThreadHit(
        thread_id=row.thread_id,
        title=row.title,
        workspace_id=row.workspace_id,
        workspace_name=row.workspace_name,
        automation_name=row.automation_name,
        updated_at=row.updated_at,
        is_pinned=row.is_pinned,
        has_title=row.has_title,
        match=match_ranges(row.title, terms),
    )


def _turn(row: db.TurnMatch, terms: list[str]) -> TurnHit:
    text, matches = snippet(row.text, terms)
    return TurnHit(
        thread_id=row.thread_id,
        thread_title=row.thread_title,
        workspace_id=row.workspace_id,
        workspace_name=row.workspace_name,
        turn_index=row.turn_index,
        side="user" if row.in_user else "agent",
        snippet=text,
        matches=matches,
        turn_at=row.turn_at,
        is_pinned=row.is_pinned,
        has_title=row.has_title,
    )


def _file(row: db.FileMatch, terms: list[str]) -> FileHit:
    return FileHit(
        workspace_id=row.workspace_id,
        workspace_name=row.workspace_name,
        path=row.path,
        name=row.name,
        size=row.size,
        modified_at=row.modified_at,
        match=match_ranges(row.path, terms),
    )


def _clip(text: str) -> str:
    return text[:PREVIEW_CHARS] + "…" if len(text) > PREVIEW_CHARS else text


async def preview(
    user_id: str, thread_id: str, turn_index: int | None
) -> TurnPreview | None:
    """A turn's message and answer as indexed, each clipped to
    ``PREVIEW_CHARS``; the thread's newest indexed turn without
    ``turn_index``. None when the thread is not the user's or the turn has
    no row."""
    row = await db.get_turn_texts(user_id, thread_id, turn_index, clip=PREVIEW_CHARS)
    if row is None:
        return None
    return TurnPreview(
        thread_id=row.thread_id,
        title=row.title,
        workspace_id=row.workspace_id,
        workspace_name=row.workspace_name,
        turn_index=row.turn_index,
        turn_at=row.turn_at,
        user_text=_clip(row.user_text),
        answer_text=_clip(row.answer_text),
    )
