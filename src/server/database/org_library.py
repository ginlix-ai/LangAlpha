"""The organization library's items (migrations 139 and 140).

A thread or file item is a pointer to a link its publisher keeps on; whether
it is listed is read off that link on every listing, never stored, so turning
the link off takes the item out at once and turning it back on brings it
back. A skill item holds its own snapshot and is always listed.
"""

from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Collection
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from psycopg.rows import dict_row

from src.server.database.conversation._sql import TITLE_OR_FIRST_QUERY
from src.server.database.pool import get_db_connection
from src.server.database.shared_workspaces import reach_sql

Kind = Literal["thread", "file", "skill"]

PAGE_SIZE = 50
TITLE_MAX = 200
_MAX_PAGE_SIZE = 100

#: An item with what a member needs to open it: the thread's token or the
#: file link's code and whether that link is on in a live workspace, or the
#: skill's snapshot and the copy ``%(viewer)s`` installed from it.
_SELECT = """
    SELECT i.item_id::text, i.org_id, i.kind, i.title, i.summary,
           i.publisher_id, u.name AS publisher_name,
           i.thread_id::text, i.artifact_id::text,
           i.published_at, i.updated_at,
           t.share_token, l.code, l.path,
           i.skill_name, i.content_hash, i.archive_key, i.archive_bytes,
           us.content_hash AS installed_hash,
           {audience} AS audience,
           {listed} AS listed
      FROM org_library_items i
      LEFT JOIN conversation_threads t ON t.conversation_thread_id = i.thread_id
      LEFT JOIN share_links l ON l.artifact_id = i.artifact_id
      LEFT JOIN workspaces w ON w.workspace_id = COALESCE(t.workspace_id, l.workspace_id)
      LEFT JOIN users u ON u.user_id = i.publisher_id
      LEFT JOIN user_skills us
        ON i.kind = 'skill' AND us.user_id = %(viewer)s
       AND us.workspace_id IS NULL AND us.org_library_item_id = i.item_id
"""

_AUDIENCE = """CASE i.kind
    WHEN 'thread' THEN COALESCE(t.share_permissions->>'audience', 'public')
    WHEN 'file' THEN COALESCE(l.audience, 'public')
    ELSE 'organization'
END"""
_LISTED = """(i.kind = 'skill' OR (w.status IS NOT NULL AND w.status <> 'deleted' AND CASE i.kind
    WHEN 'thread' THEN t.is_shared
    ELSE l.shared_at IS NOT NULL
END))"""
_SELECT = _SELECT.format(audience=_AUDIENCE, listed=_LISTED)

def _not_kept_in(teams: bool) -> str:
    """No shared workspace keeps the item's work in from ``%(viewer)s``: one
    whose managers keep its work inside it, and that the viewer (in the
    teams ``%(viewer_teams)s`` when ``teams``) does not reach."""
    listed = """EXISTS (SELECT 1 FROM shared_workspace_members m
                        WHERE m.workspace_id = s.workspace_id
                          AND m.user_id = %(viewer)s)"""
    reach = reach_sql("s", listed, "%(viewer_teams)s" if teams else None)
    return f"""NOT EXISTS (
    SELECT 1 FROM shared_workspaces s
     WHERE s.workspace_id = w.workspace_id AND s.share_out = 'none'
       AND NOT {reach})"""


def decode_cursor(cursor: str | None) -> tuple[datetime, str] | None:
    """The ``(published_at, item_id)`` keyset a cursor names. Malformed
    raises, so a bad cursor is a 400 rather than the first page again."""
    if not cursor:
        return None
    try:
        raw = urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        published, _, item_id = raw.partition("|")
        stamp = datetime.fromisoformat(published)
        if stamp.tzinfo is None:
            raise ValueError("naive timestamp")
        return stamp, str(UUID(item_id))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("malformed cursor") from exc


def _encode_cursor(row: dict[str, Any]) -> str:
    raw = f"{row['published_at'].isoformat()}|{row['item_id']}"
    return urlsafe_b64encode(raw.encode()).decode().rstrip("=")


async def list_items(
    org_id: str,
    *,
    viewer: str | None = None,
    viewer_teams: Collection[str] = (),
    kind: Kind | None = None,
    cursor: str | None = None,
    limit: int = PAGE_SIZE,
    listed_only: bool = True,
    kept_in: bool = False,
    public_links: bool = True,
) -> tuple[list[dict[str, Any]], str | None]:
    """One page of the organization's items, newest first, and the cursor
    for the next page (None on the last). ``listed_only`` keeps to the items
    a member can open now; the console's moderation list sees every item.
    ``kept_in`` also leaves out the items of a shared workspace whose
    managers keep its work inside it, unless ``viewer`` (in the teams
    ``viewer_teams``) reaches it, since its link opens for no one else
    (migration 144). ``public_links`` false also leaves out the items whose
    link opens to anyone, which the organization's rule stops serving while
    it stands, to its members too."""
    limit = max(1, min(int(limit or PAGE_SIZE), _MAX_PAGE_SIZE))
    where = ["i.org_id = %(org_id)s"]
    params: dict[str, Any] = {"org_id": org_id, "viewer": viewer, "limit": limit + 1}
    if listed_only:
        where.append(_LISTED)
        if not public_links:
            where.append(f"{_AUDIENCE} <> 'public'")
    if kept_in:
        where.append(_not_kept_in(bool(viewer_teams)))
        if viewer_teams:
            params["viewer_teams"] = sorted(viewer_teams)
    if kind is not None:
        where.append("i.kind = %(kind)s")
        params["kind"] = kind
    keyset = decode_cursor(cursor)
    if keyset is not None:
        where.append("(i.published_at, i.item_id) < (%(after_at)s, %(after_id)s)")
        params["after_at"], params["after_id"] = keyset
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                {_SELECT}
                 WHERE {" AND ".join(where)}
                 ORDER BY i.published_at DESC, i.item_id DESC
                 LIMIT %(limit)s
                """,
                params,
            )
            rows = await cur.fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    return rows, _encode_cursor(rows[-1]) if more else None


async def get_item(item_id: str, *, viewer: str | None = None) -> dict[str, Any] | None:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"{_SELECT} WHERE i.item_id = %(item_id)s",
                {"item_id": item_id, "viewer": viewer},
            )
            return await cur.fetchone()


async def find_item(
    *, thread_id: str | None = None, artifact_id: str | None = None
) -> dict[str, Any] | None:
    """The item a thread or a file is published as, if any."""
    column, value = (
        ("i.thread_id", thread_id) if thread_id else ("i.artifact_id", artifact_id)
    )
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"{_SELECT} WHERE {column} = %(value)s",
                {"value": value, "viewer": None},
            )
            return await cur.fetchone()


async def publish(
    *,
    org_id: str,
    publisher_id: str,
    kind: Kind,
    target_id: str,
    title: str | None,
    summary: str | None,
    owner_id: str | None = None,
) -> tuple[str, bool] | None:
    """Publish the publisher's thread or file whose link is on, or edit its
    item if it is already published: ``(item_id, created)``. With no title
    the item is named as the app names the content (the thread's title or
    first message, the file's name), and publishing again keeps the title
    and summary it was not given. None when the target is not the
    publisher's, or its link is off, by the time of the write; the caller
    has checked both, so None means a race lost. ``owner_id`` is whose
    workspace the target is in when that is not the publisher's: a shared
    workspace's holder, for a member publishing its work."""
    if kind == "thread":
        target = f"""
            SELECT t.conversation_thread_id AS thread_id, NULL::uuid AS artifact_id,
                   {TITLE_OR_FIRST_QUERY} AS named
              FROM conversation_threads t
              JOIN workspaces w ON w.workspace_id = t.workspace_id
               AND w.user_id = %(owner_id)s AND w.status <> 'deleted'
             WHERE t.conversation_thread_id = %(target_id)s AND t.is_shared
        """
        conflict = "(thread_id) WHERE thread_id IS NOT NULL"
    else:
        target = """
            SELECT NULL::uuid AS thread_id, l.artifact_id,
                   regexp_replace(l.path, '^.*/', '') AS named
              FROM share_links l
              JOIN workspaces w ON w.workspace_id = l.workspace_id
               AND w.user_id = %(owner_id)s AND w.status <> 'deleted'
             WHERE l.artifact_id = %(target_id)s AND l.kind = 'file'
               AND l.shared_at IS NOT NULL
        """
        conflict = "(artifact_id) WHERE artifact_id IS NOT NULL"
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO org_library_items AS i
                    (org_id, kind, title, summary, publisher_id, thread_id, artifact_id)
                SELECT %(org_id)s, %(kind)s,
                       left(COALESCE(%(title)s, NULLIF(btrim(x.named), ''), 'Untitled'), {TITLE_MAX}),
                       COALESCE(%(summary)s, ''), %(publisher_id)s,
                       x.thread_id, x.artifact_id
                  FROM ({target}) x
                ON CONFLICT {conflict} DO UPDATE
                   SET title = COALESCE(%(title)s, i.title),
                       summary = COALESCE(%(summary)s, i.summary),
                       updated_at = now()
                 WHERE i.publisher_id = EXCLUDED.publisher_id
                RETURNING i.item_id::text, (xmax = 0) AS created
                """,
                {
                    "org_id": org_id,
                    "kind": kind,
                    "title": title,
                    "summary": summary,
                    "publisher_id": publisher_id,
                    "owner_id": owner_id or publisher_id,
                    "target_id": target_id,
                },
            )
            row = await cur.fetchone()
    return (row["item_id"], row["created"]) if row else None


async def edit(
    item_id: str, *, publisher_id: str, title: str | None, summary: str | None
) -> bool:
    """Change the title or summary of the publisher's own item."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE org_library_items
                   SET title = COALESCE(%s, title), summary = COALESCE(%s, summary),
                       updated_at = now()
                 WHERE item_id = %s AND publisher_id = %s
                """,
                (title, summary, item_id, publisher_id),
            )
            return cur.rowcount > 0


async def remove(
    item_id: str, *, publisher_id: str | None = None, org_id: str | None = None
) -> dict[str, Any] | None:
    """Remove an item: the publisher's own, or any of ``org_id``'s (the
    console). Returns what was removed, None when nothing matched."""
    where = ["item_id = %s"]
    params: list[Any] = [item_id]
    if publisher_id is not None:
        where.append("publisher_id = %s")
        params.append(publisher_id)
    if org_id is not None:
        where.append("org_id = %s")
        params.append(org_id)
    if len(params) == 1:
        raise ValueError("a removal names the publisher or the organization")
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                DELETE FROM org_library_items WHERE {" AND ".join(where)}
                RETURNING item_id::text, org_id, kind, title, publisher_id,
                          thread_id::text, artifact_id::text, archive_key
                """,
                params,
            )
            return await cur.fetchone()


async def find_skill_item(
    org_id: str, publisher_id: str, skill_name: str
) -> dict[str, Any] | None:
    """The item a publisher's skill is published as in the organization."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""{_SELECT}
                 WHERE i.org_id = %(org_id)s AND i.kind = 'skill'
                   AND i.publisher_id = %(publisher_id)s AND i.skill_name = %(skill_name)s""",
                {
                    "org_id": org_id,
                    "publisher_id": publisher_id,
                    "skill_name": skill_name,
                    "viewer": None,
                },
            )
            return await cur.fetchone()


async def publish_skill(
    *,
    org_id: str,
    publisher_id: str,
    skill_name: str,
    title: str | None,
    summary: str | None,
    content_hash: str,
    archive_key: str | None,
    archive_blob: bytes | None,
    archive_bytes: int,
) -> tuple[str, bool, str | None]:
    """Publish a snapshot of the publisher's skill, or replace the snapshot
    of the item it is already published as: ``(item_id, created,
    superseded_key)``, the last being the previous snapshot's object key
    when the new one is stored under another."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT archive_key FROM org_library_items
                     WHERE org_id = %s AND kind = 'skill'
                       AND publisher_id = %s AND skill_name = %s
                     FOR UPDATE
                    """,
                    (org_id, publisher_id, skill_name),
                )
                prior = await cur.fetchone()
                await cur.execute(
                    f"""
                    INSERT INTO org_library_items AS i
                        (org_id, kind, title, summary, publisher_id, skill_name,
                         content_hash, archive_key, archive_blob, archive_bytes)
                    VALUES (%(org_id)s, 'skill',
                            left(COALESCE(%(title)s, %(skill_name)s), {TITLE_MAX}),
                            COALESCE(%(summary)s, ''), %(publisher_id)s, %(skill_name)s,
                            %(content_hash)s, %(archive_key)s, %(archive_blob)s,
                            %(archive_bytes)s)
                    ON CONFLICT (org_id, publisher_id, skill_name) WHERE kind = 'skill'
                    DO UPDATE SET
                        title = COALESCE(%(title)s, i.title),
                        summary = COALESCE(%(summary)s, i.summary),
                        content_hash = EXCLUDED.content_hash,
                        archive_key = EXCLUDED.archive_key,
                        archive_blob = EXCLUDED.archive_blob,
                        archive_bytes = EXCLUDED.archive_bytes,
                        updated_at = now()
                    RETURNING i.item_id::text, (xmax = 0) AS created
                    """,
                    {
                        "org_id": org_id,
                        "publisher_id": publisher_id,
                        "skill_name": skill_name,
                        "title": title,
                        "summary": summary,
                        "content_hash": content_hash,
                        "archive_key": archive_key,
                        "archive_blob": archive_blob,
                        "archive_bytes": archive_bytes,
                    },
                )
                row = await cur.fetchone()
    prior_key = prior["archive_key"] if prior else None
    superseded = prior_key if prior_key and prior_key != archive_key else None
    return row["item_id"], row["created"], superseded


async def skill_archive(item_id: str) -> tuple[str | None, bytes | None] | None:
    """A skill item's snapshot: its object key, or its inline bytes."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT archive_key, archive_blob FROM org_library_items "
                "WHERE item_id = %s AND kind = 'skill'",
                (item_id,),
            )
            row = await cur.fetchone()
    if row is None:
        return None
    blob = row["archive_blob"]
    return row["archive_key"], bytes(blob) if blob is not None else None


async def archive_in_use(archive_key: str) -> bool:
    """Whether any item still holds the snapshot stored under ``archive_key``:
    keys are content-addressed per organization, so two items can share one."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM org_library_items WHERE archive_key = %s LIMIT 1",
                (archive_key,),
            )
            return await cur.fetchone() is not None


async def own_skill_origin(user_id: str, name: str) -> tuple[bool, str | None]:
    """Whether the user has an account-wide skill named ``name``, and the
    library item it was installed from (None for one of their own)."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT org_library_item_id::text AS item_id FROM user_skills "
                "WHERE user_id = %s AND name = %s AND workspace_id IS NULL",
                (user_id, name),
            )
            row = await cur.fetchone()
    return (row is not None, row["item_id"] if row else None)
