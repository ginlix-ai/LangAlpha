"""Share links: one row per file or app, private until ``shared_at`` is set.

A file's row is also its artifact record, with versions and comments; that
side of it is ``database/artifacts``. Every read here takes the record's
columns too, so a file's link carries its record and no caller reads the row
twice.

Nothing here is cached. A revoke has to hold on the next request on every
worker, and the row is the only place all of them can read it from.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional

from psycopg.rows import dict_row

from src.server.database.artifacts.records import (
    ARTIFACT_COLUMNS,
    Artifact,
    artifact_from_row,
)
from src.server.database.pool import get_db_connection
from src.server.database.share_codes import mint_share_code

KIND_FILE = "file"
KIND_APP = "app"

# Off ``share_links l``.
_COLUMNS = f"{ARTIFACT_COLUMNS}, l.kind, l.port, l.title"


def app_display_title(port: Optional[int], title: Optional[str] = None) -> str:
    return title or f"App on port {port}"


@dataclass(frozen=True)
class ShareLink:
    code: str
    workspace_id: str
    kind: Literal["file", "app"]
    path: Optional[str]
    port: Optional[int]
    title: Optional[str]
    files: Optional[tuple[str, ...]]
    shared_at: Optional[datetime]
    created_at: datetime
    # Moves on every share and every stop, so it never comes back to a value
    # a caller read before; ``shared_at`` does, when a stop clears it.
    revision: int = 0
    #: A file's artifact record, read with the link; None for an app.
    artifact: Optional[Artifact] = None

    @property
    def artifact_id(self) -> Optional[str]:
        return self.artifact.artifact_id if self.artifact else None

    @property
    def shared(self) -> bool:
        """Whether a visitor may open it. An app link never is (049's check)."""
        return self.kind == KIND_FILE and self.shared_at is not None

    @property
    def display_title(self) -> str:
        if self.kind == KIND_APP:
            return app_display_title(self.port, self.title)
        return (self.path or "").rsplit("/", 1)[-1]


def _link(record: dict[str, Any]) -> ShareLink:
    files = record.get("files")
    return ShareLink(
        code=record["code"],
        workspace_id=str(record["workspace_id"]),
        kind=record["kind"],
        path=record.get("path"),
        port=record.get("port"),
        title=record.get("title"),
        files=tuple(files) if files is not None else None,
        shared_at=record.get("shared_at"),
        created_at=record["created_at"],
        revision=record["revision"],
        # An app row has an artifact_id too, from the column default, and
        # never uses it.
        artifact=artifact_from_row(record) if record["kind"] == KIND_FILE else None,
    )


def file_link(artifact: Artifact) -> ShareLink:
    """A file's link as its record stands, with no read."""
    return ShareLink(
        code=artifact.code,
        workspace_id=artifact.workspace_id,
        kind=KIND_FILE,
        path=artifact.path,
        port=None,
        title=None,
        files=artifact.files,
        shared_at=artifact.shared_at,
        created_at=artifact.created_at,
        revision=artifact.revision,
        artifact=artifact,
    )


async def _fetch_link(cur) -> Optional[ShareLink]:
    record = await cur.fetchone()
    return _link(record) if record else None


async def _insert_or_select(
    insert_sql: str, insert_params: tuple, select_sql: str, select_params: tuple
) -> ShareLink:
    """The inserted row, or the one a racing writer committed first.

    The INSERT is ON CONFLICT DO NOTHING against the item's unique index, so
    two writers for the same item both land on one row and one code.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(insert_sql, insert_params)
            created = await _fetch_link(cur)
            if created is not None:
                return created
            await cur.execute(select_sql, select_params)
            existing = await _fetch_link(cur)
    if existing is None:
        raise RuntimeError("share link vanished between insert and read")
    return existing


async def ensure_app_link(workspace_id: str, port: int) -> ShareLink:
    """The app's one link, as it stands; see ``describe_app_link`` to rename it."""
    return await _insert_or_select(
        "INSERT INTO share_links AS l (code, workspace_id, kind, port) "
        "VALUES (%s, %s, 'app', %s) "
        "ON CONFLICT (workspace_id, port) WHERE kind = 'app' DO NOTHING "
        f"RETURNING {_COLUMNS}",
        (mint_share_code(), workspace_id, port),
        f"SELECT {_COLUMNS} FROM share_links l "
        "WHERE l.workspace_id = %s AND l.kind = 'app' AND l.port = %s",
        (workspace_id, port),
    )


async def describe_app_link(
    workspace_id: str, port: int, *, title: Optional[str], path: Optional[str]
) -> ShareLink:
    """The app's link, with the title and entry path the latest call gave it.

    Each ``GetPreviewUrl`` call describes the whole app afresh, so both
    columns are replaced, a missing title included, and the code never is:
    it may already be a URL in someone's hands.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "INSERT INTO share_links AS l "
                "(code, workspace_id, kind, port, title, path) "
                "VALUES (%s, %s, 'app', %s, %s, %s) "
                "ON CONFLICT (workspace_id, port) WHERE kind = 'app' "
                "DO UPDATE SET title = EXCLUDED.title, path = EXCLUDED.path "
                f"RETURNING {_COLUMNS}",
                (mint_share_code(), workspace_id, port, title, path),
            )
            link = await _fetch_link(cur)
    if link is None:
        raise RuntimeError("app link upsert returned no row")
    return link


async def get_link(code: str) -> Optional[ShareLink]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM share_links l WHERE l.code = %s", (code,)
            )
            return await _fetch_link(cur)


async def set_private(code: str) -> Optional[ShareLink]:
    """Stop sharing. Unconditional: a revoke wins over whatever it races."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE share_links l "
                "SET shared_at = NULL, files = NULL, revision = l.revision + 1 "
                f"WHERE l.code = %s RETURNING {_COLUMNS}",
                (code,),
            )
            return await _fetch_link(cur)


async def list_shared(workspace_id: str) -> list[ShareLink]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM share_links l "
                "WHERE l.workspace_id = %s AND l.shared_at IS NOT NULL "
                "ORDER BY l.shared_at DESC",
                (workspace_id,),
            )
            return [_link(r) for r in await cur.fetchall()]
