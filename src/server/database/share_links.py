"""Share links: one row per file or app, private until ``shared_at`` is set.

Nothing here is cached. A revoke has to hold on the next request on every
worker, and the row is the only place all of them can read it from.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.share_codes import mint_share_code

KIND_FILE = "file"
KIND_APP = "app"

_COLUMNS = (
    "code, workspace_id, kind, path, port, title, files, shared_at, created_at, revision"
)


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


async def ensure_file_link(workspace_id: str, path: str) -> ShareLink:
    """The file's one link, created private on first sight."""
    return await _insert_or_select(
        "INSERT INTO share_links (code, workspace_id, kind, path) "
        "VALUES (%s, %s, 'file', %s) "
        "ON CONFLICT (workspace_id, path) WHERE kind = 'file' DO NOTHING "
        f"RETURNING {_COLUMNS}",
        (mint_share_code(), workspace_id, path),
        f"SELECT {_COLUMNS} FROM share_links "
        "WHERE workspace_id = %s AND kind = 'file' AND path = %s",
        (workspace_id, path),
    )


async def ensure_app_link(workspace_id: str, port: int) -> ShareLink:
    """The app's one link, as it stands; see ``describe_app_link`` to rename it."""
    return await _insert_or_select(
        "INSERT INTO share_links (code, workspace_id, kind, port) "
        "VALUES (%s, %s, 'app', %s) "
        "ON CONFLICT (workspace_id, port) WHERE kind = 'app' DO NOTHING "
        f"RETURNING {_COLUMNS}",
        (mint_share_code(), workspace_id, port),
        f"SELECT {_COLUMNS} FROM share_links "
        "WHERE workspace_id = %s AND kind = 'app' AND port = %s",
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
                "INSERT INTO share_links (code, workspace_id, kind, port, title, path) "
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
                f"SELECT {_COLUMNS} FROM share_links WHERE code = %s", (code,)
            )
            return await _fetch_link(cur)


async def set_shared(
    code: str, files: list[str], *, expected_revision: int
) -> Optional[ShareLink]:
    """Share with exactly ``files``, if the link is still the revision the caller read.

    The caller builds ``files`` between that read and this write, long enough
    for a Stop to land, and the compare-and-set keeps it from being
    overwritten, even by a share and a stop that land in between and leave
    ``shared_at`` as it was read. None means the link moved or is gone. The
    row's check constraint refuses a list that does not carry the entry path.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE share_links "
                "SET shared_at = NOW(), files = %s, revision = revision + 1 "
                "WHERE code = %s AND kind = 'file' AND revision = %s "
                f"RETURNING {_COLUMNS}",
                (list(files), code, expected_revision),
            )
            return await _fetch_link(cur)


async def set_private(code: str) -> Optional[ShareLink]:
    """Stop sharing. Unconditional: a revoke wins over whatever it races."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE share_links "
                "SET shared_at = NULL, files = NULL, revision = revision + 1 "
                f"WHERE code = %s RETURNING {_COLUMNS}",
                (code,),
            )
            return await _fetch_link(cur)


async def list_shared(workspace_id: str) -> list[ShareLink]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM share_links "
                "WHERE workspace_id = %s AND shared_at IS NOT NULL "
                "ORDER BY shared_at DESC",
                (workspace_id,),
            )
            return [_link(r) for r in await cur.fetchall()]
