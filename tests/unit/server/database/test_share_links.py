"""The share-link writes: one row per item, and what each write may touch.

A second call for the same item must hand back the same code, since the
first one may already be a URL in someone's hands. An app described afresh
moves its title and entry path onto that code in one upsert; a file link's
path is the item's identity and is never rewritten. Sharing is a
compare-and-set, so a Stop that lands mid-share holds.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import patch

import pytest

from src.server.database import share_links
from src.server.database.share_codes import is_share_code
from src.server.database.share_links import (
    KIND_APP,
    KIND_FILE,
    describe_app_link,
    ensure_app_link,
    ensure_file_link,
    set_private,
    set_shared,
)

WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"


def _row(**overrides) -> dict[str, Any]:
    row = {
        "code": "AbCdEfGh1234",
        "workspace_id": WORKSPACE_ID,
        "kind": KIND_FILE,
        "path": "out/report.html",
        "port": None,
        "title": None,
        "files": None,
        "shared_at": None,
        "created_at": datetime.now(timezone.utc),
        "revision": 0,
    }
    row.update(overrides)
    return row


def _app_row(**overrides) -> dict[str, Any]:
    fields = {"kind": KIND_APP, "path": None, "port": 3000}
    fields.update(overrides)
    return _row(**fields)


class _Cursor:
    """Answers each statement from a scripted queue and keeps what was run."""

    def __init__(self, answers: list[Optional[dict]]) -> None:
        self._answers = list(answers)
        self.statements: list[tuple[str, Any]] = []
        self._row = None

    async def execute(self, sql: str, params: Any = None) -> None:
        self.statements.append((re.sub(r"\s+", " ", sql).strip(), params))
        assert self._answers, f"unscripted statement: {sql[:60]}"
        self._row = self._answers.pop(0)

    async def fetchone(self):
        return self._row


def _script(answers):
    cur = _Cursor(answers)

    @asynccontextmanager
    async def _conn_cm():
        class _Conn:
            @asynccontextmanager
            async def cursor(self, **kwargs):
                yield cur

        yield _Conn()

    return cur, patch.object(share_links, "get_db_connection", _conn_cm)


@pytest.mark.asyncio
async def test_a_first_sight_file_is_inserted_private_on_its_partial_index():
    cur, db = _script([_row()])
    with db:
        link = await ensure_file_link(WORKSPACE_ID, "out/report.html")

    (sql, params), = cur.statements
    assert sql.startswith("INSERT INTO share_links (code, workspace_id, kind, path)")
    assert "ON CONFLICT (workspace_id, path) WHERE kind = 'file' DO NOTHING" in sql
    assert is_share_code(params[0])
    assert params[1:] == (WORKSPACE_ID, "out/report.html")
    assert not link.shared
    assert link.display_title == "report.html"


@pytest.mark.asyncio
async def test_a_racing_writer_reads_back_the_winners_row():
    cur, db = _script([None, _row(code="WinnerCode01")])
    with db:
        link = await ensure_file_link(WORKSPACE_ID, "out/report.html")

    assert link.code == "WinnerCode01"
    (_, _), (select_sql, select_params) = cur.statements
    assert select_sql.startswith("SELECT")
    assert select_params == (WORKSPACE_ID, "out/report.html")


@pytest.mark.asyncio
async def test_an_existing_app_link_is_read_back_unrenamed():
    cur, db = _script([None, _app_row(title="Dashboard")])
    with db:
        link = await ensure_app_link(WORKSPACE_ID, 3000)

    insert_sql, params = cur.statements[0]
    assert "ON CONFLICT (workspace_id, port) WHERE kind = 'app' DO NOTHING" in insert_sql
    assert params[1:] == (WORKSPACE_ID, 3000)
    assert link.display_title == "Dashboard"


@pytest.mark.asyncio
async def test_describing_an_app_is_one_upsert_that_replaces_title_and_path():
    cur, db = _script([_app_row(path="index.html")])
    with db:
        link = await describe_app_link(
            WORKSPACE_ID, 3000, title=None, path="index.html"
        )

    (sql, params), = cur.statements
    assert "ON CONFLICT (workspace_id, port) WHERE kind = 'app'" in sql
    assert "DO UPDATE SET title = EXCLUDED.title, path = EXCLUDED.path" in sql
    assert params[1:] == (WORKSPACE_ID, 3000, None, "index.html")
    assert link.display_title == "App on port 3000"


@pytest.mark.asyncio
async def test_sharing_is_a_compare_and_set_on_the_revision_read_first():
    cur, db = _script([None])
    with db:
        result = await set_shared(
            "AbCdEfGh1234", ["out/report.html"], expected_revision=3
        )

    assert result is None
    (sql, params), = cur.statements
    assert "revision = revision + 1" in sql
    assert "AND revision = %s" in sql
    assert params == (["out/report.html"], "AbCdEfGh1234", 3)


@pytest.mark.asyncio
async def test_a_stop_moves_the_revision_so_an_older_share_cannot_land():
    """A stop clears shared_at to what a slow share read; the revision never returns."""
    cur, db = _script([_row(revision=4)])
    with db:
        link = await set_private("AbCdEfGh1234")

    (sql, _), = cur.statements
    assert "SET shared_at = NULL, files = NULL, revision = revision + 1" in sql
    assert link.revision == 4
