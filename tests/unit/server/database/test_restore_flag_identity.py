"""The completeness flag lands only while the row names the expected sandbox."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.database import workspace as ws


class _Cur:
    def __init__(self) -> None:
        self.sql = ""
        self.params: tuple = ()
        self.rowcount = 1

    async def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = params


@pytest.fixture
def cur():
    c = _Cur()

    @asynccontextmanager
    async def _cursor(conn=None):
        yield c

    with patch.object(ws, "_ws_cursor", _cursor):
        yield c


GUARDED = "WHERE workspace_id = %s AND sandbox_id IS NOT DISTINCT FROM %s"


@pytest.mark.asyncio
async def test_a_clear_naming_a_sandbox_lands_only_on_that_row(cur):
    await ws.set_files_restore_incomplete("ws-1", False, sandbox_id="sb-1")
    assert cur.sql.endswith(GUARDED)
    assert cur.params == (None, "ws-1", "sb-1")


@pytest.mark.asyncio
async def test_a_raise_names_the_sandbox_the_row_is_expected_to_hold(cur):
    await ws.set_files_restore_incomplete("ws-1", True, sandbox_id="sb-previous")
    assert cur.sql.endswith(GUARDED)
    assert cur.params[1:] == ("ws-1", "sb-previous")


@pytest.mark.asyncio
async def test_a_fresh_row_is_expected_to_name_no_sandbox(cur):
    """A never-provisioned workspace holds NULL; the CAS that follows expects
    exactly that, so the raise must too, and equality would never match."""
    await ws.set_files_restore_incomplete("ws-1", True, sandbox_id=None)
    assert cur.sql.endswith(GUARDED)
    assert cur.params[1:] == ("ws-1", None)


@pytest.mark.asyncio
async def test_any_sandbox_skips_the_guard(cur):
    """A runtime with no sandbox id (a local provider) still has to write."""
    await ws.set_files_restore_incomplete("ws-1", False, sandbox_id=ws.ANY_SANDBOX)
    assert cur.sql.endswith("WHERE workspace_id = %s")
    assert cur.params == (None, "ws-1")


@pytest.mark.asyncio
async def test_the_write_reports_whether_it_landed(cur):
    assert await ws.set_files_restore_incomplete("ws-1", True, sandbox_id="sb-1") is True
    cur.rowcount = 0
    assert await ws.set_files_restore_incomplete("ws-1", True, sandbox_id="sb-1") is False
