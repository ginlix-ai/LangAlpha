"""The run list pages by reading one row past the page, never by a count."""

from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from src.server.database import automation as auto_db


@pytest.fixture
def db(mock_connection, mock_cursor):
    @asynccontextmanager
    async def _connection():
        yield mock_connection

    with patch("src.server.database.automation.get_db_connection", new=_connection):
        yield mock_cursor


@pytest.mark.asyncio
@pytest.mark.parametrize(("fetched", "has_more"), [(3, True), (2, False), (0, False)])
async def test_a_page_reads_one_row_past_itself(db, fetched, has_more):
    db.fetchall.return_value = [{"automation_execution_id": f"run-{i}"} for i in range(fetched)]

    rows, more = await auto_db.list_executions("user-1", limit=2, offset=4)

    assert [r["automation_execution_id"] for r in rows] == [f"run-{i}" for i in range(min(fetched, 2))]
    assert more is has_more
    db.execute.assert_awaited_once()
    sql, params = db.execute.await_args.args
    assert "COUNT" not in sql.upper()
    assert params[-2:] == (3, 4)
