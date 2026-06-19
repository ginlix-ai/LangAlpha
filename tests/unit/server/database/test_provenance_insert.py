"""Insert/sync path for provenance records.

Drives ``insert_provenance_records`` / ``sync_provenance_for_response`` against a
mocked psycopg3 cursor (the ``mock_connection`` / ``mock_cursor`` fixtures) and
asserts:

* the write is delete-then-insert keyed by conversation_response_id, batched via
  a single multi-row INSERT, guarded by a per-response advisory lock inside a
  transaction (savepoint) so a failed write can't poison the turn-persist commit,
* every TEXT bind is NUL-stripped and JSONB ``args_fingerprint`` wraps in SafeJson,
* ``sync_provenance_for_response`` is best-effort: it never raises and returns 0
  when the underlying insert fails.
"""

from unittest.mock import AsyncMock, patch

import pytest
from psycopg.types.json import Json

from src.server.database.provenance import (
    _INSERT_COLUMNS,
    insert_provenance_records,
    sync_provenance_for_response,
)

RESPONSE_ID = "resp-1"
THREAD_ID = "thread-1"
NCOLS = len(_INSERT_COLUMNS)


def _record(**fields):
    base = {
        "source_type": "web_search",
        "identifier": "https://example.test/a",
        "title": "A title",
        "tool_call_id": "call-1",
        "args_fingerprint": {"query": "test"},
        "result_sha256": "sha-a",
        "result_size": 1234,
        "result_snippet": "snippet",
        "agent": "main",
        "provider": "tavily",
    }
    base.update(fields)
    return base


def _execute_sqls(mock_cursor):
    return [c.args[0] for c in mock_cursor.execute.call_args_list]


def _insert_call(mock_cursor):
    """The (sql, params) of the single multi-row INSERT execute call."""
    return next(
        c
        for c in mock_cursor.execute.call_args_list
        if "INSERT INTO provenance_records" in c.args[0]
    )


class TestInsertProvenanceRecords:
    @pytest.mark.asyncio
    async def test_lock_delete_then_batched_insert(self, mock_connection, mock_cursor):
        n = await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[_record(), _record(identifier="https://example.test/b")],
        )
        assert n == 2

        sqls = _execute_sqls(mock_cursor)
        # Advisory lock first (serializes concurrent drains for this response).
        assert any("pg_advisory_xact_lock" in s for s in sqls)
        # Then the DELETE keyed by response_id.
        delete = next(c for c in mock_cursor.execute.call_args_list
                      if "DELETE FROM provenance_records" in c.args[0])
        assert delete.args[1] == (RESPONSE_ID,)

        # Rows go in one multi-row INSERT, not per-row executemany.
        mock_cursor.executemany.assert_not_awaited()
        sql, params = _insert_call(mock_cursor).args
        # Two value tuples (the column list opens with "(conversation_..." so
        # only the VALUES rows match "(%s"), and a flat 2 * NCOLS bind list.
        assert sql.count("(%s") == 2
        assert len(params) == 2 * NCOLS

    @pytest.mark.asyncio
    async def test_detail_bound_at_its_column(self, mock_connection, mock_cursor):
        # `detail` (the data-kind slug) must reach the INSERT at its _INSERT_COLUMNS
        # position so GET /provenance can distinguish e.g. company_overview from
        # daily_prices for one ticker. Bind index = position in the column tuple.
        await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[_record(detail="daily_prices")],
        )
        _, params = _insert_call(mock_cursor).args
        assert params[_INSERT_COLUMNS.index("detail")] == "daily_prices"

    @pytest.mark.asyncio
    async def test_empty_records_only_deletes(self, mock_connection, mock_cursor):
        n = await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[],
        )
        assert n == 0
        # Idempotent re-run with no provenance still clears prior rows...
        assert any("DELETE FROM provenance_records" in s
                   for s in _execute_sqls(mock_cursor))
        # ...and never runs an insert.
        assert not any("INSERT INTO provenance_records" in s
                       for s in _execute_sqls(mock_cursor))
        mock_cursor.executemany.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nul_bytes_stripped_from_text_binds(
        self, mock_connection, mock_cursor
    ):
        dirty = _record(
            identifier="https://example.test/\x00a",
            title="ti\x00tle",
            result_snippet="snip\x00pet",
            result_sha256="sha\x00a",
            agent="ma\x00in",
            provider="tav\x00ily",
            tool_call_id="cal\x00l",
            source_type="web_\x00search",
        )
        await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[dirty],
        )
        _, params = _insert_call(mock_cursor).args
        for value in params:
            if isinstance(value, str):
                assert "\x00" not in value

    @pytest.mark.asyncio
    async def test_args_fingerprint_wrapped_in_safejson(
        self, mock_connection, mock_cursor
    ):
        await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[_record(args_fingerprint={"sha256": "te\x00st"})],
        )
        _, params = _insert_call(mock_cursor).args
        json_binds = [v for v in params if isinstance(v, Json)]
        assert len(json_binds) == 1, "args_fingerprint should be a JSONB bind"
        # SafeJson serializes to NUL-free JSON text (the  escape is stripped).
        assert "\\u0000" not in json_binds[0].dumps(json_binds[0].obj)

    @pytest.mark.asyncio
    async def test_null_args_fingerprint_bound_as_none(
        self, mock_connection, mock_cursor
    ):
        await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[_record(args_fingerprint=None)],
        )
        _, params = _insert_call(mock_cursor).args
        assert not any(isinstance(v, Json) for v in params)

    @pytest.mark.asyncio
    async def test_args_wrapped_in_safejson(self, mock_connection, mock_cursor):
        # Readable redacted args are a JSONB bind, NUL-stripped like the hash.
        await insert_provenance_records(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            records=[_record(args_fingerprint=None, args={"symbol": "AA\x00PL"})],
        )
        _, params = _insert_call(mock_cursor).args
        json_binds = [v for v in params if isinstance(v, Json)]
        assert len(json_binds) == 1, "args should be a JSONB bind"
        assert "\\u0000" not in json_binds[0].dumps(json_binds[0].obj)


class TestSyncProvenanceForResponse:
    @pytest.mark.asyncio
    async def test_returns_inserted_count(self, mock_connection, mock_cursor):
        events = [
            {
                "event": "provenance",
                "source_type": "web_search",
                "identifier": "https://example.test/a",
                "result_sha256": "sha-a",
            }
        ]
        n = await sync_provenance_for_response(
            mock_connection,
            conversation_response_id=RESPONSE_ID,
            conversation_thread_id=THREAD_ID,
            turn_index=0,
            sse_events=events,
        )
        assert n == 1
        assert any("INSERT INTO provenance_records" in s
                   for s in _execute_sqls(mock_cursor))

    @pytest.mark.asyncio
    async def test_swallows_insert_failure_returns_zero(self, mock_connection):
        """A provenance failure must NOT raise — it runs inside the turn-persist
        transaction, so propagating would roll back the real response + billing."""
        events = [
            {
                "event": "provenance",
                "source_type": "web_search",
                "identifier": "https://example.test/a",
                "result_sha256": "sha-a",
            }
        ]
        with patch(
            "src.server.database.provenance.insert_provenance_records",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            n = await sync_provenance_for_response(
                mock_connection,
                conversation_response_id=RESPONSE_ID,
                conversation_thread_id=THREAD_ID,
                turn_index=0,
                sse_events=events,
            )
        assert n == 0  # swallowed, never re-raised
