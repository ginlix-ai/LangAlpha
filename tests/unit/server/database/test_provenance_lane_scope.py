"""Lane-scoped provenance rewrites.

A response's provenance rows are rewritten per lane: the finalize replaces the
main lane from its own record, a subagent archive replaces only the lanes it
holds. The delete is therefore scoped to the given lanes plus every inserted
record's own agent, so a write always replaces what it inserts and never
touches a lane it does not name.
"""

import pytest

from src.server.database.provenance import (
    MAIN_PROVENANCE_LANE,
    insert_provenance_records,
    sync_provenance_for_response,
)

RESPONSE_ID = "resp-1"
THREAD_ID = "thread-1"

_SCOPED_DELETE = (
    "DELETE FROM provenance_records "
    "WHERE conversation_response_id = %s "
    "AND COALESCE(agent, '') = ANY(%s)"
)


def _record(agent, identifier="https://example.test/a"):
    return {
        "source_type": "web_search",
        "identifier": identifier,
        "result_sha256": f"sha-{identifier}",
        "agent": agent,
    }


def _statements(mock_cursor):
    return [" ".join(c.args[0].split()) for c in mock_cursor.execute.call_args_list]


def _delete_call(mock_cursor):
    return next(
        c
        for c in mock_cursor.execute.call_args_list
        if "DELETE FROM provenance_records" in c.args[0]
    )


async def _insert(conn, records, lanes):
    return await insert_provenance_records(
        conn,
        conversation_response_id=RESPONSE_ID,
        conversation_thread_id=THREAD_ID,
        turn_index=0,
        records=records,
        lanes=lanes,
    )


@pytest.mark.asyncio
async def test_the_main_lane_replaces_only_its_own_rows(mock_connection, mock_cursor):
    n = await _insert(
        mock_connection,
        [_record("main"), _record("main", "https://example.test/b")],
        (MAIN_PROVENANCE_LANE,),
    )
    assert n == 2

    statements = _statements(mock_cursor)
    assert [s.split()[0] for s in statements] == ["SELECT", "DELETE", "INSERT"]
    assert "pg_advisory_xact_lock" in statements[0]
    sql, params = _delete_call(mock_cursor).args
    assert " ".join(sql.split()) == _SCOPED_DELETE
    assert params == (RESPONSE_ID, ["main"])


@pytest.mark.asyncio
async def test_every_records_own_agent_joins_the_scope(mock_connection, mock_cursor):
    await _insert(
        mock_connection,
        [
            _record("task:k7"),
            _record(None, "https://example.test/b"),
            _record("main", "https://example.test/c"),
        ],
        (MAIN_PROVENANCE_LANE,),
    )
    _, params = _delete_call(mock_cursor).args
    # An agentless record is lane "", which COALESCE(agent, '') matches.
    assert params == (RESPONSE_ID, ["", "main", "task:k7"])


@pytest.mark.asyncio
async def test_a_record_without_an_agent_key_is_lane_empty(
    mock_connection, mock_cursor
):
    record = _record(None)
    del record["agent"]
    await _insert(mock_connection, [record], ())
    _, params = _delete_call(mock_cursor).args
    assert params == (RESPONSE_ID, [""])


@pytest.mark.asyncio
async def test_the_scope_is_the_sorted_union_whatever_the_lanes_order(
    mock_connection, mock_cursor
):
    await _insert(mock_connection, [_record("task:a")], {"task:c", "task:b", "task:a"})
    _, params = _delete_call(mock_cursor).args
    assert params == (RESPONSE_ID, ["task:a", "task:b", "task:c"])


@pytest.mark.asyncio
async def test_an_empty_lane_still_clears_its_rows(mock_connection, mock_cursor):
    """A lane rewritten with no records (an archive that emptied it) loses
    the rows it had, and only those."""
    n = await _insert(mock_connection, [], (MAIN_PROVENANCE_LANE,))
    assert n == 0

    sql, params = _delete_call(mock_cursor).args
    assert " ".join(sql.split()) == _SCOPED_DELETE
    assert params == (RESPONSE_ID, ["main"])
    assert not any(s.startswith("INSERT") for s in _statements(mock_cursor))


@pytest.mark.asyncio
async def test_the_sync_entry_point_passes_its_lanes_through(
    mock_connection, mock_cursor
):
    events = [
        {
            "event": "provenance",
            "data": {
                "source_type": "web_search",
                "identifier": "https://example.test/a",
                "result_sha256": "sha-a",
                "agent": "task:k7",
            },
        }
    ]
    n = await sync_provenance_for_response(
        mock_connection,
        conversation_response_id=RESPONSE_ID,
        conversation_thread_id=THREAD_ID,
        turn_index=0,
        sse_events=events,
        strict=True,
        lanes=("task:k7",),
    )
    assert n == 1
    _, params = _delete_call(mock_cursor).args
    assert params == (RESPONSE_ID, ["task:k7"])
