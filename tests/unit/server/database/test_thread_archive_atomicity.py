"""Archive must carry its seen-stamp — atomically, or not at all.

An archived thread leaves the feed snapshot. Below the unseen cutoff, absence
is indistinguishable from truncation, so the archive is only honest once the
latest terminal attempt is also marked seen. A committed archive with a lost
stamp recreates exactly that ambiguity, which is why the two writes are one
statement rather than two connections.
"""

from pathlib import Path

import pytest

from src.server.contracts.status import RAW_TERMINAL_SNAPSHOT_STATUSES
from src.server.database.conversation.threads_write import update_thread_fields
from src.server.services.thread_lifecycle import project_lifecycle


def _updated_row(**over):
    row = {
        "conversation_thread_id": "t-1",
        "workspace_id": "ws-1",
        "current_status": "completed",
        "msg_type": "ptc",
        "thread_index": 0,
        "title": "hi",
        "platform": "web",
        "metadata": {},
        "is_pinned": False,
        "archived_at": "2026-08-01T00:00:00Z",
        "last_seen_run_seq": 42,
        "created_at": "2026-08-01T00:00:00Z",
        "updated_at": "2026-08-01T00:00:00Z",
    }
    row.update(over)
    return row


@pytest.mark.asyncio
async def test_archive_and_seen_stamp_are_one_statement(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchone.return_value = _updated_row()

    await update_thread_fields("t-1", archived=True)

    assert mock_cursor.execute.await_count == 1
    sql = mock_cursor.execute.call_args.args[0]
    assert "archived_at = NOW()" in sql
    assert "last_seen_run_seq = GREATEST(" in sql


@pytest.mark.asyncio
async def test_stamp_only_counts_a_terminal_latest_attempt(
    mock_db_connection, mock_cursor
):
    """A live-LIKE latest attempt (in_progress/interrupted) must not be swept
    seen: on unarchive its dot legitimately reflects work that settled while
    the thread was away."""
    mock_cursor.fetchone.return_value = _updated_row()

    await update_thread_fields("t-1", archived=True)

    sql = mock_cursor.execute.call_args.args[0]
    for status in RAW_TERMINAL_SNAPSHOT_STATUSES:
        assert f"'{status}'" in sql
    assert "'in_progress'" not in sql
    assert "'interrupted'" not in sql
    # Newest attempt only — never "stamp whatever is latest across the thread".
    assert "ORDER BY cr.run_seq DESC" in sql
    assert "LIMIT 1" in sql


@pytest.mark.asyncio
async def test_archive_binds_the_thread_id_to_both_stamp_and_predicate(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchone.return_value = _updated_row()

    await update_thread_fields("t-1", archived=True)

    assert mock_cursor.execute.call_args.args[1] == ("t-1", "t-1")


@pytest.mark.asyncio
async def test_unarchive_clears_the_flag_without_stamping(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchone.return_value = _updated_row(archived_at=None)

    await update_thread_fields("t-1", archived=False)

    sql = mock_cursor.execute.call_args.args[0]
    assert "archived_at = NULL" in sql
    assert "last_seen_run_seq" not in sql.split("RETURNING")[0]


@pytest.mark.asyncio
async def test_a_failing_write_applies_neither_half(
    mock_db_connection, mock_cursor
):
    """One statement means one atom: an injected failure can't leave the row
    archived-but-unstamped, and the caller sees the error (so no prune event
    is published for a write that never landed)."""
    mock_cursor.execute.side_effect = RuntimeError("deadlock detected")

    with pytest.raises(RuntimeError):
        await update_thread_fields("t-1", archived=True)

    assert mock_cursor.execute.await_count == 1


def test_archived_list_rows_carry_what_the_projection_needs():
    """The stamp only suppresses the dot if the archived listing actually
    returns the cursor: the client's post-archive invalidation refetches that
    list, and a row missing `last_seen_run_seq` projects it as 0 — which
    re-seeds `unseen` for every terminal thread and resurrects the dot the
    stamp just cleared. Spans two modules, so neither one's test catches it."""
    from src.server.database.conversation import threads_read

    src = Path(threads_read.__file__).read_text()
    paged = src.split("async def get_workspace_threads")[1].split("async def ")[0]
    assert "last_seen_run_seq" in paged
    for col in ("latest_run_seq", "latest_run_status"):
        assert col in threads_read._LATEST_ATTEMPT_LATERAL_COLS


def test_stamped_archived_row_projects_as_seen():
    """End of the same chain: cursor >= latest terminal attempt ⇒ no dot."""
    stamped = project_lifecycle(
        {
            "latest_run_status": "completed",
            "latest_run_seq": 42,
            "last_seen_run_seq": 42,
            "latest_run_id": "r-1",
        }
    )
    assert stamped["unseen"] is False

    # The same row without the stamp is exactly the resurrected dot.
    assert project_lifecycle(
        {
            "latest_run_status": "completed",
            "latest_run_seq": 42,
            "last_seen_run_seq": 0,
            "latest_run_id": "r-1",
        }
    )["unseen"] is True


@pytest.mark.asyncio
async def test_pin_toggle_never_stamps_seen(mock_db_connection, mock_cursor):
    mock_cursor.fetchone.return_value = _updated_row(is_pinned=True)

    await update_thread_fields("t-1", is_pinned=True)

    sql = mock_cursor.execute.call_args.args[0]
    assert "last_seen_run_seq" not in sql.split("RETURNING")[0]
