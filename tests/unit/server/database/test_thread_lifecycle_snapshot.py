"""Feed-snapshot query: the status vocabulary it binds to, and its row shape.

The classification lives in SQL now, so the thing a unit test can actually
protect is the binding — that the literals interpolated into the statement stay
the ones ``contracts.status`` defines. The bug this round caught (public
``failed`` where run rows persist ``error``, classifying every errored run as
live forever) was produced by a second, hand-maintained copy of that list.
"""

import pytest

from src.server.contracts.status import (
    LIVE_PUBLIC_STATUSES,
    RAW_LIVE_STATUSES,
    RAW_TERMINAL_SNAPSHOT_STATUSES,
    TERMINAL_PUBLIC_STATUSES,
    TERMINAL_STATUSES,
    to_public,
)
from src.server.database.conversation import threads_read
from src.server.database.conversation.threads_read import (
    _LIFECYCLE_SNAPSHOT_SQL,
    get_thread_lifecycle_rows,
)


# ---------------------------------------------------------------------------
# Schema binding
# ---------------------------------------------------------------------------


def test_raw_snapshot_vocabulary_partitions_the_run_row_statuses():
    """Live ∪ terminal covers every run-row status, with no overlap."""
    assert set(RAW_LIVE_STATUSES) | set(RAW_TERMINAL_SNAPSHOT_STATUSES) == set(
        TERMINAL_STATUSES
    ) | {"in_progress"}
    assert not set(RAW_LIVE_STATUSES) & set(RAW_TERMINAL_SNAPSHOT_STATUSES)
    # TERMINAL_STATUSES minus interrupted — interrupted awaits the user, so the
    # feed's live branch owns it.
    assert set(RAW_TERMINAL_SNAPSHOT_STATUSES) == set(TERMINAL_STATUSES) - {
        "interrupted"
    }


def test_raw_statuses_are_run_row_spellings_never_public_ones():
    """`failed` is a to_public output; a run row can never hold it."""
    assert "failed" not in RAW_LIVE_STATUSES
    assert "failed" not in RAW_TERMINAL_SNAPSHOT_STATUSES
    assert "error" in RAW_TERMINAL_SNAPSHOT_STATUSES


def test_raw_branches_project_onto_the_matching_public_partition():
    assert {to_public(s) for s in RAW_TERMINAL_SNAPSHOT_STATUSES} == set(
        TERMINAL_PUBLIC_STATUSES
    )
    assert {to_public(s) for s in RAW_LIVE_STATUSES} <= set(
        LIVE_PUBLIC_STATUSES
    ) | {"interrupted"}


def test_snapshot_sql_literals_equal_the_exported_vocabulary():
    """A status rename must not be able to split SQL from the contract."""
    import re

    in_lists = re.findall(
        r"latest_run_status IN \(\s*([^)]*?)\s*\)", _LIFECYCLE_SNAPSHOT_SQL
    )
    assert len(in_lists) == 2, in_lists
    parsed = [
        tuple(s.strip().strip("'") for s in raw.split(",")) for raw in in_lists
    ]
    assert parsed[0] == RAW_LIVE_STATUSES
    assert parsed[1] == RAW_TERMINAL_SNAPSHOT_STATUSES


def test_snapshot_sql_excludes_archived_rows_from_both_branches():
    assert _LIFECYCLE_SNAPSHOT_SQL.count("o.archived_at IS NULL") == 2


def test_snapshot_sql_caps_only_the_unseen_branch():
    """The live branch must stay UNCAPPED — absence there is the client's
    proof a run isn't live, which a LIMIT would silently forge."""
    live = _LIFECYCLE_SNAPSHOT_SQL.split("live AS (")[1].split("),")[0]
    unseen = _LIFECYCLE_SNAPSHOT_SQL.split("unseen AS (")[1]
    assert "LIMIT" not in live
    assert "ORDER BY o.latest_run_seq DESC" in unseen
    assert "LIMIT %s" in unseen


def test_watermark_is_computed_before_filtering():
    """as_of_seq spans the owned set, not the branches — an empty snapshot
    still has to advance the client's watermark."""
    watermark = _LIFECYCLE_SNAPSHOT_SQL.split("watermark AS (")[1].split(
        "\n    ),"
    )[0]
    assert "MAX(latest_run_seq)" in watermark
    assert "FROM owned" in watermark
    assert "archived_at" not in watermark
    assert "latest_run_status" not in watermark


# ---------------------------------------------------------------------------
# Row shaping
# ---------------------------------------------------------------------------


def _row(branch, seq, as_of=900, **over):
    row = {
        "as_of_seq": as_of,
        "branch": branch,
        "conversation_thread_id": f"t-{seq}",
        "workspace_id": "ws-1",
        "archived_at": None,
        "last_seen_run_seq": 0,
        "latest_run_id": f"r-{seq}",
        "latest_run_status": "completed",
        "latest_cancel_requested_at": None,
        "latest_interrupt_reason": None,
        "latest_run_seq": seq,
        "latest_run_started_at": None,
    }
    row.update(over)
    return row


@pytest.mark.asyncio
async def test_returns_rows_and_watermark(mock_db_connection, mock_cursor):
    mock_cursor.fetchall.return_value = [_row("live", 10), _row("unseen", 9)]

    rows, as_of = await get_thread_lifecycle_rows("u-1")

    assert as_of == 900
    assert [r["branch"] for r in rows] == ["live", "unseen"]


@pytest.mark.asyncio
async def test_empty_branches_still_advance_the_watermark(
    mock_db_connection, mock_cursor
):
    """The LEFT JOIN yields one all-NULL row when nothing is live or unseen —
    it carries the watermark and must not be mistaken for a thread."""
    mock_cursor.fetchall.return_value = [
        {"as_of_seq": 4242, "branch": None, "conversation_thread_id": None}
    ]

    rows, as_of = await get_thread_lifecycle_rows("u-1")

    assert rows == []
    assert as_of == 4242


@pytest.mark.asyncio
async def test_no_owned_threads_returns_zero_watermark(
    mock_db_connection, mock_cursor
):
    mock_cursor.fetchall.return_value = []
    assert await get_thread_lifecycle_rows("u-1") == ([], 0)


@pytest.mark.asyncio
async def test_cap_is_bound_as_cap_plus_one(mock_db_connection, mock_cursor):
    """SQL fetches one row past the cap so Python can see the overflow."""
    mock_cursor.fetchall.return_value = []
    await get_thread_lifecycle_rows("u-1", unseen_cap=256)

    params = mock_cursor.execute.call_args.args[1]
    assert params == ("u-1", 257)


def test_sql_literal_renderer_rejects_nothing_but_renders_quoted():
    from src.server.database.conversation._sql import sql_literals

    assert sql_literals(("a", "b")) == "'a', 'b'"
