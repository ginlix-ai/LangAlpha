"""Regression tests for the idempotent ``create_query`` collision path.

The idempotent ``INSERT ... ON CONFLICT DO UPDATE`` gates the UPDATE on
``content IS NOT DISTINCT FROM EXCLUDED.content`` so that:

* same-content retries (HITL resume, network retry) silently no-op, AND
* different-content collisions (a concurrent POST that bypassed the
  in-process admission lock) surface as ``QueryConflictError`` — never
  silently overwrite the loser's row.

The "no RETURNING row + WHERE-gate fired" path is the only one that
distinguishes these two cases. These tests pin that branch by driving the
mock cursor through the two sequential fetchone() calls the function makes:
the original ``RETURNING`` from the gated UPDATE, and the follow-up SELECT
that fetches the colliding row's content.
"""

import pytest

from src.server.database.conversation import QueryConflictError, create_query


# ---------------------------------------------------------------------------
# QueryConflictError surface
# ---------------------------------------------------------------------------


class TestCreateQueryIdempotentConflict:
    """When the WHERE-gate blocks the UPDATE and a colliding row already
    exists with different content, ``create_query`` must raise
    ``QueryConflictError`` carrying the turn_index and the existing content.
    """

    @pytest.mark.asyncio
    async def test_raises_query_conflict_when_existing_content_differs(
        self, mock_db_connection, mock_cursor
    ):
        # First fetchone(): the INSERT...RETURNING returned no row (the
        # WHERE-gate blocked the UPDATE because EXCLUDED.content differs).
        # Second fetchone(): the follow-up SELECT finds the row that won
        # the race, with its (different) content.
        mock_cursor.fetchone.side_effect = [
            None,  # gated UPDATE returned nothing
            {"content": "other text"},  # the colliding row's content
        ]

        with pytest.raises(QueryConflictError) as excinfo:
            await create_query(
                conversation_query_id="q-1",
                conversation_thread_id="t-1",
                turn_index=3,
                content="new content",
                query_type="ptc",
            )

        err = excinfo.value
        assert err.thread_id == "t-1"
        assert err.turn_index == 3
        assert err.existing_content == "other text", (
            f"existing_content should be the row that won the race; "
            f"got {err.existing_content!r}"
        )
        # Message text includes the thread and turn (used by ops dashboards
        # and log alerts).
        assert "t-1" in str(err)
        assert "turn_index=3" in str(err)

    @pytest.mark.asyncio
    async def test_existing_content_none_when_followup_select_finds_nothing(
        self, mock_db_connection, mock_cursor
    ):
        """Edge case: the gated UPDATE returned nothing AND the follow-up
        SELECT also finds no row (rare — the conflict row was deleted
        between the two queries). Surface the conflict anyway so the caller
        sees the collision instead of returning None silently."""
        mock_cursor.fetchone.side_effect = [None, None]

        with pytest.raises(QueryConflictError) as excinfo:
            await create_query(
                conversation_query_id="q-1",
                conversation_thread_id="t-1",
                turn_index=0,
                content="anything",
                query_type="ptc",
            )

        assert excinfo.value.existing_content is None
