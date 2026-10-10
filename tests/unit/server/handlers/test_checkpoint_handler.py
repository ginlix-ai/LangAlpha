"""
Tests for src/server/handlers/checkpoint_handler.py

Covers:
- get_thread_turns: turn boundary detection, branch walking, HITL resume
- get_retry_checkpoint: checkpoint validation, auto-detection
- Missing checkpoint handling
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cp_tuple(
    checkpoint_id: str,
    parent_checkpoint_id: str | None = None,
    source: str = "loop",
    pending_writes: list | None = None,
    turn_index: int | None = None,
):
    """Build a minimal checkpoint tuple matching the structure used by LangGraph.

    ``turn_index`` is the stamp a run's config metadata leaves on every
    checkpoint it writes.
    """
    config = {"configurable": {"checkpoint_id": checkpoint_id}}
    parent_config = None
    if parent_checkpoint_id:
        parent_config = {"configurable": {"checkpoint_id": parent_checkpoint_id}}
    metadata = {"source": source}
    if turn_index is not None:
        metadata["turn_index"] = turn_index
    return SimpleNamespace(
        config=config,
        parent_config=parent_config,
        metadata=metadata,
        pending_writes=pending_writes or [],
    )


@pytest.fixture(autouse=True)
def persisted_turns():
    """The thread's persisted turn_indexes; contiguous unless a test says not."""
    with patch(
        "src.server.handlers.checkpoint_handler.queries_db.get_query_turn_indexes",
        new=AsyncMock(return_value=list(range(10))),
    ) as read:
        yield read


def _checkpointer(*newest_first):
    checkpointer = AsyncMock()

    async def alist(config):
        for cp in newest_first:
            yield cp

    checkpointer.alist = alist
    return checkpointer


async def _turns(checkpointer):
    from src.server.handlers.checkpoint_handler import get_thread_turns

    with patch(
        "src.server.handlers.checkpoint_handler.get_checkpointer",
        return_value=checkpointer,
    ):
        return await get_thread_turns("t1")


# A thread whose turn 1 ran and failed before its first checkpoint: turns 0, 2
# and 3 have boundaries, and turn 2 is interrupted and answered by turn 3.
_DEAD_TURN_THEN_RESUME = (
    _make_cp_tuple("cp-6", "cp-5", source="loop", turn_index=3),
    _make_cp_tuple(
        "cp-5",
        "cp-4",
        source="loop",
        turn_index=2,
        pending_writes=[("task-1", "__resume__", {"decisions": [{"type": "approve"}]})],
    ),
    _make_cp_tuple("cp-4", "cp-3", source="input", turn_index=2),
    _make_cp_tuple("cp-3", "cp-2", source="loop", turn_index=0),
    _make_cp_tuple("cp-2", "cp-1", source="loop", turn_index=0),
    _make_cp_tuple("cp-1", source="input", turn_index=0),
)


# ---------------------------------------------------------------------------
# get_thread_turns
# ---------------------------------------------------------------------------


class TestGetThreadTurns:
    """Tests for get_thread_turns."""

    @pytest.mark.asyncio
    async def test_empty_checkpoints_returns_empty_response(self):
        mock_checkpointer = AsyncMock()

        async def empty_alist(config):
            return
            yield  # noqa: unreachable — makes this an async generator

        mock_checkpointer.alist = empty_alist

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_thread_turns

            result = await get_thread_turns("t1")

        assert result.thread_id == "t1"
        assert result.turns == []
        assert result.retry_checkpoint_id is None

    @pytest.mark.asyncio
    async def test_single_input_turn(self):
        """One source=input checkpoint should produce one turn."""
        cp1 = _make_cp_tuple("cp-1", source="input")  # Turn 0

        mock_checkpointer = AsyncMock()

        async def alist(config):
            yield cp1

        mock_checkpointer.alist = alist

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_thread_turns

            result = await get_thread_turns("t1")

        assert len(result.turns) == 1
        assert result.turns[0].turn_index == 0
        assert result.turns[0].regenerate_checkpoint_id == "cp-1"
        assert result.retry_checkpoint_id == "cp-1"

    @pytest.mark.asyncio
    async def test_multiple_turns_with_loop_checkpoints(self):
        """Two input checkpoints with loop checkpoints in between."""
        # Newest first (as alist returns)
        cp4 = _make_cp_tuple("cp-4", parent_checkpoint_id="cp-3", source="loop")
        cp3 = _make_cp_tuple("cp-3", parent_checkpoint_id="cp-2", source="input")
        cp2 = _make_cp_tuple("cp-2", parent_checkpoint_id="cp-1", source="loop")
        cp1 = _make_cp_tuple("cp-1", source="input")

        mock_checkpointer = AsyncMock()

        async def alist(config):
            for cp in [cp4, cp3, cp2, cp1]:
                yield cp

        mock_checkpointer.alist = alist

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_thread_turns

            result = await get_thread_turns("t1")

        assert len(result.turns) == 2
        assert result.turns[0].turn_index == 0
        assert result.turns[0].regenerate_checkpoint_id == "cp-1"
        assert result.turns[1].turn_index == 1
        assert result.turns[1].regenerate_checkpoint_id == "cp-3"
        # edit_checkpoint_id for turn 1 is the parent of the input checkpoint
        assert result.turns[1].edit_checkpoint_id == "cp-2"
        assert result.retry_checkpoint_id == "cp-4"

    @pytest.mark.asyncio
    async def test_hitl_resume_detected_as_turn(self):
        """A checkpoint with __resume__ in pending_writes is treated as a turn boundary."""
        cp2 = _make_cp_tuple(
            "cp-2",
            parent_checkpoint_id="cp-1",
            source="loop",
            pending_writes=[("task-1", "__resume__", {"decisions": [{"type": "approve"}]})],
        )
        cp1 = _make_cp_tuple("cp-1", source="input")

        mock_checkpointer = AsyncMock()

        async def alist(config):
            for cp in [cp2, cp1]:
                yield cp

        mock_checkpointer.alist = alist

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_thread_turns

            result = await get_thread_turns("t1")

        # Two turns: source=input at cp-1 and HITL resume at cp-2
        assert len(result.turns) == 2
        assert result.turns[1].regenerate_checkpoint_id == "cp-2"
        # HITL resume turns have no edit_checkpoint_id (only source=input turns do)
        assert result.turns[1].edit_checkpoint_id is None

    @pytest.mark.asyncio
    async def test_checkpointer_error_raises_500(self):
        mock_checkpointer = AsyncMock()

        async def alist_error(config):
            raise RuntimeError("DB connection failed")
            yield  # noqa

        mock_checkpointer.alist = alist_error

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_thread_turns

            with pytest.raises(HTTPException) as exc_info:
                await get_thread_turns("t1")
            assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_branch_tip_checkpoint_id_parameter(self):
        """When branch_tip_checkpoint_id is provided, it is used as the branch tip."""
        cp3 = _make_cp_tuple("cp-3", parent_checkpoint_id="cp-2", source="loop")
        cp2 = _make_cp_tuple("cp-2", parent_checkpoint_id="cp-1", source="input")
        cp1 = _make_cp_tuple("cp-1", source="input")

        mock_checkpointer = AsyncMock()

        async def alist(config):
            for cp in [cp3, cp2, cp1]:
                yield cp

        mock_checkpointer.alist = alist

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_thread_turns

            # Use cp-2 as branch tip (skips cp-3)
            result = await get_thread_turns("t1", branch_tip_checkpoint_id="cp-2")

        # retry_checkpoint_id should be the branch tip, not the newest
        assert result.retry_checkpoint_id == "cp-2"


class TestTurnNumbers:
    """``turn_index`` is the turn's persisted number, which the client matches
    against the turn its bubble replayed under."""

    @pytest.mark.asyncio
    async def test_a_turn_with_no_checkpoint_keeps_its_number(self, persisted_turns):
        persisted_turns.return_value = [0, 1, 2, 3]

        result = await _turns(_checkpointer(*_DEAD_TURN_THEN_RESUME))

        assert [(t.turn_index, t.regenerate_checkpoint_id) for t in result.turns] == [
            (0, "cp-1"),
            (2, "cp-4"),
            (3, "cp-5"),
        ]
        assert result.turns[1].edit_checkpoint_id == "cp-3"
        assert result.turns[2].edit_checkpoint_id is None

    @pytest.mark.asyncio
    async def test_unstamped_turns_take_the_persisted_turns_in_order(
        self, persisted_turns
    ):
        """Threads from before the stamp: each boundary names the next
        persisted turn, and one with none left to name is not offered."""
        persisted_turns.return_value = [0, 1]
        cp3 = _make_cp_tuple("cp-3", "cp-2", source="input")
        cp2 = _make_cp_tuple("cp-2", "cp-1", source="input")
        cp1 = _make_cp_tuple("cp-1", source="input")

        result = await _turns(_checkpointer(cp3, cp2, cp1))

        assert [(t.turn_index, t.regenerate_checkpoint_id) for t in result.turns] == [
            (0, "cp-1"),
            (1, "cp-2"),
        ]

    @pytest.mark.asyncio
    async def test_a_thread_with_no_boundary_reads_no_rows(self, persisted_turns):
        result = await _turns(_checkpointer())

        assert result.turns == []
        persisted_turns.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_row_read_fails_the_listing(self, persisted_turns):
        persisted_turns.side_effect = RuntimeError("db down")

        with pytest.raises(HTTPException) as exc_info:
            await _turns(_checkpointer(*_DEAD_TURN_THEN_RESUME))
        assert exc_info.value.status_code == 500


class TestResolveForkTurn:
    """The turn a fork replaces comes from the checkpoint it forks at."""

    async def _resolve(self, checkpoint_id, *, regenerate, requested=None):
        from src.server.handlers.checkpoint_handler import resolve_fork_turn

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=_checkpointer(*_DEAD_TURN_THEN_RESUME),
            ),
            patch(
                "src.server.handlers.checkpoint_handler.threads_read.get_thread_checkpoint_id",
                new=AsyncMock(return_value="cp-6"),
            ),
        ):
            return await resolve_fork_turn(
                "t1", checkpoint_id, regenerate=regenerate, requested=requested
            )

    @pytest.mark.asyncio
    async def test_an_edit_replaces_the_turn_after_its_checkpoint(self, persisted_turns):
        persisted_turns.return_value = [0, 1, 2, 3]

        assert await self._resolve("cp-3", regenerate=False) == 2

    @pytest.mark.asyncio
    async def test_an_edit_may_name_a_failed_turn_before_the_forked_one(
        self, persisted_turns
    ):
        """Turn 1 never checkpointed, so editing its message forks where
        turn 2 forks; the client's number decides between the two, and any
        number that would delete kept turn 0 or keep turn 2 does not."""
        persisted_turns.return_value = [0, 1, 2, 3]

        assert await self._resolve("cp-3", regenerate=False, requested=1) == 1
        assert await self._resolve("cp-3", regenerate=False, requested=0) == 2
        assert await self._resolve("cp-3", regenerate=False, requested=3) == 2

    @pytest.mark.asyncio
    async def test_a_regenerate_replaces_the_turn_at_its_checkpoint(
        self, persisted_turns
    ):
        persisted_turns.return_value = [0, 1, 2, 3]

        assert await self._resolve("cp-4", regenerate=True) == 2
        assert await self._resolve("cp-5", regenerate=True) == 3
        # A regenerate keeps its turn's question, so no other turn will do.
        assert await self._resolve("cp-4", regenerate=True, requested=1) == 2

    @pytest.mark.asyncio
    async def test_a_checkpoint_that_forks_no_turn_is_refused(self, persisted_turns):
        persisted_turns.return_value = [0, 1, 2, 3]

        for checkpoint_id, regenerate in (
            ("cp-4", False),  # a boundary is no edit point
            ("cp-3", True),  # an edit point is no boundary
            ("cp-off-branch", False),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await self._resolve(checkpoint_id, regenerate=regenerate)
            assert exc_info.value.status_code == 409
            assert exc_info.value.detail["code"] == "stale_fork"


# ---------------------------------------------------------------------------
# get_retry_checkpoint
# ---------------------------------------------------------------------------


class TestGetRetryCheckpoint:
    """Tests for get_retry_checkpoint."""

    @pytest.mark.asyncio
    async def test_explicit_checkpoint_id_validated(self):
        mock_checkpointer = AsyncMock()
        mock_checkpointer.aget_tuple = AsyncMock(
            return_value=_make_cp_tuple("cp-42")
        )

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1", "checkpoint_id": "cp-42"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_retry_checkpoint

            result = await get_retry_checkpoint("t1", checkpoint_id="cp-42")

        assert result == "cp-42"

    @pytest.mark.asyncio
    async def test_explicit_checkpoint_id_not_found_raises_404(self):
        mock_checkpointer = AsyncMock()
        mock_checkpointer.aget_tuple = AsyncMock(return_value=None)

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1", "checkpoint_id": "missing"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_retry_checkpoint

            with pytest.raises(HTTPException) as exc_info:
                await get_retry_checkpoint("t1", checkpoint_id="missing")
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_auto_detect_returns_latest(self):
        mock_checkpointer = AsyncMock()
        mock_checkpointer.aget_tuple = AsyncMock(
            return_value=_make_cp_tuple("cp-latest")
        )

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_retry_checkpoint

            result = await get_retry_checkpoint("t1")

        assert result == "cp-latest"

    @pytest.mark.asyncio
    async def test_auto_detect_no_checkpoints_raises_404(self):
        mock_checkpointer = AsyncMock()
        mock_checkpointer.aget_tuple = AsyncMock(return_value=None)

        with (
            patch(
                "src.server.handlers.checkpoint_handler.get_checkpointer",
                return_value=mock_checkpointer,
            ),
            patch(
                "src.server.handlers.checkpoint_handler.build_checkpoint_config",
                return_value={"configurable": {"thread_id": "t1"}},
            ),
        ):
            from src.server.handlers.checkpoint_handler import get_retry_checkpoint

            with pytest.raises(HTTPException) as exc_info:
                await get_retry_checkpoint("t1")
            assert exc_info.value.status_code == 404
