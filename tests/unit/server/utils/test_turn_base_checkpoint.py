"""A turn's base checkpoint fails closed.

After a fork whose run never started, the newest checkpoint is the replaced
branch's; only the reads tell, so a read that fails must stop the turn rather
than leave it to start there and write those turns back.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest

HELPERS = "src.server.utils.checkpoint_helpers"
BRANCH_READ = "src.server.database.conversation.threads_read.get_thread_branch_read"
PINNED = "src.server.database.runs.lifecycle.pointer_pinned_by_fork"
ABANDONED_RUN = "5b0f3f7e-9a51-4c3e-8d7a-2f6c1e0b9a44"


def _dropped():
    return psycopg.OperationalError("server closed the connection unexpectedly")


@asynccontextmanager
async def _dropped_connection():
    raise _dropped()
    yield


def _pointer(checkpoint_id="cp-pointer"):
    from src.server.database.conversation.threads_read import ThreadBranchRead

    return AsyncMock(return_value=ThreadBranchRead(checkpoint_id, 3))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "step_run_id", [ABANDONED_RUN, None], ids=["stamped", "unstamped"]
)
async def test_a_newest_step_a_fork_replaced_starts_from_the_pointer(step_run_id):
    """A step from before runs were stamped names no run; the fork's pin
    decides for it as for one whose run the fork deleted."""
    from src.server.utils.checkpoint_helpers import turn_base_checkpoint_id

    pinned = AsyncMock(return_value=True)
    with (
        patch(BRANCH_READ, _pointer()),
        patch(
            f"{HELPERS}._newest_step",
            AsyncMock(return_value=(False, step_run_id, True)),
        ),
        patch(PINNED, pinned),
    ):
        assert await turn_base_checkpoint_id(MagicMock(), "t-1") == "cp-pointer"
    pinned.assert_awaited_once_with("t-1", "cp-pointer", step_run_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "newest_step, pinned",
    [
        ((True, None, True), AsyncMock(side_effect=AssertionError("read"))),
        ((False, None, True), AsyncMock(return_value=False)),
        ((False, ABANDONED_RUN, False), AsyncMock(return_value=True)),
    ],
    ids=["on-the-pointer", "no-fork-pin", "pointer-gone"],
)
async def test_the_newest_checkpoint_stands_unless_a_fork_replaced_it(
    newest_step, pinned
):
    from src.server.utils.checkpoint_helpers import turn_base_checkpoint_id

    with (
        patch(BRANCH_READ, _pointer()),
        patch(f"{HELPERS}._newest_step", AsyncMock(return_value=newest_step)),
        patch(PINNED, pinned),
    ):
        assert await turn_base_checkpoint_id(MagicMock(), "t-1") is None


@pytest.mark.asyncio
async def test_a_failed_pointer_read_raises():
    from src.server.utils.checkpoint_helpers import turn_base_checkpoint_id

    with patch("src.server.database.pool.get_db_connection", _dropped_connection):
        with pytest.raises(psycopg.OperationalError):
            await turn_base_checkpoint_id(MagicMock(), "t-1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "newest_step, pinned",
    [
        (AsyncMock(side_effect=_dropped()), AsyncMock(return_value=True)),
        (
            AsyncMock(return_value=(False, None, True)),
            AsyncMock(side_effect=_dropped()),
        ),
    ],
    ids=["newest-step", "fork-pin"],
)
async def test_a_failed_branch_read_raises(newest_step, pinned):
    from src.server.utils.checkpoint_helpers import turn_base_checkpoint_id

    with (
        patch(BRANCH_READ, _pointer()),
        patch(f"{HELPERS}._newest_step", newest_step),
        patch(PINNED, pinned),
    ):
        with pytest.raises(psycopg.OperationalError):
            await turn_base_checkpoint_id(MagicMock(), "t-1")
