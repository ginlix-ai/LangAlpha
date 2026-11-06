"""fail_open_run on a run whose row is gone: nothing holds the thread, so
there is nothing to settle and nothing to report as a held slot."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from src.server.database.runs.lifecycle import RunNotFoundError
from src.server.services.runs.coordinator import RunCoordinator, RunHandle


@pytest.mark.asyncio
async def test_a_run_with_no_row_is_nothing_to_settle(caplog):
    coordinator = RunCoordinator()
    coordinator.finalize_run = AsyncMock(side_effect=RunNotFoundError("run-1"))
    handle = RunHandle(run_id="run-1", thread_id="thread-1", turn_index=0, attempt_no=1)

    with caplog.at_level(logging.INFO, logger="src.server.services.runs.coordinator"):
        assert await coordinator.fail_open_run(handle, "worker task died") is None
        await asyncio.sleep(0)

    assert [r.levelno for r in caplog.records] == [logging.INFO]
    assert "nothing to settle" in caplog.records[0].getMessage()
