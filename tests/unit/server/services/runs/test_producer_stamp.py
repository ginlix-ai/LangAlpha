"""START names a turn's producer, so no caller has to.

Every turn runs the full agent, so the run row and the query row it writes are
stamped 'ptc' at START, and nothing a later step writes over the run's
metadata may carry the key: finalize merges its outcome onto the row, and a
missing value there would land as null over the stamp.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.runs.lifecycle import QuerySpec
from src.server.services import writer_guard as wg_module
from src.server.services.runs.coordinator import RunCoordinator
from src.server.services.runs.finalization import assemble_finalize_artifacts

_ROW = {
    "turn_index": 0,
    "attempt_no": 1,
    "created_at": datetime.now(timezone.utc),
    "run_seq": 1,
}


async def _start(**kwargs):
    with (
        patch(f"{wg_module.__name__}.guard_enabled", return_value=False),
        patch(
            "src.server.database.runs.lifecycle.start_run",
            new=AsyncMock(return_value=_ROW),
        ) as start,
        patch(
            "src.server.services.thread_control_stream.announce_run_started",
            new=AsyncMock(),
        ),
        patch(
            "src.server.services.thread_lifecycle_feed.publish_run_started",
            new=AsyncMock(),
        ),
    ):
        await RunCoordinator().start_run(thread_id="t-1", run_id="r-1", **kwargs)
    return start.await_args.kwargs


@pytest.mark.asyncio
async def test_start_stamps_the_run_and_its_query_row():
    query = QuerySpec(
        query_id="q-1",
        content="hello",
        query_type="initial",
        metadata={"workspace_id": "ws-1", "llm_model": "m-1"},
    )

    written = await _start(query=query, run_metadata={"locale": "en"})

    assert written["metadata"]["msg_type"] == "ptc"
    assert written["query"].metadata == {
        "msg_type": "ptc",
        "workspace_id": "ws-1",
        "llm_model": "m-1",
    }
    # The caller's spec is left as it was handed over.
    assert "msg_type" not in query.metadata


@pytest.mark.asyncio
async def test_a_retry_writes_no_query_row():
    written = await _start(query=None)

    assert written["query"] is None
    assert written["metadata"]["msg_type"] == "ptc"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "error", "cancelled", "interrupted"])
async def test_a_finalize_keeps_the_start_stamp(status):
    outcome = await assemble_finalize_artifacts(
        ("t-1", "r-1"),
        metadata={"workspace_id": "ws-1", "is_byok": False},
        status=status,
        phase="completion",
        interrupt_reason=None,
        pause_message=None,
        error=None,
        handler=None,
        cancelled_by_user=False,
        workspace_id="ws-1",
        user_id="u-1",
    )

    # finalize writes ``metadata || outcome.metadata`` onto the START row, so
    # the key must be absent, not null.
    assert "msg_type" not in outcome.metadata
