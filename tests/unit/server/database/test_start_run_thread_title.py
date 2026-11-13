"""START hands back the thread's title from the projection write it makes
anyway, so a background run's start notice names the thread without a read
of its own."""

from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.runs.lifecycle import start_run

THREAD_ID = "11111111-1111-1111-1111-111111111111"
RUN_ID = "22222222-2222-2222-2222-222222222222"


@pytest.fixture(autouse=True)
def _no_move():
    with patch(
        "src.server.database.workspace.workspace_is_moving",
        new=AsyncMock(return_value=False),
    ):
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "thread, title",
    [({"title": "NVDA after earnings"}, "NVDA after earnings"), (None, None)],
    ids=["named", "thread_gone"],
)
async def test_start_returns_the_thread_title(mock_connection, mock_cursor, thread, title):
    mock_cursor.fetchone.side_effect = [
        {"workspace_id": "ws-1", "status": "running"},  # the workspace lock
        None,  # no run under this request key
        {"conversation_response_id": RUN_ID, "run_seq": 7},  # the run row
        thread,  # the projection write
    ]

    row = await start_run(
        run_id=RUN_ID,
        thread_id=THREAD_ID,
        request_key="req-1",
        turn_index=0,
        conn=mock_connection,
    )

    assert row == {"conversation_response_id": RUN_ID, "run_seq": 7, "thread_title": title}
    projection = mock_cursor.execute.await_args_list[-1].args[0]
    assert "SET current_status = 'in_progress'" in projection
    assert "RETURNING title" in projection
