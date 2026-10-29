"""The chat's whole-thread token totals, read from the rows the usage writer stores."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _call(model: str, input_tokens: int, output_tokens: int) -> dict:
    """One model call, as the per-call tracker records it."""
    return {
        "model_name": model,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
        "run_id": str(uuid.uuid4()),
    }


async def _write(thread_id, workspace_id, user_id, response_id, calls, **kw):
    """A usage row through the real writer, so the read sees the stored shape."""
    from src.server.services.persistence.usage import UsagePersistenceService

    usage = UsagePersistenceService(
        thread_id=thread_id, workspace_id=workspace_id, user_id=user_id
    )
    if calls is not None:
        await usage.track_llm_usage(calls)
    else:
        # A run that only used tools stores no token payload at all.
        usage.record_tool_usage("WebSearchTool")
    assert await usage.persist_usage(response_id=response_id, **kw)


@pytest_asyncio.fixture
async def thread(seed_workspace, patched_get_db_connection):
    from src.server.database.conversation import create_thread

    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )
    return thread_id, str(seed_workspace["workspace_id"]), seed_workspace["user_id"]


async def test_totals_sum_every_usage_row_of_the_thread(thread):
    from src.server.database.conversation.usage import get_thread_token_totals

    thread_id, ws_id, user_id = thread
    run_1, run_cut, run_2 = (str(uuid.uuid4()) for _ in range(3))
    # Two models in one run, a subagent's row under it, an attempt an edit
    # later cut, a tool-only run and a second turn.
    await _write(
        thread_id, ws_id, user_id, run_1,
        [_call("model-a", 1000, 50), _call("model-a", 1200, 70), _call("model-b", 300, 9)],
    )
    await _write(
        thread_id, ws_id, user_id, run_1, [_call("model-b", 400, 20)], msg_type="task"
    )
    await _write(
        thread_id, ws_id, user_id, run_cut, [_call("model-a", 2000, 5)], status="error"
    )
    await _write(thread_id, ws_id, user_id, str(uuid.uuid4()), None)
    await _write(thread_id, ws_id, user_id, run_2, [_call("model-a", 5000, 300)])
    # Another thread's row counts for that thread only.
    await _write(str(uuid.uuid4()), ws_id, user_id, str(uuid.uuid4()), [_call("model-a", 7, 7)])

    totals = await get_thread_token_totals(thread_id)

    assert totals == {
        "input_tokens": 1000 + 1200 + 300 + 400 + 2000 + 5000,
        "output_tokens": 50 + 70 + 9 + 20 + 5 + 300,
    }


async def test_totals_leave_out_the_live_runs_own_row(thread):
    """A row the live run's finalize commits mid-read stays out, while the
    subagent rows filed under it count: the client streams only the former."""
    from src.server.database.conversation.usage import get_thread_token_totals

    thread_id, ws_id, user_id = thread
    settled, live = str(uuid.uuid4()), str(uuid.uuid4())
    await _write(thread_id, ws_id, user_id, settled, [_call("model-a", 1000, 10)])
    await _write(thread_id, ws_id, user_id, live, [_call("model-a", 400, 4)])
    await _write(thread_id, ws_id, user_id, live, [_call("model-b", 60, 6)], msg_type="task")

    assert await get_thread_token_totals(thread_id, live_run_id=live) == {
        "input_tokens": 1060,
        "output_tokens": 16,
    }
    assert await get_thread_token_totals(thread_id) == {
        "input_tokens": 1460,
        "output_tokens": 20,
    }


async def test_a_thread_without_usage_rows_reads_zero(thread):
    from src.server.database.conversation.usage import get_thread_token_totals

    thread_id, _, _ = thread
    assert await get_thread_token_totals(thread_id) == {
        "input_tokens": 0,
        "output_tokens": 0,
    }
