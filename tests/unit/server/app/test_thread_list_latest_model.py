"""``GET /api/v1/threads`` names the model each thread's latest turn ran on.

``llm_model`` is empty for a thread that follows the account default, so a
listing (``la threads list``) reads the model from the latest run instead.
"""

from unittest.mock import AsyncMock, patch

import pytest

from tests.unit.server.app.test_threads_subagents_allowed import THREADS_MOD, _request, _row


@pytest.mark.asyncio
async def test_list_items_name_the_model_the_latest_turn_ran_on():
    rows = [
        _row(conversation_thread_id="t-ran", latest_run_seq=2, latest_run_model="claude-opus-5-5"),
        _row(conversation_thread_id="t-new", latest_run_seq=0, latest_run_model=None),
    ]
    with patch(f"{THREADS_MOD}.get_threads_for_user", new=AsyncMock(return_value=(rows, 2))):
        resp = await _request("GET", "/api/v1/threads")

    assert resp.status_code == 200
    by_id = {t["thread_id"]: t["latest_run_model"] for t in resp.json()["threads"]}
    assert by_id == {"t-ran": "claude-opus-5-5", "t-new": None}
