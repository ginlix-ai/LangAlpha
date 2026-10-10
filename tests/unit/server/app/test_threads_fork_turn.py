"""The send route settles a fork's turn from its checkpoint before any run.

Every reader after the route (the truncation, the regenerate's turn, the
prior-turn read) takes the turn from the request, so the route is where the
client's count gives way to the branch.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from tests.unit.server.app.test_threads_authz import (
    CALLER,
    _app_for,
    _empty_async_gen,
    _stub_downstream,
)

_RESOLVE = "src.server.handlers.checkpoint_handler.resolve_fork_turn"


async def _post(body):
    app = _app_for(CALLER)
    workflow = MagicMock(return_value=_empty_async_gen())
    with (
        _stub_downstream(owner_id=CALLER),
        patch("src.server.handlers.chat.astream_ptc_workflow", workflow),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/v1/threads/tid-mine/messages",
                json={"workspace_id": "ws-placeholder", "agent_mode": "ptc", **body},
            )
    return resp, workflow


@pytest.mark.asyncio
async def test_an_edit_runs_as_the_turn_its_checkpoint_forks():
    resolve = AsyncMock(return_value=3)
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {
                "messages": [{"role": "user", "content": "edited"}],
                "checkpoint_id": "cp-before-3",
                "fork_from_turn": 2,
            }
        )

    assert resp.status_code == 200
    resolve.assert_awaited_once_with(
        "tid-mine", "cp-before-3", regenerate=False, requested=2
    )
    assert workflow.call_args.kwargs["request"].fork_from_turn == 3


@pytest.mark.asyncio
async def test_a_regenerate_is_resolved_at_its_boundary():
    resolve = AsyncMock(return_value=3)
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {"messages": [], "checkpoint_id": "cp-3", "fork_from_turn": 3}
        )

    assert resp.status_code == 200
    resolve.assert_awaited_once_with(
        "tid-mine", "cp-3", regenerate=True, requested=3
    )
    assert workflow.call_args.kwargs["request"].fork_from_turn == 3


@pytest.mark.asyncio
async def test_a_fork_at_no_turn_of_the_branch_starts_nothing():
    resolve = AsyncMock(
        side_effect=HTTPException(status_code=409, detail={"code": "stale_fork"})
    )
    with patch(_RESOLVE, resolve):
        resp, workflow = await _post(
            {"messages": [], "checkpoint_id": "cp-gone", "fork_from_turn": 1}
        )

    assert resp.status_code == 409
    workflow.assert_not_called()


@pytest.mark.asyncio
async def test_a_plain_send_reads_no_branch():
    resolve = AsyncMock()
    with patch(_RESOLVE, resolve):
        resp, _ = await _post({"messages": [{"role": "user", "content": "hi"}]})

    assert resp.status_code == 200
    resolve.assert_not_awaited()
