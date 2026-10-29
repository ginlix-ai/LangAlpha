"""POST /api/v1/threads with a title the caller names.

A thread is born titled: by default with the stamp of its first message, which
the title generator then replaces. A caller may name the title instead, for a
first turn that opens with no message of the user's (the first query is empty,
or is not the user's words): the row stores that title and no generation runs,
so nothing written for the model can become the thread's name.
"""

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from src.server.models.conversation import ThreadCreateRequest
from tests.unit.server.app.test_threads_subagents_allowed import _request, _row

CREATE_MOD = "src.server.app.threads.create"


async def _create(body):
    from src.server.services.turn_runtime import TurnRoute

    with (
        patch(f"{CREATE_MOD}.HOST_MODE", "oss"),
        patch(
            f"{CREATE_MOD}.requested_workspace",
            new=AsyncMock(return_value={"workspace_id": "ws-1", "status": "active"}),
        ),
        patch(f"{CREATE_MOD}.turn_route", return_value=TurnRoute("ws-1", "analyst")),
        patch(
            f"{CREATE_MOD}.create_thread", new=AsyncMock(return_value=_row())
        ) as create,
        patch(
            f"{CREATE_MOD}.schedule_title_generation", return_value=None
        ) as generate,
    ):
        resp = await _request("POST", "/api/v1/threads", body)
    return resp, create, generate


@pytest.mark.asyncio
async def test_a_named_title_is_stored_and_nothing_generates_over_it():
    resp, create, generate = await _create({"workspace_id": "ws-1", "title": "Getting started"})

    assert resp.status_code == 201
    assert create.await_args.kwargs["title"] == "Getting started"
    assert resp.json()["title"] == "Getting started"
    generate.assert_not_called()


@pytest.mark.asyncio
async def test_a_named_title_wins_over_the_first_query():
    resp, create, generate = await _create(
        {"workspace_id": "ws-1", "first_query": "hello", "title": "Getting started"}
    )

    assert resp.status_code == 201
    assert create.await_args.kwargs["title"] == "Getting started"
    generate.assert_not_called()


@pytest.mark.asyncio
async def test_without_one_the_first_query_is_stamped_and_generated_from():
    resp, create, generate = await _create({"workspace_id": "ws-1", "first_query": "hello"})

    assert resp.status_code == 201
    assert create.await_args.kwargs["title"] == "hello"
    generate.assert_called_once()
    assert generate.call_args.kwargs["first_query"] == "hello"
    assert generate.call_args.kwargs["expected_title"] == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"workspace_id": "ws-1"},
        {"workspace_id": "ws-1", "first_query": ""},
        {"workspace_id": "ws-1", "first_query": "", "title": ""},
    ],
    ids=["neither", "blank-query", "blank-both"],
)
async def test_a_create_with_nothing_to_title_it_is_refused(body):
    resp, create, _ = await _create(body)

    assert resp.status_code == 422
    create.assert_not_awaited()


def test_a_named_title_is_capped_like_a_rename():
    with pytest.raises(ValidationError):
        ThreadCreateRequest(title="x" * 256)
    assert ThreadCreateRequest(title="x" * 255).first_query == ""
