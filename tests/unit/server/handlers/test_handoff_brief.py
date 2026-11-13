"""Which turns a hand-off brief is written for, and when a kept one is
reused rather than paid for again."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.compaction.brief import BRIEF_KEY
from src.llms.claude_code import MESSAGE_ID_KEY
from src.server.handlers import handoff_brief
from src.server.handlers.handoff_brief import brief_kept_at_head
from src.server.services.llm import thread_model

USER = "u-1"


@pytest.mark.asyncio
@pytest.mark.parametrize("workspace", ["ws-1", "home"])
async def test_no_turn_runs_claude_code_on_the_default_while_the_user_has_it_off(workspace):
    with (
        patch.object(thread_model.user_models, "get_model_preference", AsyncMock(return_value={})),
        patch.object(thread_model, "effective_default_model", return_value="sonnet-code"),
        patch("src.llms.claude_code.is_claude_code_model", return_value=True),
        patch("src.server.database.home_workspace.is_home_id", return_value=workspace == "home"),
        patch("src.server.services.claude_code.flow.rows_on", AsyncMock(return_value=False)),
    ):
        assert not await thread_model.next_turn_runs_claude_code(USER, workspace, None)


@pytest.mark.asyncio
async def test_regenerating_claudes_first_reply_reuses_the_brief_its_turn_kept():
    turn_start = [HumanMessage("a", id="h1"), AIMessage("b", id="other"), HumanMessage("c", id="h2")]
    brief = {"text": "brief", "through": "other", "source": "model"}
    claude = AIMessage("d", id="claude", response_metadata={MESSAGE_ID_KEY: "msg_claude"})
    head = {"messages": [*turn_start, claude], BRIEF_KEY: brief}
    graph = SimpleNamespace(aget_state=AsyncMock(return_value=SimpleNamespace(values=head)))

    assert await brief_kept_at_head(graph, "t-1", {"messages": turn_start}) == brief
    edited = [HumanMessage("a", id="h1"), AIMessage("b2", id="other-2"), HumanMessage("c", id="h3")]
    assert await brief_kept_at_head(graph, "t-1", {"messages": edited}) is None


@pytest.mark.asyncio
async def test_a_switch_made_back_off_claude_code_while_the_brief_waited_writes_none():
    writers = AsyncMock()
    with (
        patch(
            "src.server.services.thread_agent.thread_route",
            AsyncMock(return_value=({"workspace_id": "ws-1", "llm_model": "gpt-6.1-terra"}, None)),
        ),
        patch.object(thread_model, "next_turn_runs_claude_code", AsyncMock(return_value=False)) as runs,
        patch("src.server.handlers.thread_maintenance._resolver", writers),
    ):
        await handoff_brief._write_held("t-1", USER, "sonnet-code", mutation=None)
    runs.assert_awaited_once_with(USER, "ws-1", "gpt-6.1-terra")
    writers.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_brief_for_the_account_default_is_fitted_to_that_defaults_threshold():
    compaction = SimpleNamespace(enabled=True, token_threshold=100_000)
    resolve = AsyncMock(return_value=SimpleNamespace(compaction=compaction))
    assert await handoff_brief._reader_room(resolve, None) == 50_000
    resolve.assert_awaited_once_with(None)
