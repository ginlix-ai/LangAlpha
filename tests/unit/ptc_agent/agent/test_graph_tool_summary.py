"""Per-turn read path: the session-cached MCP tool summary is plumbed into
create_agent and reused byte-stable across turns (no per-turn recompute).

Regression #6 at the session-cache layer: two consecutive turns of the same
session pass the IDENTICAL cached summary string into create_agent — the hot
path never re-resolves or recomputes, keeping the prompt-cache prefix warm.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.graph import build_ptc_graph_with_session


def _make_session(summary):
    session = MagicMock()
    session.conversation_id = "ws-1"
    session.sandbox = MagicMock()
    session.sandbox.vault_secrets = None
    session.mcp_registry = MagicMock()
    session.mcp_tool_summary = summary
    session.invalidate_agent_md = MagicMock()
    return session


@pytest.mark.asyncio
async def test_session_summary_passed_to_create_agent():
    session = _make_session("CACHED-SUMMARY")
    config = MagicMock()
    config.subagents = MagicMock(enabled=[])

    fake_agent = MagicMock()
    fake_agent.create_agent = MagicMock(return_value="GRAPH")

    with patch(
        "ptc_agent.agent.graph.PTCAgent", return_value=fake_agent
    ), patch(
        "ptc_agent.agent.graph.fetch_user_data_counts",
        new=AsyncMock(return_value=None),
    ):
        await build_ptc_graph_with_session(session=session, config=config)

    kwargs = fake_agent.create_agent.call_args.kwargs
    assert kwargs["tool_summary"] == "CACHED-SUMMARY"


@pytest.mark.asyncio
async def test_two_turns_pass_identical_cached_summary():
    """The cached summary string is identical across consecutive turns —
    the per-turn path reads it, never recomputes it (prompt-cache stays warm)."""
    session = _make_session("STABLE-SUMMARY")
    config = MagicMock()
    config.subagents = MagicMock(enabled=[])

    summaries = []
    fake_agent = MagicMock()

    def capture(**kwargs):
        summaries.append(kwargs["tool_summary"])
        return "GRAPH"

    fake_agent.create_agent = MagicMock(side_effect=capture)

    with patch(
        "ptc_agent.agent.graph.PTCAgent", return_value=fake_agent
    ), patch(
        "ptc_agent.agent.graph.fetch_user_data_counts",
        new=AsyncMock(return_value=None),
    ):
        await build_ptc_graph_with_session(session=session, config=config)
        await build_ptc_graph_with_session(session=session, config=config)

    assert summaries == ["STABLE-SUMMARY", "STABLE-SUMMARY"]
    assert summaries[0] is summaries[1]
