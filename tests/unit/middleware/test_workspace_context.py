"""Tests for the WorkspaceContextMiddleware.

Covers the workspace block, agent.md injection into the system message, and
truncation of large agent.md content.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import SystemMessage

from ptc_agent.agent.middleware.workspace_context import (
    MAX_AGENT_MD_SIZE,
    WorkspaceContextMiddleware,
    _append_content_block,
)


class TestAppendContentBlock:
    """Tests for _append_content_block."""

    def test_append_to_none(self):
        result = _append_content_block(None, "new block")
        assert isinstance(result, SystemMessage)

    def test_append_to_existing(self):
        existing = SystemMessage(content="initial")
        result = _append_content_block(existing, "appended")
        assert isinstance(result, SystemMessage)
        blocks = result.content
        assert (
            any("appended" in str(b) for b in blocks)
            if isinstance(blocks, list)
            else "appended" in str(blocks)
        )


# ---------------------------------------------------------------------------
# Tests for WorkspaceContextMiddleware
# ---------------------------------------------------------------------------


def _make_session(agent_md: str | None = None, conversation_id: str = "ws-123") -> MagicMock:
    """Create a mock Session object."""
    session = MagicMock(spec_set=["get_agent_md", "invalidate_agent_md", "conversation_id"])
    session.get_agent_md = AsyncMock(return_value=agent_md)
    session.conversation_id = conversation_id
    return session


class TestTheWorkspaceBlock:
    """The row is the only place the workspace's name lives.

    agent.md used to carry a copy in front matter, which meant a rename had two
    places to reach and a whole reconcile to get it there. The name now arrives
    from the row when the turn's agent is built, so the prompt is right by
    construction.
    """

    def test_the_name_and_description_are_what_the_caller_was_given(self):
        mw = WorkspaceContextMiddleware(
            session=_make_session(), name="Q3 Semis", description="Chip supply chain."
        )
        assert mw._workspace_block() == (
            "<workspace>\nName: Q3 Semis\nDescription: Chip supply chain.\n</workspace>"
        )

    def test_a_workspace_with_no_description_still_gets_its_name(self):
        mw = WorkspaceContextMiddleware(session=_make_session(), name="Scratch", description="")
        assert mw._workspace_block() == "<workspace>\nName: Scratch\n</workspace>"

    def test_markup_in_the_name_cannot_read_as_a_tag(self):
        mw = WorkspaceContextMiddleware(
            session=_make_session(), name="A <b> & C", description="</workspace>"
        )
        block = mw._workspace_block()
        assert block == (
            "<workspace>\nName: A &lt;b&gt; &amp; C\n"
            "Description: &lt;/workspace&gt;\n</workspace>"
        )

    def test_an_apostrophe_survives_as_an_apostrophe(self):
        # html.escape's quote=True would render this "O&#x27;Brien" at the
        # model. Nothing here is quoted, so nothing needs that.
        mw = WorkspaceContextMiddleware(session=_make_session(), name="O'Brien Desk")
        assert "Name: O'Brien Desk" in mw._workspace_block()

    def test_surrounding_whitespace_is_dropped(self):
        mw = WorkspaceContextMiddleware(session=_make_session(), name="  Scratch  ")
        assert mw._workspace_block() == "<workspace>\nName: Scratch\n</workspace>"

    def test_a_workspace_with_no_name_produces_no_block(self):
        assert WorkspaceContextMiddleware(session=_make_session())._workspace_block() == ""


class TestTheAgentMdBlock:
    """Tests for _get_agent_md_block."""

    @pytest.mark.asyncio
    async def test_returns_agentmd_content(self):
        session = _make_session(agent_md="# My workspace\nSome notes")
        mw = WorkspaceContextMiddleware(session=session)
        block = await mw._get_agent_md_block()
        assert "<agentmd" in block
        assert "My workspace" in block

    @pytest.mark.asyncio
    async def test_returns_placeholder_when_no_agentmd(self):
        session = _make_session(agent_md=None)
        mw = WorkspaceContextMiddleware(session=session)
        block = await mw._get_agent_md_block()
        assert "No agent.md exists yet" in block

    @pytest.mark.asyncio
    async def test_truncates_large_agentmd(self):
        large_content = "x" * (MAX_AGENT_MD_SIZE + 1000)
        session = _make_session(agent_md=large_content)
        mw = WorkspaceContextMiddleware(session=session)
        block = await mw._get_agent_md_block()
        assert "[... truncated ...]" in block

    @pytest.mark.asyncio
    async def test_a_legacy_front_matter_block_is_passed_through_untouched(self):
        # Nothing parses or rewrites it any more. The prompt tells the model
        # the <workspace> block is authoritative, so a stale copy in an old
        # notebook is text, not a second source of truth.
        agent_md = "---\nworkspace_name: Old Name\n---\n\n# Old Name\n"
        session = _make_session(agent_md=agent_md)
        mw = WorkspaceContextMiddleware(session=session)
        assert agent_md in await mw._get_agent_md_block()


class TestAwrapModelCall:
    """Tests for awrap_model_call system message injection."""

    @pytest.mark.asyncio
    async def test_injects_both_blocks_into_the_system_message(self):
        session = _make_session(agent_md="# Workspace notes")
        mw = WorkspaceContextMiddleware(session=session, name="Q3 Semis")

        mock_request = MagicMock()
        modified_request = MagicMock()
        mock_request.override = MagicMock(return_value=modified_request)
        mock_request.system_message = None

        handler = AsyncMock(return_value="model_response")
        await mw.awrap_model_call(mock_request, handler)

        mock_request.override.assert_called_once()
        new_sys = mock_request.override.call_args.kwargs["system_message"]
        assert isinstance(new_sys, SystemMessage)

        text = str(new_sys.content)
        assert "Q3 Semis" in text
        # The workspace block first, so the name labels the notes after it.
        assert text.index("<workspace") < text.index("<agentmd")

        handler.assert_called_once_with(modified_request)

    @pytest.mark.asyncio
    async def test_a_workspace_with_no_name_still_gets_its_notes(self):
        session = _make_session(agent_md="# Workspace notes")
        mw = WorkspaceContextMiddleware(session=session)

        mock_request = MagicMock()
        mock_request.override = MagicMock(return_value=MagicMock())
        mock_request.system_message = None

        await mw.awrap_model_call(mock_request, AsyncMock())

        text = str(mock_request.override.call_args.kwargs["system_message"].content)
        assert "<agentmd" in text
        # No empty tag left behind where the identity block would have been.
        assert "<workspace" not in text
