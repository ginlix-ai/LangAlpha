"""Session.get_agent_md fault tolerance: sandbox read failures must not kill the turn."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.ptc_agent.core.session import Session


def _make_sandbox(read_result=None, read_side_effect=None):
    sandbox = MagicMock()
    sandbox.normalize_path = MagicMock(side_effect=lambda p: f"/workspace/{p}")
    sandbox.aread_file_text = AsyncMock(
        return_value=read_result, side_effect=read_side_effect
    )
    return sandbox


def _make_session(sandbox):
    config = MagicMock()
    config.mcp.servers = []
    session = Session("test-conversation", config)
    session.sandbox = sandbox
    return session


class TestGetAgentMd:
    @pytest.mark.asyncio
    async def test_missing_file_returns_none(self):
        session = _make_session(_make_sandbox(read_result=None))
        assert await session.get_agent_md() is None

    @pytest.mark.asyncio
    async def test_transient_error_returns_none_and_recovers(self):
        failing = _make_sandbox(read_side_effect=SandboxTransientError("boom"))
        session = _make_session(failing)
        assert await session.get_agent_md() is None

        # After the transport recovers, an invalidation must allow a re-read.
        session.invalidate_agent_md()
        session.sandbox = _make_sandbox(read_result="# agent.md")
        assert await session.get_agent_md() == "# agent.md"

    @pytest.mark.asyncio
    async def test_content_cached_until_invalidated(self):
        sandbox = _make_sandbox(read_result="# agent.md")
        session = _make_session(sandbox)

        assert await session.get_agent_md() == "# agent.md"
        assert await session.get_agent_md() == "# agent.md"
        assert sandbox.aread_file_text.await_count == 1

        session.invalidate_agent_md()
        assert await session.get_agent_md() == "# agent.md"
        assert sandbox.aread_file_text.await_count == 2
