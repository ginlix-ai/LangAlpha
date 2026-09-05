"""Session's agent.md cache must not turn a failed read into "no notes".

``get_agent_md`` used to cache a read failure as None and clear its dirty flag,
so one unanswered sandbox call told the rest of the turn the workspace had no
agent.md at all — and the prompt then invites the agent to create a file that
already exists.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.session import Session

AGENT_MD = "---\nworkspace_name: Scratch\n---\n\n# Scratch\n"


def _make_session() -> Session:
    config = MagicMock()
    config.mcp = MagicMock()
    config.mcp.servers = []
    session = Session("ws-1", config)
    session.sandbox = MagicMock()
    session.sandbox.normalize_path = lambda p: f"/work/{p}"
    return session


class TestAFailedReadKeepsTheLastGoodContent:
    @pytest.mark.asyncio
    async def test_a_read_failure_does_not_cache_absence(self):
        session = _make_session()
        session.sandbox.aread_file_text = AsyncMock(return_value=AGENT_MD)
        assert await session.get_agent_md() == AGENT_MD

        session.invalidate_agent_md()
        session.sandbox.aread_file_text = AsyncMock(side_effect=RuntimeError("gone"))

        # The previous content stands rather than becoming "no agent.md".
        assert await session.get_agent_md() == AGENT_MD

    @pytest.mark.asyncio
    async def test_a_read_failure_stays_dirty_so_the_next_call_retries(self):
        session = _make_session()
        session.sandbox.aread_file_text = AsyncMock(side_effect=RuntimeError("gone"))
        assert await session.get_agent_md() is None

        session.sandbox.aread_file_text = AsyncMock(return_value=AGENT_MD)
        # No explicit invalidate: the failure must not have cleared the flag.
        assert await session.get_agent_md() == AGENT_MD

    @pytest.mark.asyncio
    async def test_a_genuinely_absent_file_is_still_cached(self):
        # aread_file_text returns None (not raises) when the file is missing;
        # that answer is real and must not force a re-read every model call.
        session = _make_session()
        reader = AsyncMock(return_value=None)
        session.sandbox.aread_file_text = reader
        assert await session.get_agent_md() is None
        assert await session.get_agent_md() is None
        reader.assert_awaited_once()
