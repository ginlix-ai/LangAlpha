"""TaskOutput for a stop-killed subagent returns a clear cancelled message.

After a user stop wipes the registry, a resumed turn that follows its own
pseudo-result instruction and re-asks for the killed subagent must be told it
was cancelled — not that it "never existed".
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.tools import (
    create_task_output_tool,
)


@pytest.mark.asyncio
async def test_task_output_missing_task_reports_cancelled():
    middleware = MagicMock()
    registry = MagicMock()
    registry.get_by_task_id = AsyncMock(return_value=None)
    middleware.registry = registry

    tool = create_task_output_tool(middleware)

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "cancelled by a user stop" in result.lower()
    assert "not found" not in result.lower()
