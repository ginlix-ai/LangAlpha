"""A transient met inside a reconnect starts no second one.

A reconnect run directly, as a session reopening its sandbox runs it, is no
task a caller can join, so a call inside it that met a transient started a
fresh reconnect: that one closed and replaced the provider the running one
was still using.
"""

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    RuntimeState,
    SandboxRuntime,
    SandboxTransientError,
)
from tests.unit.core.sandbox.fake_provider import fake_provider


def _config() -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


@pytest.mark.asyncio
async def test_a_transient_inside_a_direct_reconnect_starts_no_second_one():
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
    runtime.exec = AsyncMock(return_value=ExecResult("", "", 0))
    provider = fake_provider()
    provider.get = AsyncMock(return_value=runtime)
    refused: list[SandboxTransientError] = []

    async def working_dir() -> str:
        # Stands in for any call the reconnect makes that meets a transient.
        try:
            await sandbox._ensure_sandbox_connected()
        except SandboxTransientError as e:
            refused.append(e)
        return "/home/workspace"

    runtime.fetch_working_dir = AsyncMock(side_effect=working_dir)
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider", return_value=provider
    ) as create:
        sandbox = PTCSandbox(config=_config())
        sandbox._start_internal_mcp_servers = AsyncMock(return_value=True)

        await sandbox.reconnect("sb-1")

    assert len(refused) == 1
    assert create.call_count == 1
    provider.close.assert_not_awaited()
    assert sandbox._reconnect_task is None
    assert sandbox._reconnects_running == 0
