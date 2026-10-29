"""An SSH machine outlives every create, so a change to the env this deployment
gives a computer (a key added, ``sandbox.ssh.trusted_host`` turned on or off)
reaches an existing one only at its next open. The MCP supervisor holds the
env it started with, so a change replaces it, and only a change: a needless
replace costs every server a cold start.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox import mcp_setup
from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import ExecResult, RuntimeState
from tests.unit.core.sandbox.fake_provider import fake_provider

ROOT = "/home/u/langalpha"


class _Machine:
    def __init__(self, changed: bool) -> None:
        self.replace_env = AsyncMock(return_value=changed)

    async def get_state(self):
        return RuntimeState.RUNNING

    async def fetch_working_dir(self):
        return ROOT


@pytest.mark.parametrize("changed", [False, True])
@pytest.mark.asyncio
async def test_a_reconnect_replaces_the_supervisor_only_when_the_env_changed(changed):
    machine = _Machine(changed)
    provider = fake_provider("ssh")
    provider.get.return_value = machine
    config = CoreConfig(
        sandbox=SandboxConfig(),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider", return_value=provider
    ):
        sandbox = PTCSandbox(config)
    sandbox._start_internal_mcp_servers = AsyncMock(return_value=None)

    await sandbox.reconnect("sb-1")

    machine.replace_env.assert_awaited_once()
    sandbox._start_internal_mcp_servers.assert_awaited_once_with(replace=changed)


@pytest.mark.parametrize("replace", [False, True])
@pytest.mark.asyncio
async def test_a_replacing_start_retires_the_running_daemon_first(replace):
    commands: list[str] = []

    async def exec_(command):
        commands.append(command)
        return ExecResult(stdout="", stderr="", exit_code=0)

    async def runtime_call(func, *args, retry_policy):
        return await func(*args)

    sandbox = SimpleNamespace(
        runtime=SimpleNamespace(exec=exec_),
        _runtime_call=runtime_call,
        _work_dir=ROOT,
        mounts_livefs=False,
    )

    await mcp_setup._start_internal_mcp_servers(sandbox, replace=replace)

    (command,) = commands
    assert ("--retire" in command) is replace
    if replace:
        assert command.index("--retire") < command.index("nohup")


@pytest.mark.asyncio
async def test_the_machine_env_is_rewritten_only_when_it_differs():
    runtime = object.__new__(SSHRuntime)
    link = object()
    runtime._ensure_running = AsyncMock(return_value=link)
    runtime._read_env = AsyncMock(return_value={"A": "1"})
    runtime._write_env = AsyncMock()

    assert await runtime.replace_env({"A": "1"}) is False
    runtime._write_env.assert_not_awaited()

    assert await runtime.replace_env({"A": "1", "B": "2"}) is True
    runtime._write_env.assert_awaited_once_with(link, {"A": "1", "B": "2"})
