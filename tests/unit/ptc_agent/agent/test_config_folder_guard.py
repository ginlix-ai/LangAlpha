"""Bash and ExecuteCode refuse a command naming LangAlpha's config folder by
the path of the login's custom config home, which holds the machine's Claude
Code tokens and which no fixed spelling names: a glob there would read them
past the redactor, encoded. Any other path runs, the default config home
included, which ``LANGALPHA_CONFIG_SPELLINGS`` covers in the middleware.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.middleware.tool.protected_path import BLOCKED_RESPONSE
from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.config.core import CoreConfig, DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from tests.unit.core.sandbox.fake_provider import fake_provider
from tests.unit.ptc_agent.agent.job_fakes import outcome


def _runtime(*answers: str | Exception) -> SimpleNamespace:
    """An SSH runtime whose login says ``answers`` in turn, one per read."""
    return SimpleNamespace(says_config_home=True, login_config_home=AsyncMock(side_effect=list(answers)))


def _sandbox(runtime: SimpleNamespace | None) -> PTCSandbox:
    """A sandbox on a machine whose login says what ``runtime`` answers, where a transient read reconnects."""
    provider = fake_provider()
    provider.is_transient_error = MagicMock(side_effect=lambda e: isinstance(e, SandboxTransientError))
    config = CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")), security=SecurityConfig(), mcp=MCPConfig(),
        logging=LoggingConfig(), filesystem=FilesystemConfig(),
    )
    with patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider", return_value=provider):
        sandbox = PTCSandbox(config=config)
    sandbox.sandbox_id = "c-1"
    sandbox.runtime = runtime
    return sandbox


class _Backend:
    """A backend on a machine whose config home is ``config_home``, recording what ran."""

    provider = "ssh"
    channel = "ssh"
    config_folder = SandboxBackend.config_folder

    def __init__(self, config_home: str | None = None, *, sandbox: PTCSandbox | None = None) -> None:
        self.sandbox = sandbox or _sandbox(_runtime(config_home))
        self.ran: list[str] = []

    async def settled_livefs(self, workspace_id=None):
        return None

    async def start_bash_job(self, command, **_):
        self.ran.append(command)
        return outcome(output="ok")

    async def start_code_job(self, code, **_):
        self.ran.append(code)
        return outcome(output="ok")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "working_dir"),
    [
        ("base64 /srv/config/langalpha/claude-code/*", None),
        ("cd /srv/config/langalpha/claude-code && base64 *", None),
        ("tar c /srv/config/langalpha | base64", None),
        ("ls /SRV/Config/LangAlpha/", None),
        ("base64 *", "/srv/config/langalpha/claude-code"),
    ],
)
async def test_bash_naming_the_custom_config_folder_never_runs(command, working_dir):
    backend = _Backend("/srv/config")
    tool = create_execute_bash_tool(backend)

    result = await tool.ainvoke({"command": command, "working_dir": working_dir})

    assert result == BLOCKED_RESPONSE
    assert backend.ran == []


@pytest.mark.asyncio
async def test_code_naming_the_custom_config_folder_never_runs():
    backend = _Backend("/srv/config/")
    tool = create_execute_code_tool(backend, None)

    code = "import glob\nprint(open(glob.glob('/srv/config/langalpha/claude-code/*')[0]).read())"
    result = await tool.ainvoke({"code": code})

    assert result == BLOCKED_RESPONSE
    assert backend.ran == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config_home", "command"),
    [
        ("/srv/config", "ls /srv/config/langalpha-old /srv/config/other"),
        ("/srv/config", "git clone https://example.com/langalpha && ls langalpha"),
        ("", "ls /srv/config/langalpha"),
        ("cfg", "ls cfg/langalpha"),
    ],
)
async def test_other_commands_run(config_home, command):
    backend = _Backend(config_home)
    tool = create_execute_bash_tool(backend)

    await tool.ainvoke({"command": command})

    assert backend.ran == [command]


@pytest.mark.asyncio
async def test_a_command_on_a_sandbox_still_starting_is_read_against_its_config_home_once_it_is_up():
    sandbox = _sandbox(None)
    sandbox._ready_event = asyncio.Event()
    backend = _Backend(sandbox=sandbox)
    call = asyncio.ensure_future(create_execute_bash_tool(backend).ainvoke({"command": "base64 /srv/config/langalpha/claude-code/*"}))
    await asyncio.sleep(0.05)
    assert not call.done()
    sandbox.runtime = _runtime("/srv/config")
    sandbox._ready_event.set()

    assert await call == BLOCKED_RESPONSE
    assert backend.ran == []


@pytest.mark.asyncio
async def test_a_config_home_the_machine_could_not_say_is_asked_again_after_a_reconnect():
    sandbox = _sandbox(_runtime(SandboxTransientError("connection lost"), "/srv/config"))
    backend = _Backend(sandbox=sandbox)

    with patch.object(sandbox, "_ensure_sandbox_connected", new=AsyncMock()) as reconnect:
        result = await create_execute_bash_tool(backend).ainvoke({"command": "base64 /srv/config/langalpha/claude-code/*"})

    assert result == BLOCKED_RESPONSE
    assert backend.ran == [] and reconnect.await_count == 1


@pytest.mark.asyncio
async def test_a_command_whose_config_home_cannot_be_read_never_runs():
    """The guard is best effort against the agent, but a read that fails lets no command past it unread."""
    backend = _Backend(sandbox=_sandbox(_runtime(RuntimeError("the machine refused the login"))))

    with pytest.raises(RuntimeError, match="refused the login"):
        await create_execute_bash_tool(backend).ainvoke({"command": "base64 /srv/config/langalpha/claude-code/*"})

    assert backend.ran == []
