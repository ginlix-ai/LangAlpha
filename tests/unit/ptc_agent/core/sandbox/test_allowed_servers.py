"""PTC_ALLOWED_SERVERS: exported by a limited subagent's code and shell, enforced by the sandbox connector client, kept from the shared daemon."""

from __future__ import annotations

import re
import shlex
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox import mcp_client_runtime as client
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    SandboxProvider,
    SandboxRuntime,
)

ENV = "PTC_ALLOWED_SERVERS"
WORK_DIR = "/home/workspace"


class TestTheClientGate:
    @pytest.fixture(autouse=True)
    def workspace(self, monkeypatch):
        monkeypatch.setattr(
            client, "_workspace_view", lambda: {"servers": {"srv_a": {}, "srv_b": {}}}
        )

    @pytest.mark.parametrize(
        ("limit", "server", "allowed"),
        [
            (None, "srv_a", True),
            (None, "srv_b", True),
            ("srv_a", "srv_a", True),
            ("srv_a", "srv_b", False),
            ("srv_a,srv_b", "srv_b", True),
            # Set but empty is a subagent allowed nothing, not an unlimited one.
            ("", "srv_a", False),
            # A list of names, not a string to search in.
            ("srv_ab", "srv_a", False),
            # The limit narrows the workspace's set and never widens it.
            ("srv_c", "srv_c", False),
        ],
    )
    def test_a_call_is_refused_outside_the_limit(self, monkeypatch, limit, server, allowed):
        if limit is None:
            monkeypatch.delenv(ENV, raising=False)
        else:
            monkeypatch.setenv(ENV, limit)

        if allowed:
            client._require_enabled(server)
        else:
            with pytest.raises(RuntimeError, match=f"'{server}'"):
                client._require_enabled(server)

    def test_the_shared_daemon_never_inherits_the_limit(self, monkeypatch, tmp_path):
        """The daemon serves every agent on the computer, so a limit it
        inherited from the subagent that started it would bind the others."""
        package = tmp_path / client._LAYOUT["INTERNAL_SRC_DIR"] / client._SUPERVISOR_PACKAGE
        package.mkdir(parents=True)
        (package / "daemon.py").write_text("")
        monkeypatch.setattr(client, "_WORK_DIR", str(tmp_path))
        monkeypatch.setattr(client, "_INTERNAL_ROOT", str(tmp_path / "_internal"))
        started: list[dict[str, str]] = []
        monkeypatch.setattr(client, "_socket_alive", lambda path: bool(started))
        monkeypatch.setattr(
            client.subprocess, "Popen", lambda *args, env, **kwargs: started.append(env)
        )
        monkeypatch.setenv(ENV, "srv_a")
        monkeypatch.setenv("PTC_PLACEHOLDER_KEPT", "1")

        assert client._start_supervisor(str(tmp_path / "daemon.sock")) is True

        (env,) = started
        assert ENV not in env
        assert env["PTC_PLACEHOLDER_KEPT"] == "1"


async def _passthrough_runtime_call(func, *args, **kwargs):
    for retry_kwarg in (
        "retry_policy",
        "allow_reconnect",
        "retries",
        "initial_delay_s",
        "total_timeout",
    ):
        kwargs.pop(retry_kwarg, None)
    return await func(*args, **kwargs)


@pytest.fixture
def runtime():
    fake = AsyncMock(spec=SandboxRuntime)
    fake.working_dir = WORK_DIR
    fake.fetch_working_dir = AsyncMock(return_value=WORK_DIR)
    fake.exec = AsyncMock(return_value=ExecResult("", "", 0))
    fake.upload_file = AsyncMock()
    return fake


@pytest.fixture
def sandbox(runtime):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    config = CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    with patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider"):
        box = PTCSandbox(config=config)
    box.provider = AsyncMock(spec=SandboxProvider)
    box.runtime = runtime
    box._work_dir = WORK_DIR
    box._runtime_call = AsyncMock(side_effect=_passthrough_runtime_call)
    box.aread_file_text = AsyncMock(return_value="")
    return box


def _launch_env(runtime) -> dict[str, str]:
    """What a job's wrapper starts with: the launch's export, before its exec."""
    (script, *_) = [
        c.args[0] for c in runtime.exec.await_args_list if "MCP_TRACE_FILE=" in c.args[0]
    ]
    words = re.search(r"\( export ([^;]*); exec ", script).group(1)
    return dict(word.split("=", 1) for word in shlex.split(words))


LIMITS = [
    (None, None),
    (frozenset({"srv_b", "srv_a"}), "srv_a,srv_b"),
    (frozenset(), ""),
]
LIMIT_IDS = ["unlimited", "two-servers", "none-allowed"]


class TestTheExport:
    """What a limited subagent's tools put in the environment its code starts
    with, from the tool down: a link that drops it lifts the limit silently."""

    @pytest.mark.parametrize(("allowed", "exported"), LIMITS, ids=LIMIT_IDS)
    @pytest.mark.asyncio
    async def test_execute_code(self, sandbox, runtime, allowed, exported):
        tool = create_execute_code_tool(SandboxBackend(sandbox), None, allowed_servers=allowed)

        await tool.ainvoke({"code": "print(1)"})

        assert _launch_env(runtime).get(ENV) == exported

    @pytest.mark.parametrize(("allowed", "exported"), LIMITS, ids=LIMIT_IDS)
    @pytest.mark.asyncio
    async def test_bash(self, sandbox, runtime, allowed, exported):
        tool = create_execute_bash_tool(SandboxBackend(sandbox), allowed_servers=allowed)

        await tool.ainvoke({"command": "pwd"})

        assert _launch_env(runtime).get(ENV) == exported
