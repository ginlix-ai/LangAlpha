"""Where a turn's processes start.

One computer holds several workspace folders, so the working directory is a
property of the turn, not of the machine. A code run gets there through the
``PTC_TURN_CWD`` entry of the environment its job starts with, which the
shipped ``sitecustomize.py`` reads at interpreter startup;
``execute_bash_command`` defaults its ``cd`` to the same place. With no
project bound both fall back to the computer root, which is what an unsplit
machine has always done.

The provider surface carries no directory at all: nothing prepends a prelude to
the submitted source, because a first line that is not the caller's costs a
``from __future__`` import and every traceback line number.

A shell a tool call runs also starts tagged with that call's id, which the file
mount reads to name the call a save through it belongs to.
"""

from __future__ import annotations

import ast
import base64
import re
import shlex
from contextlib import asynccontextmanager
from pathlib import Path
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
from ptc_agent.core import paths as core_paths
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.project_context import ProjectContext, set_project
from ptc_agent.core.sandbox.livefs_runtime.protocol import CALL_ENV
from ptc_agent.core.sandbox._shared import (
    _TURN_CWD_SOURCE,
    TURN_CWD_ENV,
    TURN_CWD_SANDBOX_NAME,
    _internal_package_files,
)
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    SandboxProvider,
    SandboxRuntime,
)

WORK_DIR = "/home/workspace"
DIR_NAME = "acme-ab12"
WORKSPACE = f"{WORK_DIR}/{DIR_NAME}"


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


@pytest.fixture
def project():
    from ptc_agent.core import project_context

    SandboxLayout(WORK_DIR).for_workspace(DIR_NAME)
    token = set_project(
        ProjectContext(workspace_id="ws-1", dir_name=DIR_NAME)
    )
    yield
    project_context._current.reset(token)


def _launch(runtime) -> str:
    (script, *_) = [
        c.args[0] for c in runtime.exec.await_args_list if "MCP_TRACE_FILE=" in c.args[0]
    ]
    return script


def _launch_env(runtime) -> dict[str, str]:
    """What a job's wrapper starts with: the launch's export, before its exec."""
    words = re.search(r"\( export ([^;]*); exec ", _launch(runtime)).group(1)
    return dict(word.split("=", 1) for word in shlex.split(words))


def _saved_files(runtime) -> dict[str, bytes]:
    """The files a launch writes ahead of its job, by path."""
    return {
        path: base64.b64decode(payload)
        for payload, path in re.findall(
            r"printf %s (\S+) \| base64 -d > (\S+)\.tmp\$\$", _launch(runtime)
        )
        for payload, path in [(shlex.split(payload)[0], shlex.split(path)[0])]
    }


class TestExecuteCode:
    @pytest.mark.asyncio
    async def test_the_interpreter_starts_in_the_workspace(
        self, sandbox, runtime, project
    ):
        await sandbox.start_code_job("print(1)", wait_s=0)
        assert _launch_env(runtime)[TURN_CWD_ENV] == WORKSPACE
        # The directory travels in the env, and the saved source is the
        # caller's own: a prepended chdir would cost a line number.
        assert list(_saved_files(runtime).values()) == [b"print(1)"]

    @pytest.mark.asyncio
    async def test_without_a_project_it_starts_at_the_root(self, sandbox, runtime):
        await sandbox.start_code_job("print(1)", wait_s=0)
        assert _launch_env(runtime)[TURN_CWD_ENV] == WORK_DIR


class TestBash:
    @pytest.mark.asyncio
    async def test_the_default_working_dir_is_the_workspace(
        self, sandbox, runtime, project
    ):
        await sandbox.execute_bash_command("pwd")
        commands = [c.args[0] for c in runtime.exec.await_args_list]
        assert any(f"cd {WORKSPACE} && pwd" in c for c in commands), commands

    @pytest.mark.asyncio
    async def test_without_a_project_it_is_the_root(self, sandbox, runtime):
        await sandbox.execute_bash_command("pwd")
        commands = [c.args[0] for c in runtime.exec.await_args_list]
        assert any(f"cd {WORK_DIR} && pwd" in c for c in commands), commands

    @pytest.mark.asyncio
    async def test_an_explicit_directory_still_wins(self, sandbox, runtime, project):
        await sandbox.execute_bash_command("pwd", working_dir=f"{WORKSPACE}/work")
        commands = [c.args[0] for c in runtime.exec.await_args_list]
        assert any(f"cd {WORKSPACE}/work && pwd" in c for c in commands), commands


class TestTheCallId:
    """The mount reads a process's starting environment, and the shell's own
    redirections are requests of the shell's pid, so the id goes on the
    shell's start, not in an export inside it."""

    @staticmethod
    def _sent(runtime) -> list[str]:
        return [c.args[0] for c in runtime.exec.await_args_list]

    @pytest.mark.parametrize("call_id", [None, "c0ffee00c0ffee00"], ids=["untagged", "tagged"])
    @pytest.mark.asyncio
    async def test_a_job_starts_tagged_with_its_call(self, sandbox, runtime, call_id):
        """A job outlives its call, so the tag rides the wrapper's starting
        environment, which every process the command starts inherits."""
        await sandbox.start_bash_job("pwd", wait_s=0, call_id=call_id)

        (launch, *_) = [c for c in self._sent(runtime) if "MCP_TRACE_FILE=" in c]
        words = re.search(r"\( export ([^;]*); exec ", launch).group(1)
        env = dict(word.split("=", 1) for word in shlex.split(words))
        assert env.get(CALL_ENV) == call_id


class TestTheShippedModule:
    """The env var only moves a process if ``site`` imports the module that reads it."""

    @pytest.mark.asyncio
    async def test_it_ships_into_the_directory_site_imports_from(
        self, sandbox, runtime
    ):
        src_dir = Path(core_paths.__file__).parents[2]
        shipped = {str(rel): local for local, rel in _internal_package_files(src_dir)}
        # Flat at the top of _internal/src/, under the one name site looks for:
        # any other name is a module nothing imports.
        assert TURN_CWD_SANDBOX_NAME == "sitecustomize.py"
        assert shipped.get(TURN_CWD_SANDBOX_NAME) == _TURN_CWD_SOURCE
        # And the file that lands there is the one that does the chdir.
        landed = shipped[TURN_CWD_SANDBOX_NAME].read_text()
        assert TURN_CWD_ENV in landed
        assert "chdir" in landed

        await sandbox.start_code_job("print(1)", wait_s=0)
        # Prepended to the computer's own, which the shell appends.
        ours = _launch_env(runtime)["PYTHONPATH"].split("${")[0]
        assert sandbox.layout.internal_src in ours.split(":")

    def test_the_turn_cwd_stops_at_a_nested_mcp_client(self):
        """A supervisor outlives the turn that started it and serves every
        workspace on the computer, so it must not inherit the turn's folder.

        Read out of the source because the name is a function local in a module
        that ships into the sandbox, where the host constant is not importable.
        """
        runtime_source = _TURN_CWD_SOURCE.with_name("mcp_client_runtime.py")
        tree = ast.parse(runtime_source.read_text())
        caller_only: tuple[str, ...] | None = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "_CALLER_ONLY_ENV"
                for t in node.targets
            ):
                caller_only = tuple(ast.literal_eval(node.value))
        assert caller_only is not None, "_CALLER_ONLY_ENV is gone from the runtime"
        assert TURN_CWD_ENV in caller_only, caller_only


class TestProviderSurface:
    """Neither provider's exec takes a directory; both run at the computer root."""

    @pytest.mark.asyncio
    async def test_daytona_exec_gets_no_cwd(self):
        from ptc_agent.core.sandbox.providers.daytona_runtime import DaytonaRuntime

        inner = AsyncMock()
        inner.process.exec = AsyncMock(
            return_value=type("R", (), {"result": "", "exit_code": 0})()
        )
        runtime = DaytonaRuntime.__new__(DaytonaRuntime)
        runtime._sandbox = inner

        # exec runs from the sandbox's own working directory; a turn that wants
        # another one says so in the command (see TestBash).
        await runtime.exec("pwd")
        assert inner.process.exec.await_args.args[0] == "pwd"
        assert "cwd" not in inner.process.exec.await_args.kwargs

    @pytest.mark.asyncio
    async def test_docker_exec_runs_at_the_computer_root(self):
        from ptc_agent.core.sandbox.providers.docker import DockerRuntime

        stream = AsyncMock()
        stream.read_out = AsyncMock(return_value=None)

        @asynccontextmanager
        async def _start():
            yield stream

        exec_obj = AsyncMock()
        exec_obj.start = _start
        exec_obj.inspect = AsyncMock(return_value={"ExitCode": 0})
        container = AsyncMock()
        container.exec = AsyncMock(return_value=exec_obj)
        runtime = DockerRuntime.__new__(DockerRuntime)
        runtime._container = container
        runtime._working_dir = WORK_DIR

        # Every exec runs at the container's own working directory, the
        # computer root.
        await runtime.exec("true")
        assert container.exec.await_args.kwargs["workdir"] == WORK_DIR


class TestTheRuntimesOwnFiles:
    """Saved code belongs to the machine, wherever the turn is running."""

    @pytest.mark.asyncio
    async def test_the_saved_code_file_stays_at_the_computer_root(
        self, sandbox, runtime, project
    ):
        await sandbox.start_code_job("print(1)", wait_s=0)
        (dest,) = _saved_files(runtime)
        assert dest.startswith(f"{WORK_DIR}/.system/code/bg-"), dest

    @pytest.mark.asyncio
    async def test_the_saved_bash_script_stays_at_the_computer_root(
        self, sandbox, runtime, project
    ):
        await sandbox.start_bash_job("pwd", wait_s=0)
        (dest,) = _saved_files(runtime)
        assert dest.startswith(f"{WORK_DIR}/.system/code/bg-") and dest.endswith(".sh"), dest
