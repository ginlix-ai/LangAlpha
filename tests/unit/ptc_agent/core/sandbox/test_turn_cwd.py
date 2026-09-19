"""Where a turn's processes start.

One computer holds several workspace folders, so the working directory is a
property of the turn, not of the machine. Python gets there through the
``PTC_TURN_CWD`` entry of the execution env, which the shipped
``sitecustomize.py`` reads at interpreter startup; ``execute_bash_command``
defaults its ``cd`` to the same place. With no project bound both fall back to
the computer root, which is what an unsplit machine has always done.

The provider surface carries no directory at all: nothing prepends a prelude to
the submitted source, because a first line that is not the caller's costs a
``from __future__`` import and every traceback line number.
"""

from __future__ import annotations

import ast
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
from ptc_agent.core.sandbox._shared import (
    _TURN_CWD_SOURCE,
    TURN_CWD_ENV,
    TURN_CWD_SANDBOX_NAME,
    _internal_package_files,
)
from ptc_agent.core.sandbox.runtime import (
    CodeRunResult,
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
    fake.code_run = AsyncMock(return_value=CodeRunResult("out", "", 0, []))
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


class TestExecuteCode:
    @pytest.mark.asyncio
    async def test_the_interpreter_starts_in_the_workspace(
        self, sandbox, runtime, project
    ):
        await sandbox.execute("print(1)", auto_install=False)
        call = runtime.code_run.await_args
        assert call.kwargs["env"][TURN_CWD_ENV] == WORKSPACE
        # The directory travels in the env and nowhere else: no provider
        # argument, and the submitted source reaches code_run untouched.
        assert "cwd" not in call.kwargs
        assert call.args[0] == "print(1)"

    @pytest.mark.asyncio
    async def test_without_a_project_it_starts_at_the_root(self, sandbox, runtime):
        await sandbox.execute("print(1)", auto_install=False)
        assert runtime.code_run.await_args.kwargs["env"][TURN_CWD_ENV] == WORK_DIR


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

        await sandbox.execute("print(1)", auto_install=False)
        env = runtime.code_run.await_args.kwargs["env"]
        assert sandbox.layout.internal_src in env["PYTHONPATH"].split(":")

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
    """Neither provider takes a directory; both hand the env to the interpreter."""

    @pytest.mark.asyncio
    async def test_daytona_code_run_sends_the_env_and_the_source_unchanged(self):
        from ptc_agent.core.sandbox.providers.daytona import DaytonaRuntime

        inner = AsyncMock()
        inner.process.exec = AsyncMock(
            return_value=type("R", (), {"result": "", "exit_code": 0})()
        )
        inner.process.code_run = AsyncMock(
            return_value=type("R", (), {"result": "", "exit_code": 0})()
        )
        runtime = DaytonaRuntime.__new__(DaytonaRuntime)
        runtime._sandbox = inner

        code = "from __future__ import annotations\nprint(1)"
        await runtime.code_run(code, env={TURN_CWD_ENV: WORKSPACE})
        # code_run's request carries code, argv and env and no directory, so
        # the env entry is the whole mechanism, and the first line stays the
        # caller's: a prepended chdir would make this source a SyntaxError.
        assert inner.process.code_run.await_args.args[0] == code
        params = inner.process.code_run.await_args.kwargs["params"]
        assert params.env[TURN_CWD_ENV] == WORKSPACE

        # exec runs from the sandbox's own working directory; a turn that wants
        # another one says so in the command (see TestBash).
        await runtime.exec("pwd")
        assert inner.process.exec.await_args.args[0] == "pwd"
        assert "cwd" not in inner.process.exec.await_args.kwargs

    @pytest.mark.asyncio
    async def test_docker_code_run_exports_the_turn_cwd(self):
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

        await runtime.code_run("print(1)", env={TURN_CWD_ENV: WORKSPACE})
        commands = [c.kwargs["cmd"][-1] for c in container.exec.await_args_list]
        # The interpreter process is started with the var in its environment,
        # which is what sitecustomize.py reads. Split rather than matched so
        # the assertion is about the value, not the quoting.
        run_cmd = next(c for c in commands if "python3 " in c and "base64" not in c)
        tokens = shlex.split(run_cmd)
        assert tokens[0] == "export"
        assert f"{TURN_CWD_ENV}={WORKSPACE}" in tokens

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
        await sandbox.execute("print(1)", auto_install=False)
        dest = runtime.upload_file.await_args.args[1]
        assert dest.startswith(f"{WORK_DIR}/.system/code/"), dest

    @pytest.mark.asyncio
    async def test_the_saved_bash_script_stays_at_the_computer_root(
        self, sandbox, runtime, project
    ):
        await sandbox.execute_bash_command("pwd")
        dest = runtime.upload_file.await_args.args[1]
        assert dest.startswith(f"{WORK_DIR}/.system/code/"), dest
