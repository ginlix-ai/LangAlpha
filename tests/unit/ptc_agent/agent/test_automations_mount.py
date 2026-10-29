"""The automations folder is reachable through the file tools, and through the
sandbox only while the file mount serves it.

Each file in `.agents/user/automations/` is a row in Postgres served by a
mounted route, so Read and Write must reach that route with the build's own
defaults, while Bash and ExecuteCode on a sandbox without the file mount must
refuse the path before they touch the sandbox. A shell that ran would find no
file there, or write one nothing reads. With the mount, each command files who
it runs for under its call id before it runs, which is where a save through
the mount takes its defaults from.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.automations import AutomationsBackend
from ptc_agent.agent.backends.db_json_route import Plan
from ptc_agent.agent.filesystem_routes import IdentityGates, build_filesystem_backend
from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.agent.tools.command_handoff import HandOff
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.services.automations.file import AutomationFile, FilePlan
from tests.unit.ptc_agent.agent.job_fakes import outcome, tool_call

ROOT = "/home/workspace"
FILE_NAME = "morning-brief.json"
FILE_PATH = f"{ROOT}/.agents/user/automations/{FILE_NAME}"
CONTENT = '{\n  "name": "Morning brief"\n}\n'
USER = "user-fake-1"
WORKSPACE = "00000000-0000-4000-8000-00000000aaaa"
THREAD = "00000000-0000-4000-8000-00000000bbbb"
CONTEXT = CallContext(workspace_id=WORKSPACE, thread_id=THREAD, timezone="Asia/Tokyo")


def _gates() -> IdentityGates:
    return IdentityGates(
        user_memory=False,
        workspace_memory=False,
        memo=False,
        user_data=True,
        workflow=False,
        workflow_fs=False,
        workflow_tool=False,
    )


def _sandbox() -> MagicMock:
    sb = MagicMock()
    sb.computer_root = ROOT
    sb.normalize_path.side_effect = lambda p: p if p.startswith("/") else f"{ROOT}/{p}"
    sb.aread_text = AsyncMock(side_effect=AssertionError("read fell through to the sandbox"))
    sb.awrite_text = AsyncMock(side_effect=AssertionError("write fell through to the sandbox"))
    return sb


@pytest.fixture
def automation(monkeypatch) -> MagicMock:
    """One automation's file with its steps faked; any other name holds none."""
    fake = MagicMock(spec=AutomationFile(FILE_NAME))
    fake.unchanged = None
    fake.fetch = AsyncMock(return_value=["row"])
    fake.render = MagicMock(return_value=(CONTENT, "v1"))
    monkeypatch.setattr(
        AutomationsBackend, "file_named", classmethod(lambda cls, name: fake if name == FILE_NAME else None)
    )
    return fake


@pytest.fixture
def filesystem():
    backend, _ = build_filesystem_backend(
        backend=_sandbox(),
        gates=_gates(),
        store=None,
        user_id=USER,
        workspace_id=WORKSPACE,
        call=CONTEXT,
    )
    return backend


class TestMount:
    @pytest.mark.asyncio
    async def test_a_read_is_served_from_the_users_rows(self, filesystem, automation):
        content = await filesystem.aread_text(FILE_PATH)

        assert content == CONTENT
        automation.fetch.assert_awaited_once_with(USER)

    @pytest.mark.asyncio
    async def test_a_write_carries_the_builds_defaults_for_a_new_automation(
        self, filesystem, automation, monkeypatch
    ):
        conn = MagicMock()

        @asynccontextmanager
        async def _open(*_):
            yield conn

        conn.transaction = conn.cursor = _open
        monkeypatch.setattr(db_json_route, "get_db_connection", _open)
        automation.render.side_effect = lambda rows: None if rows == [] else (CONTENT, "v1")
        automation.fetch.return_value = []
        automation.plan.return_value = Plan(FilePlan())
        automation.hold.return_value = None
        automation.commit.return_value = 'Saved morning-brief.json: created "Morning brief"'

        await filesystem.awrite_text(FILE_PATH, '{"name": "Morning brief"}')

        automation.parse.assert_awaited_once_with(USER, CONTEXT, ANY, None)
        assert automation.plan.call_args.args[0] == CONTEXT


class _Untouchable:
    """A sandbox backend without the file mount that fails the test on any other use."""

    livefs = None
    provider = "docker"  # read once, when the tool is built
    channel = "docker"

    async def settled_livefs(self, workspace_id=None):
        return None

    def __getattr__(self, name: str):
        raise AssertionError(f"the sandbox was touched: {name}")


class TestShellGuards:
    @pytest.mark.parametrize(
        "command",
        [
            f"cat .agents/user/automations/{FILE_NAME}",
            f"ls {ROOT}/.agents/user/automations/",
        ],
    )
    @pytest.mark.asyncio
    async def test_bash_refuses_the_path(self, command):
        tool = create_execute_bash_tool(_Untouchable())

        result = await tool.ainvoke({"command": command})

        assert result.startswith("ERROR")
        assert ".agents/user/automations/** is kept on the server" in result

    @pytest.mark.asyncio
    async def test_execute_code_refuses_the_path(self):
        tool = create_execute_code_tool(_Untouchable(), None)

        result = await tool.ainvoke(
            {"code": f"import json\njson.load(open('.agents/user/automations/{FILE_NAME}'))"}
        )

        assert result.startswith("ERROR")
        assert ".agents/user/automations/** is kept on the server" in result


class _Mount:
    """A serving file mount recording, in order, what a command asked of it."""

    def __init__(self) -> None:
        self.events: list[tuple] = []

    async def prepare(self, call_id=None, context=None) -> None:
        self.events.append(("prepare", call_id, context))

    async def report(self, call_id: str, output: str, context=None) -> str:
        self.events.append(("report", call_id))
        return ""


class _Mounted:
    """A sandbox backend whose sandbox the file mount serves."""

    provider = "docker"
    channel = "docker"

    def __init__(self) -> None:
        self.mount = _Mount()
        self.livefs = self.mount
        self.asked_for: list[str | None] = []

    async def settled_livefs(self, workspace_id=None):
        self.asked_for.append(workspace_id)
        return self.livefs

    async def start_bash_job(self, command, *, call_id=None, wait_s=None, **_):
        self.mount.events.append(("run", call_id))
        return outcome(done=wait_s != 0, output="ok")

    async def start_code_job(self, code, *, call_id=None, **_):
        self.mount.events.append(("run", call_id))
        return outcome(output="ok")


def _hand_off() -> HandOff:
    return HandOff(
        thread_id=THREAD,
        user_id=USER,
        workspace_id=WORKSPACE,
        wakeups=True,
    )


class TestWithTheMount:
    @pytest.mark.parametrize("background", [False, True], ids=["foreground", "background"])
    @pytest.mark.asyncio
    async def test_bash_files_who_the_command_runs_for_before_it_runs(self, background):
        """A background command outlives its call, and its saves still read
        this conversation's defaults."""
        backend = _Mounted()
        tool = create_execute_bash_tool(
            backend,
            session=SimpleNamespace(computer_id="00000000-0000-4000-8000-00000000cccc"),
            call_context=CONTEXT,
            hand_off=_hand_off(),
        )

        record = AsyncMock()
        with patch("src.server.database.runs.command_runs.record_handoff", new=record):
            await tool.ainvoke(
                tool_call("Bash", command=f"cat {FILE_PATH}", run_in_background=background)
            )

        assert record.await_count == background
        (_, call_id, context), run, report = backend.mount.events
        assert call_id and context == CONTEXT
        assert run == ("run", call_id) and report == ("report", call_id)
        # The mount is asked for the folder the command runs in.
        assert backend.asked_for == [CONTEXT.workspace_id]

    @pytest.mark.asyncio
    async def test_execute_code_files_who_the_code_runs_for_before_it_runs(self):
        backend = _Mounted()
        tool = create_execute_code_tool(backend, None, call_context=CONTEXT)

        await tool.ainvoke({"code": f"print(open({FILE_PATH!r}).read())"})

        (_, call_id, context), run, report = backend.mount.events
        assert call_id and context == CONTEXT
        assert run == ("run", call_id) and report == ("report", call_id)
        # The mount is asked for the folder the command runs in.
        assert backend.asked_for == [CONTEXT.workspace_id]
