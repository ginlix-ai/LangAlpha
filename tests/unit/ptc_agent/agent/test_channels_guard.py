"""Bash and ExecuteCode refuse a command naming the chat-app settings folder.

The folder is reached only through the file tools, so with the channels
route on, a command whose text names it is refused before the sandbox or the
mount is asked anything, mounted or not. With the route off it is an ordinary
path.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.core.paths import SandboxLayout

ONLY = (SandboxLayout.CHANNELS_DIR,)


class _Untouchable:
    """A sandbox backend that fails the test on any use."""

    def __getattr__(self, name: str):
        raise AssertionError(f"the sandbox was touched: {name}")


class _Mounted:
    """A sandbox the file mount serves, recording what ran."""

    def __init__(self) -> None:
        self.ran: list[str] = []
        self.livefs = SimpleNamespace(prepare=self._prepare, report=self._report)

    async def _prepare(self, call_id=None, context=None) -> None:
        return None

    async def _report(self, call_id, output, context=None) -> str:
        return ""

    async def settled_livefs(self, workspace_id=None):
        return self.livefs

    async def aexecute_bash(self, command, *, call_id=None, **_):
        self.ran.append(command)
        return {"success": True, "stdout": "ok", "stderr": "", "exit_code": 0}

    async def aexecute_code(self, code, *, thread_id=None, call_id=None):
        self.ran.append(code)
        return SimpleNamespace(success=True, stdout="ok", stderr="", mcp_trace=[])


COMMANDS = [
    "cat .agents/user/channels/channels.json",
    "ls /home/workspace/.agents/user/channels/",
    "# just a comment about .agents/user/channels\necho hi",
]


class TestBash:
    @pytest.mark.parametrize("command", COMMANDS)
    @pytest.mark.parametrize(
        "sandbox", [_Untouchable, _Mounted], ids=["unmounted", "mounted"]
    )
    @pytest.mark.asyncio
    async def test_a_command_naming_the_folder_is_refused(self, sandbox, command):
        tool = create_execute_bash_tool(sandbox(), file_tools_only=ONLY)

        result = await tool.ainvoke({"command": command})

        assert result == (
            "ERROR: .agents/user/channels/ is reachable only through the file tools. Use Read, "
            "Edit and Write on the files in .agents/user/channels/. Bash can't see or change them."
        )

    @pytest.mark.asyncio
    async def test_other_commands_run(self):
        backend = _Mounted()
        tool = create_execute_bash_tool(backend, file_tools_only=ONLY)

        await tool.ainvoke({"command": "ls .agents/user/profile"})

        assert backend.ran == ["ls .agents/user/profile"]

    @pytest.mark.asyncio
    async def test_without_the_route_the_path_is_ordinary(self):
        backend = _Mounted()
        tool = create_execute_bash_tool(backend)

        await tool.ainvoke({"command": COMMANDS[0]})

        assert backend.ran == [COMMANDS[0]]


class TestExecuteCode:
    @pytest.mark.parametrize(
        "sandbox", [_Untouchable, _Mounted], ids=["unmounted", "mounted"]
    )
    @pytest.mark.asyncio
    async def test_code_naming_the_folder_is_refused(self, sandbox):
        tool = create_execute_code_tool(sandbox(), None, file_tools_only=ONLY)

        result = await tool.ainvoke(
            {"code": "open('.agents/user/channels/channels.json').read()"}
        )

        assert result.startswith(
            "ERROR: .agents/user/channels/ is reachable only through the file tools."
        )
        assert "Read tool and pass the content in" in result

    @pytest.mark.asyncio
    async def test_without_the_route_the_path_is_ordinary(self):
        backend = _Mounted()
        tool = create_execute_code_tool(backend, None)

        await tool.ainvoke({"code": "print(open('.agents/user/channels/x').read())"})

        assert len(backend.ran) == 1
