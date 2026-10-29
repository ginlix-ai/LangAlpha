"""The Chief of Staff's machine tools take a `computer` and run there."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Annotated, Any
from unittest.mock import Mock

import pytest
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import InjectedToolCallId, StructuredTool, tool

from ptc_agent.agent.tools.computer_reach import (
    COMPUTER_FIELD,
    ComputerTarget,
    ComputerTools,
    ComputerUnreachable,
    ran_elsewhere,
)
from ptc_agent.agent.tools import computer_reach
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.project_context import (
    MachinePaths,
    ProjectContext,
    current_project,
    project_bound,
)
from ptc_agent.core.sandbox import path_resolution as _paths


def _bash(label: str, seen: list | None = None) -> StructuredTool:
    async def Bash(
        command: str,
        description: str | None = None,
        *,
        config: RunnableConfig,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> tuple[str, dict[str, Any]]:
        project = current_project()
        if seen is not None:
            seen.append((config, project))
        where = project.sibling_dir_names if project else None
        return f"{label}:{command}:{tool_call_id}:{where}", {"mcp_trace": []}

    return StructuredTool.from_function(
        coroutine=Bash,
        name="Bash",
        description="run bash",
        response_format="content_and_artifact",
    )


def _read(label: str) -> StructuredTool:
    @tool("Read")
    async def read_file(file_path: str, offset: int | None = None) -> str:
        """Read a file."""
        return f"{label}:{file_path}:{offset}"

    return read_file


def _execute_code(label: str) -> StructuredTool:
    async def ExecuteCode(code: str) -> tuple[str, dict[str, Any]]:
        return f"{label}:ran", {"mcp_trace": []}

    return StructuredTool.from_function(
        coroutine=ExecuteCode,
        name="ExecuteCode",
        description="run code",
        response_format="content_and_artifact",
    )


MAC_ROOT = "/Users/studio/langalpha"
REACHED = ProjectContext(
    workspace_id="", dir_name=None, sibling_dir_names=("semis",), machine=MachinePaths()
)


def _mac_sandbox() -> Any:
    """A computer rooted at ``MAC_ROOT`` with the real path rules over it."""
    layout = SandboxLayout(MAC_ROOT)
    sandbox = SimpleNamespace(_work_dir=MAC_ROOT, layout=layout, channel="ssh")
    sandbox.config = SimpleNamespace(
        sandbox=SimpleNamespace(provider="ssh"),
        filesystem=SimpleNamespace(
            working_directory=MAC_ROOT,
            allowed_directories=layout.allowed_directories,
            denied_directories=layout.denied_directories,
            enable_path_validation=True,
        ),
    )

    def workspace(project: ProjectContext | None = None) -> Any:
        ctx = project if project is not None else current_project()
        return layout.for_workspace(ctx.dir_name if ctx is not None else None)

    sandbox.workspace = workspace
    sandbox.normalize_path = lambda path, project=None: _paths.normalize_path(sandbox, path, project)
    sandbox.validate_path = lambda path, project=None: _paths.validate_path(sandbox, path, project)
    return sandbox


MAC = ComputerTarget(
    computer_id="22222222-2222-2222-2222-222222222222",
    name="Studio Mac",
    session=SimpleNamespace(sandbox=_mac_sandbox()),
    project=REACHED,
    root=MAC_ROOT,
)

OWN_ROOT = "/home/workspace"
HOME = ProjectContext(workspace_id="home", dir_name="Home", sibling_dir_names=("semis",))


def _own_sandbox() -> Any:
    """The turn's own computer, rooted at ``OWN_ROOT``, with the real path rules."""
    layout = SandboxLayout(OWN_ROOT)
    sandbox = SimpleNamespace(_work_dir=OWN_ROOT, working_dir=OWN_ROOT, layout=layout)
    sandbox.config = SimpleNamespace(
        filesystem=SimpleNamespace(
            allowed_directories=layout.allowed_directories,
            denied_directories=layout.denied_directories,
            enable_path_validation=True,
        ),
    )
    sandbox.workspace = lambda project=None: layout.for_workspace(
        (project or current_project() or HOME).dir_name
    )
    sandbox.normalize_path = lambda path, project=None: _paths.normalize_path(sandbox, path, project)
    return sandbox


class _Reach:
    def __init__(self, target: ComputerTarget = MAC) -> None:
        self.asked: list[str] = []
        self.target = target
        self.counted: list[str] = []

    async def in_use(self, target: ComputerTarget) -> None:
        self.counted.append(target.computer_id)

    async def reach(self, computer: str):
        self.asked.append(computer)
        if computer == "nope":
            raise ComputerUnreachable('The user has no computer named "nope".')
        if computer == "own":
            return None
        return self.target


def _tools(reach=None, build=None) -> ComputerTools:
    return ComputerTools(
        reach or _Reach(),
        build
        or (
            lambda _t: {
                "Bash": _bash("there"),
                "Read": _read("there"),
                "ExecuteCode": _execute_code("there"),
            }
        ),
        _own_sandbox(),
    )


async def _call(tool_, args, call_id="call_1"):
    return await tool_.ainvoke({"type": "tool_call", "name": tool_.name, "args": args, "id": call_id})


def test_the_model_sees_a_computer_argument_and_still_not_the_injected_ones():
    tools = _tools()
    for wrapped in (tools.wrap(_bash("here")), tools.wrap(_read("here"))):
        props = wrapped.tool_call_schema.model_json_schema()["properties"]
        assert props["computer"]["description"] == COMPUTER_FIELD
        assert {"type": "string"} in props["computer"]["anyOf"]
        assert "tool_call_id" not in props
        assert "config" not in props
    bash_props = tools.wrap(_bash("here")).tool_call_schema.model_json_schema()
    assert "computer" not in bash_props.get("required", [])
    assert "command" in bash_props["required"]


@pytest.mark.asyncio
@pytest.mark.parametrize("computer", [None, "", "   ", "own"])
async def test_no_computer_or_the_turns_own_runs_the_original_tool(computer):
    reach = _Reach()
    wrapped = _tools(reach).wrap(_read("here"))
    args = {"file_path": "a.md"} if computer is None else {"file_path": "a.md", "computer": computer}
    msg = await _call(wrapped, args)
    assert msg.content == "here:a.md:None"
    if computer is None or not computer.strip():
        assert reach.asked == []


@pytest.mark.asyncio
async def test_a_call_naming_another_computer_runs_its_tool_there_from_its_root():
    seen: list = []
    tools = _tools(build=lambda _t: {"Bash": _bash("there", seen)})
    wrapped = tools.wrap(_bash("here"))

    msg = await _call(wrapped, {"command": "ls", "computer": "Studio Mac"}, "call_9")

    assert msg.content == "there:ls:call_9:('semis',)"
    assert msg.artifact == {"mcp_trace": []}
    (config, project) = seen[0]
    assert config  # the call's config reaches the inner coroutine
    assert project is MAC.project
    assert current_project() is None


@pytest.mark.asyncio
async def test_an_unreachable_computer_is_an_error_in_the_tools_own_format():
    tools = _tools()
    bash = await _call(tools.wrap(_bash("here")), {"command": "ls", "computer": "nope"})
    assert bash.content == 'ERROR: The user has no computer named "nope".'
    assert bash.artifact == {"mcp_trace": []}

    read = await _call(tools.wrap(_read("here")), {"file_path": "x", "computer": "nope"})
    assert read.content == 'ERROR: The user has no computer named "nope".'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "from tools import x",
        "import tools",
        "from tools.yfinance import y",
        "import os\n    from tools import x",
        "import json, tools.market_data",
        "import os; import tools.x as t",
        "def f():\n    from tools import x\n    return x",
        "await asyncio.sleep(0)\nimport tools",
    ],
)
async def test_code_importing_the_data_tools_is_refused_on_another_computer(code):
    wrapped = _tools().wrap(_execute_code("here"))
    msg = await _call(wrapped, {"code": code, "computer": "Studio Mac"})
    assert msg.content.startswith("ERROR: The data tools")
    assert msg.artifact == {"mcp_trace": []}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "print('from tools import x')",
        "import toolz",
        "import mytools",
        "x = 1  # import tools later",
        '"""\nimport tools\n"""',
        "from .tools import x",
    ],
)
async def test_code_that_only_mentions_tools_runs_on_another_computer(code):
    wrapped = _tools().wrap(_execute_code("here"))
    msg = await _call(wrapped, {"code": code, "computer": "Studio Mac"})
    assert msg.content == "there:ran"


@pytest.mark.asyncio
async def test_the_data_tools_import_runs_on_the_turns_own_computer():
    wrapped = _tools().wrap(_execute_code("here"))
    for args in ({"code": "from tools import x"}, {"code": "from tools import x", "computer": "own"}):
        assert (await _call(wrapped, args)).content == "here:ran"


def test_a_computers_tools_are_built_once_and_again_only_for_a_new_session():
    build = Mock(side_effect=lambda t: {"Read": _read("there")})
    tools = ComputerTools(_Reach(), build, _own_sandbox())
    first = tools.on(MAC)
    assert tools.on(MAC) is first
    assert build.call_count == 1

    restarted = ComputerTarget(MAC.computer_id, MAC.name, object(), MAC.project, MAC.root)
    assert tools.on(restarted) is not first
    assert build.call_count == 2


def test_only_a_non_empty_string_computer_counts_as_running_elsewhere():
    assert ran_elsewhere({"computer": "Studio Mac"})
    assert not ran_elsewhere({"computer": ""})
    assert not ran_elsewhere({"computer": "  "})
    assert not ran_elsewhere({"computer": None})
    assert not ran_elsewhere({"computer": 3})
    assert not ran_elsewhere({})
    assert not ran_elsewhere(None)


@pytest.mark.parametrize(
    ("path", "whole"),
    [
        (f"{MAC_ROOT}/semis/agent.md", f"{MAC_ROOT}/semis/agent.md"),
        ("semis/agent.md", f"{MAC_ROOT}/semis/agent.md"),
        ("", MAC_ROOT),
        # Another machine's path, or one outside the root, stays what it says:
        # folded under the root it would name a file nobody meant.
        ("/Users/studio/Documents/a.pdf", "/Users/studio/Documents/a.pdf"),
        ("/home/workspace/Home/a.md", "/home/workspace/Home/a.md"),
        ("~/a.md", "~/a.md"),
    ],
)
def test_a_call_from_another_computer_takes_the_machines_own_paths(path, whole):
    sandbox = _mac_sandbox()
    assert _paths.normalize_path(sandbox, path, REACHED) == whole
    assert _paths.virtualize_path(sandbox, whole, REACHED) == whole


def test_a_turns_own_folder_keeps_its_virtual_spelling():
    sandbox = _mac_sandbox()
    own = ProjectContext(workspace_id="ws", dir_name="semis")
    assert _paths.normalize_path(sandbox, "/notes.md", own) == f"{MAC_ROOT}/semis/notes.md"
    assert _paths.virtualize_path(sandbox, f"{MAC_ROOT}/semis/notes.md", own) == "/notes.md"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "says"),
    [
        (
            "/Users/studio/Documents/a.pdf",
            'ERROR: /Users/studio/Documents/a.pdf is outside what the file tools reach on '
            f'computer "Studio Mac": its root `{MAC_ROOT}` and `/tmp`.',
        ),
        ("/home/workspace/Home/a.md", f"its root `{MAC_ROOT}`"),
        ("../a.md", f"its root `{MAC_ROOT}`"),
        ("~/Documents/a.pdf", f'ERROR: `~` is not expanded on computer "Studio Mac". Write the path whole, under its root `{MAC_ROOT}`.'),
        (".agents/user/memory/notes.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        (f"{MAC_ROOT}/.agents/workflows/w.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        # The mount links these into every workspace folder too, and where it
        # does not serve, a save there would land on plain disk.
        (f"{MAC_ROOT}/semis/.agents/memory/x.md", "ERROR: A workspace's memory and past threads are kept by the server"),
        ("semis/.agents/user/profile/x.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        ("semis/.agents/transcripts/t.jsonl", "ERROR: A workspace's memory and past threads are kept by the server"),
        (".agents/threads.jsonl", "ERROR: A workspace's memory and past threads are kept by the server"),
    ],
)
async def test_a_path_the_file_tools_cannot_use_there_is_refused_naming_the_root(path, says):
    tools = _tools(_Reach(MAC), build=lambda _t: {"Read": _read("there")})
    msg = await _call(tools.wrap(_read("here")), {"file_path": path, "computer": "Studio Mac"})
    assert says in msg.content
    assert msg.content.startswith("ERROR:")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        f"{MAC_ROOT}/semis/agent.md",
        "semis/agent.md",
        "/tmp/prices.csv",
        # A command's log on that computer, which its notice points at.
        ".agents/threads/abcd1234/commands/bg-1.log",
        # A folder's own agent files that the mount does not serve.
        "semis/.agents/skills/dcf/SKILL.md",
    ],
)
async def test_a_path_under_the_root_or_tmp_runs_there(path):
    tools = _tools(_Reach(MAC), build=lambda _t: {"Read": _read("there")})
    msg = await _call(tools.wrap(_read("here")), {"file_path": path, "computer": "Studio Mac"})
    assert msg.content == f"there:{path}:None"


@pytest.mark.asyncio
async def test_a_path_on_the_turns_own_computer_is_not_checked_against_another_root():
    tools = _tools(_Reach(MAC), build=lambda _t: {"Read": _read("there")})
    msg = await _call(tools.wrap(_read("here")), {"file_path": "~/a.md", "computer": "own"})
    assert msg.content == "here:~/a.md:None"


def _grep(label: str) -> StructuredTool:
    @tool("Grep")
    async def grep(pattern: str, path: str | None = None) -> str:
        """Search files."""
        return f"{label}:{pattern}:{path}"

    return grep


class _ReachWithWorkspaces(_Reach):
    def __init__(self, elsewhere=(("Studio Mac", MAC_ROOT),)) -> None:
        super().__init__(MAC)
        self._elsewhere = elsewhere

    async def elsewhere(self):
        return self._elsewhere


async def _grep_here(args: dict, reach: _Reach | None = None) -> str:
    tools = ComputerTools(
        reach or _ReachWithWorkspaces(),
        lambda _t: {"Grep": _grep("there")},
        _own_sandbox(),
    )
    with project_bound(HOME):
        msg = await _call(tools.wrap(_grep("here")), args)
    return msg.content


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["..", "../", OWN_ROOT, f"{OWN_ROOT}/"])
async def test_a_search_of_the_whole_computer_names_the_workspaces_it_left_out(path):
    content = await _grep_here({"pattern": "Albemarle", "path": path})
    assert content.startswith(f"here:Albemarle:{path}\n\n")
    assert f'"Studio Mac" (root `{MAC_ROOT}`)' in content
    assert "`computer`" in content


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [None, ".", "notes", "../semis", "/tmp"])
async def test_a_search_of_part_of_the_computer_says_nothing_of_the_others(path):
    args = {"pattern": "Albemarle", **({"path": path} if path is not None else {})}
    assert await _grep_here(args) == f"here:Albemarle:{path}"


@pytest.mark.asyncio
async def test_no_other_computer_with_workspaces_means_no_note():
    content = await _grep_here({"pattern": "x", "path": ".."}, _ReachWithWorkspaces(()))
    assert content == "here:x:.."


@pytest.mark.asyncio
async def test_a_search_on_another_computer_gets_no_note():
    content = await _grep_here({"pattern": "x", "path": MAC_ROOT, "computer": "Studio Mac"})
    assert content == f"there:x:{MAC_ROOT}"


MAC_HOME = "/Users/studio"
OWN_MAC = ComputerTarget(
    computer_id=MAC.computer_id,
    name=MAC.name,
    session=SimpleNamespace(sandbox=_mac_sandbox()),
    project=ProjectContext(
        workspace_id="",
        dir_name=None,
        sibling_dir_names=("semis",),
        machine=MachinePaths(home=MAC_HOME),
    ),
    root=MAC_ROOT,
)


@pytest.mark.parametrize(
    ("path", "whole"),
    [
        ("~", MAC_HOME),
        ("~/Documents/a.pdf", f"{MAC_HOME}/Documents/a.pdf"),
        ("~other/a.pdf", "~other/a.pdf"),
        ("/Users/studio/Desktop/b.png", "/Users/studio/Desktop/b.png"),
        ("semis/agent.md", f"{MAC_ROOT}/semis/agent.md"),
    ],
)
def test_on_a_machine_of_the_users_own_tilde_is_the_login_home(path, whole):
    sandbox = OWN_MAC.session.sandbox
    assert sandbox.normalize_path(path, OWN_MAC.project) == whole


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    ["~/Documents/a.pdf", "/Users/studio/Documents/a.pdf", "/etc/hosts", f"{MAC_ROOT}/semis/agent.md"],
)
async def test_the_file_tools_reach_the_whole_of_a_machine_of_the_users_own(path):
    tools = _tools(_Reach(OWN_MAC), build=lambda _t: {"Read": _read("there")})
    msg = await _call(tools.wrap(_read("here")), {"file_path": path, "computer": "Studio Mac"})
    assert msg.content == f"there:{path}:None"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "says"),
    [
        ("~other/a.pdf", 'ERROR: `~other` is not expanded on computer "Studio Mac". Write the path whole.'),
        (f"{MAC_ROOT}/_internal/x", f'ERROR: The file tools do not reach {MAC_ROOT}/_internal/x on computer "Studio Mac".'),
        (".agents/user/memory/notes.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        ("/mnt/livefs/user/memory/notes.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        ("/mnt/.livefs/3/user/memory/notes.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        # Linux reads a leading `//` as `/`, which kept as written walked past
        # every prefix test above.
        ("//mnt/livefs/user/memory/notes.md", "ERROR: Memory, memos, the profile, automations and workflows are the same on every computer"),
        (f"/{MAC_ROOT}/_internal/x", f'ERROR: The file tools do not reach /{MAC_ROOT}/_internal/x on computer "Studio Mac".'),
    ],
)
async def test_a_machine_of_the_users_own_still_refuses_what_no_turn_may_touch(path, says):
    tools = _tools(_Reach(OWN_MAC), build=lambda _t: {"Read": _read("there")})
    msg = await _call(tools.wrap(_read("here")), {"file_path": path, "computer": "Studio Mac"})
    assert says in msg.content


def test_a_search_there_reads_tilde_as_the_login_home():
    sandbox = OWN_MAC.session.sandbox
    sandbox._normalize_search_path = lambda p: _paths._normalize_search_path(sandbox, p)
    with project_bound(OWN_MAC.project):
        assert sandbox._normalize_search_path("~/Documents") == f"{MAC_HOME}/Documents"
        assert sandbox._normalize_search_path(".") == MAC_ROOT


@pytest.mark.asyncio
async def test_a_call_running_on_another_computer_keeps_counting_as_use(monkeypatch):
    """Stamped only as it reached the computer, a foreground call longer than
    the idle sweep's wait was stopped under it."""
    monkeypatch.setattr(computer_reach, "_IN_USE_EVERY_S", 0.01)
    reach = _Reach()

    async def slow(**kwargs):
        await asyncio.sleep(0.05)
        return "there:done", {"mcp_trace": []}

    long_bash = StructuredTool(
        name="Bash",
        description="run",
        args_schema=_bash("x").args_schema,
        coroutine=slow,
        response_format="content_and_artifact",
    )
    tools = _tools(reach, build=lambda _t: {"Bash": long_bash})
    msg = await _call(tools.wrap(_bash("here")), {"command": "sleep 600", "computer": "Studio Mac"})
    assert msg.content == "there:done"
    assert len(reach.counted) >= 2
    assert set(reach.counted) == {MAC.computer_id}

    # Done, it counts no more.
    counted = len(reach.counted)
    await asyncio.sleep(0.03)
    assert len(reach.counted) == counted
