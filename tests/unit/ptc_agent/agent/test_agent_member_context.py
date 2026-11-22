"""Where a member's own context is wired in a build, and where it is not.

Only the main agent of a shared workspace's turn whose member is granted it
gets the middleware, placed inside the naming step and just outside the row
carrier, and its own Read, Write, Edit and Glob the member's memory; Bash and
code refuse the folder, and every other tool, Grep and each subagent's among
them, is built without it. Every other build is wired exactly as before.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

from ptc_agent.agent import agent as agent_module
from ptc_agent.agent.agent import PTCAgent
from ptc_agent.agent.middleware.member_context import (
    MemberContext,
    MemberContextMiddleware,
    MembersClosed,
    close_members,
    member_memory,
)
from ptc_agent.agent.middleware.runtime_context import RowCarrierMiddleware
from ptc_agent.agent.middleware.sender_naming import SenderNamingMiddleware
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.project_context import ProjectContext

HOLDER = "shared-00000000-0000-4000-8000-0000000000aa"
ANA = "a0000000-0000-4000-8000-000000000001"
WORKSPACE = "00000000-0000-4000-8000-00000000aaaa"
MEMBER = MemberContext(
    user_id=ANA, name="Ana", handle="1a2b3c", workspace_id=WORKSPACE, alias_key=b"k" * 32
)
MEMBER_FILE = "/home/workspace/Desk/.agents/members/1a2b3c/memory/codeword.md"


def _build(**kwargs) -> dict:
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    llm = GenericFakeChatModel(messages=iter([]))
    built: dict = {"agents": []}

    def fake_create_agent(_model, **kwargs):
        built["agents"].append(kwargs)
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    spies = {
        "bash": MagicMock(wraps=agent_module.create_execute_bash_tool),
        "code": MagicMock(wraps=agent_module.create_execute_code_tool),
        "tasks": MagicMock(wraps=agent_module.task_middleware),
        "compiler": MagicMock(wraps=agent_module.subagent_compiler),
    }
    sandbox = MagicMock(computer_root="/home/workspace", mounts_livefs=True)
    sandbox.workspace.return_value.workspace = "/home/workspace/Desk"
    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=fake_create_agent),
        patch("ptc_agent.agent.agent.create_execute_bash_tool", spies["bash"]),
        patch("ptc_agent.agent.agent.create_execute_code_tool", spies["code"]),
        patch("ptc_agent.agent.agent.task_middleware", spies["tasks"]),
        patch("ptc_agent.agent.agent.subagent_compiler", spies["compiler"]),
    ):
        PTCAgent(config).create_agent(
            sandbox=sandbox,
            mcp_registry=MagicMock(),
            llm=llm,
            thread_id="00000000-0000-4000-8000-00000000bbbb",
            project=ProjectContext(WORKSPACE, "Desk"),
            user_id=ANA,
            owner_id=HOLDER,
            **{"store": MagicMock(), **kwargs},
        )
    main = next(a for a in built["agents"] if a.get("state_schema") is not None)
    return {"main": main, **spies}


def _member_middleware(main: dict) -> list:
    return [m for m in main["middleware"] if isinstance(m, MemberContextMiddleware)]


def test_a_granted_turn_wires_it_into_the_main_agent_alone():
    built = _build(own_data=False, member_context=MEMBER)
    stack = built["main"]["middleware"]
    [middleware] = _member_middleware(built["main"])
    # Inside the naming step, so the block follows the message's tag, and
    # just outside the row carrier, so what it adds never reaches state.
    at = stack.index(middleware)
    assert isinstance(stack[at - 1], SenderNamingMiddleware)
    assert isinstance(stack[at + 1], RowCarrierMiddleware)
    assert middleware.member is MEMBER

    for spy in (built["bash"], built["code"]):
        assert ".agents/members" in spy.call_args.kwargs["file_tools_only"]

    # The main agent's own Read, Write, Edit and Glob reach it; no other
    # tool does, Grep and the subagents' among them.
    mount = middleware._mount
    main_tools = {t.name: t for t in built["main"]["tools"]}
    default_tools = {t.name: t for t in built["tasks"].call_args.kwargs["tools"]}
    sub_tools = {t.name: t for t in built["compiler"].call_args.kwargs["filesystem"]}
    assert [main_tools[n] for n in ("Read", "Write", "Edit", "Glob")] == [
        mount.read, mount.write, mount.edit, mount.glob
    ]
    assert main_tools["Grep"] is default_tools["Grep"]
    for name in ("Read", "Write", "Edit", "Glob", "Grep"):
        assert default_tools[name] not in (mount.read, mount.write, mount.edit, mount.glob)
        assert sub_tools[name] not in (mount.read, mount.write, mount.edit, mount.glob)
    # In the order a build without it has them.
    assert [t.name for t in built["main"]["tools"]] == [t.name for t in _build(own_data=False)["main"]["tools"]]
    assert stack is not built["tasks"].call_args.kwargs["middleware"]
    assert not [m for m in built["tasks"].call_args.kwargs["middleware"] if isinstance(m, MemberContextMiddleware)]


def test_any_other_build_has_none_of_it():
    for kwargs in ({"own_data": False}, {}):
        built = _build(**kwargs)
        assert _member_middleware(built["main"]) == []


def test_a_shared_build_closes_the_members_folder_to_every_tool():
    """Whether or not a member's own context rides the turn: nothing left on
    the disk there passes for a member's memory."""
    for kwargs, shared in (
        ({"own_data": False}, True),
        ({"own_data": False, "member_context": MEMBER}, True),
        ({}, False),
    ):
        closing = MagicMock(wraps=agent_module.close_members)
        with patch.object(agent_module, "close_members", closing):
            built = _build(**kwargs)
        assert closing.called is shared
        for spy in (built["bash"], built["code"]):
            assert (".agents/members" in spy.call_args.kwargs["file_tools_only"]) is shared


def test_the_folder_is_closed_with_no_store_too():
    closing = MagicMock(wraps=agent_module.close_members)
    with patch.object(agent_module, "close_members", closing):
        _build(own_data=False, store=None)
    assert closing.called and closing.call_args.kwargs["store"] is None


def test_the_closed_folder_routes_all_but_the_members_own():
    from ptc_agent.agent.backends import CompositeFilesystemBackend
    from ptc_agent.core.paths import WorkspaceLayout

    layout = MagicMock(spec=WorkspaceLayout, workspace="/home/workspace/Desk")
    sandbox = MagicMock()
    sandbox.normalize_path.side_effect = lambda path: path
    closed = close_members(
        CompositeFilesystemBackend(sandbox=sandbox, routes=[]),
        store=MagicMock(),
        sandbox=sandbox,
        layout=layout,
    )
    other = "/home/workspace/Desk/.agents/members/zzzzzz/memory/x.md"
    assert isinstance(closed.route_for(other), MembersClosed)
    assert isinstance(closed.route_for("/home/workspace/Desk/.agents/members"), MembersClosed)
    # A sibling of the folder is the disk's.
    for sibling in (".agents/members-old/report.md", ".agents/membership.json"):
        assert closed.route_for(f"/home/workspace/Desk/{sibling}") is None
    mount = member_memory(MEMBER, store=MagicMock(), files=closed, sandbox=sandbox, layout=layout)
    own = f"/home/workspace/Desk/{MEMBER.folder}memory.md"
    assert mount.files.route_for(own) is mount.memory
    assert isinstance(mount.files.route_for(other), MembersClosed)


def test_a_claude_code_turn_has_none_of_it():
    """Its runtime takes no per-call context: no block to add, no read to
    fill back in. The build is the one place that refuses it."""
    from tests.unit.ptc_agent.agent.claude_code.test_agent import _built

    with (
        patch.object(agent_module, "MemberContextMiddleware") as middleware,
        patch.object(agent_module, "member_memory") as mount,
    ):
        _built(
            member_context=MEMBER,
            own_data=False,
            owner_id=HOLDER,
            store=MagicMock(),
            project=ProjectContext(WORKSPACE, "Desk"),
        )
    middleware.assert_not_called()
    mount.assert_not_called()
