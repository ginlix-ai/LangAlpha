"""read_context_parts(): the composer's context panel asks what the thread's
next call sends ahead of its messages. Counted on request, never in a turn,
and only where a running computer and the agent's own request exist."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.compaction import NoCompaction, Parts
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.core.sandbox.attach_only import SandboxNotRunning, attaching_only

HANDLER = "src.server.handlers.thread_maintenance"
ASK = "ptc_agent.agent.middleware.compaction.ask_between_turns"
PARTS = {"system": 10_000, "tools": 9_000, "skills": 2_000}


@contextmanager
def _thread(*, build=None, ask=None):
    """The handler with the thread's config resolved and its agent built by
    ``build``, answering asks with ``ask``."""
    from src.server.handlers.thread_maintenance import ThreadAgent

    async def built(thread_id, verb, **kwargs):
        built.attach_only = attaching_only()
        built.kwargs = kwargs
        state = MagicMock(values={"messages": []})
        return ThreadAgent(thread_id, MagicMock(), state, [], "ws-1", None, ProjectContext("ws-1", "Macro"))

    built.attach_only = None
    resolved = AsyncMock(return_value=(MagicMock(llm_client=None), "analyst"))
    with (
        patch(f"{HANDLER}._compaction_config", new=resolved),
        patch(f"{HANDLER}._thread_agent", new=build or built) as agent,
        patch(ASK, new=ask or AsyncMock(return_value=PARTS)) as asked,
    ):
        yield agent, asked


@pytest.mark.asyncio
async def test_the_parts_are_asked_of_the_threads_agent_without_starting_its_computer():
    from src.server.handlers.thread_context import read_context_parts

    with _thread() as (agent, asked):
        answer = await read_context_parts("t-1", user_id="u-1")

    assert answer == {"thread_id": "t-1", "parts": PARTS}
    assert isinstance(asked.await_args.args[2], Parts)
    # A panel is not activity: the computer is used only while it runs.
    assert agent.attach_only is True
    assert agent.kwargs["user_id"] == "u-1" and agent.kwargs["role"] == "analyst"
    # It writes nothing, so it reads on the pooled saver, outside any fence.
    assert "checkpointer" not in agent.kwargs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "reason"),
    [(SandboxNotRunning("stopped"), "computer_stopped"), (NoCompaction("off"), "no_compaction")],
    ids=["computer_stopped", "no_compaction"],
)
async def test_a_thread_with_nothing_to_count_says_why(raised, reason):
    from src.server.handlers.thread_context import read_context_parts

    with _thread(build=AsyncMock(side_effect=raised)):
        answer = await read_context_parts("t-1", user_id="u-1")

    assert answer == {"thread_id": "t-1", "parts": None, "reason": reason}


def _claude_code_thread(parts):
    """A Claude Code thread whose turn says its session sends ``parts`` ahead."""
    from src.server.handlers.thread_maintenance import ThreadAgent

    async def built(thread_id, verb, **kwargs):
        built.attach_only = attaching_only()
        turn = MagicMock(ahead_parts=AsyncMock(return_value=parts))
        graph = MagicMock(turn=MagicMock(return_value=turn))
        built.turn = turn
        return ThreadAgent(
            thread_id, graph, MagicMock(values={"messages": []}), [], "ws-1", None, None, claude_code=True
        )

    return built


@pytest.mark.asyncio
async def test_a_claude_code_threads_parts_are_its_sessions_prompt_and_tools():
    from src.server.handlers.thread_context import read_context_parts

    build = _claude_code_thread({"system": 704, "tools": 111})
    with _thread(build=build) as (_, asked):
        answer = await read_context_parts("t-1", user_id="u-1")

    assert answer == {"thread_id": "t-1", "parts": {"system": 704, "tools": 111}}
    assert build.attach_only is True
    build.turn.ahead_parts.assert_awaited_once_with({"messages": []})
    # Claude Code runs no model call of ours to ask.
    asked.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refused_config_is_a_reason_not_an_error():
    # A refused login, a used-up plan, no credits, a revoked key: nothing a
    # panel can act on, whatever the thread runs on.
    from fastapi import HTTPException

    from src.server.handlers.thread_context import read_context_parts

    refused = AsyncMock(side_effect=HTTPException(status_code=402, detail="insufficient credits"))
    with patch(f"{HANDLER}._compaction_config", new=refused):
        answer = await read_context_parts("t-1", user_id="u-1")

    assert answer == {"thread_id": "t-1", "parts": None, "reason": "refused"}
    # A read: a removed model stays on the thread for its next turn to clear.
    refused.assert_awaited_once_with("t-1", "u-1", clear_dead=False)


def test_a_pin_is_counted_as_claude_counts_its_prompt_and_tools(monkeypatch):
    pytest.importorskip("claude_agent_sdk")
    import asyncio
    from types import SimpleNamespace

    from langchain_core.messages import SystemMessage

    from ptc_agent.agent.claude_code.runtime import ClaudeTurn
    from ptc_agent.agent.middleware.compaction import utils
    from ptc_agent.agent.middleware.compaction.utils import count_tokens_claude, count_tokens_tiktoken

    # tiktoken fetches its encoding over the network on first use.
    fake = SimpleNamespace(encode=lambda text: [0] * (len(text) // 4 + 1))
    monkeypatch.setattr(utils, "_get_tiktoken_encoder", lambda: fake)

    prompt = "You research equities and keep the deliverables in the workspace. " * 40
    tools = [{"name": "Bash", "description": "Run a command.", "input_schema": {"type": "object"}, "read_only": False}]
    turn = object.__new__(ClaudeTurn)
    turn.current_pin = lambda: {"system_prompt": prompt, "tools": tools}

    parts = asyncio.run(turn.ahead_parts({}))

    assert list(parts) == ["system", "tools"]
    # Claude's count, which tiktoken's falls about a third short of on prose.
    assert parts["system"] == count_tokens_claude([SystemMessage(prompt)])
    assert parts["system"] > 1.4 * count_tokens_tiktoken([SystemMessage(prompt)])

    # A session on another build's pin launches on its own.
    kept = {"system_prompt": "kept", "tools": []}
    state = {"claude_session": {"session_id": "s-1", "pinned": kept}}
    assert list(asyncio.run(turn.ahead_parts(state))) == ["system"]
