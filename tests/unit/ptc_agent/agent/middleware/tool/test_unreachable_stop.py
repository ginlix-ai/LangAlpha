"""A turn nobody watches ends on the first tool call whose machine did not answer.

The tool hands the failure back as text, so without the stop the agent keeps
calling the model against a Mac that is asleep. The sandbox notes how its
calls ended, and the middleware raises from the connect failure once the tool
returns, which the run's failure funnel stamps as ``computer_unreachable``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware import SubAgentMiddleware, ToolErrorHandlingMiddleware
from ptc_agent.agent.middleware.runtime_context import TurnContext
from ptc_agent.agent.middleware.tool.unreachable_stop import (
    ComputerUnreachableError,
    ComputerUnreachableStopMiddleware,
)
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import (
    ExecResult,
    SandboxHostUnavailableError,
    SandboxRuntime,
)
from src.server.services.runs.stream_errors import unreachable_kind
from tests.unit.core.sandbox.fake_provider import fake_provider


def _sandbox():
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    config = CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=fake_provider("ssh"),
    ):
        sandbox = PTCSandbox(config=config)
    sandbox.runtime = AsyncMock(spec=SandboxRuntime)
    return sandbox


def _asleep() -> SandboxHostUnavailableError:
    """What a running computer's call raises once its reconnect finds the
    machine's tunnel down."""
    try:
        raise SSHConnectError("not_connected", "Work Mac is not connected.")
    except SSHConnectError as e:
        try:
            raise SandboxHostUnavailableError("sb-1", e.for_owner) from e
        except SandboxHostUnavailableError as wrapped:
            return wrapped


async def _call(sandbox, outcome) -> None:
    """One sandbox call, ending as ``outcome`` (an exception or a result)."""
    exec_ = AsyncMock(
        side_effect=outcome if isinstance(outcome, BaseException) else None,
        return_value=outcome,
    )
    await sandbox._runtime_call(
        exec_, "true", retry_policy=RetryPolicy.SAFE, allow_reconnect=False, retries=1
    )


def _request():
    call = {"name": "Bash", "args": {"command": "ls"}, "id": "call-1", "type": "tool_call"}
    return SimpleNamespace(tool_call=call, tool=None)


def _swallowing_tool(*calls):
    """A tool that, like Bash, turns each failed sandbox call into text."""

    async def handler(_request):
        for sandbox, outcome in calls:
            try:
                await _call(sandbox, outcome)
            except Exception as e:
                return ToolMessage(content=f"ERROR: {e}", tool_call_id="call-1")
        return ToolMessage(content="ok", tool_call_id="call-1")

    return handler


@pytest.mark.asyncio
async def test_a_call_whose_machine_did_not_answer_ends_the_turn():
    sandbox = _sandbox()
    middleware = ComputerUnreachableStopMiddleware(sandbox)

    with pytest.raises(ComputerUnreachableError) as raised:
        await middleware.awrap_tool_call(
            _request(), _swallowing_tool((sandbox, _asleep()))
        )

    # The run's funnel stamps it from the chain, as a turn failing at its start.
    assert unreachable_kind(raised.value) == {
        "error_kind": "computer_unreachable",
        "error_code": "not_connected",
    }
    assert str(raised.value) == "Work Mac is not connected."


@pytest.mark.asyncio
async def test_the_stop_names_the_computer_that_did_not_answer():
    """Its error frame names that computer, so the card says which machine
    to wake rather than the one the thread's page shows."""
    from ptc_agent.core.sandbox.unreachable import unreachable_computer_id

    sandbox = _sandbox()
    middleware = ComputerUnreachableStopMiddleware(sandbox, computer_id="c-mac")

    with pytest.raises(ComputerUnreachableError) as raised:
        await middleware.awrap_tool_call(
            _request(), _swallowing_tool((sandbox, _asleep()))
        )

    try:
        raise RuntimeError("the run failed") from raised.value
    except RuntimeError as wrapped:
        assert unreachable_computer_id(wrapped) == "c-mac"


@pytest.mark.asyncio
async def test_a_blip_the_call_rode_out_does_not():
    sandbox = _sandbox()
    middleware = ComputerUnreachableStopMiddleware(sandbox)

    async def handler(_request):
        with pytest.raises(SandboxHostUnavailableError):
            await _call(sandbox, _asleep())
        await _call(sandbox, ExecResult("", "", 0))
        return ToolMessage(content="ok", tool_call_id="call-1")

    result = await middleware.awrap_tool_call(_request(), handler)
    assert result.content == "ok"


@pytest.mark.asyncio
async def test_another_computer_asleep_leaves_the_turn_to_the_agent():
    """A Chief of Staff call reaching another of the user's computers fails on
    its own; the turn can still work on its own computer."""
    own, other = _sandbox(), _sandbox()
    middleware = ComputerUnreachableStopMiddleware(own)

    result = await middleware.awrap_tool_call(
        _request(), _swallowing_tool((other, _asleep()))
    )
    assert result.content.startswith("ERROR:")


@pytest.mark.asyncio
async def test_a_machine_that_answered_and_refused_leaves_it_to_the_agent():
    sandbox = _sandbox()
    middleware = ComputerUnreachableStopMiddleware(sandbox)
    refused = SSHConnectError("auth_failed", "Work Mac refused the key.")

    result = await middleware.awrap_tool_call(
        _request(), _swallowing_tool((sandbox, refused))
    )
    assert result.content.startswith("ERROR:")


# ─── Where the stop sits ──────────────────────────────────────────────


def _stacks(turn_context) -> tuple[list, list]:
    """The main agent's middleware and the one every subagent is built on."""
    from ptc_agent.agent.agent import PTCAgent

    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    llm = GenericFakeChatModel(messages=iter([]))
    main: list[list] = []
    handed: list[list] = []
    real = SubAgentMiddleware

    def build(_model, **built):
        main.append(list(built["middleware"]))
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    def spy(*args, **kwargs):
        handed.append(list(kwargs["default_middleware"]))
        return real(*args, **kwargs)

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=build),
        patch("ptc_agent.agent.subagents.assembly.SubAgentMiddleware", side_effect=spy),
    ):
        PTCAgent(config).create_agent(
            llm=llm,
            thread_id="t-1",
            sandbox=MagicMock(),
            mcp_registry=MagicMock(),
            turn_context=turn_context,
        )
    return main[0], handed[0]


def test_a_turn_nobody_watches_stops_outside_the_error_handler():
    """Inside it, the stop would come back to the model as one more result."""
    for stack in _stacks(TurnContext(unwatched=True)):
        kinds = [type(m) for m in stack]
        assert kinds.index(ComputerUnreachableStopMiddleware) < kinds.index(
            ToolErrorHandlingMiddleware
        )


def test_a_turn_someone_sent_leaves_the_failure_to_the_agent():
    for stack in _stacks(TurnContext()):
        assert not any(isinstance(m, ComputerUnreachableStopMiddleware) for m in stack)
