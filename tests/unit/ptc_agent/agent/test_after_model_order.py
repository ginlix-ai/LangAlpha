"""What the approval and order hooks see on a langalpha turn.

Both read the newest assistant message and the results after it, which a
Claude Code turn needs: it runs them once the rest of a batch is answered. On a
langalpha turn they run right after the model, and every after-model hook that
runs ahead of them writes no message, so the newest message there is the
model's own and no result follows it: reading it that way changes nothing.
"""

from unittest.mock import MagicMock, patch

from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.tools import StructuredTool

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from ptc_agent.agent.middleware.order_governance import OrderGovernanceMiddleware
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import SystemPinMiddleware
from ptc_agent.config.agent import AgentConfig, LLMConfig
from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig
from src.tools.secretary.approvals import StandingApprovalMiddleware


def _main_stack() -> list:
    from ptc_agent.agent.agent import PTCAgent

    config = AgentConfig(
        llm=LLMConfig(name="test-model"), security=SecurityConfig(), logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")), mcp=MCPConfig(), filesystem=FilesystemConfig(),
    )
    order = StructuredTool.from_function(func=lambda symbol: "placed", name="mcp__broker__place_order", description="t")
    order.metadata = {METADATA_KEY: {"server": "broker", "tool": "place_order", "approval": True,
                                     "order": {"action": "place", "mode": "paper"}}}
    llm = GenericFakeChatModel(messages=iter([]))
    built: list[list] = []

    def capture(_model, **kwargs):
        built.append(list(kwargs["middleware"]))
        graph = MagicMock()
        graph.with_config.return_value = graph
        return graph

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=capture),
    ):
        PTCAgent(config).create_agent(
            llm=llm, thread_id="t-1", user_id="u-1", sandbox=MagicMock(), mcp_registry=MagicMock(),
            role="chief_of_staff", direct_mcp=MagicMock(tools=[order]), order_ledger=MagicMock(),
        )
    return built[0]


def _after_model_order(stack: list) -> list[type]:
    """The after-model hooks in the order a turn runs them: innermost first."""
    hooks = [m for m in stack if type(m).aafter_model is not AgentMiddleware.aafter_model
             or type(m).after_model is not AgentMiddleware.after_model]
    return [type(m) for m in reversed(hooks)]


def test_the_approval_and_order_hooks_run_ahead_of_any_after_model_hook_that_writes_a_message():
    order = _after_model_order(_main_stack())
    assert StandingApprovalMiddleware in order and OrderGovernanceMiddleware in order
    ahead = order[: order.index(OrderGovernanceMiddleware)]
    assert set(ahead) <= {StandingApprovalMiddleware, SystemPinMiddleware}
    assert set(order[: order.index(StandingApprovalMiddleware)]) <= {SystemPinMiddleware}
