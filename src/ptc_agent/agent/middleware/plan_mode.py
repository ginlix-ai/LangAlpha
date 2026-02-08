"""Plan Mode middleware for user-approved task execution.

This middleware adds a `SubmitPlan` tool that requires user approval
before the agent can proceed with execution. It enables a two-phase
workflow: explore/plan, then execute after approval.
"""

from typing import Annotated, Any

from langchain.agents.middleware import InterruptOnConfig
from langchain.agents.middleware.types import AgentMiddleware, AgentState
from langchain_core.messages import ToolCall, ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.runtime import Runtime
from langgraph.types import Command

# Type for injected tool call ID
try:
    from langchain.tools import InjectedToolCallId
except ImportError:
    from langchain_core.tools import InjectedToolCallId


class PlanModeMiddleware(AgentMiddleware):
    """Middleware that adds SubmitPlan tool for plan approval workflow.

    When enabled, this middleware provides a `SubmitPlan` tool that the agent
    should call after exploring the codebase and before executing any write
    operations. The tool triggers a HITL interrupt for user approval.

    Usage:
        middleware = PlanModeMiddleware()
        tools.extend(middleware.tools)

        # Also need to add HITL middleware with interrupt config:
        interrupt_config = create_plan_mode_interrupt_config()
        hitl_middleware = HumanInTheLoopMiddleware(interrupt_on=interrupt_config)
    """

    def __init__(self) -> None:
        """Initialize the plan mode middleware."""
        self._submit_plan_tool = self._create_submit_plan_tool()

    def _create_submit_plan_tool(self) -> BaseTool:
        """Create the SubmitPlan tool."""

        @tool("SubmitPlan")
        def submit_plan(
            description: str,
            tool_call_id: Annotated[str, InjectedToolCallId],
        ) -> Command:
            """Submit a plan for user approval before execution.

            Call this ONCE when your plan is ready for review. After user approval,
            use write_todos to track progress during execution.

            Args:
                description: Detailed description of your plan including what you
                    intend to do, which files you'll modify, and your approach.
            """
            return Command(
                update={
                    "plan_description": description,
                    "messages": [
                        ToolMessage(
                            content="Plan approved. Proceed with execution.",
                            tool_call_id=tool_call_id,
                        ),
                    ],
                }
            )

        return submit_plan

    @property
    def tools(self) -> list[BaseTool]:  # type: ignore[override]
        """Return the tools provided by this middleware."""
        return [self._submit_plan_tool]


def format_plan_description(
    tool_call: ToolCall,
    state: AgentState,
    runtime: Runtime[Any],
) -> str:
    """Format the SubmitPlan tool call for the approval prompt.

    This function is called by HumanInTheLoopMiddleware to generate
    the description shown to the user when approving/rejecting the plan.

    Args:
        tool_call: The tool call being interrupted
        state: Current agent state
        runtime: LangGraph runtime

    Returns:
        Formatted description string for the approval UI
    """
    args = tool_call.get("args", {})
    return args.get("description", "No description provided")


def create_plan_mode_interrupt_config() -> dict[str, InterruptOnConfig]:
    """Create the HITL interrupt configuration for plan mode.

    Returns:
        Dictionary mapping tool names to their interrupt configurations.
        Only includes 'SubmitPlan' which triggers approval.
    """
    return {
        "SubmitPlan": InterruptOnConfig(
            allowed_decisions=["approve", "reject"],
            description=format_plan_description,
        )
    }
