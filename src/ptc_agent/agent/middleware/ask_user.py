"""AskUserQuestion middleware for structured user input during agent execution.

Allows the agent to pause execution, present a question with selectable
options to the user, and receive their answer via the existing HITL
interrupt/resume wire format.
"""

from typing import Annotated

from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, tool
from langgraph.types import Command, interrupt

from src.server.models.chat import rejection_feedback

try:
    from langchain.tools import InjectedToolCallId
except ImportError:
    from langchain_core.tools import InjectedToolCallId


class AskUserMiddleware(AgentMiddleware):
    """Middleware that provides AskUserQuestion tool.

    Calls ``interrupt()`` directly so the user's answer flows back as
    the return value (unlike HumanInTheLoopMiddleware which swallows
    the response on "approve").
    """

    def __init__(self) -> None:
        self._ask_user_tool = self._create_ask_user_tool()

    def _create_ask_user_tool(self) -> BaseTool:
        @tool("AskUserQuestion")
        def ask_user_question(
            question: str,
            options: list[str],
            allow_multiple: bool = False,
            tool_call_id: Annotated[str, InjectedToolCallId] = "",
        ) -> Command:
            """Ask the user a question and wait for their answer.

            Use this when you need user input to proceed — choosing between
            approaches, confirming preferences, or getting a decision.
            The user can pick from the provided options or type a custom answer.

            Args:
                question: The question to ask. Be clear and specific.
                options: 2-4 short option labels for the user to choose from.
                allow_multiple: If True, user can select multiple options.
            """
            # Pause graph — value is sent to frontend as interrupt SSE event
            response = interrupt(
                {
                    "action_requests": [
                        {
                            "type": "ask_user_question",
                            "question": question,
                            "options": options,
                            "allow_multiple": allow_multiple,
                        }
                    ]
                }
            )

            # Extract answer from hitl_response format:
            # {"decisions": [{"type": "approve", "message": "answer"}]}
            # or {"decisions": [{"type": "reject", "message": "..."}]} for skip
            answer = ""
            skipped = False
            if isinstance(response, dict):
                decisions = response.get("decisions", [])
                if decisions:
                    decision = decisions[0]
                    if decision.get("type") == "reject":
                        skipped = True
                        answer = rejection_feedback(decision.get("message"))
                    else:
                        answer = decision.get("message", "")
            elif isinstance(response, str):
                answer = response

            if skipped:
                content = "User skipped the question."
                if answer:
                    content += f" They said: {answer}"
            else:
                content = (
                    f"User answered: {answer}" if answer else "User provided no answer."
                )

            return Command(
                update={
                    "messages": [
                        ToolMessage(content=content, tool_call_id=tool_call_id),
                    ],
                }
            )

        return ask_user_question

    @property
    def tools(self) -> list[BaseTool]:
        return [self._ask_user_tool]
