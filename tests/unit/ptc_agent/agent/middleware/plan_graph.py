"""One harness for the plan's middleware: the real ``create_agent`` graph,
driven by a model that plays back a script, with the real ``UpdatePlan`` and
a stand-in ``Bash``."""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import interrupt

from ptc_agent.agent.middleware.plan import PlanStateMiddleware
from ptc_agent.agent.middleware.plan_close_out import PlanCloseOutMiddleware
from ptc_agent.agent.middleware.runtime_context.durable import runtime_update_from_message
from ptc_agent.agent.tools.plan import UpdatePlanTool


class Script(BaseChatModel):
    """Plays back ``replies``, streaming each one's text a word at a time and
    then its calls, as a provider does. An exception in the script is raised
    in that call's place. ``sent`` keeps the messages each call was sent."""

    replies: list[Any]
    calls: int = 0
    sent: list[list[BaseMessage]] = []

    @property
    def _llm_type(self) -> str:
        return "script"

    def bind_tools(self, tools, **kwargs):
        return self

    def _next(self, messages: list[BaseMessage]) -> AIMessage:
        self.sent = [*self.sent, list(messages)]
        reply = self.replies[self.calls]
        self.calls += 1
        if isinstance(reply, BaseException):
            raise reply
        return reply

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=self._next(messages))])

    def _stream(self, messages, stop=None, run_manager=None, **kwargs) -> Iterator[ChatGenerationChunk]:
        reply = self._next(messages)
        for word in str(reply.content).split(" ") if reply.content else ():
            chunk = ChatGenerationChunk(message=AIMessageChunk(content=word + " ", id=reply.id))
            if run_manager:
                run_manager.on_llm_new_token(word, chunk=chunk)
            yield chunk
        yield ChatGenerationChunk(
            message=AIMessageChunk(
                content="",
                id=reply.id,
                tool_call_chunks=[
                    {"name": c["name"], "args": json.dumps(c["args"]), "id": c["id"], "index": i}
                    for i, c in enumerate(reply.tool_calls)
                ],
            )
        )


@tool
def Bash(command: str = "") -> str:  # noqa: N802 - the agent's own tool name
    """Run a command."""
    return "ran"


@tool
def Ask(question: str = "") -> str:  # noqa: N802 - a tool that waits for the user
    """Ask the user."""
    return str(interrupt(question))


def pending(value: bool = False):
    async def work_pending(state) -> bool:
        return value

    return work_pending


def plan_call(args: dict, id: str = "p1", text: str = "") -> AIMessage:
    return AIMessage(content=text, id=f"m-{id}", tool_calls=[{"name": "UpdatePlan", "args": args, "id": id}])


def bash(n: int, start: int = 0) -> list[AIMessage]:
    return [
        AIMessage(content="", id=f"b-msg{i}", tool_calls=[{"name": "Bash", "args": {"command": "ls"}, "id": f"b{i}"}])
        for i in range(start, start + n)
    ]


def answer(text: str = "The answer.", id: str = "m-answer") -> AIMessage:
    return AIMessage(content=text, id=id)


#: A plan with its first step running and its second to come.
PLAN = {"steps": [{"title": "Gather", "status": "in_progress"}, {"title": "Write", "status": "pending"}]}


@dataclass
class Turn:
    final: dict[str, Any]
    #: The text the stream carried.
    streamed: str
    model: Script


@dataclass
class Thread:
    """A thread whose turns each run on an agent built for them, as a
    request's agent is, sharing one checkpointer."""

    saver: InMemorySaver = field(default_factory=InMemorySaver)
    id: str = "t"

    def agent(self, model: Script, *middleware: Any, tools: Sequence[Any] = ()) -> Any:
        return create_agent(
            model,
            tools=[UpdatePlanTool(), Bash, *tools],
            middleware=list(middleware),
            checkpointer=self.saver,
        )

    async def run(self, agent: Any, model: Script, input: Any) -> Turn:
        streamed: list[str] = []
        final: dict[str, Any] = {}
        config = {"configurable": {"thread_id": self.id}}
        async for mode, data in agent.astream(input, config, stream_mode=["messages", "values"]):
            if mode == "messages":
                message, _meta = data
                if isinstance(message, (AIMessage, AIMessageChunk)) and isinstance(message.content, str):
                    streamed.append(message.content)
            else:
                final = data
        return Turn(final, "".join(streamed), model)

    async def turn(self, *replies: Any, middleware: Sequence[Any] | None = None, text: str = "go",
                   is_pending: bool = False, tools: Sequence[Any] = ()) -> Turn:
        """A turn of the user's ``text`` the model plays out as ``replies``,
        on an agent with the plan's middleware as the agent's stack mounts it
        (the close-out first, the outermost after-model hook) unless given."""
        model = Script(replies=list(replies))
        if middleware is None:
            middleware = [PlanCloseOutMiddleware(pending(is_pending)), PlanStateMiddleware()]
        agent = self.agent(model, *middleware, tools=tools)
        return await self.run(agent, model, {"messages": [{"role": "user", "content": text}]})


async def turn(*replies: Any, **kwargs: Any) -> Turn:
    """One turn on a thread of its own."""
    return await Thread().turn(*replies, **kwargs)


def rows(messages: list, kind: str) -> list[int]:
    """Where rows of ``kind`` sit, counted in tool results before each."""
    results = 0
    at: list[int] = []
    for message in messages:
        if isinstance(message, ToolMessage):
            results += 1
        elif (row := runtime_update_from_message(message)) is not None and row.kind == kind:
            at.append(results)
    return at


def row_texts(messages: list, kind: str) -> list[str]:
    return [m.content for m in messages if (r := runtime_update_from_message(m)) is not None and r.kind == kind]


def texts(messages: list) -> list[str]:
    return [m.content.strip() for m in messages if isinstance(m, AIMessage) and isinstance(m.content, str) and m.content]
