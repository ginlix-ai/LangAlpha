"""What reaches a Claude Code turn while it runs.

The agent's steering takes in what is sent into the thread for the run, as it
does before each model call of a langalpha turn. A Claude Code turn reads it
in the hook the CLI calls as each of langalpha's tool calls returns, which is
also where a result MCP cannot carry is handed to the CLI in the form Claude
reads, and as a query ends, where it decides as the orchestrator does whether
the turn goes on.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.config import get_stream_writer

from ptc_agent.agent.claude_code.tools_bridge import ToolResults
from ptc_agent.agent.middleware.background_subagent.utils import build_message_checker, config_own_run_id
from ptc_agent.agent.middleware.steering import SteeringMiddleware, announce, opened_with
from ptc_agent.agent.transcript.steer_text import report_text, sender_block, sender_runs, sent_steers


class TurnSteering:
    """The agent's ``SteeringMiddleware`` as a Claude Code turn reads it.

    ``alert_senders`` are the notices nobody sent that bring a turn back as
    its query ends, as they do a langalpha turn
    (``BackgroundSubagentOrchestrator``), where the steering takes reports.
    Without steering (a turn built with no agent) nothing is taken.
    """

    def __init__(self, steering: SteeringMiddleware | None, alert_senders: frozenset[str] = frozenset()) -> None:
        self._steering = steering
        self._alert_senders = alert_senders if steering is not None and "report" in steering.kinds else frozenset()

    async def take(self, config: RunnableConfig) -> HumanMessage | None:
        """What was sent into the thread for this run since it last looked, held for it."""
        if self._steering is None:
            return None
        return await self._steering.take_message(config["configurable"]["thread_id"], config_own_run_id(config))

    async def waiting(self, config: RunnableConfig) -> list[str]:
        """Who sent what waits that a langalpha turn would re-enter its agent
        for, by the orchestrator's own check; empty when nothing would."""
        if self._steering is None:
            return []
        checker = await build_message_checker(
            config["configurable"]["thread_id"], own_run_id=config_own_run_id(config), alert_senders=self._alert_senders
        )
        return await checker() if checker is not None else []

    def delivered(self, config: RunnableConfig, message: HumanMessage) -> None:
        """Tell the stream a message the turn took in was delivered."""
        announce(get_stream_writer(), config["configurable"]["thread_id"], message.additional_kwargs["steering_delivered"])

    def opened(self, state: Mapping[str, Any], config: RunnableConfig) -> None:
        """Show the steers the turn opened with, ahead of its input, as delivered."""
        if self._steering is None:
            return
        input_id = config["configurable"].get("steer_input_message_id")
        for delivered in opened_with(state, input_id, config_own_run_id(config)):
            announce(get_stream_writer(), config["configurable"]["thread_id"], delivered)

    async def read(self, messages: Sequence[Any]) -> None:
        """Close the rows the messages Claude was sent since it last spoke
        hold, as a langalpha model call does: they are checkpointed, and the
        CLI has them."""
        if self._steering is not None:
            await self._steering.confirm_read(messages)


class AfterTool:
    """The hook the CLI calls as each of our calls returns, before it goes on.

    A result holding a block MCP cannot carry (a PDF) is handed back as the
    blocks a langalpha turn sends Claude, which the CLI records as the
    result. Once every call of a ``tools`` step's batch is in, what was sent
    into the thread while the agent worked is taken in and handed to the CLI,
    which places it after the batch's results: each steer as a message sent
    mid-turn (``send``), which the CLI frames as the user's, and any report
    ahead of them as the hook's context, which it frames as a tool hook's.
    The CLI waits for the hook, so nothing is taken after it has moved on,
    and the step commits what was taken with those results.
    """

    def __init__(self, results: ToolResults, send: Callable[[str], Awaitable[None]]) -> None:
        self.results = results
        self.send = send
        self._batch: frozenset[str] = frozenset()
        self._returned: set[str] = set()
        self._take: Callable[[], Awaitable[HumanMessage | None]] | None = None
        self._taking: asyncio.Future[HumanMessage | None] | None = None

    def open(self, ids: Sequence[str], take: Callable[[], Awaitable[HumanMessage | None]]) -> None:
        """Start a batch, with what takes in what waits once its last call is in."""
        self._batch, self._returned, self._take, self._taking = frozenset(ids), set(), take, None

    def close(self) -> None:
        """Take nothing more, and hand the CLI nothing a take under way
        returns: the run is ending, and a CLI handed a steer now would start
        another request for it."""
        self._take = None

    @property
    def took(self) -> bool:
        """Whether the batch has started taking in what waits."""
        return self._taking is not None

    async def taken(self) -> HumanMessage | None:
        """What the batch took in, once it has."""
        return await self._taking if self._taking is not None else None

    async def __call__(self, hook_input: Mapping[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if tool_use_id is None:
            return {}
        event = hook_input.get("hook_event_name")
        out: dict[str, Any] = {"hookEventName": event}
        if event == "PostToolUse" and (blocks := self.results.blocks(tool_use_id)) is not None:
            out["updatedMCPToolOutput"] = blocks
        self._returned.add(tool_use_id)
        if self._take is not None and self._taking is None and all(
            t in self._returned or self.results.refused(t) for t in self._batch
        ):
            self._taking = asyncio.ensure_future(self._take())
            if (message := await self._taking) is not None and self._take is not None:
                if reports := report_text(message):
                    out["additionalContext"] = reports
                for text in mid_turn_messages(message):
                    await self.send(text)
        return {"hookSpecificOutput": out} if len(out) > 1 else {}


def mid_turn_messages(message: HumanMessage) -> list[str]:
    """The steers a steering message carries, as the CLI is handed them to
    deliver mid-turn in its own words: each of the user's as written, and
    each run of another sender's in the line naming who sent it."""
    texts: list[str] = []
    for sender, contents in sender_runs((s.sender, s.text) for s in sent_steers(message)):
        if sender == "user":
            texts.extend(contents)
        else:
            texts.append(sender_block(sender, contents))
    return texts
