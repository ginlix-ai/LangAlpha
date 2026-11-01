"""What reaches a Claude Code turn while it runs, and what holds it.

The agent's steering takes in what is sent into the thread for the run, as it
does before each model call of a langalpha turn. A Claude Code turn reads it
in the hook the CLI calls as each of langalpha's tool calls returns, which is
also where a result MCP cannot carry is handed to the CLI in the form Claude
reads, and as a query ends, where it decides as the orchestrator does whether
the turn goes on.

The hook the CLI calls before each call is where a turn stops for the user:
a call that may wait on them, or one the credit gate stops, is deferred, so
the CLI ends its query with the call open instead of holding it across the
pause, and asks for it again once the turn resumes.
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
from ptc_agent.agent.middleware.credit_gate import current_credit_gate
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


#: Whether a call, by name and arguments, may wait for the user.
Waits = Callable[[str, Mapping[str, Any]], bool]

#: What a call that would wait beside another one in its batch is answered
#: with: the CLI resumes one deferred call, so a batch holds at most one.
NOT_RUN = "Not run: only one call can wait for the user at a time. Call this again after the user answers."


def _decision(decision: str, reason: str | None = None) -> dict[str, Any]:
    out = {"hookEventName": "PreToolUse", "permissionDecision": decision}
    return {"hookSpecificOutput": {**out, "permissionDecisionReason": reason} if reason else out}


def credit_stops() -> bool:
    """Whether the turn's credit gate says stop, as it does at a langalpha
    model boundary; the CLI's next request is the one a call leads to."""
    gate = current_credit_gate.get()
    return gate is not None and gate.kind == "run" and gate.lease.should_stop(gate.lease.spend())


class BeforeTool:
    """The hook the CLI calls before each of our calls runs.

    The first call of a query that may wait for the user (``waits``), or
    that the credit gate stops, is deferred: the CLI ends the query with it
    open, and a resumed CLI asks for it again. The CLI resumes one deferred
    call only, so another call of that batch that would wait is answered as
    not run, and one only the gate stops runs, a batch past the boundary as
    the gate allows. A call that ends the turn (``ends_turn_call``) gets past
    the gate, as a langalpha turn's quiet end does: no request follows it.
    When its tool refuses to end the turn, the CLI stops at the result and the
    turn at the gate (``ToolResults.owes_gate``). A call whose result is
    already here, the one a resumed CLI asks for again, runs.
    """

    def __init__(self, results: ToolResults, waits: Waits, ends_turn_call: Waits) -> None:
        self.results = results
        self.waits = waits
        self.ends_turn_call = ends_turn_call
        self._deferred: str | None = None

    def reopen(self) -> None:
        """The turn sent the CLI another query, which may defer a call of its own."""
        self._deferred = None

    async def __call__(self, hook_input: Mapping[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if tool_use_id is None or self.results.holds(tool_use_id):
            return {}
        name, args = hook_input.get("tool_name") or "", hook_input.get("tool_input") or {}
        waits = self.waits(name, args)
        if not waits and not credit_stops():
            return {}
        if not waits and self.ends_turn_call(name, args):
            self.results.pass_gate(tool_use_id)
            return {}
        if self._deferred is None:
            self._deferred = tool_use_id
            self.results.defer(tool_use_id)
            return _decision("defer")
        return _decision("deny", NOT_RUN) if waits else {}


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
    and the step commits what was taken with those results. A result that
    ends the turn (a quiet wake turn's), or one the credit gate is owed for,
    stops the CLI before it asks Claude again, and takes nothing in: what
    waits is for the next turn, or for the turn once the gate lets it on.
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
        if self.results.ends_turn(tool_use_id) or self.results.owes_gate(tool_use_id):
            return {"continue": False, **({"hookSpecificOutput": out} if len(out) > 1 else {})}
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
