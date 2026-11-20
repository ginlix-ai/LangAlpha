"""What reaches a Claude Code turn while it runs, and what holds it.

The agent's steering takes in what is sent into the thread for the run, as it
does before each model call of a langalpha turn. A Claude Code turn reads it
in the hook the CLI calls as each of langalpha's tool calls returns, which is
also where a result MCP cannot carry is handed to the CLI in the form Claude
reads, and as a query ends, where it decides as the orchestrator does whether
the turn goes on.

The hook the CLI calls before each call is where a turn stops for the user:
a call that may wait on them is deferred, so the CLI ends its query with the
call open instead of holding it across the pause, and asks for it again once
the turn resumes. The credit gate stops a turn where a langalpha turn's does,
before its next request: once a batch's results are in, and before a query
the turn sends.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langchain_core.runnables import RunnableConfig
from langgraph.config import get_stream_writer

from ptc_agent.agent.claude_code.tools_bridge import ToolResults
from ptc_agent.agent.middleware.background_subagent.steering import SubagentSteeringMiddleware
from ptc_agent.agent.middleware.background_subagent.utils import build_message_checker, config_own_run_id
from ptc_agent.agent.middleware.credit_gate import account_ends, credit_stops
from ptc_agent.agent.middleware.steering import SteeringMiddleware, announce, opened_with
from ptc_agent.agent.transcript.sender import HARNESS_NAMED_KEY, escape_unrecorded
from ptc_agent.agent.transcript.steer_text import (
    SteerSender,
    report_text,
    sender_block,
    sender_runs,
    sent_steers,
)

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.plan import PlanWatch


@dataclass(frozen=True)
class Query:
    """What the turn lets one of its queries do. Every query is ``ORDINARY``
    but one the turn sends of its own after an answer, whose shape is stated
    here once rather than as flags across the hooks and steps that read it
    (``_Run.query``).
    """

    #: The tools whose calls run; None runs every call. Any other call is
    #: refused before it runs.
    admits: frozenset[str] | None = None
    #: Whether the CLI stops once a batch's results are in rather than asking
    #: Claude again, so Claude is never asked about a refusal.
    last_batch: bool = False
    #: Whether its replies reach the stream, and ``on_reply``.
    streamed: bool = True
    #: What of each reply the thread keeps; None keeps it whole. A reply kept
    #: with no calls is not kept at all.
    keep: Callable[[AIMessage], AIMessage] | None = None
    #: What takes the query out of the thread where nothing of it is kept: a
    #: reply kept as nothing, or a query that failed. A query with any is
    #: best-effort: it ends the turn on what came before it rather than
    #: failing a turn whose answer the user already has.
    dropped: tuple[RemoveMessage, ...] = ()
    #: The calls of the newest reply ``keep`` left out, which the CLI answers
    #: itself; set as each reply is read, so the step after it waits for
    #: their records too.
    refused: tuple[str, ...] = ()


#: A query of the turn's own: its prompt, or what was sent into the thread.
ORDINARY = Query()


@dataclass(frozen=True)
class HandOff:
    """What a batch hands the CLI once its last result is in: the rows the
    turn writes then, and what was sent into the thread, with the state the
    rows go with. The step commits it all with the batch's results."""

    rows: tuple[HumanMessage, ...] = ()
    steer: HumanMessage | None = None
    update: Mapping[str, Any] = field(default_factory=dict)

    @property
    def messages(self) -> list[HumanMessage]:
        return [*self.rows, *([self.steer] if self.steer is not None else [])]


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

    async def resume(self, config: RunnableConfig) -> AIMessage | None:
        """The call that resumes the tasks whose runs ended with messages
        they never read, held for this run; None when none waits."""
        if self._steering is None:
            return None
        return await self._steering.resume_call(config["configurable"]["thread_id"], config_own_run_id(config))

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


class TaskSteering:
    """A subagent's ``SubagentSteeringMiddleware`` as a Claude Code run of
    its task reads it, in the places ``TurnSteering`` is read: what is sent
    into the run is taken in after each batch of calls, as it is before each
    model call of a langalpha run, and its take records the delivery on the
    task's stream itself. Nothing brings a query that ended back, as nothing
    brings back a langalpha run that answered."""

    def __init__(self, steering: SubagentSteeringMiddleware) -> None:
        self._steering = steering

    async def take(self, config: RunnableConfig) -> HumanMessage | None:
        return await self._steering.take_message()

    async def waiting(self, config: RunnableConfig) -> list[str]:
        return []

    async def resume(self, config: RunnableConfig) -> AIMessage | None:
        return None

    def delivered(self, config: RunnableConfig, message: HumanMessage) -> None:
        pass

    def opened(self, state: Mapping[str, Any], config: RunnableConfig) -> None:
        pass

    async def read(self, messages: Sequence[Any]) -> None:
        await self._steering.confirm_read(messages)


#: Whether a call, by name and arguments, may wait for the user.
Waits = Callable[[str, Mapping[str, Any]], bool]

#: What a call that would wait beside another one in its batch is answered
#: with: the CLI resumes one deferred call, so a batch holds at most one.
NOT_RUN = "Not run: only one call can wait for the user at a time. Call this again after the user answers."


def _decision(decision: str, reason: str | None = None) -> dict[str, Any]:
    out = {"hookEventName": "PreToolUse", "permissionDecision": decision}
    return {"hookSpecificOutput": {**out, "permissionDecisionReason": reason} if reason else out}


class BeforeTool:
    """The hook the CLI calls before each of our calls runs.

    The first call of a query that may wait for the user (``waits``) is
    deferred: the CLI ends the query with it open, and a resumed CLI asks for
    it again. The CLI resumes one deferred call only, so another call of that
    batch that would wait is answered as not run. Every other call runs, past
    the credit boundary too, as a langalpha turn runs the calls of a round:
    the gate stops the request after them (``AfterTool``). A call whose result
    is already here, the one a resumed CLI asks for again, runs. A call the
    query does not admit (``Query.admits``) is refused, and in a query that
    ends at its batch, the CLI stops once the batch is done: no
    ``PostToolUse`` follows a refusal to say so.
    """

    def __init__(self, results: ToolResults, waits: Waits) -> None:
        self.results = results
        self.waits = waits
        self.query = ORDINARY
        self._deferred: str | None = None

    def reopen(self, query: Query = ORDINARY) -> None:
        """The turn sent the CLI another query, which may defer a call of its own."""
        self._deferred = None
        self.query = query

    async def __call__(self, hook_input: Mapping[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if tool_use_id is None:
            return {}
        if self.query.admits is not None and hook_input.get("tool_name") not in self.query.admits:
            return {**_decision("deny"), **({"continue": False} if self.query.last_batch else {})}
        if self.results.holds(tool_use_id):
            return {}
        if not self.waits(hook_input.get("tool_name") or "", hook_input.get("tool_input") or {}):
            return {}
        if self._deferred is None:
            self._deferred = tool_use_id
            self.results.defer(tool_use_id)
            return _decision("defer")
        return _decision("deny", NOT_RUN)


class AfterTool:
    """The hook the CLI calls as each of our calls returns, before it goes on.

    A result holding a block MCP cannot carry (a PDF) is handed back as the
    blocks a langalpha turn sends Claude, which the CLI records as the
    result. Once every call of a ``tools`` step's batch is in, what was sent
    into the thread while the agent worked is taken in and handed to the CLI,
    which places it after the batch's results: each steer as a message sent
    mid-turn (``send``), which the CLI frames as the user's, and the turn's
    rows (``HandOff``) and any report ahead of them as the hook's context,
    which it frames as a tool hook's, so a row costs no request of its own.
    The CLI waits for the hook, so nothing is taken after it has moved on,
    and the step commits what was taken with those results.

    Whether the CLI stops is decided once the batch's last result is in, so
    every call of the batch runs first, as a langalpha turn's round does. It
    stops before it asks Claude again when a result ended the turn (a quiet
    wake turn's), or when the credit gate stops by then, or the account may no
    longer work (``ToolResults.owes_gate``),
    which a quota refresh can do while a call runs, with no PreToolUse after
    it to catch the CLI's next request, or when the Claude plan is used up
    under Stop (``PlanWatch.gate``), which ends the run there, or when the
    query ends at its batch (``Query.last_batch``). It then takes nothing in:
    what waits is for the next turn, for the turn once the gate lets it on,
    or for the query after.
    """

    def __init__(
        self,
        results: ToolResults,
        send: Callable[[str], Awaitable[None]],
        plan: PlanWatch,
        *,
        shared: bool = False,
    ) -> None:
        self.results = results
        self.send = send
        self.plan = plan
        self.shared = shared
        self._batch: frozenset[str] = frozenset()
        self._returned: set[str] = set()
        self.query = ORDINARY
        self._take: Callable[[], Awaitable[HandOff]] | None = None
        self._taking: asyncio.Future[HandOff] | None = None

    def reopen(self, query: Query = ORDINARY) -> None:
        """The turn sent the CLI another query."""
        self.query = query

    def open(self, ids: Sequence[str], take: Callable[[], Awaitable[HandOff]]) -> None:
        """Start a batch, with what takes in what is handed over once its last
        call is in, asked only where the CLI goes on to Claude."""
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

    async def taken(self) -> HandOff:
        """What the batch took in, once it has."""
        return await self._taking if self._taking is not None else HandOff()

    async def __call__(self, hook_input: Mapping[str, Any], tool_use_id: str | None, context: Any) -> dict[str, Any]:
        if tool_use_id is None:
            return {}
        event = hook_input.get("hook_event_name")
        out: dict[str, Any] = {"hookEventName": event}
        if event == "PostToolUse" and (blocks := self.results.blocks(tool_use_id)) is not None:
            out["updatedMCPToolOutput"] = blocks
        self._returned.add(tool_use_id)
        if not all(t in self._returned or self.results.refused(t) for t in self._batch):
            return {"hookSpecificOutput": out} if len(out) > 1 else {}
        ends = any(self.results.ends_turn(t) for t in self._batch or {tool_use_id})
        at_end = not ends and tool_use_id in self._batch
        plan_stops = at_end and await self.plan.gate()
        if at_end and not plan_stops and (credit_stops() or await account_ends()):
            # The gate's ``before_model`` ahead of the next CLI ends the run
            # for an account that may no longer work.
            self.results.pass_gate(tool_use_id)
        if ends or plan_stops or self.results.owes_gate(tool_use_id) or self.query.last_batch:
            return {"continue": False, **({"hookSpecificOutput": out} if len(out) > 1 else {})}
        if self._take is not None and self._taking is None:
            self._taking = asyncio.ensure_future(self._take())
            handed = await self._taking
            if self._take is not None:
                steer = handed.steer
                texts = [*(row.text for row in handed.rows), report_text(steer) if steer is not None else None]
                if context := "\n\n".join(text for text in texts if text):
                    out["additionalContext"] = context
                for text in mid_turn_messages(steer, shared=self.shared) if steer is not None else ():
                    await self.send(text)
        return {"hookSpecificOutput": out} if len(out) > 1 else {}


def mid_turn_messages(message: HumanMessage, *, shared: bool = False) -> list[str]:
    """The steers a steering message carries, as the CLI is handed them to
    deliver mid-turn in its own words: each of the user's as written, and
    each run of another sender's in the line naming who sent it. A steer no
    member is recorded as writing is escaped beside a member's, as
    ``_steering_message`` escapes it, and anywhere in a shared run, as the API
    path's naming step escapes an unrecorded message (``name_senders``)."""
    named = shared or bool((message.additional_kwargs or {}).get(HARNESS_NAMED_KEY))
    texts: list[str] = []
    for sender, contents in sender_runs(
        (s.steer_sender, s.text if s.member is not None or not named else escape_unrecorded(s.text))
        for s in sent_steers(message)
    ):
        if sender == SteerSender("user"):
            texts.extend(contents)
        else:
            texts.append(sender_block(sender, contents))
    return texts
