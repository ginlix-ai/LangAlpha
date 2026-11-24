"""The Claude Code runtime: a thread whose turns Claude Code runs.

A turn is a small LangGraph graph over the thread's own checkpoint, shaped
like the graph ``create_agent`` builds: a ``model`` node per assistant
message and a ``tools`` node per batch of tool calls. The difference is who
decides. Claude Code runs the loop in its own process; each ``model`` step
streams one assistant message from it, and each ``tools`` step runs the
calls of that message the CLI asks for through langalpha's own tool node and
hands the results back to the CLI, keeping for the others the answer the CLI
gave Claude itself. Because the graph keeps the node names, the state
schema and the message shapes of a langalpha turn, the SSE producer, the
checkpoint replay, transcripts and the run ledger take a Claude Code turn as
they take any other.

Every step commits the messages it produced together with the session
records the CLI wrote for them (``session.py``), so a checkpoint is always a
point Claude's session can resume from.

The SDK client and the CLI live for one run, in the ``ClaudeTurn`` the
worker that owns the run creates and closes around the graph's stream. It
is execution context only: nothing outside the run reads it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass, replace
from enum import Enum, auto
from functools import partial
from itertools import pairwise
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeVar

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKError,
    ClaudeSDKClient,
    RateLimitEvent,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    ToolResultBlock,
    UserMessage,
)
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, RemoveMessage, ToolCall, ToolMessage
from langchain_core.messages import SystemMessage as PromptMessage
from langchain_core.runnables import RunnableConfig
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.prebuilt import ToolNode
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.claude_code.cli import RuntimeSpec, connected, launch, received, turn_failure
from ptc_agent.agent.claude_code.cli_waits import CliWaits
from ptc_agent.agent.claude_code.compaction import (
    NO_RULES,
    CompactionRules,
    SessionCompaction,
    compact,
    context_signal,
    prompt_tokens,
    report_usage,
)
from ptc_agent.agent.claude_code.launcher import Launcher, Prepared
from ptc_agent.agent.claude_code.mid_turn import ORDINARY, AfterTool, BeforeTool, HandOff, Query, TaskSteering, TurnSteering
from ptc_agent.agent.middleware.credit_gate import account_ends, credit_stops, refuse_ended_account
from ptc_agent.agent.claude_code.plan import PlanWatch
from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay, DeltaJoin, anthropic_content
from ptc_agent.agent.claude_code.session import (
    ENDED,
    RULES_KEY,
    SETTLE_TIMEOUT_S,
    CheckpointSessionStore,
    SessionDrift,
    SessionPin,
    SessionRecords,
    SessionWrite,
    answered_in,
    compacted,
    resume_from,
    records_pin,
    same_pin,
    session_channel,
    unpinned,
)
from ptc_agent.agent.claude_code.tools_bridge import (
    Answered,
    Deferred,
    Ended,
    Run,
    ToolResults,
    tool_specs,
)
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction import parts as context_parts
from ptc_agent.agent.middleware.compaction.brief import (
    BRIEF_CONFIG_KEY,
    kept,
    BRIEF_KEY,
    brief_cut,
    brief_due,
    handoff_mark,
    stored_brief,
)
from ptc_agent.agent.middleware.compaction.utils import count_tokens_claude, stamp_compaction_summary
from ptc_agent.agent.middleware._utils import nostream
from ptc_agent.agent.pending_calls import pending_calls
from ptc_agent.agent.middleware.runtime_context.carrier import compose_request
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from ptc_agent.agent.middleware.steering import SteeringMiddleware
from ptc_agent.agent.middleware.stream_heartbeat import stream_heartbeat
from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.transcript.classify import ORCHESTRATOR_SOURCE, reentry_trigger
from ptc_agent.agent.transcript.sender import name_senders
from ptc_agent.agent.transcript.store import TranscriptTarget
from ptc_agent.agent.prompts import release
from src.llms.claude_code import MESSAGE_ID_KEY as CLAUDE_MESSAGE_ID
from src.llms.claude_code import ClaudeTurnFailed, PlanStopped, another_model_replied_last, last_reply_index
from src.llms.model_identity import CATALOG_MODEL_KEY
from src.tools.secretary.approvals import StandingApprovalState

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.agent_plan import AgentPlan

TURN_KEY = "claude_turn"
T = TypeVar("T")
logger = logging.getLogger(__name__)
#: How long a run's close waits for the CLI to end the query it interrupted.
DRAIN_TIMEOUT_S = 5.0
#: The most queries one turn sends the CLI: its own, then one for each time
#: steers waited as the last ended, as a langalpha turn re-enters its agent
#: (``BackgroundSubagentOrchestrator.max_iterations``).
TURN_QUERIES = 3
TOKEN_CHECK_TIMEOUT_S = 60
#: How a query fails: as its end reads (``turn_failure``), or as the SDK
#: reports a CLI that exited or lost its connection after its init, out of
#: the run's reads and its writes to the CLI.
_QUERY_FAILED = (ClaudeTurnFailed, ClaudeSDKError)



async def token_works(launcher: Launcher, model: str) -> bool:
    """Whether Anthropic serves the token the launcher signs in with: one minimal turn on ``model``, through a turn's own transport.

    ``claude auth status`` reads only local state and reports any string as a
    login, so the check asks Anthropic. A served request or a used-up plan
    means the token is good, a refused login or none at all that it is not;
    anything else raises, so a network failure or a CLI that would not start
    is never read as a bad token, nor as a good one. Over SSH the token is the computer's own or the one relayed to it,
    whichever a turn there would use. What it reports of the plan behind the
    token is kept as a turn's is (``PlanWatch``).
    """
    plan = await PlanWatch.open(launcher, model, enforce=False)
    prepared = await launcher.scratch()
    result: ResultMessage | None = None
    errors: list[tuple[str, str]] = []
    try:
        options = ClaudeAgentOptions(
            tools=[],
            system_prompt="Reply with OK.",
            setting_sources=[],
            strict_mcp_config=True,
            extra_args={"disable-slash-commands": None},
            model=model,
            max_turns=1,
            cwd=prepared.cwd,
            env={"CLAUDE_CONFIG_DIR": prepared.config_dir, "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"},
        )
        client = ClaudeSDKClient(options, transport=launcher.transport(options))
        try:
            async with asyncio.timeout(TOKEN_CHECK_TIMEOUT_S):
                async with connected(client):
                    await client.query("Reply with OK.")
                async for m in received(client):
                    if isinstance(m, AssistantMessage) and m.error:
                        errors.append((m.error, " ".join(getattr(b, "text", "") for b in m.content).strip()))
                    elif isinstance(m, RateLimitEvent):
                        plan.seen(m)
                    elif isinstance(m, ResultMessage):
                        result = m
                        break
        finally:
            # Outside the timeout, and before the dir goes: the close waits
            # for the CLI to exit, which would otherwise write its session
            # into a dir it then recreates.
            with contextlib.suppress(Exception):
                await client.disconnect()
    finally:
        await launcher.release(prepared)
    failure = turn_failure(result, errors, plan)
    if failure is None or failure.kind in ("plan_exhausted", "plan_stopped"):
        return True
    if failure.kind == "auth_failed":
        return False
    raise failure


class ClaudeCodeState(StandingApprovalState, MainAgentState):
    """A Claude Code thread's checkpoint: a main-agent thread's, plus Claude's
    session, the compaction rules its last turn ran under (``RULES_KEY``),
    and the calls approved in advance that the approval step marks. The brief
    a turn opens on after another model's reply (``BRIEF_KEY``) is the main
    agent's, which writes it between turns. The
    main-agent state comes last, so its channels (``messages`` a
    ``DeltaChannel``) win over the plain agent state the approval one extends."""

    claude_session: NotRequired[Annotated[SessionRecords, session_channel()]]
    _claude_code_rules: NotRequired[dict[str, Any]]


class _End(Enum):
    END = auto()


#: The run's end, after its last event.
_END = _End.END


@dataclass(frozen=True)
class _Compacting:
    """The CLI compacting the session ahead of its next request: started,
    failed with ``error``, or done at the ``boundary`` record."""

    boundary: str | None = None
    error: str | None = None


#: What the run's queue carries: the CLI's raw stream events, a compaction
#: it makes, and the run's end.
_Event = dict[str, Any] | _Compacting | Literal[_End.END]


class _Message:
    """One assistant message's raw events, as the CLI streams them."""

    def __init__(self, first: dict[str, Any], run: _Run) -> None:
        self.id: str = (first.get("message") or {}).get("id", "")
        #: Whether the stream reached the message's ``message_stop``.
        self.stopped = False
        #: Whether the CLI cut the stream off and started the message again.
        self.restarted = False
        self._first = first
        self._run = run
        self._join = DeltaJoin(run)

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        item = self._first
        while True:
            item = await self._join.grow(item)
            yield item
            if item.get("type") == "message_stop":
                self.stopped = True
                return
            item = await self._run.next_event()
            if item is _END:
                return
            if isinstance(item, _Compacting):
                self._run.hold(item)
                return
            if item.get("type") == "message_start":
                self.restarted = True
                self._run.hold(item)
                return


class _Run:
    """A started run: the CLI, the SDK client driving it, and what the CLI has said so far."""

    def __init__(self, launcher: Launcher, prepared: Prepared, store: CheckpointSessionStore,
                 client: ClaudeSDKClient, results: ToolResults, spec: RuntimeSpec,
                 before_tool: BeforeTool, after_tool: AfterTool, plan: PlanWatch, *, resumed: bool,
                 sent_at: float | None = None) -> None:
        self.launcher = launcher
        self.prepared = prepared
        self.store = store
        self.client = client
        self.results = results
        self.before_tool = before_tool
        self.after_tool = after_tool
        self.model = spec.model
        #: Whether the CLI resumed a session, rather than starting one of its own.
        self.resumed = resumed
        self.result: ResultMessage | None = None
        #: What the turn's responses said of the Claude plan, and the stop at its limit.
        self.plan = plan
        #: (error, text) of each assistant message the CLI marked as an error.
        self.assistant_errors: list[tuple[str, str]] = []
        self.drift: SessionDrift | None = None
        #: The queries sent the CLI this run: the turn's own, then one for
        #: each time steers waited as the last ended.
        self.queries = 1
        #: What the query running now may do (``again``).
        self.query = ORDINARY
        #: When the CLI was last handed something to send Claude (a query, or
        #: the results of a batch) or reported itself started, on this
        #: server's clock: the request it sends next starts after it.
        self._input_at = time.time() if sent_at is None else sent_at
        #: When the request behind the newest assistant message started, as
        #: near as ``_input_at`` tells it: the input is stamped as this pump
        #: reads it, so it errs late only by the read's delay, against
        #: seconds to a minute of prompt processing that a stamp taken at the
        #: response's start would err by.
        self.last_request_at: float | None = None
        self._events: asyncio.Queue[_Event] = asyncio.Queue()
        self._held: _Event | None = None
        self._streamed: set[str] = set()
        self._assistant: dict[str, list[str]] = {}
        self._closing: asyncio.Future[None] | None = None
        self._finishing: asyncio.Future[None] | None = None
        self._init_seen = False
        #: The stop reason of the assistant message streaming now.
        self._stop_reason: str | None = None
        #: The interrupt of a CLI the plan stopped mid-query (``_read``).
        self._halting: asyncio.Future[None] | None = None
        self._pump = asyncio.create_task(self._read())

    @classmethod
    async def start(cls, spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, thread_key: str,
                    identity: str, records: SessionRecords | None, prompt: Prompt | None,
                    answered: ToolMessage | None = None, *, plan: PlanWatch, compact_at: int = 0,
                    auto_compact: bool = True, transcript_note: str = "") -> _Run:
        """Start the CLI on the turn (``cli.launch``); with no prompt it carries
        on from the tool results that end its session. ``answered`` is the
        result of the call the session holds deferred, which the CLI asks for
        first and is handed at once (``resume_from``). ``plan`` is the turn's
        watch of the Claude plan, which stops the CLI at its limit."""
        store, results = CheckpointSessionStore(), ToolResults(spec.ends_turn)
        if answered is not None:
            results.resolve(answered)
        before_tool = BeforeTool(results, spec.waits)
        query = prompt if prompt is None or isinstance(prompt, str) else _user_message(prompt)
        sent_at = time.time()
        prepared, client, after_tool = await launch(
            spec, pin, launcher, thread_key=thread_key, identity=identity, records=records, store=store,
            results=results, query=query, before_tool=before_tool, compact_at=compact_at, auto_compact=auto_compact,
            transcript_note=transcript_note, plan=plan,
        )
        return cls(launcher, prepared, store, client, results, spec, before_tool, after_tool, plan,
                   resumed=bool((records or {}).get("session_id")), sent_at=sent_at)

    @property
    def deferred(self) -> bool:
        """Whether the query ended with a call deferred, which only a new CLI
        asks for again, or at a result the credit gate is owed for
        (``ToolResults.owes_gate``), which only a new CLI carries on from."""
        return self.result is not None and (self.result.terminal_reason == "tool_deferred" or self.results.owes_gate())

    async def again(self, prompt: Prompt, query: Query = ORDINARY) -> None:
        """Send the turn's next query on the same CLI, once the last one ended
        done, with what ``query`` lets it do. What the CLI said of the last
        one's end goes with it."""
        self.queries += 1
        self.result, self._finishing = None, None
        self.assistant_errors = []
        self.query = query
        self.plan.reopen()
        self.results.reopen()
        self.before_tool.reopen(query)
        self.after_tool.reopen(query)
        self._input_at = time.time()
        await self.client.query(prompt if isinstance(prompt, str) else _user_message(prompt))
        self._pump = asyncio.create_task(self._read())

    async def _read(self) -> None:
        """Read what the CLI says until the turn's result.

        Its init comes ahead of its first request. One naming another model
        than the turn's is a policy refusing it (``availableModels``), one
        no file on the machine shows, such as the organization's account's,
        where the CLI would run another in its place, so the turn fails before
        anything is streamed.

        A reply cut at the output limit the CLI continues by itself, with no
        batch end where the gate could stop it, so where the gate says the
        plan is used up (``PlanWatch.gate``) the CLI is interrupted as the
        reply ends: its continuation would be billed to usage credits.
        """
        try:
            async for m in received(self.client, init_seen=self._init_seen):
                if isinstance(m, SystemMessage) and m.subtype == "init":
                    self._init_seen = True
                    # Its first request starts after this, not the launch's
                    # wait for a CLI slot and the CLI's start.
                    self._input_at = time.time()
                    if m.data.get("model") != self.model:
                        logger.warning("[ClaudeCode] the CLI would run %s in place of %s", m.data.get("model"), self.model)
                        raise ClaudeTurnFailed("model_unavailable", self.model)
                elif isinstance(m, SystemMessage) and m.subtype == "status" and m.data.get("status") == "compacting":
                    await self._events.put(_Compacting())
                elif isinstance(m, SystemMessage) and m.subtype == "status" and m.data.get("compact_result") == "failed":
                    await self._events.put(_Compacting(error=str(m.data.get("compact_error") or "the compaction failed")))
                elif isinstance(m, SystemMessage) and m.subtype == "compact_boundary":
                    await self._events.put(_Compacting(boundary=m.data.get("uuid") or ""))
                elif isinstance(m, StreamEvent):
                    if m.parent_tool_use_id is None:
                        kind = m.event.get("type")
                        if kind == "message_start":
                            self._streamed.add((m.event.get("message") or {}).get("id", ""))
                            self._stop_reason = None
                            self.last_request_at = self._input_at
                        elif kind == "message_delta":
                            self._stop_reason = (m.event.get("delta") or {}).get("stop_reason")
                        await self._events.put(m.event)
                        if kind == "message_stop":
                            await self._unless_continued()
                elif isinstance(m, AssistantMessage):
                    if m.error and m.parent_tool_use_id is None:
                        self.assistant_errors.append((m.error, " ".join(getattr(b, "text", "") for b in m.content).strip()))
                    if m.parent_tool_use_id is None and m.message_id:
                        if not m.error and m.message_id not in self._streamed:
                            self.drift = SessionDrift(f"assistant message {m.message_id} arrived without its stream")
                            break
                        self._assistant.setdefault(m.message_id, []).append(m.uuid)
                elif isinstance(m, UserMessage):
                    blocks = [b for b in (m.content if isinstance(m.content, list) else []) if isinstance(b, ToolResultBlock)]
                    self.results.answered(blocks)
                    if blocks and m.parent_tool_use_id is None:
                        # The CLI records a batch's results as it sends them on.
                        self._input_at = time.time()
                elif isinstance(m, ResultMessage):
                    self.result = m
                    break
                elif isinstance(m, RateLimitEvent):
                    self.plan.seen(m)
                    await self._unless_continued()
                elif isinstance(m, SystemMessage) and m.subtype == "mirror_error":
                    self.drift = SessionDrift(f"session mirror dropped a batch: {m.data.get('error')}")
                    break
        finally:
            self.results.ended()
            await self._events.put(_END)

    async def _unless_continued(self) -> None:
        """Interrupt the CLI where the reply it just streamed was cut at the
        output limit and the gate says the plan is used up. The response's
        report comes after its last event, so both are asked."""
        if self._halting is None and await self.plan.halts(self._stop_reason):
            self._halting = asyncio.ensure_future(self._halt())

    async def _halt(self) -> None:
        with contextlib.suppress(Exception):
            await self.client.interrupt()

    async def _unless_crashed(self, awaitable: Awaitable[T]) -> T:
        """Await something the CLI's side delivers, failing at once if the pump stops first on an error or drift.

        A pump that ended on the turn's result goes on waiting: the mirror can
        still be delivering the last records.
        """
        task = asyncio.ensure_future(awaitable)
        try:
            await asyncio.wait({task, self._pump}, return_when=asyncio.FIRST_COMPLETED)
            if not task.done() and self._pump.done() and not self._pump.cancelled():
                if self._pump.exception() is not None:
                    raise self._pump.exception()
                if self.drift is not None:
                    raise self.drift
            return await task
        finally:
            task.cancel()

    async def next_event(self, *, within: float | None = None) -> _Event | None:
        """The next event, waiting for it at most ``within`` seconds where
        given, None where none came by then."""
        if self._held is not None:
            item, self._held = self._held, None
            return item
        if within is None:
            return await self._events.get()
        try:
            return self._events.get_nowait()
        except asyncio.QueueEmpty:
            if within <= 0:
                return None
        try:
            async with asyncio.timeout(within):
                return await self._events.get()
        except TimeoutError:
            return None

    def hold(self, item: _Event) -> None:
        """Keep an event read past the end of what it was read for, for the next read."""
        self._held = item

    async def next_message(self, compacting: Callable[[_Compacting], Awaitable[None]]) -> _Message | _Compacting | None:
        """The next assistant message, or a compaction the CLI made ahead of
        it once ``compacting`` has taken it in, or None once the turn has
        ended. A compaction's start and failure go to ``compacting`` alone."""
        while (first := await self.next_event()) is not _END:
            if not isinstance(first, _Compacting):
                return _Message(first, self)
            await compacting(first)
            if first.boundary is not None:
                return first
        return None

    async def complete(self, message: _Message) -> None:
        """Fail the turn unless the message streamed whole: a cut-off message is never committed.

        One the CLI cut off and started again is dropped instead; its restart
        is the next message.
        """
        if message.stopped or message.restarted:
            return
        if self._pump.done():
            await self.finish()
        raise ClaudeTurnFailed("api_error", f"assistant message {message.id} ended before its message_stop")

    def assistant_records(self, message_id: str) -> list[str]:
        """The records the CLI wrote for an assistant message it has finished streaming.

        The CLI emits each block's record ahead of the message's ``message_stop``
        and the pump reads in order, so all of them are known once the relay
        has the whole message; none means that order broke.
        """
        uuids = self._assistant.get(message_id)
        if not uuids:
            raise SessionDrift(f"assistant message {message_id} streamed without a record")
        return list(uuids)

    async def tool_result_records(self, tool_use_ids: Sequence[str]) -> list[str]:
        """The records of a batch's results. The CLI writes each once the
        hook returns, so a wait that ran out while the hook took in what
        waits is given the window again from there."""
        wait = partial(self.store.results.wait, tool_use_ids, SETTLE_TIMEOUT_S)
        try:
            return await self._unless_crashed(wait())
        except SessionDrift:
            if not self.after_tool.took:
                raise
            await self.after_tool.taken()
            return await self._unless_crashed(wait())

    async def deferral_record(self, tool_use_id: str) -> str:
        """The record the CLI writes for a call it deferred."""
        [uuid] = await self._unless_crashed(self.store.deferrals.wait([tool_use_id], SETTLE_TIMEOUT_S))
        return uuid

    async def cut_off(self, calls: Sequence[ToolCall]) -> tuple[list[ToolMessage], list[str]]:
        """The results the CLI recorded, resuming, for calls a run before it
        cut off, as ToolMessages, and their records. It records them as it
        loads the session, without a word on its stream. A call it recorded
        none for (it started the session over, or another model made it) is
        answered as a langalpha turn answers one (``PatchToolCallsMiddleware``),
        with no record; a session the CLI starts holds none to wait for."""
        if not self.resumed:
            return [_unrecorded_answer(c) for c in calls], []
        ids = [c["id"] for c in calls]
        with contextlib.suppress(SessionDrift):
            await self._unless_crashed(self.store.results.wait(ids, SETTLE_TIMEOUT_S))
        answers, uuids = [], []
        for c in calls:
            if (recorded := self.store.result_blocks.get(c["id"])) is None:
                answers.append(_unrecorded_answer(c))
                continue
            uuid, block = recorded
            uuids.append(uuid)
            given = ToolResultBlock(tool_use_id=c["id"], content=block.get("content"), is_error=block.get("is_error"))
            answers.append(_as_given(ToolMessage(content="", tool_call_id=c["id"], name=c["name"]), given))
        return answers, uuids

    async def settle(self, uuids: Sequence[str]) -> None:
        """Wait for the records behind a step's messages to reach the store, or fail the turn."""
        if self.drift is not None:
            raise self.drift
        await self._unless_crashed(self.store.arrived.wait(uuids, SETTLE_TIMEOUT_S))
        if self.drift is not None:
            raise self.drift

    async def finish(self) -> None:
        """Wait for the query's end, and fail the turn unless it did its work.

        Once per run, however many calls of a step find the query over: a
        refused token is reported once.
        """
        if self._finishing is None:
            self._finishing = asyncio.ensure_future(self._finish())
        await asyncio.shield(self._finishing)

    async def _finish(self) -> None:
        await self._pump
        if self.drift is not None:
            raise self.drift
        failed = turn_failure(self.result, self.assistant_errors, self.plan)
        if failed is None:
            return
        if failed.kind == "auth_failed":
            await self.launcher.refused()
        if isinstance(failed, PlanStopped):
            await self.plan.settled()
        raise failed

    async def close(self) -> None:
        """End the CLI and remove the turn's dir. Shielded: a close cancelled
        midway (the SDK's disconnect stops at its first await) would leave
        the CLI running, on a user's computer for as long as the connection
        lasts."""
        if self._closing is None:
            self._closing = asyncio.ensure_future(self._close())
        await asyncio.shield(self._closing)

    async def _close(self) -> None:
        """A query still going is interrupted and drained first: a call failed
        while the CLI still runs is a result it would hand Claude, starting
        another request on the user's plan, as is a steer the hook would
        hand it, so the hook takes nothing more. Both are bounded: a CLI on a
        computer that stopped answering never acknowledges the interrupt."""
        self.after_tool.close()
        if not self._pump.done():

            async def drain() -> None:
                with contextlib.suppress(Exception):
                    await self.client.interrupt()
                await asyncio.shield(self._pump)

            with contextlib.suppress(BaseException):
                await asyncio.wait_for(drain(), DRAIN_TIMEOUT_S)
        self.results.fail_all("The turn ended before this call ran.")
        with contextlib.suppress(Exception):
            await self.client.disconnect()
        if not self._pump.done():
            self._pump.cancel()
        with contextlib.suppress(BaseException):
            await self._pump
        await self.launcher.release(self.prepared)


class ClaudeTurn:
    """One run's Claude Code, started by the step that first needs it and closed by the worker after the stream.

    ``identity`` is who the turn runs for (their user id): Claude Code is told one
    device per identity. ``thread_key`` is the thread's id and ``checkpoint_ns``
    the run's namespace, a subagent task's for its runs. Together they name
    the run's working directory (``run_key``), which is part of the prompt
    Claude caches, so it is the same on every run of that thread or task, and
    the transcript its compaction summaries point at.
    The launcher picks the token the run signs in with, and ``compaction``
    when the session compacts and moves to this build's prompt and tools.
    ``steering`` takes in what is sent into the thread while the turn runs
    (``TurnSteering``), or into the task for a subagent's run (``TaskSteering``).
    ``waits`` keeps the marks of its waits for one of its computer's CLIs,
    which each step commits.
    """

    def __init__(self, spec: RuntimeSpec, launcher: Launcher, *, identity: str, thread_key: str,
                 checkpoint_ns: str = "", baseline: Baseline | None = None, compaction: CompactionRules = NO_RULES,
                 steering: SteeringMiddleware | TaskSteering | None = None,
                 alert_senders: frozenset[str] = frozenset(), waits: CliWaits | None = None) -> None:
        self.spec = spec
        self.waits = waits if waits is not None else CliWaits()
        self.launcher = launcher
        self.identity = identity
        self.thread_key = thread_key
        self.checkpoint_ns = checkpoint_ns
        self.compaction = compaction
        self._baseline = baseline
        self.steering = steering if isinstance(steering, TaskSteering) else TurnSteering(steering, alert_senders)
        #: The session this run writes records to, and what its next write
        #: owes it: the pin and baseline this run launched with that it does
        #: not hold yet.
        self._session: str | None = None
        self._owed: dict[str, Any] = {}
        #: Whether the run moves the session to a pin with another prefix,
        #: whose write then waits for the CLI's record of it (``commit``).
        self._moving = False
        #: What a session this run starts holds, since the CLI may start one
        #: over instead of resuming.
        self._fresh: dict[str, Any] = {}
        #: The compaction the run's next reply follows, by its boundary
        #: record, with the summarize signal's payload replay reads off it.
        self._notice: tuple[str, dict[str, Any]] | None = None
        #: The boundary of a compaction the CLI made whose records have not arrived yet.
        self._unsettled: str | None = None
        self._run: _Run | None = None
        #: The text of the brief the turn opened on, by the reply it ends
        #: on, where state keeps it without (``brief.kept``).
        self._brief_text: tuple[str, str] | None = None
        #: The queries the turn sent on a run it ended to move its session to
        #: this build's pin ahead of the next; none while it has not.
        self._sent_before = 0
        #: What the turn's launches said of the Claude plan, opened by the
        #: first (``watch``), so a compaction's stops the run after it.
        self._plan: PlanWatch | None = None

    @property
    def current(self) -> _Run:
        """The started run. Only a ``model`` step starts one.

        A ``tools`` step that finds none was resumed from a checkpoint between
        a tool call and its result (recovery after a worker died). The CLI resumes such a turn by dropping the
        cut-off call and asking again, which would leave the checkpoint's call
        unanswered, so the turn fails instead.
        """
        if self._run is None:
            raise ClaudeTurnFailed(
                "nothing_to_resume", "a Claude Code run does not resume between a tool call and its result yet"
            )
        return self._run

    @property
    def started(self) -> bool:
        return self._run is not None

    @property
    def run_key(self) -> str:
        """The run's working directory: the thread's, or ``<thread>-<task>``
        for a task's runs, so no two CLIs on a thread share one."""
        task = TranscriptTarget.for_agent(self.thread_key, self.checkpoint_ns).task_id
        return f"{self.thread_key}-{task}" if task else self.thread_key

    async def watch(self) -> PlanWatch:
        """The turn's watch of the Claude plan, opened before its first launch reads a token."""
        if self._plan is None:
            self._plan = await PlanWatch.open(self.launcher, self.spec.model, enforce=True)
        return self._plan

    async def run(self, state: ClaudeCodeState) -> _Run:
        """The run, started on first use, and started again once its query
        ended with a call deferred: only a new CLI asks for that call again,
        and it is handed the result the thread holds for it."""
        if self._run is not None and self._run.deferred:
            run, self._run = self._run, None
            await run.close()
        if self._run is None:
            records = state.get("claude_session") or {}
            entries, answered = resume_from(records, state["messages"])
            if records:
                state = {**state, "claude_session": {**records, "entries": entries}}
            pin, prompt = self.opening(await self._briefed(state))
            if self._moving:
                # The CLI would replay the old pin from its records.
                logger.info("[ClaudeCode] re-pin=yes reason=at_launch")
                state = {**state, "claude_session": {**state["claude_session"], "entries": unpinned(entries)}}
            self._run = await _Run.start(
                self.spec, pin, self.launcher, self.run_key, self.identity, state.get("claude_session"), prompt,
                answered, plan=await self.watch(), compact_at=self.compaction.threshold,
                auto_compact=self.compaction.enabled,
                transcript_note=await self.compaction.transcript_note(self.thread_key, state, self.checkpoint_ns),
            )
            self._run.queries += self._sent_before
        return self._run

    async def again(self, state: ClaudeCodeState, config: RunnableConfig, run: _Run) -> list[HumanMessage]:
        """What a turn whose query ended sends the CLI next: what waited as
        it ended, after the trigger a langalpha turn re-enters its agent
        with, when a langalpha turn would re-enter for it (a steer, or a
        notice of ``alert_senders``). Empty when the turn may send no more
        queries (``_room``), or when nothing that would waits, which ends it.
        A query the turn holds back (``_held_back``) ends the run instead:
        the next step then compacts and moves the session as a turn start
        does, or stops at the credit gate before it is sent, as a langalpha
        turn's next model call is, and launches the query on a new CLI.
        What waits stays for the next turn."""
        if not (senders := await self.steering.waiting(config)) or not await self._room(run):
            return []
        if (message := await self.steering.take(config)) is None:
            return []
        trigger = HumanMessage(
            content=reentry_trigger(senders), name="orchestrator", additional_kwargs={"lc_source": ORCHESTRATOR_SOURCE}
        )
        sent = ensure_message_ids([trigger, message])
        prompt = _followed_by(state, sent, shared=self.spec.shared)
        if await self._held_back(state, prompt):
            await run.close()
            self._run, self._sent_before = None, run.queries
        else:
            await run.again(prompt)
        self.steering.delivered(config, message)
        return sent

    async def sendable(self, state: ClaudeCodeState, run: _Run, added: Sequence[BaseMessage]) -> Prompt | None:
        """The prompt that sends ``added`` as the turn's next query on
        ``run``, or None where ``again`` would send none or hold it back: for
        a query the turn sends of its own, which no new CLI is worth
        launching for."""
        if not await self._room(run):
            return None
        prompt = _followed_by(state, added, shared=self.spec.shared)
        return None if await self._held_back(state, prompt) else prompt

    @staticmethod
    async def _room(run: _Run) -> bool:
        """Whether the run may take another query: within the turn's queries,
        and not once the Claude plan is used up under Stop, where it would be
        billed to usage credits (``PlanWatch.gate``)."""
        return run.queries < TURN_QUERIES and not await run.plan.gate()

    async def _held_back(self, state: ClaudeCodeState, prompt: Prompt) -> bool:
        """Whether ``prompt`` may not go on the same CLI: past the credit
        boundary or for an account that may no longer work, which the gate
        stops before a request (``credit_stops``), or where it would take a
        session on another pin to the threshold, where the CLI would compact
        it on that pin (``CompactionRules.moves_mid_turn``)."""
        if credit_stops() or await account_ends():
            return True
        return self.compaction.moves_mid_turn(state.get("claude_session") or {}, self.current_pin(), prompt_tokens(prompt))

    async def _briefed(self, state: ClaudeCodeState) -> ClaudeCodeState:
        """``state`` with the text of the brief it keeps, which it keeps none
        of where the brief is the thread itself (``kept``): the text the turn
        was handed, else the thread rendered again."""
        brief = stored_brief(state)
        if brief is None or "text" in brief:
            return state
        through, text = self._brief_text or (None, None)
        if through != brief["through"]:
            # Kept for one Claude model, it may open a turn on another
            # (a regenerate or an edit), which is handed no more of the
            # thread than half the context its own CLI compacts at.
            room = min(brief["room"], self.compaction.threshold // 2) if self.compaction.threshold else brief["room"]
            rendered = await self.compaction.thread_brief(self.thread_key, state, room, self.checkpoint_ns)
            text = rendered.get("text") if rendered else None
            if text:
                self._brief_text = (brief["through"], text)
            else:
                logger.warning("[ClaudeCode] the thread brief through %s rendered empty", brief["through"])
        return {**state, BRIEF_KEY: {**brief, "text": text}} if text else state

    async def hand_off(self, state: ClaudeCodeState, config: RunnableConfig) -> dict[str, Any]:
        """The update that takes a thread another model replied on last onto
        a session of Claude's own, opened on a brief of everything before
        (``turn_prompt``): it ends the session Claude held, which has none of
        that, and keeps the brief, the one the run was handed
        (``BRIEF_CONFIG_KEY``) else one put together without a model
        (``CompactionRules.digest_brief``).

        The turn is marked as opening on the brief (``handoff_mark``),
        streamed as it happens and kept for the history to show.

        Saved in a step of its own, so a launch that fails after it leaves
        the retry nothing to redo. Empty when none is due or it is saved.
        """
        if (due := brief_due(state["messages"])) is None:
            return {}
        update: dict[str, Any] = {}
        if (brief := stored_brief(state)) is None:
            given = (config.get("configurable") or {}).get(BRIEF_CONFIG_KEY)
            brief = given if given and given["through"] == due else await self.compaction.digest_brief(
                self.thread_key, state, self.checkpoint_ns
            )
            logger.info("[ClaudeCode] hand-off brief=%s", brief["source"])
            update[BRIEF_KEY] = kept(brief)
            if "text" in brief:
                self._brief_text = (brief["through"], brief["text"])
        mark = handoff_mark(brief, to=self.spec.catalog_model)
        if all(record.get("id") != mark["id"] for record in state.get("ui") or []):
            update["ui"] = [mark]
            get_stream_writer()({"type": "context_window", "action": "brief", "signal": "complete", **mark["props"]})
        if (state.get("claude_session") or {}).get("session_id"):
            update["claude_session"] = ENDED
        return update

    def reading(self, run: _Run) -> dict[str, Any]:
        """The turn's rules as its end keeps them (``RULES_KEY``), with when
        the request behind its last answer started (``requested_at``, epoch
        seconds, None when the run streamed none), and the model and effort
        it ran on: the session's newest cache entry lives from then, and only
        a request on that model and effort reads it, which an idle compaction
        does before it expires."""
        return {**self.compaction.reading(), "requested_at": run.last_request_at, "model": self.spec.model,
                "effort": self.spec.effort}

    async def ended(self, state: ClaudeCodeState, run: _Run) -> dict[str, Any]:
        """What the turn's end adds: the reply it ended on, under Stop,
        stamped with the notice that the plan ran out during it
        (``PlanWatch.stamp``)."""
        last = state["messages"][-1] if state["messages"] else None
        if not isinstance(last, AIMessage) or (stamped := await run.plan.stamp(last)) is None:
            return {}
        return {"messages": [stamped]}

    async def _repin(self, state: ClaudeCodeState) -> list[SessionWrite]:
        """The records of the compaction this turn start makes, when it is
        due one (``CompactionRules.repin_plan``: a move to this build's pin,
        or a cold cache) and the CLI made it."""
        records = state.get("claude_session") or {}
        messages = state["messages"]
        if not records.get("session_id") or not (messages and isinstance(messages[-1], HumanMessage)):
            return []
        if another_model_replied_last(messages):
            return []
        asked = turn_prompt(state, baseline=self._baseline_due(state)[1], shared=self.spec.shared)
        plan = self.compaction.repin_plan(records, self.current_pin(), time.time(), pending=prompt_tokens(asked))
        if plan is None:
            return []
        moves, reason = plan
        if not moves:
            logger.info("[ClaudeCode] re-pin=waits reason=%s", reason)
            return []
        context_signal("summarize", "start")
        try:
            held = await self._compact(state)
        except BaseException as e:
            context_signal("summarize", "error", error=str(e))
            if not isinstance(e, Exception):
                raise
            logger.warning("[ClaudeCode] the session's compaction failed; the turn runs on its pin", exc_info=True)
            return []
        logger.info("[ClaudeCode] re-pin=%s reason=%s", "yes" if held else "no_compaction", reason)
        if held:
            self._announce(SessionCompaction(records.get("entries") or [], held))
        else:
            context_signal("summarize", "error", error="nothing to summarize")
        return held

    async def compacting(self, state: ClaudeCodeState, stage: _Compacting) -> None:
        """Signal a compaction the CLI makes ahead of a request of the run's
        as the compaction middleware signals an API thread's."""
        if stage.boundary is None:
            context_signal("summarize", "error" if stage.error else "start", **({"error": stage.error} if stage.error else {}))
            return
        self._unsettled = stage.boundary
        with contextlib.suppress(SessionDrift):
            await self.current.store.arrived.wait([stage.boundary], SETTLE_TIMEOUT_S)
        self.settled(state)

    def settled(self, state: ClaudeCodeState) -> None:
        """Announce the compaction the CLI made once its records have arrived.
        Records slower than the wait leave it to the reply after it, whose own
        records the CLI writes behind the compaction's."""
        if self._unsettled is None:
            return
        committed = (state.get("claude_session") or {}).get("entries") or []
        entries = [*committed, *self.current.store.pending()]
        if any(e.get("uuid") == self._unsettled for e in entries):
            self._announce(SessionCompaction.at(entries, self._unsettled))
            self._unsettled = None

    def compacted(self, state: ClaudeCodeState, run: _Run) -> dict[str, Any]:
        """A compaction the CLI made ahead of a request, saved in a step of
        its own once its records have arrived, with its notice stamped on the
        message it followed: the request after it can fail or be stopped,
        and a retry then resumes the session compacted rather than make and
        bill the compaction again. The step takes the compaction's records
        alone: its boundary, its summary and the attachments the CLI writes
        around them, the prompt and tools it runs on from then on among them.
        The records of the reply after it can be here already, and they
        belong to the step that commits that reply. Records slower than the
        wait are left to that reply (``settled``)."""
        if self._unsettled is not None:
            return {}
        pending = run.store.pending()
        boundary = self._notice[0] if self._notice else None
        start = next((i for i, e in enumerate(pending) if e.get("uuid") == boundary), None)
        if start is None:
            return {}
        through, summarized = boundary, False
        for e in pending[start + 1:]:
            if e.get("type") == "attachment" or e.get("isCompactSummary"):
                through, summarized = e.get("uuid") or through, summarized or bool(e.get("isCompactSummary"))
            elif e.get("uuid"):
                break
        if not summarized:
            return {}
        followed = state["messages"][-1].model_copy()
        self.stamp(followed)
        return {"messages": [followed], **self.commit(run.store.drain(through=[through]))}

    def _announce(self, compaction: SessionCompaction) -> None:
        notice = compaction.notice()
        context_signal("summarize", "complete", **notice)
        self._notice = (compaction.boundary, notice)

    def stamp(self, message: BaseMessage) -> None:
        """Stamp a reply with the compaction it follows, or the message a
        compaction saved in a step of its own followed (the turn's prompt or
        a tool result), for replay to show it where it fired, as an API
        thread's is."""
        if self._notice is not None:
            stamp_compaction_summary(message, *self._notice)
            self._notice = None

    async def compact_between_turns(self, state: ClaudeCodeState) -> SessionCompaction | None:
        """A manual compaction, which moves the session to this build's pin
        as a turn start due one does. None when the thread has no session yet
        or the CLI found nothing to summarize."""
        records = state.get("claude_session") or {}
        if not records.get("session_id"):
            return None
        writes = await self._compact(state)
        return SessionCompaction(records.get("entries") or [], writes) if writes else None

    async def _compact(self, state: ClaudeCodeState) -> list[SessionWrite]:
        """The CLI compacting the session under the pin it runs on, its
        records carrying this build's pin, which the session runs on from
        then on: the CLI records a launch's prompt and tools after a compaction."""
        records = state.get("claude_session") or {}
        entries, answered = resume_from(records, state["messages"])
        writes = await compact(self.spec, records.get("pinned") or self.current_pin(), self.launcher,
                               plan=await self.watch(), thread_key=self.run_key, identity=self.identity,
                               records={**records, "entries": answered_in(entries, answered) if answered else entries},
                               transcript_note=self.compaction.transcript_note(self.thread_key, state, self.checkpoint_ns))
        return _pinned_after(writes, self.current_pin())

    async def ahead_parts(self, state: ClaudeCodeState) -> dict[str, int]:
        """What the thread's next run sends ahead of its messages, in tokens
        by part: the prompt and tools of the pin it launches on, which are all
        the CLI is given ahead of them.

        Estimated as Claude counts (``count_tokens_claude``) rather than asked
        of the CLI, whose ``/context`` answers either from character counts,
        about a third low on this prompt, or by sending each category to the
        token-count API on the user's login from a CLI started for the
        purpose. The baseline is no part here: it goes in a turn's prompt,
        among the messages.
        """
        pin, _ = self._launch_pin(state)
        prompt = pin.get("system_prompt")
        return await context_parts.ahead_parts(PromptMessage(prompt) if prompt else None, pin.get("tools"), {},
                                               model=None, counter=count_tokens_claude)

    def _launch_pin(self, state: ClaudeCodeState, *, moves: bool = True) -> tuple[SessionPin, SessionPin | None]:
        """The pin the next run launches on, and the session's own, if it has
        a session: its own, unless it has none, it sends this build's prefix,
        or the CLI compacted it on that pin mid-turn (``repin_due``), which a
        launch that ``moves`` takes to this build's pin. Only a launch that
        sends a prompt moves it: one that carries on from tool results would
        change the prefix under the call it answers. With compaction off, no
        session moves (``CompactionRules.enabled``)."""
        records = state.get("claude_session") or {}
        kept = records.get("pinned") if records.get("session_id") else None
        current = self.current_pin()
        moving = moves and records.get("repin_due") and self.compaction.enabled
        return (current if not kept or same_pin(kept, current) or moving else kept), kept

    def opening(self, state: ClaudeCodeState) -> tuple[SessionPin, Prompt | None]:
        """What the run starts Claude on: the session's own pin, unless the
        session has none (a new session, or one from before sessions kept
        theirs), it sends the prefix this build's does, or the run moves it
        (``_launch_pin``). This build's pin is then the one the session holds
        from the run's first records on, or on a move from the CLI's record
        of it, so a pin names the last build that sent its prefix. And the
        turn's prompt."""
        records = state.get("claude_session") or {}
        key, block = self._baseline_due(state)
        prompt = turn_prompt(state, baseline=block, shared=self.spec.shared)
        self._session = records.get("session_id")
        pin, kept = self._launch_pin(state, moves=prompt is not None)
        self._moving = bool(kept) and not same_pin(kept, pin)
        sent = {"baseline": key} if prompt is not None and block else {}
        self._fresh = {"pinned": pin, **sent}
        # A session due to move that launches on this build's pin as its own (the build went back
        # to it) has the pin written again, which clears the due; on its old pin it stays due.
        back = bool(records.get("repin_due")) and same_pin(pin, self.current_pin())
        self._owed = self._fresh if pin != kept or back else sent
        return pin, prompt

    def current_pin(self) -> SessionPin:
        """The prompt and tools this build would start a session on."""
        return {
            "system_prompt": self.spec.system_prompt,
            "tools": tool_specs(self.spec.tools, self.spec.read_only),
            "source_digest": release.source_digest(),
            "prompt_version": release.prompt_version(),
        }

    def commit(self, session: list[SessionWrite]) -> dict[str, Any]:
        """A step's session records, the first of its own carrying what the
        session holds from then on that it did not before, and the marks of
        the waits for a CLI ahead of it.

        A move's pin waits for the write that carries the CLI's record of it
        (``records_pin``): a step can commit records the CLI wrote ahead of
        that one (the results of calls it resumes from), and a run that fails
        between would otherwise leave the session naming a pin its records do
        not. Held, the session stays due, so the next launch moves it again."""
        for write in session:
            if write["session_id"] != self._session:
                self._session, self._owed, self._moving = write["session_id"], self._fresh, False
            if self._owed:
                held = self._moving and not any(records_pin(e) for e in write["entries"])
                write.update({k: v for k, v in self._owed.items() if not (held and k == "pinned")})
                self._owed = {"pinned": self._owed["pinned"]} if held and "pinned" in self._owed else {}
        pin = self._fresh.get("pinned")
        waited = self.waits.drain()
        return {"claude_session": _pinned_after(session, pin, due=bool(pin) and not same_pin(pin, self.current_pin())),
                **({"ui": waited} if waited else {})}

    def _baseline_due(self, state: ClaudeCodeState) -> tuple[str, str | None]:
        """The thread's baseline, unless Claude's session already holds its epoch."""
        if self._baseline is None:
            return "", None
        try:
            key, block = self._baseline(state)
        except Exception:  # noqa: BLE001 - context is never worth failing a turn for
            logger.warning("[ClaudeCode] baseline render failed; the turn opens without it", exc_info=True)
            return "", None
        if not block or (state.get("claude_session") or {}).get("baseline") == key:
            return key, None
        return key, block

    async def close(self) -> None:
        """End the run's CLI, then wait, briefly, for what its responses said
        of the Claude plan to be kept: the run's closing frame comes after,
        and the client reads the plan again on it, however the run ended."""
        if self._run is not None:
            await self._run.close()
        if self._plan is not None:
            await self._plan.settled()


def _pinned_after(writes: list[SessionWrite], pin: SessionPin | None, *, due: bool = False) -> list[SessionWrite]:
    """``writes``, and the pin the session runs on after a compaction among
    them on a write of its own behind them: a build whose reducer voids a
    pin at a compaction, as earlier ones did, then keeps it too. ``due``
    when the pin is not this build's, so the compaction the CLI made on it
    was the one the session's move was due, and its next launch makes it."""
    if pin and any(compacted(e) for w in writes for e in w["entries"]):
        return [*writes, {"session_id": writes[-1]["session_id"], "entries": [], "pinned": pin, "repin_due": due}]
    return writes


def _turn(config: RunnableConfig) -> ClaudeTurn:
    return config["configurable"][TURN_KEY]


#: What a turn sends Claude: the user's text, or Anthropic content blocks.
Prompt = str | list[dict[str, Any]]
#: One output of the tool node, as the ``tools`` step returns it.
_Update = dict[str, Any] | Command
#: The thread's baseline as one block, with a key naming its epoch.
Baseline = Callable[[Any], tuple[str, str]]


def turn_prompt(state: ClaudeCodeState, *, baseline: str | None = None, shared: bool = False) -> Prompt | None:
    """The messages the turn added since Claude last spoke, as one user message.

    A single plain message goes as the string it is. Otherwise the message is
    a list of blocks in the form a ``ChatAnthropic`` call sends (an attached
    image as an image block), with the runtime rows the turn opened with
    carried as an API thread's reminder-shaped rows are, and ``baseline``,
    when given, ahead of everything as a reminder of its own. All of it is
    stored verbatim in the session, so a row or a baseline reaches Claude once.

    None when there is no user message because a run is resumed from a
    checkpoint taken after its tool results, which the CLI carries on from.
    Any other checkpoint without one (after an assistant message) is not
    something the CLI resumes, so it fails the turn.

    A thread whose last reply another model wrote opens on the brief it
    keeps for that reply (``ClaudeTurn.hand_off``), which stands in for
    everything up to it, as a reminder after the baseline; it fails without
    one: Claude's session holds only what Claude said, so it would carry on
    without that reply and everything before it. With nothing after the reply
    (its pause was answered as cancelled), the brief is all the turn sends.

    A task the turn resumed as it opened (``resume`` in
    ``build_claude_code_graph``) is told as the result of each resume, as a
    reminder after the messages ahead of it: Claude made no call for it, and
    its session holds none.
    """
    brief = (stored_brief(state) or {}).get("text")
    if brief is None and another_model_replied_last(state["messages"]):
        raise ClaudeTurnFailed("other_model_history", "the thread's last reply is another model's, which Claude's session does not hold")
    messages = state["messages"]
    start = 0 if (at := last_reply_index(messages)) is None else at + 1
    # A brief also stands in for the summary a thread with no reply opens on.
    window = messages[max(start, brief_cut(messages)) if brief is not None else start :]
    resumed = {c["id"] for m in window if isinstance(m, AIMessage) for c in m.tool_calls}
    added = [
        m if isinstance(m, HumanMessage) else HumanMessage(frame_reminder(m.text), id=m.id)
        for m in window
        if isinstance(m, HumanMessage) or (isinstance(m, ToolMessage) and m.tool_call_id in resumed)
    ]
    if not added and brief is None:
        last = state["messages"][-1] if state["messages"] else None
        if isinstance(last, ToolMessage) and (state.get("claude_session") or {}).get("session_id"):
            return None
        raise ClaudeTurnFailed(
            "nothing_to_resume",
            f"a Claude Code run resumes from a user message or after its tool results, not after a {type(last).__name__}",
        )
    # Named outside the carrier, as the API path's naming step does: only a
    # shared workspace's member messages carry a writer, and only there is a
    # message with none escaped, so elsewhere every message passes as it is.
    carried = compose_request(name_senders(added, shared=shared), "reminder").messages if added else []
    if baseline is None and brief is None and len(carried) == 1 and isinstance(carried[0].content, str):
        return carried[0].content
    blocks: list[dict[str, Any]] = [
        {"type": "text", "text": frame_reminder(ahead)} for ahead in (baseline, brief) if ahead
    ]
    for msg in carried:
        content = anthropic_content(msg)
        blocks.extend([{"type": "text", "text": content}] if isinstance(content, str) else content)
    return blocks


def _followed_by(state: ClaudeCodeState, added: Sequence[BaseMessage], *, shared: bool) -> Prompt:
    """The turn's next query: ``added``, as ``turn_prompt`` hands Claude what
    came since it last spoke."""
    prompt = turn_prompt({**state, "messages": [*state["messages"], *added]}, shared=shared)
    assert prompt is not None, "a prompt holds the messages added"
    return prompt


async def _user_message(blocks: list[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    yield {"type": "user", "message": {"role": "user", "content": blocks}, "parent_tool_use_id": None}


def build_claude_code_graph(
    tool_node: ToolNode, *, entry: Sequence[tuple[str, Any]] = (), pause: Sequence[tuple[str, Any]] = (),
    checkpointer: Any = None, abandon: Callable[[AIMessage, Sequence[ToolCall]], Awaitable[None]] | None = None,
    on_reply: Callable[[AIMessage], Awaitable[None]] | None = None, plan: AgentPlan | None = None,
) -> Any:
    """The turn graph. ``tool_node`` is the one a langalpha turn runs tools with.

    ``entry`` are the nodes a langalpha turn opens with, by name, run in
    order before the first ``model`` step: the hooks that write the turn's
    rows and freeze the baseline ``turn_prompt`` hands Claude. Then
    ``resume`` resumes the tasks messages wait for, as a langalpha turn's
    steering does before its first model call.

    A call the CLI deferred (``BeforeTool``) runs in ``held``, with no CLI,
    once the rest of its batch is committed and after ``pause``, the nodes a
    langalpha turn may stop in before a call runs (the credit gate, an order
    put to the user), so a pause there or in the call itself resumes on any
    worker with the batch and the CLI's records in the checkpoint. The next
    ``model`` step starts a new CLI, which asks for the call again and is
    handed its result. A result that ends the turn (``RuntimeSpec.ends_turn``)
    stops the CLI before it asks Claude again. So does the last result of a
    batch the credit gate stops by then, which then goes through ``pause`` as
    a deferred call does, and a new CLI carries on from it.

    A ``model`` step commits one assistant message and goes to ``tools`` when it
    made calls, else back to ``model``: the CLI can go on without a tool call
    (after an output-limit stop it continues the reply), so only the query's
    end, seen by a ``model`` step with no message left, ends the turn, unless
    steers waited then, which the step sends the CLI as the turn's next query.
    When that query needs a new CLI, the steers go through ``pause`` first,
    where the credit gate stops them as it stops a langalpha turn's next call.

    Each assistant message a ``model`` step commits goes to ``on_reply``
    first, as a langalpha agent's model call returns it to its middleware.
    The step beats on the stream while it waits for the CLI, as a langalpha
    model call does (``StreamHeartbeatMiddleware``).

    The run's first step answers the calls a run before it cut off (a stop, a
    worker that died mid-tool) as the CLI did when it resumed, so the thread's
    messages say what Claude was told, and hands them to ``abandon``, which
    refuses the order cards among them, since nothing will answer those now.

    A turn start due a new pin compacts in a ``model`` step of its own, ahead
    of the launch, so a launch that fails after it leaves the compaction
    saved: the next turn neither makes nor bills it again. The session then
    holds this build's pin, so the next step finds nothing to move. The step
    stamps the compaction on the prompt it followed, so the history shows it
    whether or not a reply comes. A compaction the CLI makes ahead of a
    request of the run's is saved the same way (``ClaudeTurn.compacted``).
    So is one the turn's next query is due, which the step after the run
    that ended for it makes (``ClaudeTurn.again``).

    A query the turn sends of its own is shaped by its ``Query``. An
    unstreamed reply is not handed to ``on_reply`` either: it is no step of
    a task, as a call kept out of the stream is not. A reply kept as nothing
    is not committed, and the query's own messages leave with it; the next
    ``model`` step reads on, so the query ends as any other does.

    ``plan`` keeps the agent's plan honest as a langalpha turn's model-call
    hooks do (``agent_plan``): a reply is counted as it is committed, a
    ``tools`` step hands its batch's check-in or nudge over with the results,
    and the close-out follows a query's end where it is due.
    """

    async def model(state: ClaudeCodeState, config: RunnableConfig) -> Command:
        """Each of the CLI's model steps, as a langalpha turn's model call.

        A best-effort query (``Query.dropped``) that fails, whether the CLI
        ends it as failed or exits or loses its connection while it runs, is
        given up wherever the step finds it: the user already has the answer
        before it. Any other query's failure fails the turn."""
        await refuse_ended_account()
        turn = _turn(config)
        try:
            return await model_step(turn, state, config)
        except _QUERY_FAILED:
            if not turn.started or not (run := turn.current).query.dropped:
                raise
            return await given_up(turn, state, run)

    async def model_step(turn: ClaudeTurn, state: ClaudeCodeState, config: RunnableConfig) -> Command:
        turn.waits.after = state["messages"][-1].id if state["messages"] else None
        opening = not turn.started
        if opening and (handed := await turn.hand_off(state, config)):
            return Command(update=handed, goto="model")
        if opening and (compacted := await turn._repin(state)):
            asked = state["messages"][-1].model_copy()
            turn.stamp(asked)
            waited = turn.waits.drain()
            return Command(update={"claude_session": compacted, "messages": [asked], **({"ui": waited} if waited else {})},
                           goto="model")
        run = await turn.run(state)
        if opening:
            turn.steering.opened(state, config)
            if cut := _cut_off(state["messages"]):
                at, calls = cut
                answers, uuids = await run.cut_off(calls)
                if abandon is not None:
                    await abandon(state["messages"][at], calls)
                patched = [*state["messages"][: at + 1], *answers, *state["messages"][at + 1 :]]
                return Command(
                    update={"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *patched],
                            **(turn.commit(run.store.drain(through=uuids)) if uuids else {})},
                    goto="model",
                )
        await turn.steering.read(state["messages"])
        async with stream_heartbeat(get_stream_writer()):
            message = await run.next_message(lambda stage: turn.compacting(state, stage))
        if isinstance(message, _Compacting):
            return Command(update=turn.compacted(state, run), goto="model")
        if message is None:
            await run.finish()
            if sent := await turn.again(state, config, run):
                return Command(update={"messages": sent, **turn.commit(run.store.drain())},
                               goto="model" if turn.started else held[0])
            if plan is not None and (step := await plan.close_out_step(turn, state, run)) is not None:
                return step
            return await end(turn, state, run)
        relay = ClaudeStreamRelay(model_name=turn.spec.model, metadata=dict(turn.spec.billing), cache=False).feed(message.events())
        ai: AIMessage = await relay.ainvoke([], config if run.query.streamed else nostream(config))
        await run.complete(message)
        if message.restarted:
            return Command(goto="model")
        if (keep := run.query.keep) is not None:
            kept = keep(ai)
            ids = {c["id"] for c in kept.tool_calls}
            # An unparsable call is among them: the CLI answers it itself.
            run.query = replace(run.query, refused=tuple(
                c["id"] for c in [*ai.tool_calls, *ai.invalid_tool_calls] if c.get("id") and c["id"] not in ids))
            ai = kept
            if not ai.tool_calls:
                uuids = [*run.assistant_records(message.id), *await run.tool_result_records(run.query.refused)]
                await run.settle(uuids)
                return Command(update={"messages": _still_in(state, run.query.dropped),
                                       **turn.commit(run.store.drain(through=uuids))}, goto="model")
        if ai.invalid_tool_calls:
            raise ClaudeTurnFailed("api_error", f"tool calls that did not parse: {[c.get('name') for c in ai.invalid_tool_calls]}")
        ai.response_metadata.setdefault("model_name", turn.spec.model)
        # The resilience middleware stamps this on every other turn; a Claude
        # Code turn never passes through it.
        ai.response_metadata[CATALOG_MODEL_KEY] = turn.spec.catalog_model
        ai.response_metadata[CLAUDE_MESSAGE_ID] = message.id
        uuids = run.assistant_records(message.id)
        await run.settle(uuids)
        turn.settled(state)
        # After a compaction whose records came late: it ran ahead of this
        # call, so its notice goes out first, or the window this call reports
        # would read as cleared by it.
        report_usage(ai, turn.compaction.threshold)
        turn.stamp(ai)
        if on_reply is not None and run.query.streamed:
            await on_reply(ai)
        counted = plan.counted(state, ai) if plan is not None else {}
        return Command(
            update={"messages": [ai], **counted, **turn.commit(run.store.drain(through=uuids))},
            goto="tools" if ai.tool_calls else "model",
        )

    async def end(turn: ClaudeTurn, state: ClaudeCodeState, run: _Run, gone: Sequence[RemoveMessage] = ()) -> Command:
        """The step that ends the turn as its query ended, taking ``gone``
        out first: the end reads the thread without them."""
        ids = {r.id for r in gone}
        state = {**state, "messages": [m for m in state["messages"] if m.id not in ids]}
        update = {**turn.commit(run.store.drain()), **(await turn.ended(state, run)), RULES_KEY: turn.reading(run)}
        if gone:
            update["messages"] = [*gone, *update.get("messages", [])]
        return Command(update=update, goto=END)

    async def given_up(turn: ClaudeTurn, state: ClaudeCodeState, run: _Run) -> Command:
        """The end of a turn whose best-effort query failed (``Query.dropped``):
        on what came before it, since the user already has that answer."""
        logger.warning("[ClaudeCode] the turn's own query failed; the turn ends on what came before it", exc_info=True)
        return await end(turn, state, run, _still_in(state, run.query.dropped))

    async def tools(state: ClaudeCodeState, config: RunnableConfig) -> Any:
        """Run each call once the CLI asks for it, with the arguments it sends,
        as ``create_agent``'s own ``Send`` of one call runs it, and keep the
        CLI's own answer for each it refused, so the results read as Claude's
        session has them, in call order. A call that raises fails the step at
        once: the CLI waits for its result, and the rest are cancelled."""
        turn = _turn(config)
        run = turn.current
        calls = state["messages"][-1].tool_calls
        #: Each call's output as it comes back, which the rows read.
        outs: list[Any] = []

        async def take() -> HandOff:
            rows, update = plan.rows(state, outs) if plan is not None else ((), {})
            return HandOff(rows, await turn.steering.take(config), update)

        run.after_tool.open([c["id"] for c in calls], take)
        put_off: str | None = None

        async def call(c: ToolCall) -> list[_Update]:
            match await run.results.due(c["id"]):
                case Ended():
                    await run.finish()
                    raise ClaudeTurnFailed("api_error", f"the CLI ended the turn before it ran call {c['id']}")
                case Deferred():
                    nonlocal put_off
                    put_off = c["id"]
                    return []
                case Answered(block):
                    return [{"messages": [_as_given(ToolMessage(content="", tool_call_id=c["id"], name=c["name"]), block)]}]
                case Run(args):
                    out = await tool_node.ainvoke([{**c, "args": args}], config)
                    outs.append(out)
                    for m in _tool_messages(out):
                        run.results.resolve(m)
                    return out if isinstance(out, list) else [out]

        try:
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(call(c)) for c in calls]
        except BaseExceptionGroup as failed:
            raise failed.exceptions[0] from None
        uuids = await run.tool_result_records([*(c["id"] for c in calls if c["id"] != put_off), *run.query.refused])
        gated = put_off or any(run.results.owes_gate(c["id"]) for c in calls)
        stop = held[0] if gated else END if any(run.results.ends_turn(c["id"]) for c in calls) else None
        if stop:
            await run.finish()
            uuids += [await run.deferral_record(put_off)] if put_off else []
        await run.settle(uuids)
        handed = await run.after_tool.taken()
        if handed.steer is not None:
            turn.steering.delivered(config, handed.steer)
        session = turn.commit(run.store.drain() if stop else run.store.drain(through=uuids))
        updates = [_given_instead(u, run.results, c) for c, t in zip(calls, tasks, strict=True) for u in t.result()]
        rules = {RULES_KEY: turn.reading(run)} if stop else {}
        return _step(updates, {"messages": handed.messages, **handed.update, **session, **rules}, stop or "model")

    async def resume(state: ClaudeCodeState, config: RunnableConfig) -> Command:
        """Resume the tasks whose runs ended with messages they never read,
        as a langalpha turn does before its first model call: the resume
        call is committed, then run in ``held``, with no CLI, whose tool
        node finds the rows it holds on it. The steers the turn opened with
        are shown first, since the call ends the window they are read in.
        A turn that opens on calls a run before it cut off resumes none: the
        tasks wait for the next turn, so the call never hides the ones
        ``model`` answers."""
        turn = _turn(config)
        if _cut_off(state["messages"]) or (call := await turn.steering.resume(config)) is None:
            return Command(goto="model")
        turn.steering.opened(state, config)
        return Command(update={"messages": [call]}, goto="held")

    async def held_call(state: ClaudeCodeState, config: RunnableConfig) -> Any:
        """The call the CLI deferred, run as ``tools`` would with no CLI, where
        it may pause the run; nothing when a ``pause`` node answered it."""
        _, calls = pending_calls(state["messages"])
        outs = [await tool_node.ainvoke([c], config) for c in calls]
        return _step([u for out in outs for u in (out if isinstance(out, list) else [out])], {}, "model")

    held = [*(name for name, _ in pause), "held"]
    g = StateGraph(ClaudeCodeState)
    for name, node in [*entry, *pause]:
        g.add_node(name, node)
    g.add_node("resume", resume, destinations=("model", "held"))
    g.add_node("model", model, destinations=("tools", "model", END))
    g.add_node("tools", tools, destinations=("model", END, held[0]))
    g.add_node("held", held_call, destinations=("model",))
    for a, b in [*pairwise([START, *(name for name, _ in entry), "resume"]), *pairwise(held)]:
        g.add_edge(a, b)
    return g.compile(checkpointer=checkpointer)


def _step(updates: Sequence[_Update], update: dict[str, Any], goto: str) -> Command | list[Command | dict[str, Any]]:
    """A ``tools`` step's output: its calls' outputs, then ``update``, going to ``goto``."""
    if all(isinstance(u, dict) for u in updates):
        messages = [*(m for u in updates for m in u["messages"]), *update.get("messages", [])]
        return Command(update={**update, "messages": messages}, goto=goto)
    return [*updates, Command(update=update, goto=goto)]


def _cut_off(messages: Sequence[BaseMessage]) -> tuple[int, list[ToolCall]] | None:
    """Where the thread's last assistant message is, and its calls no result
    answers, when the thread went on past it without them."""
    at, calls = pending_calls(messages)
    return (at, calls) if calls and at != len(messages) - 1 else None


def _as_given(message: ToolMessage, block: ToolResultBlock) -> ToolMessage:
    return message.model_copy(update={"content": block.content or "", "status": "error" if block.is_error else "success"})


def _unrecorded(call: ToolCall) -> str:
    """What ``PatchToolCallsMiddleware`` answers a call no result was recorded for."""
    return (
        f"Tool call {call['name'] or 'unknown'} with id {call['id']} did not complete - no result was recorded. "
        "It may have been cancelled or interrupted."
    )


def _unrecorded_answer(call: ToolCall) -> ToolMessage:
    given = ToolResultBlock(tool_use_id=call["id"], content=_unrecorded(call), is_error=True)
    return _as_given(ToolMessage(content="", tool_call_id=call["id"], name=call["name"]), given)


def _given_instead(update: _Update, results: ToolResults, call: ToolCall) -> _Update:
    """A call's output with its result as the CLI gave it to Claude, where
    that differs (``ToolResults.given_instead``); its state updates and
    artifact stay the step's."""
    block = results.given_instead(call["id"], call["name"])
    if block is None:
        return update
    command = update if isinstance(update, Command) else None
    values = command.update if command is not None else update
    if not isinstance(values, dict):
        return update
    messages = [_as_given(m, block) if isinstance(m, ToolMessage) and m.tool_call_id == call["id"] else m
                for m in values.get("messages", [])]
    values = {**values, "messages": messages}
    return replace(command, update=values) if command is not None else values


def node_updates(out: Any) -> Iterator[dict[str, Any]]:
    """The state updates in a tool node's output, whatever its shape: an
    update, a Command, or a list of either."""
    for item in out if isinstance(out, list) else [out]:
        update = item.update if isinstance(item, Command) else item
        if isinstance(update, dict):
            yield update


def _tool_messages(out: Any) -> list[ToolMessage]:
    return [m for update in node_updates(out) for m in update.get("messages", []) if isinstance(m, ToolMessage)]


def _still_in(state: ClaudeCodeState, removals: Sequence[RemoveMessage]) -> list[RemoveMessage]:
    """``removals`` that still name a message of the thread."""
    ids = {m.id for m in state["messages"]}
    return [r for r in removals if r.id in ids]
