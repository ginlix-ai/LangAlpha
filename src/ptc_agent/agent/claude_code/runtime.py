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
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from functools import partial
from itertools import pairwise
from typing import Annotated, Any, TypeVar

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    RateLimitEvent,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    ToolResultBlock,
    UserMessage,
)
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, RemoveMessage, ToolCall, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.prebuilt import ToolNode
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.claude_code.cli import RuntimeSpec, connected, launch, received, turn_failure
from ptc_agent.agent.claude_code.compaction import (
    NO_RULES,
    CompactionRules,
    SessionCompaction,
    compact,
    context_signal,
    report_usage,
    same_pin,
)
from ptc_agent.agent.claude_code.launcher import Launcher, Prepared
from ptc_agent.agent.claude_code.mid_turn import AfterTool, TurnSteering
from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay, anthropic_content
from ptc_agent.agent.claude_code.session import (
    SETTLE_TIMEOUT_S,
    CheckpointSessionStore,
    SessionDrift,
    SessionPin,
    SessionRecords,
    SessionWrite,
    compacted,
    session_channel,
)
from ptc_agent.agent.claude_code.tools_bridge import (
    Answered,
    Ended,
    Run,
    ToolResults,
    tool_specs,
)
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction.utils import stamp_compaction_summary
from ptc_agent.agent.middleware.runtime_context.carrier import Carrier, compose_request
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from ptc_agent.agent.middleware.steering import SteeringMiddleware
from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.transcript.classify import ORCHESTRATOR_SOURCE, reentry_trigger
from ptc_agent.agent.prompts import release
from src.llms.claude_code import MESSAGE_ID_KEY as CLAUDE_MESSAGE_ID
from src.llms.claude_code import ClaudeTurnFailed, another_model_replied_last

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



async def token_works(launcher: Launcher, model: str) -> bool:
    """Whether Anthropic serves the token the launcher signs in with: one minimal turn on ``model``, through a turn's own transport.

    ``claude auth status`` reads only local state and reports any string as a
    login, so the check asks Anthropic. A served request or a used-up plan
    means the token is good, a refused login or none at all that it is not;
    anything else raises, so a network failure or a CLI that would not start
    is never read as a bad token, nor as a good one. Over SSH the token is the computer's own or the one relayed to it,
    whichever a turn there would use.
    """
    prepared = await launcher.scratch()
    result: ResultMessage | None = None
    errors: list[tuple[str, str]] = []
    limits: list[RateLimitEvent] = []
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
                        limits.append(m)
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
    failure = turn_failure(result, errors, limits)
    if failure is None or failure.kind == "plan_exhausted":
        return True
    if failure.kind == "auth_failed":
        return False
    raise failure


class ClaudeCodeState(MainAgentState):
    """A Claude Code thread's checkpoint: a main-agent thread's, plus Claude's session."""

    claude_session: NotRequired[Annotated[SessionRecords, session_channel()]]


_END = object()


@dataclass(frozen=True)
class _Compacting:
    """The CLI compacting the session ahead of its next request: started,
    failed with ``error``, or done at the ``boundary`` record."""

    boundary: str | None = None
    error: str | None = None


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

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        item = self._first
        while True:
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
                 client: ClaudeSDKClient, results: ToolResults, spec: RuntimeSpec, after_tool: AfterTool) -> None:
        self.launcher = launcher
        self.prepared = prepared
        self.store = store
        self.client = client
        self.results = results
        self.after_tool = after_tool
        self.model = spec.model
        self.result: ResultMessage | None = None
        self.rate_limits: list[RateLimitEvent] = []
        #: (error, text) of each assistant message the CLI marked as an error.
        self.assistant_errors: list[tuple[str, str]] = []
        self.drift: SessionDrift | None = None
        #: The queries sent the CLI this run: the turn's own, then one for
        #: each time steers waited as the last ended.
        self.queries = 1
        self._events: asyncio.Queue[Any] = asyncio.Queue()
        self._held: dict[str, Any] | None = None
        self._streamed: set[str] = set()
        self._assistant: dict[str, list[str]] = {}
        self._closing: asyncio.Future[None] | None = None
        self._finishing: asyncio.Future[None] | None = None
        self._init_seen = False
        self._pump = asyncio.create_task(self._read())

    @classmethod
    async def start(cls, spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, thread_key: str,
                    identity: str, records: SessionRecords | None, prompt: Prompt | None, *, compact_at: int = 0,
                    auto_compact: bool = True, transcript_note: str = "") -> _Run:
        """Start the CLI on the turn (``cli.launch``); with no prompt it carries
        on from the tool results that end its session."""
        store, results = CheckpointSessionStore(), ToolResults()
        query = prompt if prompt is None or isinstance(prompt, str) else _user_message(prompt)
        prepared, client, after_tool = await launch(
            spec, pin, launcher, thread_key=thread_key, identity=identity, records=records, store=store,
            results=results, query=query, compact_at=compact_at, auto_compact=auto_compact, transcript_note=transcript_note,
        )
        return cls(launcher, prepared, store, client, results, spec, after_tool)

    async def again(self, prompt: Prompt) -> None:
        """Send the turn's next query on the same CLI, once the last one ended
        done. What the CLI said of the last one's end goes with it."""
        self.queries += 1
        self.result, self._finishing = None, None
        self.rate_limits, self.assistant_errors = [], []
        self.results.reopen()
        await self.client.query(prompt if isinstance(prompt, str) else _user_message(prompt))
        self._pump = asyncio.create_task(self._read())

    async def _read(self) -> None:
        """Read what the CLI says until the turn's result.

        Its init comes ahead of its first request. One naming another model
        than the turn's is a policy refusing it (``availableModels``), one
        no file on the machine shows, such as the organization's account's,
        where the CLI would run another in its place, so the turn fails before
        anything is streamed.
        """
        try:
            async for m in received(self.client, init_seen=self._init_seen):
                if isinstance(m, SystemMessage) and m.subtype == "init":
                    self._init_seen = True
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
                        if m.event.get("type") == "message_start":
                            self._streamed.add((m.event.get("message") or {}).get("id", ""))
                        await self._events.put(m.event)
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
                elif isinstance(m, ResultMessage):
                    self.result = m
                    break
                elif isinstance(m, RateLimitEvent):
                    self.rate_limits.append(m)
                elif isinstance(m, SystemMessage) and m.subtype == "mirror_error":
                    self.drift = SessionDrift(f"session mirror dropped a batch: {m.data.get('error')}")
                    break
        finally:
            self.results.ended()
            await self._events.put(_END)

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

    async def next_event(self) -> Any:
        if self._held is not None:
            item, self._held = self._held, None
            return item
        return await self._events.get()

    def hold(self, item: dict[str, Any]) -> None:
        """Keep an event read past the end of one message for the next."""
        self._held = item

    async def next_message(self, compacting: Callable[[_Compacting], Awaitable[None]]) -> _Message | None:
        """The next assistant message, or None once the turn has ended, each
        compaction the CLI makes ahead of it handed to ``compacting`` as it goes."""
        while (first := await self.next_event()) is not _END:
            if not isinstance(first, _Compacting):
                return _Message(first, self)
            await compacting(first)
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

    async def cut_off(self, calls: Sequence[ToolCall]) -> tuple[list[ToolMessage], list[str]]:
        """The results the CLI recorded, resuming, for calls a run before it
        cut off, as ToolMessages, and their records. It records them as it
        loads the session, without a word on its stream. A call it recorded
        none for (it started the session over) is answered as a langalpha
        turn answers one (``PatchToolCallsMiddleware``), with no record."""
        ids = [c["id"] for c in calls]
        with contextlib.suppress(SessionDrift):
            await self._unless_crashed(self.store.results.wait(ids, SETTLE_TIMEOUT_S))
        answers, uuids = [], []
        for c in calls:
            if (recorded := self.store.result_blocks.get(c["id"])) is None:
                given = ToolResultBlock(tool_use_id=c["id"], content=_unrecorded(c), is_error=True)
            else:
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
        failed = turn_failure(self.result, self.assistant_errors, self.rate_limits)
        if failed is None:
            return
        if failed.kind == "auth_failed":
            await self.launcher.refused()
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
    device per identity. ``thread_key`` names the thread's working directory,
    which is part of the prompt Claude caches, so it is the same on every turn.
    The launcher picks the token the run signs in with, and ``compaction``
    when the session compacts and moves to this build's prompt and tools.
    ``steering`` takes in what is sent into the thread while the turn runs
    (``TurnSteering``).
    """

    def __init__(self, spec: RuntimeSpec, launcher: Launcher, *, identity: str, thread_key: str,
                 baseline: Baseline | None = None, compaction: CompactionRules = NO_RULES,
                 steering: SteeringMiddleware | None = None, alert_senders: frozenset[str] = frozenset()) -> None:
        self.spec = spec
        self.launcher = launcher
        self.identity = identity
        self.thread_key = thread_key
        self.compaction = compaction
        self._baseline = baseline
        self.steering = TurnSteering(steering, alert_senders)
        #: The session this run writes records to, and what its next write
        #: owes it: the pin and baseline this run launched with that it does
        #: not hold yet.
        self._session: str | None = None
        self._owed: dict[str, Any] = {}
        #: What a session this run starts holds, since the CLI may start one
        #: over instead of resuming.
        self._fresh: dict[str, Any] = {}
        #: The compaction the run's next reply follows, by its boundary
        #: record, with the summarize signal's payload replay reads off it.
        self._notice: tuple[str, dict[str, Any]] | None = None
        #: The boundary of a compaction the CLI made whose records have not arrived yet.
        self._unsettled: str | None = None
        self._run: _Run | None = None

    @property
    def current(self) -> _Run:
        """The started run. Only a ``model`` step starts one.

        A ``tools`` step that finds none was resumed from a checkpoint between a
        tool call and its result (an interrupt, or recovery after a worker
        died). The CLI resumes such a turn by dropping the cut-off call and
        asking again, which would leave the checkpoint's call unanswered, so the
        turn fails instead.
        """
        if self._run is None:
            raise ClaudeTurnFailed(
                "nothing_to_resume", "a Claude Code run does not resume between a tool call and its result yet"
            )
        return self._run

    @property
    def started(self) -> bool:
        return self._run is not None

    async def run(self, state: ClaudeCodeState) -> _Run:
        if self._run is None:
            pin, prompt = self.opening(state)
            self._run = await _Run.start(
                self.spec, pin, self.launcher, self.thread_key, self.identity, state.get("claude_session"), prompt,
                compact_at=self.compaction.threshold, auto_compact=self.compaction.enabled,
                transcript_note=await self.compaction.transcript_note(self.thread_key, state),
            )
        return self._run

    async def again(self, state: ClaudeCodeState, config: RunnableConfig, run: _Run) -> list[HumanMessage]:
        """What a turn whose query ended sends the CLI next: what waited as
        it ended, after the trigger a langalpha turn re-enters its agent
        with, when a langalpha turn would re-enter for it (a steer, or a
        notice of ``alert_senders``). Empty once the turn has sent its last
        query, or when nothing that would waits, which ends it."""
        if run.queries >= TURN_QUERIES or not (senders := await self.steering.waiting(config)):
            return []
        if (message := await self.steering.take(config)) is None:
            return []
        trigger = HumanMessage(
            content=reentry_trigger(senders), name="orchestrator", additional_kwargs={"lc_source": ORCHESTRATOR_SOURCE}
        )
        sent = ensure_message_ids([trigger, message])
        await run.again(turn_prompt({**state, "messages": [*state["messages"], *sent]}))
        self.steering.delivered(config, message)
        return sent


    async def _repin(self, state: ClaudeCodeState) -> list[SessionWrite]:
        """The records of the compaction that moves the session to this
        build's pin, when this turn start is due one
        (``CompactionRules.repin_plan``) and the CLI made it."""
        records = state.get("claude_session") or {}
        messages = state["messages"]
        if not records.get("session_id") or not (messages and isinstance(messages[-1], HumanMessage)):
            return []
        if another_model_replied_last(messages):
            return []
        plan = self.compaction.repin_plan(records, self.current_pin(), time.time())
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

    def _announce(self, compaction: SessionCompaction) -> None:
        notice = compaction.notice()
        context_signal("summarize", "complete", **notice)
        self._notice = (compaction.boundary, notice)

    def stamp(self, message: BaseMessage) -> None:
        """Stamp a reply with the compaction it follows, or the prompt a
        compaction at the turn's start followed, for replay to show it where
        it fired, as an API thread's is."""
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
        writes = await compact(self.spec, records.get("pinned") or self.current_pin(), self.launcher,
                               thread_key=self.thread_key, identity=self.identity, records=records,
                               transcript_note=await self.compaction.transcript_note(self.thread_key, state))
        return _pinned_after(writes, self.current_pin())

    def opening(self, state: ClaudeCodeState) -> tuple[SessionPin, Prompt | None]:
        """What the run starts Claude on: the session's own pin, unless the
        session has none (a new session, or one from before sessions kept
        theirs) or it sends the prefix this build's does. This build's pin is
        then the one the session holds from the run's first records on, so a
        pin names the last build that sent its prefix. And the turn's prompt."""
        records = state.get("claude_session") or {}
        key, block = self._baseline_due(state)
        prompt = turn_prompt(state, baseline=block)
        self._session = records.get("session_id")
        kept = records.get("pinned") if self._session else None
        current = self.current_pin()
        pin = current if not kept or same_pin(kept, current) else kept
        sent = {"baseline": key} if prompt is not None and block else {}
        self._fresh = {"pinned": pin, **sent}
        self._owed = sent if pin == kept else self._fresh
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
        session holds from then on that it did not before."""
        for write in session:
            if write["session_id"] != self._session:
                self._session, self._owed = write["session_id"], self._fresh
            if self._owed:
                write.update(self._owed)
                self._owed = {}
        return {"claude_session": _pinned_after(session, self._fresh.get("pinned"))}

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
        if self._run is not None:
            await self._run.close()


def _pinned_after(writes: list[SessionWrite], pin: SessionPin | None) -> list[SessionWrite]:
    """``writes``, and the pin the session runs on after a compaction among
    them on a write of its own behind them: a build whose reducer voids a
    pin at a compaction, as earlier ones did, then keeps it too."""
    if pin and any(compacted(e) for w in writes for e in w["entries"]):
        return [*writes, {"session_id": writes[-1]["session_id"], "entries": [], "pinned": pin}]
    return writes


def _turn(config: RunnableConfig) -> ClaudeTurn:
    return config["configurable"][TURN_KEY]


#: What a turn sends Claude: the user's text, or Anthropic content blocks.
Prompt = str | list[dict[str, Any]]
#: One output of the tool node, as the ``tools`` step returns it.
_Update = dict[str, Any] | Command
#: The thread's baseline as one block, with a key naming its epoch.
Baseline = Callable[[Any], tuple[str, str]]


def turn_prompt(state: ClaudeCodeState, *, baseline: str | None = None) -> Prompt | None:
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

    A thread whose last reply another model wrote fails too: Claude's session
    holds only what Claude said, so it would carry on without that reply and
    everything before it that another model saw.
    """
    if another_model_replied_last(state["messages"]):
        raise ClaudeTurnFailed("other_model_history", "the thread's last reply is another model's, which Claude's session does not hold")
    added: list[HumanMessage] = []
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage):
            break
        if isinstance(msg, HumanMessage):
            added.append(msg)
    if not added:
        last = state["messages"][-1] if state["messages"] else None
        if isinstance(last, ToolMessage) and (state.get("claude_session") or {}).get("session_id"):
            return None
        raise ClaudeTurnFailed(
            "nothing_to_resume",
            f"a Claude Code run resumes from a user message or after its tool results, not after a {type(last).__name__}",
        )
    added.reverse()
    carried = compose_request(added, Carrier("reminder")).messages
    if baseline is None and len(carried) == 1 and isinstance(carried[0].content, str):
        return carried[0].content
    blocks: list[dict[str, Any]] = [{"type": "text", "text": frame_reminder(baseline)}] if baseline else []
    for msg in carried:
        content = anthropic_content(msg)
        blocks.extend([{"type": "text", "text": content}] if isinstance(content, str) else content)
    return blocks


async def _user_message(blocks: list[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    yield {"type": "user", "message": {"role": "user", "content": blocks}, "parent_tool_use_id": None}


def build_claude_code_graph(
    tool_node: ToolNode, *, entry: Sequence[tuple[str, Any]] = (), checkpointer: Any = None
) -> Any:
    """The turn graph. ``tool_node`` is the one a langalpha turn runs tools with.

    ``entry`` are the nodes a langalpha turn opens with, by name, run in
    order before the first ``model`` step: the hooks that write the turn's
    rows and freeze the baseline ``turn_prompt`` hands Claude.

    A ``model`` step commits one assistant message and goes to ``tools`` when it
    made calls, else back to ``model``: the CLI can go on without a tool call
    (after an output-limit stop it continues the reply), so only the query's
    end, seen by a ``model`` step with no message left, ends the turn, unless
    steers waited then, which the step sends the CLI as the turn's next query.

    The run's first step answers the calls a run before it cut off (a stop, a
    worker that died mid-tool) as the CLI did when it resumed, so the thread's
    messages say what Claude was told.

    A turn start due a new pin compacts in a ``model`` step of its own, ahead
    of the launch, so a launch that fails after it leaves the compaction
    saved: the next turn neither makes nor bills it again. The session then
    holds this build's pin, so the next step finds nothing to move. The step
    stamps the compaction on the prompt it followed, so the history shows it
    whether or not a reply comes.
    """

    async def model(state: ClaudeCodeState, config: RunnableConfig) -> Command:
        turn = _turn(config)
        opening = not turn.started
        if opening and (compacted := await turn._repin(state)):
            asked = state["messages"][-1].model_copy()
            turn.stamp(asked)
            return Command(update={"claude_session": compacted, "messages": [asked]}, goto="model")
        run = await turn.run(state)
        if opening:
            turn.steering.opened(state, config)
            if cut := _cut_off(state["messages"]):
                at, calls = cut
                answers, uuids = await run.cut_off(calls)
                patched = [*state["messages"][: at + 1], *answers, *state["messages"][at + 1 :]]
                return Command(
                    update={"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *patched],
                            **(turn.commit(run.store.drain(through=uuids)) if uuids else {})},
                    goto="model",
                )
        await turn.steering.read(state["messages"])
        message = await run.next_message(lambda stage: turn.compacting(state, stage))
        if message is None:
            await run.finish()
            if sent := await turn.again(state, config, run):
                return Command(update={"messages": sent, **turn.commit(run.store.drain())}, goto="model")
            return Command(update=turn.commit(run.store.drain()), goto=END)
        relay = ClaudeStreamRelay(model_name=turn.spec.model, metadata=dict(turn.spec.billing), cache=False).feed(message.events())
        ai: AIMessage = await relay.ainvoke([], config)
        await run.complete(message)
        if message.restarted:
            return Command(goto="model")
        if ai.invalid_tool_calls:
            raise ClaudeTurnFailed("api_error", f"tool calls that did not parse: {[c.get('name') for c in ai.invalid_tool_calls]}")
        ai.response_metadata.setdefault("model_name", turn.spec.model)
        ai.response_metadata[CLAUDE_MESSAGE_ID] = message.id
        report_usage(ai, turn.compaction.threshold)
        uuids = run.assistant_records(message.id)
        await run.settle(uuids)
        turn.settled(state)
        turn.stamp(ai)
        return Command(
            update={"messages": [ai], **turn.commit(run.store.drain(through=uuids))},
            goto="tools" if ai.tool_calls else "model",
        )

    async def tools(state: ClaudeCodeState, config: RunnableConfig) -> Any:
        """Run each call once the CLI asks for it, with the arguments it sends,
        as ``create_agent``'s own ``Send`` of one call runs it, and keep the
        CLI's own answer for each it refused, so the results read as Claude's
        session has them, in call order. A call that raises fails the step at
        once: the CLI waits for its result, and the rest are cancelled."""
        turn = _turn(config)
        run = turn.current
        calls = state["messages"][-1].tool_calls
        run.after_tool.open([c["id"] for c in calls], partial(turn.steering.take, config))

        async def call(c: ToolCall) -> list[_Update]:
            match await run.results.due(c["id"]):
                case Ended():
                    await run.finish()
                    raise ClaudeTurnFailed("api_error", f"the CLI ended the turn before it ran call {c['id']}")
                case Answered(block):
                    return [{"messages": [_as_given(ToolMessage(content="", tool_call_id=c["id"], name=c["name"]), block)]}]
                case Run(args):
                    out = await tool_node.ainvoke([{**c, "args": args}], config)
                    for m in _tool_messages(out):
                        run.results.resolve(m)
                    return out if isinstance(out, list) else [out]

        try:
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(call(c)) for c in calls]
        except BaseExceptionGroup as failed:
            raise failed.exceptions[0] from None
        uuids = await run.tool_result_records([c["id"] for c in calls])
        await run.settle(uuids)
        steered: list[HumanMessage] = []
        if (message := await run.after_tool.taken()) is not None:
            turn.steering.delivered(config, message)
            steered.append(message)
        session = turn.commit(run.store.drain(through=uuids))
        updates = [_given_instead(u, run.results, c) for c, t in zip(calls, tasks, strict=True) for u in t.result()]
        if all(isinstance(u, dict) for u in updates):
            return {"messages": [*(m for u in updates for m in u["messages"]), *steered], **session}
        return [*updates, Command(update={"messages": steered, **session})]

    g = StateGraph(ClaudeCodeState)
    for name, node in entry:
        g.add_node(name, node)
    g.add_node("model", model, destinations=("tools", "model", END))
    g.add_node("tools", tools)
    path = [START, *(name for name, _ in entry), "model"]
    for a, b in pairwise(path):
        g.add_edge(a, b)
    g.add_edge("tools", "model")
    return g.compile(checkpointer=checkpointer)


def _cut_off(messages: Sequence[BaseMessage]) -> tuple[int, list[ToolCall]] | None:
    """Where the thread's last assistant message is, and its calls no result
    answers, when the thread went on past it without them."""
    at = next((i for i in range(len(messages) - 1, -1, -1) if isinstance(messages[i], AIMessage)), None)
    if at is None or at == len(messages) - 1:
        return None
    answered = {m.tool_call_id for m in messages[at + 1 :] if isinstance(m, ToolMessage)}
    calls = [c for c in messages[at].tool_calls if c["id"] not in answered]
    return (at, calls) if calls else None


def _as_given(message: ToolMessage, block: ToolResultBlock) -> ToolMessage:
    return message.model_copy(update={"content": block.content or "", "status": "error" if block.is_error else "success"})


def _unrecorded(call: ToolCall) -> str:
    """What ``PatchToolCallsMiddleware`` answers a call no result was recorded for."""
    return (
        f"Tool call {call['name'] or 'unknown'} with id {call['id']} did not complete - no result was recorded. "
        "It may have been cancelled or interrupted."
    )


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


def _tool_messages(out: Any) -> list[ToolMessage]:
    if isinstance(out, dict):
        return [m for m in out.get("messages", []) if isinstance(m, ToolMessage)]
    found: list[ToolMessage] = []
    for item in out:
        update = item.update if isinstance(item, Command) else item
        if isinstance(update, dict):
            found.extend(m for m in update.get("messages", []) if isinstance(m, ToolMessage))
    return found
