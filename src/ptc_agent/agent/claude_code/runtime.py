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
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from itertools import pairwise
from typing import Annotated, Any, TypeVar

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    Message,
    ProcessError,
    RateLimitEvent,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    ToolResultBlock,
    UserMessage,
)
from langchain_core.messages import AIMessage, HumanMessage, ToolCall, ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.claude_code.launcher import Launcher, Prepared
from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay, anthropic_content
from ptc_agent.agent.claude_code.session import (
    Arrivals,
    CheckpointSessionStore,
    SessionDrift,
    SessionPin,
    SessionRecords,
    SessionWrite,
    session_channel,
)
from ptc_agent.agent.claude_code.tools_bridge import (
    SERVER,
    Answered,
    Ended,
    Run,
    ToolResults,
    allow_rule,
    build_tool_server,
    tool_specs,
)
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.runtime_context.carrier import Carrier, compose_request
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from src.llms.claude_code import MESSAGE_ID_KEY as CLAUDE_MESSAGE_ID
from src.llms.claude_code import ClaudeTurnFailed, another_model_replied_last
from src.server.utils.error_sanitization import sanitize_error_text

TURN_KEY = "claude_turn"
T = TypeVar("T")
logger = logging.getLogger(__name__)
#: How long a step waits for the CLI's records behind the messages it commits.
SETTLE_TIMEOUT_S = 10.0
TOKEN_CHECK_TIMEOUT_S = 60
#: How much of the CLI's stderr a failed start passes on.
STARTUP_REASON_LINES = 5
STARTUP_REASON_CHARS = 600


#: Ends of a query that did what was asked. Anything else is a failed turn.
_DONE = {None, "completed", "tool_deferred"}
#: The CLI's own login messages; a word like "authentication" alone also turns up in tool and MCP errors.
_AUTH_TEXT = ("not logged in", "run /login", "invalid api key", "oauth token has been revoked")
_ASSISTANT_ERROR_KIND = {"authentication_failed": "auth_failed", "rate_limit": "plan_exhausted", "billing_error": "plan_exhausted"}


def turn_failure(
    result: ResultMessage | None,
    assistant_errors: Sequence[tuple[str, str]],
    rate_limits: Sequence[RateLimitEvent] = (),
) -> ClaudeTurnFailed | None:
    """Why a finished query did not do its turn, or None if it did."""
    if assistant_errors:
        error, text = assistant_errors[0]
        return ClaudeTurnFailed(_ASSISTANT_ERROR_KIND.get(error, "api_error"), text or error)
    if result is not None and not result.is_error and result.subtype == "success" and result.terminal_reason in _DONE:
        return None
    # Only a query that failed reads the rate limits: a rejected plan window
    # beside a served request means extra usage paid for it.
    rejected = [e.rate_limit_info for e in rate_limits if e.rate_limit_info.status == "rejected"]
    if rejected:
        info = rejected[-1]
        return ClaudeTurnFailed("plan_exhausted", f"{info.rate_limit_type} limit reached, resets at {info.resets_at}")
    if result is None:
        return ClaudeTurnFailed("no_result", "the CLI ended without a result")
    text = str(result.result or "") or "; ".join(result.errors or [])
    detail = f"{result.subtype}/{result.terminal_reason}: {text or 'no detail'}"
    if str(result.terminal_reason or "").startswith("aborted"):
        return ClaudeTurnFailed("interrupted", detail)
    # The login text is read only where the CLI's request failed: any other
    # end's text is the model's or a tool's, which can quote a vendor's.
    cli_said = result.terminal_reason == "api_error" and any(s in text.lower() for s in _AUTH_TEXT)
    if result.api_error_status in (401, 403) or cli_said:
        return ClaudeTurnFailed("auth_failed", detail)
    if result.api_error_status == 429:
        return ClaudeTurnFailed("plan_exhausted", detail)
    return ClaudeTurnFailed("api_error", detail)


def startup_failure(error: ProcessError) -> ClaudeTurnFailed:
    """The CLI exiting before it took the turn, in its own words: the last
    lines of its stderr, bounded, and scrubbed although a refusal to start
    names no credential."""
    lines = [line.strip() for line in (error.stderr or "").splitlines() if line.strip()]
    reason = sanitize_error_text(" ".join(lines[-STARTUP_REASON_LINES:]))[:STARTUP_REASON_CHARS]
    return ClaudeTurnFailed("startup_failed", reason or f"it exited with code {error.exit_code}")


@contextlib.asynccontextmanager
async def _open(client: ClaudeSDKClient) -> AsyncIterator[None]:
    """Start the CLI for the body to send the query. A CLI that exits first,
    or while the query goes, fails in its own words."""
    try:
        await client.connect()
        yield
    except ProcessError as e:
        raise startup_failure(e) from None


async def _received(client: ClaudeSDKClient) -> AsyncIterator[Message]:
    """What the CLI says. One that exits before its init never took the
    turn, so it fails in its own words as at the start."""
    init_seen = False
    try:
        async for m in client.receive_messages():
            init_seen = init_seen or (isinstance(m, SystemMessage) and m.subtype == "init")
            yield m
    except ProcessError as e:
        if init_seen:
            raise
        raise startup_failure(e) from None


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
                async with _open(client):
                    await client.query("Reply with OK.")
                async for m in _received(client):
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


@dataclass(frozen=True)
class RuntimeSpec:
    """What this build runs a Claude Code turn with.

    The system prompt and the tool list are part of the prefix every earlier
    thinking block is bound to, so they only start a session: its later turns
    run on the copy it keeps (``session.SessionPin``), and what changes
    reaches Claude as appended messages.
    """

    system_prompt: str
    model: str
    tools: Sequence[BaseTool]
    #: The client metadata usage is recorded under (``billing_attribution``),
    #: as the LLM layer stamps it for the thread's catalog entry.
    billing: Mapping[str, str]
    effort: str | None = None
    read_only: frozenset[str] = frozenset()


def options_for(
    spec: RuntimeSpec,
    *,
    pin: SessionPin,
    server: Any,
    store: CheckpointSessionStore,
    session_id: str | None,
    prepared: Prepared,
) -> ClaudeAgentOptions:
    """The SDK options: Claude Code with none of its own prompt, settings or tools.

    The system prompt goes as a file in the turn's config dir, never on the
    command line, where Linux caps one argument at 128 KiB and any local
    account can read it.
    """
    return ClaudeAgentOptions(
        tools=[],
        system_prompt={"type": "file", "path": prepared.system_prompt_file},
        setting_sources=[],
        strict_mcp_config=True,
        extra_args={"disable-slash-commands": None},
        mcp_servers={SERVER: server},
        allowed_tools=[allow_rule(t["name"]) for t in pin["tools"]],
        permission_mode="dontAsk",
        model=spec.model,
        effort=spec.effort,
        thinking={"type": "adaptive", "display": "summarized"},
        include_partial_messages=True,
        session_store=store,
        session_store_flush="eager",
        resume=session_id,
        # User text reaches Claude as written: no @path expansion reading files
        # on this machine, and no slash commands.
        verbatim_prompts=True,
        cwd=prepared.cwd,
        env={
            "CLAUDE_CONFIG_DIR": prepared.config_dir,
            "ENABLE_TOOL_SEARCH": "false",
            "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "CLAUDE_CODE_MCP_AUTO_BACKGROUND_MS": "0",
            "MCP_TOOL_TIMEOUT": "3600000",
            "MAX_MCP_OUTPUT_TOKENS": "200000",
            # Claude sees our tools under the names the prompt and the
            # docstrings use. With the CLI's mcp__langalpha__ prefix they would
            # read as the prompt's "MCP tools", which have no tool-call form.
            "CLAUDE_AGENT_SDK_MCP_NO_PREFIX": "1",
        },
        # A tool named Bash makes the CLI add its own git attribution reminder.
        settings=json.dumps({"includeGitInstructions": False}),
    )


_END = object()


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
            if item.get("type") == "message_start":
                self.restarted = True
                self._run.hold(item)
                return


class _Run:
    """A started run: the CLI, the SDK client driving it, and what the CLI has said so far."""

    def __init__(self, launcher: Launcher, prepared: Prepared, store: CheckpointSessionStore,
                 client: ClaudeSDKClient, results: ToolResults, spec: RuntimeSpec) -> None:
        self.launcher = launcher
        self.prepared = prepared
        self.store = store
        self.client = client
        self.results = results
        self.model = spec.model
        self.result: ResultMessage | None = None
        self.rate_limits: list[RateLimitEvent] = []
        #: (error, text) of each assistant message the CLI marked as an error.
        self.assistant_errors: list[tuple[str, str]] = []
        self.drift: SessionDrift | None = None
        self._events: asyncio.Queue[Any] = asyncio.Queue()
        self._held: dict[str, Any] | None = None
        self._streamed: set[str] = set()
        self._assistant: dict[str, list[str]] = {}
        self._tool_results = Arrivals("tool result records")
        self._closing: asyncio.Future[None] | None = None
        self._finishing: asyncio.Future[None] | None = None
        self._pump = asyncio.create_task(self._read())

    @classmethod
    async def start(cls, spec: RuntimeSpec, pin: SessionPin, launcher: Launcher, thread_key: str,
                    identity: str, records: SessionRecords | None, prompt: Prompt | None) -> _Run:
        """Lay out the session, start the CLI and send the turn; nothing is left behind if any step fails.

        With no prompt the CLI is resumed as an interrupted turn: it carries on
        from the tool results that end its session without a new query.
        """
        session_id = (records or {}).get("session_id")
        prepared = await launcher.prepare(
            thread_key, session_id, (records or {}).get("entries") or [],
            identity=identity, system_prompt=pin["system_prompt"],
        )
        client: ClaudeSDKClient | None = None
        try:
            store, results = CheckpointSessionStore(), ToolResults()
            options = options_for(
                spec, pin=pin, server=build_tool_server(pin["tools"], results), store=store,
                session_id=session_id, prepared=prepared,
            )
            if prompt is None:
                options.env["CLAUDE_CODE_RESUME_INTERRUPTED_TURN"] = "1"
            client = ClaudeSDKClient(options, transport=launcher.transport(options))
            async with _open(client):
                if prompt is not None:
                    await client.query(prompt if isinstance(prompt, str) else _user_message(prompt))
        except BaseException:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
            await launcher.release(prepared)
            raise
        return cls(launcher, prepared, store, client, results, spec)

    async def _read(self) -> None:
        """Read what the CLI says until the turn's result.

        Its init comes ahead of its first request. One naming another model
        than the turn's is a policy refusing it (``availableModels``), one
        no file on the machine shows, such as the organization's account's,
        where the CLI would run another in its place, so the turn fails before
        anything is streamed.
        """
        try:
            async for m in _received(self.client):
                if isinstance(m, SystemMessage) and m.subtype == "init":
                    if m.data.get("model") != self.model:
                        logger.warning("[ClaudeCode] the CLI would run %s in place of %s", m.data.get("model"), self.model)
                        raise ClaudeTurnFailed("model_unavailable", self.model)
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
                    await self._tool_results.add(dict.fromkeys((b.tool_use_id for b in blocks), m.uuid))
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

    async def next_message(self) -> _Message | None:
        """The next assistant message, or None once the turn has ended."""
        first = await self.next_event()
        if first is _END:
            return None
        return _Message(first, self)

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
        return await self._unless_crashed(self._tool_results.wait(tool_use_ids, SETTLE_TIMEOUT_S))

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
    The launcher picks the token the run signs in with.
    """

    def __init__(self, spec: RuntimeSpec, launcher: Launcher, *, identity: str, thread_key: str,
                 baseline: Baseline | None = None) -> None:
        self.spec = spec
        self.launcher = launcher
        self.identity = identity
        self.thread_key = thread_key
        self._baseline = baseline
        #: The session this run writes records to, and what its next write
        #: owes it: the pin and baseline this run launched with that it does
        #: not hold yet.
        self._session: str | None = None
        self._owed: dict[str, Any] = {}
        #: What a session this run starts holds, since the CLI may start one
        #: over instead of resuming.
        self._fresh: dict[str, Any] = {}
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

    async def run(self, state: ClaudeCodeState) -> _Run:
        if self._run is None:
            pin, prompt = self.opening(state)
            self._run = await _Run.start(
                self.spec, pin, self.launcher, self.thread_key, self.identity, state.get("claude_session"), prompt
            )
        return self._run

    def opening(self, state: ClaudeCodeState) -> tuple[SessionPin, Prompt | None]:
        """What the run starts Claude on: the session's own pin, else this
        build's, which the session holds from the run's first records on (a
        new session, one from before sessions kept theirs, or one Claude Code
        compacted), and the turn's prompt."""
        records = state.get("claude_session") or {}
        key, block = self._baseline_due(state)
        prompt = turn_prompt(state, baseline=block)
        self._session = records.get("session_id")
        kept = records.get("pinned") if self._session else None
        pin = kept or self.current_pin()
        sent = {"baseline": key} if prompt is not None and block else {}
        self._fresh = {"pinned": pin, **sent}
        self._owed = sent if kept else self._fresh
        return pin, prompt

    def current_pin(self) -> SessionPin:
        """The prompt and tools this build would start a session on."""
        return {"system_prompt": self.spec.system_prompt, "tools": tool_specs(self.spec.tools, self.spec.read_only)}

    def commit(self, session: list[SessionWrite]) -> dict[str, Any]:
        """A step's session records, the first of them carrying what the
        session holds from then on that it did not before."""
        for write in session:
            if write["session_id"] != self._session:
                self._session, self._owed = write["session_id"], self._fresh
            if self._owed:
                write.update(self._owed)
                self._owed = {}
        return {"claude_session": session}

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
    end, seen by a ``model`` step with no message left, ends the turn.
    """

    async def model(state: ClaudeCodeState, config: RunnableConfig) -> Command:
        turn = _turn(config)
        run = await turn.run(state)
        message = await run.next_message()
        if message is None:
            await run.finish()
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
        uuids = run.assistant_records(message.id)
        await run.settle(uuids)
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
        session = turn.commit(run.store.drain(through=uuids))
        updates = [_given_instead(u, run.results, c) for c, t in zip(calls, tasks, strict=True) for u in t.result()]
        if all(isinstance(u, dict) for u in updates):
            return {"messages": [m for u in updates for m in u["messages"]], **session}
        return [*updates, Command(update=session)]

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


def _as_given(message: ToolMessage, block: ToolResultBlock) -> ToolMessage:
    return message.model_copy(update={"content": block.content or "", "status": "error" if block.is_error else "success"})


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
