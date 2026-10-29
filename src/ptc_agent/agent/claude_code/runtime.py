"""The Claude Code runtime: a thread whose turns Claude Code runs.

A turn is a small LangGraph graph over the thread's own checkpoint, shaped
like the graph ``create_agent`` builds: a ``model`` node per assistant
message and a ``tools`` node per batch of tool calls. The difference is who
decides. Claude Code runs the loop in its own process; each ``model`` step
streams one assistant message from it, and each ``tools`` step runs the
calls that message made through langalpha's own tool node and hands the
results back to the CLI. Because the graph keeps the node names, the state
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
from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
from dataclasses import dataclass
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
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.claude_code.launcher import LocalLauncher, Prepared
from ptc_agent.agent.claude_code.relay import ClaudeStreamRelay, anthropic_content
from ptc_agent.agent.claude_code.session import (
    Arrivals,
    CheckpointSessionStore,
    SessionDrift,
    SessionRecords,
    session_channel,
)
from ptc_agent.agent.claude_code.tools_bridge import SERVER, ToolResults, allow_rule, build_tool_server
from ptc_agent.agent.main_state import MainAgentState

TURN_KEY = "claude_turn"
T = TypeVar("T")
#: How long a step waits for the CLI's records behind the messages it commits.
SETTLE_TIMEOUT_S = 10.0


class ClaudeTurnFailed(RuntimeError):
    """The CLI ended the turn without doing it.

    The CLI reports a refused or failed request as an ordinary result
    (``subtype="success"``, ``terminal_reason="api_error"``) with its error
    text as the assistant's reply, so a logged-out machine or a spent plan
    would otherwise commit an empty turn. ``kind`` is what the user is told:
    ``auth_failed``, ``plan_exhausted``, ``interrupted``, ``api_error``,
    ``no_result`` or ``nothing_to_resume``.
    """

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


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
    if result.api_error_status in (401, 403) or any(s in text.lower() for s in _AUTH_TEXT):
        return ClaudeTurnFailed("auth_failed", detail)
    if result.api_error_status == 429:
        return ClaudeTurnFailed("plan_exhausted", detail)
    return ClaudeTurnFailed("api_error", detail)


class ClaudeCodeState(MainAgentState):
    """A Claude Code thread's checkpoint: a main-agent thread's, plus Claude's session."""

    claude_session: NotRequired[Annotated[SessionRecords, session_channel()]]


@dataclass(frozen=True)
class RuntimeSpec:
    """What a Claude Code turn runs with, fixed for the session's life.

    The system prompt and the tool list are part of the prefix every earlier
    thinking block is bound to, so they are set when the session starts and
    never change under it; what changes reaches Claude as appended messages.
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
        allowed_tools=[allow_rule(t.name) for t in spec.tools],
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

    def __init__(self, launcher: LocalLauncher, prepared: Prepared, store: CheckpointSessionStore,
                 client: ClaudeSDKClient, results: ToolResults) -> None:
        self.launcher = launcher
        self.prepared = prepared
        self.store = store
        self.client = client
        self.results = results
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
        self._pump = asyncio.create_task(self._read())

    @classmethod
    async def start(cls, spec: RuntimeSpec, launcher: LocalLauncher, thread_key: str, identity: str,
                    records: SessionRecords | None, prompt: Prompt | None) -> _Run:
        """Lay out the session, start the CLI and send the turn; nothing is left behind if any step fails.

        With no prompt the CLI is resumed as an interrupted turn: it carries on
        from the tool results that end its session without a new query.
        """
        session_id = (records or {}).get("session_id")
        prepared = await launcher.prepare(
            thread_key, session_id, (records or {}).get("entries") or [],
            identity=identity, system_prompt=spec.system_prompt,
        )
        client: ClaudeSDKClient | None = None
        try:
            store, results = CheckpointSessionStore(), ToolResults()
            options = options_for(
                spec, server=build_tool_server(spec.tools, results, spec.read_only), store=store,
                session_id=session_id, prepared=prepared,
            )
            if prompt is None:
                options.env["CLAUDE_CODE_RESUME_INTERRUPTED_TURN"] = "1"
            client = ClaudeSDKClient(options, transport=launcher.transport(options))
            await client.connect()
            if prompt is not None:
                await client.query(prompt if isinstance(prompt, str) else _user_message(prompt))
        except BaseException:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
            await launcher.release(prepared)
            raise
        return cls(launcher, prepared, store, client, results)

    async def _read(self) -> None:
        try:
            async for m in self.client.receive_messages():
                if isinstance(m, StreamEvent):
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
                    ids = [b.tool_use_id for b in (m.content if isinstance(m.content, list) else []) if isinstance(b, ToolResultBlock)]
                    await self._tool_results.add(dict.fromkeys(ids, m.uuid))
                elif isinstance(m, ResultMessage):
                    self.result = m
                    break
                elif isinstance(m, RateLimitEvent):
                    self.rate_limits.append(m)
                elif isinstance(m, SystemMessage) and m.subtype == "mirror_error":
                    self.drift = SessionDrift(f"session mirror dropped a batch: {m.data.get('error')}")
                    break
        finally:
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
        """Wait for the query's end, and fail the turn unless it did its work."""
        await self._pump
        if self.drift is not None:
            raise self.drift
        failed = turn_failure(self.result, self.assistant_errors, self.rate_limits)
        if failed is not None:
            raise failed

    async def close(self) -> None:
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
    """

    def __init__(self, spec: RuntimeSpec, launcher: LocalLauncher, *, identity: str, thread_key: str) -> None:
        self.spec = spec
        self.launcher = launcher
        self.identity = identity
        self.thread_key = thread_key
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
            self._run = await _Run.start(
                self.spec, self.launcher, self.thread_key, self.identity, state.get("claude_session"), turn_prompt(state)
            )
        return self._run

    async def close(self) -> None:
        if self._run is not None:
            await self._run.close()


def _turn(config: RunnableConfig) -> ClaudeTurn:
    return config["configurable"][TURN_KEY]


#: What a turn sends Claude: the user's text, or Anthropic content blocks (turn rows ahead of it).
Prompt = str | list[dict[str, Any]]


def turn_prompt(state: ClaudeCodeState) -> Prompt | None:
    """The messages the turn added since Claude last spoke, as one user message.

    A single plain message goes as the string it is. Several, or any with
    content blocks, go as one list of blocks in order, so runtime rows that
    open the turn ride ahead of the user's text as their own blocks, stored
    verbatim in the session. Blocks are in the form a ``ChatAnthropic`` call
    sends them (an attached image as an image block).

    None when there is no user message because a run is resumed from a
    checkpoint taken after its tool results, which the CLI carries on from.
    Any other checkpoint without one (after an assistant message) is not
    something the CLI resumes, so it fails the turn.
    """
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
    if len(added) == 1 and isinstance(added[0].content, str):
        return added[0].content
    blocks: list[dict[str, Any]] = []
    for msg in added:
        content = anthropic_content(msg)
        blocks.extend([{"type": "text", "text": content}] if isinstance(content, str) else content)
    return blocks


async def _user_message(blocks: list[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    yield {"type": "user", "message": {"role": "user", "content": blocks}, "parent_tool_use_id": None}


def build_claude_code_graph(tool_node: ToolNode, *, checkpointer: Any = None) -> Any:
    """The turn graph. ``tool_node`` is the one a langalpha turn runs tools with.

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
            return Command(update={"claude_session": run.store.drain()}, goto=END)
        relay = ClaudeStreamRelay(model_name=turn.spec.model, metadata=dict(turn.spec.billing), cache=False).feed(message.events())
        ai: AIMessage = await relay.ainvoke([], config)
        await run.complete(message)
        if message.restarted:
            return Command(goto="model")
        if ai.invalid_tool_calls:
            raise ClaudeTurnFailed("api_error", f"tool calls that did not parse: {[c.get('name') for c in ai.invalid_tool_calls]}")
        ai.response_metadata.setdefault("model_name", turn.spec.model)
        ai.response_metadata["claude_message_id"] = message.id
        uuids = run.assistant_records(message.id)
        await run.settle(uuids)
        return Command(
            update={"messages": [ai], "claude_session": run.store.drain(through=uuids)},
            goto="tools" if ai.tool_calls else "model",
        )

    async def tools(state: ClaudeCodeState, config: RunnableConfig) -> Any:
        run = _turn(config).current
        out = await tool_node.ainvoke(state, config)
        for m in _tool_messages(out):
            run.results.resolve(m)
        uuids = await run.tool_result_records([tc["id"] for tc in state["messages"][-1].tool_calls])
        await run.settle(uuids)
        session = {"claude_session": run.store.drain(through=uuids)}
        if isinstance(out, list):
            return [*out, Command(update=session)]
        return {**out, **session}

    g = StateGraph(ClaudeCodeState)
    g.add_node("model", model, destinations=("tools", "model", END))
    g.add_node("tools", tools)
    g.add_edge(START, "model")
    g.add_edge("tools", "model")
    return g.compile(checkpointer=checkpointer)


def _tool_messages(out: Any) -> list[ToolMessage]:
    if isinstance(out, dict):
        return [m for m in out.get("messages", []) if isinstance(m, ToolMessage)]
    found: list[ToolMessage] = []
    for item in out:
        update = item.update if isinstance(item, Command) else item
        if isinstance(update, dict):
            found.extend(m for m in update.get("messages", []) if isinstance(m, ToolMessage))
    return found
