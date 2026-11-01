"""Starting the Claude Code CLI for a turn: its options, its launch, and reading what it says.

The options give Claude Code none of its own prompt, settings or tools, so a
turn's prefix is the one langalpha builds. A launch goes through ``launch``,
which leaves nothing behind when a step fails.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, HookMatcher, Message, ProcessError, SystemMessage
from langchain_core.tools import BaseTool

from ptc_agent.agent.claude_code.launcher import Launcher, Prepared
from ptc_agent.agent.claude_code.mid_turn import AfterTool
from ptc_agent.agent.claude_code.session import CheckpointSessionStore, SessionPin, SessionRecords
from ptc_agent.agent.claude_code.tools_bridge import SERVER, ToolResults, allow_rule, build_tool_server
from ptc_agent.agent.middleware.file_operations.multimodal import MAX_CONTENT_BYTES
from src.llms.claude_code import ClaudeTurnFailed
from src.server.utils.error_sanitization import sanitize_error_text

#: How much of the CLI's stderr a failed start passes on.
STARTUP_REASON_LINES = 5
STARTUP_REASON_CHARS = 600


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
    after_tool: AfterTool,
) -> ClaudeAgentOptions:
    """The SDK options: Claude Code with none of its own prompt, settings or tools.

    The system prompt goes as a file in the turn's config dir, never on the
    command line, where Linux caps one argument at 128 KiB and any local
    account can read it. ``after_tool`` is the hook the CLI calls once each
    of our calls returns, whether it succeeded or failed. A line the CLI
    writes can carry a whole result, a PDF's document block included, so
    the SDK reads lines up to twice the largest one a tool returns.
    """
    hooks = {event: [HookMatcher(matcher=None, hooks=[after_tool])] for event in ("PostToolUse", "PostToolUseFailure")}
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
        hooks=hooks,
        max_buffer_size=2 * MAX_CONTENT_BYTES,
    )


def startup_failure(error: ProcessError) -> ClaudeTurnFailed:
    """The CLI exiting before it took the turn, in its own words: the last
    lines of its stderr, bounded, and scrubbed although a refusal to start
    names no credential."""
    lines = [line.strip() for line in (error.stderr or "").splitlines() if line.strip()]
    reason = sanitize_error_text(" ".join(lines[-STARTUP_REASON_LINES:]))[:STARTUP_REASON_CHARS]
    return ClaudeTurnFailed("startup_failed", reason or f"it exited with code {error.exit_code}")


@contextlib.asynccontextmanager
async def connected(client: ClaudeSDKClient) -> AsyncIterator[None]:
    """Start the CLI for the body to send the query. A CLI that exits first,
    or while the query goes, fails in its own words."""
    try:
        await client.connect()
        yield
    except ProcessError as e:
        raise startup_failure(e) from None


async def received(client: ClaudeSDKClient, *, init_seen: bool = False) -> AsyncIterator[Message]:
    """What the CLI says. One that exits before its init never took the
    turn, so it fails in its own words as at the start."""
    try:
        async for m in client.receive_messages():
            init_seen = init_seen or (isinstance(m, SystemMessage) and m.subtype == "init")
            yield m
    except ProcessError as e:
        if init_seen:
            raise
        raise startup_failure(e) from None


async def launch(
    spec: RuntimeSpec,
    pin: SessionPin,
    launcher: Launcher,
    *,
    thread_key: str,
    identity: str,
    records: SessionRecords | None,
    store: CheckpointSessionStore,
    results: ToolResults,
    query: str | AsyncIterable[dict[str, Any]] | None,
) -> tuple[Prepared, ClaudeSDKClient, AfterTool]:
    """Lay out the session, start the CLI on ``pin`` and send ``query``; nothing
    is left behind if any step fails, and the caller releases what it gets,
    with the hook the CLI calls as each of our calls returns.

    With no query the CLI is resumed as an interrupted turn: it carries on
    from the tool results that end its session without a new query.
    """
    session_id = (records or {}).get("session_id")
    prepared = await launcher.prepare(
        thread_key, session_id, (records or {}).get("entries") or [], identity=identity, system_prompt=pin["system_prompt"],
    )
    client: ClaudeSDKClient | None = None
    try:
        after_tool = AfterTool(results, send=lambda text: client.query(text))
        options = options_for(
            spec, pin=pin, server=build_tool_server(pin["tools"], results), store=store, session_id=session_id,
            prepared=prepared, after_tool=after_tool,
        )
        if query is None:
            options.env["CLAUDE_CODE_RESUME_INTERRUPTED_TURN"] = "1"
        client = ClaudeSDKClient(options, transport=launcher.transport(options))
        async with connected(client):
            if query is not None:
                await client.query(query)
    except BaseException:
        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect()
        await launcher.release(prepared)
        raise
    return prepared, client, after_tool
