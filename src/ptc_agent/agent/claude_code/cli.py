"""Starting the Claude Code CLI for a turn: its options, its launch, and reading what it says.

The options give Claude Code none of its own prompt, settings or tools, so a
turn's prefix is the one langalpha builds. Every launch, a turn's and a
compaction's, goes through ``launch``, which leaves nothing behind when a
step fails.
"""

from __future__ import annotations

import contextlib
import json
import re
from collections.abc import AsyncIterable, AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    Message,
    ProcessError,
    RateLimitEvent,
    ResultMessage,
    SystemMessage,
)
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

#: Ends of a query that did what was asked. Anything else is a failed turn.
_DONE = {None, "completed", "tool_deferred"}
#: The CLI's own login messages; a word like "authentication" alone also turns up in tool and MCP errors.
_AUTH_TEXT = ("not logged in", "run /login", "invalid api key", "oauth token has been revoked")
#: How the CLI says a session holds nothing it can summarize.
_NOTHING_TO_COMPACT = ("no messages to compact", "not enough messages to compact")
_ASSISTANT_ERROR_KIND = {"authentication_failed": "auth_failed", "rate_limit": "plan_exhausted", "billing_error": "plan_exhausted"}


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


def is_haiku(model: str) -> bool:
    """Whether ``model``, a session's model by its API id, is a Claude Haiku
    (``claude-haiku-*``), whose summary of a chat answers the chat's last
    prompt instead, so its compactions get help no other model's need: every
    other model compacts as Claude Code does on its own."""
    return model.startswith("claude-haiku-")


def options_for(
    spec: RuntimeSpec,
    *,
    pin: SessionPin,
    server: Any,
    store: CheckpointSessionStore,
    session_id: str | None,
    prepared: Prepared,
    after_tool: AfterTool,
    compaction: bool = False,
) -> ClaudeAgentOptions:
    """The SDK options: Claude Code with none of its own prompt, settings or tools.

    The system prompt goes as a file in the turn's config dir, never on the
    command line, where Linux caps one argument at 128 KiB and any local
    account can read it. ``after_tool`` is the hook the CLI calls once each
    of our calls returns, whether it succeeded or failed. A line the CLI
    writes can carry a whole result, a PDF's document block included, so
    the SDK reads lines up to twice the largest one a tool returns. A
    ``compaction`` launch is the one that sends a slash command (``/compact``).
    """
    hooks = {event: [HookMatcher(matcher=None, hooks=[after_tool])] for event in ("PostToolUse", "PostToolUseFailure")}
    return ClaudeAgentOptions(
        tools=[],
        system_prompt={"type": "file", "path": prepared.system_prompt_file},
        setting_sources=[],
        strict_mcp_config=True,
        extra_args={} if compaction else {"disable-slash-commands": None},
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
        verbatim_prompts=not compaction,
        cwd=prepared.cwd,
        env={
            "CLAUDE_CONFIG_DIR": prepared.config_dir,
            "ENABLE_TOOL_SEARCH": "false",
            "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
            "CLAUDE_CODE_DISABLE_CLAUDE_MDS": "1",
            "CLAUDE_CODE_DISABLE_ADVISOR_TOOL": "1",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            # The 1h cache whatever the login: an API key or an account in
            # overage would otherwise get 5m, and a thread's gaps (a reply
            # read, a background command, a wakeup) often pass five minutes.
            "CLAUDE_CODE_PROMPT_CACHE_TTL": "1h",
            "CLAUDE_CODE_MCP_AUTO_BACKGROUND_MS": "0",
            "MCP_TOOL_TIMEOUT": "3600000",
            "MAX_MCP_OUTPUT_TOKENS": "200000",
            # Claude sees our tools under the names the prompt and the
            # docstrings use. With the CLI's mcp__langalpha__ prefix they would
            # read as the prompt's "MCP tools", which have no tool-call form.
            "CLAUDE_AGENT_SDK_MCP_NO_PREFIX": "1",
            # A compaction strips the thinking of the turn it keeps after its
            # summary: the next launch may send it under another prefix, where
            # the API drops it, and the record says so from then on.
            **({"CLAUDE_CODE_WISE_COMET": "1"} if compaction else {}),
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



def compaction_failure(error: str) -> ClaudeTurnFailed | None:
    """Why a compaction the CLI reports failed did, read as a turn's failure
    is; None when it found nothing to summarize, the one failure that leaves
    a session as compacted as it can be."""
    text = error.lower()
    if any(s in text for s in _NOTHING_TO_COMPACT):
        return None
    status = re.search(r"API Error: (\d{3})", error)
    if any(s in text for s in _AUTH_TEXT) or (status and status[1] in ("401", "403")):
        return ClaudeTurnFailed("auth_failed", error)
    return ClaudeTurnFailed("plan_exhausted" if status and status[1] == "429" else "api_error", error or "the compaction failed")


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
    compaction: bool = False,
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
            prepared=prepared, after_tool=after_tool, compaction=compaction,
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
