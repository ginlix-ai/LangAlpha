"""Execute bash commands in the sandbox.

Every command runs as a detached job on the computer (``core/sandbox/jobs``).
The main agent's call waits up to its ``timeout`` and hands a job still running
off to its thread (``command_handoff``); a subagent's call waits until the job
ends.
"""

import inspect
from dataclasses import replace
from datetime import UTC, datetime
from typing import Annotated, Any

import structlog
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, InjectedToolCallId, StructuredTool
from pydantic import BaseModel, ConfigDict
from pydantic.json_schema import SkipJsonSchema

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.tools import command_handoff
from ptc_agent.agent.tools.command_handoff import CommandCall, HandOff
from ptc_agent.agent.tools.mount_guard import run_guarded
from ptc_agent.agent.tools.utils import resource_tier_of
from ptc_agent.core.paths import (
    MEMO_USER_DIR,
    MEMORY_USER_DIR,
    WorkspaceLayout,
)
from ptc_agent.core.sandbox import livefs_mount
from ptc_agent.core.sandbox.execution import JobExecFailed
from ptc_agent.core.sandbox.jobs import Ended, Failed, JobHandle, JobNotLaunched, JobOutcome
from src.observability import (
    TOOL_CHANNEL,
    TOOL_PROVIDER,
    mark_tool_failed,
    stamp_resource_tier,
)

logger = structlog.get_logger(__name__)

# Memory and memo live in the store, which the sandbox sees only while the
# file mount serves it. Without the mount, refuse those paths so the agent
# routes them through the file tools instead of silently dropping writes on
# the sandbox (which would also let the agent fabricate fake memos invisible
# to the UI).
_MEMORY_ROUTE_ERROR = (
    f"ERROR: Store-backed paths ({MEMORY_USER_DIR}/**, "
    f"{WorkspaceLayout.MEMORY_DIR}/**, "
    f"{MEMO_USER_DIR}/**) are managed by the long-term memory/memo system and "
    "are NOT on the workspace filesystem. Use the Write, Edit, Read, Glob, or "
    "Grep file tools for these paths so they route to the store. Memo paths are "
    "additionally read-only to the agent — ask the user to upload via the memo "
    "panel. Bash will not see or persist memory or memo content."
)


# The automation files are rows in the database, which the sandbox sees only
# while the file mount serves them.
_FILES_ROUTE_ERROR = (
    "ERROR: {dir}/** is kept on the server, not on the sandbox filesystem, and no "
    "file mount serves it to Bash right now. Use Read, Edit and Write on the files in "
    "{dir}/. Bash can't see or change them."
)


# A directory only the file tools reach, mounted or not.
_FILE_TOOLS_ONLY_ERROR = (
    "ERROR: {dir}/ is reachable only through the file tools. Use Read, Edit and "
    "Write on the files in {dir}/. Bash can't see or change them."
)


_DEFAULT_TIMEOUT_S = command_handoff.HANDOFF_WAIT_S
_MAX_TIMEOUT_S = 600

# Ahead of the output of a command run in the foreground on a full disk.
_DISK_FULL_NOTE = (
    "(The computer's disk is full. Until there is free space, a command runs "
    "without a log and is stopped at its timeout instead of moving to the "
    "background.)\n"
)
# The answers to a command asked for the background whose job could not be
# set up, on a full disk and otherwise. Nothing ran, so no exit code leads them.
_DISK_FULL_NO_BACKGROUND = (
    "The computer's disk is full, so the command could not start in the "
    "background and did not run. Free space, then run it again."
)
_NOT_LAUNCHED = (
    "The command could not start in the background, so it did not run. Run it "
    "again, or without run_in_background."
)

_BASH_SUMMARY = """\
Execute bash commands in a fresh shell in your workspace folder.

Use for: git, npm, docker, system commands, directory operations,
and running Python scripts written to files.
NOT for: reading/writing/editing files - use Read/Write/Edit tools instead
"""

# The main agent's: a command still running at the wait moves to the
# background and reports back to the thread. The hand-off result and the
# notice tell the agent what then becomes of it, so this keeps only what
# shapes the call before it runs.
_BASH_DOC = (
    _BASH_SUMMARY
    + """
Args:
    command: The bash command to execute. Quote paths containing spaces.
    description: Brief description (5-10 words, active voice)
    timeout: Milliseconds to wait (default: 300000, max: 600000). A command
        still running then moves to the background instead of being killed.
    run_in_background: Return a command_id at once instead of waiting
        (default: False). Use it for a server you need to keep alive, or work
        you have no reason to wait on.
    working_dir: Where to run (default: your workspace folder, which
        relative paths in the command resolve against)

Returns:
    Combined stdout and stderr, or an ERROR message.

Write a job that may run for hours so it saves its progress as it goes and a
rerun resumes where it stopped.
"""
)

# A subagent's: it already runs in the background, so its call waits until
# the command ends, and nothing reports back to it later.
_SUBAGENT_BASH_DOC = (
    _BASH_SUMMARY
    + """
Args:
    command: The bash command to execute. Quote paths containing spaces.
    description: Brief description (5-10 words, active voice)
    working_dir: Where to run (default: your workspace folder, which
        relative paths in the command resolve against)

Returns:
    Combined stdout and stderr, or an ERROR message.

The call waits until the command ends, which can take hours; if your own
time limit runs out first, the command is stopped. To leave a server
running, end the command with & and send its output to a file, as in
nohup python -m http.server 8050 > server.log 2>&1 &
"""
)


# Arguments the main agent's Bash takes and a subagent's does not. Dropping
# them would leave a subagent that asked for the background waiting on a
# server that never ends, so a call naming either is answered with this.
_SUBAGENT_REFUSED_ARGS = ("run_in_background", "timeout")
_SUBAGENT_REFUSED = (
    "ERROR: A subagent's Bash takes no {names}: the call waits until the command "
    "ends. To leave a server running, end the command with & and send its output "
    "to a file."
)


class _NotGiven:
    def __repr__(self) -> str:
        return "<not given>"


_NOT_GIVEN: Any = _NotGiven()


class _SubagentBashArgs(BaseModel):
    """A subagent's Bash arguments. Any other key is refused rather than
    dropped; the two the main agent's Bash takes are accepted only to be
    refused by name, and stay out of the schema the model sees."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    command: str
    description: str | None = None
    working_dir: str | None = None
    run_in_background: SkipJsonSchema[Any] = _NOT_GIVEN
    timeout: SkipJsonSchema[Any] = _NOT_GIVEN


def subagent_refusal(**given: Any) -> str | None:
    """The answer to a subagent's Bash call naming an argument only the main
    agent's takes; None when it names none."""
    names = [n for n in _SUBAGENT_REFUSED_ARGS if given.get(n, _NOT_GIVEN) is not _NOT_GIVEN]
    return _SUBAGENT_REFUSED.format(names=" or ".join(names)) if names else None


def bash_description(hand_off: HandOff | None) -> str:
    """The Bash tool's description for the agent it is built for."""
    return inspect.cleandoc(_BASH_DOC if hand_off is not None else _SUBAGENT_BASH_DOC)


def _timeout_seconds(timeout: int | None) -> int:
    """How long the call waits, in whole seconds, between 1 and 600.

    The argument is documented in milliseconds, but a model often passes
    seconds. A value under 1000 is read as seconds: no shell command is
    meant to get less.
    """
    if not timeout or timeout < 0:
        return _DEFAULT_TIMEOUT_S
    seconds = timeout if timeout < 1000 else timeout // 1000
    return min(seconds, _MAX_TIMEOUT_S)


def create_execute_bash_tool(
    backend: SandboxBackend,
    thread_id: str = "",
    *,
    session: Any = None,
    call_context: livefs_mount.CallContext | None = None,
    file_tools_only: tuple[str, ...] = (),
    allowed_servers: frozenset[str] | None = None,
    hand_off: HandOff | None = None,
) -> BaseTool:
    """Factory function to create Bash tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox
        thread_id: Short thread ID (first 8 chars) for thread-scoped script storage
        session: The machine's session, read at call time for its
            ``resource_tier`` to tag the run
        call_context: Who the command runs for, which a save through the file
            mount reads its defaults from
        file_tools_only: Directories under the sandbox root only the file
            tools reach; a command naming one is refused before it runs
        allowed_servers: The only MCP servers a script it runs may call, for
            a subagent the user limited; None leaves the workspace's whole set
        hand_off: The main agent's thread, which a command still running at
            the wait is handed to. None for a subagent, whose call waits until
            the command ends.

    Returns:
        Configured Bash tool function
    """

    async def Bash(
        command: str,
        description: str | None = None,
        timeout: int | None = 300000,
        run_in_background: bool | None = False,
        working_dir: str | None = None,
        *,
        config: RunnableConfig,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> tuple[str, dict[str, Any]]:
        call = CommandCall("Bash", command, description, config, tool_call_id, session)
        return await _guarded(
            command,
            lambda call_id: _run(
                call,
                working_dir,
                call_id,
                first_wait=_timeout_seconds(timeout),
                background=bool(run_in_background),
            ),
        )

    async def SubagentBash(
        command: str,
        description: str | None = None,
        working_dir: str | None = None,
        run_in_background: Any = _NOT_GIVEN,
        timeout: Any = _NOT_GIVEN,
        *,
        config: RunnableConfig,
    ) -> tuple[str, dict[str, Any]]:
        refused = subagent_refusal(run_in_background=run_in_background, timeout=timeout)
        if refused is not None:
            return refused, {"mcp_trace": []}
        call = CommandCall("Bash", command, description, config, None, session)
        return await _guarded(command, lambda call_id: _run(call, working_dir, call_id))

    async def _guarded(command: str, run: Any) -> tuple[str, dict[str, Any]]:
        return await run_guarded(
            backend,
            command,
            call_context,
            run,
            memory_error=_MEMORY_ROUTE_ERROR,
            files_error=_FILES_ROUTE_ERROR,
            file_tools_only=file_tools_only,
            file_tools_error=_FILE_TOOLS_ONLY_ERROR,
            blocked_event="Blocked bash command touching a store-backed path",
            command_length=len(command),
        )

    async def _run(
        call: CommandCall,
        working_dir: str | None,
        call_id: str | None,
        *,
        first_wait: float = command_handoff.HANDOFF_WAIT_S,
        background: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        # Every run, not only a failed one; read per call because a long
        # turn's session can be rebound after the tool is built.
        stamp_resource_tier(resource_tier_of(session, backend))
        logger.debug(
            "Executing bash command",
            command_length=len(call.command),
            working_dir=working_dir,
            thread_id=thread_id or None,
        )

        async def launch(wait_s: float) -> JobOutcome:
            try:
                return await backend.start_bash_job(
                    call.command,
                    working_dir=working_dir,
                    wait_s=wait_s,
                    thread_id=thread_id or None,
                    call_id=call_id,
                    allowed_servers=allowed_servers,
                )
            except JobNotLaunched as e:
                return await _not_launched(e, wait_s, call.config)

        try:
            return await command_handoff.run_command(
                backend,
                call,
                launch,
                _result,
                hand_off=hand_off,
                first_wait=first_wait,
                background=background,
            )
        except Exception as e:
            return _raised(e)

    async def _not_launched(
        e: JobNotLaunched, wait_s: float, config: RunnableConfig
    ) -> Ended | Failed:
        """A command whose job could not be set up runs in the foreground, as
        Bash ran every command before jobs, so a full disk does not refuse
        every command, including the ones that would free it. One asked for
        the background gets nothing run: a foreground exec cannot outlive the
        call."""
        if wait_s <= 0:
            reason = _DISK_FULL_NO_BACKGROUND if e.disk_full else _NOT_LAUNCHED
            return Failed(JobHandle(e.spec.ref, datetime.now(UTC)), "not_launched", reason)
        if hand_off is None:
            # A subagent's call runs it to its own clock, not to its first wait.
            wait_s = command_handoff.wait_left(config) or _DEFAULT_TIMEOUT_S
        outcome = await backend.run_foreground(e.spec, int(min(wait_s, _MAX_TIMEOUT_S)))
        if not e.disk_full:
            return outcome
        if isinstance(outcome, Ended):
            return replace(outcome, output=_DISK_FULL_NOTE + outcome.output)
        # Most of all one stopped at its timeout, which says why it was not
        # moved to the background.
        return replace(outcome, message=_DISK_FULL_NOTE + outcome.message)

    async def _result(outcome: Ended | Failed) -> tuple[str, dict[str, Any]]:
        """An ended job's output, as the call returns it."""
        # Provenance for any MCP calls a script run here made (absent/empty
        # for plain shell commands). Stripped from the artifact by the
        # provenance middleware before it leaves the host.
        artifact = {"mcp_trace": list(outcome.trace)}
        output = await command_handoff.settle(backend, outcome, hand_off)
        match outcome:
            case Failed():
                logger.warning(
                    "Bash command failed in the sandbox",
                    command_id=outcome.ref.command_id,
                    failure=outcome.failure,
                )
                # No exit code was seen, so the reason is all there is to say.
                if outcome.reason in ("not_launched", "lost"):
                    return f"ERROR: {output}", artifact
                code = -1
            case Ended(exit_code=0):
                logger.debug(
                    "Bash command executed successfully",
                    command_id=outcome.ref.command_id,
                    output_length=len(output),
                )
                # Command succeeded but no output (e.g., mkdir)
                return output or "Command completed successfully", artifact
            case Ended(exit_code=code):
                logger.warning(
                    "Bash command failed",
                    command_id=outcome.ref.command_id,
                    exit_code=code,
                    output_length=len(output),
                )
        return (
            f"ERROR: Command failed (exit code {code})\n"
            + (output or "Command execution failed (no output)"),
            artifact,
        )

    def _raised(e: Exception) -> tuple[str, dict[str, Any]]:
        error_msg = f"Failed to execute bash command: {e!s}"
        logger.error(error_msg, error=str(e), exc_info=True)
        # The provider's own class, which a job exec's failure wraps.
        mark_tool_failed(e.cause if isinstance(e, JobExecFailed) else e)
        return f"ERROR: {error_msg}", {"mcp_trace": []}

    bash = StructuredTool.from_function(
        coroutine=Bash if hand_off is not None else SubagentBash,
        name="Bash",
        description=bash_description(hand_off),
        args_schema=None if hand_off is not None else _SubagentBashArgs,
        response_format="content_and_artifact",
    )
    # Every run, not only a failed one, so an error rate reads per provider.
    bash.metadata = {
        TOOL_PROVIDER: backend.provider,
        TOOL_CHANNEL: backend.channel,
    }
    return bash
