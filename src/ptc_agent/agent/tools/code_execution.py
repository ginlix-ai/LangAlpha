"""Execute code tool for running Python code in the PTC sandbox.

Every run is a detached job on the computer (``core/sandbox/jobs``). The main
agent's call waits five minutes and hands a run still going off to its thread
(``command_handoff``); a subagent's call waits until the run ends.
"""

import inspect
from contextlib import AbstractAsyncContextManager
from typing import Annotated, Any

import structlog
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, InjectedToolCallId, StructuredTool

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.tools import command_handoff
from ptc_agent.agent.tools.code_admission import (
    code_run_slot,
    resource_tiers_of,
)
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
from ptc_agent.core.sandbox.jobs import Ended, Failed, JobOutcome
from src.observability import (
    TOOL_CHANNEL,
    TOOL_PROVIDER,
    mark_tool_failed,
    stamp_resource_tier,
)

logger = structlog.get_logger(__name__)

# Same guard as bash: without the file mount, sandbox Python cannot reach the
# store-backed memory or user-managed memo store. Memo paths are additionally
# read-only to the agent.
_MEMORY_ROUTE_ERROR = (
    f"ERROR: Store-backed paths ({MEMORY_USER_DIR}/**, "
    f"{WorkspaceLayout.MEMORY_DIR}/**, "
    f"{MEMO_USER_DIR}/**) are managed by the long-term memory/memo system and "
    "are NOT on the sandbox filesystem. Read them with the Read tool before this "
    "call and pass the content in as a string; write memory paths with "
    "Write/Edit. Memo paths are read-only — ask the user to upload via the memo "
    "panel. ExecuteCode cannot persist to memory or memo."
)


# Files the server keeps, which the sandbox sees only while the file mount
# serves them.
_FILES_ROUTE_ERROR = (
    "ERROR: {dir}/** is kept on the server, not on the sandbox filesystem, and no "
    "file mount serves it to ExecuteCode right now. Read a file with the Read tool and "
    "pass the content in if your code needs it; change them with Edit and Write."
)


# A directory only the file tools reach, mounted or not.
_FILE_TOOLS_ONLY_ERROR = (
    "ERROR: {dir}/ is reachable only through the file tools. Read a file with the "
    "Read tool and pass the content in if your code needs it; change them with Edit "
    "and Write."
)


_CODE_DOC = """\
Execute Python code directly.

Use for: disposable one-shots — quick MCP calls, small transforms, sanity checks.
Do not use for iterative or reusable code - write to a file and run via Bash instead.
Import MCP tools: from tools.{server} import {tool}

Args:
    code: Python code to execute. Print a summary to stdout. It runs in
        your workspace folder, so use RELATIVE paths (<task>/,
        data/); a leading slash is the real filesystem root,
        where none of those exist.
    description: Brief description (5-10 words, active voice)

Returns:
    SUCCESS or ERROR, then what it printed, stdout and stderr together.{after}
"""
# The main agent's run moves to the background past the wait; a subagent's
# call waits until its run ends. The hand-off result and the notice tell the
# agent what then becomes of it.
_HANDED_OFF_RETURNS = "\n    A run still going after 5 minutes moves to the background."


def code_description(hand_off: HandOff | None) -> str:
    """The ExecuteCode tool's description for the agent it is built for."""
    after = _HANDED_OFF_RETURNS if hand_off is not None else ""
    return inspect.cleandoc(_CODE_DOC.replace("{after}", after))


def create_execute_code_tool(
    backend: SandboxBackend,
    mcp_registry: Any,
    thread_id: str = "",
    *,
    session: Any = None,
    call_context: livefs_mount.CallContext | None = None,
    file_tools_only: tuple[str, ...] = (),
    allowed_servers: frozenset[str] | None = None,
    hand_off: HandOff | None = None,
) -> BaseTool:
    """Factory function to create execute_code tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox
        mcp_registry: MCPRegistry instance with available MCP tools
        thread_id: Short thread ID (first 8 chars) for thread-scoped code storage
        session: The machine's session, read at call time for its ``computer_id``,
            ``resource_tier`` and provider config to size per-computer
            admission. Omitted by callers with no notion of a computer, which
            skips admission.
        call_context: Who the code runs for, which a save through the file
            mount reads its defaults from
        file_tools_only: Directories under the sandbox root only the file
            tools reach; a program naming one is refused before it runs
        allowed_servers: The only MCP servers the code may call, for a
            subagent the user limited; None leaves the workspace's whole set
        hand_off: The main agent's thread, which a run still going at the
            wait is handed to. None for a subagent, whose call waits until the
            run ends.

    Returns:
        Configured execute_code tool function
    """

    async def execute_code(
        code: str,
        description: str | None = None,
        *,
        config: RunnableConfig,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> tuple[str, dict[str, Any]]:
        call = CommandCall("ExecuteCode", code, description, config, tool_call_id, session)
        return await _guarded(code, lambda call_id: _run(call, call_id))

    async def subagent_execute_code(
        code: str,
        description: str | None = None,
        *,
        config: RunnableConfig,
    ) -> tuple[str, dict[str, Any]]:
        call = CommandCall("ExecuteCode", code, description, config, None, session)
        return await _guarded(code, lambda call_id: _run(call, call_id))

    async def _guarded(code: str, run: Any) -> tuple[str, dict[str, Any]]:
        return await run_guarded(
            backend,
            code,
            call_context,
            run,
            memory_error=_MEMORY_ROUTE_ERROR,
            files_error=_FILES_ROUTE_ERROR,
            file_tools_only=file_tools_only,
            file_tools_error=_FILE_TOOLS_ONLY_ERROR,
            blocked_event="Blocked execute_code referencing a store-backed path",
            code_length=len(code),
        )

    def _slot() -> AbstractAsyncContextManager[Any]:
        # The machine's tier bounds how many runs go on it at once; read per
        # call because a long turn's session can be rebound after the tool
        # is built.
        return code_run_slot(
            getattr(session, "computer_id", None),
            tier=getattr(session, "resource_tier", None),
            resource_tiers=resource_tiers_of(session),
        )

    async def _run(call: CommandCall, call_id: str | None) -> tuple[str, dict[str, Any]]:
        # Every run, not only a failed one, and read here for the same reason
        # as the admission tier.
        stamp_resource_tier(resource_tier_of(session, backend))
        logger.info("Executing code in sandbox", code_length=len(call.command), thread_id=thread_id)

        async def launch(wait_s: float) -> JobOutcome:
            return await backend.start_code_job(
                call.command,
                wait_s=wait_s,
                thread_id=thread_id or None,
                call_id=call_id,
                allowed_servers=allowed_servers,
            )

        try:
            return await command_handoff.run_command(
                backend, call, launch, _result, hand_off=hand_off, slot=_slot
            )
        except Exception as e:
            return _raised(e)

    async def _result(outcome: Ended | Failed) -> tuple[str, dict[str, Any]]:
        """An ended run's output, as the call returns it."""
        artifact = {"mcp_trace": list(outcome.trace)}
        output = await command_handoff.settle(backend, outcome, hand_off)
        if isinstance(outcome, Ended) and outcome.exit_code == 0:
            logger.info("Code executed successfully", output_length=len(output))
            return ("SUCCESS\n" + output) if output else "SUCCESS", artifact
        logger.warning(
            "Code execution failed",
            command_id=outcome.ref.command_id,
            exit_code=outcome.exit_code if isinstance(outcome, Ended) else None,
            failure=outcome.failure if isinstance(outcome, Failed) else None,
            output_length=len(output),
        )
        if isinstance(outcome, Failed) and outcome.reason in ("not_launched", "lost"):
            # Nothing ran, or nothing says how it ended, so the reason is all
            # there is to say.
            return f"ERROR: {output}", artifact
        return f"ERROR\n{output}", artifact

    def _raised(e: Exception) -> tuple[str, dict[str, Any]]:
        logger.error("Code execution exception", error=str(e), exc_info=True)
        # The provider's own class, which a job exec's failure wraps.
        mark_tool_failed(e.cause if isinstance(e, JobExecFailed) else e)
        return f"ERROR: {e!s}", {"mcp_trace": []}

    tool = StructuredTool.from_function(
        coroutine=execute_code if hand_off is not None else subagent_execute_code,
        name="ExecuteCode",
        description=code_description(hand_off),
        response_format="content_and_artifact",
    )
    # Every run, not only a failed one, so an error rate reads per provider.
    tool.metadata = {
        TOOL_PROVIDER: backend.provider,
        TOOL_CHANNEL: backend.channel,
    }
    return tool
