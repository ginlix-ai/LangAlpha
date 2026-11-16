"""What Bash and ExecuteCode share around a command: the store-backed path
guard, and the call id its mount saves and paid relay calls are filed under."""

import re
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.provenance.trace import RELAY_CALL_KEY
from ptc_agent.core.sandbox import livefs_mount

logger = structlog.get_logger(__name__)


async def run_guarded(
    backend: SandboxBackend,
    text: str,
    call_context: livefs_mount.CallContext | None,
    run: Callable[[str | None], Awaitable[tuple[str, dict[str, Any]]]],
    *,
    memory_error: str,
    files_error: str,
    blocked_event: str,
    file_tools_only: tuple[str, ...] = (),
    file_tools_error: str = "",
    **blocked_fields: Any,
) -> tuple[str, dict[str, Any]]:
    """Refuse ``text`` if it names a directory only the file tools reach,
    LangAlpha's config folder by the path of a custom config home, or a
    store-backed tree no mount serves, else run it through the mount.

    Every check reads the command's text, so a path built at runtime passes
    them. The first and last spare the agent a command that would find
    nothing: neither tree is on the sandbox's disk. The config folder holds
    the machine's Claude Code tokens, which the command could read as the
    same login, so its check is best effort against the agent only.
    """
    only = next((d for d in file_tools_only if d in text), None)
    if only is not None:
        logger.info(blocked_event, tree=only, **blocked_fields)
        return file_tools_error.format(dir=only), {"mcp_trace": []}
    folder = await backend.config_folder()
    if folder is not None and re.search(rf"{re.escape(folder)}(?![\w.-])", text, re.IGNORECASE):
        # Here, not at the top: the middleware package imports the tools.
        from ptc_agent.agent.middleware.tool.protected_path import BLOCKED_RESPONSE

        logger.warning("Protected path access blocked", **blocked_fields)
        return BLOCKED_RESPONSE, {"mcp_trace": []}
    mount = await backend.settled_livefs(
        call_context.workspace_id if call_context is not None else None
    )
    tree = livefs_mount.unserved_tree(mount, text)
    if tree is not None:
        logger.info(blocked_event, tree=tree, **blocked_fields)
        error = memory_error if tree == "memory" else files_error.format(dir=tree)
        return error, {"mcp_trace": []}

    async def run_as(call_id: str | None) -> tuple[str, dict[str, Any]]:
        text, artifact = await run(call_id)
        return await _with_paid_calls(backend, call_id, text, artifact)

    return await livefs_mount.through_mount(
        mount, backend.sandbox.call_filer, call_context, run_as
    )


async def _with_paid_calls(
    backend: SandboxBackend, call_id: str | None, text: str, artifact: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Add what the command's paid relay calls cost to its output, and leave
    its call id on the artifact, where provenance collects the sources the
    relay held under it (``ProvenanceMiddleware``, which strips it)."""
    if call_id is None:
        return text, artifact
    if isinstance(artifact, dict):
        artifact[RELAY_CALL_KEY] = call_id
    paid_calls = getattr(backend.sandbox, "paid_calls", None)
    charged = await paid_calls(call_id) if paid_calls is not None else ""
    return (f"{text}\n\n{charged}" if charged else text), artifact
