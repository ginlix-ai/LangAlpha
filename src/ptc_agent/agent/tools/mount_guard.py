"""What Bash and ExecuteCode share around a command: the store-backed path
guard, and the call id its mount saves and paid relay calls are filed under."""

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import structlog

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.provenance.trace import RELAY_CALL_KEY
from ptc_agent.core.sandbox import drive_mount, livefs_mount

logger = structlog.get_logger(__name__)

#: How long a command naming the Drive waits for a start under way.
DRIVE_WAIT_S = 8.0



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
    file_tools_errors: Mapping[str, str] | None = None,
    config: Mapping[str, Any] | None = None,
    **blocked_fields: Any,
) -> tuple[str, dict[str, Any]]:
    """Refuse ``text`` if it names a directory only the file tools reach,
    LangAlpha's config folder by the path of a custom config home, or a
    store-backed tree no mount serves, else run it through the mount.
    ``file_tools_errors`` holds the refusal for a directory that has its own,
    ``file_tools_error`` the one for every other.

    Every check reads the command's text, so a path built at runtime passes
    them. The first and last spare the agent a command that would find
    nothing: neither tree is on the sandbox's disk. The config folder holds
    the machine's Claude Code tokens, which the command could read as the
    same login, so its check is best effort against the agent only.
    """
    # At a path boundary, so a sibling such as ``members-old`` runs.
    only = next((d for d in file_tools_only if re.search(rf"{re.escape(d)}(?![\w.-])", text)), None)
    if only is not None:
        logger.info(blocked_event, tree=only, **blocked_fields)
        error = (file_tools_errors or {}).get(only, file_tools_error)
        return error.format(dir=only), {"mcp_trace": []}
    folder = await backend.config_folder()
    if folder is not None and re.search(rf"{re.escape(folder)}(?![\w.-])", text, re.IGNORECASE):
        # Here, not at the top: the middleware package imports the tools.
        from ptc_agent.agent.middleware.tool.protected_path import BLOCKED_RESPONSE

        logger.warning("Protected path access blocked", **blocked_fields)
        return BLOCKED_RESPONSE, {"mcp_trace": []}
    if call_context is not None:
        call_context = call_context.for_call(config)
    mount = await backend.settled_livefs(
        call_context.workspace_id if call_context is not None else None
    )
    tree = livefs_mount.unserved_tree(mount, text)
    if tree is not None:
        logger.info(blocked_event, tree=tree, **blocked_fields)
        error = memory_error if tree == "memory" else files_error.format(dir=tree)
        return error, {"mcp_trace": []}
    sandbox = backend.sandbox
    drive = getattr(sandbox, "drive", None) or drive_mount.NO_DRIVE
    # A sandbox the host handed no view of the Drive (a machine outside the
    # product's acquire) keeps ``.agents/drive`` as a folder like any other.
    if drive is not drive_mount.NO_DRIVE and drive_mount.names_drive(text):
        # As for the file mount unserved: a command writing there now would
        # make plain folders the Drive never sees and a backup skips.
        refusal = drive_mount.drive_refusal(await drive.on_disk(wait_s=DRIVE_WAIT_S))
        if refusal is not None:
            logger.info(blocked_event, tree="drive", **blocked_fields)
            return refusal, {"mcp_trace": []}

    async def run_tagged(call_id: str | None) -> tuple[str, dict[str, Any]]:
        output, artifact = await run(call_id)
        # Where provenance collects the sources the relay held under the call
        # (``ProvenanceMiddleware``, which strips it).
        if call_id is not None and isinstance(artifact, dict):
            artifact[RELAY_CALL_KEY] = call_id
        return output, artifact

    return await livefs_mount.run_call(
        run_tagged,
        command=text,
        mount=mount,
        drive=drive,
        filer=sandbox.call_filer,
        context=call_context,
        paid_calls=getattr(sandbox, "paid_calls", None),
    )
