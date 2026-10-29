"""API session lifecycle functions for the CLI."""

from collections.abc import Callable

import structlog

from ptc_cli.agent.persistence import (
    delete_persisted_session,
    load_persisted_session,
    save_persisted_session,
    update_session_last_used,
)
from ptc_cli.api.client import SSEStreamClient, WorkspaceNameTakenError

logger = structlog.get_logger(__name__)

# How many numbered spellings of the agent's name to try before giving up.
_NEW_NAME_ATTEMPTS = 100
# The server refuses a longer workspace name.
_NAME_MAX_CHARS = 80
# The server cuts a name's folder to this many UTF-8 bytes, and the folder is
# what two names may not share: a number past the cut keys as the bare name.
_FOLDER_MAX_BYTES = 255


def _numbered_name(name: str, attempt: int) -> str:
    """The name to try on ``attempt``, trimmed so its number still fits, as the server trims."""
    suffix = "" if attempt == 0 else f" ({attempt + 1})"
    base = name[: _NAME_MAX_CHARS - len(suffix)].rstrip()
    while suffix and base and len(f"{base}{suffix}".encode("utf-8")) > _FOLDER_MAX_BYTES:
        base = base[:-1].rstrip()
    return f"{base}{suffix}"


async def _create_named_workspace(
    client: SSEStreamClient, name: str, *, reuse_existing: bool
) -> tuple[str, bool]:
    """Create a workspace called ``name``; returns its id and whether it was reused.

    Names are unique per user. A persisted session takes the workspace already
    holding the name, since that is the agent's own from an earlier run; a new
    workspace was asked for otherwise, so the name is numbered on (``name (2)``,
    ``name (3)``) the way the server numbers a name that is taken.
    """
    for attempt in range(_NEW_NAME_ATTEMPTS):
        candidate = _numbered_name(name, attempt)
        try:
            workspace = await client.create_workspace(name=candidate)
        except WorkspaceNameTakenError as e:
            if reuse_existing and e.workspace_id:
                return e.workspace_id, True
            continue
        return workspace["workspace_id"], False
    raise WorkspaceNameTakenError(
        f'Every name from "{name}" to "{name} ({_NEW_NAME_ATTEMPTS})" is taken.', None
    )


async def create_api_session(
    agent_name: str,
    server_url: str = "http://localhost:8000",
    workspace_id: str | None = None,
    *,
    persist_session: bool = True,
    on_progress: Callable[[str], None] | None = None,
) -> tuple[SSEStreamClient, str, bool]:
    """Create API session with workspace.

    Args:
        agent_name: Agent identifier for session storage
        server_url: PTC Agent server URL
        workspace_id: Optional existing workspace ID to reuse (overrides persistence)
        persist_session: Whether to persist/reuse workspace sessions
        on_progress: Optional callback for progress updates

    Returns:
        Tuple of (client, workspace_id, reusing_workspace)
    """
    def report(step: str) -> None:
        if on_progress:
            on_progress(step)

    client = SSEStreamClient(base_url=server_url, user_id=agent_name)
    reusing_workspace = False

    try:
        if workspace_id:
            # Explicit workspace_id provided via CLI - use it directly
            report("Connecting to workspace...")
            workspace = await client.get_workspace(workspace_id)
            if workspace:
                if workspace.get("status") == "stopped":
                    report("Starting workspace...")
                    await client.start_workspace(workspace_id)
                reusing_workspace = True
                client.workspace_id = workspace_id
            else:
                raise ValueError(f"Workspace {workspace_id} not found")

        elif persist_session:
            # Check for persisted session
            persisted = load_persisted_session(agent_name)
            if persisted and persisted.get("workspace_id"):
                persisted_workspace_id = persisted["workspace_id"]
                report("Reconnecting to workspace...")
                try:
                    workspace = await client.get_workspace(persisted_workspace_id)
                    if workspace:
                        if workspace.get("status") == "stopped":
                            report("Starting workspace...")
                            await client.start_workspace(persisted_workspace_id)
                        workspace_id = persisted_workspace_id
                        reusing_workspace = True
                        client.workspace_id = workspace_id
                        update_session_last_used(agent_name)
                    else:
                        # Workspace not found, delete persisted session
                        delete_persisted_session(agent_name)
                except Exception:  # noqa: BLE001
                    # Reconnection failed, delete persisted session
                    report("Workspace reconnection failed...")
                    delete_persisted_session(agent_name)

        # Create new workspace if needed
        if not workspace_id:
            report("Creating workspace...")
            workspace_id, reusing_workspace = await _create_named_workspace(
                client, f"cli-{agent_name}", reuse_existing=persist_session
            )
            client.workspace_id = workspace_id

            if persist_session:
                save_persisted_session(agent_name, workspace_id)

        return client, workspace_id, reusing_workspace

    except Exception:
        # Clean up client on error
        await client.close()
        raise
