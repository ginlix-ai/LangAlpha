"""PTC Graph Factory — builds per-conversation agents with dependency-injected session management."""

import asyncio
import logging
from typing import Any, Protocol, runtime_checkable

from ptc_agent.agent.agent import AgentRole, PTCAgent
from ptc_agent.agent.middleware.runtime_context import TurnContext
from ptc_agent.agent.middleware.subagent_switch import SubagentSwitchReader
from ptc_agent.config import AgentConfig
from ptc_agent.core.project_context import ProjectContext
from ptc_agent.core.session import Session

logger = logging.getLogger(__name__)


async def fetch_user_data_counts(user_id: str | None) -> dict[str, Any] | None:
    """Lightweight counts for the static `<user_profile>` block, plus the
    watchlist symbols the preferred-market vote reads.

    Four indexed queries in parallel, read per turn rather than through the
    cached profile: a watchlist edit invalidates no cache, and the market it
    implies has to follow the edit. Failure is non-fatal: returns None and
    the awareness block omits the counts line.
    """
    if not user_id:
        return None
    try:
        from src.server.services import user_data_io as io
        portfolio_count, watchlist_counts, prefs_set, symbols = await asyncio.gather(
            io.count_portfolio_for_user(user_id),
            io.count_watchlist_for_user(user_id),
            io.exists_preferences_for_user(user_id),
            io.list_watchlist_symbols_for_user(user_id),
        )
        wl_count, item_count = watchlist_counts
        return {
            "portfolio_count": int(portfolio_count),
            "watchlist_summary": f"{wl_count}:{item_count}",
            "prefs_set": bool(prefs_set),
            "watchlist_symbols": list(symbols),
        }
    except Exception:
        logger.warning("user-data counts fetch failed; awareness block will omit counts", exc_info=True)
        return None


@runtime_checkable
class SessionProvider(Protocol):
    """Dependency-injection boundary for session management (server, CLI, tests)."""

    async def get_or_create_session(
        self, conversation_id: str, sandbox_id: str | None = None
    ) -> Session:
        ...


_HOME_DESCRIPTION = (
    "Your own folder: everything you produce is kept here, "
    "whether it spans workspaces or belongs to none."
)


async def _read_harness_blocks(
    user_id: str | None,
    home_id: str,
    thread_id: str | None,
    turn_context: TurnContext | None,
    role: AgentRole,
) -> dict[str, str | None]:
    """The baseline blocks the role adds, keyed by kind, read before the build.

    A kind left out is a block this build does not have, so the analyst's
    build reads nothing. A None value is a read that did not answer, which the
    baseline marks as a hole and asks again next turn. A Chief of Staff with
    no user has no activity to read, so its build leaves the block out rather
    than marking a hole that no later turn could fill.
    """
    if role != "chief_of_staff" or not user_id:
        return {}
    from src.tools.secretary.activity import read_activity

    activity = await read_activity(
        user_id,
        home_id=home_id,
        thread_id=thread_id,
        timezone=turn_context.tool_timezone if turn_context else "UTC",
    )
    return {"activity": activity}


async def _read_workspace_naming(
    workspace_id: str, role: AgentRole = "analyst"
) -> tuple[str | None, str | None]:
    """The workspace's name and description for the prompt's `<workspace>` block.

    Read once here rather than inside the model call, so the values are bound
    when the turn's agent is built and the model never sees the name change
    under it mid-answer. The baseline freezes the pair per epoch; a rename
    reaches the model as a `workspace_changed` row on the next turn. A read
    that fails answers None, not an empty name: the baseline must not file a
    row saying the workspace lost its name. The Chief of Staff's Home is named
    for its folder, not for the row it is stored in.
    """
    if not workspace_id:
        return None, None
    if role == "chief_of_staff":
        from src.server.database.workspace_names import HOME_FOLDER

        return HOME_FOLDER, _HOME_DESCRIPTION
    try:
        from src.server.database.workspace import get_workspace_name_and_description

        row = await get_workspace_name_and_description(workspace_id) or {}
        return (row.get("name") or "").strip(), (row.get("description") or "").strip()
    except Exception as e:
        logger.warning(f"Failed to read the name of workspace {workspace_id}: {e}")
        return None, None


async def build_ptc_graph(
    conversation_id: str,
    config: AgentConfig,
    session_provider: SessionProvider,
    subagent_names: list[str] | None = None,
    sandbox_id: str | None = None,
    operation_callback: Any | None = None,
    checkpointer: Any | None = None,
    background_registry: Any | None = None,
    store: Any | None = None,
    on_signed_url: Any | None = None,
    user_id: str | None = None,
) -> Any:
    """Build a BackgroundSubagentOrchestrator for ``conversation_id``, acquiring a session via ``session_provider``."""
    logger.debug(f"Building PTC graph for conversation: {conversation_id}")

    # Get session from provider
    session = await session_provider.get_or_create_session(
        conversation_id=conversation_id,
        sandbox_id=sandbox_id,
    )

    if not session.sandbox or not session.mcp_registry:
        raise RuntimeError(
            f"Failed to initialize session for conversation {conversation_id}"
        )

    ptc_agent, user_data_counts, (workspace_name, workspace_description) = await asyncio.gather(
        asyncio.to_thread(PTCAgent, config),
        fetch_user_data_counts(user_id),
        _read_workspace_naming(conversation_id),
    )

    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        subagent_names=subagent_names or config.subagents.enabled,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        # session gives workspace-tier memory a real namespace.
        session=session,
        workspace_name=workspace_name,
        workspace_description=workspace_description,
        store=store,
        on_signed_url=on_signed_url,
        user_id=user_id,
        user_data_counts=user_data_counts,
        tool_summary=getattr(session, "mcp_tool_summary", None),
    )

    logger.debug(
        f"Created PTC agent for {conversation_id} with "
        f"subagents: {subagent_names or config.subagents.enabled} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    return inner_agent


async def build_ptc_graph_with_session(
    session: Session,
    config: AgentConfig,
    subagent_names: list[str] | None = None,
    operation_callback: Any | None = None,
    checkpointer: Any | None = None,
    background_registry: Any | None = None,
    user_id: str | None = None,
    user_profile: dict[str, Any] | None = None,
    thread_id: str | None = None,
    store: Any | None = None,
    on_signed_url: Any | None = None,
    namespace_owner: Any | None = None,
    disable_subagents: bool = False,
    direct_mcp: Any | None = None,
    order_ledger: Any | None = None,
    turn_context: TurnContext | None = None,
    project: ProjectContext | None = None,
    tool_view: Any | None = None,
    role: AgentRole = "analyst",
    subagent_switch: SubagentSwitchReader | None = None,
) -> Any:
    """Build a BackgroundSubagentOrchestrator from a pre-acquired session (WorkspaceManager path).

    ``turn_context`` is what this turn knows about itself, for the turn anchor
    row. It is optional because this builder also serves context-free callers
    (thread maintenance) that have no turn. ``user_profile`` is the caller's
    read of the profile, the one its ``turn_context`` zone came from, so the
    identity block and the stamp never answer from two different reads.

    ``project`` is the workspace folder the turn runs in. The build happens
    before the run's task binds it, so it travels as an argument.

    ``tool_view`` is the project's frozen registry and summary. The session's
    own fields belong to whichever project on the machine resolved last.

    ``subagent_switch`` reads the thread's subagent switch on every call; a
    build without one leaves subagents as built.
    """
    mcp_registry = (
        tool_view.mcp_registry if tool_view is not None else session.mcp_registry
    )
    tool_summary = (
        tool_view.mcp_tool_summary
        if tool_view is not None
        else getattr(session, "mcp_tool_summary", None)
    )
    # From the project, never from the session: the session is cached per
    # computer and several workspaces share it, so its own label names
    # whichever workspace happened to acquire it first.
    workspace_id = project.workspace_id if project else ""
    logger.debug(f"Building PTC graph with session for workspace: {workspace_id}")

    if not session.sandbox or not mcp_registry:
        raise RuntimeError(
            f"Session for workspace {workspace_id} is not properly initialized"
        )

    (
        user_data_counts,
        ptc_agent,
        (workspace_name, workspace_description),
        harness_blocks,
    ) = await asyncio.gather(
        fetch_user_data_counts(user_id),
        asyncio.to_thread(PTCAgent, config),
        _read_workspace_naming(workspace_id, role),
        _read_harness_blocks(user_id, workspace_id, thread_id, turn_context, role),
    )

    if workspace_id and user_id:
        from src.server.database.user_vault_secrets import get_user_secrets_decrypted

        # Leak detection redacts the owner's whole vault, read fresh: every
        # workspace can read every secret, and the sandbox's cached copy is
        # process-local, so a rotation handled by another worker leaves it stale.
        vault_secrets = await get_user_secrets_decrypted(user_id)
    else:
        vault_secrets = dict(getattr(session.sandbox, "vault_secrets", None) or {})

    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=mcp_registry,
        subagent_names=subagent_names or config.subagents.enabled,
        disable_subagents=disable_subagents,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        namespace_owner=namespace_owner,
        user_profile=user_profile,
        session=session,
        thread_id=thread_id,
        workspace_name=workspace_name,
        workspace_description=workspace_description,
        on_agent_md_write=session.note_agent_md_write,
        store=store,
        on_signed_url=on_signed_url,
        vault_secrets=vault_secrets,
        user_id=user_id,
        user_data_counts=user_data_counts,
        # Session-cached tool summary (precomputed once per session) so the per
        # turn create_agent never recomputes it — keeps the prompt-cache prefix
        # byte-stable. None → create_agent computes from the registry.
        tool_summary=tool_summary,
        direct_mcp=direct_mcp,
        order_ledger=order_ledger,
        turn_context=turn_context,
        project=project,
        role=role,
        harness_blocks=harness_blocks,
        subagent_switch=subagent_switch,
    )

    logger.debug(
        f"Created PTC agent for workspace {workspace_id} with "
        f"subagents: {subagent_names or config.subagents.enabled} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    return inner_agent
