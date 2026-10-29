"""PTC Graph Factory — builds per-conversation agents with dependency-injected session management."""

import asyncio
import logging
from typing import Any, Protocol, runtime_checkable

from ptc_agent.agent.agent import AgentRole, PTCAgent
from ptc_agent.agent.middleware.runtime_context import TurnContext
from ptc_agent.agent.middleware.runtime_context.epoch import Workspace
from ptc_agent.agent.middleware.compaction.window import WindowCoverage
from ptc_agent.agent.middleware.subagent_switch import SubagentSwitchReader
from ptc_agent.agent.middleware.wakeups import CancelNotices
from ptc_agent.agent.tools.computer_reach import ComputerReach
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
    session: Session,
    home_id: str,
    thread_id: str | None,
    turn_context: TurnContext | None,
    role: AgentRole,
    touched: set[str] | None = None,
) -> tuple[dict[str, str | None], ComputerReach | None]:
    """The baseline blocks the role adds, keyed by kind, read before the build,
    and the reach of its tools.

    A kind left out is a block this build does not have, so the analyst's
    build reads nothing. A None value is a read that did not answer, which the
    baseline marks as a hole and asks again next turn. A Chief of Staff with
    no user has no activity to read, so its build leaves the block out rather
    than marking a hole that no later turn could fill. The user's computers
    are read once for both: ``<activity>`` tells the agent which computers to
    name, and the reach is None when there is no other, which leaves the
    ``computer`` argument off its tools.
    """
    if role != "chief_of_staff" or not user_id:
        return {}, None
    from src.server.database.computer import get_computer_roster
    from src.server.services.computer_reach import turn_computers
    from src.tools.secretary.activity import read_activity

    try:
        computers = await get_computer_roster(user_id)
    except Exception:  # noqa: BLE001 - the turn runs on its own computer
        logger.warning("Could not read the computers of %s", user_id, exc_info=True)
        return {"activity": None}, None
    activity = await read_activity(
        user_id,
        home_id=home_id,
        thread_id=thread_id,
        timezone=turn_context.tool_timezone if turn_context else "UTC",
        computers=computers,
    )
    reach = turn_computers(user_id, session.computer_id, session, computers, touched)
    return {"activity": activity}, reach


async def _read_workspace_naming(
    workspace_id: str, role: AgentRole = "analyst"
) -> Workspace | None:
    """The workspace's name, description and computer for the `<workspace>` block.

    Read once here rather than inside the model call, so the values are bound
    when the turn's agent is built and the model never sees the name change
    under it mid-answer. The baseline freezes them per epoch; a rename
    reaches the model as a `workspace_changed` row on the next turn. A read
    that fails answers None, not an empty name: the baseline must not file a
    row saying the workspace lost its name. The Chief of Staff's Home is named
    for its folder, not for the row it is stored in.
    """
    if not workspace_id:
        return None
    try:
        from src.server.database.workspace import get_workspace_naming

        row = await get_workspace_naming(workspace_id) or {}
    except Exception as e:
        logger.warning(f"Failed to read the name of workspace {workspace_id}: {e}")
        return None
    from src.tools.secretary.utils import computer_label, computer_machine

    computer = computer_label(row.get("computer_name"), row.get("computers") or 0) or ""
    # Whose machine it is matters with one computer too: it is where the work runs.
    machine = computer_machine(row.get("computer_kind"), row.get("provider_config"))
    root = (row.get("root_dir") or "") if machine else ""
    if role == "chief_of_staff":
        from src.server.database.workspace_names import HOME_FOLDER

        return Workspace(
            name=HOME_FOLDER,
            description=_HOME_DESCRIPTION,
            computer=computer,
            machine=machine,
            root=root,
        )
    return Workspace(
        name=(row.get("name") or "").strip(),
        description=(row.get("description") or "").strip(),
        computer=computer,
        machine=machine,
        root=root,
    )


async def build_ptc_graph(
    conversation_id: str,
    config: AgentConfig,
    session_provider: SessionProvider,
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

    ptc_agent, user_data_counts, workspace = await asyncio.gather(
        asyncio.to_thread(PTCAgent, config),
        fetch_user_data_counts(user_id),
        _read_workspace_naming(conversation_id),
    )

    inner_agent = ptc_agent.create_agent(
        sandbox=session.sandbox,
        mcp_registry=session.mcp_registry,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        # session gives workspace-tier memory a real namespace.
        session=session,
        workspace=workspace,
        store=store,
        on_signed_url=on_signed_url,
        user_id=user_id,
        user_data_counts=user_data_counts,
        tool_summary=getattr(session, "mcp_tool_summary", None),
    )

    logger.debug(
        f"Created PTC agent for {conversation_id} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    return inner_agent


async def build_ptc_graph_with_session(
    session: Session,
    config: AgentConfig,
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
    window_coverage: WindowCoverage | None = None,
    cancel_notices: CancelNotices | None = None,
    reached_computers: set[str] | None = None,
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
    build without one leaves subagents as built. ``window_coverage`` lets the
    main agent trim its checkpoint to its window (see ``compaction.window``);
    a build without one never trims. ``cancel_notices`` tells the agent of
    the user's cancels of its wakeups; a build without one tells it of none.
    ``reached_computers`` collects the user's other computers a Chief of Staff
    turn ran calls on, for the turn's end to back up.
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
        workspace,
        (harness_blocks, computers),
    ) = await asyncio.gather(
        fetch_user_data_counts(user_id),
        asyncio.to_thread(PTCAgent, config),
        _read_workspace_naming(workspace_id, role),
        _read_harness_blocks(
            user_id, session, workspace_id, thread_id, turn_context, role,
            reached_computers,
        ),
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
        disable_subagents=disable_subagents,
        operation_callback=operation_callback,
        checkpointer=checkpointer,
        background_registry=background_registry,
        namespace_owner=namespace_owner,
        user_profile=user_profile,
        session=session,
        thread_id=thread_id,
        workspace=workspace,
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
        window_coverage=window_coverage,
        cancel_notices=cancel_notices,
        computers=computers,
    )

    logger.debug(
        f"Created PTC agent for workspace {workspace_id} "
        f"(checkpointer={'enabled' if checkpointer else 'disabled'})"
    )

    return inner_agent
