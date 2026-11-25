"""A thread's agent, resolved and built as its turns resolve and build it.

A model call made between turns (manual /compact, the hand-off at a model
switch) is checked by the provider against the turn's own request: the
prompt, the tools and the thinking it was sent with, which its cache and the
thinking in the kept turns are verified against. So a turn and a call between
turns read those inputs here, and an input added for one reaches the other.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from src.server.services.shared_workspaces.turn import TurnIdentity

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.launcher import Launcher
    from ptc_agent.agent.middleware.member_context import MemberContext
    from ptc_agent.agent.roles import AgentRole
    from ptc_agent.agent.transcript import BorrowedFiles
    from ptc_agent.config.agent import Admitted, AgentConfig
    from ptc_agent.core.claude_code_clis import CliQueue
    from ptc_agent.core.project_context import ProjectContext
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
    from src.server.services.computer_manager._types import WorkspaceToolView
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from src.server.services.forks.model import ForkLineage
    from src.server.services.livefs.history_files import ThreadHistory
    from src.server.services.turn_runtime import TurnRoute


def thread_resolver(
    user_id: str,
    thread_id: str,
    route: TurnRoute,
    *,
    identity: TurnIdentity,
    thread: Mapping[str, Any] | None,
    is_byok: bool,
    reasoning_effort: str | None = None,
    fast_mode: bool | None = None,
    enabled_subagents: list[str] | None = None,
) -> Callable[[str | None], Awaitable[AgentConfig]]:
    """Maps a model name (None for the account default) to the config a turn
    on the thread runs it under, in the route's workspace and role.

    A turn naming no effort or fast mode runs the thread's own (``thread``,
    its row), picked in its composer, and with none of those the user's
    Settings. A client sends what its composer shows, so the row decides only
    for a turn no composer started: a report-back, a resume after a reload, a
    call between turns.
    """
    from src.server.app import setup
    from src.server.services.llm.config import resolve_llm_config

    row = thread or {}
    effort = reasoning_effort or row.get("reasoning_effort")
    fast = fast_mode if fast_mode is not None else row.get("fast_mode")

    async def resolve(model: str | None) -> AgentConfig:
        return await resolve_llm_config(
            setup.agent_config,
            user_id,
            model,
            is_byok,
            reasoning_effort=effort,
            fast_mode=fast,
            thread_id=thread_id,
            enabled_subagents=enabled_subagents,
            workspace_id=route.workspace_id,
            identity=identity,
            role=route.role,
        )

    return resolve


async def thread_route(thread_id: str, user_id: str) -> tuple[dict[str, Any], TurnRoute]:
    """The thread's row and where its next turn runs, decided as a send on
    the thread decides it."""
    from src.server.database.conversation import get_thread_auth_meta
    from src.server.database.workspace import get_workspace
    from src.server.services.turn_runtime import turn_route

    row = await get_thread_auth_meta(thread_id)
    if not row or not row.get("workspace_id"):
        raise HTTPException(status_code=404, detail=f"Thread not found: {thread_id}")
    workspace = await get_workspace(str(row["workspace_id"]))
    return row, turn_route(user_id, workspace)


@dataclass(frozen=True)
class AgentInputs:
    """What the agent is built from that its prompt and tools depend on, read
    as a turn reads it. A run adds what only it has (its saver, its guard,
    its turn row, the order ledger) beside ``build_kwargs``."""

    #: The sender, and the workspace's owner: whose connectors are bound,
    #: whose vault is redacted, and whose memory and files the agent mounts.
    identity: TurnIdentity
    thread_id: str
    workspace_id: str
    role: AgentRole
    #: The workspace folder the turn runs in; the run binds it too.
    project: ProjectContext | None
    #: The project's frozen registry and summary: the session's own fields
    #: belong to whichever sibling on the machine resolved last.
    tool_view: WorkspaceToolView
    #: The turn's profile (``TurnIdentity.profile``), naming the trading
    #: rule the tools bring.
    user_profile: dict[str, Any] | None
    #: The directly bound tools, unopened: a run opens them (``drive``).
    direct_mcp: DirectMCPBinding
    #: The computer the session runs on, where a Claude Code turn's CLI may.
    computer_id: str | None
    #: The thread's own transcript, for the file tools of a turn in a thread
    #: its sender keeps private in a shared workspace (``ThreadHistory``).
    thread_history: ThreadHistory | None = None
    #: A snapshot fork's turns before its own, whose files its transcript
    #: pointers name in the chats it was forked from (``ThreadStart``).
    transcript_borrowed: tuple[BorrowedFiles, ...] = ()
    #: In a shared workspace, the member whose own context the turn carries
    #: (``shared_workspaces_member_context``); None for a call between
    #: turns and for every other turn.
    member_context: MemberContext | None = None

    @classmethod
    async def read(
        cls,
        session: Any,
        *,
        identity: TurnIdentity,
        workspace_id: str,
        thread_id: str,
        role: AgentRole,
        user_profile: dict[str, Any] | None,
        acting_for: str | None = None,
        member_context: MemberContext | None = None,
        fork: ForkLineage | None = None,
    ) -> AgentInputs:
        """``workspace_id`` is the one whose folder the turn runs in;
        ``acting_for`` the thread's, when that is another (Home's turn in its
        stand-in). ``fork`` is the thread's lineage, which the caller read
        with its row."""
        from dataclasses import replace

        from src.server.services.egress.direct_tools import direct_tools_for_turn
        from src.server.services.forks.start import ThreadStart
        from src.server.services.livefs.history_files import private_thread_history
        from src.server.services.trading_rule import with_trading_rule
        from src.server.services.workspace_layout import resolve_project_context
        from src.server.services.workspace_manager import WorkspaceManager

        tool_view = WorkspaceManager.get_instance().tool_view(session, workspace_id)
        project = await resolve_project_context(session, workspace_id)
        if project is not None and acting_for and acting_for != workspace_id:
            project = replace(project, acting_for=acting_for)
        borrowed = ThreadStart.of(fork).borrowed
        return cls(
            identity=identity,
            thread_id=thread_id,
            workspace_id=workspace_id,
            role=role,
            project=project,
            tool_view=tool_view,
            # Off the resolve, not the bind, so it moves only when a setting does.
            user_profile=with_trading_rule(user_profile, tool_view.trading_rule),
            # The connectors are the workspace owner's, as on the computer.
            direct_mcp=await direct_tools_for_turn(
                session, user_id=identity.owner, workspace_id=workspace_id, view=tool_view
            ),
            computer_id=session.computer_id,
            thread_history=(
                await private_thread_history(
                    workspace_id,
                    project.dir_name if project is not None else None,
                    thread_id,
                    identity.sender,
                    tuple(files.thread_id for files in borrowed),
                )
                if identity.shared and thread_id
                else None
            ),
            transcript_borrowed=borrowed,
            member_context=member_context,
        )

    def build_kwargs(self) -> dict[str, Any]:
        """The arguments ``build_ptc_graph_with_session`` takes from these."""
        from src.server.app import setup
        from src.server.services.preview_links import cache_tool_preview

        return {
            "user_id": self.identity.sender,
            "owner_id": self.identity.owner,
            "own_data": not self.identity.shared,
            "user_profile": self.user_profile,
            "thread_id": self.thread_id,
            "store": setup.store,
            "on_signed_url": partial(cache_tool_preview, workspace_id=self.workspace_id),
            "direct_mcp": self.direct_mcp,
            "project": self.project,
            "tool_view": self.tool_view,
            "role": self.role,
            "claude_code_launcher": self.claude_code_launcher,
            "thread_history": self.thread_history,
            "transcript_borrowed": self.transcript_borrowed,
            "member_context": self.member_context,
        }

    def claude_code_launcher(
        self, sandbox: PTCSandbox, admitted: Admitted, queue: CliQueue | None = None, *,
        slot_wait_s: float | None = None,
    ) -> Launcher:
        """Where a Claude Code CLI runs, a turn's or a subagent's, and the token
        it signs in with, by what admission let it run on (``admitted``).
        ``slot_wait_s`` is how long a CLI on an own computer waits for a slot
        there, a subagent's shorter than a turn's, and ``queue`` is told while
        it waits. Imported here, never at the top: the SDK loads only for such a CLI."""
        from src.server.services.claude_code.launchers import turn_launcher

        return turn_launcher(
            sandbox, computer_id=self.computer_id, workspace_id=self.workspace_id, user_id=self.identity.sender,
            admitted=admitted, slot_wait_s=slot_wait_s, queue=queue,
        )
