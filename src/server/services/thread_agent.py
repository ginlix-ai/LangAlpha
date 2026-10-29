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

if TYPE_CHECKING:
    from ptc_agent.agent.roles import AgentRole
    from ptc_agent.config.agent import AgentConfig
    from ptc_agent.core.project_context import ProjectContext
    from src.server.services.computer_manager._types import WorkspaceToolView
    from src.server.services.egress.direct_tools import DirectMCPBinding
    from src.server.services.turn_runtime import TurnRoute


def thread_resolver(
    user_id: str,
    thread_id: str,
    route: TurnRoute,
    *,
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

    user_id: str | None
    thread_id: str
    workspace_id: str
    role: AgentRole
    #: The workspace folder the turn runs in; the run binds it too.
    project: ProjectContext | None
    #: The project's frozen registry and summary: the session's own fields
    #: belong to whichever sibling on the machine resolved last.
    tool_view: WorkspaceToolView
    #: The profile the prompt states, naming the trading rule the tools bring.
    user_profile: dict[str, Any] | None
    #: The directly bound tools, unopened: a run opens them (``drive``).
    direct_mcp: DirectMCPBinding

    @classmethod
    async def read(
        cls,
        session: Any,
        *,
        user_id: str | None,
        workspace_id: str,
        thread_id: str,
        role: AgentRole,
        user_profile: dict[str, Any] | None,
    ) -> AgentInputs:
        from src.server.services.egress.direct_tools import direct_tools_for_turn
        from src.server.services.trading_rule import with_trading_rule
        from src.server.services.workspace_layout import resolve_project_context
        from src.server.services.workspace_manager import WorkspaceManager

        tool_view = WorkspaceManager.get_instance().tool_view(session, workspace_id)
        return cls(
            user_id=user_id,
            thread_id=thread_id,
            workspace_id=workspace_id,
            role=role,
            project=await resolve_project_context(session, workspace_id),
            tool_view=tool_view,
            # Off the resolve, not the bind, so it moves only when a setting does.
            user_profile=with_trading_rule(user_profile, tool_view.trading_rule),
            direct_mcp=await direct_tools_for_turn(
                session, user_id=user_id, workspace_id=workspace_id, view=tool_view
            ),
        )

    def build_kwargs(self) -> dict[str, Any]:
        """The arguments ``build_ptc_graph_with_session`` takes from these."""
        from src.server.app import setup
        from src.server.services.preview_links import cache_tool_preview

        return {
            "user_id": self.user_id,
            "user_profile": self.user_profile,
            "thread_id": self.thread_id,
            "store": setup.store,
            "on_signed_url": partial(cache_tool_preview, workspace_id=self.workspace_id),
            "direct_mcp": self.direct_mcp,
            "project": self.project,
            "tool_view": self.tool_view,
            "role": self.role,
        }
