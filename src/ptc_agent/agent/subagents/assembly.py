"""The subagent half of a PTC build: the compiled roster and the Task middleware.

Two steps, because the main agent's stack sits between them: the background
middleware is built with the compiled roster's names, and the Task middleware
takes a snapshot of the main tools that middleware adds to.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.context_stack import SubagentContext
from ptc_agent.agent.middleware import SubAgentMiddleware
from ptc_agent.agent.subagents import SubagentCompiler, create_subagents
from ptc_agent.agent.subagents.merge import turn_roster
from ptc_agent.agent.tools import think_tool

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.runtime_context import (
        BaselineSources,
        BlockReader,
        TurnContext,
    )
    from ptc_agent.agent.middleware.skills.registry import SkillDefinition
    from ptc_agent.agent.roles import AgentRole
    from ptc_agent.config.agent import AgentConfig
    from ptc_agent.core.project_context import ProjectContext


def compile_subagents(
    config: AgentConfig,
    role: AgentRole,
    *,
    sandbox: Any,
    mcp_registry: Any,
    execute_code: Any,
    bash: Any,
    filesystem: list[Any],
    web: list[Any],
    finance: list[Any],
    todo: list[Any],
    search_tool: Callable[..., Any],
    sandbox_tools: Callable[[frozenset[str]], list[Any]],
    turn_context: TurnContext | None,
    user_profile: dict[str, Any] | None,
    user_data_counts: dict[str, Any] | None,
    current_time: str,
    thread_id: str,
    project: ProjectContext | None,
    sources: BaselineSources | None,
    skills: BlockReader,
    skill_registry: dict[str, SkillDefinition],
    scratchpad: str | None,
) -> list[dict[str, Any]]:
    """The turn's roster for ``role``, as the specs ``SubAgentMiddleware`` takes.

    The tools are the main agent's own instances, which a subagent shares
    unless its settings call for a copy (``search_tool``, ``sandbox_tools``).
    ``sources``, ``skills`` and ``scratchpad`` are the main agent's, which
    each subagent's baseline narrows to what its definition reaches.
    ``skill_registry`` is the build's, less the skills no subagent gets.
    """
    tool_sets: dict[str, list[Any]] = {
        "execute_code": [execute_code],
        "bash": [bash],
        "filesystem": list(filesystem),
        "web_search": web,
        "finance": finance,
        "think": [think_tool],
        "todo": todo,
    }
    compiler = SubagentCompiler(
        sandbox=sandbox,
        mcp_registry=mcp_registry,
        tool_sets=tool_sets,
        user_profile=user_profile,
        current_time=current_time,
        thread_id=thread_id,
        config=config,
        skill_registry=skill_registry,
        skill_dirs=config.skills.local_skill_dirs(),
        project=project,
        search_tool=search_tool,
        sandbox_tools=sandbox_tools,
        context=SubagentContext.build(
            turn_context=turn_context,
            user_profile=user_profile,
            user_data_counts=user_data_counts,
            sources=sources,
            skills=skills,
            scratchpad=scratchpad,
        ),
    )
    return create_subagents(turn_roster(config.subagents), compiler, role)


def task_middleware(
    subagents: list[dict[str, Any]],
    *,
    model: Any,
    tools: list[Any],
    middleware: list[Any],
    registry: Any,
    checkpointer: Any | None,
) -> SubAgentMiddleware:
    """The Task tool's middleware, which compiles one graph per spec.

    ``tools`` is copied, so a tool the main agent binds afterwards never
    reaches a subagent.
    """
    return SubAgentMiddleware(
        default_model=model,
        default_tools=list(tools),
        subagents=subagents,
        default_middleware=middleware,
        # The roster is the whole list: an implicit general-purpose
        # agent would outlive the user turning theirs off.
        general_purpose_agent=False,
        registry=registry,
        checkpointer=checkpointer,
    )
