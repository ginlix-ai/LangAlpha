"""The subagent half of a PTC build: the roster's compiler and the Task middleware.

Two steps, because the main agent's stack sits between them: the background
middleware is built with the compiled roster's names, and the Task middleware
takes a snapshot of the main tools that middleware adds to. The compiler
outlives the first, since a Task that names a model compiles its subagent
again on that model.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.context_stack import SubagentContext
from ptc_agent.agent.middleware import SubAgentMiddleware
from ptc_agent.agent.middleware.background_subagent.subagent import ClaudeCodeSubagent
from ptc_agent.agent.subagents import SubagentCompiler
from ptc_agent.agent.tools import think_tool

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.runtime_context import (
        BaselineSources,
        BlockReader,
        TurnContext,
    )
    from ptc_agent.agent.middleware.skills.registry import SkillDefinition
    from ptc_agent.config.agent import Admitted, AgentConfig, TaskModel
    from ptc_agent.core.project_context import ProjectContext


def subagent_compiler(
    config: AgentConfig,
    *,
    sandbox: Any,
    mcp_registry: Any,
    execute_code: Any,
    bash: Any,
    filesystem: list[Any],
    web: list[Any],
    finance: list[Any],
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
    model: str = "",
    prompt_vars: Mapping[str, bool] | None = None,
    shared: bool = False,
) -> SubagentCompiler:
    """What compiles the turn's subagents into the specs ``SubAgentMiddleware``
    takes (``create_subagents``), and one of them again on a model a Task
    names (``SubagentCompiler.compile``).

    The tools are the main agent's own instances, which a subagent shares
    unless its settings call for a copy (``search_tool``, ``sandbox_tools``).
    ``sources``, ``skills`` and ``scratchpad`` are the main agent's, which
    each subagent's baseline narrows to what its definition reaches.
    ``skill_registry`` is the build's, less the skills no subagent gets.
    ``model`` is the turn's model label, stated by a subagent that runs it.
    ``prompt_vars`` are the main agent's (``IdentityGates.prompt_vars``), so
    a subagent's prompt points at no more of the person's own than the main
    agent's does. ``shared`` is a shared workspace's build, whose baselines
    name nobody (``SubagentContext``).
    """
    tool_sets: dict[str, list[Any]] = {
        "execute_code": [execute_code],
        "bash": [bash],
        "filesystem": list(filesystem),
        "web_search": web,
        "finance": finance,
        "think": [think_tool],
        # Binds nothing: the plan is the main agent's, and the name stays so
        # an operator's subagent that still lists it is not an unknown set.
        "todo": [],
    }
    return SubagentCompiler(
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
        prompt_vars=prompt_vars,
        context=SubagentContext.build(
            turn_context=turn_context,
            user_profile=user_profile,
            user_data_counts=user_data_counts,
            sources=sources,
            skills=skills,
            scratchpad=scratchpad,
            model=model,
            shared=shared,
        ),
    )


def task_middleware(
    subagents: list[dict[str, Any]],
    *,
    model: Any,
    tools: list[Any],
    middleware: list[Any],
    registry: Any,
    checkpointer: Any | None,
    claude_code: ClaudeCodeSubagent | None = None,
    compile_on: Callable[[str, TaskModel], Any] | None = None,
    claude_code_on: Callable[[Admitted], ClaudeCodeSubagent] | None = None,
) -> SubAgentMiddleware:
    """The Task tool's middleware, which compiles one graph per spec, a
    spec on a Claude Code row through ``claude_code``.

    ``tools`` is copied, so a tool the main agent binds afterwards never
    reaches a subagent. ``compile_on`` compiles a type's spec on a model a
    Task names, and ``claude_code_on`` wraps it where that model is a Claude
    Code row.
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
        claude_code=claude_code,
        compile_on=compile_on,
        claude_code_on=claude_code_on,
    )
