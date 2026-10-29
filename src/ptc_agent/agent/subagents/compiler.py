"""SubagentCompiler — turns SubagentDefinitions into SubAgent TypedDicts.

The compiler handles:
  * Prompt rendering (custom_prompt > custom_prompt_template > base + role)
  * Tool-set resolution from string identifiers
  * Preloaded skill bodies
  * Section-toggle computation (defaults + definition overrides)
"""

from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.agent.middleware.runtime_context.profile import TRADING_KEYS
from ptc_agent.agent.middleware.skills.content import load_skill_content
from ptc_agent.agent.middleware.skills.registry import SkillDefinition
from ptc_agent.agent.prompts import (
    RegistryTools,
    format_registry_tools,
    get_loader,
    guidance_template_vars,
    read_registry_tools,
    resolve_prompt_guidance,
    workspace_path_vars,
)
from ptc_agent.agent.subagents.definition import SubagentDefinition

if TYPE_CHECKING:
    from ptc_agent.agent.context_stack import SubagentContext
    from ptc_agent.config.agent import AgentConfig
    from ptc_agent.core.project_context import ProjectContext

logger = structlog.get_logger(__name__)

# ── Section toggle defaults ───────────────────────────────────────────
# True = section included, False = excluded.
# The definition.sections dict overrides these per-subagent.

_SECTION_DEFAULTS: dict[str, bool] = {
    "task_workflow": False,
    "workspace_paths": True,
    "tool_guide": True,
    "subagent_coordination": False,
    "data_processing": True,
    "visualizations": True,
    "output_guidelines": False,
    "workspace_context": False,
    "ask_user_guidelines": False,
}

def _with_preloaded(prompt: str, preloaded: str) -> str:
    """A standalone prompt closed by the block ``subagent_base.md.j2`` ends
    with, so a skill the deployment or the user preloads reaches it too."""
    if not preloaded:
        return prompt
    return f"{prompt.rstrip()}\n\n<preloaded_skills>\n{preloaded}\n</preloaded_skills>\n"


class _StandIn(str):
    """A turn's value where none exists: set, as a turn's always is, but empty."""

    def __bool__(self) -> bool:
        return True


_STAND_IN = _StandIn()


def subagent_template_vars(
    defn: SubagentDefinition,
    *,
    guidance: str,
    current_time: str | None = _STAND_IN,
    thread_id: str = _STAND_IN,
    files_mounted: bool = True,
    market_watch: bool = True,
    todo: bool = True,
) -> dict[str, Any]:
    """What a subagent's templates read beside the loader's path table.

    A turn passes its own values. The settings pane renders a base's body
    with the defaults, which stand in for them at the same types and
    truthiness, so an operator's template that reads one (``thread_id[:8]``)
    or branches on it renders there as it does in a turn, less the thread's
    own text, which a copy could not use, and the two cannot come to pass
    different names.
    """
    return {
        "current_time": current_time,
        "thread_id": thread_id,
        **_tool_gates(defn, market_watch=market_watch, todo=todo),
        **guidance_template_vars(guidance),
        "files_mounted": files_mounted,
    }


def _tool_gates(defn: SubagentDefinition, *, market_watch: bool, todo: bool) -> dict[str, bool]:
    """Which tool tiers this subagent's prompt is allowed to advertise.

    The tool guide is shared, the tool sets are not: report-builder binds
    neither finance nor web, no subagent binds show_widget, and the yaml
    default for market_watch (true) would advertise a tool ``agent.py`` has
    already gated out of this user's finance set. The chart annotation
    tools are off unconditionally: ``agent.py`` binds them on the main
    agent only, so no tool set can carry them here.
    """
    bound = set(defn.tools)
    return {
        "market_watch_enabled": market_watch and "finance" in bound,
        "todo_enabled": "todo" in bound and todo,
        "show_widget_enabled": "show_widget" in bound,
        "chart_annotation_enabled": False,
        "finance_enabled": "finance" in bound,
        "web_enabled": "web_search" in bound,
    }


class SubagentCompiler:
    """Compile :class:`SubagentDefinition` instances into ``SubAgent`` TypedDicts.

    Instances are created once per ``PTCAgent.create_agent()`` call and hold
    the runtime context (sandbox, tools, time, etc.) shared across all subagents.
    """

    def __init__(
        self,
        *,
        sandbox: Any | None = None,
        mcp_registry: Any | None = None,
        tool_sets: dict[str, list[Any]] | None = None,
        user_profile: dict[str, Any] | None = None,
        current_time: str | None = None,
        thread_id: str = "",
        config: AgentConfig | None = None,
        skill_registry: dict[str, SkillDefinition] | None = None,
        skill_dirs: list[str] | None = None,
        project: ProjectContext | None = None,
        search_tool: Callable[..., Any] | None = None,
        sandbox_tools: Callable[[frozenset[str]], list[Any]] | None = None,
        context: SubagentContext | None = None,
    ) -> None:
        self._sandbox = sandbox
        # What each subagent's runtime-context baseline is built from. Without
        # it (bare and test construction) a subagent gets no baseline, and its
        # prompt says nothing that would point at one.
        self._context = context
        # A subagent runs in the parent turn's workspace folder, so its copy of
        # the path table has to name the same one.
        self._project = project
        self._mcp_registry = mcp_registry
        self._tool_sets: dict[str, list[Any]] = tool_sets or {}
        # A subagent is bound no order tools, so the main agent's trading
        # permission would describe calls it cannot make.
        self._user_profile = (
            {
                k: v
                for k, v in user_profile.items()
                if k not in TRADING_KEYS
            }
            if user_profile
            else user_profile
        )
        self._current_time = current_time
        self._thread_id = thread_id
        self._config = config
        # Per-build registry (feature gates + user disables + user skills),
        # authoritative when provided; None (bare/test construction) keeps the
        # global-registry behavior.
        self._skill_registry = skill_registry
        self._skill_dirs = skill_dirs
        # A subagent whose search or connector access differs from the main
        # agent's runs its own copy of those tools. The builders take the
        # setting (``search_tool(provider=, depth=)``, ``sandbox_tools(allowed)``);
        # without them every subagent shares the main agent's. Copies are
        # made once per distinct setting for this build.
        self._make_search_tool = search_tool
        self._make_sandbox_tools = sandbox_tools
        self._search_tools: dict[tuple[str, str], Any] = {}
        self._sandbox_tools: dict[frozenset[str], dict[str, Any]] = {}

    @property
    def _files_mounted(self) -> bool:
        return self._sandbox is not None and self._sandbox.livefs is not None

    # ── Public API ────────────────────────────────────────────────────

    def compile(self, definition: SubagentDefinition) -> dict[str, Any]:
        """Compile a single definition into a ``SubAgent`` TypedDict."""
        # A credentialed user's resolved client outranks the definition's
        # string model name.
        resolved = (
            self._config.client_for_role(
                f"subagent:{definition.name}", fallback_to_main=False
            )
            if self._config is not None
            else None
        )
        model = resolved if resolved is not None else definition.model
        sections = self._compute_sections(definition)
        roster = (
            self._build_tool_summary(definition)
            if sections.get("tool_guide", False)
            else None
        )
        if roster is None:
            # Nothing left to guide it to: the guide would point at an
            # <mcp-servers> element its baseline does not have.
            sections["tool_guide"] = False
        # The sections that follow from what the subagent can do and what its
        # baseline holds, rather than from a switch of their own.
        bound = set(definition.tools)
        runs_code = bool(bound & {"execute_code", "bash"})
        held = {"memory_system": False, "thread_scratchpad": False}
        baseline = None
        if self._context is not None:
            held, baseline = self._context.baseline(
                user_profile=self._user_profile,
                memory=definition.memory,
                mcp_servers=roster,
                # File tools reach the memory files whether or not the file
                # mount serves; code and shell reach them only through it.
                files="filesystem" in bound or (runs_code and self._files_mounted),
                writes_files=runs_code or "filesystem" in bound,
                guidance=self._guidance(definition),
            )
        sections.update(held, security_policy=runs_code)
        result: dict[str, Any] = {
            "name": definition.name,
            "description": definition.description,
            "system_prompt": self._resolve_prompt(definition, sections),
            "tools": self._resolve_tools(definition),
        }
        if model is not None:
            result["model"] = model
        # Appended after the default subagent middleware, so the baseline
        # follows the caching middlewares, as the main agent's does, and its
        # block lands after their system breakpoint.
        if baseline is not None:
            result["middleware"] = [baseline]

        return result

    # ── Prompt resolution ─────────────────────────────────────────────

    def _resolve_prompt(
        self, defn: SubagentDefinition, sections: dict[str, bool]
    ) -> str:
        """Resolve the system prompt based on priority.

        1. ``custom_prompt`` — raw string, used directly.
        2. ``custom_prompt_template`` — standalone template.
        3. Base template + role prompt (default).

        Each one ends with the preloaded skills.

        Nothing here is the user's or the thread's: the profile, the MCP
        roster, the skills and the scratchpad ride the baseline, so the
        prompt is one cached prefix for every user of a definition.
        """
        loader = get_loader()
        preloaded = self._load_preloaded_skills(defn)

        # 1. Raw custom prompt — bypass all rendering
        if defn.custom_prompt is not None:
            return _with_preloaded(defn.custom_prompt, preloaded)

        # Shared template variables
        template_kwargs = subagent_template_vars(
            defn,
            guidance=self._guidance(defn),
            current_time=self._current_time,
            thread_id=self._thread_id,
            files_mounted=self._files_mounted,
            market_watch=(
                self._config is not None and self._config.feature_enabled("market_watch")
            ),
            todo=bool(self._tool_sets.get("todo")),
        )
        # Resolve the turn's own folder so workspace_paths renders it
        if self._sandbox is not None and hasattr(self._sandbox, "config"):
            template_kwargs.update(
                workspace_path_vars(
                    self._sandbox.workspace(self._project),
                    root=self._sandbox.config.filesystem.working_directory,
                )
            )

        # 2. Standalone custom template — render it directly
        if defn.custom_prompt_template is not None:
            return _with_preloaded(
                loader.render(defn.custom_prompt_template, **template_kwargs), preloaded
            )

        # 3. Base template + role prompt (default path)
        return loader.get_subagent_base_prompt(
            identity_line=self._identity_line(defn),
            role_prompt_template=defn.role_prompt_template,
            role_prompt=defn.role_prompt,
            preloaded_skills_content=preloaded,
            sections=sections,
            tools_narrowed=defn.tools_narrowed,
            **template_kwargs,
        )

    def _guidance(self, defn: SubagentDefinition) -> str:
        """Scaffolding level for the model this subagent runs.

        ``resolve_llm_config`` settles it per role and stamps it on the config,
        so a subagent pinned to its own model is scaffolded for that model
        rather than for whatever the orchestrator runs. The manifest probe is
        the floor under a build that never went through the resolver.
        """
        config = self._config
        if config is None:
            return resolve_prompt_guidance(None)
        return config.prompt_guidance_for_role(
            f"subagent:{defn.name}"
        ) or resolve_prompt_guidance(
            defn.model or (config.llm.name if config.llm else None)
        )

    def _identity_line(self, defn: SubagentDefinition) -> str:
        """Build the first-line identity for a subagent."""
        return (
            f"You are the {defn.name} subagent. The parent agent spawned you "
            "from its thread to do one task, and its brief, with any update it "
            "sends, is all you see of that thread. Only your final message goes "
            "back to it; it does not see the steps in between."
        )

    def _compute_sections(self, defn: SubagentDefinition) -> dict[str, bool]:
        """Merge the section defaults with the definition's overrides."""
        return {**_SECTION_DEFAULTS, **defn.sections}

    @cached_property
    def _registry(self) -> RegistryTools | None:
        """This build's registry, read on first use and shared by every subagent."""
        return read_registry_tools(self._mcp_registry)

    def _allowed_servers(self, defn: SubagentDefinition) -> frozenset[str] | None:
        """The servers this subagent's code may call, or None for all of them.

        Taken from this turn's registry, which already holds only what the
        workspace selected. A brokerage connection is told apart by its
        address, the one identity its rules are keyed on.
        """
        if defn.connectors and defn.brokerages != "off":
            return None
        registry = self._registry
        if registry is None:
            return frozenset()
        from src.server.services.brokerage_capabilities import vendor_for_url

        allowed: set[str] = set()
        for name in {*registry.tools_by_server, *registry.server_configs}:
            config = registry.server_configs.get(name)
            if vendor_for_url(getattr(config, "url", None)) is not None:
                if defn.brokerages != "off":
                    allowed.add(name)
            elif defn.connectors:
                allowed.add(name)
        return frozenset(allowed)

    def _build_tool_summary(self, defn: SubagentDefinition) -> str | None:
        """The MCP tool summary this subagent reads, or None when it can reach
        no server this workspace offers. Code is its one way to reach one: a
        shell's limit is empty, and the direct tools stay with the main agent."""
        if "execute_code" not in defn.tools:
            return None
        registry = self._registry
        allowed = self._allowed_servers(defn)
        if allowed is not None and (
            registry is None or not allowed & registry.tools_by_server.keys()
        ):
            return None
        if registry is None:
            return ""
        return format_registry_tools(
            registry, mode="full", servers=allowed, preferred=defn.preferred_connectors
        )

    def _load_preloaded_skills(self, defn: SubagentDefinition) -> str:
        """Load SKILL.md content for preloaded skills."""
        if not defn.preload_skills:
            return ""
        parts: list[str] = []
        for skill_name in defn.preload_skills:
            content = load_skill_content(
                skill_name, self._skill_dirs, registry=self._skill_registry
            )
            if content:
                parts.append(f"## Skill: {skill_name}\n\n{content}")
            else:
                logger.warning("preload skill not found", skill=skill_name)
        return "\n\n".join(parts)

    # ── Tool resolution ───────────────────────────────────────────────

    def _own_tools(self, defn: SubagentDefinition) -> dict[str, Any]:
        """Tools this subagent runs in place of the main agent's, by tool name."""
        own: dict[str, Any] = {}
        config = self._config
        if (
            self._make_search_tool is not None
            and config is not None
            and "web_search" in defn.tools
            and (defn.search_provider or defn.search_depth)
        ):
            main = (config.search_api, config.search_depth)
            pair = (defn.search_provider or main[0], defn.search_depth or main[1])
            if pair != main:
                search = self._search_tools.get(pair)
                if search is None:
                    search = self._make_search_tool(provider=pair[0], depth=pair[1])
                    self._search_tools[pair] = search
                own[search.name] = search
        if self._make_sandbox_tools is not None and (
            "execute_code" in defn.tools or "bash" in defn.tools
        ):
            # Connectors are a Code capability, as the settings show them: a
            # shell alone reaches none, though it could start Python.
            allowed = self._allowed_servers(defn) if "execute_code" in defn.tools else frozenset()
            if allowed is not None:
                limited = self._sandbox_tools.get(allowed)
                if limited is None:
                    limited = {t.name: t for t in self._make_sandbox_tools(allowed)}
                    self._sandbox_tools[allowed] = limited
                own.update(limited)
        return own

    def _resolve_tools(self, defn: SubagentDefinition) -> list[Any]:
        """Resolve tool-set identifiers to tool objects."""
        tools: list[Any] = []
        own = self._own_tools(defn)

        # Each tool-set identifier, with this subagent's own copy in place of
        # any shared tool it has one of
        for tool_id in defn.tools:
            tool_list = self._tool_sets.get(tool_id)
            if tool_list is not None:
                tools.extend(own.get(getattr(t, "name", None), t) for t in tool_list)
            else:
                logger.warning(
                    "unknown tool set identifier",
                    tool_id=tool_id,
                    subagent=defn.name,
                    available=list(self._tool_sets),
                )

        tools.extend(defn.extra_tools)

        return tools

