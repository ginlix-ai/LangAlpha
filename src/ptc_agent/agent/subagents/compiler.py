"""SubagentCompiler — turns SubagentDefinitions into SubAgent TypedDicts.

The compiler handles:
  * Prompt rendering (custom_prompt > custom_prompt_template > base + role)
  * Tool-set resolution from string identifiers
  * Skill integration (runtime + preload)
  * Section-toggle computation (mode defaults + definition overrides)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from ptc_agent.agent.middleware.skills.content import load_skill_content
from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY, SkillDefinition
from ptc_agent.agent.prompts import (
    build_tool_summary_from_registry,
    get_loader,
    guidance_template_vars,
    resolve_prompt_guidance,
)
from ptc_agent.agent.subagents.definition import SubagentDefinition

if TYPE_CHECKING:
    from ptc_agent.config.agent import AgentConfig

logger = structlog.get_logger(__name__)

# ── Section toggle defaults per mode ──────────────────────────────────
# True = section included, False = excluded.
# The definition.sections dict overrides these per-subagent.

_PTC_SUBAGENT_DEFAULTS: dict[str, bool] = {
    "task_workflow": False,
    "plan_mode": False,
    "workspace_paths": True,
    "tool_guide": True,
    "subagent_coordination": False,
    "data_processing": True,
    "visualizations": True,
    "output_guidelines": False,
    "workspace_context": False,
    "ask_user_guidelines": False,
}

_FLASH_SUBAGENT_DEFAULTS: dict[str, bool] = {
    "task_workflow": False,
    "plan_mode": False,
    "workspace_paths": False,
    "tool_guide": False,
    "subagent_coordination": False,
    "data_processing": False,
    "visualizations": False,
    "trading_constraints": False,
    "output_guidelines": False,
    "workspace_context": False,
    "ask_user_guidelines": False,
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
    ) -> None:
        self._sandbox = sandbox
        self._mcp_registry = mcp_registry
        self._tool_sets: dict[str, list[Any]] = tool_sets or {}
        self._user_profile = user_profile
        self._current_time = current_time
        self._thread_id = thread_id
        self._config = config
        # Per-build registry (feature gates + user disables + user skills),
        # authoritative when provided; None (bare/test construction) keeps the
        # global-registry behavior.
        self._skill_registry = skill_registry
        self._skill_dirs = skill_dirs

    # ── Public API ────────────────────────────────────────────────────

    def compile(self, definition: SubagentDefinition) -> dict[str, Any]:
        """Compile a single definition into a ``SubAgent`` TypedDict."""
        result: dict[str, Any] = {
            "name": definition.name,
            "description": definition.description,
            "system_prompt": self._resolve_prompt(definition),
            "tools": self._resolve_tools(definition),
        }

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
        if model is not None:
            result["model"] = model

        return result

    def compile_many(
        self, definitions: list[SubagentDefinition]
    ) -> list[dict[str, Any]]:
        """Compile multiple definitions."""
        return [self.compile(d) for d in definitions]

    # ── Prompt resolution ─────────────────────────────────────────────

    def _resolve_prompt(self, defn: SubagentDefinition) -> str:
        """Resolve the system prompt based on priority.

        1. ``custom_prompt`` — raw string, used directly.
        2. ``custom_prompt_template`` — standalone template.
        3. Base template + role prompt (default).
        """
        loader = get_loader()

        # 1. Raw custom prompt — bypass all rendering
        if defn.custom_prompt is not None:
            return defn.custom_prompt

        # Shared template variables
        template_kwargs: dict[str, Any] = {
            "current_time": self._current_time,
            "thread_id": self._thread_id,
            "max_iterations": defn.max_iterations,
            "user_profile": self._user_profile,
            **self._tool_gates(defn),
            **guidance_template_vars(self._guidance(defn)),
        }
        # Pass working_directory so workspace_paths template can use it
        if self._sandbox is not None and hasattr(self._sandbox, "config"):
            template_kwargs["working_directory"] = (
                self._sandbox.config.filesystem.working_directory
            )

        # 2. Standalone custom template — render it directly
        if defn.custom_prompt_template is not None:
            return loader.render(defn.custom_prompt_template, **template_kwargs)

        # 3. Base template + role prompt (default path)
        sections = self._compute_sections(defn)
        if sections.get("tool_guide", False):
            template_kwargs["tool_summary"] = self._build_tool_summary()

        preloaded_content = self._load_preloaded_skills(defn)

        return loader.get_subagent_base_prompt(
            identity_line=self._identity_line(defn),
            role_prompt_template=defn.role_prompt_template,
            role_prompt=defn.role_prompt,
            preloaded_skills_content=preloaded_content,
            sections=sections,
            **template_kwargs,
        )

    def _tool_gates(self, defn: SubagentDefinition) -> dict[str, bool]:
        """Which tool tiers this subagent's prompt is allowed to advertise.

        The tool guide is shared, the tool sets are not: report-builder binds
        neither finance nor web, no subagent binds show_widget, and the yaml
        default for market_watch (true) would advertise a tool ``agent.py`` has
        already gated out of this user's finance set.
        """
        bound = set(defn.tools)
        market_watch = (
            self._config is not None
            and self._config.feature_enabled("market_watch")
            and "finance" in bound
        )
        return {
            "market_watch_enabled": market_watch,
            "show_widget_enabled": "show_widget" in bound,
            "finance_enabled": "finance" in bound,
            "web_enabled": "web_search" in bound,
        }

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
        return f"You are a {defn.name} task execution sub-agent."

    def _compute_sections(self, defn: SubagentDefinition) -> dict[str, bool]:
        """Merge mode defaults with definition overrides."""
        if defn.mode == "flash":
            base = dict(_FLASH_SUBAGENT_DEFAULTS)
        else:
            base = dict(_PTC_SUBAGENT_DEFAULTS)
        base.update(defn.sections)
        return base

    def _build_tool_summary(self) -> str:
        """Build MCP tool summary if mcp_registry is available."""
        return build_tool_summary_from_registry(self._mcp_registry, mode="full")

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

    def _resolve_tools(self, defn: SubagentDefinition) -> list[Any]:
        """Resolve tool-set identifiers + skills to tool objects."""
        tools: list[Any] = []

        # 1. Resolve tool-set identifiers
        for tool_id in defn.tools:
            tool_list = self._tool_sets.get(tool_id)
            if tool_list is not None:
                tools.extend(tool_list)
            else:
                logger.warning(
                    "unknown tool set identifier",
                    tool_id=tool_id,
                    subagent=defn.name,
                    available=list(self._tool_sets),
                )

        # 2. Resolve skills (both runtime and preload) → add skill tools.
        # The per-build registry keeps a skill the main agent dropped (feature
        # gate, user disable) from handing its tools to a subagent anyway.
        registry = self._skill_registry if self._skill_registry is not None else SKILL_REGISTRY
        all_skill_names = set(defn.skills) | set(defn.preload_skills)
        for skill_name in all_skill_names:
            skill = registry.get(skill_name)
            if skill and skill.tools:
                tools.extend(skill.tools)

        # 3. Add extra raw tool objects
        tools.extend(defn.extra_tools)

        return tools
