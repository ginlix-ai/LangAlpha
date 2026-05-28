"""SubagentCompiler — turns SubagentDefinitions into SubAgent TypedDicts.

The compiler handles:
  * Prompt rendering (custom_prompt > custom_prompt_template > base + role)
  * Tool-set resolution from string identifiers
  * Skill integration (runtime + preload)
  * Section-toggle computation (mode defaults + definition overrides)
"""

from __future__ import annotations

from typing import Any

import structlog

from ptc_agent.agent.middleware.skills.content import load_skill_content
from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY
from ptc_agent.agent.prompts import build_tool_summary_from_registry, get_loader
from ptc_agent.agent.subagents.definition import SubagentDefinition

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
        config: Any | None = None,
    ) -> None:
        self._sandbox = sandbox
        self._mcp_registry = mcp_registry
        self._tool_sets: dict[str, list[Any]] = tool_sets or {}
        self._user_profile = user_profile
        self._current_time = current_time
        self._thread_id = thread_id
        self._config = config

    # ── Public API ────────────────────────────────────────────────────

    def compile(self, definition: SubagentDefinition) -> dict[str, Any]:
        """Compile a single definition into a ``SubAgent`` TypedDict."""
        prompt = self._resolve_prompt(definition)
        tools = self._resolve_tools(definition)

        result: dict[str, Any] = {
            "name": definition.name,
            "description": definition.description,
            "system_prompt": prompt,
            "tools": tools,
        }

        # Resolved client (credentialed user) overrides the string model name.
        resolved = None
        if self._config is not None:
            resolved = self._config.client_for_role(
                f"subagent:{definition.name}", fallback_to_main=False
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
            content = load_skill_content(skill_name)
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

        # 2. Resolve skills (both runtime and preload) → add skill tools
        all_skill_names = set(defn.skills) | set(defn.preload_skills)
        for skill_name in all_skill_names:
            skill = SKILL_REGISTRY.get(skill_name)
            if skill and skill.tools:
                tools.extend(skill.tools)

        # 3. Add extra raw tool objects
        tools.extend(defn.extra_tools)

        return tools
