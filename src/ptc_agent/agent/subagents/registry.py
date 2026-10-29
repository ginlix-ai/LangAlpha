"""Subagent registry — merges built-in and user-defined definitions."""

from __future__ import annotations

from dataclasses import replace

import structlog

from ptc_agent.agent.roles import ALL_ROLES, AgentRole
from ptc_agent.agent.subagents.builtins import BUILTIN_SUBAGENTS
from ptc_agent.agent.subagents.definition import CODE_SECTIONS, SubagentDefinition
from ptc_agent.config.agent import SubagentConfig

logger = structlog.get_logger(__name__)


_PROMPT_FIELDS = frozenset(
    {"role_prompt", "role_prompt_template", "custom_prompt_template", "custom_prompt"}
)


def _patched(builtin: SubagentDefinition, cfg: SubagentConfig) -> SubagentDefinition:
    """``builtin`` with only the fields the YAML sets, the way an overlay reads.

    ``sections`` merges key by key and lists replace. Tools that add or drop
    ``execute_code`` switch the code sections with it, as a user's tool edit
    does, unless the YAML sets them. The result is still that built-in, so a
    user's changes written against it keep applying on top, and it keeps the
    built-in's roles, which the YAML has no say over: reshaping an
    analyst-only subagent must not hand it to the Chief of Staff.
    """
    changes = {field: getattr(cfg, field) for field in cfg.model_fields_set}
    # A blank description is a missing one, as the loader reads it for a new
    # subagent: the parent delegates by it, so the built-in's stays.
    if not changes.get("description"):
        changes.pop("description", None)
    if changes.get("memory") is None:
        changes.pop("memory", None)
    if "tools" in changes or "sections" in changes:
        sections = dict(builtin.sections)
        has_code = "execute_code" in changes.get("tools", builtin.tools)
        if has_code != ("execute_code" in builtin.tools):
            sections.update(dict.fromkeys(CODE_SECTIONS, has_code))
        changes["sections"] = {**sections, **changes.get("sections", {})}
    # The built-in's own prompt names its tools, so it says when they are fewer.
    # A prompt field set to the value it has (an empty inline prompt above all,
    # which still renders the role template) leaves that prompt in place.
    own_prompt = any(changes.get(f, getattr(builtin, f)) != getattr(builtin, f) for f in _PROMPT_FIELDS)
    if "tools" in changes and not own_prompt:
        changes["tools_narrowed"] = bool(set(builtin.tools) - set(cfg.tools))
    return replace(builtin, **changes)


class SubagentRegistry:
    """Registry that merges built-in and user-defined subagent definitions.

    Resolution order: built-ins first, then the deployment's YAML definitions.
    A definition with a built-in's name sets only the fields it lists.
    """

    def __init__(
        self,
        builtins: dict[str, SubagentDefinition] | None = None,
        user_definitions: dict[str, SubagentConfig] | None = None,
    ) -> None:
        self._definitions: dict[str, SubagentDefinition] = {}

        # 1. Load built-ins
        for name, defn in (builtins or BUILTIN_SUBAGENTS).items():
            self._definitions[name] = defn

        # 2. Load the deployment's definitions; one with a built-in's name
        #    changes only the fields it sets.
        for name, cfg in (user_definitions or {}).items():
            builtin = self._definitions.get(name)
            if builtin is not None:
                logger.info("deployment definition patches builtin", name=name)
                self._definitions[name] = _patched(builtin, cfg)
                continue

            self._definitions[name] = SubagentDefinition(
                name=name,
                description=cfg.description or "",
                role_prompt=cfg.role_prompt,
                role_prompt_template=cfg.role_prompt_template,
                custom_prompt_template=cfg.custom_prompt_template,
                custom_prompt=cfg.custom_prompt,
                tools=cfg.tools,
                preload_skills=cfg.preload_skills,
                model=cfg.model,
                reasoning_effort=cfg.reasoning_effort,
                search_provider=cfg.search_provider,
                search_depth=cfg.search_depth,
                connectors=cfg.connectors,
                brokerages=cfg.brokerages,
                preferred_connectors=cfg.preferred_connectors,
                memory=cfg.memory if cfg.memory is not None else True,
                sections=cfg.sections,
                roles=ALL_ROLES,
                source="user",
            )

    def get(self, name: str) -> SubagentDefinition | None:
        """Get a definition by name."""
        return self._definitions.get(name)

    def get_enabled(
        self, enabled_names: list[str], role: AgentRole | None = None
    ) -> list[SubagentDefinition]:
        """Return definitions for enabled subagents, in order.

        With a ``role``, a subagent that role is not listed for is left out
        rather than refused: ``enabled`` is one list for every role, and the
        role decides which of it applies.

        Raises:
            ValueError: If an enabled name is not found in the registry.
        """
        result: list[SubagentDefinition] = []
        for name in enabled_names:
            defn = self._definitions.get(name)
            if defn is None:
                available = ", ".join(sorted(self._definitions))
                msg = (
                    f"Subagent '{name}' is in 'enabled' list but not defined. "
                    f"Available: [{available}]"
                )
                raise ValueError(msg)
            if role is not None and role not in defn.roles:
                continue
            result.append(defn)
        return result

    def list_all(self) -> dict[str, SubagentDefinition]:
        """Return a copy of all registered definitions."""
        return dict(self._definitions)

