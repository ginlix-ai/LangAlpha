"""
Skills module — registry, middleware, and helpers for dynamic tool loading.

This module provides:
- SkillDefinition: Dataclass for defining loadable skills
- SkillMode: Type alias for agent modes ("ptc" or "flash")
- SKILL_REGISTRY: Registry of all available skills
- SkillsMiddleware: Unified middleware for skill discovery and tool gating
- Helper functions for skill management
"""

from ptc_agent.agent.middleware.skills.content import (
    SkillPrefixResult,
    SkillRequest,
    build_skill_content,
    compute_already_loaded,
    load_skill_content,
)
from ptc_agent.agent.middleware.skills.discovery import SkillMetadata
from ptc_agent.agent.middleware.skills.lock import (
    SkillLockEntry,
    SkillsLockFile,
)
from ptc_agent.agent.middleware.skills.registry import (
    SKILL_REGISTRY,
    SkillDefinition,
    SkillMode,
    get_all_skill_tool_names,
    get_all_skill_tools,
    get_command_to_skill_map,
    get_sandbox_skill_names,
    get_skill,
    get_skill_registry,
    list_skills,
)
from ptc_agent.agent.middleware.skills.middleware import (
    SkillsMiddleware,
)

__all__ = [
    "SkillDefinition",
    "SkillLockEntry",
    "SkillMetadata",
    "SkillMode",
    "SkillsLockFile",
    "SKILL_REGISTRY",
    "SkillPrefixResult",
    "SkillRequest",
    "SkillsMiddleware",
    "build_skill_content",
    "compute_already_loaded",
    "get_command_to_skill_map",
    "get_skill",
    "get_skill_registry",
    "get_all_skill_tools",
    "get_all_skill_tool_names",
    "get_sandbox_skill_names",
    "list_skills",
    "load_skill_content",
]
