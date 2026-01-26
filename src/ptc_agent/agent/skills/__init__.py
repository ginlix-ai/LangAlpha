"""
Skills module for dynamic tool loading.

This module provides:
- SkillDefinition: Dataclass for defining loadable skills
- SKILL_REGISTRY: Registry of all available skills
- Helper functions for skill management
"""

from ptc_agent.agent.skills.registry import (
    SkillDefinition,
    SKILL_REGISTRY,
    get_skill,
    get_all_skill_tools,
    get_all_skill_tool_names,
    list_skills,
)

__all__ = [
    "SkillDefinition",
    "SKILL_REGISTRY",
    "get_skill",
    "get_all_skill_tools",
    "get_all_skill_tool_names",
    "list_skills",
]
