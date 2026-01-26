"""
Skill registry for dynamic tool loading.

This module defines the registry of available skills that can be dynamically
loaded by the agent via the load_skill mechanism. Each skill contains a set
of tools that are pre-registered but hidden until the skill is loaded.
"""

from dataclasses import dataclass
from typing import Any

from src.tools.user_profile import USER_PROFILE_TOOLS


@dataclass
class SkillDefinition:
    """Definition of a loadable skill.

    Attributes:
        name: Unique skill identifier
        description: Human-readable description of what the skill does
        tools: List of LangChain tools included in this skill
        skill_md_path: Optional path to SKILL.md with detailed instructions
    """
    name: str
    description: str
    tools: list[Any]
    skill_md_path: str | None = None

    def get_tool_names(self) -> list[str]:
        """Get list of tool names in this skill."""
        return [
            getattr(t, "name", str(t))
            for t in self.tools
        ]


# Registry of all available skills
# Skills are pre-registered at agent creation but tools are hidden until loaded
SKILL_REGISTRY: dict[str, SkillDefinition] = {
    "user-profile": SkillDefinition(
        name="user-profile",
        description="Manage user profile: watchlists, portfolio, and preferences",
        tools=USER_PROFILE_TOOLS,
        skill_md_path="skills/user-profile/SKILL.md",
    ),
}


def get_skill(skill_name: str) -> SkillDefinition | None:
    """Get a skill definition by name.

    Args:
        skill_name: Name of the skill to retrieve

    Returns:
        SkillDefinition if found, None otherwise
    """
    return SKILL_REGISTRY.get(skill_name)


def get_all_skill_tools() -> list[Any]:
    """Get all tools from all registered skills.

    Used during agent creation to pre-register all tools with ToolNode.

    Returns:
        Flat list of all tools from all skills
    """
    all_tools = []
    for skill in SKILL_REGISTRY.values():
        all_tools.extend(skill.tools)
    return all_tools


def get_all_skill_tool_names() -> set[str]:
    """Get names of all tools from all registered skills.

    Used by middleware to identify which tools belong to skills.

    Returns:
        Set of tool names
    """
    names = set()
    for skill in SKILL_REGISTRY.values():
        names.update(skill.get_tool_names())
    return names


def list_skills() -> list[dict[str, Any]]:
    """List all available skills with their metadata.

    Returns:
        List of skill info dicts with name, description, and tool count
    """
    return [
        {
            "name": skill.name,
            "description": skill.description,
            "tool_count": len(skill.tools),
            "tools": skill.get_tool_names(),
        }
        for skill in SKILL_REGISTRY.values()
    ]
