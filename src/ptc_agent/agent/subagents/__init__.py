"""Declarative subagent system.

Public API:
    SubagentDefinition  — dataclass describing a subagent
    SubagentRegistry    — merges built-in + user-defined definitions
    SubagentCompiler    — compiles definitions into SubAgent TypedDicts
    BUILTIN_SUBAGENTS   — shipped defaults
    create_subagents    — main entry point
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ptc_agent.agent.roles import AgentRole

from .builtins import BUILTIN_SUBAGENTS
from .compiler import SubagentCompiler
from .definition import SubagentDefinition
from .registry import SubagentRegistry

if TYPE_CHECKING:
    from .merge import ResolvedSubagents


def create_subagents(
    roster: ResolvedSubagents,
    compiler: SubagentCompiler,
    role: AgentRole,
) -> list[dict[str, Any]]:
    """Compile the roster's enabled subagents for ``role`` into SubAgent TypedDicts."""
    return [compiler.compile(defn) for defn in roster.for_role(role)]


__all__ = [
    "BUILTIN_SUBAGENTS",
    "SubagentCompiler",
    "SubagentDefinition",
    "SubagentRegistry",
    "create_subagents",
]
