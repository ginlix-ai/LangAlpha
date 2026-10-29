"""Declarative subagent definition.

A SubagentDefinition describes *what* a subagent is — its tools, skills,
prompt strategy, and section toggles.  It gets compiled into a SubAgent TypedDict
at runtime by SubagentCompiler.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from ptc_agent.agent.roles import ALL_ROLES, AgentRole

BrokerageAccess = Literal["off", "on"]

#: The sections about writing and running code, which follow ``execute_code``.
CODE_SECTIONS = ("tool_guide", "workspace_paths", "data_processing", "visualizations")


@dataclass
class SubagentDefinition:
    """Declarative definition of a subagent.

    Prompt resolution priority (compiler checks in order):
      1. ``custom_prompt`` — raw string, used directly, no rendering.
      2. ``custom_prompt_template`` — standalone Jinja2 template, bypasses base.
      3. ``role_prompt`` / ``role_prompt_template`` — rendered via
         ``subagent_base.md.j2`` with section toggles + role appended (default).
    """

    # ── Identity ──────────────────────────────────────────────────────
    name: str
    """Unique identifier (e.g. ``"research"``, ``"general-purpose"``)."""

    description: str
    """Shown to the main agent for delegation decisions."""

    # ── Prompt configuration ──────────────────────────────────────────
    role_prompt: str = ""
    """Inline role-specific instructions appended to base template."""

    role_prompt_template: str | None = None
    """Path to a ``roles/*.md.j2`` template (alternative to inline)."""

    custom_prompt_template: str | None = None
    """Standalone Jinja2 template path — bypasses base template entirely."""

    custom_prompt: str | None = None
    """Raw prompt string — bypasses all template rendering."""

    # ── Skills ────────────────────────────────────────────────────────
    preload_skills: list[str] = field(default_factory=list)
    """Skill names whose SKILL.md is put into the prompt."""

    # ── Model ─────────────────────────────────────────────────────────
    model: str | None = None
    """Model override.  ``None`` = use the parent agent's model."""

    reasoning_effort: str | None = None
    """Effort the subagent runs at, whatever its model. ``None`` = the model's
    saved effort when a model is pinned, else the turn's."""

    # ── Tools ─────────────────────────────────────────────────────────
    tools: list[str] = field(default_factory=list)
    """Tool set identifiers resolved at compile time (e.g. ``["execute_code", "filesystem"]``)."""

    extra_tools: list[Any] = field(default_factory=list)
    """Additional raw tool objects injected at runtime (not serialisable to YAML)."""

    search_provider: str | None = None
    """Search provider for this subagent's ``web_search``. ``None`` = the turn's."""

    search_depth: str | None = None
    """Search depth level for that provider. ``None`` = the turn's."""

    tools_narrowed: bool = False
    """Whether the user's settings leave out a tool its instructions were
    written for. Its prompt then says a tool it names may be missing, since
    the built-in role prompts and tool guide name tools by name."""

    # ── Access ────────────────────────────────────────────────────────
    connectors: bool = True
    """Whether its code may call connectors other than brokerage connections."""

    brokerages: BrokerageAccess = "on"
    """Brokerage connections: none, or general access."""

    memory: bool = False
    """Whether it gets the user's memory indices and the rules for reading them.
    Read only: anything worth saving comes back in its result."""

    preferred_connectors: list[str] = field(default_factory=list)
    """Servers its tool guide lists first. They never limit what it can call."""

    # ── Template section toggles ──────────────────────────────────────
    sections: dict[str, bool] = field(default_factory=dict)
    """Override per-section defaults for the base template.

    Only used when the base template path is active (i.e. not ``custom_prompt``
    or ``custom_prompt_template``).
    """

    # ── Roles ─────────────────────────────────────────────────────────
    roles: frozenset[AgentRole] = ALL_ROLES
    """Which agent roles get this subagent in their roster.

    Narrowed for a subagent whose work one role hands to another instead, so
    the roster the prompt advertises is the one the build compiles.
    """

    # ── Source tracking ───────────────────────────────────────────────
    source: str = "builtin"
    """``"builtin"``, ``"user"`` (deployment YAML), ``"plugin"`` (a plugin's
    ``agents/`` file) or ``"custom"`` (a user's own row)."""
