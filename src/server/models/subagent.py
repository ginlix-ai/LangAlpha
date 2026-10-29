"""Wire shapes for the Subagents pane (``/api/v1/subagents``)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

ToolWord = Literal["web", "market_data", "code", "shell"]
BrokerageAccess = Literal["off", "on"]
Role = Literal["analyst", "chief_of_staff"]
ProblemCode = Literal[
    "model_unavailable",
    "skill_missing",
    "connector_missing",
    "search_not_on_plan",
    "search_unavailable",
    "depth_not_offered",
    "effort_not_offered",
    "description_matches",
    "base_removed",
    "name_collision",
]


class SubagentProblem(BaseModel):
    code: ProblemCode
    #: The missing name, the matching subagent, the provider or level.
    detail: str | None = None


class SubagentValues(BaseModel):
    """The values an override may set, as a base sets them."""

    model: str | None
    effort: str | None
    tools: list[ToolWord]
    search_provider: str | None
    search_depth: str | None
    connectors: bool
    brokerages: BrokerageAccess
    #: Reads the user's memory; never writes it.
    memory: bool
    preferred_connectors: list[str]
    preferred_skills: list[str]
    enabled: bool


class SubagentPlugin(BaseModel):
    name: str
    display_name: str
    #: False on an override whose plugin is gone, which has no page to open.
    installed: bool = True


class SubagentView(SubagentValues):
    name: str
    kind: Literal["base", "custom"]
    source: Literal["builtin", "deployment", "plugin", "custom"]
    plugin: SubagentPlugin | None = None
    #: A base the user changed.
    edited: bool
    #: Keys the user's row sets.
    user_fields: list[str]
    description: str
    #: Custom: its own text. Base: the rendered, read-only body.
    instructions: str
    available_to: list[Role]
    #: The base's own values; null for a custom subagent.
    base: SubagentValues | None
    problems: list[SubagentProblem]
    updated_by: Literal["user", "agent"] | None = None
    updated_thread_id: str | None = None
    updated_thread_title: str | None = None
    updated_at: str | None = None
    #: Its file in the agent's folder; null for a deployment name no file can carry.
    file: str | None
    #: Send back on PUT and DELETE.
    version: str


class MainAgentValues(BaseModel):
    model: str | None
    effort: str | None
    search_provider: str | None
    search_depth: str | None


class SubagentConnector(BaseModel):
    name: str
    kind: Literal["builtin", "plugin", "user", "brokerage"]
    label: str
    #: Off for this account; a preferred entry naming it is skipped until it's back on.
    enabled: bool


class BrokerageSummary(BaseModel):
    #: Some brokerage is connected: the pane shows the Brokerages block.
    connected: bool


class SubagentLimits(BaseModel):
    max_custom: int
    description_max: int
    instructions_max_bytes: int


class SubagentsResponse(BaseModel):
    main: MainAgentValues
    subagents: list[SubagentView]
    connectors: list[SubagentConnector]
    #: The names valid for preferred_skills.
    skills: list[str]
    brokerages: BrokerageSummary
    limits: SubagentLimits


class SubagentWrite(BaseModel):
    #: The version read; required for an existing subagent, null creates a custom one.
    version: str | None = None
    #: Create only: copy that subagent's effective values.
    start_from: str | None = None
    #: Partial. Null clears the user's value back to the base's; "inherit"
    #: (model, effort, search_provider, search_depth) chooses the thread's or
    #: the user's own value explicitly. Checked by the shared validator, so
    #: each refused key comes back as one field error.
    fields: dict[str, Any] = Field(default_factory=dict)
