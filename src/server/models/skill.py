"""Request and response models for the skills routers."""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class SkillInfo(BaseModel):
    name: str
    description: str
    tool_count: int
    tools: list[str] = Field(default_factory=list)
    command: str | None = None
    origin: Literal["platform", "user", "workspace"] = "platform"
    enabled: bool = True
    editable: bool = False
    deletable: bool = False
    confirmed: bool = True
    plugin_id: str | None = None
    # Display-only provenance: the owning plugin's name and its enable state
    # (None on hand-made or detached rows). plugin_enabled=False explains a
    # row the delivery predicate is suppressing.
    plugin_name: str | None = None
    plugin_enabled: bool | None = None
    size_bytes: int = 0
    updated_at: str | None = None
    # Which tier switched an inherited skill off (workspace views only).
    disabled_scope: Literal["user", "workspace"] | None = None
    # Workspace row reusing (and thereby hiding) a user-tier name here.
    shadows_inherited: bool = False
    # The scope a workspace-tier row belongs to (None = user/platform tier).
    workspace_id: str | None = None
    # Workspaces where an all-workspaces skill is switched off (deny-list) —
    # populated in the all-scopes view only, for the "active in" checklist.
    disabled_workspace_ids: list[str] = Field(default_factory=list)


class SkillsResponse(BaseModel):
    skills: list[SkillInfo]


class SkillEnabledInput(BaseModel):
    """PATCH body; both fields optional so one call can toggle, re-alias, or
    both. ``command: null`` (sent explicitly) clears the alias back to the
    name — ``model_fields_set`` is what tells that apart from absent."""

    enabled: bool | None = None
    command: str | None = None


class SkillFile(BaseModel):
    path: str
    size: int


class SkillContentResponse(BaseModel):
    name: str
    path: str = "SKILL.md"
    # None when the file is not text, or too large to show on a page.
    content: Optional[str]
    size: int = 0
    files: list[SkillFile] = Field(default_factory=list)


class SkillLimitsResponse(BaseModel):
    # The zip as sent.
    max_upload_bytes: int
    # The skill as kept, re-zipped after what an upload sets aside; None
    # where object storage holds archives and the unpacked caps bound it.
    max_stored_bytes: Optional[int]
    max_files: int
    max_file_bytes: int
    max_unpacked_bytes: int
    max_skill_md_bytes: int


class SkillMoveInput(BaseModel):
    """Both scopes are explicit: names are only unique within one scope, so
    the source disambiguates which row moves."""

    from_workspace_id: str | None = None
    to_workspace_id: str | None = None
