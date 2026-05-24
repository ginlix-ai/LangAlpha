"""Pydantic models for the template system.

Templates are upper-layer "applications" (e.g. sirius-valuation) that group
workspaces under a shared dashboard. Each template entry binds 1:1 to a
workspace via CASCADE FK.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class TemplateEntryStatus(str, Enum):
    """Lifecycle of a template entry's analysis run."""

    PENDING = "pending"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    PARTIAL = "partial"   # 部分完成（如 EVI 模板：分部估值 OK，但集团 SOTP 未完成）
    FAILED = "failed"


# =============================================================================
# Template manifest (returned by /api/v1/templates)
# =============================================================================


class TemplateField(BaseModel):
    """A single field in the instantiation form (rendered by the frontend)."""

    name: str = Field(description="Field name (matches a key in `params`)")
    label: str = Field(description="Display label")
    type: str = Field(
        description="Field type: text / select / number",
    )
    required: bool = Field(default=True)
    placeholder: Optional[str] = None
    options: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description="Options for type=select, list of {value, label}",
    )


class TemplateManifest(BaseModel):
    """Public-facing template definition exposed via /api/v1/templates."""

    id: str = Field(description="Stable template id, e.g. 'sirius-valuation'")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="Short description shown on the card")
    icon: Optional[str] = Field(
        default=None,
        description="Optional icon hint (e.g. 'trending-up')",
    )
    version: str = Field(default="1.0.0")

    # Form fields used by the "instantiate" dialog. The frontend renders these
    # generically using shared/InstantiateDialog.tsx.
    fields: List[TemplateField] = Field(default_factory=list)

    # Estimated analysis duration (used by the UI to show a hint).
    estimated_minutes: Optional[int] = Field(default=None)


class TemplateListResponse(BaseModel):
    """Response for GET /api/v1/templates."""

    templates: List[TemplateManifest]


# =============================================================================
# Template entries
# =============================================================================


class TemplateEntryInstantiateRequest(BaseModel):
    """Request body for POST /api/v1/templates/{template_id}/entries.

    Either ``entry_key`` OR ``display_name`` must be non-empty. The
    orchestrator validates this and derives a stable placeholder key from
    ``display_name`` when ``entry_key`` is omitted.
    """

    entry_key: str = Field(
        default="",
        max_length=128,
        description=(
            "Business key within the template (e.g. stock symbol). "
            "May be empty if display_name is provided."
        ),
    )
    display_name: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Human-readable name (e.g. company name)",
    )
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Template-specific parameters (validated by the template manifest)",
    )


class TemplateEntryResponse(BaseModel):
    """Response model for a single template entry."""

    entry_id: str
    user_id: str
    template_id: str
    workspace_id: str
    entry_key: str
    display_name: Optional[str] = None

    status: TemplateEntryStatus
    progress: Dict[str, Any] = Field(default_factory=dict)
    summary: Dict[str, Any] = Field(default_factory=dict)
    payload: Dict[str, Any] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None

    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class TemplateEntryListResponse(BaseModel):
    """Response for GET /api/v1/templates/{template_id}/entries."""

    entries: List[TemplateEntryResponse] = Field(default_factory=list)
    total: int = 0
    limit: int
    offset: int


# =============================================================================
# Internal finalize endpoint (called by sandbox-side persist_entry.py)
# =============================================================================


class TemplateEntryFinalizeRequest(BaseModel):
    """Body for POST /api/v1/templates/_internal/entries/{entry_id}/finalize.

    Called from inside the sandbox by persist_entry.py after analysis is done.
    Authenticated via X-Internal-Service-Token header.
    """

    status: TemplateEntryStatus = Field(
        description="completed or failed",
    )
    summary: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Compact dashboard data (4-8 fields)",
    )
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Full structured analysis result",
    )
    error_message: Optional[str] = Field(default=None)


class TemplateEntryProgressUpdate(BaseModel):
    """Body for POST /api/v1/templates/_internal/entries/{entry_id}/progress."""

    progress: Dict[str, Any]
    status: Optional[TemplateEntryStatus] = None
