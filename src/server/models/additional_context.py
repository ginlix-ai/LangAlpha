"""
Additional context models for workflow execution.

Supports flexible context types that can be passed along with user queries.
Contexts are fetched, formatted, and appended to user messages before processing.
"""

from datetime import datetime
from typing import Annotated, Any, Literal, Optional, List, Union
from pydantic import BaseModel, Discriminator, Field, Tag


class AdditionalContextBase(BaseModel):
    """Base model for additional context with type discrimination."""

    type: str = Field(..., description="Type of context (e.g., 'skills')")
    id: Optional[str] = Field(None, description="Resource identifier for fetching context")


class SkillContext(AdditionalContextBase):
    """Context requesting skill instructions to be loaded for the agent."""

    type: Literal["skills"] = "skills"
    name: str = Field(..., description="Skill name (e.g., 'user-profile')")
    instruction: Optional[str] = Field(
        None,
        description="Additional instruction for the skill (e.g., 'Help the user with first time onboarding')"
    )


class MultimodalContext(AdditionalContextBase):
    """Context providing an image, PDF, or arbitrary file attachment."""

    type: Literal["image", "pdf", "file"] = "image"
    data: str = Field(..., description="Base64 data URL (data:<mime>;base64,...)")
    description: Optional[str] = Field(None, description="Filename or caption for the attachment")


class DirectiveContext(AdditionalContextBase):
    """Context injecting a directive inline with the user message via XML tags."""

    type: Literal["directive"] = "directive"
    content: str = Field(..., description="Directive text to inject inline with user message")


class WidgetContext(AdditionalContextBase):
    """Context attached from a dashboard widget snapshot.

    Carries pre-rendered ``<widget-context>...</widget-context>`` markdown that
    is concatenated into a single ``<system-reminder>`` and appended to the last
    user message. Image bytes for chart-type widgets ride the existing
    ``MultimodalContext(type='image')`` channel — this model does not transport
    image data.
    """

    type: Literal["widget"] = "widget"
    widget_type: str = Field(..., description="Widget definition id (e.g., 'markets.chart')")
    widget_id: str = Field(..., description="Widget instance id (uuid) — stable across reflows")
    label: str = Field(..., description="Human-readable label for the snapshot (chip title)")
    text: str = Field(..., description="Pre-rendered <widget-context>...</widget-context> markdown")
    data: dict[str, Any] = Field(default_factory=dict, description="Structured raw payload for replay")
    captured_at: Optional[datetime] = Field(None, description="When the snapshot was taken (client clock)")
    description: Optional[str] = Field(None, description="Optional caption / freshness note")


AdditionalContext = Annotated[
    Union[
        Annotated[SkillContext, Tag("skills")],
        Annotated[MultimodalContext, Tag("image")],
        Annotated[MultimodalContext, Tag("pdf")],
        Annotated[MultimodalContext, Tag("file")],
        Annotated[DirectiveContext, Tag("directive")],
        Annotated[WidgetContext, Tag("widget")],
    ],
    Discriminator(lambda v: v.get("type") if isinstance(v, dict) else getattr(v, "type", None)),
]


def format_additional_contexts(contexts: List[AdditionalContextBase]) -> str:
    """Join multiple context strings into a single markdown section with a separator."""
    if not contexts:
        return ""

    return "\n\n---\n\n" + "\n\n".join(contexts)
