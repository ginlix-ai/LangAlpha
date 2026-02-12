"""
Additional context models for workflow execution.

Supports flexible context types that can be passed along with user queries.
Contexts are fetched, formatted, and appended to user messages before processing.
"""

from typing import Annotated, Literal, Optional, List, Dict, Any, Union
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
    """Context providing an image or PDF to inject into the conversation as native content blocks."""

    type: Literal["image"] = "image"
    data: str = Field(..., description="Base64 data URL (data:image/...;base64,... or data:application/pdf;base64,...)")
    description: Optional[str] = Field(None, description="Optional caption for the attachment")


# Union type for all context types - discriminated by "type" field
AdditionalContext = Annotated[
    Union[
        Annotated[SkillContext, Tag("skills")],
        Annotated[MultimodalContext, Tag("image")],
    ],
    Discriminator(lambda v: v.get("type") if isinstance(v, dict) else getattr(v, "type", None)),
]


def format_additional_contexts(contexts: List[AdditionalContextBase]) -> str:
    """
    Format multiple additional contexts into a single markdown section.

    Args:
        contexts: List of formatted context strings

    Returns:
        Combined markdown section with separator
    """
    if not contexts:
        return ""

    return "\n\n---\n\n" + "\n\n".join(contexts)
