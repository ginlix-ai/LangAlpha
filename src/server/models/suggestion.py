"""Pydantic models for follow-up suggestion generation."""

from pydantic import BaseModel, Field


class SuggestionItem(BaseModel):
    """A single follow-up suggestion."""

    text: str = Field(
        description="A concise follow-up question or suggestion, in the user's language"
    )


class SuggestionResponse(BaseModel):
    """Structured output for LLM-generated follow-up suggestions."""

    suggestions: list[SuggestionItem] = Field(
        max_length=5,
        description="1-5 follow-up suggestions based on the assistant's last reply",
    )
