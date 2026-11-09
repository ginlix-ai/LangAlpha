"""Request models for reports to the platform (see ``services/reports``)."""

from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

# What the platform made of a call (``services.reports.PlatformResult``).
PlatformStatus = Literal[
    "accepted",
    "duplicate",
    "rate_limited",
    "unavailable",
    "not_found",
    "closed",
    "note_limit",
]


class ReportError(BaseModel):
    # The platform refuses an empty one, on every retry.
    type: str = Field(min_length=1, max_length=200)
    message: str = Field(default="", max_length=2000)

    @field_validator("message", mode="before")
    @classmethod
    def _fit(cls, value: object) -> object:
        # The banner's text is a provider's error as it came, with no limit of
        # its own; refusing a long one would refuse the report on every retry.
        return value[:2000] if isinstance(value, str) else value


class ReportClient(BaseModel):
    surface: Literal["web", "desktop", "mobile"]
    app_version: Optional[str] = Field(default=None, max_length=64)
    locale: Optional[str] = Field(default=None, max_length=35)


class ReportCreate(BaseModel):
    """What a client sends. The server adds who, where and the links."""

    # Made by the client, so a retry of the same report is the same report.
    report_id: UUID
    event: Literal["user_report", "error_report"]
    source: Literal["button", "error_banner"]
    category: Optional[Literal["bug", "feedback", "billing", "other"]] = None
    user_note: Optional[str] = Field(default=None, max_length=4000)
    # The thread the report is about, when the user chose to include it. A
    # UUID, so every use of it, run ownership included, sees one form.
    thread_id: Optional[UUID] = None
    # The failed run the banner shows, so a report names it even when the
    # thread has moved on, or the run is not yet finalized, when it arrives.
    run_id: Optional[UUID] = None
    error: Optional[ReportError] = None
    client: ReportClient


class ReportFollowUp(BaseModel):
    """A note the user added to a thumbs-down's report after sending it."""

    note: str
    at: str
    # The client's id for the note; absent on none, kept for its retries.
    id: Optional[str] = None
