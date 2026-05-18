"""
Pydantic models for workspace thread management API.

This module defines response models for workspace thread endpoints that work with
the query-response schema (workspaces, thread, query, response).
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, model_validator


# ==================== Workspace Thread Response Models ====================

class WorkspaceThreadListItem(BaseModel):
    """Response model for a thread in list view."""
    thread_id: str = Field(..., description="Thread ID")
    workspace_id: str = Field(..., description="Workspace ID")
    thread_index: int = Field(..., description="Thread index within workspace")
    current_status: str = Field(..., description="Thread status")
    msg_type: Optional[str] = Field(None, description="Message type")
    title: Optional[str] = Field(None, description="User-editable thread title")
    first_query_content: Optional[str] = Field(
        None, description="First user query content (preview)"
    )
    platform: Optional[str] = Field(
        None,
        description="Origin platform: 'web', 'market_view:<SYMBOL>', or channel "
        "name ('telegram', 'slack', 'discord', 'feishu'). NULL for pre-tracking rows.",
    )
    is_shared: bool = Field(False, description="Whether thread is publicly shared")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "thread_id": "3d4e5f6g-7h8i-9j0k-1l2m-3n4o5p6q7r8s",
            "workspace_id": "ws-abc123",
            "thread_index": 0,
            "current_status": "completed",
            "msg_type": "ptc",
            "title": "Tesla Stock Analysis",
            "platform": "market_view:TSLA",
            "is_shared": False,
            "created_at": "2025-10-15T10:30:00Z",
            "updated_at": "2025-10-15T14:45:00Z",
        },
    })


class WorkspaceThreadsListResponse(BaseModel):
    """Response model for listing threads in a workspace."""
    threads: List[WorkspaceThreadListItem] = Field(
        default_factory=list, description="List of threads"
    )
    total: int = Field(0, description="Total number of threads")
    limit: int = Field(..., description="Page limit")
    offset: int = Field(..., description="Page offset")


# ==================== Debug Response Models ====================

class ResponseFullDetail(BaseModel):
    """Complete response details for debug inspection."""
    response_id: str = Field(..., description="Response ID")
    thread_id: str = Field(..., description="Thread ID")
    turn_index: int = Field(..., description="Turn index")
    status: str = Field(..., description="Response status")
    interrupt_reason: Optional[str] = Field(None, description="Interrupt reason")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Response metadata")
    warnings: List[str] = Field(default_factory=list, description="Warning messages")
    errors: List[str] = Field(default_factory=list, description="Error messages")
    execution_time: float = Field(0.0, description="Execution time in seconds")
    created_at: datetime = Field(..., description="Response timestamp")


# ==================== Messages API Response Models ====================

class MessageQuery(BaseModel):
    """Query information for messages endpoint."""
    query_id: str = Field(..., description="Query ID")
    content: str = Field(..., description="Query content")
    type: str = Field(..., description="Query type (initial, resume_feedback)")
    feedback_action: Optional[str] = Field(None, description="Feedback action if applicable")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Query metadata")
    created_at: datetime = Field(..., description="Query timestamp")


class MessageResponse(BaseModel):
    """Response information for messages endpoint."""
    response_id: str = Field(..., description="Response ID")
    status: str = Field(..., description="Response status (completed, interrupted, error, timeout)")
    interrupt_reason: Optional[str] = Field(None, description="Interrupt reason if applicable")
    execution_time: float = Field(0.0, description="Execution time in seconds")
    warnings: List[str] = Field(default_factory=list, description="Warning messages")
    errors: List[str] = Field(default_factory=list, description="Error messages")
    created_at: datetime = Field(..., description="Response timestamp")


class ConversationMessage(BaseModel):
    """A single message pair in a conversation/thread (query + response)."""
    turn_index: int = Field(..., description="Turn index within thread (0-based)")
    thread_id: str = Field(..., description="Thread ID (internal reference)")
    thread_index: int = Field(..., description="Thread index within workspace (0-based)")
    query: MessageQuery = Field(..., description="Query details")
    response: Optional[MessageResponse] = Field(None, description="Response details (may be null if pending)")


class WorkspaceMessagesResponse(BaseModel):
    """Response model for getting all messages in a workspace."""
    workspace_id: str = Field(..., description="Workspace ID")
    user_id: str = Field(..., description="User ID")
    name: Optional[str] = Field(None, description="Workspace name")
    messages: List[ConversationMessage] = Field(default_factory=list, description="All messages chronologically")
    total_messages: int = Field(0, description="Total message count")
    has_more: bool = Field(False, description="Whether more messages are available")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "workspace_id": "ws-abc123",
            "user_id": "user123",
            "name": "Code Analysis Project",
            "messages": [
                {
                    "turn_index": 0,
                    "thread_id": "thread-1",
                    "thread_index": 0,
                    "query": {
                        "query_id": "query-1",
                        "content": "Analyze the codebase",
                        "type": "initial",
                        "created_at": "2025-10-15T21:03:43Z",
                    },
                    "response": {
                        "response_id": "resp-1",
                        "status": "completed",
                        "execution_time": 123.45,
                        "created_at": "2025-10-15T21:05:47Z",
                    },
                }
            ],
            "total_messages": 1,
            "has_more": False,
            "created_at": "2025-10-15T21:03:43Z",
            "updated_at": "2025-10-15T21:05:47Z",
        },
    })


# ==================== Thread Management Request/Response Models ====================

class ThreadUpdateRequest(BaseModel):
    """Request model for updating a thread."""
    title: Optional[str] = Field(None, max_length=255, description="New thread title")

    model_config = ConfigDict(json_schema_extra={
        "example": {"title": "Tesla Stock Analysis"},
    })


class ThreadDeleteResponse(BaseModel):
    """Response model for deleting a thread."""
    success: bool = Field(..., description="Whether deletion was successful")
    thread_id: str = Field(..., description="Deleted thread ID")
    message: str = Field(..., description="Status message")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "success": True,
            "thread_id": "3d4e5f6g-7h8i-9j0k-1l2m-3n4o5p6q7r8s",
            "message": "Thread deleted successfully",
        },
    })


# ==================== Thread Sharing Models ====================

class SharePermissions(BaseModel):
    """Configurable permissions for a shared thread."""
    allow_files: bool = Field(False, description="Allow browsing and reading workspace files")
    allow_download: bool = Field(False, description="Allow downloading raw workspace files")

    @model_validator(mode="after")
    def download_requires_files(self):
        if self.allow_download and not self.allow_files:
            self.allow_files = True
        return self


class ThreadShareRequest(BaseModel):
    """Request model for toggling thread sharing."""
    is_shared: bool = Field(..., description="Enable or disable public sharing")
    permissions: Optional[SharePermissions] = Field(
        None, description="Share permissions (only provided fields are updated)"
    )


class ThreadShareResponse(BaseModel):
    """Response model for thread share status."""
    is_shared: bool = Field(..., description="Whether the thread is publicly shared")
    share_token: Optional[str] = Field(None, description="Opaque share token")
    share_url: Optional[str] = Field(None, description="Full share URL")
    permissions: SharePermissions = Field(
        default_factory=SharePermissions, description="Current share permissions"
    )


# ==================== Feedback Models ====================

class FeedbackRequest(BaseModel):
    """Request model for submitting feedback on a response."""
    turn_index: int = Field(..., description="Turn index of the response to rate")
    rating: str = Field(..., pattern=r"^(thumbs_up|thumbs_down)$", description="Rating: thumbs_up or thumbs_down")
    issue_categories: Optional[List[str]] = Field(None, description="Issue categories for thumbs_down")
    comment: Optional[str] = Field(None, description="Optional free-text comment")
    consent_human_review: bool = Field(False, description="Whether user consents to anonymous human review")


class FeedbackResponse(BaseModel):
    """Response model for feedback."""
    conversation_feedback_id: str
    turn_index: int
    rating: str
    issue_categories: Optional[List[str]] = None
    comment: Optional[str] = None
    consent_human_review: bool = False
    review_status: Optional[str] = None
    created_at: str

