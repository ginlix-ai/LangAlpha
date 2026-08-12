"""
Pydantic models for workspace thread management API.

This module defines response models for workspace thread endpoints that work with
the query-response schema (workspaces, thread, query, response).
"""

from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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
        description="Surface the thread originated from: 'web', 'market_view:<SYMBOL>', "
        "or channel name ('telegram', 'slack', 'discord', 'feishu'). NULL for "
        "pre-tracking rows and system-initiated threads.",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Thread annotations. Known keys: 'origin' — "
        "{type: 'agent'|'automation'|'system', id?} recording who initiated "
        "the thread; absent origin = user-initiated.",
    )
    is_shared: bool = Field(False, description="Whether thread is publicly shared")
    is_pinned: bool = Field(
        False, description="Pinned threads sort first within their workspace"
    )
    archived_at: Optional[datetime] = Field(
        None,
        description="When the thread was archived; NULL = active. Archived "
        "threads are excluded from listings unless explicitly requested.",
    )
    turn_count: Optional[int] = Field(
        None,
        description="Number of turns (user queries) in the thread; only the "
        "list queries compute it — other constructor sites omit it.",
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    # Lifecycle enrichment (thread-lifecycle v6) — derived from the latest
    # attempt on the run ledger. All optional: only the list queries join the
    # lifecycle LATERAL; other constructor sites (rename, external-id) omit
    # them. `current_status` above stays for back-compat.
    run_status: Optional[str] = Field(
        None,
        description="Latest run's PUBLIC status (idle|queued|running|stopping|"
        "recovering|completed|interrupted|failed|cancelled); 'idle' when the "
        "thread has no runs.",
    )
    interrupt_reason: Optional[str] = Field(
        None, description="Latest attempt's interrupt reason when interrupted"
    )
    latest_run_seq: Optional[int] = Field(
        None, description="Latest attempt's monotonic run sequence (0 = none)"
    )
    latest_run_id: Optional[str] = Field(
        None, description="Latest attempt's run id — the causal /seen token"
    )
    last_seen_run_seq: Optional[int] = Field(
        None, description="Durable seen cursor (thread row)"
    )
    unseen: Optional[bool] = Field(
        None,
        description="Latest run settled (completed/failed/cancelled) and the "
        "user hasn't seen it. interrupted is NOT unseen — needs-input is its "
        "own indicator.",
    )
    run_started_at: Optional[datetime] = Field(
        None, description="Latest attempt's start time (start, not settle)"
    )

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

class ThreadCreateRequest(BaseModel):
    """Request model for POST /api/v1/threads (pre-creation with auto-title).

    ``workspace_id`` is required for ptc mode; flash mode resolves the shared
    flash workspace when omitted — mirroring the message-send path so a
    pre-created thread is indistinguishable from an ensure_thread_exists row.
    """
    workspace_id: Optional[str] = Field(None, description="Target workspace")
    first_query: str = Field(
        ..., min_length=1,
        description="The user's first message; stamped as the initial title "
        "and fed to title generation.",
    )

    @field_validator("first_query")
    @classmethod
    def _clip_first_query(cls, v: str) -> str:
        # Clients send the full first message; the server consumes at most
        # 4000 chars of it (title-prompt clip; the title stamp is [:255]).
        # Truncate rather than reject so long pastes don't 422 the fast door.
        return v[:4000]
    agent_mode: Literal["flash", "ptc"] = Field("ptc")
    # Same contract as ChatRequest.platform (see models/chat.py for the format
    # rationale) — pre-created MarketView threads must carry their tag since
    # ensure_thread_exists never updates platform on an existing row.
    platform: Optional[str] = Field(
        default=None,
        pattern=r"^[a-z_]+(:[A-Z0-9][A-Z0-9.-]*)?$",
        max_length=50,
        description="Origin/platform identifier, e.g. 'web', 'market_view:AAPL'.",
    )
    timezone: Optional[str] = Field(
        default=None,
        max_length=64,
        description="IANA timezone identifier (e.g., 'America/New_York'); "
        "localizes relative-time resolution during title generation.",
    )


class ThreadCreateResponse(BaseModel):
    """201 body of POST /api/v1/threads — the created row.

    ``title`` is the creation-time stamp (the raw first query). The generated
    title arrives later on the lifecycle feed; it is never awaited here.
    """
    thread_id: str
    workspace_id: str
    thread_index: int
    title: Optional[str] = None
    msg_type: str
    created_at: datetime
    updated_at: datetime


class ThreadUpdateRequest(BaseModel):
    """Request model for updating a thread (title, pin, archive).

    Only fields explicitly present in the request are applied — the endpoint
    reads ``model_fields_set``, so ``{"is_pinned": true}`` cannot clear the
    title as a side effect.
    """
    title: Optional[str] = Field(None, max_length=255, description="New thread title")
    is_pinned: Optional[bool] = Field(
        None, description="Pin (true) or unpin (false) the thread"
    )
    archived: Optional[bool] = Field(
        None, description="Archive (true) or unarchive (false) the thread"
    )

    model_config = ConfigDict(json_schema_extra={
        "example": {"title": "Tesla Stock Analysis"},
    })


class ThreadExternalIdRequest(BaseModel):
    """Request model for stamping a channel identity onto a thread.

    Both fields are required and non-empty — the stamp API never clears
    ``external_id`` back to NULL (the partial-unique dedup index relies on it).
    """
    platform: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Origin platform to stamp (e.g. 'telegram', 'slack', "
        "'discord', 'feishu').",
    )
    external_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="External channel thread identifier to stamp "
        "(e.g. 'chat_id:topic_id').",
    )

    model_config = ConfigDict(json_schema_extra={
        "example": {"platform": "telegram", "external_id": "chat_id:topic_id"},
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

