"""Tests for conversation / thread management Pydantic models.

Covers models in src/server/models/conversation.py including thread list items,
messages, sharing, and feedback request/response.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.server.models.conversation import (
    ConversationMessage,
    FeedbackRequest,
    FeedbackResponse,
    MessageQuery,
    MessageResponse,
    ResponseFullDetail,
    SharePermissions,
    ThreadDeleteResponse,
    ThreadExternalIdRequest,
    ThreadShareRequest,
    ThreadShareResponse,
    ThreadUpdateRequest,
    WorkspaceMessagesResponse,
    WorkspaceThreadListItem,
    WorkspaceThreadsListResponse,
)


NOW = datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# WorkspaceThreadListItem
# ---------------------------------------------------------------------------


class TestWorkspaceThreadListItem:
    """Thread list item construction and defaults."""

    def test_valid_minimal(self):
        item = WorkspaceThreadListItem(
            thread_id="t-1",
            workspace_id="ws-1",
            thread_index=0,
            current_status="completed",
            created_at=NOW,
            updated_at=NOW,
        )
        assert item.is_shared is False
        assert item.title is None
        assert item.first_query_content is None
        assert item.msg_type is None

    def test_required_fields(self):
        with pytest.raises(ValidationError) as exc_info:
            WorkspaceThreadListItem(
                thread_id="t-1",
                current_status="running",
                created_at=NOW,
                updated_at=NOW,
            )
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("workspace_id",) for e in errors)


class TestWorkspaceThreadsListResponse:
    """Paginated thread list response."""

    def test_defaults(self):
        resp = WorkspaceThreadsListResponse(limit=20, offset=0)
        assert resp.threads == []
        assert resp.total == 0


# ---------------------------------------------------------------------------
# SharePermissions model_validator
# ---------------------------------------------------------------------------


class TestSharePermissions:
    """SharePermissions auto-corrects allow_download -> allow_files."""

    def test_defaults(self):
        perms = SharePermissions()
        assert perms.allow_files is False
        assert perms.allow_download is False

    def test_download_implies_files(self):
        perms = SharePermissions(allow_download=True, allow_files=False)
        assert perms.allow_files is True
        assert perms.allow_download is True

    def test_files_without_download(self):
        perms = SharePermissions(allow_files=True, allow_download=False)
        assert perms.allow_files is True
        assert perms.allow_download is False


# ---------------------------------------------------------------------------
# ThreadShareRequest / ThreadShareResponse
# ---------------------------------------------------------------------------


class TestThreadShareRequest:
    """ThreadShareRequest validation."""

    def test_enable_sharing(self):
        req = ThreadShareRequest(is_shared=True)
        assert req.is_shared is True
        assert req.permissions is None

    def test_with_permissions(self):
        req = ThreadShareRequest(
            is_shared=True,
            permissions=SharePermissions(allow_files=True),
        )
        assert req.permissions.allow_files is True


class TestThreadShareResponse:
    """ThreadShareResponse defaults."""

    def test_not_shared(self):
        resp = ThreadShareResponse(is_shared=False)
        assert resp.share_token is None
        assert resp.share_url is None
        assert resp.permissions.allow_files is False


# ---------------------------------------------------------------------------
# FeedbackRequest
# ---------------------------------------------------------------------------


class TestFeedbackRequest:
    """FeedbackRequest rating pattern validation."""

    def test_thumbs_up(self):
        req = FeedbackRequest(turn_index=0, rating="thumbs_up")
        assert req.rating == "thumbs_up"

    def test_thumbs_down(self):
        req = FeedbackRequest(turn_index=1, rating="thumbs_down")
        assert req.consent_human_review is False

    def test_invalid_rating(self):
        with pytest.raises(ValidationError):
            FeedbackRequest(turn_index=0, rating="neutral")


# ---------------------------------------------------------------------------
# ConversationMessage
# ---------------------------------------------------------------------------


class TestConversationMessage:
    """ConversationMessage with query and optional response."""

    def test_with_response(self):
        query = MessageQuery(
            query_id="q-1",
            content="Hello",
            type="initial",
            created_at=NOW,
        )
        response = MessageResponse(
            response_id="r-1",
            status="completed",
            created_at=NOW,
        )
        msg = ConversationMessage(
            turn_index=0,
            thread_id="t-1",
            thread_index=0,
            query=query,
            response=response,
        )
        assert msg.response.status == "completed"

    def test_without_response(self):
        query = MessageQuery(
            query_id="q-1",
            content="Hello",
            type="initial",
            created_at=NOW,
        )
        msg = ConversationMessage(
            turn_index=0,
            thread_id="t-1",
            thread_index=0,
            query=query,
        )
        assert msg.response is None


# ---------------------------------------------------------------------------
# WorkspaceMessagesResponse
# ---------------------------------------------------------------------------


class TestWorkspaceMessagesResponse:
    """WorkspaceMessagesResponse defaults."""

    def test_defaults(self):
        resp = WorkspaceMessagesResponse(
            workspace_id="ws-1",
            user_id="u-1",
            created_at=NOW,
            updated_at=NOW,
        )
        assert resp.messages == []
        assert resp.total_messages == 0
        assert resp.has_more is False
        assert resp.name is None


# ---------------------------------------------------------------------------
# ThreadUpdateRequest
# ---------------------------------------------------------------------------


class TestThreadUpdateRequest:
    """ThreadUpdateRequest title constraint (rename only)."""

    def test_valid_title(self):
        req = ThreadUpdateRequest(title="My Thread")
        assert req.title == "My Thread"

    def test_title_too_long(self):
        with pytest.raises(ValidationError):
            ThreadUpdateRequest(title="x" * 256)

    def test_empty_allowed(self):
        req = ThreadUpdateRequest()
        assert req.title is None


# ---------------------------------------------------------------------------
# ThreadExternalIdRequest
# ---------------------------------------------------------------------------


class TestThreadExternalIdRequest:
    """ThreadExternalIdRequest — both fields required and non-empty."""

    def test_valid(self):
        req = ThreadExternalIdRequest(platform="telegram", external_id="chat:42")
        assert req.platform == "telegram"
        assert req.external_id == "chat:42"

    def test_platform_required(self):
        with pytest.raises(ValidationError):
            ThreadExternalIdRequest(external_id="chat:42")

    def test_external_id_required(self):
        with pytest.raises(ValidationError):
            ThreadExternalIdRequest(platform="telegram")

    def test_empty_rejected(self):
        with pytest.raises(ValidationError):
            ThreadExternalIdRequest(platform="", external_id="")

    def test_platform_too_long(self):
        with pytest.raises(ValidationError):
            ThreadExternalIdRequest(platform="x" * 51, external_id="chat:42")

    def test_external_id_too_long(self):
        with pytest.raises(ValidationError):
            ThreadExternalIdRequest(platform="telegram", external_id="x" * 256)
