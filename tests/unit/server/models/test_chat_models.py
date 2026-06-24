"""Tests for chat Pydantic models and HITL utility functions.

Covers request/response models for the chat API (src/server/models/chat.py),
including HITL serialization and summarization helpers.
"""

import pytest
from pydantic import ValidationError

from src.server.models.chat import (
    ChatMessage,
    ChatRequest,
    ContentItem,
    HITLDecision,
    HITLResponse,
    SubagentMessageRequest,
    TTSRequest,
    _format_rejection_message,
    serialize_hitl_response_map,
    summarize_hitl_response_map,
)


# ---------------------------------------------------------------------------
# HITL Models
# ---------------------------------------------------------------------------


class TestHITLDecision:
    """HITLDecision model validation."""

    def test_approve(self):
        d = HITLDecision(type="approve")
        assert d.type == "approve"
        assert d.message is None

    def test_reject_with_message(self):
        d = HITLDecision(type="reject", message="Too risky")
        assert d.type == "reject"
        assert d.message == "Too risky"

    def test_invalid_type(self):
        with pytest.raises(ValidationError):
            HITLDecision(type="maybe")


class TestHITLResponse:
    """HITLResponse wrapping decisions."""

    def test_single_decision(self):
        resp = HITLResponse(decisions=[HITLDecision(type="approve")])
        assert len(resp.decisions) == 1

    def test_multiple_decisions(self):
        resp = HITLResponse(
            decisions=[
                HITLDecision(type="approve"),
                HITLDecision(type="reject", message="No"),
            ]
        )
        assert len(resp.decisions) == 2


# ---------------------------------------------------------------------------
# HITL utility functions
# ---------------------------------------------------------------------------


class TestFormatRejectionMessage:
    """_format_rejection_message helper."""

    def test_with_feedback(self):
        msg = _format_rejection_message("needs more detail")
        assert "needs more detail" in msg
        assert "rejected" in msg.lower()

    def test_without_feedback(self):
        msg = _format_rejection_message(None)
        assert "No specific feedback" in msg

    def test_blank_feedback(self):
        msg = _format_rejection_message("   ")
        assert "No specific feedback" in msg


class TestSerializeHitlResponseMap:
    """serialize_hitl_response_map converts models to dicts."""

    def test_pydantic_model(self):
        resp = HITLResponse(decisions=[HITLDecision(type="approve")])
        result = serialize_hitl_response_map({"int-1": resp})
        assert isinstance(result["int-1"], dict)
        assert result["int-1"]["decisions"][0]["type"] == "approve"

    def test_dict_input(self):
        raw = {"decisions": [{"type": "reject", "message": "bad"}]}
        result = serialize_hitl_response_map({"int-2": raw})
        assert "rejected" in result["int-2"]["decisions"][0]["message"].lower()

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError, match="Unsupported HITL response type"):
            serialize_hitl_response_map({"int-x": 42})


class TestSummarizeHitlResponseMap:
    """summarize_hitl_response_map aggregates approve/reject status."""

    def test_all_approved(self):
        resp = HITLResponse(decisions=[HITLDecision(type="approve")])
        summary = summarize_hitl_response_map({"i1": resp})
        assert summary["feedback_action"] == "APPROVED"
        assert summary["content"] == ""
        assert "i1" in summary["interrupt_ids"]

    def test_any_reject_means_declined(self):
        resp = HITLResponse(
            decisions=[
                HITLDecision(type="approve"),
                HITLDecision(type="reject", message="No"),
            ]
        )
        summary = summarize_hitl_response_map({"i1": resp})
        assert summary["feedback_action"] == "DECLINED"
        assert "No" in summary["content"]

    def test_dict_input(self):
        raw = {"decisions": [{"type": "reject", "message": "Nope"}]}
        summary = summarize_hitl_response_map({"i1": raw})
        assert summary["feedback_action"] == "DECLINED"
        assert "Nope" in summary["content"]

    def test_unsupported_decision_type_raises(self):
        raw = {"decisions": [123]}
        with pytest.raises(TypeError, match="Unsupported HITL decision type"):
            summarize_hitl_response_map({"i1": raw})


# ---------------------------------------------------------------------------
# Content / Message models
# ---------------------------------------------------------------------------


class TestContentItem:
    """ContentItem model."""

    def test_text_item(self):
        item = ContentItem(type="text", text="hello")
        assert item.type == "text"
        assert item.text == "hello"
        assert item.image_url is None

    def test_image_item(self):
        item = ContentItem(type="image", image_url="https://example.com/img.png")
        assert item.image_url == "https://example.com/img.png"

    def test_type_required(self):
        with pytest.raises(ValidationError):
            ContentItem(text="hello")


class TestChatMessage:
    """ChatMessage with string or list content."""

    def test_string_content(self):
        msg = ChatMessage(role="user", content="Hi")
        assert msg.content == "Hi"

    def test_list_content(self):
        items = [ContentItem(type="text", text="hello")]
        msg = ChatMessage(role="assistant", content=items)
        assert isinstance(msg.content, list)

    def test_valid_roles_accepted(self):
        # Roles that both convert_to_messages can build AND langgraph stamps.
        for role in ("user", "assistant", "system", "human", "ai"):
            assert ChatMessage(role=role, content="x").role == role

    def test_developer_role_rejected(self):
        """``developer`` converts fine (convert_to_messages -> SystemMessage) but is
        NOT in langgraph's ``_MESSAGE_ROLES``, so ``ensure_message_ids`` never stamps
        it. It would persist id-less and duplicate on the hard-stop checkpoint flush
        (a full-list ``aupdate_state`` write-back the non-minting reducer can't
        dedup). Reject at the boundary; clients send ``system`` for the same effect."""
        with pytest.raises(ValidationError, match="Unsupported message role"):
            ChatMessage(role="developer", content="x")

    def test_every_valid_role_is_stamped_by_langgraph(self):
        """The invariant behind the set: every accepted role MUST be in langgraph's
        ``_MESSAGE_ROLES`` (the set ``ensure_message_ids`` stamps at put_writes).
        An accepted-but-unstamped role persists id-less and duplicates on the
        hard-stop flush. Ties the validator to langgraph's actual stamping contract:
        re-adding an unstamped role (e.g. ``developer``) or a langgraph bump that
        narrows ``_MESSAGE_ROLES`` fails here instead of corrupting threads in prod."""
        from langgraph.pregel._messages import _MESSAGE_ROLES

        from src.server.models.chat import _VALID_MESSAGE_ROLES

        unstamped = _VALID_MESSAGE_ROLES - _MESSAGE_ROLES
        assert not unstamped, (
            f"roles accepted but NOT stamped by langgraph: {sorted(unstamped)} — "
            "each persists id-less and duplicates on the hard-stop flush"
        )

    def test_tool_and_function_roles_rejected(self):
        """``tool``/``function`` are accepted by convert_to_messages but map to
        ToolMessage/FunctionMessage, which require tool_call_id/name that
        ChatMessage cannot supply. A client POST of {"role":"tool"} would pass
        the model, then raise KeyError('tool_call_id') in the reducer — and under
        DeltaChannel the raw write is persisted before the reducer, so it
        re-raises on every reconstruction and bricks the thread. Reject at the
        boundary."""
        for role in ("tool", "function"):
            with pytest.raises(ValidationError, match="Unsupported message role"):
                ChatMessage(role=role, content="x")

    def test_unknown_role_rejected(self):
        """A client-controlled unknown role is rejected at the request boundary
        (422), so it is never persisted to the delta channel where it would
        re-raise in convert_to_messages on every reconstruction and brick the
        thread."""
        with pytest.raises(ValidationError, match="Unsupported message role"):
            ChatMessage(role="banana", content="hi")

    def test_role_is_case_sensitive(self):
        # langchain matches roles case-sensitively (msg dict 'role' used as-is),
        # so "User" would raise downstream — reject it cleanly here too.
        with pytest.raises(ValidationError):
            ChatMessage(role="User", content="hi")

    def test_chatrequest_with_bad_role_rejected(self):
        """End-to-end: the bad role is caught when the request body is parsed,
        before any handler / graph / checkpoint runs."""
        with pytest.raises(ValidationError):
            ChatRequest(messages=[{"role": "banana", "content": "hi"}])


# ---------------------------------------------------------------------------
# ChatRequest
# ---------------------------------------------------------------------------


class TestChatRequest:
    """ChatRequest with defaults and constraints."""

    def test_minimal(self):
        req = ChatRequest()
        assert req.agent_mode is None
        assert req.messages == []
        assert req.plan_mode is False
        assert req.hitl_response is None

    def test_agent_mode_validation(self):
        req = ChatRequest(agent_mode="flash")
        assert req.agent_mode == "flash"

    def test_invalid_agent_mode(self):
        with pytest.raises(ValidationError):
            ChatRequest(agent_mode="turbo")

    def test_reasoning_effort_values(self):
        for level in ("low", "medium", "high", "xhigh"):
            req = ChatRequest(reasoning_effort=level)
            assert req.reasoning_effort == level

    def test_invalid_reasoning_effort(self):
        with pytest.raises(ValidationError):
            ChatRequest(reasoning_effort="ultra")

    def test_fork_from_turn_ge_zero(self):
        req = ChatRequest(fork_from_turn=0)
        assert req.fork_from_turn == 0

        with pytest.raises(ValidationError):
            ChatRequest(fork_from_turn=-1)


# ---------------------------------------------------------------------------
# Utility request models
# ---------------------------------------------------------------------------


class TestTTSRequest:
    """TTSRequest defaults."""

    def test_defaults(self):
        req = TTSRequest(text="hello world")
        assert req.text == "hello world"
        assert req.speed_ratio == 1.0
        assert req.encoding == "mp3"

    def test_text_required(self):
        with pytest.raises(ValidationError):
            TTSRequest()


class TestSubagentMessageRequest:
    """SubagentMessageRequest validation."""

    def test_valid(self):
        req = SubagentMessageRequest(content="Do X")
        assert req.content == "Do X"

    def test_content_required(self):
        with pytest.raises(ValidationError):
            SubagentMessageRequest()
