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
    rejection_feedback,
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


class TestRejectionFeedback:
    """A tool that words its own result reads back only the user's words."""

    def test_round_trips_the_users_words(self):
        assert rejection_feedback(_format_rejection_message(" use my NVDA one ")) == (
            "use my NVDA one"
        )

    def test_no_feedback_is_empty_not_the_placeholder(self):
        assert rejection_feedback(_format_rejection_message(None)) == ""
        assert rejection_feedback(None) == ""


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

    def test_an_order_reject_carries_its_reason_once(self):
        # The client sends an order verdict in its positional slot and again
        # under order_decisions; the reason must reach the stored message once.
        raw = {
            "decisions": [{"type": "reject", "message": "too rich"}],
            "order_decisions": {
                "11111111-2222-4333-8444-555555555555": {
                    "type": "reject", "message": "too rich",
                },
            },
        }
        summary = summarize_hitl_response_map({"i1": raw})
        assert summary["feedback_action"] == "DECLINED"
        assert summary["content"] == "too rich"

    def test_an_order_reject_alone_declines_the_turn(self):
        raw = {
            "decisions": [{"type": "approve"}],
            "order_decisions": {
                "11111111-2222-4333-8444-555555555555": {"type": "reject"},
            },
        }
        assert summarize_hitl_response_map({"i1": raw})["feedback_action"] == "DECLINED"


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
        assert req.messages == []
        assert req.hitl_response is None

    def test_retired_plan_mode_field_is_ignored(self):
        # Channel gateways still send it; a 422 would drop their turns.
        req = ChatRequest(plan_mode=True)
        assert not hasattr(req, "plan_mode")

    def test_retired_agent_mode_field_is_ignored(self):
        # Cached web clients and older CLI builds still send it; a 422 would
        # drop their turns.
        req = ChatRequest(agent_mode="flash", workspace_id="ws-1")
        assert not hasattr(req, "agent_mode")
        assert req.workspace_id == "ws-1"

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

    @pytest.mark.parametrize(
        "platform",
        [
            "web",
            "market_view",
            "telegram",
            "market_view:AAPL",  # US, letter-first
            "market_view:BRK.B",  # US, dotted class share
            "market_view:BRK-B",  # US, hyphenated class share
            "market_view:002851.SZ",  # Shenzhen A-share (digit-first)
            "market_view:600519.SH",  # Shanghai A-share
            "market_view:300750.SZ",  # ChiNext
            "market_view:688981.SH",  # STAR board
            "market_view:430047.BJ",  # Beijing exchange
            "market_view:0700.HK",  # Hong Kong (Tencent), digit-first
        ],
    )
    def test_platform_accepts_real_market_symbols(self, platform):
        # Regression: the suffix must not require a leading [A-Z] — every CN
        # A-share and HK ticker is digit-first, which the old pattern rejected
        # and 422'd the whole market-view chat send.
        assert ChatRequest(platform=platform).platform == platform

    @pytest.mark.parametrize(
        "platform",
        [
            "Web",  # prefix must be lowercase
            "market view:AAPL",  # space not allowed in prefix
            "market_view:",  # empty suffix
            "market_view:.SZ",  # suffix must start alnum, not `.`
            "market_view:aapl",  # suffix is uppercased upstream; lowercase invalid
        ],
    )
    def test_platform_rejects_malformed(self, platform):
        with pytest.raises(ValidationError):
            ChatRequest(platform=platform)

    def test_surface_rules_defaults_to_none(self):
        assert ChatRequest().surface_rules is None

    def test_surface_rules_keeps_the_text_it_was_sent(self):
        rules = "Surface slack: plain text, one message per answer."
        assert ChatRequest(surface_rules=rules).surface_rules == rules

    @pytest.mark.parametrize("sent", ["", "   ", "\n\t "])
    def test_blank_surface_rules_are_no_rules(self, sent):
        """A gateway with nothing to say and one that drops the key have to
        land in the same state, or the model gets an empty paragraph."""
        assert ChatRequest(surface_rules=sent).surface_rules is None

    def test_surface_rules_are_stripped(self):
        assert ChatRequest(surface_rules="  keep this  ").surface_rules == "keep this"

    def test_surface_rules_are_capped(self):
        """The text rides in every thread that changes it, so an unbounded
        field is an unbounded prompt."""
        assert ChatRequest(surface_rules="x" * 2000).surface_rules == "x" * 2000

        with pytest.raises(ValidationError):
            ChatRequest(surface_rules="x" * 2001)


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

    def test_input_id_is_optional(self):
        assert SubagentMessageRequest(content="Do X").input_id is None

    def test_input_id_accepts_a_client_token(self):
        req = SubagentMessageRequest(
            content="Do X", input_id="1b4e28ba-2fa1-41d2-883f-0016d3cca427"
        )
        assert req.input_id == "1b4e28ba-2fa1-41d2-883f-0016d3cca427"

    @pytest.mark.parametrize("input_id", ["", "a b", "../queue", "x" * 65, "id\n"])
    def test_input_id_rejects_anything_but_a_short_safe_token(self, input_id):
        with pytest.raises(ValidationError):
            SubagentMessageRequest(content="Do X", input_id=input_id)


# ---------------------------------------------------------------------------
# Request keys
# ---------------------------------------------------------------------------


class TestRequestKey:
    """The key lands in a UUID column, so the request model has to refuse what
    the column would, as a validation error rather than a failed insert."""

    @pytest.mark.parametrize("model", ["send", "retry"])
    @pytest.mark.parametrize("key", ["not-a-uuid", "", "4f121907-7851-438c-93ea"])
    def test_refuses_a_key_that_is_not_a_uuid(self, model, key):
        with pytest.raises(ValidationError):
            _request(model, key)

    @pytest.mark.parametrize("model", ["send", "retry"])
    def test_keeps_a_uuid_in_its_canonical_form(self, model):
        key = "4F121907-7851-438C-93EA-023DD02A6914"
        assert _request(model, key).request_key == key.lower()

    @pytest.mark.parametrize("model", ["send", "retry"])
    def test_a_key_is_optional(self, model):
        assert _request(model, None).request_key is None


def _request(model: str, key):
    from src.server.models.workflow import RetryRequest

    if model == "send":
        return ChatRequest(messages=[], request_key=key)
    return RetryRequest(workspace_id="ws", request_key=key)
