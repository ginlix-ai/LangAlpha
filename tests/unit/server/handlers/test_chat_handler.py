"""
Tests for chat handler helpers.

Covers:
- HITL response serialization (serialize_hitl_response_map, summarize_hitl_response_map)
- _append_to_last_user_message helper

Error classification (classify_error) is tested in test_chat_common.py.
LLM config resolution is tested in test_resolve_llm_config.py.
"""

import copy

import pytest


# ---------------------------------------------------------------------------
# HITL response serialization
# ---------------------------------------------------------------------------


class TestSerializeHitlResponseMap:
    """Tests for serialize_hitl_response_map."""

    def test_serialize_pydantic_model(self):
        from src.server.models.chat import (
            HITLDecision,
            HITLResponse,
            serialize_hitl_response_map,
        )

        response = HITLResponse(
            decisions=[HITLDecision(type="approve", message=None)]
        )
        result = serialize_hitl_response_map({"int-1": response})
        assert isinstance(result["int-1"], dict)
        assert result["int-1"]["decisions"][0]["type"] == "approve"

    def test_serialize_dict_input(self):
        from src.server.models.chat import serialize_hitl_response_map

        raw = {"decisions": [{"type": "approve", "message": None}]}
        result = serialize_hitl_response_map({"int-1": raw})
        assert result["int-1"]["decisions"][0]["type"] == "approve"

    def test_serialize_rejection_formats_message(self):
        from src.server.models.chat import serialize_hitl_response_map

        raw = {"decisions": [{"type": "reject", "message": "Too expensive"}]}
        result = serialize_hitl_response_map({"int-1": raw})
        msg = result["int-1"]["decisions"][0]["message"]
        assert "rejected" in msg.lower()
        assert "Too expensive" in msg

    def test_serialize_rejection_without_feedback(self):
        from src.server.models.chat import serialize_hitl_response_map

        raw = {"decisions": [{"type": "reject", "message": None}]}
        result = serialize_hitl_response_map({"int-1": raw})
        msg = result["int-1"]["decisions"][0]["message"]
        assert "rejected" in msg.lower()
        assert "No specific feedback" in msg

    def test_serialize_unsupported_type_raises(self):
        from src.server.models.chat import serialize_hitl_response_map

        with pytest.raises(TypeError, match="Unsupported HITL response type"):
            serialize_hitl_response_map({"int-1": 42})

    def test_serialize_does_not_mutate_original(self):
        from src.server.models.chat import serialize_hitl_response_map

        original = {"decisions": [{"type": "reject", "message": "fix it"}]}
        original_copy = copy.deepcopy(original)
        serialize_hitl_response_map({"int-1": original})
        assert original == original_copy


# ---------------------------------------------------------------------------
# HITL response summarization
# ---------------------------------------------------------------------------


class TestSummarizeHitlResponseMap:
    """Tests for summarize_hitl_response_map."""

    def test_all_approve_returns_approved(self):
        from src.server.models.chat import summarize_hitl_response_map

        raw = {"decisions": [{"type": "approve"}, {"type": "approve"}]}
        result = summarize_hitl_response_map({"int-1": raw})
        assert result["feedback_action"] == "APPROVED"
        assert result["content"] == ""

    def test_any_reject_returns_declined(self):
        from src.server.models.chat import summarize_hitl_response_map

        raw = {
            "decisions": [
                {"type": "approve"},
                {"type": "reject", "message": "too slow"},
            ]
        }
        result = summarize_hitl_response_map({"int-1": raw})
        assert result["feedback_action"] == "DECLINED"
        assert "too slow" in result["content"]

    def test_interrupt_ids_are_collected(self):
        from src.server.models.chat import summarize_hitl_response_map

        result = summarize_hitl_response_map({
            "int-1": {"decisions": [{"type": "approve"}]},
            "int-2": {"decisions": [{"type": "approve"}]},
        })
        assert set(result["interrupt_ids"]) == {"int-1", "int-2"}

    def test_pydantic_model_input(self):
        from src.server.models.chat import (
            HITLDecision,
            HITLResponse,
            summarize_hitl_response_map,
        )

        response = HITLResponse(
            decisions=[HITLDecision(type="reject", message="bad plan")]
        )
        result = summarize_hitl_response_map({"int-1": response})
        assert result["feedback_action"] == "DECLINED"
        assert "bad plan" in result["content"]


# ---------------------------------------------------------------------------
# _append_to_last_user_message
# ---------------------------------------------------------------------------


class TestAppendToLastUserMessage:
    """Tests for the _append_to_last_user_message helper."""

    def test_appends_to_string_content(self):
        from src.server.handlers.chat._common import _append_to_last_user_message

        messages = [{"role": "user", "content": "hello"}]
        _append_to_last_user_message(messages, " world")
        assert messages[0]["content"] == "hello world"

    def test_appends_to_list_content(self):
        from src.server.handlers.chat._common import _append_to_last_user_message

        messages = [
            {"role": "user", "content": [{"type": "text", "text": "hello"}]}
        ]
        _append_to_last_user_message(messages, " extra")
        assert len(messages[0]["content"]) == 2
        assert messages[0]["content"][1] == {"type": "text", "text": " extra"}

    def test_no_op_when_empty_messages(self):
        from src.server.handlers.chat._common import _append_to_last_user_message

        messages = []
        _append_to_last_user_message(messages, "text")
        assert messages == []

    def test_no_op_when_last_is_not_user(self):
        from src.server.handlers.chat._common import _append_to_last_user_message

        messages = [{"role": "assistant", "content": "hi"}]
        _append_to_last_user_message(messages, " appended")
        assert messages[0]["content"] == "hi"


