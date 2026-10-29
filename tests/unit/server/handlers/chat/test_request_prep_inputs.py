"""
Tests for src/server/handlers/chat/request_prep.py: reading the request's
messages, HITL answers, and context into the turn.

Covers:
- process_hitl_response: the PreparedHitl record, various HITL scenarios
- normalize_request_messages: dict conversion, multimodal, empty, ids
- serialize_context_metadata: context serialization + slash fallback
- prepare_skill_contexts: skill contexts and slash-command detection
- inject_inline_reminders: reminders appended to the user tail
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph

from ptc_agent.agent.state import DeltaAgentState
from src.server.models.additional_context import SkillContext

PREP = "src.server.handlers.chat.request_prep"


# ---------------------------------------------------------------------------
# process_hitl_response
# ---------------------------------------------------------------------------


class TestProcessHitlResponse:
    """Every scenario runs through the wire type, because the handler now
    normalizes to it once and reads plain attributes below that."""

    def _make_request(self, hitl_response):
        req = MagicMock()
        req.hitl_response = hitl_response
        return req

    def _prepared(self, hitl_response, **summary):
        from src.server.handlers.chat.request_prep import process_hitl_response

        with patch(f"{PREP}.summarize_hitl_response_map", return_value=summary):
            return process_hitl_response(self._make_request(hitl_response))

    def test_approve_with_message(self):
        prepared = self._prepared(
            {"int-1": {"decisions": [{"type": "approve", "message": "yes please"}]}},
            feedback_action="QUESTION_ANSWERED",
            content="approved: yes please",
            interrupt_ids=["int-1"],
        )
        assert prepared.feedback_action == "QUESTION_ANSWERED"
        assert prepared.query_content == "approved: yes please"
        assert prepared.metadata["hitl_interrupt_ids"] == ["int-1"]
        assert prepared.metadata["hitl_answers"]["int-1"] == "yes please"

    def test_reject_without_message(self):
        prepared = self._prepared(
            {"int-1": {"decisions": [{"type": "reject", "message": ""}]}},
            feedback_action="QUESTION_SKIPPED",
            content="rejected",
            interrupt_ids=["int-1"],
        )
        assert prepared.feedback_action == "QUESTION_SKIPPED"
        assert prepared.metadata["hitl_answers"]["int-1"] is None

    def test_a_validated_model_and_the_raw_dict_agree(self):
        """The wire type is ``Dict[str, HITLResponse]``, but a caller that built
        the request itself hands over what the client sent."""
        from src.server.models.chat import HITLResponse

        raw = {"decisions": [{"type": "approve", "message": "ok"}]}
        summary = dict(
            feedback_action="QUESTION_ANSWERED", content="ok", interrupt_ids=["int-1"]
        )
        assert self._prepared({"int-1": raw}, **summary) == self._prepared(
            {"int-1": HITLResponse.model_validate(raw)}, **summary
        )

    def test_multiple_interrupts(self):
        prepared = self._prepared(
            {
                "int-1": {"decisions": [{"type": "approve", "message": "answer 1"}]},
                "int-2": {"decisions": [{"type": "reject", "message": ""}]},
            },
            feedback_action="QUESTION_ANSWERED",
            content="mixed",
            interrupt_ids=["int-1", "int-2"],
        )
        assert prepared.feedback_action == "QUESTION_ANSWERED"
        assert prepared.metadata["hitl_answers"] == {
            "int-1": "answer 1",
            "int-2": None,
        }

    def test_empty_decisions(self):
        prepared = self._prepared(
            {"int-1": {"decisions": []}},
            feedback_action="QUESTION_SKIPPED",
            content="",
            interrupt_ids=["int-1"],
        )
        assert prepared.feedback_action == "QUESTION_SKIPPED"
        # Nothing to file beyond the ids, so nothing is filed.
        assert prepared.metadata == {"hitl_interrupt_ids": ["int-1"]}

    def test_batch_records_a_decision_per_action_request(self):
        """A mixed batch keeps every verdict, which hitl_answers cannot."""
        prepared = self._prepared(
            {
                "int-1": {
                    "decisions": [
                        {"type": "approve", "message": None},
                        {"type": "reject", "message": "not this one"},
                    ]
                }
            },
            feedback_action="QUESTION_SKIPPED",
            content="not this one",
            interrupt_ids=["int-1"],
        )
        assert prepared.metadata["hitl_decisions"]["int-1"] == [
            {"type": "approve", "message": None},
            {"type": "reject", "message": "not this one"},
        ]
        # The collapsed record cannot tell this from rejecting both.
        assert "hitl_answers" not in prepared.metadata

    def test_an_order_verdict_is_filed_under_its_attempt(self):
        """An order authorizes one execution of one call, so its answer is
        keyed by attempt id and never by a position in a list."""
        prepared = self._prepared(
            {
                "int-1": {
                    "decisions": [],
                    "order_decisions": {
                        "att-1": {"type": "reject", "message": "too big"}
                    },
                }
            },
            feedback_action="DECLINED",
            content="too big",
            interrupt_ids=["int-1"],
        )
        assert prepared.metadata["order_decisions"] == {
            "att-1": {"type": "reject", "message": "too big"}
        }


# ---------------------------------------------------------------------------
# normalize_request_messages
# ---------------------------------------------------------------------------


class TestNormalizeRequestMessages:
    def _make_request(self, messages):
        req = MagicMock()
        req.messages = messages
        return req

    def test_string_content(self):
        from src.server.handlers.chat.request_prep import normalize_request_messages

        msg = MagicMock()
        msg.role = "user"
        msg.content = "hello"
        result = normalize_request_messages(self._make_request([msg]))
        assert result == [{"id": result[0]["id"], "role": "user", "content": "hello"}]

    def test_every_message_carries_its_own_id(self):
        from src.server.handlers.chat.request_prep import normalize_request_messages

        item = MagicMock(type="text", text="look")
        messages = [
            MagicMock(role="user", content="hello"),
            MagicMock(role="user", content=[item]),
        ]
        request = self._make_request(messages)
        ids = [m["id"] for _ in range(2) for m in normalize_request_messages(request)]
        assert len(set(ids)) == 4
        assert all(uuid.UUID(i) for i in ids)

    @pytest.mark.asyncio
    async def test_a_regenerate_brings_the_turns_message_back_under_its_id(self):
        """A regenerate reruns the input its turn's checkpoint stored, so the
        id must travel in that input for the rerun to be the same message."""
        from src.server.handlers.chat.request_prep import normalize_request_messages

        def reply(state):
            return {"messages": [AIMessage("a", id=str(uuid.uuid4()))]}

        graph = (
            StateGraph(DeltaAgentState)
            .add_node("reply", reply)
            .add_edge(START, "reply")
            .compile(checkpointer=InMemorySaver())
        )
        thread = {"configurable": {"thread_id": "regenerate-keeps-id"}}
        request = self._make_request([MagicMock(role="user", content="q0")])
        await graph.ainvoke({"messages": normalize_request_messages(request)}, thread)
        sent = (await graph.aget_state(thread)).values["messages"][0].id
        turn = [
            s
            async for s in graph.aget_state_history(thread)
            if s.metadata.get("source") == "input"
        ][0]

        await graph.ainvoke(None, turn.config)

        head = (await graph.aget_state(thread)).values["messages"]
        assert [m.id for m in head if m.type == "human"] == [sent]

    def test_list_content_text(self):
        from src.server.handlers.chat.request_prep import normalize_request_messages

        item = MagicMock()
        item.type = "text"
        item.text = "hello"
        msg = MagicMock()
        msg.role = "user"
        msg.content = [item]
        result = normalize_request_messages(self._make_request([msg]))
        assert result[0]["content"] == [{"type": "text", "text": "hello"}]

    def test_list_content_image(self):
        from src.server.handlers.chat.request_prep import normalize_request_messages

        item = MagicMock()
        item.type = "image"
        item.text = None
        item.image_url = "https://example.com/img.png"
        msg = MagicMock()
        msg.role = "user"
        msg.content = [item]
        result = normalize_request_messages(self._make_request([msg]))
        assert result[0]["content"] == [
            {"type": "image_url", "image_url": {"url": "https://example.com/img.png"}}
        ]

    def test_empty_messages(self):
        from src.server.handlers.chat.request_prep import normalize_request_messages

        result = normalize_request_messages(self._make_request([]))
        assert result == []

    def test_multiple_messages(self):
        from src.server.handlers.chat.request_prep import normalize_request_messages

        m1 = MagicMock(role="user", content="hello")
        m2 = MagicMock(role="assistant", content="hi there")
        m3 = MagicMock(role="user", content="thanks")
        result = normalize_request_messages(self._make_request([m1, m2, m3]))
        assert len(result) == 3
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"
        assert result[2]["content"] == "thanks"


# ---------------------------------------------------------------------------
# serialize_context_metadata
# ---------------------------------------------------------------------------


class TestSerializeContextMetadata:
    def _make_request(self, additional_context=None, hitl_response=None):
        req = MagicMock()
        req.additional_context = additional_context
        req.hitl_response = hitl_response
        return req

    def test_serializes_skills_and_directives(self):
        from src.server.handlers.chat.request_prep import serialize_context_metadata

        skill_ctx = MagicMock(type="skills")
        skill_ctx.name = "research"
        directive_ctx = MagicMock(type="directive", content="be concise")
        other_ctx = MagicMock(type="multimodal")

        req = self._make_request(
            additional_context=[skill_ctx, directive_ctx, other_ctx]
        )
        metadata = {}
        serialize_context_metadata(req, metadata, "hello")

        assert metadata["additional_context"] == [
            {"type": "skills", "name": "research"},
            {"type": "directive", "content": "be concise"},
        ]

    def test_slash_command_fallback(self):
        from src.server.handlers.chat.request_prep import serialize_context_metadata

        req = self._make_request(additional_context=None)
        metadata = {}

        mock_skill = MagicMock(name="research")
        with patch(
            f"{PREP}.detect_slash_commands",
            return_value=("hello", [mock_skill]),
        ):
            serialize_context_metadata(req, metadata, "hello")

        assert "additional_context" in metadata
        assert metadata["additional_context"][0]["type"] == "skills"

    def test_no_slash_commands_found(self):
        from src.server.handlers.chat.request_prep import serialize_context_metadata

        req = self._make_request(additional_context=None)
        metadata = {}

        with patch(
            f"{PREP}.detect_slash_commands",
            return_value=("hello", []),
        ):
            serialize_context_metadata(req, metadata, "hello")

        assert "additional_context" not in metadata

    def test_hitl_response_skips_slash_commands(self):
        from src.server.handlers.chat.request_prep import serialize_context_metadata

        req = self._make_request(additional_context=None, hitl_response={"int-1": {}})
        metadata = {}

        with patch(f"{PREP}.detect_slash_commands") as mock_detect:
            serialize_context_metadata(req, metadata, "hello")

        mock_detect.assert_not_called()

    def test_existing_additional_context_prevents_fallback(self):
        from src.server.handlers.chat.request_prep import serialize_context_metadata

        skill_ctx = MagicMock(type="skills")
        skill_ctx.name = "research"
        req = self._make_request(additional_context=[skill_ctx])
        metadata = {}

        with patch(f"{PREP}.detect_slash_commands") as mock_detect:
            serialize_context_metadata(req, metadata, "hello")

        # Should not call detect_slash_commands since additional_context was serialized
        mock_detect.assert_not_called()


# ---------------------------------------------------------------------------
# prepare_skill_contexts
# ---------------------------------------------------------------------------


class TestPrepareSkillContexts:
    def test_no_skills_returns_empty(self):
        from src.server.handlers.chat.request_prep import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [{"role": "user", "content": "hello"}]

        with patch(f"{PREP}.parse_skill_contexts", return_value=[]):
            result = prepare_skill_contexts(messages, request)

        assert result == []

    def test_skill_from_additional_context(self):
        from src.server.handlers.chat.request_prep import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = [MagicMock(type="skills")]
        request.hitl_response = None
        messages = [{"role": "user", "content": "hello"}]

        ctx = SkillContext(type="skills", name="research", instruction="find news")
        with patch(f"{PREP}.parse_skill_contexts", return_value=[ctx]):
            result = prepare_skill_contexts(messages, request)

        # Returns plain dicts to thread through config; no body injected here.
        assert result == [{"name": "research", "instruction": "find news"}]
        assert messages[0]["content"] == "hello"

    def test_slash_command_detection_fallback_strips_prefix(self):
        from src.server.handlers.chat.request_prep import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [{"role": "user", "content": "/research market analysis"}]

        ctx = SkillContext(type="skills", name="research")
        with (
            patch(f"{PREP}.parse_skill_contexts", return_value=[]),
            patch(
                f"{PREP}.detect_slash_commands",
                return_value=("market analysis", [ctx]),
            ),
        ):
            result = prepare_skill_contexts(messages, request)

        assert result == [{"name": "research", "instruction": None}]
        # The /command prefix is stripped in place; no skill body appended.
        assert messages[0]["content"] == "market analysis"

    def test_hitl_response_skips_slash_detection(self):
        from src.server.handlers.chat.request_prep import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = {"int-1": {}}
        messages = [{"role": "user", "content": "/research something"}]

        with (
            patch(f"{PREP}.parse_skill_contexts", return_value=[]),
            patch(f"{PREP}.detect_slash_commands") as mock_detect,
        ):
            result = prepare_skill_contexts(messages, request)

        mock_detect.assert_not_called()
        assert result == []

    def test_slash_detection_on_multimodal_list_content(self):
        """A supported attachment rewrites content into a block list; the slash
        command must still activate, be stripped in its own text block, and leave
        the attachment blocks intact."""
        from src.server.handlers.chat.request_prep import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        image_block = {"type": "image_url", "image_url": {"url": "data:image/png;x"}}
        text_block = {"type": "text", "text": "/research market analysis"}
        messages = [{"role": "user", "content": [image_block, text_block]}]

        ctx = SkillContext(type="skills", name="research")
        with (
            patch(f"{PREP}.parse_skill_contexts", return_value=[]),
            patch(
                f"{PREP}.detect_slash_commands",
                return_value=("market analysis", [ctx]),
            ),
        ):
            result = prepare_skill_contexts(messages, request)

        assert result == [{"name": "research", "instruction": None}]
        # Prefix stripped in the text block; the image block survives untouched.
        assert messages[0]["content"][0] is image_block
        assert messages[0]["content"][1]["text"] == "market analysis"

    def test_list_content_without_slash_block_skips_detection(self):
        """List content whose text blocks don't lead with `/` activates nothing."""
        from src.server.handlers.chat.request_prep import prepare_skill_contexts

        request = MagicMock()
        request.additional_context = None
        request.hitl_response = None
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "[Attached image: chart.png]"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;x"}},
                    {"type": "text", "text": "what do you see"},
                ],
            }
        ]

        with (
            patch(f"{PREP}.parse_skill_contexts", return_value=[]),
            patch(f"{PREP}.detect_slash_commands") as mock_detect,
        ):
            result = prepare_skill_contexts(messages, request)

        mock_detect.assert_not_called()
        assert result == []


# ---------------------------------------------------------------------------
# inject_inline_reminders
# ---------------------------------------------------------------------------


class TestInjectInlineReminders:
    def _inject(self, messages, reminders):
        from src.server.handlers.chat.request_prep import inject_inline_reminders

        return inject_inline_reminders(messages, reminders)

    def test_none_messages_is_noop(self):
        # HITL-resume / checkpoint-replay path: no target list, must not raise.
        self._inject(None, ["\nreminder"])

    def test_empty_messages_is_noop(self):
        msgs = []
        self._inject(msgs, ["\nreminder"])
        assert msgs == []

    def test_appends_present_reminders_in_order(self):
        msgs = [{"role": "user", "content": "hi"}]
        self._inject(msgs, ["\nA", "\nB"])
        assert msgs[-1]["content"] == "hi\nA\nB"

    def test_skips_absent_reminders(self):
        msgs = [{"role": "user", "content": "hi"}]
        self._inject(msgs, [None, "", "\nX"])
        assert msgs[-1]["content"] == "hi\nX"

    def test_does_not_append_to_non_user_tail(self):
        msgs = [{"role": "assistant", "content": "x"}]
        self._inject(msgs, ["\nA"])
        assert msgs[-1]["content"] == "x"
