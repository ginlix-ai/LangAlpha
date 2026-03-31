"""Tests for ChatCodexOpenAI system message → instructions promotion."""

import pytest
from langchain_core.messages import HumanMessage, SystemMessage

from src.llms.extension.codex import ChatCodexOpenAI


def _make_llm(**overrides):
    defaults = {
        "model": "gpt-5.4",
        "api_key": "fake",
        "output_version": "responses/v1",
        "store": False,
        "model_kwargs": {"instructions": "static placeholder"},
    }
    defaults.update(overrides)
    return ChatCodexOpenAI(**defaults)


class TestSystemToInstructions:
    """Codex API rejects role:'system' in input — must promote to instructions."""

    def test_string_system_message_promoted(self):
        llm = _make_llm()
        messages = [
            SystemMessage(content="You are a research agent."),
            HumanMessage(content="Hello"),
        ]
        payload = llm._get_request_payload(messages)

        assert payload["instructions"] == "You are a research agent.\n\nstatic placeholder"
        roles = [i["role"] for i in payload["input"] if isinstance(i, dict)]
        assert "system" not in roles

    def test_multiblock_system_message_promoted(self):
        llm = _make_llm()
        messages = [
            SystemMessage(
                content=[
                    {"type": "text", "text": "Part one."},
                    {"type": "text", "text": "Part two."},
                ]
            ),
            HumanMessage(content="Hello"),
        ]
        payload = llm._get_request_payload(messages)

        assert payload["instructions"] == "Part one.\n\nPart two.\n\nstatic placeholder"
        roles = [i["role"] for i in payload["input"] if isinstance(i, dict)]
        assert "system" not in roles

    def test_no_system_message_preserves_existing_instructions(self):
        llm = _make_llm()
        messages = [HumanMessage(content="Hello")]
        payload = llm._get_request_payload(messages)

        assert payload["instructions"] == "static placeholder"

    def test_no_system_message_no_model_kwargs_no_instructions(self):
        llm = _make_llm(model_kwargs={})
        messages = [HumanMessage(content="Hello")]
        payload = llm._get_request_payload(messages)

        assert "instructions" not in payload

    def test_system_merges_with_existing_instructions(self):
        llm = _make_llm()
        messages = [
            SystemMessage(content="Dynamic prompt"),
            HumanMessage(content="Hi"),
        ]
        payload = llm._get_request_payload(messages)

        assert payload["instructions"] == "Dynamic prompt\n\nstatic placeholder"


class TestStatelessIdSanitization:
    """Existing behavior: reasoning item IDs stripped for store=false."""

    def test_reasoning_id_stripped(self):
        llm = _make_llm()
        messages = [HumanMessage(content="Hello")]
        payload = llm._get_request_payload(messages)

        # Manually inject reasoning item to test sanitization
        payload["input"].append(
            {"type": "reasoning", "id": "rs_abc123", "content": []}
        )
        from src.llms.extension.codex import _sanitize_input_for_stateless

        sanitized = _sanitize_input_for_stateless(payload["input"])
        reasoning = [i for i in sanitized if i.get("type") == "reasoning"][0]
        assert "id" not in reasoning
