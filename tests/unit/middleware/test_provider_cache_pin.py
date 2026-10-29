"""Provider cache-breakpoint marker selection and tagging.

Moved here from the market-watch suite when the tail-envelope middleware took
over the pin: the logic is shared (``breakpoint_marker`` picks the wire form,
``tag_last_text_block`` applies it to a request-scoped copy) and it is easier
to pin directly than through a middleware that no longer owns it.
"""

from unittest.mock import MagicMock

import pytest
from langchain_core.messages import HumanMessage, ToolMessage

from ptc_agent.agent.middleware.provider_cache import (
    breakpoint_marker,
    tag_last_text_block,
)


def _anthropic_model():
    from langchain_anthropic import ChatAnthropic

    return ChatAnthropic(model="claude-sonnet-4-5", api_key="test-key")


def _openai_model(**kwargs):
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(model="gpt-5.6-sol", api_key="test-key", **kwargs)


def _pin(msg, model):
    marker = breakpoint_marker(model)
    if marker is None:
        return msg
    content = tag_last_text_block(msg.content, *marker)
    return msg if content is None else msg.model_copy(update={"content": content})


@pytest.fixture(autouse=True)
def _clean_openai_base_env(monkeypatch):
    """The endpoint gate reads these env fallbacks; isolate from the host env."""
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)


class TestMarkerSelection:
    def test_anthropic_always_gets_cache_control(self):
        assert breakpoint_marker(_anthropic_model()) == (
            "cache_control",
            {"type": "ephemeral"},
        )

    def test_openai_needs_optin_and_the_official_endpoint(self):
        opted_in = _openai_model(
            prompt_cache_options={"mode": "implicit"},
            base_url="https://api.openai.com/v1",
        )
        assert breakpoint_marker(opted_in) == (
            "prompt_cache_breakpoint",
            {"mode": "explicit"},
        )
        assert breakpoint_marker(_openai_model()) is None
        assert (
            breakpoint_marker(
                _openai_model(
                    prompt_cache_options={"mode": "implicit"},
                    base_url="https://proxy.example.com/v1",
                )
            )
            is None
        )

    def test_unknown_provider_gets_nothing(self):
        # Automatic prefix matchers must never see a foreign marker.
        assert breakpoint_marker(MagicMock()) is None


class TestPinning:
    def test_string_content_becomes_one_tagged_text_block(self):
        original = HumanMessage(content="What is NVDA doing?", id="h-1")

        pinned = _pin(original, _anthropic_model())

        assert pinned is not original
        assert pinned.content == [
            {
                "type": "text",
                "text": "What is NVDA doing?",
                "cache_control": {"type": "ephemeral"},
            }
        ]
        # The state message itself is never mutated.
        assert original.content == "What is NVDA doing?"

    def test_openai_marker_uses_its_own_wire_form(self):
        original = HumanMessage(content="What is NVDA doing?", id="h-1")

        pinned = _pin(
            original,
            _openai_model(
                prompt_cache_options={"mode": "implicit"},
                base_url="https://api.openai.com/v1",
            ),
        )

        assert pinned.content == [
            {
                "type": "text",
                "text": "What is NVDA doing?",
                "prompt_cache_breakpoint": {"mode": "explicit"},
            }
        ]

    def test_tags_the_last_text_block_of_list_content(self):
        tool = ToolMessage(
            content=[{"type": "text", "text": "part 1"}, {"type": "text", "text": "part 2"}],
            tool_call_id="tc-1",
            name="web_search",
            id="tm-1",
        )

        pinned = _pin(tool, _anthropic_model())

        assert pinned.content[0] == {"type": "text", "text": "part 1"}
        assert pinned.content[1] == {
            "type": "text",
            "text": "part 2",
            "cache_control": {"type": "ephemeral"},
        }
        assert "cache_control" not in tool.content[1]

    def test_skips_a_non_text_tail_block(self):
        # A message can end in a non-text block (an image attachment); the
        # marker lands on the last TEXT block instead, because some providers
        # reject cache markers on non-text blocks.
        image_block = {"type": "image", "source": {"type": "base64", "data": "xx"}}
        tool = ToolMessage(
            content=[{"type": "text", "text": "part 1"}, image_block],
            tool_call_id="tc-1",
            name="web_search",
            id="tm-1",
        )

        pinned = _pin(tool, _anthropic_model())

        assert pinned.content[0] == {
            "type": "text",
            "text": "part 1",
            "cache_control": {"type": "ephemeral"},
        }
        assert pinned.content[1] == image_block
        assert "cache_control" not in image_block

    def test_untaggable_message_is_returned_unchanged(self):
        # An empty body has no block that accepts the marker: degrade to no
        # breakpoint, never a malformed request.
        tool = ToolMessage(content="", tool_call_id="tc-1", name="web_search", id="tm-1")

        assert _pin(tool, _anthropic_model()) is tool
