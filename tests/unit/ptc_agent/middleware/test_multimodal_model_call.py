import pytest
from langchain_core.messages import AIMessage, HumanMessage

from ptc_agent.agent.middleware.file_operations.multimodal import (
    _strip_unsupported_content_blocks,
)


class TestStripUnsupportedContentBlocks:
    def test_vision_model_passes_through(self):
        """Vision model (has_image=True, has_pdf=True): messages returned unchanged."""
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
            AIMessage(content="I see an image"),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=True)
        assert result is msgs  # exact same object, no copy

    def test_text_only_strips_image_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        content = result[0].content
        assert len(content) == 2
        assert content[0] == {"type": "text", "text": "Look at this"}
        assert content[1]["type"] == "text"
        assert "not visible" in content[1]["text"]

    def test_text_only_strips_pdf_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "file", "base64": "abc", "mime_type": "application/pdf", "filename": "doc.pdf"},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        content = result[0].content
        assert content[0]["type"] == "text"
        assert "PDF" in content[0]["text"]

    def test_mixed_content_preserves_text_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this chart"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=True)
        content = result[0].content
        assert any(b.get("text") == "Look at this chart" for b in content)

    def test_string_content_unchanged(self):
        msgs = [HumanMessage(content="hello"), AIMessage(content="hi")]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is msgs  # no list content, no changes
        assert result[0].content == "hello"

    def test_image_supported_pdf_not(self):
        msgs = [
            HumanMessage(content=[
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
            HumanMessage(content=[
                {"type": "file", "base64": "xyz", "mime_type": "application/pdf", "filename": "doc.pdf"},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=False)
        # Image preserved
        assert result[0].content[0]["type"] == "image_url"
        # PDF stripped
        assert result[1].content[0]["type"] == "text"
        assert "PDF" in result[1].content[0]["text"]

    def test_original_messages_not_mutated(self):
        original_content = [
            {"type": "text", "text": "test"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
        ]
        msgs = [HumanMessage(content=original_content.copy())]
        _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        # Original message content should be unchanged
        assert msgs[0].content[1]["type"] == "image_url"

    def test_no_visual_blocks_passes_through(self):
        """Messages with only text blocks pass through unchanged."""
        msgs = [
            HumanMessage(content=[{"type": "text", "text": "hello"}]),
            AIMessage(content="response"),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is msgs
