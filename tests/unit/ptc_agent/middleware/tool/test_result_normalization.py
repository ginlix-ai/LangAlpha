"""Tests for ToolResultNormalizationMiddleware.

This middleware is the single chokepoint where every tool result becomes a
NUL-free string. It serves two concerns:

1. Type coercion — non-string results become strings so LLM APIs that require
   string ToolMessage content don't error out.
2. NUL stripping — a `\\x00` byte in tool output would break Postgres
   TEXT/JSONB binds, making affected threads permanently unresumable.

Both behaviors are pinned here.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.tool.result_normalization import (
    ToolResultNormalizationMiddleware,
)


@pytest.fixture
def mw() -> ToolResultNormalizationMiddleware:
    return ToolResultNormalizationMiddleware()


# ---------------------------------------------------------------------------
# _normalize_result — type coercion
# ---------------------------------------------------------------------------


class TestTypeCoercion:
    def test_string_passthrough(self, mw):
        assert mw._normalize_result("hello") == "hello"

    def test_none_becomes_empty_json_array(self, mw):
        assert mw._normalize_result(None) == "[]"

    def test_dict_becomes_json(self, mw):
        out = mw._normalize_result({"k": "v"})
        assert json.loads(out) == {"k": "v"}

    def test_list_becomes_json(self, mw):
        out = mw._normalize_result([1, "two", {"three": 3}])
        assert json.loads(out) == [1, "two", {"three": 3}]

    def test_other_type_via_str(self, mw):
        assert mw._normalize_result(42) == "42"

    def test_unicode_preserved(self, mw):
        # ensure_ascii=False — Chinese characters etc. survive intact.
        out = mw._normalize_result({"msg": "你好"})
        assert "你好" in out


# ---------------------------------------------------------------------------
# _normalize_result — NUL stripping
# ---------------------------------------------------------------------------


class TestNulStripping:
    def test_strips_nul_from_string(self, mw, caplog):
        out = mw._normalize_result("ok\x00bad")
        assert out == "okbad"
        # Warning fires so logs make NUL occurrences observable.
        assert any("Stripped NUL" in r.message for r in caplog.records)

    def test_strips_nul_inside_dict_value(self, mw):
        # Dict path goes through json.dumps first; the resulting string
        # contains the JSON `\\u0000` escape which we then strip.
        out = mw._normalize_result({"content": "stdout\x00"})
        # Either form is unacceptable in Postgres JSONB.
        assert "\\u0000" not in out
        assert "\x00" not in out

    def test_strips_nul_inside_list(self, mw):
        out = mw._normalize_result(["a\x00", "b"])
        assert "\\u0000" not in out
        assert "\x00" not in out

    def test_clean_string_does_not_emit_warning(self, mw, caplog):
        out = mw._normalize_result("clean output")
        assert out == "clean output"
        assert not any("Stripped NUL" in r.message for r in caplog.records)

    def test_multiple_nuls_all_stripped(self, mw):
        assert mw._normalize_result("\x00a\x00b\x00c\x00") == "abc"

    def test_literal_unicode_escape_in_string_preserved(self, mw, caplog):
        # On the string-passthrough path, the six-char sequence `\u0000` is
        # just text — Postgres TEXT accepts it. Stripping would mangle docs
        # or LLM output that explains Unicode escapes literally.
        text = "the unicode escape \\u0000 represents NUL"
        assert mw._normalize_result(text) == text
        assert not any("Stripped NUL" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# wrap_tool_call / awrap_tool_call — integration with ToolMessage
# ---------------------------------------------------------------------------


class TestWrapToolCall:
    def test_sync_path_strips_nul_in_toolmessage(self, mw):
        msg = ToolMessage(content="hello\x00world", tool_call_id="call-1")
        handler = MagicMock(return_value=msg)
        out = mw.wrap_tool_call(request=MagicMock(), handler=handler)
        assert out is msg
        assert msg.content == "helloworld"

    def test_sync_path_passthrough_for_non_toolmessage(self, mw):
        # If something other than a ToolMessage comes back, leave it alone.
        sentinel = object()
        handler = MagicMock(return_value=sentinel)
        assert mw.wrap_tool_call(request=MagicMock(), handler=handler) is sentinel

    @pytest.mark.asyncio
    async def test_async_path_strips_nul_in_toolmessage(self, mw):
        msg = ToolMessage(content="hello\x00world", tool_call_id="call-1")
        handler = AsyncMock(return_value=msg)
        out = await mw.awrap_tool_call(request=MagicMock(), handler=handler)
        assert out is msg
        assert msg.content == "helloworld"

    @pytest.mark.asyncio
    async def test_async_path_coerces_dict_then_strips(self, mw):
        # Tool returns a dict directly into ToolMessage.content (some tools do
        # this before LangChain wraps it). Middleware coerces to JSON string,
        # then strips the \\u0000 escape that the dict's NUL-bearing value
        # would have produced.
        msg = ToolMessage(content={"data": "stdout\x00"}, tool_call_id="call-2")
        handler = AsyncMock(return_value=msg)
        await mw.awrap_tool_call(request=MagicMock(), handler=handler)
        assert isinstance(msg.content, str)
        assert "\\u0000" not in msg.content
        assert "\x00" not in msg.content


# ---------------------------------------------------------------------------
# _normalize_result — content blocks
# ---------------------------------------------------------------------------


class TestContentBlocksSurviveAsBlocks:
    """A tool result carrying an attachment must not be serialized.

    Serializing it hands the model a wall of base64 as prose instead of an
    image it can look at, which is exactly what a live trace caught.
    """

    _IMAGE = [
        {"type": "text", "text": "Loading image: a.png\n[Viewing image]"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
    ]

    def test_a_block_list_passes_through_as_a_list(self, mw):
        out = mw._normalize_result(self._IMAGE)
        assert out == self._IMAGE

    def test_a_pdf_block_list_passes_through_as_a_list(self, mw):
        blocks = [
            {"type": "text", "text": "[Viewing document]"},
            {
                "type": "file",
                "file": {"filename": "a.pdf", "file_data": "data:application/pdf;base64,abc"},
            },
        ]
        assert mw._normalize_result(blocks) == blocks

    def test_an_unknown_block_type_is_still_serialized(self, mw):
        # The exemption is a fixed allowlist, so a list of arbitrary dicts that
        # merely happens to carry a "type" key still gets the coercion.
        out = mw._normalize_result([{"type": "row", "value": 1}])
        assert isinstance(out, str)
        assert json.loads(out) == [{"type": "row", "value": 1}]

    def test_a_mixed_list_is_still_serialized(self, mw):
        out = mw._normalize_result([{"type": "text", "text": "hi"}, "loose string"])
        assert isinstance(out, str)

    def test_an_empty_list_is_still_serialized(self, mw):
        assert mw._normalize_result([]) == "[]"

    def test_a_text_only_list_is_still_serialized(self, mw):
        """Text blocks alone are not an attachment payload.

        They read identically as a string, so exempting them would change the
        shape of any tool that answers with a list of typed dicts, for nothing.
        """
        out = mw._normalize_result([{"type": "text", "text": "hi"}])
        assert isinstance(out, str)

    def test_nuls_are_stripped_inside_blocks(self, mw):
        out = mw._normalize_result(
            [{"type": "text", "text": "ok\x00bad"}, *self._IMAGE[1:]]
        )
        assert out[0] == {"type": "text", "text": "okbad"}

    def test_the_blocks_handed_in_are_not_mutated(self, mw):
        blocks = [{"type": "text", "text": "ok\x00bad"}, *self._IMAGE[1:]]
        original = [dict(b) for b in blocks]
        mw._normalize_result(blocks)
        assert blocks == original

    @pytest.mark.asyncio
    async def test_the_tool_message_keeps_its_blocks_through_the_wrapper(self, mw):
        msg = ToolMessage(content=list(self._IMAGE), tool_call_id="call-1")
        handler = AsyncMock(return_value=msg)
        out = await mw.awrap_tool_call(request=MagicMock(), handler=handler)
        assert out is msg
        assert msg.content == self._IMAGE


class TestNulsInEveryPositionJsonReaches:
    """`json.dumps` writes an escaped NUL from places the value walk missed.

    A raw NUL in a TEXT bind and an escaped one in JSONB are the same outage:
    Postgres refuses the write and the thread stops being resumable. Stripping
    only dict *values* left two ways in, and both are shapes a sandbox tool
    plausibly returns.
    """

    def test_a_nul_in_a_dict_key(self, mw) -> None:
        out = mw._normalize_result({"a\x00b": "v"})
        assert "\\u0000" not in out
        assert json.loads(out) == {"ab": "v"}

    def test_a_nul_inside_a_tuple(self, mw) -> None:
        # json.dumps writes a tuple as an array, so it reaches JSONB the same
        # way a list does while never matching an isinstance list check.
        out = mw._normalize_result({"k": ("x\x00y",)})
        assert "\\u0000" not in out
        assert json.loads(out) == {"k": ["xy"]}

    def test_a_nul_in_a_nested_key(self, mw) -> None:
        out = mw._normalize_result({"outer": [{"in\x00ner": 1}]})
        assert "\\u0000" not in out
        assert json.loads(out) == {"outer": [{"inner": 1}]}

    def test_clean_input_is_returned_unchanged(self, mw) -> None:
        payload = {"k": ["v"], "n": 1}
        assert json.loads(mw._normalize_result(payload)) == payload

    def test_a_literal_escape_in_the_data_survives(self, mw) -> None:
        """The six characters are text, and the dump escapes the backslash.

        Cutting ``\\u0000`` out of the serialized string instead of out of the
        values matches that escaped backslash, drops the second half of the
        pair, and leaves JSON that no longer parses. Text carrying the literal
        is what a sandbox returns when it echoes a source file.
        """
        payload = {"example": "\\u0000"}
        out = mw._normalize_result(payload)
        assert json.loads(out) == payload

    def test_a_key_collision_does_not_look_like_no_change(self, mw) -> None:
        """Two keys merging into one is a change, and `zip` cannot see it.

        Cleaning ``a`` and ``a\\x00`` produces one key, so pairing the cleaned
        mapping against the original stops at the shorter of the two and never
        reaches the key that vanished. With equal values every pair it does
        reach is identical, the walk reported nothing changed, and the original
        went out with its NUL still in a key.
        """
        out = mw._normalize_result({"a": 1, "a\x00": 1})
        assert "\\u0000" not in out
        assert json.loads(out) == {"a": 1}

    def test_a_nul_bearing_tuple_key_still_serializes(self, mw) -> None:
        """Cleaning a key must not make it unhashable.

        A tuple key is already too exotic for ``json.dumps``, which raises and
        gets a readable ``str()`` of the result instead. Handing the rebuilt key
        back as a list raises out of the cleaner first, above that fallback, so
        an awkward-but-displayable result became a failed tool call.
        """
        out = mw._normalize_result({("a\x00",): 1})
        assert isinstance(out, str)
        assert "\x00" not in out and "a" in out


class TestSearchHitsAreNotAnAttachment:
    """A search result naming pictures is tool output, not a payload.

    A verbose Tavily or Bocha search answers with ``{"type": "image",
    "image_url": "<url>", ...}`` records, and on a query returning pictures and
    no pages that is the entire tool result. Recognising an attachment by its
    block type alone classified that list as one, so normalization was skipped
    and the provider received blocks no route can read in place of the search
    JSON the agent asked for.
    """

    SEARCH_HITS = [
        {"type": "image", "image_url": "https://e.test/a.png", "image_description": "a"},
        {"type": "image", "image_url": "https://e.test/b.png", "image_description": "b"},
    ]

    def test_an_images_only_result_is_serialized(self, mw) -> None:
        out = mw._normalize_result(self.SEARCH_HITS)
        assert isinstance(out, str)
        assert json.loads(out) == self.SEARCH_HITS

    def test_a_real_attachment_still_passes_through(self, mw) -> None:
        blocks = [
            {"type": "text", "text": "[Viewing image]"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
        ]
        assert mw._normalize_result(blocks) == blocks

    def test_an_envelope_with_no_url_is_not_an_attachment(self, mw) -> None:
        """The same rule from the other side: the URL is the payload.

        ``image_url`` is the one block type whose bytes live in a nested object,
        so accepting any object there accepted a block naming an image without
        carrying one, and the provider is handed an image it cannot fetch.
        """
        out = mw._normalize_result([{"type": "image_url", "image_url": {}}])
        assert isinstance(out, str)

    def test_every_wire_shape_of_a_real_attachment_passes_through(self, mw) -> None:
        for blocks in (
            [{"type": "file", "base64": "AAAA", "mime_type": "application/pdf"}],
            [{"type": "image", "source": {"type": "base64", "data": "AAAA"}}],
            [{"type": "document", "source": {"type": "base64", "data": "AAAA"}}],
            [{"type": "image", "base64": "AAAA", "mime_type": "image/png"}],
        ):
            assert mw._normalize_result(blocks) == blocks, blocks
