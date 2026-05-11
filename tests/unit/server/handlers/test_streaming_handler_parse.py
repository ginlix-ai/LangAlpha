"""Tests for `_parse_tool_args` in `src/server/handlers/streaming_handler.py`.

Frontier models occasionally emit tool-call argument JSON with an unescaped
quote inside a long string value, causing strict `json.loads` to raise
`Expecting ',' delimiter` and the call to be silently dropped. The fallback
through `json_repair` recovers the dominant case.
"""

from src.server.handlers.streaming_handler import _parse_tool_args


class TestParseToolArgs:
    def test_strict_valid_json(self):
        parsed = _parse_tool_args(
            '{"file_path": "results/note.md", "content": "hello"}'
        )
        assert parsed == {"file_path": "results/note.md", "content": "hello"}

    def test_unescaped_quote_inside_content_repairs(self):
        # Dominant failure shape: literal `"` inside the content field that
        # the model didn't escape. Strict json.loads raises
        # "Expecting ',' delimiter".
        parsed = _parse_tool_args(
            '{"file_path": "x.md", "content": "He said "hi" to me."}'
        )
        assert parsed is not None
        assert parsed.get("file_path") == "x.md"
        assert isinstance(parsed.get("content"), str)

    def test_chinese_content_with_embedded_quote_repairs(self):
        # CJK content with an embedded unescaped quote — exercises multi-byte
        # code-point handling in json_repair.
        raw = (
            '{"file_path":"results/analysis.md",'
            '"content":"# 分析报告\\n标的"AMZN.US"表现良好。"}'
        )
        parsed = _parse_tool_args(raw)
        assert parsed is not None
        assert parsed.get("file_path", "").endswith(".md")
        assert isinstance(parsed.get("content"), str)

    def test_empty_string_returns_none(self):
        assert _parse_tool_args("") is None

    def test_strict_valid_non_dict_is_rejected(self):
        # Bare strings/arrays/scalars parse via strict JSON but are not valid
        # tool-call arg shapes (LangChain contract is a dict).
        for raw in ('"just a string"', '[1, 2, 3]', 'null', '42'):
            assert _parse_tool_args(raw) is None, f"unexpected parse for {raw!r}"

    def test_repair_returning_non_dict_is_rejected(self):
        # json_repair will turn unstructured garbage into nested lists like
        # `[[["..."]]]`. Reject these so downstream tool dispatch never sees
        # a non-dict args shape.
        assert _parse_tool_args("{{{not json") is None
