"""Tests for ToolArgumentParsingMiddleware.

Both serialization directions matter:

* LLM stringifies a list/dict arg → middleware decodes back to the structure
  the tool's Pydantic schema declares.
* LLM passes a dict/list to a string-typed JSON field (Write.content,
  Edit.old_string/new_string) → middleware re-encodes so Pydantic doesn't
  reject the call with a ``string_type`` error.
"""

from __future__ import annotations

import json

from src.ptc_agent.agent.middleware.tool.argument_parsing import (
    ToolArgumentParsingMiddleware,
)


class TestStringDecoding:
    """Back-compat: stringified JSON for non-string-typed args is decoded."""

    def test_decodes_json_array_string(self):
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args({"items": '["a","b","c"]'}, tool_name="SomeTool")
        assert out == {"items": ["a", "b", "c"]}

    def test_decodes_json_object_string(self):
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args({"config": '{"k": 1}'}, tool_name="SomeTool")
        assert out == {"config": {"k": 1}}

    def test_passes_plain_string_through(self):
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args({"q": "hello"}, tool_name="SomeTool")
        assert out == {"q": "hello"}

    def test_malformed_json_left_as_string(self):
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args({"items": "[not, json"}, tool_name="SomeTool")
        assert out == {"items": "[not, json"}


class TestStringValuedArgsEncoding:
    """LLM-as-object → JSON string for the known string-typed fields."""

    def test_write_content_dict_is_encoded(self):
        mw = ToolArgumentParsingMiddleware()
        payload = {"__version__": "v1", "holdings": [{"symbol": "AAPL"}]}
        out = mw._parse_args(
            {"file_path": "/x.json", "content": payload}, tool_name="Write"
        )
        assert isinstance(out["content"], str)
        assert json.loads(out["content"]) == payload
        assert out["file_path"] == "/x.json"

    def test_write_content_list_is_encoded(self):
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args(
            {"file_path": "/x.json", "content": [{"a": 1}, {"b": 2}]},
            tool_name="Write",
        )
        assert json.loads(out["content"]) == [{"a": 1}, {"b": 2}]

    def test_edit_old_and_new_string_dicts_encoded(self):
        mw = ToolArgumentParsingMiddleware()
        old = {"items": []}
        new = {"items": [{"symbol": "MSFT"}]}
        out = mw._parse_args(
            {
                "file_path": "/x.json",
                "old_string": old,
                "new_string": new,
                "replace_all": False,
            },
            tool_name="Edit",
        )
        assert json.loads(out["old_string"]) == old
        assert json.loads(out["new_string"]) == new
        assert out["replace_all"] is False

    def test_write_content_already_string_is_preserved_not_decoded(self):
        """A correctly-stringified JSON payload must NOT be re-parsed into a
        dict — that's the exact bug the dict-encoding branch exists to avoid
        re-creating from the other direction."""
        mw = ToolArgumentParsingMiddleware()
        raw = '{"__version__": "v1", "holdings": []}'
        out = mw._parse_args(
            {"file_path": "/x.json", "content": raw}, tool_name="Write"
        )
        assert out["content"] == raw
        assert isinstance(out["content"], str)

    def test_edit_strings_with_jsonish_text_preserved(self):
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args(
            {
                "file_path": "/x.json",
                "old_string": '{"items": []}',
                "new_string": '{"items": [1]}',
                "replace_all": False,
            },
            tool_name="Edit",
        )
        assert out["old_string"] == '{"items": []}'
        assert out["new_string"] == '{"items": [1]}'

    def test_other_tools_with_dict_args_are_untouched(self):
        """No over-eager coercion: a dict arg on a tool not in the allowlist
        must pass through unchanged."""
        mw = ToolArgumentParsingMiddleware()
        out = mw._parse_args(
            {"payload": {"k": "v"}}, tool_name="SomeUnrelatedTool"
        )
        assert out == {"payload": {"k": "v"}}

    def test_unknown_tool_name_falls_through(self):
        mw = ToolArgumentParsingMiddleware()
        # content here is dict but tool isn't Write/Edit → no encoding
        out = mw._parse_args({"content": {"a": 1}}, tool_name=None)
        assert out == {"content": {"a": 1}}
