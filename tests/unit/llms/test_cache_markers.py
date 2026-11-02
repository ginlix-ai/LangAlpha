"""A compaction call places its cache markers by scope; every other call goes out as built.

A rebuild on an expired cache reads the history once and the call after it
replaces that history with the summary, so its request marks the last tool
alone: what the next call, and every thread with the same tools, reads back.
"""

from __future__ import annotations

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage

from src.llms.cache_markers import cache_scope, scoped
from src.llms.extension.anthropic_oauth import ChatAnthropicOAuth

_MARK = {"type": "ephemeral", "ttl": "5m"}


def _payload() -> dict:
    return {
        "cache_control": dict(_MARK),
        "system": [{"type": "text", "text": "prompt", "cache_control": dict(_MARK)}, {"type": "text", "text": "baseline", "cache_control": {"type": "ephemeral"}}],
        "tools": [
            {"name": "first", "input_schema": {"properties": {"cache_control": {"type": "object"}}}},
            {"name": "last", "input_schema": {"type": "object"}, "cache_control": dict(_MARK)},
        ],
        "messages": [
            {"role": "user", "content": [{"type": "document", "source": {"type": "content", "content": [{"type": "text", "text": "page", "cache_control": dict(_MARK)}]}}]},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "name": "first", "input": {"cache_control": dict(_MARK)}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok", "cache_control": {"type": "ephemeral"}}]},
        ],
    }


def _placed(sent: dict) -> dict:
    """Where the payload's markers sit, by what they mark."""
    return {
        "top": "cache_control" in sent,
        "system": ["cache_control" in block for block in sent["system"]],
        "tools": ["cache_control" in tool for tool in sent.get("tools") or []],
        "document": "cache_control" in sent["messages"][0]["content"][0]["source"]["content"][0],
        "tool_result": "cache_control" in sent["messages"][2]["content"][0],
    }


def test_outside_a_scope_the_payload_goes_out_as_built():
    payload = _payload()

    assert scoped(payload) is payload


def test_the_last_tool_scope_keeps_the_tools_marker_alone():
    payload = _payload()

    with cache_scope("last_tool"):
        sent = scoped(payload)

    assert _placed(sent) == {
        "top": False, "system": [False, False], "tools": [False, True], "document": False, "tool_result": False
    }
    assert sent["tools"][1]["cache_control"] == _MARK
    # A schema and the model's own arguments are not markers.
    assert sent["tools"][0]["input_schema"] == {"properties": {"cache_control": {"type": "object"}}}
    assert sent["messages"][1]["content"][0]["input"] == {"cache_control": _MARK}
    assert _payload() == payload


def test_a_last_tool_without_a_marker_gets_one_and_no_tools_means_none():
    payload = _payload()
    del payload["tools"][1]["cache_control"]

    with cache_scope("last_tool"):
        assert scoped(payload)["tools"][1]["cache_control"] == {"type": "ephemeral"}
        assert _placed(scoped({**payload, "tools": []})) == {
            "top": False, "system": [False, False], "tools": [], "document": False, "tool_result": False
        }


def test_a_deferred_tool_stays_unmarked_since_the_api_refuses_a_marker_there():
    """ToolSurface defers the stored tools a build lacks, after the ones it
    offers, and marks the last one it offers."""
    payload = _payload()
    payload["tools"].append({"name": "stored", "input_schema": {"type": "object"}, "defer_loading": True})

    with cache_scope("last_tool"):
        assert _placed(scoped(payload))["tools"] == [False, True, False]
        del payload["tools"][1]["cache_control"]
        assert _placed(scoped(payload))["tools"] == [False, True, False]
        assert _placed(scoped({**payload, "tools": [payload["tools"][2]]}))["tools"] == [False]


def test_the_system_and_tools_scope_keeps_their_markers_and_drops_the_rest():
    """A cold compaction that keeps the tail: the call after it sends the same
    system and tools again, never the history it summarizes."""
    payload = _payload()

    with cache_scope("system_and_tools"):
        sent = scoped(payload)

    assert _placed(sent) == {
        "top": False, "system": [True, True], "tools": [False, True], "document": False, "tool_result": False
    }
    assert sent["system"][0]["cache_control"] == _MARK and sent["tools"][1]["cache_control"] == _MARK
    assert sent["messages"][1]["content"][0]["input"] == {"cache_control": _MARK}
    assert _payload() == payload


def test_the_system_and_tools_scope_marks_the_last_tool_the_api_caches():
    payload = _payload()
    del payload["tools"][1]["cache_control"]
    payload["tools"].append({"name": "stored", "input_schema": {"type": "object"}, "defer_loading": True})

    with cache_scope("system_and_tools"):
        assert _placed(scoped(payload))["tools"] == [False, True, False]


def _request(client: ChatAnthropic) -> dict:
    tools = [{"name": "probe", "description": "Probe.", "input_schema": {"type": "object"}, "cache_control": dict(_MARK)}]
    return client._get_request_payload([HumanMessage("hi")], tools=tools, cache_control=dict(_MARK))


def test_each_anthropic_client_sends_the_scope_and_the_oauth_one_its_hour_after_it():
    api_key = ChatAnthropic(model="claude-opus-5-5", api_key="sk-ant-test")
    oauth = ChatAnthropicOAuth(model="claude-opus-5-5", api_key="sk-ant-oat-test")

    assert "cache_control" in _request(api_key)
    with cache_scope("last_tool"):
        sent, subscription = _request(api_key), _request(oauth)

    assert "cache_control" not in sent and sent["tools"][0]["cache_control"] == _MARK
    assert "cache_control" not in subscription
    assert subscription["tools"][0]["cache_control"] == {"type": "ephemeral", "ttl": "1h"}
