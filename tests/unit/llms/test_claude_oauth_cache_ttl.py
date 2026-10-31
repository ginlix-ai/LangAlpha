"""Every cache marker the subscription client sends asks for an hour.

The markers come from middleware every Anthropic route shares; the OAuth client
alone applies the hour, to the whole request at once, since Anthropic refuses a
request whose lifetimes are out of order. The thread-level check, and the API-key
route's unchanged bytes, are in ``test_provider_isolation``.
"""

from __future__ import annotations

from src.llms.extension.anthropic_oauth import _with_cache_ttl

_FIVE = {"type": "ephemeral"}
_HOUR = {"type": "ephemeral", "ttl": "1h"}


def _payload(marker: dict) -> dict:
    return {
        "cache_control": marker,
        "system": [{"type": "text", "text": "prompt", "cache_control": marker}],
        "tools": [
            {
                "name": "probe",
                "input_schema": {"properties": {"cache_control": {"type": "object"}}},
                "input_examples": [{"cache_control": dict(marker)}],
                "cache_control": marker,
            },
            {"type": "mcp_toolset", "mcp_server_name": "probe", "configs": {"cache_control": {"enabled": True}}, "cache_control": marker},
        ],
        "output_config": {"format": {"type": "json_schema", "schema": {"properties": {"cache_control": {"type": "object"}}}}},
        "messages": [
            {"role": "user", "content": "plain"},
            {"role": "user", "content": [{"type": "document", "source": {"type": "content", "content": [{"type": "text", "text": "page", "cache_control": marker}]}}]},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "name": "probe", "input": {"cache_control": dict(marker)}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok", "cache_control": marker}]},
        ],
    }


def test_every_marker_asks_for_an_hour_wherever_it_sits():
    sent = _with_cache_ttl(_payload(_FIVE))

    assert sent["cache_control"] == _HOUR
    assert sent["system"][0]["cache_control"] == _HOUR
    assert sent["tools"][0]["cache_control"] == _HOUR
    assert sent["tools"][1]["cache_control"] == _HOUR
    assert sent["messages"][1]["content"][0]["source"]["content"][0]["cache_control"] == _HOUR
    assert sent["messages"][3]["content"][0]["cache_control"] == _HOUR


def test_a_tool_schema_and_the_models_own_arguments_are_left_as_they_are():
    sent = _with_cache_ttl(_payload(_FIVE))

    assert sent["tools"][0]["input_schema"] == {"properties": {"cache_control": {"type": "object"}}}
    assert sent["tools"][0]["input_examples"] == [{"cache_control": _FIVE}]
    assert sent["tools"][1]["configs"] == {"cache_control": {"enabled": True}}
    assert sent["output_config"]["format"]["schema"] == {"properties": {"cache_control": {"type": "object"}}}
    assert sent["messages"][2]["content"][0]["input"] == {"cache_control": _FIVE}


def test_the_request_it_was_built_from_is_not_changed():
    marker = dict(_FIVE)
    payload = _payload(marker)

    sent = _with_cache_ttl(payload)

    assert sent["system"][0]["cache_control"] == _HOUR
    assert marker == _FIVE
    assert payload["system"][0]["cache_control"] is marker
