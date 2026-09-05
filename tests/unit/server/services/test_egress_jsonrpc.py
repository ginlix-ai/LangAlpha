"""Strict JSON-RPC canonicalization for the egress relay.

The policy check and the forwarded bytes must describe the same request, so
this suite pins both halves: what the parser accepts (and the exact bytes it
emits) and every shape a smuggling attempt could hide in.
"""

from __future__ import annotations

import gzip
import json
import zlib

import pytest

from src.server.services.egress.jsonrpc import (
    MAX_BODY_BYTES,
    CanonicalRequest,
    JsonRpcRejected,
    canonicalize_request,
)


def _body(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


TOOLS_CALL = {
    "jsonrpc": "2.0",
    "id": 7,
    "method": "tools/call",
    "params": {"name": "search_docs", "arguments": {"query": "quarterly filings"}},
}


# ---------------------------------------------------------------------------
# Accept
# ---------------------------------------------------------------------------


class TestAccepts:
    def test_tools_call_is_canonicalized(self):
        result = canonicalize_request(_body(TOOLS_CALL))

        assert isinstance(result, CanonicalRequest)
        assert result.method == "tools/call"
        assert result.tool_name == "search_docs"
        assert result.is_notification is False
        assert json.loads(result.body) == TOOLS_CALL

    def test_canonical_bytes_are_compact_and_utf8(self):
        spaced = b'{ "jsonrpc" : "2.0" ,\n  "id" : 1 , "method" : "tools/list" }'
        result = canonicalize_request(spaced)

        assert result.body == b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
        assert b" " not in result.body

    def test_non_ascii_arguments_survive_unescaped(self):
        payload = {
            "jsonrpc": "2.0",
            "id": "abc",
            "method": "tools/call",
            "params": {"name": "lookup", "arguments": {"q": "café — 中文"}},
        }
        result = canonicalize_request(json.dumps(payload).encode("utf-8"))

        assert "café — 中文".encode("utf-8") in result.body
        assert json.loads(result.body) == payload

    def test_notification_has_no_id(self):
        result = canonicalize_request(
            _body({"jsonrpc": "2.0", "method": "notifications/initialized"})
        )
        assert result.is_notification is True
        assert result.tool_name is None

    def test_explicit_null_id_still_counts_as_a_request(self):
        result = canonicalize_request(
            _body({"jsonrpc": "2.0", "id": None, "method": "tools/list"})
        )
        assert result.is_notification is False

    def test_tool_name_only_extracted_for_tools_call(self):
        result = canonicalize_request(
            _body(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "resources/read",
                    "params": {"name": "not-a-tool"},
                }
            )
        )
        assert result.method == "resources/read"
        assert result.tool_name is None

    def test_method_is_reported_from_the_body(self):
        """The relay's tool policy keys off THIS value, so it must come from
        the parsed body — the module takes no caller-declared method to trust
        instead (nothing in the request headers can override it)."""
        result = canonicalize_request(_body({**TOOLS_CALL, "method": "tools/list"}))
        assert result.method == "tools/list"
        assert result.tool_name is None

    def test_body_exactly_at_the_cap_is_accepted(self):
        filler_len = MAX_BODY_BYTES - len(
            _body({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "pad": ""})
        )
        raw = _body(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "pad": "p" * filler_len}
        )
        assert len(raw) == MAX_BODY_BYTES
        assert canonicalize_request(raw).method == "tools/list"


# ---------------------------------------------------------------------------
# Reject
# ---------------------------------------------------------------------------


class TestRejects:
    def test_rejection_is_a_value_error(self):
        assert issubclass(JsonRpcRejected, ValueError)

    def test_body_over_the_cap(self):
        raw = b"x" * (MAX_BODY_BYTES + 1)
        with pytest.raises(JsonRpcRejected, match="exceeds"):
            canonicalize_request(raw)

    def test_cap_is_configurable_per_call(self):
        raw = _body(TOOLS_CALL)
        with pytest.raises(JsonRpcRejected, match="exceeds 32 bytes"):
            canonicalize_request(raw, max_bytes=32)

    def test_oversize_is_refused_before_parsing(self):
        """The cap is a byte check, so an unparseable giant never reaches the
        JSON decoder."""
        with pytest.raises(JsonRpcRejected, match="exceeds"):
            canonicalize_request(b"{" * (MAX_BODY_BYTES + 1))

    def test_batch_array(self):
        with pytest.raises(JsonRpcRejected, match="batch"):
            canonicalize_request(_body([TOOLS_CALL, TOOLS_CALL]))

    def test_empty_array(self):
        with pytest.raises(JsonRpcRejected, match="batch"):
            canonicalize_request(b"[]")

    @pytest.mark.parametrize("raw", [b"5", b'"tools/call"', b"true", b"null"])
    def test_non_object_scalar(self, raw):
        with pytest.raises(JsonRpcRejected, match="single object"):
            canonicalize_request(raw)

    def test_duplicate_top_level_keys(self):
        raw = b'{"jsonrpc":"2.0","id":1,"method":"tools/list","method":"tools/call"}'
        with pytest.raises(JsonRpcRejected, match="duplicate key 'method'"):
            canonicalize_request(raw)

    def test_duplicate_keys_nested_in_params(self):
        raw = (
            b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
            b'"params":{"name":"allowed","name":"blocked"}}'
        )
        with pytest.raises(JsonRpcRejected, match="duplicate key 'name'"):
            canonicalize_request(raw)

    @pytest.mark.parametrize("raw", [b"", b"{", b"{'jsonrpc': '2.0'}", b"{,}"])
    def test_invalid_json(self, raw):
        with pytest.raises(JsonRpcRejected, match="not valid JSON"):
            canonicalize_request(raw)

    def test_trailing_content_after_the_object(self):
        with pytest.raises(JsonRpcRejected, match="not valid JSON"):
            canonicalize_request(_body(TOOLS_CALL) + b'{"jsonrpc":"2.0"}')

    @pytest.mark.parametrize(
        "compress",
        [gzip.compress, zlib.compress],
        ids=["gzip", "deflate"],
    )
    def test_compressed_bodies_are_refused(self, compress):
        """There is no decompression step: a compressed frame is opaque bytes
        that never survives strict parsing, so the policy check can never be
        handed a body it did not inspect."""
        with pytest.raises(JsonRpcRejected):
            canonicalize_request(compress(_body(TOOLS_CALL)))

    def test_invalid_utf8(self):
        with pytest.raises(JsonRpcRejected, match="not valid UTF-8"):
            canonicalize_request(b'{"jsonrpc":"2.0","method":"\xff\xfe"}')

    @pytest.mark.parametrize("version", ["1.0", "2.00", 2.0, None])
    def test_wrong_jsonrpc_version(self, version):
        with pytest.raises(JsonRpcRejected, match='"jsonrpc": "2.0"'):
            canonicalize_request(_body({"jsonrpc": version, "method": "tools/list"}))

    def test_missing_jsonrpc_version(self):
        with pytest.raises(JsonRpcRejected, match='"jsonrpc": "2.0"'):
            canonicalize_request(_body({"id": 1, "method": "tools/list"}))

    @pytest.mark.parametrize("method", [None, "", 12, {"name": "tools/call"}, ["x"]])
    def test_missing_or_non_string_method(self, method):
        payload = {"jsonrpc": "2.0", "id": 1}
        if method is not None:
            payload["method"] = method
        with pytest.raises(JsonRpcRejected, match="string method"):
            canonicalize_request(_body(payload))

    @pytest.mark.parametrize("params", [None, "search_docs", ["search_docs"], 7])
    def test_tools_call_without_object_params(self, params):
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call"}
        if params is not None:
            payload["params"] = params
        with pytest.raises(JsonRpcRejected, match="object params"):
            canonicalize_request(_body(payload))

    @pytest.mark.parametrize("name", [None, "", 5, {"a": 1}])
    def test_tools_call_without_a_string_name(self, name):
        params = {} if name is None else {"name": name}
        with pytest.raises(JsonRpcRejected, match="string params.name"):
            canonicalize_request(
                _body(
                    {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}
                )
            )
