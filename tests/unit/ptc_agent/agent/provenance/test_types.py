"""Unit tests for the shared provenance types and helpers."""

from __future__ import annotations

import hashlib
import json

import pytest

from ptc_agent.agent.provenance import (
    ProvenanceSource,
    build_provenance_event,
    fingerprint_result,
    fingerprint_result_with_body,
    hash_args,
    redact_args,
)


def test_hash_args_is_deterministic_digest_not_raw():
    args = {"symbol": "TST", "limit": 5}
    out = hash_args(args)
    # Never the raw args — only a non-reversible digest.
    assert out == {"sha256": hash_args({"limit": 5, "symbol": "TST"})["sha256"]}
    assert set(out) == {"sha256"}
    assert args != out
    canonical = json.dumps(args, sort_keys=True, default=str, ensure_ascii=False)
    assert out["sha256"] == hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def test_hash_args_none_for_empty():
    assert hash_args(None) is None
    assert hash_args({}) is None


def test_fingerprint_deterministic_regardless_of_key_order():
    a = {"alpha": 1, "beta": 2, "gamma": {"x": 1, "y": 2}}
    b = {"gamma": {"y": 2, "x": 1}, "beta": 2, "alpha": 1}

    sha_a, size_a, snippet_a = fingerprint_result(a)
    sha_b, size_b, snippet_b = fingerprint_result(b)

    assert sha_a == sha_b
    assert size_a == size_b
    assert snippet_a == snippet_b

    # Hash matches the documented canonical form.
    canonical = json.dumps(a, sort_keys=True, default=str, ensure_ascii=False)
    assert sha_a == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert size_a == len(canonical.encode("utf-8"))


def test_fingerprint_snippet_truncates_at_500_chars():
    value = "x" * 5000
    sha, size, snippet = fingerprint_result(value)

    assert len(snippet) == 500
    assert snippet == "x" * 500
    assert size == 5000
    assert sha == hashlib.sha256(value.encode("utf-8")).hexdigest()


def test_fingerprint_handles_nul_byte_without_crashing():
    # dict path: json.dumps escapes NUL as a backslash-u-0000 escape.
    sha, size, snippet = fingerprint_result({"field": "before\x00after"})
    assert isinstance(sha, str) and len(sha) == 64
    assert size > 0
    assert "\\u0000" in snippet

    # plain-string path: NUL is preserved verbatim (downstream sanitizes it,
    # not this function); the key guarantee is no crash.
    sha2, size2, snippet2 = fingerprint_result("before\x00after")
    assert isinstance(sha2, str) and len(sha2) == 64
    assert "\x00" in snippet2


def test_fingerprint_handles_odd_inputs_without_raising():
    for value in (None, b"raw-bytes", object(), [1, {"k": "v"}, None]):
        sha, size, snippet = fingerprint_result(value)
        assert isinstance(sha, str) and len(sha) == 64
        assert isinstance(size, int) and size >= 0
        assert isinstance(snippet, str)


def test_fingerprint_with_body_matches_fingerprint_and_is_hash_consistent():
    # The 4-tuple's sha/size/snippet must equal fingerprint_result's, and the
    # returned body must be byte-identical to the string that produced the sha —
    # that identity is the whole point (a verifier hashes the stored body).
    for value in (
        {"gamma": {"y": 2, "x": 1}, "alpha": 1},
        [1, {"k": "v"}, None],
        "plain string result",
        "x" * 5000,
        None,
    ):
        sha, size, snippet = fingerprint_result(value)
        sha_b, size_b, snippet_b, body = fingerprint_result_with_body(value)
        assert (sha_b, size_b, snippet_b) == (sha, size, snippet)
        assert hashlib.sha256(body.encode("utf-8")).hexdigest() == sha
        assert len(body.encode("utf-8")) == size
        assert body[:500] == snippet  # snippet is the canonical body's head


def test_build_event_generates_record_id_and_timestamp_when_omitted():
    event = build_provenance_event(
        source_type="web_search",
        identifier="https://example.test/q",
        title="Example result",
        provider="example_provider",
    )

    assert event["type"] == "provenance"
    assert event["source_type"] == "web_search"
    assert event["identifier"] == "https://example.test/q"
    assert event["title"] == "Example result"
    assert event["provider"] == "example_provider"
    # Generated fields are present and well-formed.
    assert isinstance(event["record_id"], str) and event["record_id"]
    assert isinstance(event["timestamp"], str) and "T" in event["timestamp"]
    # agent stays None unless explicitly set (handler resolves it later).
    assert event["agent"] is None


def test_build_event_from_source_round_trips_fields():
    source = ProvenanceSource(
        record_id="rec-123",
        source_type="file_read",
        identifier="work/notes.md",
        timestamp="2026-01-01T00:00:00+00:00",
        title="Notes",
        provider=None,
        tool_call_id="call-abc",
        args_fingerprint={"path": "work/notes.md"},
        args={"file_path": "work/notes.md"},
        result_sha256="deadbeef",
        result_size=42,
        result_snippet="hello",
        agent="task:xyz",
    )

    event = build_provenance_event(source)

    assert event["type"] == "provenance"
    assert event["record_id"] == "rec-123"
    assert event["source_type"] == "file_read"
    assert event["identifier"] == "work/notes.md"
    assert event["timestamp"] == "2026-01-01T00:00:00+00:00"
    assert event["tool_call_id"] == "call-abc"
    assert event["args_fingerprint"] == {"path": "work/notes.md"}
    assert event["args"] == {"file_path": "work/notes.md"}
    assert event["result_sha256"] == "deadbeef"
    assert event["result_size"] == 42
    assert event["result_snippet"] == "hello"
    assert event["agent"] == "task:xyz"


def test_build_event_kwargs_override_source():
    source = ProvenanceSource(
        record_id="rec-1",
        source_type="web_fetch",
        identifier="https://example.test/a",
        timestamp="2026-01-01T00:00:00+00:00",
    )

    event = build_provenance_event(source, identifier="https://example.test/b")

    assert event["identifier"] == "https://example.test/b"
    assert event["record_id"] == "rec-1"


def test_build_event_has_full_key_set():
    event = build_provenance_event(source_type="market_data", identifier="ABC")

    expected_keys = {
        "type",
        "record_id",
        "source_type",
        "identifier",
        "title",
        "detail",
        "provider",
        "tool_call_id",
        "args_fingerprint",
        "args",
        "result_sha256",
        "result_size",
        "result_snippet",
        "timestamp",
        "agent",
    }
    assert set(event.keys()) == expected_keys


def test_build_event_never_carries_result_body():
    """The transient ``result_body`` is consumed live by the middleware and must
    NEVER ride the SSE event — that keeps ``sse_events`` gaining 0 bytes. The
    event key set stays exactly the pre-body shape even when the source carries
    a large body."""
    source = ProvenanceSource(
        record_id="rec-1",
        source_type="web_fetch",
        identifier="https://example.test/a",
        timestamp="2026-01-01T00:00:00+00:00",
        result_sha256="deadbeef",
        result_size=99999,
        result_snippet="snip",
        result_body="x" * 50000,  # a substantial full body
    )

    event = build_provenance_event(source)

    assert "result_body" not in event
    # No value in the event equals the body (it isn't smuggled under another key).
    assert source.result_body not in event.values()
    # The key set is unchanged vs. the documented pre-body shape.
    assert set(event.keys()) == {
        "type",
        "record_id",
        "source_type",
        "identifier",
        "title",
        "detail",
        "provider",
        "tool_call_id",
        "args_fingerprint",
        "args",
        "result_sha256",
        "result_size",
        "result_snippet",
        "timestamp",
        "agent",
    }
    # build_provenance_event takes no result_body kwarg (a TypeError guards the
    # contract that the body can't be injected through the builder either).
    with pytest.raises(TypeError):
        build_provenance_event(result_body="leak")  # type: ignore[call-arg]


# ----- redact_args: security-critical deny-list -----------------------------

_REDACTED = "[redacted]"


@pytest.mark.parametrize(
    "key",
    [
        "token",
        "api_key",
        "x-api-key",
        "access_token",
        "Authorization",
        "password",
        "client_secret",
        "session",
        "cookie",
        "apiKey",
        "secret_key",
        "private_key",
        "passphrase",
        "credential",
        "refresh_token",
    ],
)
def test_redact_args_redacts_secret_keys(key):
    """Any value under a secret-ish key is replaced regardless of its content."""
    out = redact_args({key: "some-innocent-looking-value", "symbol": "TST"})
    assert out[key] == _REDACTED
    assert out["symbol"] == "TST"  # neighbor untouched


def test_redact_args_redacts_nested_secret_key():
    out = redact_args(
        {
            "url": "https://api.test/v1",
            "headers": {"Authorization": "Bearer abc", "Accept": "json"},
        }
    )
    assert out["url"] == "https://api.test/v1"
    assert out["headers"]["Authorization"] == _REDACTED
    # Non-secret sibling key inside the nested dict survives.
    assert out["headers"]["Accept"] == "json"


@pytest.mark.parametrize(
    "value",
    [
        "Bearer abc123",
        "eyJhbG.eyJzdWI.sig",
        "sk-" + "A" * 20,
        "ghp_" + "A" * 30,
        "xoxb-" + "1" * 15,
        "AKIA" + "A" * 16,
        "a" * 40,  # 40-char hex
    ],
)
def test_redact_args_redacts_secret_value_under_innocent_key(value):
    """A credential-shaped value is redacted even under a harmless key name."""
    out = redact_args({"note": value})
    assert out["note"] == _REDACTED


@pytest.mark.parametrize(
    "key,value",
    [
        ("symbol", "AAPL"),
        ("ticker", "MSFT"),
        ("start_date", "2026-01-01"),
        ("end_date", "2026-06-01"),
        ("period", "quarterly"),
        ("interval", "1d"),
        ("statement_type", "income_statement"),
        ("limit", 50),
        ("query", "tech sector outlook"),
        ("url", "https://docs.test/page"),
        ("file_path", "work/analysis/out.csv"),
        ("mapping", "value"),  # contains 'pin' substring — must NOT redact
        ("sort_key", "name"),  # contains 'key' substring — must NOT redact
        ("endpoint", "/v1/quotes"),  # innocent name, no secret substring
    ],
)
def test_redact_args_keeps_clean_values_verbatim(key, value):
    """No false positives: meaningful args pass through unchanged."""
    out = redact_args({key: value})
    assert out[key] == value


def test_redact_args_short_ambiguous_tokens_match_whole_key_only():
    """`pin`/`key`/`sig` redact as exact keys but not as substrings."""
    out = redact_args(
        {
            "pin": "1234",
            "key": "raw",
            "sig": "xx",
            "mapping": "ok",  # 'pin' substring
            "sort_key": "ok",  # 'key' substring
            "config": "ok",  # 'sig' substring (reversed) — irrelevant, kept
        }
    )
    assert out["pin"] == _REDACTED
    assert out["key"] == _REDACTED
    assert out["sig"] == _REDACTED
    assert out["mapping"] == "ok"
    assert out["sort_key"] == "ok"
    assert out["config"] == "ok"


def test_redact_args_clamps_long_string():
    out = redact_args({"query": "z" * 500})
    assert len(out["query"]) == 256


def test_redact_args_caps_dict_breadth():
    out = redact_args({f"k{i}": i for i in range(100)})
    assert len(out) == 32


def test_redact_args_caps_list_length():
    out = redact_args({"items": list(range(100))})
    assert len(out["items"]) == 16


def test_redact_args_bounds_deep_nesting():
    # Build a chain deeper than _MAX_ARG_DEPTH (4). The deepest payload should be
    # bounded to "[redacted]" rather than recursed into.
    deep = {"v": "leaf"}
    for _ in range(8):
        deep = {"nested": deep}
    out = redact_args(deep)
    cur = out
    # Walk down to where recursion was cut off; eventually we hit the marker.
    saw_redacted = False
    for _ in range(10):
        if cur == _REDACTED:
            saw_redacted = True
            break
        if isinstance(cur, dict) and "nested" in cur:
            cur = cur["nested"]
        else:
            break
    assert saw_redacted


def test_redact_args_returns_none_for_empty_or_non_dict():
    assert redact_args(None) is None
    assert redact_args({}) is None
    assert redact_args("a string") is None
    assert redact_args([1, 2, 3]) is None
    assert redact_args(42) is None


def test_redact_args_never_raises_on_odd_input():
    class Weird:
        def __str__(self):
            raise RuntimeError("boom")

    # Odd nested values are coerced via str(); a str() that itself raises must be
    # swallowed (redact_args returns None rather than propagating).
    assert redact_args({"x": b"raw-bytes"}) == {"x": "b'raw-bytes'"}
    assert redact_args({"x": {1, 2, 3}})["x"].startswith("{")
    assert redact_args({"x": object()})["x"].startswith("<")
    assert redact_args({"x": Weird()}) is None  # swallowed, no propagation


def test_redact_args_bool_and_numbers_pass_through():
    out = redact_args({"flag": True, "count": 3, "ratio": 1.5, "none": None})
    assert out == {"flag": True, "count": 3, "ratio": 1.5, "none": None}
