"""Extraction + dedup logic for provenance records (pure, no DB).

``extract_provenance_from_sse_events`` filters accumulated SSE events down to
``event == "provenance"`` entries and collapses duplicates within a turn on
``(source_type, identifier, result_sha256)``.
"""

from src.server.database.provenance import extract_provenance_from_sse_events


def _provenance_event(**fields):
    """Flat-field provenance SSE event (the at-persist-time shape)."""
    base = {
        "source_type": "web_search",
        "identifier": "https://example.test/a",
        "result_sha256": "sha-a",
    }
    base.update(fields)
    return {"event": "provenance", **base}


class TestExtractProvenance:
    def test_empty_or_none_returns_empty(self):
        assert extract_provenance_from_sse_events(None) == []
        assert extract_provenance_from_sse_events([]) == []

    def test_filters_non_provenance_events(self):
        events = [
            {"event": "message_chunk", "data": {"text": "hi"}},
            _provenance_event(),
            {"event": "tool_calls", "data": {}},
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 1
        assert records[0]["source_type"] == "web_search"
        assert records[0]["identifier"] == "https://example.test/a"

    def test_normalizes_all_fields(self):
        events = [
            _provenance_event(
                source_type="mcp_tool",
                identifier="server:get_prices",
                title="Daily prices",
                detail="daily_prices",
                tool_call_id="call-1",
                args_fingerprint={"symbol": "TEST"},
                args={"symbol": "TEST", "api_key": "[redacted]"},
                result_sha256="sha-z",
                result_size=4096,
                result_snippet="snippet text",
                agent="task:abc",
                provider="mcp:finance",
            )
        ]
        record = extract_provenance_from_sse_events(events)[0]
        assert record["source_type"] == "mcp_tool"
        assert record["identifier"] == "server:get_prices"
        assert record["title"] == "Daily prices"
        assert record["detail"] == "daily_prices"
        assert record["tool_call_id"] == "call-1"
        assert record["args_fingerprint"] == {"symbol": "TEST"}
        # Readable redacted args ride alongside the hash.
        assert record["args"] == {"symbol": "TEST", "api_key": "[redacted]"}
        assert record["result_sha256"] == "sha-z"
        assert record["result_size"] == 4096
        assert record["result_snippet"] == "snippet text"
        assert record["agent"] == "task:abc"
        assert record["provider"] == "mcp:finance"

    def test_dedup_same_source_same_content(self):
        # Same (source_type, identifier, result_sha256) collapses to one row,
        # even if other fields (e.g. tool_call_id) differ.
        events = [
            _provenance_event(tool_call_id="call-1"),
            _provenance_event(tool_call_id="call-2"),
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 1

    def test_distinct_content_kept(self):
        # Same URL, different content hash → two rows (content changed).
        events = [
            _provenance_event(result_sha256="sha-a"),
            _provenance_event(result_sha256="sha-b"),
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 2
        assert {r["result_sha256"] for r in records} == {"sha-a", "sha-b"}

    def test_distinct_identifiers_kept(self):
        events = [
            _provenance_event(identifier="https://example.test/a", result_sha256="s"),
            _provenance_event(identifier="https://example.test/b", result_sha256="s"),
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 2

    def test_nested_data_shape_supported(self):
        # Defensive: if a producer ever wraps fields under "data", still extract.
        events = [
            {
                "event": "provenance",
                "data": {
                    "source_type": "web_fetch",
                    "identifier": "https://example.test/x",
                    "result_sha256": "sha-x",
                },
            }
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 1
        assert records[0]["source_type"] == "web_fetch"
        assert records[0]["identifier"] == "https://example.test/x"

    def test_ignores_turn_index_and_response_id_at_extraction(self):
        # Replay-added fields must not affect extraction output.
        events = [_provenance_event(turn_index=2, response_id="resp-1")]
        record = extract_provenance_from_sse_events(events)[0]
        assert "turn_index" not in record
        assert "response_id" not in record

    def test_skips_missing_or_nonstring_source_type(self):
        # source_type is NOT NULL and partly fed by untrusted in-sandbox traces;
        # a missing/garbage source_type must be dropped, never reach the insert.
        events = [
            {"event": "provenance", "identifier": "x", "result_sha256": "s"},  # no type
            {"event": "provenance", "source_type": None, "identifier": "y"},
            {"event": "provenance", "source_type": 123, "identifier": "z"},
            _provenance_event(),  # valid
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 1
        assert records[0]["source_type"] == "web_search"

    def test_coerces_result_size_to_int_or_none(self):
        events = [
            _provenance_event(identifier="a", result_size="4096"),
            _provenance_event(identifier="b", result_size=12.9),
            _provenance_event(identifier="c", result_size="not-a-number"),
            _provenance_event(identifier="d", result_size=None),
        ]
        by_id = {r["identifier"]: r for r in extract_provenance_from_sse_events(events)}
        assert by_id["a"]["result_size"] == 4096  # numeric string coerced
        assert by_id["b"]["result_size"] == 12  # float truncated
        assert by_id["c"]["result_size"] is None  # garbage dropped, not raised
        assert by_id["d"]["result_size"] is None

    def test_result_size_nonfinite_or_overflow_dropped(self):
        # A poisoned in-sandbox trace can carry inf/nan or an int wider than
        # BIGINT. int(float('inf')) raises OverflowError and a >BIGINT value
        # overflows the bind — either would abort the insert and (via the
        # best-effort wrapper) drop ALL provenance for the response. Must be None.
        events = [
            _provenance_event(identifier="a", result_size=float("inf")),
            _provenance_event(identifier="b", result_size=float("nan")),
            _provenance_event(identifier="c", result_size="9" * 400),  # >> BIGINT
            _provenance_event(identifier="d", result_size=2**63),  # one past max
            _provenance_event(identifier="e", result_size=2**63 - 1),  # max, kept
        ]
        by_id = {r["identifier"]: r for r in extract_provenance_from_sse_events(events)}
        assert by_id["a"]["result_size"] is None
        assert by_id["b"]["result_size"] is None
        assert by_id["c"]["result_size"] is None
        assert by_id["d"]["result_size"] is None
        assert by_id["e"]["result_size"] == 2**63 - 1

    def test_nonstring_text_fields_coerced_not_raised(self):
        # Untrusted text fields (identifier/result_sha256/snippet/...) are copied
        # from the trace verbatim. A non-string would make strip_pg_nul_str raise
        # at bind time AND make the hashable dedup_key membership test raise — both
        # would drop the whole set. They must coerce to str-or-None instead.
        events = [
            {
                "event": "provenance",
                "source_type": "mcp_tool",
                "identifier": {"nested": "object"},  # forged non-string
                "result_sha256": 12345,  # forged int
                "result_snippet": ["a", "b"],  # forged list
                "title": 99,
            }
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 1
        r = records[0]
        assert isinstance(r["identifier"], str)
        assert isinstance(r["result_sha256"], str)
        assert isinstance(r["result_snippet"], str)
        assert isinstance(r["title"], str)

    def test_none_text_fields_stay_none(self):
        # Coercion must not turn a genuinely-absent field into the string "None".
        records = extract_provenance_from_sse_events(
            [{"event": "provenance", "source_type": "web_search"}]
        )
        assert records[0]["identifier"] is None
        assert records[0]["result_sha256"] is None
        assert records[0]["title"] is None

    def test_keeps_same_source_distinct_agents(self):
        # main and a subagent fetching the same URL+content are distinct accesses
        # and must each keep a row (agent is part of the dedup key).
        events = [
            _provenance_event(agent="main"),
            _provenance_event(agent="task:abc"),
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == 2
        assert {r["agent"] for r in records} == {"main", "task:abc"}

    def test_source_timestamp_parsed_or_none(self):
        events = [
            _provenance_event(identifier="a", timestamp="2026-06-14T12:00:00+00:00"),
            _provenance_event(identifier="b", timestamp="not-a-date"),  # agent-forged
            _provenance_event(identifier="c"),  # absent
        ]
        by_id = {r["identifier"]: r for r in extract_provenance_from_sse_events(events)}
        assert by_id["a"]["source_timestamp"] is not None
        assert by_id["a"]["source_timestamp"].year == 2026
        assert by_id["b"]["source_timestamp"] is None  # bad value coerced, not raised
        assert by_id["c"]["source_timestamp"] is None

    def test_caps_records_per_response(self):
        from src.server.database.provenance import _MAX_RECORDS_PER_RESPONSE

        events = [
            _provenance_event(identifier=f"https://example.test/{i}", result_sha256=str(i))
            for i in range(_MAX_RECORDS_PER_RESPONSE + 50)
        ]
        records = extract_provenance_from_sse_events(events)
        assert len(records) == _MAX_RECORDS_PER_RESPONSE
