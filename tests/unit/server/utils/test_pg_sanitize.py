"""Tests for src.server.utils.pg_sanitize.

`pg_sanitize` is the shared helper that strips NUL bytes / `\\u0000` escapes
before values are bound to Postgres TEXT/JSONB columns. These tests pin its
behavior so future changes don't silently re-expose the persistence boundary
to NUL-bearing input.
"""

from __future__ import annotations

import json
import time

import pytest

from src.server.utils.pg_sanitize import SafeJson, _safe_dumps, strip_pg_nul_str


class TestStripPgNulStr:
    def test_strips_embedded_nul(self):
        assert strip_pg_nul_str("a\x00b") == "ab"

    def test_strips_multiple_nuls(self):
        assert strip_pg_nul_str("\x00a\x00b\x00") == "ab"

    def test_passthrough_when_no_nul_returns_same_object(self):
        # Critical for hot path: no allocation when input is clean.
        s = "hello world"
        assert strip_pg_nul_str(s) is s

    def test_empty_passthrough(self):
        assert strip_pg_nul_str("") == ""

    def test_none_passthrough(self):
        assert strip_pg_nul_str(None) is None


class TestSafeDumps:
    def test_strips_unicode_escape_in_string_value(self):
        out = _safe_dumps({"k": "a\x00b"})
        assert "\\u0000" not in out
        # Round-trip should give back the cleaned value.
        assert json.loads(out) == {"k": "ab"}

    def test_clean_input_is_unchanged_aside_from_serialization(self):
        out = _safe_dumps({"k": "v"})
        assert json.loads(out) == {"k": "v"}

    def test_handles_nested_structures(self):
        out = _safe_dumps({"k": "a\x00b", "n": [1, "c\x00", {"deep": "d\x00"}]})
        roundtrip = json.loads(out)
        assert roundtrip == {"k": "ab", "n": [1, "c", {"deep": "d"}]}

    def test_unserializable_raises_typeerror(self):
        # No `default=` fallback — non-JSON types must surface as TypeError so
        # accidental datetime/UUID/Pydantic-object binds don't get silently
        # stringified into JSONB.
        class Custom:
            pass

        with pytest.raises(TypeError):
            _safe_dumps({"x": Custom()})

    def test_strips_nul_in_dict_keys(self):
        out = _safe_dumps({"k\x00": 1})
        assert json.loads(out) == {"k": 1}


class TestSafeJson:
    def test_is_a_psycopg_Json_subclass(self):
        from psycopg.types.json import Json

        assert isinstance(SafeJson({}), Json)

    def test_dumps_strips_nul(self):
        wrapped = SafeJson({"a": "x\x00", "b": ["y\x00z"]})
        # psycopg calls dumps() on the wrapped value at bind time. Replicate.
        rendered = wrapped.dumps(wrapped.obj)
        assert "\\u0000" not in rendered
        assert json.loads(rendered) == {"a": "x", "b": ["yz"]}


@pytest.mark.parametrize("size_mb", [1, 10])
def test_safe_dumps_performance_smoke(size_mb: int):
    """Catch accidental Python-level walks if someone "improves" _safe_dumps.

    With the current dumps-based approach, a 10 MB nested dict should
    serialize+strip in well under 1 second on any modern machine. The bound
    is generous to keep the test stable on shared CI runners.
    """
    chunk = "x" * 1024  # 1 KB strings
    blob = {"items": [{"i": i, "data": chunk} for i in range(size_mb * 1024)]}
    start = time.perf_counter()
    _safe_dumps(blob)
    elapsed = time.perf_counter() - start
    assert elapsed < 5.0, f"_safe_dumps too slow: {elapsed:.2f}s for {size_mb} MB"
