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

from src.server.utils.pg_sanitize import (
    SafeJson,
    _safe_dumps,
    finite_json_dumps,
    normalize_uuid,
    strip_pg_nul_str,
)


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


class TestFiniteJsonDumps:
    def test_nan_and_inf_become_null(self):
        out = finite_json_dumps({"a": float("nan"), "b": [1.0, float("inf")]})
        assert out == '{"a": null, "b": [1.0, null]}'

    def test_negative_inf_becomes_null(self):
        assert finite_json_dumps([float("-inf")]) == "[null]"

    def test_nan_nested_in_tuple_becomes_null(self):
        out = finite_json_dumps({"t": (1.0, float("nan"))})
        assert json.loads(out) == {"t": [1.0, None]}

    def test_happy_path_matches_json_dumps(self):
        value = {"a": 1.5, "b": ["x", {"c": -2}], "d": None}
        assert finite_json_dumps(value) == json.dumps(value)

    def test_happy_path_single_dumps_call(self, monkeypatch):
        # Clean input must not pay the tree-walk fallback.
        calls = {"n": 0}
        real_dumps = json.dumps

        def counting_dumps(*args, **kwargs):
            calls["n"] += 1
            return real_dumps(*args, **kwargs)

        import src.server.utils.pg_sanitize as mod

        monkeypatch.setattr(mod.json, "dumps", counting_dumps)
        finite_json_dumps({"a": 1.0})
        assert calls["n"] == 1

    def test_kwargs_forwarded(self):
        out = finite_json_dumps({"k": "é"}, ensure_ascii=False)
        assert out == '{"k": "é"}'

    def test_no_bare_nan_token_ever(self):
        out = finite_json_dumps(
            {"deep": [{"x": float("nan")}, (float("inf"), 2.0)]}
        )
        assert "NaN" not in out and "Infinity" not in out
        assert json.loads(out) == {"deep": [{"x": None}, [None, 2.0]]}


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

    def test_dumps_replaces_nan_with_null(self):
        # NaN floats from upstream data (e.g. a forming Yahoo bar) must never
        # reach Postgres as a bare `NaN` token — that's invalid JSON and the
        # whole turn fails to persist (InvalidTextRepresentation).
        wrapped = SafeJson({"price": float("nan"), "series": [1.0, float("inf")]})
        rendered = wrapped.dumps(wrapped.obj)
        assert "NaN" not in rendered and "Infinity" not in rendered
        assert json.loads(rendered) == {"price": None, "series": [1.0, None]}


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


CANONICAL = "123e4567-e89b-12d3-a456-426614174000"


class TestNormalizeUuid:
    def test_canonical_passthrough(self):
        assert normalize_uuid(CANONICAL) == CANONICAL

    @pytest.mark.parametrize(
        "value",
        [
            "123E4567-E89B-12D3-A456-426614174000",
            "urn:uuid:123e4567-e89b-12d3-a456-426614174000",
            "123e4567e89b12d3a456426614174000",
            "{123e4567-e89b-12d3-a456-426614174000}",
        ],
    )
    def test_non_canonical_forms_normalize(self, value):
        assert normalize_uuid(value) == CANONICAL

    @pytest.mark.parametrize(
        "value",
        [
            "results",
            "notes.md",
            "",
            None,
            12345,
            "123e4567-e89b-12d3-a456",
        ],
    )
    def test_non_uuid_returns_none(self, value):
        assert normalize_uuid(value) is None
