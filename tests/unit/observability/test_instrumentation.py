"""Sanity tests for the smaller observability surface (hash_id, normalize_content_type)."""

from __future__ import annotations



def test_hash_id_stable_and_bounded():
    from src.observability.tracing import hash_id

    out = hash_id("user-123")
    assert isinstance(out, str)
    assert len(out) == 16
    # Stable
    assert hash_id("user-123") == out
    # Different inputs differ
    assert hash_id("user-124") != out


def test_hash_id_none_returns_empty():
    from src.observability.tracing import hash_id

    assert hash_id(None) == ""


def test_hash_id_disabled_returns_raw(monkeypatch):
    # _HASH_IDS is sampled once at import; tests patch the module attribute directly.
    from src.observability import tracing

    monkeypatch.setattr(tracing, "_HASH_IDS", False)
    assert tracing.hash_id("user-123") == "user-123"


def test_normalize_content_type_known_types():
    from src.observability.metrics import normalize_content_type

    assert normalize_content_type("application/pdf") == "pdf"
    assert normalize_content_type("text/markdown") == "md"
    assert normalize_content_type("text/csv") == "csv"
    assert normalize_content_type("application/json") == "json"
    assert normalize_content_type("text/plain") == "txt"


def test_normalize_content_type_unknown_collapses_to_other():
    from src.observability.metrics import normalize_content_type

    assert normalize_content_type("video/mp4") == "other"
    assert normalize_content_type("") == "other"
    assert normalize_content_type(None) == "other"
