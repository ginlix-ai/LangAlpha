"""Tests for the ``loaded_skills`` state reducer.

The channel is semantically a set (every consumer reads it via ``set(...)``),
so the reducer must dedup-union rather than accumulate — otherwise a turn that
re-seeds an already-loaded skill grows the persisted list unboundedly.
"""

from ptc_agent.agent.middleware.skills.middleware import _union_loaded_skills


def test_union_appends_new_names():
    assert _union_loaded_skills(["a"], ["b"]) == ["a", "b"]


def test_union_dedupes_repeated_name():
    # The bug operator.add had: re-seeding "a" must not duplicate it.
    assert _union_loaded_skills(["a"], ["a"]) == ["a"]


def test_union_preserves_left_order_and_appends_only_fresh():
    assert _union_loaded_skills(["a", "b"], ["b", "c", "a", "d"]) == ["a", "b", "c", "d"]


def test_union_handles_none_operands():
    assert _union_loaded_skills(None, ["a"]) == ["a"]
    assert _union_loaded_skills(["a"], None) == ["a"]
    assert _union_loaded_skills(None, None) == []


def test_union_dedupes_within_right():
    assert _union_loaded_skills([], ["x", "x", "y"]) == ["x", "y"]


def test_union_dedupes_within_left():
    # Threads persisted under the old operator.add reducer can carry left-side
    # duplicates; the reducer must re-bound them, not pass them through forever.
    assert _union_loaded_skills(["a", "a", "b"], []) == ["a", "b"]
    assert _union_loaded_skills(["a", "b", "a"], ["b", "c"]) == ["a", "b", "c"]
