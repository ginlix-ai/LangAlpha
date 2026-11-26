"""Line diffs and the run-length form of an attribution."""

from __future__ import annotations

from src.server.services.drive import linediff


def test_lines_as_the_web_shows_them():
    assert linediff.lines_of("") == []
    assert linediff.lines_of("a\n") == ["a"]
    assert linediff.lines_of("a\n\nb") == ["a", "", "b"]


def test_a_costly_diff_falls_back_to_a_multiset(monkeypatch):
    monkeypatch.setattr(linediff, "DIFF_WORK_MAX", 1000)
    a, b = ["x"] * 200, ["x", "y"] * 100
    assert linediff.counts(a, b) == (100, 100)
    assert linediff.counts(["k", "a"], ["a", "n"]) == (1, 1)
    added, removed, by = linediff.step(a, b, [1] * 200, 2)
    assert (added, removed) == (100, 100)
    assert by == [1, 2] * 100


def test_runs_join_adjacent_writers():
    assert linediff.runs_of([3, 2, 3, 1, 5, 4]) == [(1, 3, 3), (4, 4, 5)]
    assert linediff.packed(linediff.unpacked([3, 2, 5, 1])) == [3, 2, 5, 1]
