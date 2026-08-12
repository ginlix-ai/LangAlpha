"""Tests for stamp_run — provider attribution written onto the LangSmith run."""

import pytest

from src.observability import stamp_run


class _FakeRun:
    def __init__(self, tags=None):
        self.metadata = {}
        self.tags = tags


@pytest.fixture
def fake_run(monkeypatch):
    """Stand in for the enclosing run tree. ``run`` None = tracing disabled."""
    holder = {"run": _FakeRun()}

    def _get_current_run_tree():
        return holder["run"]

    monkeypatch.setattr(
        "langsmith.run_helpers.get_current_run_tree", _get_current_run_tree
    )
    return holder


def test_writes_metadata_and_tags(fake_run):
    stamp_run(tags=["fetch_provider:inhouse"], fetch_provider="inhouse", fetch_source="live")

    assert fake_run["run"].metadata == {"fetch_provider": "inhouse", "fetch_source": "live"}
    assert fake_run["run"].tags == ["fetch_provider:inhouse"]


def test_drops_none_fields(fake_run):
    """A provider that never resolved must not write a null over the run."""
    stamp_run(fetch_provider=None, fetch_error="timeout")

    assert fake_run["run"].metadata == {"fetch_error": "timeout"}


def test_preserves_and_dedupes_existing_tags(fake_run):
    fake_run["run"] = _FakeRun(tags=["existing", "fetch_provider:inhouse"])

    stamp_run(tags=["fetch_provider:inhouse", "new"])

    assert fake_run["run"].tags == ["existing", "fetch_provider:inhouse", "new"]


def test_dedupes_within_one_call(fake_run):
    """Dedup was against the pre-existing tags only, so a repeat inside the
    incoming list landed twice."""
    stamp_run(tags=["fetch_provider:inhouse", "fetch_provider:inhouse"])

    assert fake_run["run"].tags == ["fetch_provider:inhouse"]


def test_no_run_is_a_noop(fake_run):
    """Tracing off — callers stamp unconditionally, so this must not raise."""
    fake_run["run"] = None

    stamp_run(fetch_provider="inhouse")


def test_never_raises_into_the_caller(monkeypatch):
    """Telemetry failure must not take a tool call down with it."""

    def _boom():
        raise RuntimeError("run tree exploded")

    monkeypatch.setattr("langsmith.run_helpers.get_current_run_tree", _boom)

    stamp_run(fetch_provider="inhouse")
