"""When a session moves to the build's pin at a turn start: the main agent's rules, read off Claude's records."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ptc_agent.agent.claude_code import compaction
from ptc_agent.agent.claude_code.compaction import CompactionRules

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC).timestamp()
CURRENT = {"system_prompt": "new", "tools": [], "source_digest": "build-2", "prompt_version": 3}


@pytest.fixture(autouse=True)
def _build(monkeypatch):
    monkeypatch.setattr(compaction.release, "source_digest", lambda: "build-2")
    monkeypatch.setattr(compaction.release, "prompt_version", lambda: 3)
    monkeypatch.setattr(compaction.release, "rebuild_before", lambda: 0)


def records(*, minutes_ago: float, tokens: int = 200_000, ttl: str = "1h", **pin) -> dict:
    stamp = datetime.fromtimestamp(NOW - minutes_ago * 60, UTC).isoformat().replace("+00:00", "Z")
    usage = {"input_tokens": 10, "cache_read_input_tokens": tokens - 10, "cache_creation_input_tokens": 0, "output_tokens": 0,
             "cache_creation": {f"ephemeral_{ttl}_input_tokens": 50}}
    kept = {"system_prompt": "old", "tools": [], "source_digest": "build-1", "prompt_version": 3, **pin}
    return {"session_id": "s", "pinned": {k: v for k, v in kept.items() if v is not None},
            "entries": [{"type": "assistant", "timestamp": stamp, "message": {"usage": usage}},
                        {"type": "attachment", "timestamp": stamp}]}


def test_a_deploys_change_moves_a_large_session_once_its_cache_has_expired():
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=61), CURRENT, NOW) == (True, "deploy_cold")
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=59), CURRENT, NOW) == (False, "warm")
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=61, tokens=149_000), CURRENT, NOW) == (False, "under_floor")
    assert CompactionRules(0).repin_plan(records(minutes_ago=61), CURRENT, NOW) == (False, "off")


def test_the_cache_reads_cold_by_the_shorter_ttl_and_the_later_of_the_last_record_and_the_last_run():
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=6, ttl="5m"), CURRENT, NOW) == (True, "deploy_cold")
    settled = datetime.fromtimestamp(NOW - 30 * 60, UTC).replace(tzinfo=None)
    assert CompactionRules(150_000, settled_at=settled).repin_plan(records(minutes_ago=61), CURRENT, NOW) == (False, "warm")


def test_a_change_of_per_turn_inputs_or_no_change_never_moves_it():
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=61, source_digest="build-2"), CURRENT, NOW) == (False, "inputs_only")
    same = records(minutes_ago=61, system_prompt="new", source_digest=None, prompt_version=None)
    assert CompactionRules(150_000).repin_plan(same, CURRENT, NOW) is None
    assert CompactionRules(150_000).repin_plan({"session_id": "s", "entries": []}, CURRENT, NOW) is None


def test_a_session_at_its_threshold_moves_on_any_change_since_the_cli_compacts_it_anyway(monkeypatch):
    inputs_only = records(minutes_ago=1, tokens=300_000, source_digest="build-2")
    assert CompactionRules(150_000, threshold=300_000).repin_plan(inputs_only, CURRENT, NOW) == (True, "threshold")
    assert CompactionRules(150_000, threshold=300_001).repin_plan(inputs_only, CURRENT, NOW) == (False, "inputs_only")
    same = records(minutes_ago=1, tokens=300_000, system_prompt="new", source_digest=None, prompt_version=None)
    assert CompactionRules(150_000, threshold=300_000).repin_plan(same, CURRENT, NOW) is None
    monkeypatch.setattr(compaction.release, "prompt_version", lambda: 2)
    assert CompactionRules(150_000, threshold=300_000).repin_plan(records(minutes_ago=1, tokens=300_000), CURRENT, NOW) == (True, "threshold")


def test_a_pin_from_before_pins_kept_their_build_counts_as_a_deploys():
    legacy = records(minutes_ago=61, source_digest=None, prompt_version=None)
    assert CompactionRules(150_000).repin_plan(legacy, CURRENT, NOW) == (True, "deploy_cold")


def test_a_pin_below_rebuild_before_moves_whatever_its_size_or_cache_and_a_rollback_never(monkeypatch):
    monkeypatch.setattr(compaction.release, "rebuild_before", lambda: 3)
    assert CompactionRules(0).repin_plan(records(minutes_ago=1, tokens=1_000, prompt_version=2), CURRENT, NOW) == (True, "urgent")
    monkeypatch.setattr(compaction.release, "prompt_version", lambda: 2)
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=61, prompt_version=3), CURRENT, NOW) == (False, "rollback")


def test_the_context_counts_only_requests_since_the_last_compaction():
    """A compaction between turns sends no request of its own, so the size
    before it would read as the size after."""
    before = records(minutes_ago=1, tokens=300_000)
    boundary = {"type": "system", "subtype": "compact_boundary", "uuid": "b"}
    assert compaction.context_tokens(before["entries"]) == 300_000
    assert compaction.context_tokens([*before["entries"], boundary]) == 0
    assert compaction.context_tokens([boundary, *before["entries"]]) == 300_000



def test_with_compaction_off_no_change_moves_a_session(monkeypatch):
    """The main agent makes no turn-start rebuild then, the urgent one included."""
    from ptc_agent.config.agent import CompactionConfig

    monkeypatch.setattr(compaction.release, "rebuild_before", lambda: 3)
    rules = CompactionRules.of(CompactionConfig(enabled=False), None)
    for stale in (records(minutes_ago=1, tokens=1_000, prompt_version=2), records(minutes_ago=61), records(minutes_ago=1, tokens=500_000)):
        assert rules.repin_plan(stale, CURRENT, NOW) == (False, "off")

