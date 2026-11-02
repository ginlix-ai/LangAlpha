"""When a session moves to the build's pin at a turn start: the main agent's rules, read off Claude's records."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ptc_agent.agent.claude_code import compaction
from ptc_agent.agent.claude_code.compaction import CompactionRules, SessionCompaction

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


def test_the_cache_is_judged_when_the_turn_was_sent():
    """A send made while the cache was warm meets no move for a deploy's
    change, however long it waited to start."""
    from ptc_agent.agent.middleware.runtime_context.turn_context import TurnContext
    from ptc_agent.config.agent import CompactionConfig

    sent = NOW - 30 * 60
    rules = CompactionRules.of(CompactionConfig(token_threshold=500_000, rebuild_floor_tokens=150_000), TurnContext(sent_at=sent))
    assert rules.repin_plan(records(minutes_ago=61), CURRENT, NOW) == (False, "warm")
    assert rules.repin_plan(records(minutes_ago=91), CURRENT, NOW) == (True, "deploy_cold")


def test_a_compaction_newer_than_any_request_reads_as_a_five_minute_write():
    """Its summary request marks its cache with no ttl, so a deploy six
    minutes after a compaction finds the cache cold."""
    stamp = datetime.fromtimestamp(NOW - 6 * 60, UTC).isoformat().replace("+00:00", "Z")
    boundary = {"type": "system", "subtype": "compact_boundary", "uuid": "b", "timestamp": stamp}
    compacted = {**(r := records(minutes_ago=7)), "entries": [*r["entries"], boundary]}
    assert compaction.cache_ttl(compacted["entries"]) == 300
    assert compaction.cache_ttl([*compacted["entries"], *records(minutes_ago=1)["entries"]]) == 3600
    assert CompactionRules(150_000).repin_plan(compacted, CURRENT, NOW, pending=150_000) == (True, "deploy_cold")


def test_a_change_of_per_turn_inputs_or_no_change_never_moves_it():
    assert CompactionRules(150_000).repin_plan(records(minutes_ago=61, source_digest="build-2"), CURRENT, NOW) == (False, "inputs_only")
    same = records(minutes_ago=61, system_prompt="new", source_digest=None, prompt_version=None)
    assert CompactionRules(150_000).repin_plan(same, CURRENT, NOW) is None
    assert CompactionRules(150_000).repin_plan({"session_id": "s", "entries": []}, CURRENT, NOW) is None


def test_a_session_the_cli_compacted_on_an_old_pin_moves_whatever_its_size_or_cache():
    """That compaction was the one the move was due, and the next comes only
    a context later."""
    small = records(minutes_ago=1, tokens=1_000, source_digest="build-2")
    assert CompactionRules(150_000).repin_plan({**small, "repin_due": True}, CURRENT, NOW) == (True, "compacted")
    assert CompactionRules(150_000).repin_plan(small, CURRENT, NOW) == (False, "inputs_only")


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



@pytest.mark.parametrize(("threshold", "compacts"), [(990_000, 967_000), (500_000, 500_000), (20_000, 67_000)])
def test_a_session_moves_at_the_context_the_cli_compacts_at(threshold, compacts):
    """Not at a threshold past the CLI's window bounds: the CLI would
    compact the session under its old pin first."""
    from ptc_agent.config.agent import CompactionConfig

    rules = CompactionRules.of(CompactionConfig(token_threshold=threshold), None)
    inputs_only = {"source_digest": "build-2"}
    assert rules.repin_plan(records(minutes_ago=1, tokens=compacts, **inputs_only), CURRENT, NOW) == (True, "threshold")
    assert rules.repin_plan(records(minutes_ago=1, tokens=compacts - 1, **inputs_only), CURRENT, NOW) == (False, "inputs_only")


def test_a_session_moves_when_the_turns_prompt_takes_it_to_the_threshold():
    """The turn's first request sends the prompt beside the context the last
    one measured, and past the threshold the CLI compacts on the old pin."""
    inputs_only = records(minutes_ago=1, tokens=290_000, source_digest="build-2")
    rules = CompactionRules(150_000, threshold=300_000)
    assert rules.repin_plan(inputs_only, CURRENT, NOW, pending=10_000) == (True, "threshold")
    assert rules.repin_plan(inputs_only, CURRENT, NOW, pending=9_999) == (False, "inputs_only")


def test_a_query_mid_turn_moves_a_session_on_another_pin_only_where_the_cli_would_compact_it():
    """Only the threshold: the cache is warm mid-turn, and a compaction the
    CLI made earlier in the turn left a context the next turn start moves."""
    rules = CompactionRules(150_000, threshold=300_000)
    inputs_only = records(minutes_ago=1, tokens=290_000, source_digest="build-2")
    assert rules.moves_mid_turn(inputs_only, CURRENT, 10_000)
    assert not rules.moves_mid_turn(inputs_only, CURRENT, 9_999)
    assert not rules.moves_mid_turn({**records(minutes_ago=1, tokens=1_000), "repin_due": True}, CURRENT, 0)
    same = records(minutes_ago=1, tokens=300_000, system_prompt="new", source_digest=None, prompt_version=None)
    assert not rules.moves_mid_turn(same, CURRENT, 0)
    off = CompactionRules(150_000, threshold=300_000, enabled=False)
    assert not off.moves_mid_turn(inputs_only, CURRENT, 10_000)


def test_a_deploys_change_moves_a_cold_session_once_the_turns_prompt_takes_it_over_the_floor():
    """The first request writes the whole context again, prompt included,
    and that write would keep the old pin's cache warm."""
    cold = records(minutes_ago=61, tokens=140_000)
    rules = CompactionRules(150_000, threshold=500_000)
    assert rules.repin_plan(cold, CURRENT, NOW, pending=100_000) == (True, "deploy_cold")
    assert rules.repin_plan(cold, CURRENT, NOW, pending=9_999) == (False, "under_floor")


def test_a_turns_prompt_is_counted_offline_by_its_size(monkeypatch):
    """tiktoken fetches its encoding over the network on first use, which a turn start must not wait on."""
    from ptc_agent.agent.middleware.compaction import utils

    monkeypatch.setattr(utils, "_get_tiktoken_encoder", lambda: pytest.fail("the turn start loaded tiktoken"))
    assert compaction.prompt_tokens("ab" * 1_000) == 1_000
    assert compaction.prompt_tokens([{"type": "text", "text": "中文"}, {"type": "text", "text": "ok"}]) == 4


def test_with_compaction_off_no_change_moves_a_session(monkeypatch):
    """The main agent makes no turn-start rebuild then, the urgent one included."""
    from ptc_agent.config.agent import CompactionConfig

    monkeypatch.setattr(compaction.release, "rebuild_before", lambda: 3)
    rules = CompactionRules.of(CompactionConfig(enabled=False), None)
    for stale in (records(minutes_ago=1, tokens=1_000, prompt_version=2), records(minutes_ago=61), records(minutes_ago=1, tokens=500_000)):
        assert rules.repin_plan(stale, CURRENT, NOW) == (False, "off")


_CLI_SUMMARY = (
    "This session is being continued from a previous conversation that ran out of context.\n\nSummary:\nthe work\n\n"
    "If you need specific details from before compaction (like exact code snippets, error messages, or content you "
    "generated), read the full transcript at: /tmp/claude-cfg-k2/projects/-tmp-t/s.jsonl\n\nNote: the earliest part of the "
    "conversation was too large to include and is NOT covered by this summary (the full transcript mentioned above still "
    "has it). Say so.\nContinue the conversation from where it left off."
)


@pytest.mark.parametrize("blocks", [False, True])
def test_the_summary_a_user_is_shown_keeps_the_clis_words_but_not_its_transcript_path(blocks):
    """The path is to a file in the compaction launch's own config dir, gone once that launch ends."""
    content = [{"type": "text", "text": _CLI_SUMMARY}] if blocks else _CLI_SUMMARY
    summary = {"uuid": "sum", "type": "user", "isCompactSummary": True, "message": {"role": "user", "content": content}}
    shown = SessionCompaction([], [{"session_id": "s", "entries": [summary]}])
    assert shown.notice()["summary_text"] == shown.summary_text == (
        "This session is being continued from a previous conversation that ran out of context.\n\nSummary:\nthe work"
        "\n\nNote: the earliest part of the conversation was too large to include and is NOT covered by this summary. "
        "Say so.\nContinue the conversation from where it left off."
    )


def _cold_rules(**changes) -> CompactionRules:
    return CompactionRules(150_000, **{"threshold": 500_000, "cold_tokens": 180_000, **changes})


def test_a_cold_session_compacts_at_the_cold_threshold_on_its_own_pin():
    """As the main agent summarizes a cold Claude thread: its turn's first
    request would write the whole context to the cache again otherwise."""
    on_pin = {"system_prompt": "new"}
    assert _cold_rules().repin_plan(records(minutes_ago=61, **on_pin), CURRENT, NOW) == (True, "cold")
    assert _cold_rules().repin_plan(records(minutes_ago=59, **on_pin), CURRENT, NOW) is None
    assert _cold_rules().repin_plan(records(minutes_ago=61, tokens=170_000, **on_pin), CURRENT, NOW) is None
    assert _cold_rules().repin_plan(records(minutes_ago=61, tokens=170_000, **on_pin), CURRENT, NOW, pending=10_000) == (True, "cold")
    assert _cold_rules(cold_tokens=600_000).repin_plan(records(minutes_ago=61, **on_pin), CURRENT, NOW) is None


def test_a_cold_compaction_is_judged_when_the_turn_was_sent():
    on_pin = records(minutes_ago=61, system_prompt="new")
    assert _cold_rules(sent_at=NOW - 30 * 60).repin_plan(on_pin, CURRENT, NOW) is None
    assert _cold_rules(sent_at=NOW).repin_plan(on_pin, CURRENT, NOW - 30 * 60) == (True, "cold")


def test_a_stale_pin_that_waits_compacts_for_a_cold_cache_too():
    inputs_only = records(minutes_ago=61, source_digest="build-2")
    assert CompactionRules(150_000, threshold=500_000).repin_plan(inputs_only, CURRENT, NOW) == (False, "inputs_only")
    assert _cold_rules().repin_plan(inputs_only, CURRENT, NOW) == (True, "cold")


def test_with_the_account_setting_off_or_a_send_that_keeps_the_full_context_nothing_compacts_cold():
    """The setting reaches the rules as #662's turn start reads it
    (``apply_compaction``), and a skip holds for its send alone."""
    from types import SimpleNamespace

    from ptc_agent.agent.middleware.runtime_context.turn_context import TurnContext
    from ptc_agent.config.agent import CompactionConfig
    from src.llms.preferences import Tuning
    from src.server.services.llm.config import apply_compaction

    def rules(choice, turn=None):
        config = SimpleNamespace(compaction=CompactionConfig(token_threshold=500_000, rebuild_floor_tokens=150_000,
                                                             cold_compact_tokens=180_000))
        apply_compaction(config, Tuning(cold_compaction=choice), None)
        return CompactionRules.of(config.compaction, turn)

    on_pin = records(minutes_ago=61, system_prompt="new")
    assert rules(None).repin_plan(on_pin, CURRENT, NOW) == (True, "cold")
    assert rules(False).repin_plan(on_pin, CURRENT, NOW) is None
    assert rules(None, TurnContext(keep_full_context=True)).repin_plan(on_pin, CURRENT, NOW) == (False, "kept_full_context")
    deploy = records(minutes_ago=61)
    assert rules(None, TurnContext(keep_full_context=True)).repin_plan(deploy, CURRENT, NOW) == (True, "deploy_cold")


def test_the_composer_is_told_what_the_next_turn_start_compacts():
    """By the turn start's own rules, from the rules the last turn ran under,
    with the time the cache expires for what waits on it."""
    expires = NOW - 60 + 3600
    rules = _cold_rules().reading()
    on_pin = {"system_prompt": "new", "source_digest": "build-2"}
    assert compaction.next_summary(rules, records(minutes_ago=1, **on_pin), settled_at=None) == {"reason": "cold", "at": expires}
    assert compaction.next_summary(rules, records(minutes_ago=1, tokens=170_000, **on_pin), settled_at=None) is None
    assert compaction.next_summary({**rules, "cold_tokens": 0}, records(minutes_ago=1, **on_pin), settled_at=None) is None
    assert compaction.next_summary(rules, records(minutes_ago=1, tokens=500_000, **on_pin), settled_at=None) == {"reason": "threshold"}
    assert compaction.next_summary(rules, records(minutes_ago=1), settled_at=None) == {"reason": "rebuild", "at": expires}
    assert compaction.next_summary(rules, {**records(minutes_ago=1), "repin_due": True}, settled_at=None) == {"reason": "rebuild"}
    settled = datetime.fromtimestamp(NOW, UTC).replace(tzinfo=None)
    assert compaction.next_summary(rules, records(minutes_ago=1, **on_pin), settled_at=settled) == {"reason": "cold", "at": NOW + 3600}


@pytest.mark.asyncio
async def test_the_transcripts_save_counts_against_the_compactions_budget(monkeypatch):
    """Admission holds a send for about that budget, so a slow save and a slow
    summary together still end inside it."""
    import asyncio
    import time

    from src.llms.claude_code import ClaudeTurnFailed

    async def slow(value, seconds=0.6):
        await asyncio.sleep(seconds)
        return value

    async def compacted(*_args, **kwargs):
        return await slow([{"session_id": "s", "entries": [], "note": kwargs["transcript_note"]}])

    monkeypatch.setattr(compaction, "get_compaction_timeout", lambda: 1.0)
    monkeypatch.setattr(compaction, "_compacted", compacted)
    started = time.monotonic()
    with pytest.raises(ClaudeTurnFailed, match="within 1s"):
        await compaction.compact(None, {}, None, thread_key="t", identity="u", records={}, transcript_note=slow("the note"))
    assert time.monotonic() - started < 1.15
    written = await compaction.compact(None, {}, None, thread_key="t", identity="u", records={}, transcript_note=slow("the note", 0.2))
    assert written[0]["note"] == "the note"
