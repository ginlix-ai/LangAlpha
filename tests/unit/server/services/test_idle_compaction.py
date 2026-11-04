"""Idle compaction's arming and claiming (``services.idle_compaction``): when a
completed turn's firing comes, why one is not armed, and that a worker never
holds more firings than its cap.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.agent import CompactionConfig, IdleCompactConfig
from src.config import features
from src.server.services import idle_compaction
from src.server.services.idle_compaction import Firing, IdleCompactionSweeper, arm_after_turn, plan_firing, start_sweeper
from src.server.services.llm.config import apply_compaction
from src.llms.preferences import Tuning

T = 1_000_000.0
SETTINGS = CompactionConfig()
LEAD = SETTINGS.idle_compact.lead_seconds


def _feature(monkeypatch, on: bool) -> None:
    spec = features.FEATURES["idle_compaction"]
    monkeypatch.setitem(features.FEATURES, "idle_compaction", dataclasses.replace(spec, enabled=on))


@pytest.fixture(autouse=True)
def feature_on(monkeypatch):
    """The deployment turned the idle_compaction feature on."""
    _feature(monkeypatch, True)


def _at(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def _clock(**fields) -> dict:
    return {"requested_at": T, "tail_requested_at": T - 600, "digest": "d", **fields}


def _answer(**fields) -> dict:
    """What the turn's answer resolved for the user (``answer_reading``)."""
    return {"threshold": 500_000, "floor_tokens": 100_000, "cold_tokens": 300_000, "idle_tokens": 300_000, **fields}


def _plan(
    clock: dict | None = None, *, answer: dict | None = None, tokens: int = 200_000, stop_at: datetime | None = None
):
    return plan_firing(clock or _clock(), answer or _answer(), tokens=tokens, answered_at=T, stop_at=stop_at, lead=LEAD)


def test_a_rebuild_fires_lead_seconds_before_the_last_answers_entry_expires():
    assert _plan() == Firing("I1", _at(T + 3600 - LEAD), _at(T + 3600 - 300))


def test_from_cold_tokens_the_summary_keeping_the_tail_fires_off_its_older_entry():
    assert _plan(tokens=350_000) == Firing("I2", _at(T - 600 + 3600 - LEAD), _at(T - 600 + 3600 - 300))
    assert _plan(_clock(tail_requested_at=None), tokens=350_000).kind == "I1"


def test_a_slot_inside_the_reading_pause_falls_to_the_newer_entry_or_to_none():
    """Two thirds of the gap between the answer and the slot: a closer slot
    is a pause to read the answer, whose next message finds the cache warm."""
    assert _plan(_clock(tail_requested_at=T - 1_200), tokens=350_000).kind == "I1"
    assert _plan(_clock(requested_at=T - 1_000)) == "reading_pause"


def test_a_firing_comes_before_the_computers_idle_stop_or_not_at_all():
    gap = (3600 - LEAD) * 2 / 3
    assert _plan(stop_at=_at(T + gap + 300)).fire_at == _at(T + gap)
    assert _plan(stop_at=_at(T + gap + 299)) == "idle_stop"


def test_no_firing_at_the_threshold_or_under_the_floor():
    assert _plan(tokens=500_000) == "over_threshold"
    assert _plan(tokens=99_999) == "under_floor"


def test_a_user_who_turned_off_the_summary_after_a_break_gets_only_a_rebuild():
    """cold_compaction off resolves the turn's cold_tokens, and with them its
    idle_tokens, to 0, whatever idle_compact.threshold_tokens says."""
    off = _answer(cold_tokens=0, idle_tokens=0)
    assert _plan(answer=off, tokens=350_000).kind == "I1"
    assert _plan(answer=_answer(cold_tokens=0, idle_tokens=0, floor_tokens=0), tokens=350_000) == "under_floor"


@pytest.mark.parametrize("threshold_tokens", [None, 200_000])
def test_the_idle_threshold_resolves_to_0_with_the_summary_after_a_break_off(threshold_tokens):
    config = SimpleNamespace(
        compaction=CompactionConfig(idle_compact=IdleCompactConfig(threshold_tokens=threshold_tokens))
    )
    assert config.compaction.idle_tokens == (threshold_tokens or 300_000)

    apply_compaction(config, Tuning(cold_compaction=False), None)

    assert config.compaction.idle_tokens == 0


def test_the_idle_threshold_defaults_to_cold_compact_tokens():
    assert CompactionConfig(cold_compact_tokens=250_000).idle_tokens == 250_000
    assert CompactionConfig(idle_compact=IdleCompactConfig(threshold_tokens=180_000)).idle_tokens == 180_000


def test_a_summary_keeping_the_tail_arms_from_the_idle_threshold_alone():
    """Apart from cold_compact_tokens: here the cold threshold (400k) is never
    reached, and nothing else would arm (no floor)."""
    apart = _answer(cold_tokens=400_000, idle_tokens=300_000, floor_tokens=0)
    assert _plan(answer=apart, tokens=299_999) == "under_floor"
    assert _plan(answer=apart, tokens=300_000).kind == "I2"
    assert _plan(answer=apart, tokens=350_000).kind == "I2"


# -- arming after a completed turn --------------------------------------------


def _saved(clock: dict | None, *, answer: dict | None = None, pending_writes=()) -> SimpleNamespace:
    values = {
        "_cached_input_tokens": 200_000,
        "_cached_output_tokens": 0,
        "_last_model_response_at": T,
        "_last_answer": answer or _answer(),
    }
    if clock is not None:
        values["_idle_clock"] = clock
    return SimpleNamespace(checkpoint={"channel_values": values}, pending_writes=list(pending_writes))


@pytest.fixture
def armed():
    """The reads an arm makes, for run-1 on a running computer, and the
    table it writes."""
    latest = AsyncMock(return_value={"conversation_response_id": "run-1", "run_seq": 7, "created_at": _at(T - 60)})
    saver = SimpleNamespace(aget_tuple=AsyncMock(return_value=_saved(_clock())))
    arm, disarm = AsyncMock(), AsyncMock()
    with (
        patch("src.server.app.setup.agent_config", SimpleNamespace(compaction=SETTINGS)),
        patch("src.server.database.runs.lifecycle.get_latest_attempt", latest),
        patch(
            "src.server.database.conversation.threads_read.get_thread_branch_read",
            AsyncMock(return_value=SimpleNamespace(checkpoint_id="cp-1")),
        ),
        patch("src.server.utils.checkpoint_helpers.get_checkpointer", return_value=saver),
        patch("src.server.database.computer.get_computer_for_workspace", AsyncMock(return_value={"status": "running"})),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=SimpleNamespace(idle_stop_window=lambda computer: None),
        ),
        patch.object(idle_compaction.idle_db, "arm", arm),
        patch.object(idle_compaction.idle_db, "disarm", disarm),
    ):
        yield SimpleNamespace(latest=latest, saver=saver, arm=arm, disarm=disarm)


JOB = {"payload": {"thread_id": "t-1", "run_id": "run-1", "user_id": "u-1", "workspace_id": "ws-1"}}


@pytest.mark.asyncio
async def test_a_completed_turn_arms_its_firing_on_the_checkpoint_it_committed(armed):
    await arm_after_turn(JOB)

    armed.arm.assert_awaited_once_with(
        "t-1",
        kind="I1",
        fire_at=_at(T + 3600 - LEAD),
        deadline=_at(T + 3600 - 300),
        base_checkpoint_id="cp-1",
        armed_by_run_id="run-1",
        armed_run_seq=7,
        user_id="u-1",
    )
    armed.disarm.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_the_feature_off_a_job_arms_nothing_and_reads_nothing(armed, monkeypatch):
    _feature(monkeypatch, False)

    await arm_after_turn(JOB)

    armed.latest.assert_not_awaited()
    armed.arm.assert_not_awaited()
    armed.disarm.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_the_feature_off_no_sweeper_starts(monkeypatch):
    sweeper = IdleCompactionSweeper()
    monkeypatch.setattr(IdleCompactionSweeper, "_instance", sweeper)
    _feature(monkeypatch, False)

    assert await start_sweeper(AsyncMock()) is False
    assert sweeper._task is None

    _feature(monkeypatch, True)
    assert await start_sweeper(AsyncMock()) is True
    assert sweeper._task is not None
    await sweeper.stop()


@pytest.mark.asyncio
async def test_a_job_that_runs_after_a_later_turn_leaves_that_turns_firing(armed):
    armed.latest.return_value = {"conversation_response_id": "run-2", "run_seq": 8, "created_at": _at(T)}

    await arm_after_turn(JOB)

    armed.arm.assert_not_awaited()
    armed.disarm.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_turn_that_leaves_nothing_to_fire_drops_the_earlier_turns_firing(armed, caplog):
    caplog.set_level("INFO")
    armed.saver.aget_tuple.return_value = _saved(_clock(), answer=_answer(threshold=100_000))
    await arm_after_turn(JOB)
    armed.saver.aget_tuple.return_value = _saved(None)
    await arm_after_turn(JOB)
    armed.saver.aget_tuple.return_value = _saved(_clock(requested_at=T - 120))
    await arm_after_turn(JOB)

    assert [call.args for call in armed.disarm.await_args_list] == [("t-1", 7)] * 3
    armed.arm.assert_not_awaited()
    assert "idle=skip reason=over_threshold" in caplog.text and "reason=stale_clock" in caplog.text
    assert "no_clock" not in caplog.text


# -- claiming ------------------------------------------------------------------


@pytest.fixture
def table(monkeypatch):
    """The firings a sweep finds: ``expired`` past their deadline, and as many
    due as it asks for."""
    rows = SimpleNamespace(expired=[])
    rows.drop_expired = AsyncMock(side_effect=lambda: rows.expired)
    rows.claim_due = AsyncMock(side_effect=lambda limit: [{"thread_id": f"t-{n}", "kind": "I1"} for n in range(limit)])
    monkeypatch.setattr(idle_compaction.idle_db, "drop_expired", rows.drop_expired)
    monkeypatch.setattr(idle_compaction.idle_db, "claim_due", rows.claim_due)
    return rows


@pytest.mark.asyncio
async def test_a_worker_claims_no_more_firings_than_its_cap(table):
    released = asyncio.Event()
    claimed = table.claim_due
    sweeper = IdleCompactionSweeper()
    sweeper._fire = lambda row: released.wait()

    assert await sweeper.sweep_once() == idle_compaction.MAX_FIRINGS
    assert await sweeper.sweep_once() == 0
    claimed.assert_awaited_once_with(idle_compaction.MAX_FIRINGS)
    released.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert await sweeper.sweep_once() == idle_compaction.MAX_FIRINGS


@pytest.mark.asyncio
async def test_a_firing_that_ends_wakes_the_sweep_for_its_slot(table):
    sweeper = IdleCompactionSweeper()
    sweeper._fire = AsyncMock()

    await sweeper.sweep_once()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert sweeper._wake.is_set()


@pytest.mark.asyncio
async def test_firings_past_their_deadline_are_dropped_unclaimed_and_logged(table, caplog):
    caplog.set_level("INFO")
    table.expired = [{"thread_id": "t-old", "kind": "I2"}]
    sweeper = IdleCompactionSweeper()
    sweeper._fire = AsyncMock()

    await sweeper.sweep_once()

    assert "idle=discard reason=past_deadline kind=I2 thread_id=t-old" in caplog.text


@pytest.mark.asyncio
async def test_a_claim_that_returns_after_a_stop_starts_nothing(table, caplog):
    caplog.set_level("INFO")
    sweeper = IdleCompactionSweeper()
    sweeper._fire = AsyncMock()

    async def claim(limit):
        sweeper._stopping.set()
        return [{"thread_id": "t-1", "kind": "I1"}]

    table.claim_due.side_effect = claim

    assert await sweeper.sweep_once() == 0
    sweeper._fire.assert_not_called()
    assert "idle=discard reason=shutdown kind=I1 thread_id=t-1" in caplog.text
