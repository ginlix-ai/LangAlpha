"""A task run on Claude Code is admitted only while its computer has room for its CLI."""

from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import TaskRunRejected
from src.server.database.runs import subagent_runs as sr_db
from src.server.services.subagent_run_coordinator import SubagentRunCoordinator

COORD_SRDB = "src.server.services.subagent_run_coordinator.sr_db"
COMPUTER = "src.server.database.computer.get_computer_for_thread"
MEDIUM = {"computer_id": "c-1", "kind": "daytona", "resource_tier": "performance"}


@pytest.mark.asyncio
async def test_a_claude_code_run_over_its_computers_cap_is_refused_with_the_reason_the_agent_reads(monkeypatch):
    start = AsyncMock(side_effect=sr_db.ClaudeCodeCapError(5))
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(COMPUTER, AsyncMock(return_value=MEDIUM))
    monkeypatch.setattr("src.server.services.claude_code.hosted.cli_cap", lambda computer: 5)

    with pytest.raises(TaskRunRejected) as refused:
        await SubagentRunCoordinator("t-1").start_task_run(task_id="k1", cause="init", claude_code=True)

    assert refused.value.reason == (
        "Claude Code is already running 5 agents on this computer, the most it runs at once. "
        "Wait for one of your subagents to finish, then launch this one again"
    )
    # It lifts once a CLI on the computer ends, so it counts against nothing.
    assert refused.value.busy is True
    assert start.await_args.kwargs["claude_code"] is True
    assert start.await_args.kwargs["cli_cap"] == ("c-1", 5)


@pytest.mark.asyncio
@pytest.mark.parametrize(("claude_code", "computer", "cap"), [(False, MEDIUM, 5), (True, None, 5), (True, MEDIUM, None)])
async def test_a_run_counted_against_no_cap_is_admitted_on_the_ledger_alone(monkeypatch, claude_code, computer, cap):
    """A run on another model runs no CLI; a thread on no computer, or one of
    a size this server cannot tell, has no count to keep."""
    start = AsyncMock(side_effect=RuntimeError("stop here"))
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(COMPUTER, AsyncMock(return_value=computer))
    monkeypatch.setattr("src.server.services.claude_code.hosted.cli_cap", lambda computer: cap)

    with pytest.raises(RuntimeError):
        await SubagentRunCoordinator("t-1").start_task_run(task_id="k1", cause="init", claude_code=claude_code)

    assert start.await_args.kwargs["cli_cap"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host_mode", "claude_code", "stored"),
    [("platform", True, True), ("oss", True, False), ("platform", False, False)],
)
async def test_a_run_is_stamped_with_the_stored_token_its_build_signs_in_with(monkeypatch, host_mode, claude_code, stored):
    """From the build that launches it, not the thread's latest turn, which
    may have moved to another credential since."""
    start = AsyncMock(side_effect=RuntimeError("stop here"))
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(COMPUTER, AsyncMock(return_value=None))
    monkeypatch.setattr("src.config.settings.HOST_MODE", host_mode)

    with pytest.raises(RuntimeError):
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="k1", cause="init", claude_code=claude_code, stored_token=True
        )

    assert start.await_args.kwargs["stored_token"] is stored


PLAN = "src.server.services.llm.claude_code.subagent_plan_refusal"


@pytest.mark.asyncio
async def test_a_claude_code_run_the_plan_holds_is_refused_in_its_own_words_and_never_recorded(monkeypatch):
    start = AsyncMock()
    refusal = AsyncMock(return_value="Not started: the Claude plan's limit is used up.")
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(COMPUTER, AsyncMock(return_value=MEDIUM))
    monkeypatch.setattr(PLAN, refusal)

    with pytest.raises(TaskRunRejected) as refused:
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="k1", cause="init", claude_code=True, stored_token=True, claude_code_model="claude-sonnet-5-5"
        )

    assert (refused.value.reason, refused.value.whole, refused.value.busy) == (
        "Not started: the Claude plan's limit is used up.", True, False,
    )
    refusal.assert_awaited_once_with(MEDIUM, "claude-sonnet-5-5", True)
    start.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_replayed_task_call_whose_run_started_is_told_so_rather_than_refused_for_the_plan(monkeypatch):
    run = {"task_run_id": "r-1", "launch_tool_call_id": "call-1"}
    start = AsyncMock(side_effect=sr_db.DuplicateLaunchError(run))
    found = AsyncMock(return_value=run)
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(f"{COORD_SRDB}.find_run_by_launch_call", found)
    monkeypatch.setattr(COMPUTER, AsyncMock(return_value=MEDIUM))
    monkeypatch.setattr(PLAN, AsyncMock(return_value="Not started: the Claude plan's limit is used up."))
    monkeypatch.setattr("src.server.services.claude_code.hosted.cli_cap", lambda computer: 5)

    with pytest.raises(TaskRunRejected) as refused:
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="k1",
            cause="init",
            parent_run_id="p-1",
            launch_tool_call_id="call-1",
            claude_code=True,
            claude_code_model="claude-sonnet-5-5",
        )

    assert "already spawned run r-1" in refused.value.reason
    found.assert_awaited_once_with("p-1", "call-1")


@pytest.mark.asyncio
@pytest.mark.parametrize(("claude_code", "computer", "asked"), [(True, MEDIUM, True), (False, MEDIUM, False), (True, None, False)])
async def test_a_run_the_plan_does_not_hold_is_admitted_on_the_ledger(monkeypatch, claude_code, computer, asked):
    """A run on a platform model starts as it would, and so does a Claude
    Code run while the plan has room."""
    start = AsyncMock(side_effect=RuntimeError("stop here"))
    refusal = AsyncMock(return_value=None)
    monkeypatch.setattr(f"{COORD_SRDB}.start_task_run", start)
    monkeypatch.setattr(COMPUTER, AsyncMock(return_value=computer))
    monkeypatch.setattr(PLAN, refusal)
    monkeypatch.setattr("src.server.services.claude_code.hosted.cli_cap", lambda computer: 5)

    with pytest.raises(RuntimeError):
        await SubagentRunCoordinator("t-1").start_task_run(
            task_id="k1", cause="init", claude_code=claude_code, claude_code_model="claude-sonnet-5-5"
        )

    assert refusal.await_count == int(asked)
    start.assert_awaited_once()

