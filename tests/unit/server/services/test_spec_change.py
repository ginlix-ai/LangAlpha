"""The spec change's client contract and the manager's run around it.

Locks the one mapper from a failure to what the client is told, how a stored
change reads back, that a change settles exactly once under the machine's
locks whatever happens (cancellation included), and the compensator that
hands a replaced machine back.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.models.computer import ComputerStatus
from src.server.services.computer_errors import (
    ComputerBusyError,
    DiskTooSmallError,
    MachineBusyError,
    SpecChangeLostError,
)
from src.server.services.persistence.sync_result import (
    BackupIncomplete,
    UnsavedFile,
)
from src.server.services.spec_change import (
    SPEC_CHANGE_FAILED,
    SPEC_CHANGE_INTERRUPTED,
    SPEC_CHANGE_NOT_ALLOWED,
    spec_change_error,
    spec_change_from_row,
    spec_refusal,
)

_SPEC = "src.server.services.computer_manager._spec"


class TestMapper:
    @pytest.mark.parametrize(
        ("exc", "code", "message"),
        [
            (asyncio.CancelledError(), "interrupted", SPEC_CHANGE_INTERRUPTED),
            (MachineBusyError("turn running"), "turn_active", "turn running"),
            (ComputerBusyError("mid-operation"), "busy", "mid-operation"),
            (DiskTooSmallError("won't fit"), "disk_too_small", "won't fit"),
            # The scope gate's own string is for a developer, not the user.
            (
                HTTPException(403, "Requires scope: workspace:spec:max"),
                "not_allowed",
                SPEC_CHANGE_NOT_ALLOWED,
            ),
            (
                HTTPException(429, {"message": "Plan says no", "limit_type": "x"}),
                "not_allowed",
                "Plan says no",
            ),
            (HTTPException(403), "not_allowed", SPEC_CHANGE_NOT_ALLOWED),
            (HTTPException(403, {"code": "x"}), "not_allowed", SPEC_CHANGE_NOT_ALLOWED),
            (HTTPException(429, {"message": None}), "not_allowed", SPEC_CHANGE_NOT_ALLOWED),
            (HTTPException(429, {"message": ""}), "not_allowed", SPEC_CHANGE_NOT_ALLOWED),
            (RuntimeError("sandbox sb-1 exploded"), "unknown", SPEC_CHANGE_FAILED),
        ],
    )
    def test_every_code(self, exc, code, message):
        error = spec_change_error(exc)
        assert (error.code, error.message, error.files) == (code, message, [])

    def test_an_incomplete_backup_names_its_files_without_operator_detail(self):
        unsaved = [UnsavedFile(f"f{i}.bin", "too_large", i) for i in range(25)]
        exc = BackupIncomplete("ws-123 sb-9 strict mirror failed", unsaved)
        error = spec_change_error(exc)
        assert error.code == "backup_incomplete"
        assert error.message == exc.user_message
        assert "sb-9" not in error.message
        assert len(error.files) == 20
        assert error.files[0].model_dump() == {
            "path": "f0.bin",
            "reason": "too_large",
            "size": 0,
        }

    def test_a_refusal_has_the_outcome_shape(self):
        detail = spec_refusal(
            "backup_incomplete", "Nothing was changed", [UnsavedFile("a", "unreadable")]
        )
        assert detail == {
            "code": "backup_incomplete",
            "message": "Nothing was changed",
            "files": [{"path": "a", "reason": "unreadable", "size": None}],
        }
        assert spec_refusal("spec_in_progress", "m")["files"] == []


def _row(stale=None, **change):
    row = {
        "computer_id": "c1",
        "spec_change": {
            "target_tier": "max",
            "from_tier": "standard",
            "state": "in_progress",
            "error": None,
            "started_at": "2020-01-01T00:00:00+00:00",
            "finished_at": None,
            **change,
        },
    }
    if stale is not None:
        row["spec_change_stale"] = stale
    return row


class TestReadBack:
    def test_the_database_decides_staleness_not_the_app_clock(self):
        """Started years ago by this process's clock, still fresh by the row's."""
        change = spec_change_from_row(_row(stale=False, claim_id="c-1"))
        assert change.state == "in_progress"
        assert change.claim_id == "c-1"

    def test_a_stale_change_reads_as_interrupted(self):
        change = spec_change_from_row(_row(stale=True))
        assert change.state == "failed"
        assert change.error.code == "interrupted"
        assert change.error.message == SPEC_CHANGE_INTERRUPTED
        # The tier it was moving from is still reported, never rewritten.
        assert change.from_tier == "standard"

    def test_a_row_from_before_claim_ids_reads_sanely(self):
        change = spec_change_from_row(
            _row(
                state="failed",
                error={"code": "unknown", "message": "m"},
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        assert change.claim_id is None
        assert change.error.files == []

    def test_no_change_and_an_unreadable_one_read_as_none(self):
        assert spec_change_from_row({"spec_change": None}) is None
        assert spec_change_from_row({"spec_change": {"state": "nope"}}) is None


# ---------------------------------------------------------------------------
# The manager's run: settled once, under the locks, whatever happens.
# ---------------------------------------------------------------------------


def _computer(**overrides):
    return {
        "computer_id": "comp-1",
        "user_id": "user-1",
        "kind": "daytona",
        "provider_ref": "sb-1",
        "status": "running",
        "resource_tier": "standard",
        **overrides,
    }


@pytest.fixture
def manager():
    from src.server.services.workspace_manager import WorkspaceManager

    WorkspaceManager.reset_instance()
    config = MagicMock()
    config.sandbox.provider = "daytona"
    tiers = {
        "standard": MagicMock(disk=3),
        "max": MagicMock(disk=10),
    }
    config.to_core_config.return_value.sandbox.daytona = MagicMock(
        resource_tiers=tiers
    )
    wm = WorkspaceManager.get_instance(config=config)
    wm._provider_settings = MagicMock(return_value=MagicMock(resource_tiers=tiers))
    wm._machine_has_active_tasks = AsyncMock(return_value=False)
    yield wm
    WorkspaceManager.reset_instance()


@pytest.fixture
def run_env(manager):
    """Locks that record when they are held, and a settle that reads them."""
    events = []

    @asynccontextmanager
    async def decision(_computer_id):
        events.append("lock")
        try:
            yield True
        finally:
            events.append("unlock")

    async def settle(_computer_id, *, claim_id, error=None):
        events.append(("settle", claim_id, error and error["code"]))
        return {"status": "running"}

    manager._machine_decision_lock = decision
    with (
        patch(f"{_SPEC}.get_computer", AsyncMock(return_value=_computer())),
        patch(f"{_SPEC}.heartbeat_computer_spec_change", AsyncMock(return_value=True)),
        patch(f"{_SPEC}.settle_computer_spec_change", AsyncMock(side_effect=settle)),
        patch(f"{_SPEC}.publish_computer_status_change", AsyncMock()) as publish,
        patch(
            "src.server.dependencies.usage_limits.platform_gating_active",
            return_value=False,
        ),
    ):
        yield events, publish


@pytest.mark.asyncio
class TestRunSettles:
    async def test_a_failure_settles_once_before_the_lock_releases(
        self, manager, run_env
    ):
        events, publish = run_env
        manager._apply_spec_change = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            await manager._run_spec_change(
                "comp-1", "max", claim_id="c-1", user_id="user-1"
            )

        assert events == ["lock", ("settle", "c-1", "unknown"), "unlock"]
        publish.assert_awaited_once_with(
            "comp-1", "running", extra={"spec_change": "failed"}
        )

    async def test_a_success_settles_under_the_lock(self, manager, run_env):
        events, _ = run_env
        manager._apply_spec_change = AsyncMock()

        await manager._run_spec_change("comp-1", "max", claim_id="c-1", user_id=None)

        assert events == ["lock", ("settle", "c-1", None), "unlock"]

    async def test_cancellation_settles_as_interrupted(self, manager, run_env):
        events, _ = run_env
        started = asyncio.Event()

        async def hang(*_a, **_k):
            started.set()
            await asyncio.sleep(3600)

        manager._apply_spec_change = AsyncMock(side_effect=hang)
        task = asyncio.create_task(
            manager._run_spec_change("comp-1", "max", claim_id="c-1", user_id=None)
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert ("settle", "c-1", "interrupted") in events
        assert events.count(("settle", "c-1", "interrupted")) == 1

    async def test_a_busy_machine_lock_settles_busy(self, manager, run_env):
        events, _ = run_env

        @asynccontextmanager
        async def held(_computer_id):
            yield False

        manager._machine_decision_lock = held
        with pytest.raises(ComputerBusyError):
            await manager._run_spec_change(
                "comp-1", "max", claim_id="c-1", user_id=None
            )
        assert events == [("settle", "c-1", "busy")]

    async def test_a_refused_plan_settles_not_allowed(self, manager, run_env):
        events, _ = run_env

        @asynccontextmanager
        async def capacity(_owner):
            yield

        with (
            patch(
                "src.server.dependencies.usage_limits.platform_gating_active",
                return_value=True,
            ),
            patch(f"{_SPEC}.computer_capacity_lock", capacity),
            patch(
                "src.server.dependencies.usage_limits.assert_spec_allowed",
                AsyncMock(side_effect=HTTPException(403, "Upgrade to use max")),
            ),
        ):
            with pytest.raises(HTTPException):
                await manager._run_spec_change(
                    "comp-1", "max", claim_id="c-1", user_id=None
                )
        assert events == [("settle", "c-1", "not_allowed")]

    async def test_an_accepted_run_never_raises(self, manager, run_env):
        manager._apply_spec_change = AsyncMock(side_effect=ValueError("bad"))
        await manager.run_accepted_spec_change(
            "comp-1", "max", user_id="user-1", claim_id="c-1"
        )

    async def test_the_run_carries_its_claim_to_the_apply(self, manager, run_env):
        """Every destructive step proves ownership against the claim that
        started the run, so the apply must know which claim that is."""
        manager._apply_spec_change = AsyncMock()
        await manager._run_spec_change("comp-1", "max", claim_id="c-1", user_id=None)
        assert manager._apply_spec_change.await_args.kwargs["claim_id"] == "c-1"

    async def test_a_lost_claim_aborts_before_the_tier_is_persisted(
        self, manager, run_env
    ):
        """The claim was taken over while this runner waited for the machine
        lock. The row is the other runner's now: nothing here may write it."""
        events, _ = run_env
        with (
            patch(f"{_SPEC}.heartbeat_computer_spec_change", AsyncMock(return_value=False)),
            patch(f"{_SPEC}.db_set_computer_resource_tier", AsyncMock()) as persist,
        ):
            with pytest.raises(SpecChangeLostError):
                await manager._run_spec_change(
                    "comp-1", "max", claim_id="c-1", user_id=None
                )
        persist.assert_not_awaited()
        # The settle is still attempted; on the real row it matches nothing.
        assert ("settle", "c-1", "unknown") in events


@pytest.mark.asyncio
class TestHeartbeat:
    async def test_renews_the_claim_while_the_change_runs(self, manager):
        manager.spec_change_heartbeat_s = 0.01
        beat = AsyncMock(return_value=True)
        with patch(f"{_SPEC}.heartbeat_computer_spec_change", beat):
            async with manager._spec_change_heartbeat("comp-1", "c-1"):
                await asyncio.sleep(0.08)
        assert beat.await_count >= 2
        assert beat.await_args.kwargs == {"claim_id": "c-1"}

    async def test_stops_once_the_row_no_longer_answers_to_the_claim(self, manager):
        manager.spec_change_heartbeat_s = 0.01
        beat = AsyncMock(side_effect=[True, False, True])
        with patch(f"{_SPEC}.heartbeat_computer_spec_change", beat):
            async with manager._spec_change_heartbeat("comp-1", "c-1"):
                await asyncio.sleep(0.1)
        assert beat.await_count == 2

    async def test_a_failed_renewal_is_retried_not_fatal(self, manager):
        manager.spec_change_heartbeat_s = 0.01
        beat = AsyncMock(side_effect=[OSError("db blip"), True, True])
        with patch(f"{_SPEC}.heartbeat_computer_spec_change", beat):
            async with manager._spec_change_heartbeat("comp-1", "c-1"):
                await asyncio.sleep(0.08)
        assert beat.await_count >= 2


class TestSameTierShortcut:
    """A row's tier is only the truth when no stale change wrote it."""

    def _refusal(self, **row):
        from src.server.services.computer_manager._spec import _spec_refusal

        tiers = {"standard": object(), "max": object()}
        return _spec_refusal(
            {
                "kind": "daytona",
                "resource_tier": "max",
                "status": "stopped",
                "provider_ref": "sb-1",
                **row,
            },
            "max",
            tiers,
        )

    def test_the_same_tier_is_nothing_to_change(self):
        assert self._refusal(spec_change_stale=False) is True

    def test_a_stale_change_that_persisted_this_tier_is_not_a_no_op(self):
        """The dead worker persisted max before rebuilding anything, so the
        machine may still be at standard: the claim reverts the tier and the
        change has to run."""
        assert self._refusal(spec_change_stale=True) is False

    @pytest.mark.asyncio
    async def test_a_repeat_during_a_running_change_is_refused_not_done(self, manager):
        """The running change persisted max early, so without the guard the
        deprecated workspace route would answer a repeat as already done."""
        row = {**_computer(resource_tier="max"), **_row(stale=False)}
        with pytest.raises(ComputerBusyError):
            await manager.precheck_computer_spec(row, "max")


@pytest.mark.asyncio
class TestReplacementClaim:
    @pytest.fixture
    def status(self):
        with (
            patch(
                f"{_SPEC}.try_claim_computer_for_start",
                AsyncMock(return_value={"status": "starting"}),
            ),
            patch(f"{_SPEC}.update_computer_status", AsyncMock()) as status,
        ):
            yield status

    async def test_hands_the_machine_back_as_it_was_before_teardown(
        self, manager, status
    ):
        with pytest.raises(RuntimeError):
            async with manager._replacement_claim(
                "comp-1", from_status=ComputerStatus.RUNNING, sandbox_id="sb-1"
            ):
                raise RuntimeError("backup failed")
        status.assert_awaited_once_with(
            "comp-1", ComputerStatus.RUNNING, expected=ComputerStatus.STARTING
        )

    async def test_hands_it_back_stopped_once_teardown_began(self, manager, status):
        with pytest.raises(RuntimeError):
            async with manager._replacement_claim(
                "comp-1", from_status=ComputerStatus.RUNNING, sandbox_id="sb-1"
            ) as claim:
                claim.release_to = ComputerStatus.STOPPED
                raise RuntimeError("recreate failed")
        status.assert_awaited_once_with(
            "comp-1", ComputerStatus.STOPPED, expected=ComputerStatus.STARTING
        )

    async def test_releases_on_cancellation(self, manager, status):
        with pytest.raises(asyncio.CancelledError):
            async with manager._replacement_claim(
                "comp-1", from_status=ComputerStatus.RUNNING, sandbox_id="sb-1"
            ):
                raise asyncio.CancelledError()
        status.assert_awaited_once()

    async def test_a_success_keeps_the_claim_unless_asked(self, manager, status):
        async with manager._replacement_claim(
            "comp-1", from_status=ComputerStatus.RUNNING, sandbox_id="sb-1"
        ):
            pass
        status.assert_not_awaited()

        async with manager._replacement_claim(
            "comp-1",
            from_status=ComputerStatus.STOPPED,
            sandbox_id="sb-1",
            release_on_success=True,
        ):
            pass
        status.assert_awaited_once_with(
            "comp-1", ComputerStatus.STOPPED, expected=ComputerStatus.STARTING
        )

    async def test_a_lost_claim_refuses_busy(self, manager):
        with patch(f"{_SPEC}.try_claim_computer_for_start", AsyncMock(return_value=None)):
            with pytest.raises(ComputerBusyError):
                async with manager._replacement_claim(
                    "comp-1", from_status=ComputerStatus.RUNNING, sandbox_id="sb-1"
                ):
                    pytest.fail("entered without the claim")


@pytest.mark.asyncio
async def test_a_retried_delete_is_logged_at_info(manager, caplog):
    class Gone(Exception):
        pass

    runtime = MagicMock()
    runtime.delete = AsyncMock(side_effect=[RuntimeError("state change in progress"), None])
    gets = {"n": 0}

    async def get(_sandbox_id):
        gets["n"] += 1
        if gets["n"] > 2:
            raise Gone("404")
        return runtime

    provider = MagicMock(get=AsyncMock(side_effect=get), close=AsyncMock())
    manager._provider_for = MagicMock(return_value=provider)
    manager._is_sandbox_gone = lambda e, _binding=None: isinstance(e, Gone)

    with (
        patch(f"{_SPEC}.asyncio.sleep", new=AsyncMock()),
        caplog.at_level(logging.INFO, logger=_SPEC),
    ):
        await manager._destroy_sandbox("sb-1")

    [record] = [r for r in caplog.records if "accepted after" in r.getMessage()]
    assert record.levelno == logging.INFO
    assert "1 refusal" in record.getMessage()


@pytest.mark.asyncio
async def test_an_empty_machine_reads_its_metrics_before_its_session_deletes_it(
    manager,
):
    order: list[str] = []
    manager._claim_machine_for_stop = AsyncMock(return_value="claim")
    manager._machine_has_active_tasks = AsyncMock(return_value=False)
    manager._settle_machine_stop = AsyncMock()
    manager._cancel_mcp_discovery = MagicMock()
    manager._fold_usage_before_delete = AsyncMock(
        side_effect=lambda computer_id: order.append(f"fold {computer_id}")
    )
    # The session's cleanup is what deletes a live sandbox.
    manager._clear_session = AsyncMock(side_effect=lambda _c: order.append("delete"))
    manager._destroy_sandbox = AsyncMock()

    with (
        patch(f"{_SPEC}.get_live_workspace_ids_for_computer", AsyncMock(return_value=[])),
        patch(f"{_SPEC}.clear_computer_disk", AsyncMock()),
        patch(f"{_SPEC}.SessionManager"),
    ):
        assert await manager._drop_empty_machine_sandbox(
            {"computer_id": "c-1", "user_id": "u-1", "kind": "daytona"}, "sb-1"
        )

    assert order == ["fold c-1", "delete"]
