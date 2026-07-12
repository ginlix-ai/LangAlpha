"""Tests for the workflow_handler cancel path — v4 honest durable cancel.

cancel_workflow no longer writes a terminal ``cancelled`` state eagerly (the
old ``tracker.set_cancel_flag`` / ``tracker.mark_cancelled`` /
``update_thread_status(..., "cancelled")`` writes are gone). Instead it:

- records durable cancel *intent* on the run's in_progress row via
  ``turn_lifecycle.request_run_cancel`` (the finalize CAS writes the terminal
  ``cancelled`` state only when teardown completes), and
- signals the local task via ``manager.cancel_workflow``.

Covers the behaviors that survived the cutover:
- signal-only when a task is active (the except-handler teardown owns
  ``cancel_and_clear``); intent is recorded, no eager terminal write;
- safety-net ``cancel_and_clear`` only when NO active task exists;
- run-targeted miss with another active task skips the safety net;
- manual-compaction shortcut returns early and stamps no run intent;
- idle thread (no active run) stamps no intent — thread not mislabeled;
- an already-terminal run answers an honest "already finished".
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _active_run(run_id: str = "run-A") -> dict:
    """A minimal in_progress run row as ``get_active_run`` would return it."""
    return {"conversation_response_id": run_id}


def _patch_common(
    *,
    manager_cancel_returns: bool,
    has_active_returns: bool = False,
    manual_compaction_returns: bool = False,
    active_run: dict | None = None,
    intent_state: str = "requested",
):
    """Patch the collaborators of workflow_handler.cancel_workflow.

    Returns the patch list plus the mocked registry_store, manager, and the
    ``get_active_run`` / ``request_run_cancel`` AsyncMocks (patched at their
    source module — cancel_workflow imports ``turn_lifecycle`` inside the
    function) so tests can assert on the durable-intent path.

    ``manual_compaction_returns`` drives ``manager.cancel_compaction`` — when a
    manual /compact is in flight (and no workflow is active) the handler stops
    that compaction and returns early, skipping the run-intent path.
    ``active_run`` is what ``get_active_run`` resolves for the thread (None =
    idle). ``intent_state`` is the state ``request_run_cancel`` reports.
    """
    manager = MagicMock()
    manager.cancel_workflow = AsyncMock(return_value=manager_cancel_returns)
    manager.has_active_task_for_thread = AsyncMock(return_value=has_active_returns)
    manager.cancel_compaction = MagicMock(return_value=manual_compaction_returns)

    registry_store = MagicMock()
    registry_store.cancel_and_clear = AsyncMock(return_value=0)

    # v4 durable cancel intent lives on the run row, not a Redis flag.
    get_active_run = AsyncMock(return_value=active_run)
    request_run_cancel = AsyncMock(
        return_value={"state": intent_state, "run": active_run or {}}
    )

    patches = [
        patch(
            "src.server.services.background_task_manager.BackgroundTaskManager.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.services.background_registry_store.BackgroundRegistryStore.get_instance",
            return_value=registry_store,
        ),
        patch(
            "src.server.database.turn_lifecycle.get_active_run",
            new=get_active_run,
        ),
        patch(
            "src.server.database.turn_lifecycle.request_run_cancel",
            new=request_run_cancel,
        ),
    ]
    return patches, registry_store, manager, get_active_run, request_run_cancel


@pytest.mark.asyncio
async def test_cancel_with_active_task_is_signal_only():
    """When a task is active (manager.cancel_workflow → True), the handler
    records durable intent and signals the task, but must NOT call
    cancel_and_clear — the except-handler teardown owns it — and never writes
    a terminal 'cancelled' status eagerly (that's the finalize CAS's job)."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, get_active_run, request_run_cancel = (
        _patch_common(manager_cancel_returns=True, active_run=_active_run())
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    # Durable intent recorded on the resolved active run (no eager terminal write).
    get_active_run.assert_awaited_once_with("t-1")
    request_run_cancel.assert_awaited_once_with("run-A", thread_id="t-1")
    # The local signal targets the SAME run the intent was stamped on — an
    # untargeted (None) signal could cancel a newer run that started after
    # the resolved one finalized.
    manager.cancel_workflow.assert_awaited_once_with("t-1", "run-A")
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_with_no_active_task_runs_safety_net():
    """No active task (manager.cancel_workflow → False, none active) ⇒ the
    safety-net cancel_and_clear runs to wipe any orphaned registry. An orphaned
    in_progress run row still accepts durable intent."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, _manager, _get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            active_run=_active_run(),
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    request_run_cancel.assert_awaited_once_with("run-A", thread_id="t-1")
    registry_store.cancel_and_clear.assert_awaited_once_with("t-1", force=True)


@pytest.mark.asyncio
async def test_run_targeted_miss_with_other_active_task_skips_safety_net():
    """A run-targeted cancel that misses its run (manager.cancel_workflow →
    False) but where ANOTHER turn is still active must NOT wipe the registry —
    that would kill the other turn's subagents. The explicit run_id is stamped
    directly (no get_active_run lookup)."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, get_active_run, request_run_cancel = (
        _patch_common(manager_cancel_returns=False, has_active_returns=True)
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1", "run-A")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    # Explicit run_id targets the stopped run directly — no active-run lookup.
    get_active_run.assert_not_awaited()
    request_run_cancel.assert_awaited_once_with("run-A", thread_id="t-1")
    # run_id threaded through to the manager so it targets the stopped run.
    manager.cancel_workflow.assert_awaited_once_with("t-1", "run-A")
    # Another turn owns the thread → safety net must be skipped.
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_stops_manual_compaction_when_no_active_workflow():
    """A user Stop during a MANUAL compaction (no active workflow) cancels the
    in-flight compaction and returns early — it must NOT stamp cancel intent on
    a run row or run any workflow-cancel machinery (which would mislabel the
    thread as a stopped turn)."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            manual_compaction_returns=True,
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    assert result["message"] == "Compaction stopped."
    manager.cancel_compaction.assert_called_once_with("t-1")
    # Early return: none of the run-intent / workflow-cancel machinery runs.
    manager.cancel_workflow.assert_not_awaited()
    get_active_run.assert_not_awaited()
    request_run_cancel.assert_not_awaited()
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_idle_thread_stamps_no_intent_but_runs_safety_net():
    """A /cancel that lands on an idle thread (no BTM task, no in-flight
    compaction, no active run) — e.g. a Stop click racing a compaction that
    JUST finished — must stamp NO cancel intent (there is no run to stamp, so
    the thread isn't mislabeled) but must still run the orphan-registry safety
    net. With nothing to cancel, the honest answer is cancelled=False."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            manual_compaction_returns=False,  # compaction already finished/cleared
            active_run=None,  # idle thread: no in_progress run
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    # Nothing was running to cancel → honest "not cancelled".
    assert result["cancelled"] is False
    # No mislabel: no run to stamp, so request_run_cancel is never called.
    get_active_run.assert_awaited_once_with("t-1")
    request_run_cancel.assert_not_awaited()
    # Orphan-registry safety net still runs.
    registry_store.cancel_and_clear.assert_awaited_once_with("t-1", force=True)


@pytest.mark.asyncio
async def test_cancel_active_run_records_durable_intent():
    """A dispatched/background turn with a live in_progress run row records
    durable cancel intent on that row via request_run_cancel — replacing the
    old eager set_cancel_flag / mark_cancelled / update_thread_status writes.
    The terminal 'cancelled' state is written later by the finalize CAS."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=True,
            has_active_returns=False,
            active_run=_active_run("run-XYZ"),
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    get_active_run.assert_awaited_once_with("t-1")
    request_run_cancel.assert_awaited_once_with("run-XYZ", thread_id="t-1")
    # Signal targets the resolved run, not the thread's current pick.
    manager.cancel_workflow.assert_awaited_once_with("t-1", "run-XYZ")


@pytest.mark.asyncio
async def test_cancel_already_terminal_run_returns_already_finished():
    """A cancel that arrives after the run finalized (request_run_cancel →
    'already_terminal') is an honest idempotent 'already finished', not a
    recorded losing cancel."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            active_run=_active_run("run-DONE"),
            intent_state="already_terminal",
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is False
    assert result["state"] == "already_finished"
    request_run_cancel.assert_awaited_once_with("run-DONE", thread_id="t-1")


@pytest.mark.asyncio
async def test_active_workflow_skips_compaction_cancel_shortcircuit():
    """When a workflow is active (auto compaction runs inside the turn), the
    handler must NOT take the manual-compaction shortcut — the normal
    workflow-cancel path interrupts the turn (and its in-flight summarize)."""
    from src.server.handlers.workflow_handler import cancel_workflow

    patches, registry_store, manager, _get_active_run, _request_run_cancel = (
        _patch_common(
            manager_cancel_returns=True,
            has_active_returns=True,
            manual_compaction_returns=True,  # would early-return if reached
            active_run=_active_run(),
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    # has_active short-circuits the `and` before cancel_compaction is evaluated.
    manager.cancel_compaction.assert_not_called()
    manager.cancel_workflow.assert_awaited_once_with("t-1", "run-A")
