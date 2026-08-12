"""Tests for the workflow_handler cancel path — v4 honest durable cancel.

cancel_workflow no longer writes a terminal ``cancelled`` state eagerly (the
old ``tracker.set_cancel_flag`` / ``tracker.mark_cancelled`` /
``update_thread_status(..., "cancelled")`` writes are gone). Instead it:

- records durable cancel *intent* on the run's in_progress row via
  ``runs.lifecycle.request_run_cancel`` (the finalize CAS writes the terminal
  ``cancelled`` state only when teardown completes), and
- signals the local task via ``manager.signal_cancel``.

Covers the behaviors that survived the cutover:
- signal-only when a task is active (the except-handler teardown owns
  ``cancel_and_clear``); intent is recorded, no eager terminal write;
- RUN-scoped safety net (``cancel_run_tasks``) only when NO active task
  exists AND a target run resolved — never a thread-wide wipe, which would
  destroy a terminal local run's live tail (or an unrelated registry) when
  the target run lives on another worker;
- run-targeted miss with another active task skips the safety net;
- manual-mutation stop (ThreadMutationRunner.request_stop, v4 2.4) returns
  early and stamps no run intent — local cancel and cross-worker signal alike;
- idle thread (no active run) stamps no intent — thread not mislabeled;
- an already-terminal run answers an honest "already finished";
- a cancel that stopped nothing splits on liveness: `no_active_run` when the
  thread is idle (benign — nothing to stop), `another_run_active` when a run
  is still live (the stop the user asked for did not happen). The live run is
  reported, never cancelled as a fallback.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

REGISTRY_STORE_MOD = "src.server.services.background_registry_store"


def _active_run(run_id: str = "run-A") -> dict:
    """A minimal in_progress run row as ``get_active_run`` would return it."""
    return {"conversation_response_id": run_id}


def _patch_common(
    *,
    manager_cancel_returns: bool,
    has_active_returns: bool = False,
    mutation_stop_returns: str = "none",
    active_run: dict | None = None,
    intent_state: str = "requested",
):
    """Patch the collaborators of cancel_dispatch.cancel_workflow.

    Returns the patch list plus the mocked registry_store, manager, mutation
    runner, and the ``get_active_run`` / ``request_run_cancel`` AsyncMocks
    (patched at their source module — cancel_workflow imports
    ``runs.lifecycle`` inside the function) so tests can assert on the
    durable-intent path.

    ``mutation_stop_returns`` drives ``ThreadMutationRunner.request_stop`` —
    "cancelled" (local op cancelled) or "signalled" (stop key flagged for a
    foreign worker) makes the handler return early, skipping the run-intent
    path; "none" (no mutation in flight) falls through. ``active_run`` is what
    ``get_active_run`` resolves for the thread (None = idle). ``intent_state``
    is the state ``request_run_cancel`` reports.
    """
    manager = MagicMock()
    manager.signal_cancel = AsyncMock(return_value=manager_cancel_returns)
    manager.has_active_task_for_thread = AsyncMock(return_value=has_active_returns)

    runner = MagicMock()
    runner.request_stop = AsyncMock(return_value=mutation_stop_returns)

    registry_store = MagicMock()
    registry_store.cancel_and_clear = AsyncMock(return_value=0)
    registry_store.cancel_run_tasks = AsyncMock(return_value=0)

    # v4 durable cancel intent lives on the run row, not a Redis flag.
    get_active_run = AsyncMock(return_value=active_run)
    request_run_cancel = AsyncMock(
        return_value={"state": intent_state, "run": active_run or {}}
    )

    patches = [
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=manager,
        ),
        patch(
            "src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
            return_value=runner,
        ),
        patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=registry_store,
        ),
        patch(
            "src.server.database.runs.lifecycle.get_active_run",
            new=get_active_run,
        ),
        patch(
            "src.server.database.runs.lifecycle.request_run_cancel",
            new=request_run_cancel,
        ),
    ]
    return patches, registry_store, manager, runner, get_active_run, request_run_cancel


@pytest.mark.asyncio
async def test_cancel_with_active_task_is_signal_only():
    """When a task is active (manager.signal_cancel → True), the handler
    records durable intent and signals the task, but must NOT call
    cancel_and_clear — the except-handler teardown owns it — and never writes
    a terminal 'cancelled' status eagerly (that's the finalize CAS's job)."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
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
    manager.signal_cancel.assert_awaited_once_with("t-1", "run-A")
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_with_no_active_task_runs_safety_net():
    """No active task (manager.signal_cancel → False, none active) ⇒ the
    safety net cancels the TARGET RUN's leftover subagents only (its main task
    may have settled while tail writers survive). Never a thread-wide wipe:
    the registry may hold another terminal run's live tail whose guard drain
    must keep seeing its writers. An orphaned in_progress run row still
    accepts durable intent."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, _manager, _runner, _get_active_run, request_run_cancel = (
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
    registry_store.cancel_run_tasks.assert_awaited_once_with(
        "t-1", "run-A", force=True
    )
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_targeted_miss_with_other_active_task_skips_safety_net():
    """A run-targeted cancel that misses its run (manager.signal_cancel →
    False) but where ANOTHER turn is still active must NOT wipe the registry —
    that would kill the other turn's subagents. The explicit run_id is stamped
    directly (no get_active_run lookup)."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
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
    manager.signal_cancel.assert_awaited_once_with("t-1", "run-A")
    # Another turn owns the thread → safety net must be skipped.
    registry_store.cancel_and_clear.assert_not_awaited()
    registry_store.cancel_run_tasks.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stopped, message",
    [
        ("cancelled", "Compaction stopped."),
        ("signalled", "Compaction stop signalled to its worker."),
    ],
)
async def test_cancel_stops_manual_mutation_when_no_active_workflow(stopped, message):
    """A user Stop during a MANUAL compact/offload/delete (no active workflow)
    stops the in-flight mutation via ThreadMutationRunner.request_stop and
    returns early — it must NOT stamp cancel intent on a run row or run any
    workflow-cancel machinery (which would mislabel the thread as a stopped
    turn). Covers both the locally-owned op ("cancelled") and the
    foreign-worker stop-key path ("signalled")."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            mutation_stop_returns=stopped,
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
    assert result["message"] == message
    runner.request_stop.assert_awaited_once_with("t-1")
    # Early return: none of the run-intent / workflow-cancel machinery runs.
    manager.signal_cancel.assert_not_awaited()
    get_active_run.assert_not_awaited()
    request_run_cancel.assert_not_awaited()
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_idle_thread_stamps_no_intent_but_runs_safety_net():
    """A /cancel that lands on an idle thread (no BTM task, no in-flight
    mutation, no active run) — e.g. a Stop click racing a compaction that
    JUST finished — must stamp NO cancel intent (there is no run to stamp, so
    the thread isn't mislabeled) and must touch NO registry state: with no
    target run there is nothing to scope a cancel to, and a thread-wide wipe
    could destroy a terminal run's still-live tail. With nothing to cancel,
    the honest answer is cancelled=False."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            mutation_stop_returns="none",  # mutation already finished/cleared
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
    assert result["state"] == "no_active_run"
    # No mislabel: no run to stamp, so request_run_cancel is never called.
    # Read twice: once to resolve a target, once to answer "did this cancel
    # leave something running?" — a fresh read, since the first is stale by
    # the time the dispatch has finished.
    assert get_active_run.await_count == 2
    request_run_cancel.assert_not_awaited()
    # No target run → no registry action at all (never a thread-wide wipe).
    registry_store.cancel_and_clear.assert_not_awaited()
    registry_store.cancel_run_tasks.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_active_run_records_durable_intent():
    """A dispatched/background turn with a live in_progress run row records
    durable cancel intent on that row via request_run_cancel — replacing the
    old eager set_cancel_flag / mark_cancelled / update_thread_status writes.
    The terminal 'cancelled' state is written later by the finalize CAS."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
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
    manager.signal_cancel.assert_awaited_once_with("t-1", "run-XYZ")


@pytest.mark.asyncio
async def test_cancel_already_terminal_run_returns_already_finished():
    """A cancel that arrives after the run finalized (request_run_cancel →
    'already_terminal') is an honest idempotent 'already finished', not a
    recorded losing cancel."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
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
async def test_active_workflow_skips_mutation_stop_shortcircuit():
    """When a workflow is active (auto compaction runs inside the turn), the
    handler must NOT consult the mutation runner — the normal workflow-cancel
    path interrupts the turn (and its in-flight summarize)."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, _get_active_run, _request_run_cancel = (
        _patch_common(
            manager_cancel_returns=True,
            has_active_returns=True,
            mutation_stop_returns="cancelled",  # would early-return if reached
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
    # has_active gates the runner: request_stop is never consulted.
    runner.request_stop.assert_not_awaited()
    manager.signal_cancel.assert_awaited_once_with("t-1", "run-A")


@pytest.mark.asyncio
async def test_remote_run_cancel_never_wipes_local_registry():
    """A targeted /cancel for a run owned by ANOTHER worker (no local task,
    intent stamped remotely) must leave the local registry alone apart from
    the run-scoped cancel (a no-op for a remote run's tasks) — the registry
    may hold a terminal LOCAL run's live tail writers, whose guard drain
    releases the session the moment their registry entries vanish."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, manager, runner, get_active_run, request_run_cancel = (
        _patch_common(manager_cancel_returns=False, has_active_returns=False)
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1", "run-REMOTE")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    request_run_cancel.assert_awaited_once_with("run-REMOTE", thread_id="t-1")
    registry_store.cancel_and_clear.assert_not_awaited()
    registry_store.cancel_run_tasks.assert_awaited_once_with(
        "t-1", "run-REMOTE", force=True
    )


@pytest.mark.asyncio
async def test_stale_run_id_with_a_live_run_says_nothing_was_cancelled():
    """A run_id that no longer resolves (rewound row, wrong id) while the
    thread still has a live run stopped nothing the user asked for — and the
    caller cannot tell that from an idle thread without being told, because
    the client tears its own streaming state down before this answers.

    The live run must NOT be cancelled as a fallback: that is the cross-turn
    hazard the run_id parameter exists to prevent. Only the named run is ever
    the target of the safety net.
    """
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, registry_store, _manager, _runner, get_active_run, _intent = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            active_run=_active_run("run-LIVE"),
            intent_state="not_found",
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1", "run-GONE")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is False
    assert result["state"] == "another_run_active"
    get_active_run.assert_awaited_once_with("t-1")
    # Scoped to the named run only — the live one is reported, never touched.
    registry_store.cancel_run_tasks.assert_awaited_once_with(
        "t-1", "run-GONE", force=True
    )
    registry_store.cancel_and_clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_run_id_on_an_idle_thread_is_just_no_active_run():
    """The same unresolvable run_id with nothing live is benign: there was
    nothing to stop, which is what `no_active_run` says. Splitting these two
    is the whole point — a client that warned on both would cry wolf on every
    stop that raced its own turn's teardown."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, _registry_store, _manager, _runner, get_active_run, _intent = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            active_run=None,
            intent_state="not_found",
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1", "run-GONE")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is False
    assert result["state"] == "no_active_run"
    get_active_run.assert_awaited_once_with("t-1")


@pytest.mark.asyncio
async def test_pre_start_window_still_reports_a_dispatched_cancel():
    """`not_found` WITH a successful local signal is the pre-START window —
    the placeholder cancel took, so this is a real stop and must not be
    demoted to a no-op by the liveness check."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, _registry_store, _manager, _runner, get_active_run, _intent = (
        _patch_common(
            manager_cancel_returns=True,
            has_active_returns=False,
            active_run=_active_run("run-LIVE"),
            intent_state="not_found",
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1", "run-PENDING")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    assert "state" not in result
    get_active_run.assert_not_awaited()


# --- cancel_subagent_task (targeted single-task stop) ---------------------


def _patch_task_cancel(
    *,
    local_cancel_returns: bool,
    active_task_run: dict | None = None,
    latest_statuses: dict | None = None,
    intent_state: str = "requested",
):
    """Patch the collaborators of cancel_dispatch.cancel_subagent_task."""
    registry_store = MagicMock()
    registry_store.cancel_task = AsyncMock(return_value=local_cancel_returns)

    get_active_task_run = AsyncMock(return_value=active_task_run)
    get_latest_run_statuses = AsyncMock(return_value=latest_statuses or {})
    request_task_run_cancel = AsyncMock(
        return_value={"state": intent_state, "run": active_task_run or {}}
    )
    publish_cancel_nudge = AsyncMock()

    patches = [
        patch(
            f"{REGISTRY_STORE_MOD}.BackgroundRegistryStore.get_instance",
            return_value=registry_store,
        ),
        patch(
            "src.server.database.runs.subagent_runs.get_active_task_run",
            new=get_active_task_run,
        ),
        patch(
            "src.server.database.runs.subagent_runs.get_latest_run_statuses",
            new=get_latest_run_statuses,
        ),
        patch(
            "src.server.database.runs.subagent_runs.request_task_run_cancel",
            new=request_task_run_cancel,
        ),
        patch(
            "src.server.services.runs.cancel.publish_cancel_nudge",
            new=publish_cancel_nudge,
        ),
    ]
    return (
        patches,
        registry_store,
        get_active_task_run,
        request_task_run_cancel,
        publish_cancel_nudge,
    )


@pytest.mark.asyncio
async def test_task_cancel_local_owner_short_circuits():
    """When this worker owns the writer the registry cancel is the whole
    story — no ledger reads, no cross-worker nudge."""
    from src.server.services.cancel_dispatch import cancel_subagent_task

    patches, registry_store, get_active, _intent, nudge = _patch_task_cancel(
        local_cancel_returns=True
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_subagent_task("t-1", "abc123")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    registry_store.cancel_task.assert_awaited_once_with("t-1", "abc123")
    get_active.assert_not_awaited()
    nudge.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_cancel_foreign_owner_stamps_intent_and_nudges():
    from src.server.services.cancel_dispatch import cancel_subagent_task

    patches, _store, _get_active, intent, nudge = _patch_task_cancel(
        local_cancel_returns=False,
        active_task_run={"task_run_id": "tr-9"},
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_subagent_task("t-1", "abc123")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is True
    intent.assert_awaited_once_with("tr-9", thread_id="t-1")
    nudge.assert_awaited_once_with("t-1", None, task_id="abc123")


@pytest.mark.asyncio
async def test_task_cancel_settled_task_answers_already_finished():
    from src.server.services.cancel_dispatch import cancel_subagent_task

    patches, _store, _get_active, intent, nudge = _patch_task_cancel(
        local_cancel_returns=False,
        active_task_run=None,
        latest_statuses={"abc123": "completed"},
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_subagent_task("t-1", "abc123")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is False
    assert result["state"] == "already_finished"
    intent.assert_not_awaited()
    nudge.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_cancel_unknown_task_is_honest():
    from src.server.services.cancel_dispatch import cancel_subagent_task

    patches, _store, _get_active, _intent, nudge = _patch_task_cancel(
        local_cancel_returns=False, active_task_run=None
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_subagent_task("t-1", "nosuch")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is False
    assert result["state"] == "task_not_found"
    nudge.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_cancel_terminal_cas_race_answers_already_finished():
    """The intent CAS losing to the task's own finalize is not an error —
    the task is done, say so, and skip the nudge."""
    from src.server.services.cancel_dispatch import cancel_subagent_task

    patches, _store, _get_active, _intent, nudge = _patch_task_cancel(
        local_cancel_returns=False,
        active_task_run={"task_run_id": "tr-9"},
        intent_state="already_terminal",
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_subagent_task("t-1", "abc123")
    finally:
        for p in patches:
            p.stop()

    assert result["cancelled"] is False
    assert result["state"] == "already_finished"
    nudge.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_cancel_failure_does_not_put_infrastructure_text_on_the_wire():
    """A ledger or transport exception names hosts, queries and sometimes
    credential fragments. The log keeps it; the authenticated caller gets a
    fixed string (src/server/AGENTS.md: sanitize before the wire).
    """
    from fastapi import HTTPException

    from src.server.services.cancel_dispatch import cancel_workflow

    secret = "could not connect to db-prod-7.internal?password=hunter2"
    (
        patches, _registry_store, _manager, _runner, get_active_run, _intent,
    ) = _patch_common(manager_cancel_returns=True, active_run=_active_run())
    get_active_run.side_effect = RuntimeError(secret)

    for p in patches:
        p.start()
    try:
        with pytest.raises(HTTPException) as caught:
            await cancel_workflow("thread-x")
    finally:
        for p in patches:
            p.stop()

    assert caught.value.status_code == 500
    assert secret not in str(caught.value.detail)
    assert "db-prod-7" not in str(caught.value.detail)
    # Still says which operation failed — a fixed string, not a bare 500.
    assert "Failed to cancel workflow" in str(caught.value.detail)
