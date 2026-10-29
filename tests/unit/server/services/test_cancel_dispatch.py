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
    manager.wait_for_persistence = AsyncMock(return_value=False)

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
        patch(
            "src.server.database.runs.outbox.list_returned_steers",
            AsyncMock(return_value=[]),
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
    request_run_cancel.assert_awaited_once_with("run-A", thread_id="t-1", by_user=True)
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
    request_run_cancel.assert_awaited_once_with("run-A", thread_id="t-1", by_user=True)
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
    request_run_cancel.assert_awaited_once_with("run-A", thread_id="t-1", by_user=True)
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
async def test_a_stop_naming_a_run_never_stops_a_manual_mutation():
    """A late stop for a run that has since finished, such as the Chief of
    Staff's for a hand-off, must not end the compaction the user started on
    that thread after it. The user's own stop of a compaction names no run."""
    from src.server.services.cancel_dispatch import cancel_workflow

    patches, _registry_store, _manager, runner, _get_active_run, request_run_cancel = (
        _patch_common(
            manager_cancel_returns=False,
            has_active_returns=False,
            mutation_stop_returns="cancelled",  # would early-return if reached
            intent_state="already_terminal",
        )
    )
    for p in patches:
        p.start()
    try:
        result = await cancel_workflow("t-1", "run-DONE")
    finally:
        for p in patches:
            p.stop()

    runner.request_stop.assert_not_awaited()
    request_run_cancel.assert_awaited_once_with("run-DONE", thread_id="t-1", by_user=True)
    assert result["cancelled"] is False
    assert result["state"] == "already_finished"


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
    request_run_cancel.assert_awaited_once_with("run-XYZ", thread_id="t-1", by_user=True)
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
    request_run_cancel.assert_awaited_once_with("run-DONE", thread_id="t-1", by_user=True)


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
    request_run_cancel.assert_awaited_once_with("run-REMOTE", thread_id="t-1", by_user=True)
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


# --- returned steers: a user stop gives them back, an agent stop never -----


_STEERS = [{"content": "also cover bonds", "request_key": "rk-1", "timestamp": 1.0}]


async def _cancel_with_steers(
    *,
    by_user: bool,
    intents: dict[str, str],
    run_id: str = "run-A",
    intent_steers: list | None = None,
    listed_steers: list | None = None,
    taken: list[str] | None = None,
    held: int = 0,
    settled: bool = True,
    wait: AsyncMock | None = None,
    keep: AsyncMock | None = None,
):
    """Run ``cancel_workflow`` with a per-run intent state; ``intent_steers``
    are the steers the stamp reports, ``listed_steers`` the re-read's, and
    ``taken`` the runs ``stop_hand_off_steers`` names. ``held`` is how many
    of the user's steers the stamp left to the run, and ``settled`` whether
    the run ends within the wait."""
    from contextlib import ExitStack

    from src.server.services.cancel_dispatch import cancel_workflow

    patches, _store, _manager, _runner, _get_active, request_run_cancel = (
        _patch_common(manager_cancel_returns=False, has_active_returns=False)
    )
    request_run_cancel.side_effect = lambda rid, **_: {
        "state": intents[rid],
        "run": {},
        "returned_steers": list(intent_steers or []),
        "held_steers": held,
    }
    listed = AsyncMock(return_value=list(listed_steers or []))
    stop_hand_offs = AsyncMock(return_value=list(taken or []))
    wait = wait if wait is not None else AsyncMock(return_value=settled)
    with ExitStack() as stack:
        stack.enter_context(
            patch("src.server.services.report_back.steer.wait_run_settled", wait)
        )
        for p in patches:
            stack.enter_context(p)
        stack.enter_context(
            patch("src.server.database.runs.outbox.list_returned_steers", listed)
        )
        stack.enter_context(
            patch(
                "src.server.database.runs.outbox.keep_unanswered_steers",
                keep if keep is not None else AsyncMock(),
            )
        )
        stack.enter_context(
            patch("src.server.database.runs.outbox.stop_hand_off_steers", stop_hand_offs)
        )
        stack.enter_context(
            patch("src.server.services.runs.cancel.publish_cancel_nudge", AsyncMock())
        )
        result = await cancel_workflow("t-1", run_id, by_user=by_user)
    return result, request_run_cancel, listed, stop_hand_offs


@pytest.mark.asyncio
async def test_a_user_stop_gives_back_the_steers_its_stamp_returned():
    result, _intent, listed, stop_hand_offs = await _cancel_with_steers(
        by_user=True, intents={"run-A": "requested"}, intent_steers=_STEERS
    )

    assert result["cancelled"] is True
    assert result["returned_steers"] == _STEERS
    listed.assert_not_awaited()
    stop_hand_offs.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["already_requested", "already_terminal"])
async def test_a_repeated_user_stop_re_reads_what_an_earlier_stop_returned(state):
    """The stamp that returned them may be an earlier call's whose answer was
    lost; asking again must recover them."""
    result, _intent, listed, stop_hand_offs = await _cancel_with_steers(
        by_user=True, intents={"run-A": state}, listed_steers=_STEERS
    )

    assert result["returned_steers"] == _STEERS
    listed.assert_awaited_once_with("run-A")
    stop_hand_offs.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_agent_stop_gives_back_no_steers():
    """The Chief of Staff never sent the user's steers; they stay queued for
    the user's next move rather than vanishing into a tool result."""
    result, intent, listed, _stop_hand_offs = await _cancel_with_steers(
        by_user=False, intents={"run-A": "requested"}, intent_steers=_STEERS
    )

    assert result["cancelled"] is True
    assert result["returned_steers"] == []
    intent.assert_awaited_once_with("run-A", thread_id="t-1", by_user=False)
    listed.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_agent_stop_of_an_ended_run_stops_the_run_that_took_its_hand_off():
    """A hand-off steered into a run that ended unread became another run's
    work; stopping it by the run id ``delegate_to_analyst`` returned must
    still stop that work."""
    result, intent, listed, stop_hand_offs = await _cancel_with_steers(
        by_user=False,
        intents={"run-DONE": "already_terminal", "run-LATER": "requested"},
        run_id="run-DONE",
        intent_steers=_STEERS,
        taken=["run-LATER"],
    )

    stop_hand_offs.assert_awaited_once_with("t-1", "run-DONE")
    assert [c.args[0] for c in intent.await_args_list] == ["run-DONE", "run-LATER"]
    assert all(c.kwargs["by_user"] is False for c in intent.await_args_list)
    assert result["cancelled"] is True
    assert "state" not in result
    assert result["returned_steers"] == []
    listed.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_agent_stop_of_an_ended_run_no_one_took_over_is_already_finished():
    result, _intent, _listed, stop_hand_offs = await _cancel_with_steers(
        by_user=False, intents={"run-DONE": "already_terminal"}, run_id="run-DONE"
    )

    stop_hand_offs.assert_awaited_once_with("t-1", "run-DONE")
    assert result["cancelled"] is False
    assert result["state"] == "already_finished"
    assert result["returned_steers"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["requested", "already_requested"])
@pytest.mark.parametrize("settled", [True, False])
async def test_a_user_stop_waits_out_the_steers_the_run_still_holds(state, settled):
    """The stamp leaves a held steer to the model call confirming it or to
    the run's ending; the answer reads what that ending gave back. Once the
    wait runs out, the ones still held stay queued for the user's next move,
    since their return would reach no one."""
    wait = AsyncMock(return_value=settled)
    keep = AsyncMock()
    result, _intent, listed, _stop = await _cancel_with_steers(
        by_user=True,
        intents={"run-A": state},
        listed_steers=_STEERS,
        held=1,
        wait=wait,
        keep=keep,
    )

    wait.assert_awaited_once_with("run-A")
    if settled:
        keep.assert_not_awaited()
    else:
        keep.assert_awaited_once_with("t-1", "run-A")
    assert result["returned_steers"] == _STEERS
    assert listed.await_args_list[-1].args == ("run-A",)


@pytest.mark.asyncio
async def test_a_stop_with_nothing_held_does_not_wait():
    wait = AsyncMock(return_value=True)
    result, _intent, listed, _stop = await _cancel_with_steers(
        by_user=True, intents={"run-A": "requested"}, intent_steers=_STEERS, wait=wait
    )

    wait.assert_not_awaited()
    listed.assert_not_awaited()
    assert result["returned_steers"] == _STEERS


@pytest.mark.asyncio
async def test_an_agent_stop_never_waits_for_held_steers():
    wait = AsyncMock(return_value=True)
    result, _intent, _listed, _stop = await _cancel_with_steers(
        by_user=False, intents={"run-A": "requested"}, held=1, wait=wait
    )

    wait.assert_not_awaited()
    assert result["returned_steers"] == []


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
    assert "Failed to cancel the turn" in str(caught.value.detail)


@pytest.mark.asyncio
@pytest.mark.parametrize(("held", "waited"), [(1, True), (0, False)])
async def test_task_cancel_waits_for_the_run_holding_the_users_messages(held, waited):
    from contextlib import ExitStack

    from src.server.services.cancel_dispatch import cancel_subagent_task

    returned = [{"content": "narrow it", "input_id": "in-1", "reason": "run_ended"}]
    patches, *_ = _patch_task_cancel(local_cancel_returns=True)
    wait = AsyncMock(return_value=True)
    listed = AsyncMock(return_value=returned)
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        stack.enter_context(
            patch(
                "src.server.database.runs.subagent_runs.get_task",
                AsyncMock(return_value={"latest_run_id": "tr-1"}),
            )
        )
        stack.enter_context(
            patch("src.server.database.pool.get_db_connection", _no_conn)
        )
        stack.enter_context(
            patch(
                "src.server.database.runs.outbox.count_held_user_steers",
                AsyncMock(return_value=held),
            )
        )
        stack.enter_context(
            patch("src.server.services.report_back.steer.wait_run_settled", wait)
        )
        stack.enter_context(
            patch("src.server.database.runs.outbox.list_returned_task_steers", listed)
        )
        result = await cancel_subagent_task("t-1", "abc123")

    assert result["cancelled"] is True
    assert result["returned_steers"] == returned
    if waited:
        wait.assert_awaited_once_with("tr-1", task=True)
    else:
        wait.assert_not_awaited()


def _no_conn():
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _conn():
        yield MagicMock()

    return _conn()
