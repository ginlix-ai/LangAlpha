"""PTC (Programmatic Tool Calling) workflow — async SSE generator.

This module contains the ``astream_ptc_workflow`` async generator, refactored
from the monolithic ``chat_handler.py``.  Request preparation, persistence,
error handling, and streaming logic is delegated to ``request_prep`` and
``services.runs.admission``; PTC-specific concerns (workspace session,
sandbox, plan mode, background subagent orchestration, completion callback)
remain inline.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime

from fastapi import HTTPException
from langgraph.types import Command

from src.server.app import setup
from src.server.database.workspace import update_workspace_activity
from src.server.services.runs.sse_producer import RunSSEProducer
from src.server.models.chat import (
    ChatRequest,
    serialize_hitl_response_map,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.runs.executor import LocalRunExecutor
from src.server.services.workspace_manager import WorkspaceManager
from src.observability import (
    chat_turn_phase_duration_ms,
    safe_record,
)
from src.server.utils.directive_context import (
    build_directive_reminder,
    parse_directive_contexts,
)
from src.server.utils.widget_context import (
    build_widget_context_reminder,
    parse_widget_contexts,
    serialize_widget_contexts_for_metadata,
)
from src.server.utils.chart_selection_context import (
    build_chart_selection_reminder,
    parse_chart_selection_contexts,
    serialize_chart_selections_for_metadata,
)
from src.llms.llm import get_input_modalities
from src.server.utils.multimodal_context import (
    build_attachment_metadata,
    build_file_reminder,
    build_unsupported_reminder,
    filter_multimodal_by_capability,
    inject_multimodal_context,
    parse_multimodal_contexts,
    upload_to_sandbox,
)
from src.utils.tracking import ExecutionTracker

from ptc_agent.agent.graph import build_ptc_graph_with_session

from .request_prep import (
    DISPATCH_STARTED_MARKER,
    _append_to_last_user_message,
    _is_plan_interrupt_pending,
    _resolve_fork,
    _resolve_timezone,
    apply_fetch_override,
    build_graph_config,
    ensure_thread,
    init_tracking,
    inject_inline_reminders,
    logger,
    normalize_request_messages,
    prepare_skill_contexts,
    process_hitl_response,
    serialize_context_metadata,
    setup_steering_tracking,
)
from src.server.services.runs.admission import (
    RunScope,
    begin_run,
    dedup_retransmit_or_raise,
)
from src.config.settings import get_ptc_recursion_limit

from .admission_gate import wait_or_steer
from .error_handling import handle_workflow_error
from src.server.services.llm.config import resolve_llm_config
from .steering import drain_steering_return_event
from .run_stream_reader import stream_from_log
from .detached import fire_and_forget as _fire_and_forget


async def _resolve_origin_meta(request, thread_id: str) -> dict:
    """The watching flash thread (+ dispatch generation) for this run's hooks.

    The dispatch POST carries them; follow-up turns (HITL resume, user
    continuation) inherit them from the previous attempt — the origin is a
    THREAD property, so the stamp must stay sticky or later completions
    would fall out of the flash thread's serialization chain (and their
    gen-fenced teardowns out of their dispatch incarnation).
    """
    supplied = getattr(request, "origin_flash_thread_id", None)
    if supplied:
        return {
            "origin_flash_thread_id": supplied,
            "origin_dispatch_gen": getattr(request, "origin_dispatch_gen", None),
        }
    from src.server.database.runs import lifecycle as tl_db

    # A read failure must PROPAGATE (failing the turn start): stamping None
    # on a transient error wouldn't just mis-key this run — inheritance is
    # sticky, so every later turn would inherit the None and the thread
    # would fall out of its flash chain permanently.
    prev = await tl_db.get_latest_attempt(thread_id)
    meta = (prev.get("metadata") or {}) if prev is not None else {}
    return {
        "origin_flash_thread_id": meta.get("origin_flash_thread_id"),
        "origin_dispatch_gen": meta.get("origin_dispatch_gen"),
    }


async def astream_ptc_workflow(
    request: ChatRequest,
    thread_id: str,
    run_id: str,
    user_input: str,
    user_id: str,
    workspace_id: str,
    is_byok: bool = False,
    config=None,
    dispatched: bool = False,
):
    """Async generator that streams PTC agent workflow events.

    ``run_id`` is generated at the handler entry in ``threads.py`` and is
    1:1 with ``conversation_response_id``. State (BTM, persistence, Redis
    stream key) is keyed by ``(thread_id, run_id)`` so concurrent turns
    on the same thread share no cross-turn state by construction.

    ``dispatched`` marks the call as an X-Dispatch=background invocation
    whose BTM placeholder was created upstream in ``threads.py``. The
    handler skips ``wait_or_steer`` in that case.
    """
    start_time = time.time()
    handler = None
    run_handle = None
    token_callback = None
    tool_tracker = None
    ptc_graph = None
    timezone_str = None

    # Phase timing — collects wall-clock durations for each hot-path phase.
    # Emits a single structured summary line when the workflow starts.
    _phase_times: dict[str, float] = {}
    _phase_t0 = start_time

    def _mark_phase(name: str) -> None:
        nonlocal _phase_t0
        now = time.time()
        _phase_times[name] = (now - _phase_t0) * 1000  # ms
        _phase_t0 = now

    ExecutionTracker.start_tracking()

    # Owns the burst lease, admission lock, and open START row until the
    # executor's done-callback is armed (transfer_to_executor below).
    scope = RunScope(user_id=user_id, burst_slot_id=request.burst_slot_id)
    try:
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs.",
            )

        # =====================================================================
        # Admission gate
        # =====================================================================
        # Per-thread asyncio.Lock that serializes the
        # ``wait_or_steer → start_run → start_run`` window.
        # Without this, two simultaneous cold POSTs on an idle thread both
        # see "no in-flight task" in ``wait_or_steer`` and both attempt
        # START; the loser now fails on the in_progress slot index instead
        # of admitting, but serializing here routes it to steering.
        manager = LocalRunExecutor.get_instance()
        admission_lock = await manager.get_admission_lock(thread_id)
        await admission_lock.acquire()
        scope.hold_admission(admission_lock)

        # Idempotency probe under the admission lock: a retransmitted
        # request_key resolves to its existing run HERE, before the steering
        # or fork paths below can act on the duplicate (raises
        # DuplicateRequestError → structured duplicate_request SSE).
        await dedup_retransmit_or_raise(request)

        # =====================================================================
        # Early steering routing
        # =====================================================================
        # If a workflow is already running for this thread, route this POST
        # through the steering queue *before* any DB write. Detecting steering
        # here keeps ``conversation_queries`` clean — a steering message is
        # neither a run nor a turn (v4 identity model); its content is archived
        # on the owning response's metadata at finalize.
        workspace_manager = WorkspaceManager.get_instance()
        needs_startup = not workspace_manager.has_ready_session(workspace_id)
        # When the workspace was evicted/restarted, any in-BTM LocalRunExecution for
        # this thread holds a stale sandbox reference — cancel it first so
        # admission/steering routes against live state only (this run isn't
        # registered anywhere yet; its row commits at START, below).
        if needs_startup:
            await manager.cancel_stale_workflow(thread_id)
        # Admit a fresh turn, steer the running one, or 409 — see
        # ``wait_or_steer``. Dispatched flows pass ``can_steer=False``: any
        # in-flight run is a hard conflict, never a steer. Foreground turns
        # steer. Retries are never steerable either: a /retry that finds
        # another live run is a hard conflict, not an (empty) steering
        # message into that run.
        ready, steering_event = await wait_or_steer(
            manager,
            thread_id,
            user_input,
            user_id,
            steer_only=request.steer_only,
            can_steer=not dispatched and request.retry_of_run_id is None,
        )
        if not ready:
            await scope.release_slot()
            # Release admission immediately — no workflow will register
            # under this lock, so holding it would needlessly block any
            # follow-up POST.
            scope.release_admission()
            if steering_event:
                yield steering_event
            return

        # =====================================================================
        # Database Persistence Setup
        # =====================================================================

        await ensure_thread(
            request, thread_id, workspace_id, user_id, msg_type="ptc",
            initial_query=user_input,
        )

        query_type, fork = _resolve_fork(request=request)
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Persist query start
        feedback_action = None
        query_content = user_input
        effective_model = config.llm.name if config and config.llm else None
        query_metadata = {
            "workspace_id": request.workspace_id,
            "msg_type": "ptc",
        }
        if effective_model:
            query_metadata["llm_model"] = effective_model

        # Extract attachment and context metadata for display in history
        # (PTC skips this block for HITL resumes — contrast with Flash)
        widget_ctxs = parse_widget_contexts(request.additional_context)
        chart_selections = parse_chart_selection_contexts(request.additional_context)
        if request.additional_context and not request.hitl_response:
            multimodal_ctxs = parse_multimodal_contexts(request.additional_context)
            if multimodal_ctxs:
                query_metadata["attachments"] = await build_attachment_metadata(
                    multimodal_ctxs, thread_id
                )
            if widget_ctxs:
                query_metadata["widget_contexts"] = serialize_widget_contexts_for_metadata(
                    widget_ctxs
                )
            if chart_selections:
                query_metadata["chart_selections"] = serialize_chart_selections_for_metadata(
                    chart_selections
                )

        # Persist lightweight additional_context + slash command fallback
        # (serialize_context_metadata's slash-command branch already guards
        # on `not request.hitl_response`, so this is safe to call always.)
        if not request.hitl_response:
            serialize_context_metadata(request, query_metadata, user_input, mode="ptc")

        if request.hitl_response:
            feedback_action, query_content, hitl_answers, interrupt_ids = (
                process_hitl_response(request)
            )
            query_metadata["hitl_interrupt_ids"] = interrupt_ids
            if hitl_answers:
                query_metadata["hitl_answers"] = hitl_answers

        # =====================================================================
        # START txn (v4): query row + in_progress run row + thread projection
        # in one transaction (begin_run owns the attempt-chain derivation).
        # =====================================================================
        # Resolved ONCE and reused by the tracker re-mark below: the raw
        # request field is empty on public follow-ups (HITL resume, user
        # continuation), and stamping the durable row with the inherited gen
        # while re-marking the tracker with None would make a live admitted
        # run read as unadmitted to the fenced-teardown probe.
        origin_meta = await _resolve_origin_meta(request, thread_id)
        run_handle = await begin_run(
            request,
            thread_id=thread_id,
            run_id=run_id,
            msg_type="ptc",
            workspace_id=workspace_id,
            user_id=user_id,
            is_byok=is_byok,
            query_content=query_content,
            query_type=query_type,
            feedback_action=feedback_action,
            query_metadata=query_metadata,
            fork=fork,
            is_checkpoint_replay=is_checkpoint_replay,
            extra_run_metadata=origin_meta,
        )
        scope.attach_run(run_handle)
        if not is_checkpoint_replay:
            logger.debug(
                f"[PTC_CHAT] Run started: workspace_id={workspace_id} "
                f"thread_id={thread_id} query_type={query_type} "
                f"turn_index={run_handle.turn_index}"
            )

        if dispatched:
            # Durable receipt (v4 2.4c): the dispatch handler is priming this
            # generator and returns its 200 response only once the START txn
            # above has committed. The marker never reaches the SSE stream.
            yield DISPATCH_STARTED_MARKER

        # =====================================================================
        # Timezone and Locale Validation
        # =====================================================================

        timezone_str = _resolve_timezone(request.timezone, request.locale)

        # =====================================================================
        # Token and Tool Tracking
        # =====================================================================

        token_callback, tool_tracker = init_tracking(thread_id)

        _mark_phase("db_setup")

        # =====================================================================
        # Session and Graph Setup
        # =====================================================================

        # Resolve LLM config (pre-resolved by route handler, fallback for standalone use)
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config, user_id, request.llm_model, is_byok, mode="ptc",
                reasoning_effort=getattr(request, "reasoning_effort", None),
                fast_mode=getattr(request, "fast_mode", None),
                thread_id=thread_id,
                enabled_subagents=request.subagents_enabled,
            )

        # Propagate fetch model override to tool context
        apply_fetch_override(config)

        _mark_phase("pre_session")

        subagents = request.subagents_enabled or config.subagents.enabled
        sandbox_id = None

        # ``workspace_manager`` and ``needs_startup`` were resolved above for
        # the pre-steering stale-cancel hook. Reuse them — recomputing here
        # would race with a concurrent reconnect that could flip the state.
        #
        # The branch below emits an early "Starting workspace..." SSE pair so
        # the frontend can show a spinner instead of a silent wait. This is
        # broader than the old `ws_status == "stopped"` check — it also fires
        # on server-restart cold starts (workspace running in Daytona but no
        # session in memory). The extra "starting/ready" SSE pair is harmless.
        if not needs_startup:
            session = await workspace_manager.get_session_for_workspace(
                workspace_id, user_id=user_id
            )
        else:
            yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id})}\n\n"

            # Learn the pre-start sandbox state via a callback threaded
            # through session init → PTCSandbox.reconnect. The callback
            # fires once with the state string as soon as reconnect reads
            # it (before runtime.start() is invoked). We coordinate via
            # asyncio.Event + wait_for — no FIRST_COMPLETED race loop,
            # session_task is untouched by the wait_for timeout.
            state_event = asyncio.Event()
            state_box: dict[str, str | None] = {"value": None}

            def _on_state(state: str) -> None:
                state_box["value"] = state
                state_event.set()

            session_task = asyncio.create_task(
                workspace_manager.get_session_for_workspace(
                    workspace_id,
                    user_id=user_id,
                    on_state_observed=_on_state,
                )
            )

            try:
                # Wait up to 5s for reconnect to observe the sandbox state.
                # On the recovery path (new sandbox) the callback never fires;
                # we time out, skip the refinement, and proceed.
                try:
                    await asyncio.wait_for(state_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

                logger.info(
                    "[WS_STATUS] state observation",
                    extra={
                        "workspace_id": workspace_id,
                        "sandbox_state": state_box["value"],
                    },
                )

                if state_box["value"] == "archived":
                    yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id, 'sandbox_state': 'archived'})}\n\n"

                session = await session_task
                yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'ready', 'workspace_id': workspace_id})}\n\n"
            except BaseException:
                # Client disconnect / GeneratorExit / any error during the
                # yield or await chain above must not leak session_task.
                # Cancel and drain to surface the outcome (or CancelledError).
                if not session_task.done():
                    session_task.cancel()
                    with contextlib.suppress(BaseException):
                        await session_task
                raise

        _mark_phase("session")

        # Fire-and-forget: update workspace activity (conditional SQL, skip if <60s)
        _fire_and_forget(
            update_workspace_activity(workspace_id),
            name=f"update_activity_{workspace_id[:8]}",
        )

        # Post-session setup — parallelize when HITL (registry + plan interrupt check)
        registry_store = BackgroundRegistryStore.get_instance()
        if request.plan_mode:
            effective_plan_mode = True
            background_registry = await registry_store.get_or_create_registry(thread_id)
        elif request.hitl_response:
            background_registry, effective_plan_mode = await asyncio.gather(
                registry_store.get_or_create_registry(thread_id),
                _is_plan_interrupt_pending(thread_id),
            )
        else:
            effective_plan_mode = False
            background_registry = await registry_store.get_or_create_registry(thread_id)

        # Stamp the current turn's run_id on the registry so newly-registered
        # subagents inherit it (spawned_run_id). The collector filters by this
        # to avoid claiming subagents that belong to prior turns.
        background_registry.current_run_id = run_id

        # Build graph with the workspace's session
        # Note: agent.md is injected dynamically by WorkspaceContextMiddleware
        # on every model call, ensuring it's always the latest content.
        from src.server.app.workspace_sandbox import _set_cached_signed_url

        ptc_graph = await build_ptc_graph_with_session(
            session=session,
            config=config,
            subagent_names=subagents,
            # Structural recursion gate: a notification turn's agent is
            # built without Task/TaskOutput, so it cannot spawn background
            # work whose completion would notify again.
            disable_subagents=bool(request.disable_subagents),
            operation_callback=None,
            # I2: the run's fenced session-bound saver when the WriterGuard
            # is active; the global pooled saver otherwise.
            checkpointer=run_handle.checkpointer,
            background_registry=background_registry,
            # I2 (2.4e): the guard doubles as the task-namespace fence —
            # each background subagent takes exclusive N(thread, task:id) on
            # the run's pinned session before it may write.
            namespace_owner=run_handle.guard,
            user_id=user_id,
            plan_mode=effective_plan_mode,
            thread_id=thread_id,
            store=setup.store,
            on_signed_url=_set_cached_signed_url,
        )

        _mark_phase("graph_build")

        if session.sandbox:
            sandbox_id = getattr(session.sandbox, "sandbox_id", None)

        # PTC-only: set global for snapshot access
        setup.graph = ptc_graph

        messages = normalize_request_messages(request)

        # =====================================================================
        # Skill Context Resolution (body injection happens in SkillsMiddleware)
        # =====================================================================
        # Resolve which skills this turn activates — from additional_context or a
        # leading /<command> in the message (stripped in place). The SKILL.md body
        # is injected by SkillsMiddleware at turn entry, which dedups against bodies
        # already live in the thread so a re-sent skill isn't pasted every turn.
        #
        # Only set on normal turns: HITL resumes and checkpoint replays carry no new
        # user message, so the middleware must not inject (mirrors the prior guard).
        if not request.hitl_response and not is_checkpoint_replay:
            skill_contexts = prepare_skill_contexts(messages, request, mode="ptc")
        else:
            skill_contexts = None
        skill_dirs = (
            [local_dir for local_dir, _ in config.skills.local_skill_dirs_with_sandbox()]
            if skill_contexts
            else None
        )

        # Multimodal Context Injection
        # All attachments are uploaded to sandbox (when available) so the
        # agent always has file access.  Model-supported modalities also get
        # native content blocks merged into the user message.
        multimodal_contexts = parse_multimodal_contexts(request.additional_context)
        if multimodal_contexts and not request.hitl_response:
            # 1. Upload ALL files to sandbox
            file_paths: list = []
            if session and session.sandbox:
                file_paths = await upload_to_sandbox(
                    multimodal_contexts, session.sandbox
                )
                logger.info(
                    f"[PTC_CHAT] Uploaded {len(multimodal_contexts)} attachment(s) to sandbox"
                )

            # 2. Filter by model capability for native content blocks
            modalities = get_input_modalities(effective_model, custom_modalities=config.input_modalities) if effective_model else ["text"]
            supported, unsupported, file_only = filter_multimodal_by_capability(
                multimodal_contexts, modalities
            )

            # 3. Inject supported as native content blocks (merged into user message)
            if supported:
                supported_paths = [
                    file_paths[i]
                    for i, ctx in enumerate(multimodal_contexts)
                    if ctx in supported
                ] if file_paths else None
                messages = inject_multimodal_context(
                    messages, supported, file_paths=supported_paths
                )
                logger.info(
                    f"[PTC_CHAT] Multimodal context injected: "
                    f"{len(supported)} supported attachment(s)"
                )

            # Helper to build per-file path notes
            def _file_note(ctx, idx):
                desc = ctx.description or "file"
                data = ctx.data
                mime = data.split(":")[1].split(";")[0] if ":" in data else "unknown"
                fpath = file_paths[idx] if file_paths and idx < len(file_paths) else None
                if fpath:
                    return (
                        f"The user attached a file ({desc}, {mime}). "
                        f"It has been saved to {fpath}. "
                        f"Use Python to process it."
                    )
                return f"The user attached a file ({desc}, {mime})."

            # 4. Unsupported image/PDF: "cannot view" warning + file paths
            if unsupported:
                notes = [
                    _file_note(ctx, i)
                    for i, ctx in enumerate(multimodal_contexts)
                    if ctx in unsupported
                ]
                _append_to_last_user_message(
                    messages, build_unsupported_reminder(notes)
                )

            # 5. File-only (xlsx, csv, etc.): path notes only, no "cannot view"
            if file_only:
                notes = [
                    _file_note(ctx, i)
                    for i, ctx in enumerate(multimodal_contexts)
                    if ctx in file_only
                ]
                _append_to_last_user_message(
                    messages, build_file_reminder(notes)
                )
                logger.info(
                    f"[PTC_CHAT] {len(file_only)} file-only attachment(s) "
                    f"uploaded to sandbox for {effective_model}"
                )

        # Build input state or resume command
        if request.hitl_response:
            # Structured HITL resume payload.
            # Pydantic validates this into HITLResponse models, but LangChain's
            # HumanInTheLoopMiddleware expects plain dicts (subscriptable).
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(resume=resume_payload)
            logger.info(
                f"[PTC_RESUME] thread_id={thread_id} "
                f"hitl_response keys={list(request.hitl_response.keys())}"
            )
        elif is_checkpoint_replay:
            # Checkpoint replay/regenerate: no new messages, resume from checkpoint_id.
            # LangGraph will re-execute from the specified checkpoint state.
            input_state = None
            logger.info(
                f"[PTC_REPLAY] thread_id={thread_id} "
                f"checkpoint_id={request.checkpoint_id} (regenerate/retry)"
            )
        else:
            input_state = {
                "messages": messages,
                "current_agent": "ptc",  # For FileOperationMiddleware SSE events
            }
            # Skill tools auto-load via SkillsMiddleware (sets loaded_skills in state).

        # =====================================================================
        # Plan Mode Injection
        # =====================================================================
        # When plan_mode is enabled, inject a reminder for the agent to create
        # a plan and submit it for approval before executing any changes.
        if effective_plan_mode and not request.hitl_response:
            plan_mode_reminder = (
                "\n\n<system-reminder>\n"
                "[PLAN MODE ENABLED]\n"
                "Before making any changes, you MUST:\n"
                "1. Explore the codebase to understand the current state\n"
                "2. Create a detailed plan describing what you intend to do\n"
                "3. Call the `SubmitPlan` tool with your plan description\n"
                "4. Wait for user approval before proceeding with execution\n"
                "Do NOT execute any write operations until the plan is approved.\n"
                "</system-reminder>"
            )
            # Append reminder to the last user message
            if isinstance(input_state, dict) and input_state.get("messages"):
                _append_to_last_user_message(
                    input_state["messages"], plan_mode_reminder
                )
            logger.info(f"[PTC_CHAT] Plan mode enabled for thread_id={thread_id}")

        # =====================================================================
        # Inline Context Injection (directive + widget + chart selection)
        # =====================================================================
        # Each appends a <system-reminder> to the last user message, in order.
        # Widget image bytes ride MultimodalContext(type='image') above; chart
        # selections carry structured bounds + OHLCV bars (no screenshot). The
        # target is None on HITL-resume / checkpoint-replay (input_state is a
        # Command / None there), so injection is skipped on those turns.
        directives = parse_directive_contexts(request.additional_context)
        inline_target = (
            input_state["messages"]
            if isinstance(input_state, dict) and input_state.get("messages")
            else None
        )
        inject_inline_reminders(
            inline_target,
            [
                build_directive_reminder(directives),
                build_widget_context_reminder(widget_ctxs),
                build_chart_selection_reminder(chart_selections),
            ],
        )

        # =====================================================================
        # Save user request to system thread directory (non-critical)
        # =====================================================================
        if not request.hitl_response and session.sandbox:
            short_id = thread_id[:8]
            try:
                request_path = session.sandbox.normalize_path(
                    f".agents/threads/{short_id}/request.md"
                )
                _fire_and_forget(
                    session.sandbox.awrite_file_text(request_path, user_input),
                    name=f"write_request_{short_id}",
                )
            except Exception:
                pass  # normalize_path is sync, can still throw

        # =====================================================================
        # LangSmith Tracing Configuration
        # =====================================================================

        graph_config = build_graph_config(
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            mode="ptc",
            timezone_str=timezone_str,
            token_callback=token_callback,
            request=request,
            effective_model=effective_model,
            is_byok=is_byok,
            recursion_limit=get_ptc_recursion_limit(),
            plan_mode=effective_plan_mode,
            skill_contexts=skill_contexts,
            skill_dirs=skill_dirs,
            run_id=run_id,
            turn_index=run_handle.turn_index,
        )
        # Propagate run_id to LangGraph via the top-level config key; it
        # lands on ExecutionInfo.run_id and CheckpointMetadata.run_id so
        # LangSmith / checkpoint inspection can correlate by this UUID.
        graph_config["run_id"] = run_id

        handler = RunSSEProducer(
            thread_id=thread_id,
            run_id=run_id,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
            agent_config=config,
        )

        # Track steering messages injected mid-workflow for post-completion backfill
        setup_steering_tracking(handler)

        # =====================================================================
        # Background Execution with Completion Callback
        # =====================================================================

        # ``manager`` was acquired at the top of this handler for the early
        # steering-routing check; reuse it here. ``cancel_stale_workflow``
        # already ran there (gated on ``needs_startup``) so steering routed
        # against live state.

        # Pre-finalize artifact hook: capture sandbox images -> upload to
        # cloud storage -> rewrite storage URLs in the events about to be
        # archived. Runs inside _finalize_run, before the terminal txn.
        async def capture_artifacts(sse_events):
            if session and session.sandbox:
                from src.server.services.persistence.image_capture import (
                    capture_and_rewrite_images,
                )

                await capture_and_rewrite_images(
                    sse_events, session.sandbox, thread_id=thread_id,
                )

        # Post-finalize side effects only (v4): the durable terminal write —
        # response row, usage, projection — already happened in BTM's
        # _finalize_run before this callback fires. A failure here is logged
        # by the caller and never changes the run's outcome.
        async def on_background_workflow_complete(task_info):
            # Flash report-back moved to the hook outbox (1.7): finalize
            # enqueues a durable report_back job, so a crash right here can
            # no longer drop the dispatch. This callback keeps only
            # best-effort sandbox housekeeping.

            # Post-completion sandbox housekeeping (parallel)
            ws_manager = WorkspaceManager.get_instance()
            housekeeping = [ws_manager._backup_files_to_db(request.workspace_id)]
            if session and session.sandbox:
                housekeeping.append(session.sandbox.sync_skills_lock())
            results = await asyncio.gather(*housekeeping, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_name = "file backup" if i == 0 else "lock sync"
                    logger.warning(
                        f"[PTC_COMPLETE] {task_name} failed for {thread_id}: {result}"
                    )

        # Start workflow in background with event buffering
        await manager.start_run(
            thread_id=thread_id,
            run_id=run_id,
            workflow_generator=handler.stream_workflow(
                graph=ptc_graph,
                input_state=input_state,
                config=graph_config,
            ),
            metadata={
                "workspace_id": workspace_id,
                "user_id": user_id,
                "sandbox_id": sandbox_id,
                "sandbox": session.sandbox if session else None,
                "started_at": datetime.now().isoformat(),
                "start_time": start_time,
                "msg_type": "ptc",
                "is_byok": is_byok,
                "burst_slot_id": request.burst_slot_id,
                "locale": request.locale,
                "timezone": timezone_str,
                "handler": handler,
                "token_callback": token_callback,
                "run_handle": run_handle,
                "artifact_hook": capture_artifacts,
            },
            completion_callback=on_background_workflow_complete,
            graph=ptc_graph,
        )
        scope.transfer_to_executor()  # Manager owns burst slot release from here
        # Admission complete — release the lock so concurrent POSTs can
        # see the new RUNNING LocalRunExecution via ``wait_or_steer`` and route
        # to steering instead of contending here.
        scope.release_admission()

        _mark_phase("workflow_start")
        total_ms = (time.time() - start_time) * 1000
        phases = " ".join(f"{k}={v:.0f}ms" for k, v in _phase_times.items())
        llm_def = config.llm_definition
        model_tag = (
            f"{llm_def.provider}/{llm_def.model_id}" if llm_def
            else config.llm.name if config.llm else "unknown"
        )
        logger.info(
            f"[PTC_TIMING] thread_id={thread_id} model={model_tag} total={total_ms:.0f}ms ({phases})"
        )

        # Attach phase timings as attributes on the active chat.turn span so
        # traces show the same breakdown the log line does, and emit one
        # histogram sample per phase so dashboards can render the breakdown.
        from opentelemetry import trace as _otel_trace

        _span = _otel_trace.get_current_span()
        if _span is not None and _span.is_recording():
            for _k, _v in _phase_times.items():
                _span.set_attribute(f"chat.turn.phase.{_k}_ms", _v)
            _span.set_attribute("chat.turn.total_ms", total_ms)
        for _k, _v in _phase_times.items():
            safe_record(chat_turn_phase_duration_ms, _v, {"phase": _k, "mode": "ptc"})

        # Stream-backed first-connect: read from workflow:stream:{tid}:{rid}
        # via XREAD BLOCK. The workflow runs as a fully detached background
        # task — disconnect cannot reach it.
        async for event in stream_from_log(thread_id, run_id, last_event_id=None):
            yield event

        # After the workflow ends, return any unconsumed steering messages so
        # the client can re-render them as locally-queued context for the next
        # turn instead of losing them silently.
        steering_event = await drain_steering_return_event(thread_id)
        if steering_event:
            logger.info(
                f"[PTC_CHAT] Returning unconsumed steering message(s) "
                f"to client: thread_id={thread_id}"
            )
            yield steering_event

    except (asyncio.CancelledError, GeneratorExit):
        if scope.slot_owned:
            await scope.fail_open("client disconnected during setup")
            logger.warning(
                f"[PTC_CHAT] Generator cancelled before workflow started: "
                f"thread_id={thread_id} workspace_id={workspace_id}"
            )
        else:
            logger.warning(
                f"[PTC_CHAT] Generator cancelled (client disconnect?): "
                f"thread_id={thread_id} workspace_id={workspace_id}"
            )
        raise

    except Exception as e:
        # Pre-START on the dispatched path: the primer at the HTTP boundary
        # is still driving this generator, so admission/dedup failures must
        # surface raw as HTTP errors — never SSE frames into a stream whose
        # run was never dispatched (nothing durable exists yet).
        if dispatched and run_handle is None:
            raise
        # =====================================================================
        # Error Recovery with Retry Logic
        # =====================================================================
        # The scope encodes ownership: its owned_run_handle is non-None only
        # while this generator still owns the run (pre-handoff). After
        # start_run, BTM's _finalize_run owns the terminal write and a
        # finalize here would race it.
        async for event in handle_workflow_error(
            e,
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            handler=handler,
            token_callback=token_callback,
            scope=scope,
            start_time=start_time,
            request=request,
            is_byok=is_byok,
            msg_type="ptc",
            log_prefix="PTC_CHAT",
            timezone_str=timezone_str,
        ):
            yield event

        raise

    finally:
        # Backstop for any error path that bypassed the normal release
        # (e.g., exception before start_run); idempotent on the scope.
        scope.release_admission()
        # Always stop execution tracking to prevent memory leaks and context pollution
        ExecutionTracker.stop_tracking()
