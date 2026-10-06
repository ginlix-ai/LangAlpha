"""PTC (Programmatic Tool Calling) workflow — async SSE generator.

This module contains the ``astream_ptc_workflow`` async generator, refactored
from the monolithic ``chat_handler.py``.  Request preparation, persistence,
error handling, and streaming logic is delegated to ``request_prep`` and
``services.runs.admission``; PTC-specific concerns (workspace session,
sandbox, background subagent orchestration, completion callback)
remain inline.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime
from functools import partial

from fastapi import HTTPException
from langgraph.types import Command

from ptc_agent.core.project_context import ProjectContext, run_with_project
from src.server.app import setup
from src.server.database.conversation.threads_read import (
    read_thread_subagents_allowed,
)
from src.server.database.workspace import update_workspace_activity
from src.server.services.computer_disk import TURN_MEASURE_MIN_INTERVAL_SECONDS
from src.server.services.runs.sse_producer import RunSSEProducer
from src.server.models.chat import (
    ChatRequest,
    serialize_hitl_response_map,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.server.services.runs.executor import LocalRunExecutor
from src.server.services.workspace_manager import WorkspaceManager
from src.server.services.workspace_layout import resolve_project_placement
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
from src.server.utils.credit_resume_context import build_credit_resume_update
from src.server.utils.multimodal_context import (
    build_attachment_metadata,
    parse_multimodal_contexts,
)

from ptc_agent.agent.agent import AgentRole
from ptc_agent.agent.graph import (
    build_ptc_graph_with_session,
    get_user_profile_for_prompt,
)
from ptc_agent.agent.middleware.credit_gate import run_with_credit_gate

from .request_prep import (
    DISPATCH_STARTED_MARKER,
    _resolve_fork,
    apply_fetch_override,
    build_graph_config,
    build_turn_context,
    read_disk_notice,
    ensure_thread,
    keep_named_model,
    init_tracking,
    inject_inline_reminders,
    logger,
    normalize_request_messages,
    prepare_skill_contexts,
    process_hitl_response,
    serialize_context_metadata,
    setup_steering_tracking,
    turn_skill_names,
    user_skill_commands,
)
from src.server.services.credit_gate_port import build_run_credit_gate
from src.server.services.report_back.flash import carry
from src.server.services.runs.admission import (
    RunScope,
    begin_run,
    dedup_retransmit_or_raise,
)
from src.config.settings import get_ptc_recursion_limit

from .admission_gate import steer_allowed, wait_or_steer
from .attachments import attach_request_files
from .error_handling import handle_workflow_error
from .flash_handover import cancel_tool_calls_it_lacks, hand_over_flash_thread
from src.server.services.llm.clients import is_own_key_turn
from src.server.services.llm.config import resolve_llm_config
from src.server.services.llm.thread_model import NamedModel
from .steering import drain_steering_return_event
from .run_stream_reader import stream_from_log
from .detached import fire_and_forget as _fire_and_forget


async def _resolve_project(session, workspace_id: str) -> ProjectContext | None:
    """The workspace folder this turn runs in, for the sandbox path layer.

    One read: the folder, and the folders parked beside it on the same machine
    so the agent can name a sibling directory as someone else's work rather
    than as material it is expected to read.

    Propagates ``WorkspaceLayoutUnavailable``, which fails the turn. The turn
    would otherwise run at the computer root, where it writes into a shared
    directory and mirrors every sibling's files back as its own.
    """
    sandbox = getattr(session, "sandbox", None)
    if sandbox is None:
        return None
    placement = await resolve_project_placement(workspace_id, root=sandbox.working_dir)
    return ProjectContext(
        workspace_id=workspace_id,
        dir_name=placement.dir_name,
        sibling_dir_names=placement.sibling_dir_names,
        layout_origin=placement.layout_origin,
        previous_dir_names=placement.previous_dir_names,
    )


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
    if not meta.get("origin_flash_thread_id"):
        # Only a thread a dispatch started has an origin. A generation without
        # one is a report-back's, stamped by the summary turn that answered it
        # (on Flash, or in Home), and is that pair's, not the thread's.
        meta = {}
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
    steerable: bool = True,
    run_metadata: dict | None = None,
    named_model: NamedModel | None = None,
    role: AgentRole = "analyst",
):
    """Async generator that streams PTC agent workflow events.

    ``run_id`` is generated at the handler entry in ``threads.py`` and is
    1:1 with ``conversation_response_id``. State (BTM, persistence, Redis
    stream key) is keyed by ``(thread_id, run_id)`` so concurrent turns
    on the same thread share no cross-turn state by construction.

    ``dispatched`` marks the call as an X-Dispatch=background invocation
    whose BTM placeholder was created upstream in ``threads.py``. It, a retry
    and ``steerable=False`` all make a running turn a 409 instead of a
    steer; see ``steer_allowed``. ``run_metadata`` is the caller's own START
    stamp on the run row, which the run's finalize hooks read.
    ``named_model`` is the model a client named for this thread, kept on it
    once the turn is admitted; automations never pass one. ``role`` is the
    turn route's, so the flag decides it in one place.
    """
    start_time = time.time()
    handler = None
    run_handle = None
    token_callback = None
    tool_tracker = None
    ptc_graph = None
    turn_context = None

    # Phase timing — collects wall-clock durations for each hot-path phase.
    # Emits a single structured summary line when the workflow starts.
    _phase_times: dict[str, float] = {}
    _phase_t0 = start_time

    def _mark_phase(name: str) -> None:
        nonlocal _phase_t0
        now = time.time()
        _phase_times[name] = (now - _phase_t0) * 1000  # ms
        _phase_t0 = now

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
        # ``wait_or_steer``; who may steer at all is ``steer_allowed``.
        ready, steering_event = await wait_or_steer(
            manager,
            thread_id,
            user_input,
            user_id,
            steer_only=request.steer_only,
            can_steer=steer_allowed(
                request, dispatched=dispatched, steerable=steerable
            ),
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

        prior_thread = await ensure_thread(
            request,
            thread_id,
            workspace_id,
            user_id,
            msg_type="ptc",
            initial_query=user_input,
        )
        disk_free_mb, disk_known = await read_disk_notice(workspace_id)
        user_profile = await get_user_profile_for_prompt(user_id) if user_id else None
        turn_context = build_turn_context(
            request,
            prior_thread,
            user_profile=user_profile,
            disk_free_mb=disk_free_mb,
            disk_known=disk_known,
        )

        query_type, fork = _resolve_fork(request=request)
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Resolve LLM config (pre-resolved by the route handler, fallback for
        # standalone use). Ahead of the metadata below on purpose: that block
        # records the model and the detected slash command, and both are read
        # off this config. Resolved late, the standalone path would stamp a
        # skill the turn then refuses, because activation gates on the
        # resolved registry while the metadata had nothing to gate on. The
        # route already resolves before this generator runs, so this only
        # moves the standalone path onto the ordering production has.
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config,
                user_id,
                request.llm_model,
                is_byok,
                mode="ptc",
                reasoning_effort=getattr(request, "reasoning_effort", None),
                fast_mode=getattr(request, "fast_mode", None),
                thread_id=thread_id,
                enabled_subagents=request.subagents_enabled,
                workspace_id=workspace_id,
            )

        # Persist query start
        feedback_action = None
        query_content = user_input
        effective_model = config.llm.name if config and config.llm else None
        # Off the resolved credential, not off ``is_byok``: that flag answers
        # which ladder to try, and an automation with only an OAuth token
        # passes it false while still paying its own vendor bill.
        own_key = is_own_key_turn(config)
        query_metadata = {
            # The resolved one: a request may name none, or name Flash's.
            "workspace_id": workspace_id,
            "msg_type": "ptc",
        }
        if effective_model:
            query_metadata["llm_model"] = effective_model
        if request.origin:
            # Per-turn initiator, mirroring the thread-level metadata['origin']
            # (threads go mixed: a pinned automation thread also takes manual
            # user follow-ups, which carry no origin).
            query_metadata["origin"] = request.origin.model_dump(exclude_none=True)

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
                query_metadata["widget_contexts"] = (
                    serialize_widget_contexts_for_metadata(widget_ctxs)
                )
            if chart_selections:
                query_metadata["chart_selections"] = (
                    serialize_chart_selections_for_metadata(chart_selections)
                )

        # Persist lightweight additional_context + slash command fallback
        # (serialize_context_metadata's slash-command branch already guards
        # on `not request.hitl_response`, so this is safe to call always.)
        if not request.hitl_response:
            serialize_context_metadata(
                request,
                query_metadata,
                user_input,
                mode="ptc",
                extra_commands=user_skill_commands(config),
                allowed_skills=turn_skill_names(config, "ptc"),
            )

        if request.hitl_response:
            prepared = process_hitl_response(request)
            feedback_action = prepared.feedback_action
            query_content = prepared.query_content
            query_metadata.update(prepared.metadata)

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
        carried = await carry.carried_pair(request, thread_id)
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
            extra_run_metadata={**origin_meta, **carried, **(run_metadata or {})},
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

        await keep_named_model(thread_id, named_model)

        # =====================================================================
        # Token and Tool Tracking
        # =====================================================================

        token_callback, tool_tracker = init_tracking(thread_id)

        # Runtime credit gate (None when platform gating is inactive): the
        # run's spend meter, lease, and refresher. Admission above is its
        # seed verdict; the stream wrapper below owns its lifetime.
        credit_gate = build_run_credit_gate(
            user_id,
            run_id,
            token_callback,
            tool_tracker,
            effective_model,
            is_byok=own_key,
        )

        _mark_phase("db_setup")

        # =====================================================================
        # Session and Graph Setup
        # =====================================================================

        # Propagate fetch model override to tool context
        apply_fetch_override(config)

        _mark_phase("pre_session")

        subagents = request.subagents_enabled or config.subagents.enabled
        sandbox_id = None

        # The turn's skill state, folded to one value so the acquire can tell
        # a warm sandbox its skills moved (upload/delete/disable) without any
        # extra read — the bundle behind these fields was already loaded by
        # the config resolve above.
        from src.server.services.user_skills import skills_delivery_signature

        skills_signature = skills_delivery_signature(
            config.user_skill_dir, config.disabled_skills
        )

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
                workspace_id,
                user_id=user_id,
                skills_signature=skills_signature,
                run_id=run_id,
            )
        else:
            yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id})}\n\n"

            # Learn the pre-start sandbox state via a callback threaded
            # through session init → PTCSandbox.reconnect. The callback
            # fires once with the state string as soon as reconnect reads
            # it (before runtime.start() is invoked). We coordinate via
            # asyncio.Event and the acquisition race each other so a warm sibling
            # does not wait for a reconnect callback it can never emit.
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
                    skills_signature=skills_signature,
                    run_id=run_id,
                )
            )

            try:

                async def _wait_for_session():
                    return await asyncio.shield(session_task)

                state_waiter = asyncio.create_task(state_event.wait())
                session_waiter = asyncio.create_task(_wait_for_session())
                try:
                    done, pending = await asyncio.wait(
                        {state_waiter, session_waiter},
                        timeout=5.0,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except BaseException:
                    state_waiter.cancel()
                    session_waiter.cancel()
                    await asyncio.gather(
                        state_waiter, session_waiter, return_exceptions=True
                    )
                    raise
                for waiter in pending:
                    waiter.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

                logger.info(
                    "[WS_STATUS] state observation",
                    extra={
                        "workspace_id": workspace_id,
                        "sandbox_state": state_box["value"],
                    },
                )

                if state_box["value"] == "archived":
                    yield f"id: 0\nevent: workspace_status\ndata: {json.dumps({'status': 'starting', 'workspace_id': workspace_id, 'sandbox_state': 'archived'})}\n\n"

                session = (
                    await session_waiter
                    if session_waiter in done
                    else await session_task
                )
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

        if role == "chief_of_staff":
            # Home is often the workspace whose turn recreated the sandbox,
            # and its agent reads and edits the other workspaces' folders.
            await workspace_manager.restore_sibling_folders(session, workspace_id)

        # Fire-and-forget: update workspace activity (conditional SQL, skip if <60s)
        _fire_and_forget(
            update_workspace_activity(workspace_id),
            name=f"update_activity_{workspace_id[:8]}",
        )

        registry_store = BackgroundRegistryStore.get_instance()
        background_registry = await registry_store.get_or_create_registry(thread_id)

        # Stamp the current turn's run_id on the registry so newly-registered
        # subagents inherit it (spawned_run_id). The collector filters by this
        # to avoid claiming subagents that belong to prior turns.
        background_registry.current_run_id = run_id

        # Build graph with the workspace's session
        # Note: agent.md is injected by the runtime-context baseline middleware
        # on every model call, ensuring it's always the latest content.
        from src.server.app.workspace_sandbox import _set_cached_signed_url
        from src.server.services.egress.direct_tools import (
            bind_direct_mcp_tools,
            direct_tools_for_turn,
        )
        from src.server.services.trading_rule import with_trading_rule

        # Resolved once and used three times: the agent build mounts the
        # workspace's memory off it, the post-turn reconcile materialises that
        # folder's skills, and the run's own task binds it for the path layer.
        project = await _resolve_project(session, workspace_id)

        async def cache_workspace_preview(
            preview_sandbox_id: str, port: int, signed_url: str
        ) -> None:
            await _set_cached_signed_url(
                preview_sandbox_id,
                port,
                signed_url,
                owner_workspace_id=workspace_id,
            )
        # Frozen for this project at acquire; the session's own MCP fields are
        # whichever sibling on the machine resolved last.
        tool_view = workspace_manager.tool_view(session, workspace_id)

        # One relay session per directly bound server, held for the run.
        direct_mcp, order_ledger = await direct_tools_for_turn(
            bind_direct_mcp_tools(
                session, user_id=user_id, workspace_id=workspace_id, view=tool_view
            ),
            user_id=user_id,
            workspace_id=workspace_id,
            thread_id=thread_id,
            run_id=run_id,
            turn_index=run_handle.turn_index,
        )
        # Off the resolve, not the bind, so it moves only when a setting does.
        user_profile = with_trading_rule(user_profile, tool_view.trading_rule)

        ptc_graph = await build_ptc_graph_with_session(
            session=session,
            config=config,
            subagent_names=subagents,
            # Optional structural gate: builds the agent without
            # Task/TaskOutput. Task report-back turns don't set it (they
            # need TaskOutput to fetch the result); their re-announce
            # recursion is handled by the outbox's ledger arbitration.
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
            user_profile=user_profile,
            thread_id=thread_id,
            store=setup.store,
            on_signed_url=cache_workspace_preview,
            direct_mcp=direct_mcp,
            order_ledger=order_ledger,
            turn_context=turn_context,
            project=project,
            tool_view=tool_view,
            role=role,
            # A port, not a value: the switch can flip mid-turn from any
            # worker, so the agent re-reads the row on every call.
            subagent_switch=partial(read_thread_subagents_allowed, thread_id),
        )

        _mark_phase("graph_build")

        cancelled_calls = []
        if prior_thread.msg_type == "flash":
            cancelled_calls = await hand_over_flash_thread(
                ptc_graph, thread_id, replay=bool(request.checkpoint_id)
            )
        elif (
            role == "chief_of_staff"
            and request.hitl_response
            and not request.checkpoint_id
        ):
            # A Home thread Flash ran while the all-workspaces agent was off
            # keeps its type, and may be waiting on a step only Flash has.
            cancelled_calls = await cancel_tool_calls_it_lacks(ptc_graph, thread_id)

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
            skill_contexts = prepare_skill_contexts(
                messages,
                request,
                mode="ptc",
                extra_commands=user_skill_commands(config),
                allowed_skills=turn_skill_names(config, "ptc"),
            )
        else:
            skill_contexts = None
        skill_dirs = (
            [
                local_dir
                for local_dir, _ in config.skills.local_skill_dirs_with_sandbox()
            ]
            + ([config.user_skill_dir] if config.user_skill_dir else [])
            if skill_contexts
            else None
        )

        # Multimodal Context Injection
        messages = await attach_request_files(
            messages, request, session, effective_model, config, project=project
        )

        # Build input state or resume command
        if request.hitl_response:
            # Structured HITL resume payload.
            # Pydantic validates this into HITLResponse models, but LangChain's
            # HumanInTheLoopMiddleware expects plain dicts (subscriptable).
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(
                resume=resume_payload,
                # None for a resume that was not credit-paused, which is what
                # ``update`` defaults to anyway.
                update=await build_credit_resume_update(thread_id, run_id),
            )
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
        if not request.hitl_response and session.sandbox and project:
            short_id = thread_id[:8]
            try:
                # The project is passed explicitly: this runs on the request
                # task, before ``run_with_project`` binds the turn's, so there
                # is no bound project for the sandbox to resolve against.
                request_path = (
                    f"{session.sandbox.workspace(project).thread_dir(short_id)}"
                    "/request.md"
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
            timezone_str=turn_context.tool_timezone,
            token_callback=token_callback,
            request=request,
            effective_model=effective_model,
            recursion_limit=get_ptc_recursion_limit(),
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
                    sse_events,
                    session.sandbox,
                    thread_id=thread_id,
                    project=project,
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

            # Post-completion sandbox housekeeping. Reconcile BEFORE the
            # backup — the reconcile mutates the skills ledger and dirs, and
            # the file backup should capture the converged state.
            ws_manager = WorkspaceManager.get_instance()
            if session and session.sandbox:
                # The run is terminal, so a settle may move the folder now: the
                # pass reads it under the folder hold, not from the turn start.
                await ws_manager.reconcile_skills_if_running(
                    workspace_id, user_id, source="post_turn"
                )
            try:
                # Any project on the machine may have changed, not only this
                # one: the sweep finds which, and mirrors those.
                await ws_manager.backup_changed_projects(
                    workspace_id, session=session
                )
            except Exception as e:
                logger.warning(
                    f"[PTC_COMPLETE] file backup failed for {thread_id}: {e}"
                )
            # A turn is what fills the shared disk, so its end is when the
            # reading the warning and the next turn's context rely on moves.
            if session and session.computer_id:
                await ws_manager.refresh_computer_disk(
                    session.computer_id,
                    min_age_s=TURN_MEASURE_MIN_INTERVAL_SECONDS,
                )

        # Start workflow in background with event buffering
        await manager.start_run(
            thread_id=thread_id,
            run_id=run_id,
            # The project is bound outermost, in the run's own task: a graph
            # node runs in a task created from the caller's context, so a
            # ContextVar set inside the graph would not reach the next node.
            workflow_generator=run_with_project(
                project,
                run_with_credit_gate(
                    credit_gate,
                    # Relay sessions for direct tools open and close with the
                    # run, in the run's task, not with this request's generator.
                    direct_mcp.drive(
                        handler.stream_workflow(
                            graph=ptc_graph,
                            input_state=input_state,
                            config=graph_config,
                            settled_results=cancelled_calls,
                        )
                    ),
                ),
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
                "timezone": turn_context.tool_timezone,
                "handler": handler,
                "token_callback": token_callback,
                "run_handle": run_handle,
                "artifact_hook": capture_artifacts,
            },
            completion_callback=on_background_workflow_complete,
            graph=ptc_graph,
            # Manager owns burst slot release from registration on
            on_registered=scope.transfer_to_executor,
        )
        # Admission complete — release the lock so concurrent POSTs can
        # see the new RUNNING LocalRunExecution via ``wait_or_steer`` and route
        # to steering instead of contending here.
        scope.release_admission()

        _mark_phase("workflow_start")
        total_ms = (time.time() - start_time) * 1000
        phases = " ".join(f"{k}={v:.0f}ms" for k, v in _phase_times.items())
        llm_def = config.llm_definition
        model_tag = (
            f"{llm_def.provider}/{llm_def.model_id}"
            if llm_def
            else config.llm.name
            if config.llm
            else "unknown"
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
            logger.warning(
                f"[PTC_CHAT] Generator cancelled before workflow started: "
                f"thread_id={thread_id} workspace_id={workspace_id}"
            )
            await scope.fail_open("client disconnected during setup")
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
            turn_context=turn_context,
        ):
            yield event

        raise

    finally:
        # Backstop for any error path that bypassed the normal release
        # (e.g., exception before start_run); idempotent on the scope.
        scope.release_admission()
