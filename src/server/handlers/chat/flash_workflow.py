"""Flash agent workflow — async generator streaming SSE events.

This module contains the ``astream_flash_workflow`` function, refactored from
the monolithic ``chat_handler.py``.  Common setup, persistence, error handling,
and streaming logic is delegated to shared helpers in ``_common``.

Flash mode is optimised for speed: no sandbox, no MCP, no workspace, and only
external tools (web search, market data, SEC filings).
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from uuid import uuid4

from fastapi import HTTPException
from langgraph.types import Command

from src.server.app import setup
from src.server.database.workspace import (
    get_flash_workspace_id,
    get_or_create_flash_workspace,
)
from src.server.handlers.streaming_handler import WorkflowStreamHandler
from src.server.models.chat import (
    ChatRequest,
    serialize_hitl_response_map,
)
from src.server.services.background_task_manager import BackgroundTaskManager
from src.server.services.turn_lifecycle import (
    QuerySpec,
    TurnCoordinator,
    protected_finalize,
)
from src.server.services.workflow_tracker import WorkflowTracker
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
    build_unsupported_reminder,
    filter_multimodal_by_capability,
    inject_multimodal_context,
    parse_multimodal_contexts,
)
from src.utils.tracking import ExecutionTracker
from src.server.dependencies.usage_limits import release_burst_slot
from ptc_agent.agent.flash import build_flash_graph
from ptc_agent.agent.graph import get_user_profile_for_prompt

from ._common import (
    _append_to_last_user_message,
    _resolve_fork,
    _resolve_timezone,
    admission_conflict_detail,
    apply_fetch_override,
    build_graph_config,
    dedup_retransmit_or_raise,
    ensure_thread,
    handle_workflow_error,
    init_tracking,
    inject_inline_reminders,
    logger,
    normalize_request_messages,
    prepare_skill_contexts,
    process_hitl_response,
    resolve_retry_of,
    serialize_context_metadata,
    setup_steering_tracking,
    wait_or_steer,
)
from src.config.settings import get_flash_recursion_limit

from .llm_config import resolve_llm_config
from .steering import (
    drain_steering_return_event,
    steer_thread,
)
from .stream_from_log import stream_from_log


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _reusable_flash_workspace(flash_workspace: dict | None, user_id: str) -> bool:
    """True when a pre-resolved row is the caller's canonical flash workspace.

    The route may hand in the flash workspace it already upserted so the
    workflow can skip a duplicate upsert. Trust it only when its id matches the
    deterministic UUID v5 for this user — defends against an unrelated workspace
    ever being threaded in.
    """
    return bool(
        flash_workspace
        and str(flash_workspace.get("workspace_id")) == get_flash_workspace_id(user_id)
    )


async def astream_flash_workflow(
    request: ChatRequest,
    thread_id: str,
    run_id: str,
    user_input: str,
    user_id: str,
    is_byok: bool = False,
    config=None,
    dispatched: bool = False,
    flash_workspace: dict | None = None,
):
    """Async generator that streams Flash agent workflow events.

    Flash mode: no sandbox, no MCP, external tools only (web search, market
    data, SEC filings). State keyed by ``(thread_id, run_id)``; same
    contract as PTC.
    """
    start_time = time.time()
    handler = None
    token_callback = None
    tool_tracker = None
    flash_graph = None
    run_handle = None
    workspace_id = None
    timezone_str = None

    ExecutionTracker.start_tracking()
    logger.info(f"[FLASH_CHAT] Starting flash workflow: thread_id={thread_id}")

    slot_owned = True
    admission_held = False
    admission_lock = None
    try:
        if not setup.agent_config:
            raise HTTPException(
                status_code=503,
                detail="Flash Agent not initialized. Check server startup logs.",
            )

        # =================================================================
        # Admission gate
        # =================================================================
        # Per-thread asyncio.Lock that serializes the
        # ``wait_or_steer → start_turn → start_workflow`` window.
        # See the BTM docstring on ``get_admission_lock`` for the race
        # this defends against.
        manager = BackgroundTaskManager.get_instance()
        admission_lock = await manager.get_admission_lock(thread_id)
        await admission_lock.acquire()
        admission_held = True

        # Idempotency probe under the admission lock: a retransmitted
        # request_key resolves to its existing run HERE, before the steering
        # or fork paths below can act on the duplicate (raises
        # DuplicateRequestError → structured duplicate_request SSE).
        await dedup_retransmit_or_raise(request)

        # =================================================================
        # Early steering routing
        # =================================================================
        # If a workflow is already running for this thread, route this POST
        # through the steering queue *before* any DB write — a steering
        # message is neither a run nor a turn (v4 identity model); its
        # content is archived on the owning response's metadata at finalize.
        # Admit a fresh turn, steer the running one, or 409 —
        # see ``wait_or_steer``. Dispatched flows own the pre-registered
        # ``(thread_id, run_id)`` placeholder, so they pass it as
        # ``exclude_run_id`` (ignore it in the admission scan) and
        # ``can_steer=False`` (any OTHER in-flight run is a hard conflict,
        # never a steer). Foreground turns steer; retries never do — a
        # /retry that finds another live run is a hard conflict, not an
        # (empty) steering message into that run.
        ready, steering_event = await wait_or_steer(
            manager,
            thread_id,
            user_input,
            user_id,
            steer_only=request.steer_only,
            can_steer=not dispatched and request.retry_of_run_id is None,
            exclude_run_id=run_id if dispatched else None,
        )
        if not ready:
            slot_owned = False
            await release_burst_slot(user_id, request.burst_slot_id)
            admission_lock.release()
            admission_held = False
            if steering_event:
                yield steering_event
            return

        # =================================================================
        # Database Persistence Setup
        # =================================================================

        # Reuse the flash workspace the route already upserted this request
        # (see ``_reusable_flash_workspace``); safe only because the route's
        # upsert already applied the touch side effects (updated_at/is_pinned).
        if _reusable_flash_workspace(flash_workspace, user_id):
            flash_ws = flash_workspace
        else:
            flash_ws = await get_or_create_flash_workspace(user_id)
        workspace_id = str(flash_ws["workspace_id"])

        await ensure_thread(
            request, thread_id, workspace_id, user_id, msg_type="flash",
            initial_query=user_input,
        )

        query_type, is_fork = await _resolve_fork(
            request=request,
            thread_id=thread_id,
            log_prefix="FLASH_FORK",
        )
        is_checkpoint_replay = bool(request.checkpoint_id and not request.messages)

        # Persist query start (with attachment and context metadata for display
        # in history).  This block is flash-specific because of multimodal guard
        # differences vs PTC.
        effective_model = config.llm.flash if config and config.llm else None
        query_metadata = {"msg_type": "flash"}
        if effective_model:
            query_metadata["llm_model"] = effective_model
        widget_ctxs = parse_widget_contexts(request.additional_context)
        chart_selections = parse_chart_selection_contexts(request.additional_context)
        if request.additional_context:
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
        serialize_context_metadata(request, query_metadata, user_input, mode="flash")

        # Extract HITL answer metadata for persistence
        feedback_action = None
        query_content = user_input

        if request.hitl_response:
            feedback_action, query_content, hitl_answers, interrupt_ids = (
                process_hitl_response(request)
            )
            query_metadata["hitl_interrupt_ids"] = interrupt_ids
            if hitl_answers:
                query_metadata["hitl_answers"] = hitl_answers

        # =================================================================
        # START txn (v4): query row + in_progress run row + thread
        # projection in one transaction. Checkpoint replays reuse the
        # preserved query row (query=None) and pin the fork turn; retries
        # chain a new attempt onto the failed run's turn (no truncation).
        # =================================================================
        retry_of = await resolve_retry_of(request, thread_id)
        run_handle = await TurnCoordinator.get_instance().start_turn(
            thread_id=thread_id,
            run_id=run_id,
            msg_type="flash",
            request_key=request.request_key,
            workspace_id=workspace_id,
            user_id=user_id,
            is_byok=is_byok,
            query=(
                None
                if is_checkpoint_replay
                else QuerySpec(
                    query_id=str(uuid4()),
                    content=query_content,
                    query_type=query_type,
                    feedback_action=feedback_action,
                    metadata=query_metadata,
                )
            ),
            turn_index=(
                retry_of["turn_index"]
                if retry_of is not None
                else request.fork_from_turn
                if (is_fork and is_checkpoint_replay)
                else None
            ),
            attempt_no=(retry_of["attempt_no"] + 1 if retry_of is not None else 1),
            retry_of_run_id=(
                str(retry_of["conversation_response_id"])
                if retry_of is not None
                else None
            ),
            # Durable on the row so the startup sweep can enqueue this run's
            # terminal hooks (burst release, watch clear) without any
            # in-process context surviving the crash.
            run_metadata={
                "user_id": user_id,
                "burst_slot_id": request.burst_slot_id,
                "report_back_ptc_thread_id": getattr(
                    request, "report_back_ptc_thread_id", None
                ),
            },
        )

        logger.info(
            f"[FLASH_CHAT] Run started: workspace_id={workspace_id} "
            f"turn_index={run_handle.turn_index}"
        )

        # =================================================================
        # Token and Tool Tracking
        # =================================================================

        token_callback, tool_tracker = init_tracking(thread_id)

        # =================================================================
        # Build Flash Agent Graph
        # =================================================================

        # Resolve LLM config (pre-resolved by route handler, fallback for
        # standalone use)
        if config is None:
            config = await resolve_llm_config(
                setup.agent_config,
                user_id,
                request.llm_model,
                is_byok,
                mode="flash",
                reasoning_effort=getattr(request, "reasoning_effort", None),
                fast_mode=getattr(request, "fast_mode", None),
                thread_id=thread_id,
            )

        # Resolve timezone for metadata (observability only -- agent clock
        # uses DB user_profile)
        timezone_str = _resolve_timezone(request.timezone, request.locale)

        # Propagate fetch model override to tool context
        apply_fetch_override(config)

        flash_user_profile = None
        if user_id:
            flash_user_profile = await get_user_profile_for_prompt(user_id)

        # Build flash graph (no sandbox, no session)
        flash_graph = build_flash_graph(
            config=config,
            checkpointer=setup.checkpointer,
            user_profile=flash_user_profile,
            store=setup.store,
        )

        messages = normalize_request_messages(request)

        # Multimodal Context Injection (images and PDFs) -- Flash-specific
        # ordering: inject multimodal before skills.
        # Filter by model capability: supported items are injected as native
        # content blocks; unsupported items get a text note (Flash has no
        # sandbox for file upload).
        multimodal_contexts = parse_multimodal_contexts(request.additional_context)
        if multimodal_contexts:
            modalities = get_input_modalities(effective_model, custom_modalities=config.input_modalities) if effective_model else ["text"]
            supported, unsupported, file_only = filter_multimodal_by_capability(
                multimodal_contexts, modalities
            )
            if file_only:
                logger.warning(
                    f"[FLASH_CHAT] {len(file_only)} file-only attachment(s) "
                    f"ignored (Flash mode has no sandbox)"
                )
            if supported:
                messages = inject_multimodal_context(messages, supported)
                logger.info(
                    f"[FLASH_CHAT] Multimodal context injected: "
                    f"{len(supported)} supported attachment(s)"
                )
            if unsupported:
                types = list(set(
                    "PDF" if (c.data if hasattr(c, "data") else "").startswith("data:application/pdf") else "image"
                    for c in unsupported
                ))
                _append_to_last_user_message(
                    messages,
                    build_unsupported_reminder(
                        [f"The user attached {', '.join(types)} file(s)."]
                    ),
                )
                logger.info(
                    f"[FLASH_CHAT] {len(unsupported)} unsupported attachment(s) "
                    f"noted for {effective_model}"
                )

        # Skill Context Resolution (Flash) — body injection happens in
        # SkillsMiddleware, which dedups bodies already live in the thread. Only
        # set on normal turns; HITL/replay carry no new user message to attach to.
        if not request.hitl_response and not is_checkpoint_replay:
            skill_contexts = prepare_skill_contexts(messages, request, mode="flash")
        else:
            skill_contexts = None
        skill_dirs = (
            [local_dir for local_dir, _ in config.skills.local_skill_dirs_with_sandbox()]
            if skill_contexts
            else None
        )

        # Inline context injection (directive + widget + chart selection) --
        # Flash-specific. Skip on HITL resumes and checkpoint replay because
        # `input_state` below replaces `messages` with `Command(resume=...)` /
        # `None`, so anything appended here would be silently discarded.
        skip_inline_injection = bool(request.hitl_response) or is_checkpoint_replay
        directives = parse_directive_contexts(request.additional_context)
        inject_inline_reminders(
            None if skip_inline_injection else messages,
            [
                build_directive_reminder(directives),
                build_widget_context_reminder(widget_ctxs),
                build_chart_selection_reminder(chart_selections),
            ],
        )

        # Build input state or resume command -- Flash-specific (no
        # ``current_agent`` key)
        if request.hitl_response:
            resume_payload = serialize_hitl_response_map(request.hitl_response)
            input_state = Command(resume=resume_payload)
            logger.info(
                f"[FLASH_RESUME] thread_id={thread_id} "
                f"hitl_response keys={list(request.hitl_response.keys())}"
            )
        elif is_checkpoint_replay:
            input_state = None
            logger.info(
                f"[FLASH_REPLAY] thread_id={thread_id} "
                f"checkpoint_id={request.checkpoint_id} (regenerate/retry)"
            )
        else:
            input_state = {"messages": messages}
            # Skill tools auto-load via SkillsMiddleware (sets loaded_skills in state).

        graph_config = build_graph_config(
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            mode="flash",
            timezone_str=timezone_str,
            token_callback=token_callback,
            request=request,
            effective_model=effective_model,
            is_byok=is_byok,
            recursion_limit=get_flash_recursion_limit(),
            skill_contexts=skill_contexts,
            skill_dirs=skill_dirs,
            run_id=run_id,
            turn_index=run_handle.turn_index,
        )
        graph_config["run_id"] = run_id

        handler = WorkflowStreamHandler(
            thread_id=thread_id,
            run_id=run_id,
            token_callback=token_callback,
            tool_tracker=tool_tracker,
            agent_config=config,
        )

        # Track steering messages injected mid-workflow for post-completion backfill
        setup_steering_tracking(handler)

        # =================================================================
        # Background Execution (same pattern as PTC for reconnection
        # support)
        # =================================================================

        tracker = WorkflowTracker.get_instance()
        # ``manager`` was acquired at the top of this handler for the early
        # steering-routing check; reuse it here.

        await tracker.mark_active(
            thread_id=thread_id,
            workspace_id=workspace_id,
            user_id=user_id,
            run_id=run_id,
            metadata={
                "started_at": datetime.now().isoformat(),
                "msg_type": "flash",
                "locale": request.locale,
                "timezone": timezone_str,
            },
        )

        try:
            await manager.start_workflow(
                thread_id=thread_id,
                run_id=run_id,
                workflow_generator=handler.stream_workflow(
                    graph=flash_graph,
                    input_state=input_state,
                    config=graph_config,
                ),
                metadata={
                    "workspace_id": workspace_id,
                    "user_id": user_id,
                    "started_at": datetime.now().isoformat(),
                    "start_time": start_time,
                    "msg_type": "flash",
                    "is_byok": is_byok,
                    "burst_slot_id": request.burst_slot_id,
                    "locale": request.locale,
                    "timezone": timezone_str,
                    "handler": handler,
                    "token_callback": token_callback,
                    "run_handle": run_handle,
                    # Keys the finalize outbox decision table: a report-back
                    # flash run gets watch_clear on ANY terminal — the
                    # consumption clear on completed, teardown + error wake
                    # on error/cancelled (1.7; no in-process hook remains).
                    "report_back_ptc_thread_id": getattr(
                        request, "report_back_ptc_thread_id", None
                    ),
                },
                graph=flash_graph,
            )
        except RuntimeError:
            # Race condition: another request registered first -- queue the
            # message. The admission lock should normally prevent reaching
            # this branch, but it's kept as a belt-and-braces fallback.
            # Dispatched flows (can_steer=False) must never steer, even in this
            # fallback: leave result None so they fall through to the 409, same
            # as the primary wait_or_steer path.
            #
            # v4: START already created this run's in_progress row; whichever
            # way this branch exits (steer away or 409), no executor will ever
            # own it — release the durable slot first.
            await protected_finalize(
                TurnCoordinator.get_instance().fail_open_run(
                    run_handle,
                    "superseded by concurrent run (admission fallback)",
                    status="cancelled",
                ),
                label=run_handle.run_id,
            )
            result = None if dispatched else await steer_thread(
                thread_id, user_input, user_id
            )
            if result:
                slot_owned = False
                await release_burst_slot(user_id, request.burst_slot_id)
                admission_lock.release()
                admission_held = False
                event_data = json.dumps(
                    {
                        "thread_id": thread_id,
                        "content": user_input,
                        "position": result["position"],
                    }
                )
                yield f"event: steering_accepted\ndata: {event_data}\n\n"
                return

            raise HTTPException(
                status_code=409, detail=admission_conflict_detail("running")
            )
        else:
            slot_owned = False  # Manager owns burst slot release from here
            # Admission complete — release the lock so subsequent POSTs
            # can see the new RUNNING TaskInfo via wait_or_steer.
            admission_lock.release()
            admission_held = False

        async for event in stream_from_log(thread_id, run_id, last_event_id=None):
            yield event

        # After the workflow ends, return any unconsumed steering messages so
        # the client can re-render them as locally-queued context for the next
        # turn instead of losing them silently.
        steering_event = await drain_steering_return_event(thread_id)
        if steering_event:
            logger.info(
                f"[FLASH_CHAT] Returning unconsumed steering message(s) "
                f"to client: thread_id={thread_id}"
            )
            yield steering_event

    except (asyncio.CancelledError, GeneratorExit):
        if slot_owned:
            await release_burst_slot(user_id, request.burst_slot_id)
            # Died between START and BTM handoff: release the open run slot
            # (Phase 1 has no recovery scanner). protected_finalize: a second
            # cancel on this already-cancelled stream task must not abort
            # the write.
            if run_handle is not None:
                await protected_finalize(
                    TurnCoordinator.get_instance().fail_open_run(
                        run_handle,
                        "client disconnected during setup",
                        status="cancelled",
                    ),
                    label=run_handle.run_id,
                )
            logger.warning(
                f"[FLASH_CHAT] Generator cancelled before workflow started: "
                f"thread_id={thread_id}"
            )
        else:
            logger.warning(
                f"[FLASH_CHAT] Generator cancelled (client disconnect?): "
                f"thread_id={thread_id}"
            )
        raise

    except Exception as e:
        # run_handle only while this generator still owns the run — after
        # handoff, BTM's _finalize_run owns the terminal write.
        async for event in handle_workflow_error(
            e,
            thread_id=thread_id,
            user_id=user_id,
            workspace_id=workspace_id,
            handler=handler,
            token_callback=token_callback,
            run_handle=run_handle if slot_owned else None,
            start_time=start_time,
            request=request,
            is_byok=is_byok,
            msg_type="flash",
            log_prefix="FLASH_CHAT",
            timezone_str=timezone_str,
        ):
            yield event

        raise

    finally:
        # Release admission lock if any error path bypassed the normal
        # release (e.g., exception before start_workflow).
        if admission_held and admission_lock is not None:
            admission_lock.release()
        ExecutionTracker.stop_tracking()
