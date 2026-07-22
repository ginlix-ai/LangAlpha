"""Thread messaging: send/stream SSE, reconnect/replay, control, retry, mux."""

import json
from typing import Optional
from uuid import uuid4

import asyncio

from fastapi import Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
    require_workspace_owner,
    service_token_matches,
)
from src.server.models.chat import ChatRequest
from src.server.models.workflow import RetryRequest
from src.server.database.conversation import (
    get_thread_by_id,
    get_thread_owner_id,
    lookup_thread_by_external_id,
    get_replay_thread_data,
)
from psycopg_pool import PoolTimeout
from src.server.dependencies.usage_limits import ChatRateLimited

from src.observability import (
    observe_background_chat_turn,
    observe_chat_stream,
    observe_replay_stream,
    safe_add,
    sse_reconnects,
)

# Import setup module to access initialized globals
from src.server.app import setup

from ._deps import SSE_HEADERS, _get_service_token, _track_task, logger, router


async def _assert_stream_transport_ready() -> None:
    """503 before any durable row when the Redis event transport is down (I6).

    Every chat consumer — first connect included — tails the Redis stream
    (``stream_from_log``), so a deployment without Redis event storage can
    never deliver a live turn: refuse admission outright instead of
    committing a 200 to a stream nothing will ever write. When Redis IS
    configured, a PING failure here means the run's very first buffered
    event would finalize it failed(transport_lost); refusing admission is
    strictly cheaper.
    """
    from src.server.services.runs.executor import LocalRunExecutor
    from src.utils.cache.redis_cache import get_cache_client

    manager = LocalRunExecutor.get_instance()
    if not (manager.enable_storage and manager.event_storage_backend == "redis"):
        raise HTTPException(
            status_code=503,
            detail={
                "code": "transport_unavailable",
                "message": (
                    "This deployment is configured without the Redis "
                    "event-stream transport; live chat streaming is "
                    "unavailable."
                ),
            },
        )
    try:
        cache = get_cache_client()
        if cache.enabled and cache.client and await cache.client.ping():
            return
    except Exception:
        pass
    raise HTTPException(
        status_code=503,
        detail={
            "code": "transport_unavailable",
            "message": (
                "The event-stream transport is temporarily unreachable; "
                "retry shortly."
            ),
        },
        headers={"Retry-After": "3"},
    )


async def _consume_background_gen(
    gen,
    label: str,
    thread_id: str,
    run_id: str,
) -> bool:
    """Drain an async generator in the background, cleaning up Redis on failure."""
    _ok = True
    _error_text: str | None = None
    try:
        async for _ in gen:
            pass
    except Exception as exc:
        _ok = False
        _error_text = f"{type(exc).__name__}: {exc}"
        logger.error(
            f"[{label}] Background workflow failed: thread_id={thread_id} run_id={run_id}",
            exc_info=True,
        )
    finally:
        # Ownership check: if BTM still drives this exact run's workflow
        # task, this generator was only a dead tail (e.g. the stream
        # consumer failed after handoff). The executor owns the ledger row
        # and all terminal transport — finalizing here would terminalize a
        # row whose graph is still checkpointing, and BTM would later lose
        # its own CAS.
        _btm_live = False
        try:
            from src.server.services.runs.executor import (
                LocalRunExecutor,
            )

            _btm_live = await LocalRunExecutor.get_instance().is_run_live(
                thread_id, run_id
            )
        except Exception:
            pass
        if not _ok and _btm_live:
            logger.warning(
                f"[{label}] stream consumer died but the run's executor is "
                f"still live: thread_id={thread_id} run_id={run_id}; leaving "
                f"the run to its owner"
            )

        # When the generator raised before reaching start_run, the
        # frontend already received {status: dispatched, run_id} and
        # navigated to workflow:stream:{tid}:{rid} — but no events will
        # ever land. The coordinator is the last-resort owner (I6): it
        # settles a still-in_progress row and writes the terminal frames a
        # reconnected client needs; it never raises.
        if not _ok and not _btm_live:
            # This generator was already primed past START (2.4c) — the run
            # row exists, so the coordinator's reconcile (here, or the real
            # owner's finalize) enqueues the durable watch_clear on the
            # flash ordering chain, which owns any pair teardown. No direct
            # report-back clear here: it would run OFF-CHAIN against a live
            # admission (round-19 P1).
            from src.server.services.runs.coordinator import RunCoordinator

            await RunCoordinator.get_instance().reconcile_orphaned_dispatch(
                thread_id, run_id, error_text=_error_text, label=label
            )

    return _ok


# =============================================================================
# THREAD MESSAGES (SSE streams)
# =============================================================================


@router.post("/messages")
async def send_new_thread_message(
    request: ChatRequest, auth: ChatRateLimited, raw_request: Request
):
    """
    Create a new thread and send the first message. Returns an SSE stream.

    The server creates a new thread_id and returns it in SSE events.
    If external_thread_id + platform are provided, resolves to an existing thread first.
    """
    thread_id = None
    if request.external_thread_id and request.platform:
        thread_id = await lookup_thread_by_external_id(
            request.platform, request.external_thread_id, auth.user_id
        )
        if thread_id:
            logger.info(
                f"[CHAT] Resolved external_thread_id={request.external_thread_id} "
                f"platform={request.platform} -> thread_id={thread_id}"
            )
    if not thread_id:
        thread_id = str(uuid4())
    return await _handle_send_message(request, auth, thread_id, raw_request)


@router.post("/{thread_id}/messages")
async def send_thread_message(
    thread_id: str, request: ChatRequest, auth: ChatRateLimited,
    raw_request: Request,
):
    """
    Send a message to an existing thread. Returns an SSE stream.
    """
    return await _handle_send_message(request, auth, thread_id, raw_request)


async def _reject_duplicate_request(request_key: str, user_id: str) -> None:
    """409 if this request_key already produced a run — a retransmit.

    Route-level twin of the in-generator ``dedup_retransmit_or_raise``:
    classifying here answers with a clean HTTP 409 before a thread is
    minted, a fork truncates the rows holding the key, or a steering path
    consumes the duplicate. Discloses the existing run's identity only to
    the owning user.
    """
    from src.server.database.runs import lifecycle as tl_db

    existing = await tl_db.find_run_by_request_key(request_key)
    if existing is None:
        return
    existing_thread = str(existing["conversation_thread_id"])
    owner_id = await get_thread_owner_id(existing_thread)
    detail: dict = {
        "code": "duplicate_request",
        "message": (
            "This request was already accepted; reconnect to the existing "
            "run instead of resending."
        ),
    }
    # Fail closed: an unresolvable owner (thread deleted mid-probe) gets the
    # bare conflict, never another user's run identity.
    if owner_id is not None and owner_id == user_id:
        detail.update(
            thread_id=existing_thread,
            run_id=str(existing["conversation_response_id"]),
            run_status=existing["status"],
        )
    raise HTTPException(status_code=409, detail=detail)


async def _handle_send_message(
    request: ChatRequest, auth: ChatRateLimited, thread_id: str,
    raw_request: Request | None = None,
    *,
    retry_of_run_id: str | None = None,
):
    """Shared logic for both POST /threads/messages and POST /threads/{id}/messages.

    ``retry_of_run_id`` is retry provenance and route-internal: only the
    /retry route passes it. Whatever the public body carried is overwritten
    — a forged value could chain a new attempt onto an arbitrary failed run.
    """
    from src.server.handlers.chat import (
        astream_flash_workflow,
        astream_ptc_workflow,
    )
    from src.server.database.workspace import get_or_create_flash_workspace

    from src.server.database.workspace import get_workspace

    # Canonical run_id generation site. Each POST gets a fresh UUID that
    # flows through every downstream key: BTM ``(tid, rid)``, persistence
    # service, ``workflow:stream:{tid}:{rid}``, LangGraph ``config["run_id"]``
    # → ``CheckpointMetadata.run_id``, and the SSE ``metadata`` event the
    # frontend sees as the first event of the stream. 1:1 with
    # ``conversation_response_id``.
    run_id = str(uuid4())

    user_id = auth.user_id
    is_byok = auth.is_byok
    agent_mode = request.agent_mode or "ptc"
    workspace_id = request.workspace_id

    from src.server.services.runs.admission import RunScope

    # HTTP-window owner of the burst lease: every pre-generator failure and
    # dispatch-priming exit that doesn't hand the run to an executor releases
    # through this scope (idempotent). The generator carries its own scope
    # for the window it owns — same layering as before, one vocabulary.
    scope = RunScope(user_id=user_id, burst_slot_id=auth.burst_slot_id)

    try:
        # Retry provenance: force the route-supplied value over anything in
        # the public body (see docstring).
        if request.retry_of_run_id != retry_of_run_id:
            request = request.model_copy(update={"retry_of_run_id": retry_of_run_id})

        # Burst slot: server-stamped from the admission dependency; never
        # trust a client-sent value (see ChatRequest.burst_slot_id).
        if request.burst_slot_id != auth.burst_slot_id:
            request = request.model_copy(
                update={"burst_slot_id": auth.burst_slot_id}
            )

        # Idempotency: a request_key that already produced a run is a
        # retransmit — classify it before any durable work happens for
        # this copy (thread creation, fork truncation, steering).
        if request.request_key:
            await _reject_duplicate_request(request.request_key, user_id)

        # 403 guard: require BYOK, OAuth, or platform access (tier >= 0).
        # All flags are pre-checked by enforce_chat_limit — no DB calls here.
        from src.config.settings import HOST_MODE
        if HOST_MODE == "platform" and not auth.is_byok and not auth.has_oauth and auth.access_tier < 0:
            raise HTTPException(
                status_code=403,
                detail={
                    "message": "No provider configured. Set up an API key or connect via OAuth.",
                    "type": "no_provider",
                    "link": {"url": "/setup/method", "label": "Set up provider"},
                },
            )

        # Reject an unauthenticated background dispatch up front, before the
        # owner lookup, workspace/LLM resolution, and credit check below — a
        # request that will 403 anyway shouldn't do that work first. In oss mode
        # with no INTERNAL_SERVICE_TOKEN configured there is nothing to
        # authenticate against, so the self-dispatch is trusted; a configured
        # token is enforced in every mode. This single is_internal value is
        # reused by the field strip and both dispatch branches below.
        _req_token = (raw_request.headers.get("X-Service-Token", "") if raw_request else "")
        _svc_token = _get_service_token()
        is_internal = service_token_matches(_req_token, _svc_token) or (
            HOST_MODE == "oss" and not _svc_token
        )
        if (
            not is_internal
            and raw_request
            and raw_request.headers.get("X-Dispatch") == "background"
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Background dispatch requires internal service auth. "
                    "Configure INTERNAL_SERVICE_TOKEN and send it as "
                    "X-Service-Token."
                ),
            )

        # IDOR guard: an existing thread must belong to the caller. A brand-new
        # thread_id has no owner yet -> creation proceeds. The internal report-back
        # dispatch sets X-User-Id to the owner, so it passes.
        owner_id = await get_thread_owner_id(thread_id) if thread_id else None
        if owner_id is not None and owner_id != user_id:
            raise HTTPException(status_code=403, detail="Forbidden")

        # Resolve workspace_id from thread if not provided
        if not workspace_id and thread_id:
            thread_record = await get_thread_by_id(thread_id)
            if thread_record:
                workspace_id = str(thread_record["workspace_id"])
                logger.debug(
                    f"[CHAT] Resolved workspace_id={workspace_id} from thread_id={thread_id}"
                )

        # Validate that agent_config is initialized
        if not hasattr(setup, "agent_config") or setup.agent_config is None:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs.",
            )

        # Validate workspace_id for ptc mode
        if agent_mode == "ptc" and not workspace_id:
            raise HTTPException(
                status_code=400,
                detail="workspace_id is required for 'ptc' agent mode. Create workspace first via POST /workspaces, or use agent_mode='flash' for lightweight queries.",
            )

        # For flash mode, resolve workspace_id to the shared flash workspace.
        # The upsert returns the full row, reused by the ownership guard below
        # and by the flash workflow (skipping a repeat upsert).
        workspace: dict | None = None
        flash_workspace: dict | None = None
        if agent_mode == "flash" and not workspace_id:
            workspace = await get_or_create_flash_workspace(user_id)
            workspace_id = str(workspace["workspace_id"])
            flash_workspace = workspace

        # Single workspace lookup, shared by the flash auto-detect and the
        # ownership guard below — one DB round-trip instead of two.
        if workspace is None and workspace_id:
            workspace = await get_workspace(workspace_id)

        # Auto-detect flash workspaces: if the workspace is flash, override
        # agent_mode so follow-up messages (HITL responses, etc.) route
        # correctly even if the client doesn't send agent_mode='flash'. Skip
        # the status check when a ready session exists (PTC workspace, common path).
        if agent_mode != "flash" and workspace_id:
            from src.server.services.workspace_manager import WorkspaceManager
            if not WorkspaceManager.get_instance().has_ready_session(workspace_id):
                if workspace and workspace.get("status") == "flash":
                    agent_mode = "flash"
                    logger.debug(
                        f"[CHAT] Auto-detected flash workspace {workspace_id}, "
                        f"overriding agent_mode to 'flash'"
                    )

        # IDOR guard (workspace dimension): pairs with the thread guard above so
        # a fresh thread_id cannot run inside another user's workspace/sandbox.
        # The internal report-back dispatch sets X-User-Id to the owner, so it passes.
        require_workspace_owner(workspace, user_id=user_id)

        # Extract user input
        user_input = ""
        if request.messages:
            last_msg = request.messages[-1]
            if isinstance(last_msg.content, str):
                user_input = last_msg.content
            elif isinstance(last_msg.content, list):
                for item in last_msg.content:
                    if hasattr(item, "text") and item.text:
                        user_input = item.text
                        break

        logger.info(
            f"[{'FLASH' if agent_mode == 'flash' else 'PTC'}_CHAT] New request: "
            f"workspace_id={workspace_id} thread_id={thread_id} user_id={user_id} "
            f"mode={agent_mode}"
        )

        # Resolve LLM config eagerly — credit check must happen before SSE stream starts
        from src.server.services.llm.config import resolve_llm_config
        from src.server.dependencies.usage_limits import enforce_credit_limit
        from ptc_agent.config.agent import CredentialSource

        config = await resolve_llm_config(
            setup.agent_config,
            user_id,
            request.llm_model,
            is_byok,
            mode=agent_mode,
            reasoning_effort=getattr(request, "reasoning_effort", None),
            fast_mode=getattr(request, "fast_mode", None),
            thread_id=thread_id,
            enabled_subagents=request.subagents_enabled,
        )

        # is_byok is True only when the stamped credential_source confirms the user
        # supplied their own key (OAUTH or BYOK), not merely that a client object exists.
        is_byok = config.credential_source in (CredentialSource.OAUTH, CredentialSource.BYOK)

        # Credit check: always enforce.
        # - Platform-served (is_byok=False): block when daily limit reached.
        # - BYOK/OAuth (is_byok=True): block only on negative balance (outstanding
        #   debt from past platform usage, e.g. fallback routing).
        await enforce_credit_limit(user_id, byok=is_byok)

        # I6: Redis down at START = 503 before any durable row. Ordered after
        # the auth/authz/credit gates (their statuses are more meaningful and
        # leak nothing about infra), before anything durable happens. Without
        # the transport the run's first buffered event would kill it as
        # failed(transport_lost) anyway — refuse cheaply instead.
        await _assert_stream_transport_ready()

        # Strip internal-only fields from non-internal requests (prevent
        # spoofing system messages / forging report-back watch cleanup).
        if not is_internal:
            internal_overrides = {}
            if request.query_type:
                internal_overrides["query_type"] = None
            if request.report_back_ptc_thread_id:
                internal_overrides["report_back_ptc_thread_id"] = None
            if request.origin_flash_thread_id:
                internal_overrides["origin_flash_thread_id"] = None
            if request.origin_dispatch_gen:
                internal_overrides["origin_dispatch_gen"] = None
            if request.disable_subagents:
                internal_overrides["disable_subagents"] = None
            if internal_overrides:
                request = request.model_copy(update=internal_overrides)
    except BaseException:
        await scope.release_slot()
        raise

    # Resolve model name for observability labels (bounded by models.json keys).
    _llm = getattr(config, "llm", None)
    _model = (getattr(_llm, "flash", None) if agent_mode == "flash" else getattr(_llm, "name", None)) or ""

    # Content-Location header advertises the reconnect URL for this run.
    # Mirrors langgraph_sdk's protocol so reconnects target the exact run.
    sse_headers_with_loc = {
        **SSE_HEADERS,
        "Content-Location": f"/api/v1/threads/{thread_id}/messages/stream?run_id={run_id}",
    }

    # Route to appropriate streaming function based on agent mode
    if agent_mode == "flash":
        is_flash_dispatch = (
            is_internal
            and raw_request
            and raw_request.headers.get("X-Dispatch") == "background"
        )
        flash_gen = astream_flash_workflow(
            request=request,
            thread_id=thread_id,
            run_id=run_id,
            user_input=user_input,
            user_id=user_id,
            is_byok=is_byok,
            config=config,
            dispatched=is_flash_dispatch,
            flash_workspace=flash_workspace,
        )

        if is_flash_dispatch:
            from src.server.database.runs.lifecycle import DuplicateRequestError
            from src.server.handlers.chat.request_prep import DISPATCH_STARTED_MARKER
            # Report-back idempotency: a lost-response retry of the drainer's
            # POST must NOT start a second summary run. The claim CM SET-NXs
            # the per-(flash, ptc) run pointer (atomic on its own — no outer
            # lock needed); a prior admission's incumbent run_id is returned
            # instead, and a non-consummated exit (e.g. a priming failure
            # below) releases the claim. No-op unless
            # report_back_ptc_thread_id is set.
            rb_ptc = request.report_back_ptc_thread_id
            rb_cache = None
            if rb_ptc:
                from src.utils.cache.redis_cache import get_cache_client
                rb_cache = get_cache_client()
            from src.server.services.report_back.flash import pointer
            async with pointer.claim(
                rb_cache, thread_id, rb_ptc, run_id,
                request.origin_dispatch_gen,
                request.request_key,
            ) as rb_claim:
                if rb_claim.incumbent is not None:
                    await scope.release_slot()
                    logger.info(
                        f"[FLASH_DISPATCH] Idempotent report-back: returning "
                        f"in-flight run {rb_claim.incumbent} for ptc={rb_ptc} on "
                        f"flash thread {thread_id} (no second run)"
                    )
                    return JSONResponse({
                        "status": "dispatched",
                        "thread_id": thread_id,
                        "run_id": rb_claim.incumbent,
                    })
                if rb_claim.pair_gone:
                    await scope.release_slot()
                    logger.warning(
                        f"[FLASH_DISPATCH] Report-back pair for ptc={rb_ptc} on "
                        f"flash thread {thread_id} was already settled; refusing "
                        "to schedule an orphan summary"
                    )
                    # 410 is deliberately outside the executor's retry set: the
                    # job drops (acks) instead of re-POSTing a summary whose
                    # pair a resolution or terminal clear has already settled.
                    raise HTTPException(
                        status_code=410,
                        detail=(
                            "Report-back pair already resolved; summary not "
                            "scheduled."
                        ),
                    )
                if rb_claim.in_flight:
                    await scope.release_slot()
                    logger.info(
                        f"[FLASH_DISPATCH] Report-back for ptc={rb_ptc} on "
                        f"flash thread {thread_id} has a rowless incumbent "
                        "inside its priming lease; deferring (503 retriable)"
                    )
                    # 503 is inside the executor's always-retried set: the
                    # retry re-probes once the prior admission either commits
                    # its START (adopt) or its pointer goes stale (takeover).
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "A prior admission of this report-back may still "
                            "be starting; retry shortly."
                        ),
                    )
                # Durable receipt (v4 2.4c): drive the generator through its
                # own admission (per-thread lock, dedup, wait_or_steer, START
                # txn) to the post-START marker. The dispatched 200 below is
                # returned only once the in_progress row is committed — a
                # receipt whose run then vanishes rowlessly can no longer
                # happen. Pre-START failures surface here raw as HTTP errors.
                try:
                    first = await anext(flash_gen)
                except DuplicateRequestError as dup:
                    await scope.release_slot()
                    existing = str(dup.existing_run["conversation_response_id"])
                    logger.info(
                        f"[FLASH_DISPATCH] Retransmit adopted existing run "
                        f"{existing} for thread {thread_id}"
                    )
                    return JSONResponse({
                        "status": "dispatched",
                        "thread_id": thread_id,
                        "run_id": existing,
                    })
                except HTTPException:
                    await scope.release_slot()
                    raise
                except Exception as e:
                    await scope.release_slot()
                    raise HTTPException(
                        status_code=503,
                        detail=f"Dispatch could not start durably: {e}",
                    )
                if first != DISPATCH_STARTED_MARKER:
                    await flash_gen.aclose()
                    await scope.release_slot()
                    raise HTTPException(
                        status_code=500,
                        detail="Dispatch priming yielded an unexpected event.",
                    )
                rb_claim.consummate()
            _track_task(asyncio.create_task(
                observe_background_chat_turn(
                    _consume_background_gen(
                        flash_gen,
                        "FLASH_DISPATCH",
                        thread_id,
                        run_id,
                    ),
                    mode="flash",
                    model=_model,
                    user_id=user_id,
                    workspace_id=workspace_id,
                    thread_id=thread_id,
                ),
                name=f"flash-dispatch-{thread_id}-{run_id[:8]}",
            ))
            logger.info(
                f"[FLASH_DISPATCH] Started background workflow: "
                f"thread_id={thread_id} run_id={run_id}"
            )
            return JSONResponse({
                "status": "dispatched",
                "thread_id": thread_id,
                "run_id": run_id,
            })

        return StreamingResponse(
            observe_chat_stream(
                flash_gen,
                mode="flash",
                model=_model,
                user_id=user_id,
                workspace_id=workspace_id,
                thread_id=thread_id,
            ),
            media_type="text/event-stream",
            headers=sse_headers_with_loc,
        )

    is_ptc_dispatch = (
        is_internal
        and raw_request
        and raw_request.headers.get("X-Dispatch") == "background"
    )
    ptc_gen = astream_ptc_workflow(
        request=request,
        thread_id=thread_id,
        run_id=run_id,
        user_input=user_input,
        user_id=user_id,
        workspace_id=workspace_id,
        is_byok=is_byok,
        config=config,
        dispatched=is_ptc_dispatch,
    )

    if is_ptc_dispatch:
        from src.server.database.runs.lifecycle import DuplicateRequestError
        from src.server.services.report_back.flash import reserve
        from src.server.handlers.chat.request_prep import DISPATCH_STARTED_MARKER

        # Phantom-refusal gate BEFORE the START txn: it atomically refuses
        # generations the orphan resolver already receipted (their watch
        # state is gone; admitting would run a turn whose report-back
        # silently drops) and stamps pre-START intent on the origin, so it
        # must precede priming — a receipted gen must never reach START.
        # Admission truth is the ledger row the priming below commits (its
        # metadata carries the generation); the stamp only covers the
        # pre-START window for the resolver. Gen-less dispatches skip the
        # gate — there is nothing to receipt against.
        if request.origin_dispatch_gen:
            admitted = await reserve.admit_dispatch_gen(
                thread_id, request.origin_dispatch_gen, run_id
            )
            if not admitted:
                await scope.release_slot()
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Dispatch admission was refused or the gate was "
                        "unavailable; not scheduled."
                    ),
                )

        # Durable receipt (v4 2.4c): drive the generator through its own
        # admission (per-thread lock, dedup, wait_or_steer, START txn) to
        # the post-START marker; the dispatched 200 is returned only once
        # the in_progress row is committed. Pre-START failures surface here raw
        # as HTTP errors and retract the intent stamp (CAS'd to this
        # admission's run — a same-gen sibling's stamp is never stripped);
        # a retransmit-adopt must NOT retract, the adopted run IS this
        # generation admitted.
        try:
            first = await anext(ptc_gen)
        except DuplicateRequestError as dup:
            await scope.release_slot()
            existing = str(dup.existing_run["conversation_response_id"])
            logger.info(
                f"[PTC_DISPATCH] Retransmit adopted existing run {existing} "
                f"for thread {thread_id}"
            )
            return JSONResponse({
                "status": "dispatched",
                "thread_id": thread_id,
                "run_id": existing,
                "workspace_id": workspace_id,
            })
        except HTTPException:
            await scope.release_slot()
            if request.origin_dispatch_gen:
                await reserve.retract_dispatch_gen(
                    thread_id, request.origin_dispatch_gen, run_id
                )
            raise
        except Exception as e:
            await scope.release_slot()
            if request.origin_dispatch_gen:
                await reserve.retract_dispatch_gen(
                    thread_id, request.origin_dispatch_gen, run_id
                )
            raise HTTPException(
                status_code=503,
                detail=f"Dispatch could not start durably: {e}",
            )
        if first != DISPATCH_STARTED_MARKER:
            await ptc_gen.aclose()
            await scope.release_slot()
            raise HTTPException(
                status_code=500,
                detail="Dispatch priming yielded an unexpected event.",
            )

        _track_task(asyncio.create_task(
            observe_background_chat_turn(
                _consume_background_gen(
                    ptc_gen, "PTC_DISPATCH", thread_id, run_id,
                ),
                mode="ptc",
                model=_model,
                user_id=user_id,
                workspace_id=workspace_id,
                thread_id=thread_id,
            ),
            name=f"ptc-dispatch-{thread_id}-{run_id[:8]}",
        ))
        logger.info(
            f"[PTC_DISPATCH] Started background workflow: "
            f"thread_id={thread_id} run_id={run_id} workspace_id={workspace_id}"
        )
        return JSONResponse({
            "status": "dispatched",
            "thread_id": thread_id,
            "run_id": run_id,
            "workspace_id": workspace_id,
        })

    return StreamingResponse(
        observe_chat_stream(
            ptc_gen,
            mode="ptc",
            model=_model,
            user_id=user_id,
            workspace_id=workspace_id,
            thread_id=thread_id,
        ),
        media_type="text/event-stream",
        headers=sse_headers_with_loc,
    )


@router.get("/{thread_id}/messages/stream")
async def reconnect_to_stream(
    thread_id: str,
    x_user_id: CurrentUserId,
    last_event_id: Optional[int] = Query(None, description="Last received event ID"),
    last_event_id_header: Optional[str] = Header(None, alias="Last-Event-ID"),
    run_id: Optional[str] = Query(None, description="Specific run to reconnect to"),
):
    """Reconnect to a running or completed workflow's SSE stream.

    ``run_id`` targets a specific turn. If omitted, falls back to the
    latest run on the thread (matches the single-turn happy path).
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.chat import reconnect_to_workflow_stream
    from src.server.handlers.chat.reconnect_admission import classify_reconnect

    safe_add(sse_reconnects, 1)

    if last_event_id is None and last_event_id_header is not None:
        try:
            last_event_id = int(last_event_id_header)
        except ValueError:
            pass

    # 1.5d: admission decided here, before headers commit — a 404/409/410
    # raised inside the generator would arrive after HTTP 200.
    effective_run_id = await classify_reconnect(thread_id, run_id)

    async def stream_reconnection():
        try:
            async for event in reconnect_to_workflow_stream(
                thread_id, effective_run_id, last_event_id
            ):
                yield event
        except Exception as e:
            logger.error(f"[PTC_RECONNECT] Error: {e}", exc_info=True)
            yield f'event: error\ndata: {{"error": "Reconnection failed: {str(e)}"}}\n\n'

    return StreamingResponse(
        stream_reconnection(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/{thread_id}/watch")
async def watch_thread(thread_id: str, x_user_id: CurrentUserId):
    """Watch for new workflow activity on a thread via SSE + Redis pub/sub.

    Opens a lightweight SSE connection that emits a ``workflow_started`` event
    each time a new workflow begins on this thread (e.g. a flash report-back
    after a PTC completes). The connection stays open across the whole chain so
    N concurrent PTCs' report-backs are all delivered on one subscription; the
    client reconnects via ``/messages/stream`` per event and closes the watch
    when ``/status`` reports no more pending report-backs.

    Sends keepalive pings every 45 seconds.  Auto-closes after 30 minutes
    to prevent leaked connections from abandoned browser tabs.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)

    from src.utils.cache.redis_cache import get_cache_client
    from src.server.services.report_back.flash.core import watch_wakes

    async def watch_generator():
        cache = get_cache_client()
        async for frame in watch_wakes(cache, thread_id):
            yield frame

    return StreamingResponse(
        watch_generator(),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


@router.get("/{thread_id}/messages/replay")
async def replay_thread_messages(
    thread_id: str,
    x_user_id: CurrentUserId,
    source: str = Query(
        "auto",
        pattern="^(auto|checkpoint|sse)$",
        description=(
            "Replay source: 'checkpoint' projects the transcript from LangGraph "
            "checkpoints, 'sse' replays persisted sse_events, 'auto' prefers "
            "checkpoint and falls back to sse when coverage is incomplete."
        ),
    ),
    limit: int | None = Query(
        None,
        ge=1,
        description=(
            "Windowed replay: build only the most recent N turns from "
            "checkpoints (bounds initial-load latency to the window). "
            "Checkpoint-sourced only; ignored for source='sse'."
        ),
    ),
):
    """Replay a thread as SSE.

    Stream includes:
    - user_message: emitted once per turn_index (query content)
    - message_chunk/tool_* events: projected from checkpoints or emitted from
      stored sse_events, per ``source``
    - replay_done: terminal sentinel
    """
    try:
        owner_id, thread, queries, responses, usages, provenance = (
            await get_replay_thread_data(thread_id)
        )

        # Preserve existing 404/403 semantics from require_thread_owner
        if owner_id is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        if owner_id != x_user_id:
            raise HTTPException(status_code=403, detail="Forbidden")
        if not thread:
            raise HTTPException(
                status_code=404, detail=f"Thread not found: {thread_id}"
            )

        responses_by_turn = {
            r.get("turn_index"): r for r in responses if isinstance(r, dict)
        }

        from src.server.services.history.replay import (
            CheckpointReplayUnavailable,
            build_checkpoint_replay_items,
            build_sse_replay_items,
        )

        checkpoint_items: list[dict] | None = None
        if source in ("auto", "checkpoint"):
            try:
                if thread.get("latest_checkpoint_id") is None:
                    # The commit pointer (stamped at turn persist) is the only
                    # tip checkpoint replay may read — without it the reader
                    # would walk the newest checkpoint, which mid-run is
                    # uncommitted partial state.
                    raise CheckpointReplayUnavailable(
                        "thread has no committed checkpoint pointer"
                    )
                checkpoint_items = await build_checkpoint_replay_items(
                    thread_id,
                    queries,
                    responses_by_turn,
                    branch_tip_checkpoint_id=thread.get("latest_checkpoint_id"),
                    last_n_turns=limit,
                    usages=usages,
                    provenance=provenance,
                )
            except CheckpointReplayUnavailable as e:
                if source == "checkpoint":
                    raise HTTPException(
                        status_code=409,
                        detail=f"Checkpoint replay unavailable: {e}",
                    )
                logger.info(
                    f"[REPLAY] Checkpoint replay unavailable for {thread_id}, "
                    f"falling back to sse: {e}"
                )
            except HTTPException:
                raise
            except Exception as e:
                if source == "checkpoint":
                    raise
                logger.warning(
                    f"[REPLAY] Checkpoint replay failed for {thread_id}, "
                    f"falling back to sse: {e}",
                    exc_info=True,
                )

        replay_items = (
            checkpoint_items
            if checkpoint_items is not None
            else build_sse_replay_items(thread_id, queries, responses_by_turn)
        )

        # After source selection and outside the projection cache: task
        # artifacts replay their spawn-time payload, so the card's status is
        # stamped here from current liveness (see history/task_status.py).
        from src.server.services.history.task_status import (
            stamp_replay_task_status,
        )

        await stamp_replay_task_status(thread_id, replay_items)

        async def event_generator():
            seq = 0
            for item in replay_items:
                seq += 1
                yield (
                    f"id: {seq}\n"
                    f"event: {item['event']}\n"
                    f"data: {json.dumps(item['data'], ensure_ascii=False, default=str)}\n\n"
                )

            # Cursors for the runs replay could not project — an in-flight run
            # belongs to its stream, so the snapshot hands the client where to
            # resume instead of its content. Additive: a snapshot outage (None)
            # just omits the frame, and v1 clients ignore the unknown event.
            from src.server.services.history.snapshot import (
                build_thread_snapshot,
            )

            snapshot = await build_thread_snapshot(thread_id)
            if snapshot is not None:
                seq += 1
                yield (
                    f"id: {seq}\n"
                    f"event: snapshot\n"
                    f"data: {json.dumps(snapshot, ensure_ascii=False, default=str)}\n\n"
                )

            seq += 1
            yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps({'thread_id': thread_id}, default=str)}\n\n"

        resolved_source = "checkpoint" if checkpoint_items is not None else "sse"
        return StreamingResponse(
            observe_replay_stream(event_generator(), source="private"),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Replay-Source": resolved_source,
            },
        )

    except PoolTimeout:
        raise HTTPException(
            status_code=503,
            detail="Database connection pool busy, please retry",
            headers={"Retry-After": "2"},
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error replaying thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to replay thread: {str(e)}"
        )


# =============================================================================
# THREAD CONTROL (was "workflow")
# =============================================================================


@router.get("/{thread_id}/status")
async def get_thread_status(
    thread_id: str,
    x_user_id: CurrentUserId,
    fields: Optional[str] = Query(
        None,
        description="'report_back' returns only the report-back slice (cheap path)",
    ),
):
    """Get current workflow execution status for a thread.

    ``fields=report_back`` returns just the pending-report-back slice, skipping
    the checkpoint / background-task / share reads — used by the frontend's
    event-driven catch-up pulls so a reconnect doesn't pay for the full status.
    """
    # One query authorizes the caller AND yields is_shared, so the full-status
    # path below doesn't re-fetch the thread row.
    from src.server.database.conversation import get_thread_auth_meta

    meta = await get_thread_auth_meta(thread_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    if meta["user_id"] != x_user_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if fields == "report_back":
        from src.server.services.report_back.flash.status import read_report_back_slice

        return await read_report_back_slice(
            thread_id, msg_type=meta.get("msg_type") or ""
        )

    from src.server.services.thread_status import read_thread_runtime_status

    return await read_thread_runtime_status(
        thread_id,
        is_shared=bool(meta["is_shared"]),
        msg_type=meta.get("msg_type"),
    )


# Upper bound on ids per liveness request — one MGET stays cheap. Ids past the
# cap are dropped for that request and stay unresolved on the client (there is
# no per-card fallback); >100 concurrently-unresolved cards would need the
# frontend to chunk requests.
_MAX_LIVENESS_IDS = 100


@router.get("/dispatches/liveness")
async def get_dispatches_liveness(
    x_user_id: CurrentUserId,
    ids: str = Query(
        ...,
        description="Comma-separated thread ids to read liveness for (one MGET).",
    ),
):
    """Batched, client-keyed dispatch liveness — N cards in one round-trip.

    Reads the durable ledger (latest attempt per thread, ownership-filtered
    in the same query — no IDOR, no per-thread reads) instead of the tracker
    blobs (v4 2.4): the answer is identical on every worker, terminal rows
    never expire like the ~1h blob TTL did, and the in-process zombie-heal
    crosscheck is obsolete — the recovery scanner converges orphaned
    in_progress rows. Threads with no attempt row yet (a dispatch still
    pre-START) are silently omitted so the card keeps polling as 'starting'.
    """
    seen: set[str] = set()
    deduped: list[str] = []
    for raw in ids.split(","):
        tid = raw.strip()
        if tid and tid not in seen:
            seen.add(tid)
            deduped.append(tid)

    if len(deduped) > _MAX_LIVENESS_IDS:
        logger.warning(
            f"[LIVENESS] {len(deduped)} ids requested by {x_user_id}; capping at "
            f"{_MAX_LIVENESS_IDS} (remainder unresolved this request)"
        )
        deduped = deduped[:_MAX_LIVENESS_IDS]

    if not deduped:
        return {"liveness": []}

    from src.server.database.runs import lifecycle as tl_db
    from src.server.contracts.status import to_public

    rows = await tl_db.get_latest_attempts_for_threads(deduped, x_user_id)

    liveness = []
    for tid, row in rows.items():
        status = to_public(
            row["status"], cancel_requested_at=row.get("cancel_requested_at")
        )
        live = status in ("running", "stopping")
        liveness.append(
            {
                "thread_id": tid,
                "status": status,
                "run_id": str(row["conversation_response_id"]) if live else None,
                "can_reconnect": live,
            }
        )

    return {"liveness": liveness}


@router.post("/{thread_id}/cancel", status_code=200)
async def cancel_thread(
    thread_id: str,
    x_user_id: CurrentUserId,
    run_id: Optional[str] = Query(None),
):
    """Cancel a running workflow for this thread.

    ``run_id`` targets a specific run so a retried stop can't cancel a newer
    turn started after the stopped one ended (defaults to latest active run).
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.services.cancel_dispatch import cancel_workflow

    return await cancel_workflow(thread_id, run_id)


@router.post("/{thread_id}/summarize", status_code=200)
async def summarize_thread(
    thread_id: str,
    x_user_id: CurrentUserId,
    keep_messages: int = Query(
        default=5, ge=1, le=20, description="Number of recent messages to preserve"
    ),
):
    """Manually trigger context compaction for a thread.

    Endpoint path ``/summarize`` and function name preserved for REST contract
    compatibility — clients may call the older URL.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.thread_maintenance import trigger_compaction

    return await trigger_compaction(thread_id, keep_messages, user_id=x_user_id)


@router.post("/{thread_id}/offload", status_code=200)
async def offload_thread(thread_id: str, x_user_id: CurrentUserId):
    """Truncate large tool arguments and offload originals to sandbox (Tier 1 only)."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.thread_maintenance import trigger_offload

    return await trigger_offload(thread_id)


@router.get("/{thread_id}/turns")
async def get_thread_turns(thread_id: str, x_user_id: CurrentUserId):
    """
    Get turn-boundary checkpoint IDs for edit/regenerate/retry operations.

    Returns per-turn checkpoint IDs:
    - edit_checkpoint_id: fork BEFORE the user message (for editing)
    - regenerate_checkpoint_id: fork AFTER user message, BEFORE AI response (for regenerating)
    - retry_checkpoint_id: most recent checkpoint (for retrying after failure)
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.checkpoint_handler import (
        get_thread_turns as _get_thread_turns,
    )
    from src.server.database.conversation import get_thread_checkpoint_id

    branch_tip = await get_thread_checkpoint_id(thread_id)
    return await _get_thread_turns(thread_id, branch_tip_checkpoint_id=branch_tip)


@router.post("/{thread_id}/retry")
async def retry_thread(
    thread_id: str,
    auth: ChatRateLimited,
    body: Optional[RetryRequest] = None,
):
    """
    Retry a failed run as a new attempt on the same turn (v4 attempt chain).

    Validates the target is the thread's LATEST attempt and terminally
    retryable (status=error), then starts attempt N+1 with
    ``retry_of_run_id`` chaining — no truncation, the failed attempt stays
    archived. Graph-wise the retry resumes from the last checkpoint.
    Returns an SSE stream.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.handlers.chat.admission_gate import admission_conflict_detail
    from src.server.handlers.checkpoint_handler import get_retry_checkpoint
    from src.server.services.runs.admission import RunScope

    scope = RunScope(user_id=auth.user_id, burst_slot_id=auth.burst_slot_id)

    try:
        await auth_api.require_thread_owner(thread_id, auth.user_id)

        # I6: refuse before any durable row when the event transport is down.
        await _assert_stream_transport_ready()

        # Retransmit probe FIRST: a duplicate /retry must resolve to its
        # existing attempt, not trip the latest-attempt validation below
        # (which would mislabel it stale_retry or running).
        if body and body.request_key:
            await _reject_duplicate_request(body.request_key, auth.user_id)

        latest = await tl_db.get_latest_attempt(thread_id)
        if latest is None:
            raise HTTPException(
                status_code=404, detail=f"Thread {thread_id} has no runs to retry"
            )
        latest_run_id = str(latest["conversation_response_id"])
        if body and body.run_id and body.run_id != latest_run_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "stale_retry",
                    "message": "The requested run is no longer the latest attempt.",
                    "latest_run_id": latest_run_id,
                    "latest_status": latest["status"],
                },
            )
        if latest["status"] == "in_progress":
            raise HTTPException(
                status_code=409, detail=admission_conflict_detail("running")
            )
        if latest["status"] != "error":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "not_retryable",
                    "message": f"Latest run is {latest['status']}; only failed "
                    "runs can be retried.",
                    "latest_run_id": latest_run_id,
                    "latest_status": latest["status"],
                },
            )

        explicit_checkpoint_id = body.checkpoint_id if body else None
        retry_checkpoint_id = await get_retry_checkpoint(
            thread_id, explicit_checkpoint_id
        )

        # Resolve workspace_id from body or from the thread record
        workspace_id = body.workspace_id if body and body.workspace_id else None
        if not workspace_id:
            thread_record = await get_thread_by_id(thread_id)
            if not thread_record:
                raise HTTPException(
                    status_code=404, detail=f"Thread {thread_id} not found"
                )
            workspace_id = str(thread_record.get("workspace_id", ""))
    except BaseException:
        # ChatRateLimited acquired a burst slot at the dependency; every
        # early exit above bypasses _handle_send_message, whose own guard
        # normally releases it — without this, repeated stale retries
        # exhaust the user's burst allowance until TTL expiry.
        await scope.release_slot()
        raise

    # Delegate to the message flow as a checkpoint replay carrying the
    # attempt chain (no fork_from_turn: nothing is truncated). Retry
    # provenance travels as a route-internal parameter, never in the body.
    request = ChatRequest(
        workspace_id=workspace_id,
        messages=[],
        checkpoint_id=retry_checkpoint_id,
        request_key=(body.request_key if body else None),
        llm_model=(body.llm_model if body else None),
        reasoning_effort=(body.reasoning_effort if body else None),
        fast_mode=(body.fast_mode if body else None),
    )

    return await _handle_send_message(
        request, auth, thread_id, retry_of_run_id=latest_run_id
    )




@router.get("/{thread_id}/stream")
async def thread_stream_mux_endpoint(
    thread_id: str,
    x_user_id: CurrentUserId,
    cursors: Optional[str] = Query(
        None,
        description="Per-channel resume cursors: run:<run_id>#<entry_id>,…",
    ),
    contract: Optional[str] = Query(
        None, description="Stream contract version; 'v2' is the only contract"
    ),
    since_age_s: float = Query(
        0.0,
        ge=0.0,
        description=(
            "v2 only: seconds between the client's status/history snapshot "
            "(its knowledge horizon) and this connect; widens the server's "
            "settled-run catch-up window"
        ),
    ),
):
    """Multiplexed thread stream: every lane of the thread on one socket.

    Serves STREAM_CONTRACT_V2 run-scoped channels for ALL lanes, main
    included. The retired v1 contract (per-task epoch channels) 400s rather
    than silently changing shape under a stale pre-cutover client.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    if contract != "v2":
        raise HTTPException(
            status_code=400, detail="unsupported stream contract; use contract=v2"
        )
    from src.server.handlers.chat.thread_stream_mux_v2 import (
        parse_mux_cursors_v2,
        stream_thread_mux_v2,
    )

    return StreamingResponse(
        stream_thread_mux_v2(thread_id, parse_mux_cursors_v2(cursors), since_age_s),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
