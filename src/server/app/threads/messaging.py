"""Thread messaging: send/stream SSE, reconnect/replay, control, retry, mux."""

import json
from typing import Literal, Optional
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
    database_unavailable,
    service_token_matches,
)
from src.server.models.chat import ChatRequest
from src.server.models.workflow import RetryRequest
from src.server.database.conversation import (
    get_thread_auth_meta,
    get_thread_by_id,
    get_thread_owner_id,
    lookup_thread_by_external_id,
    get_replay_thread_data,
)
from src.server.contracts.run_times import run_completed_at
from src.server.contracts.status import to_public
from src.server.contracts.turn_kinds import ANNOUNCED_KEYS
from src.server.database.pool import DATABASE_UNAVAILABLE
from src.server.services.report_back.handoff.keys import ORIGIN_THREAD_META
from src.server.dependencies.usage_limits import (
    ChatRateLimited,
    quota_service_unavailable,
)

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


def _automation_configurable(request: ChatRequest) -> Optional[dict]:
    """The automation runs a report-back turn sends for, as its config carries
    them; None when the request names none. Only an internal request keeps the
    fields."""
    from src.server.services import automation_delivery

    run = automation_delivery.read_stamp(request.automation_delivery)
    if run is None:
        return None
    earlier = automation_delivery.read_stamps(request.automation_earlier_runs)
    return automation_delivery.turn_configurable(run, earlier)


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
            # report-back ordering chain, which owns any pair teardown. No direct
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
    return await _handle_send_message(
        _client_body(request, raw_request), auth, thread_id, raw_request
    )


@router.post("/{thread_id}/messages")
async def send_thread_message(
    thread_id: str, request: ChatRequest, auth: ChatRateLimited,
    raw_request: Request,
):
    """
    Send a message to an existing thread. Returns an SSE stream.
    """
    return await _handle_send_message(
        _client_body(request, raw_request), auth, thread_id, raw_request
    )


def _is_service_request(raw_request: Request | None) -> bool:
    """Whether the server sent ``raw_request`` itself: it carries the service
    token, or oss mode has no token configured to check, which trusts the
    self-dispatch."""
    from src.config.settings import HOST_MODE

    token = _get_service_token()
    sent = raw_request.headers.get("X-Service-Token", "") if raw_request else ""
    return service_token_matches(sent, token) or (HOST_MODE == "oss" and not token)


# Fields only the server's own requests set. A user's copy would post a system
# message, label their message as a report-back or a wakeup (whose turn may end
# without a reply), forge a report-back watch's cleanup, or settle a parked
# steer as its own turn; ``surface_rules`` is a channel gateway's, whose text
# rides the operator role on providers with a native channel.
_SERVER_ONLY_FIELDS = (
    "query_type",
    *ANNOUNCED_KEYS,
    "report_back_ptc_thread_id",
    ORIGIN_THREAD_META,
    "origin_dispatch_gen",
    "automation_delivery",
    "automation_earlier_runs",
    "disable_subagents",
    "surface_rules",
    "steer_job_id",
)


def _client_body(request: ChatRequest, raw_request: Request) -> ChatRequest:
    """``request`` with the server-only fields cleared, unless the server sent it.

    Cleared as the body enters, before the route settles anything: a
    regenerate of a turn that never started takes the type the turn's row
    stored, a report-back's ``system``, which a strip after it would clear.
    """
    if _is_service_request(raw_request):
        return request
    cleared = {name: None for name in _SERVER_ONLY_FIELDS if getattr(request, name)}
    return request.model_copy(update=cleared) if cleared else request


async def _reject_duplicate_request(
    thread_id: str,
    request_key: str,
    user_id: str,
    *,
    except_job_id: str | None = None,
) -> None:
    """409 if this request_key already reached a run, as its own turn or as
    a steer into another: a retransmit.

    Route-level twin of the in-generator ``dedup_retransmit_or_raise``:
    classifying here answers with a clean HTTP 409 before a thread is
    minted, a fork truncates the rows holding the key, or a steering path
    consumes the duplicate. Discloses the existing run's identity only to
    the owning user. ``except_job_id`` is the parked steer a server-posted
    turn carries, which is that turn's own message, not a copy of it.
    """
    from src.server.services.runs.admission import run_named_by_key

    keyed = await run_named_by_key(
        thread_id, request_key, except_job_id=except_job_id
    )
    if keyed is None:
        return
    existing = keyed.run
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
        # A steer's run is another message's turn, not one to retry or adopt.
        if keyed.steered:
            detail["steered"] = True
    raise HTTPException(status_code=409, detail=detail)


async def _settle_against_branch(
    thread_id: str, request: ChatRequest, user_id: str
) -> ChatRequest:
    """A fork or HITL answer checked against the thread's branch.

    A retransmit whose first copy started after the duplicate probe finds
    that copy's run there and reads it as a change its view never saw. Its
    key names the run, so it gets the duplicate answer, not a refusal.
    """
    from src.server.handlers.checkpoint_handler import (
        settle_fork_request,
        settle_resume_request,
    )

    try:
        if request.fork_from_turn is not None and request.checkpoint_id:
            return await settle_fork_request(thread_id, request)
        if request.hitl_response:
            return await settle_resume_request(thread_id, request)
        return request
    except HTTPException as refused:
        if refused.status_code == 409 and request.request_key:
            await _reject_duplicate_request(
                thread_id,
                request.request_key,
                user_id,
                except_job_id=request.steer_job_id,
            )
        raise


async def _handle_send_message(
    request: ChatRequest, auth: ChatRateLimited, thread_id: str,
    raw_request: Request | None = None,
    *,
    retry_of_run_id: str | None = None,
):
    """Shared logic for both POST /threads/messages and POST /threads/{id}/messages.

    Those routes pass a body already cleared of the server-only fields
    (:func:`_client_body`); /retry builds its request from the stored rows.

    ``retry_of_run_id`` is retry provenance and route-internal: only the
    /retry route passes it. Whatever the public body carried is overwritten
    — a forged value could chain a new attempt onto an arbitrary failed run.
    """
    from src.server.handlers.chat import astream_ptc_workflow
    from src.server.handlers.chat.image_intake import inline_image_urls
    from src.server.handlers.chat.error_handling import end_after_failure_frame
    from src.server.services.turn_runtime import requested_workspace, turn_route

    # Canonical run_id generation site. Each POST gets a fresh UUID that
    # flows through every downstream key: BTM ``(tid, rid)``, persistence
    # service, ``workflow:stream:{tid}:{rid}``, LangGraph ``config["run_id"]``
    # → ``CheckpointMetadata.run_id``, and the SSE ``metadata`` event the
    # frontend sees as the first event of the stream. 1:1 with
    # ``conversation_response_id``.
    run_id = str(uuid4())

    user_id = auth.user_id
    is_byok = auth.is_byok
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
            await _reject_duplicate_request(
                thread_id,
                request.request_key,
                user_id,
                except_job_id=request.steer_job_id,
            )

        # 403 guard: require BYOK, OAuth, or platform access (tier >= 0).
        # All flags are pre-checked by enforce_chat_limit — no DB calls here.
        # A tier nobody could read is an outage, not a missing provider: telling
        # a paying user to set one up sends them to the setup wizard for it.
        from src.config.settings import HOST_MODE
        needs_plan = HOST_MODE == "platform" and not auth.is_byok and not auth.has_oauth
        if needs_plan and auth.access_tier is None:
            raise quota_service_unavailable()
        if needs_plan and auth.access_tier < 0:
            # A flagged subscription does not count as a provider, but it is
            # the user's, and reconnecting it is the fix rather than setup.
            from src.server.database.oauth_tokens import flagged_oauth_connection
            from src.server.services.llm.availability import raise_oauth_reauth_required
            from src.server.services.oauth_providers import SUBSCRIPTIONS

            flagged = await flagged_oauth_connection(user_id)
            subscription = SUBSCRIPTIONS.get(flagged["provider"]) if flagged else None
            if subscription is not None:
                raise_oauth_reauth_required(subscription, None, flagged["last_error_code"])
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
        # token is enforced in every mode. Past this check a dispatch is
        # always the server's own, so ``is_dispatch`` alone says so below.
        _req_token = (raw_request.headers.get("X-Service-Token", "") if raw_request else "")
        _svc_token = _get_service_token()
        is_internal = _is_service_request(raw_request)
        is_dispatch = bool(
            raw_request and raw_request.headers.get("X-Dispatch") == "background"
        )
        if is_dispatch and not is_internal:
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
        # dispatch sets X-User-Id to the owner, so it passes. The same row also
        # gives the workspace when the request names none, and the thread's model.
        thread_meta = await get_thread_auth_meta(thread_id) if thread_id else None
        owner_id = thread_meta["user_id"] if thread_meta else None
        if owner_id is not None and owner_id != user_id:
            raise HTTPException(status_code=403, detail="Forbidden")

        # A fork deletes rows from the turn it replaces, and that turn is the
        # one its checkpoint forks, not the number the client counted. Every
        # later reader (the truncation, the regenerate's turn, the prior-turn
        # read) takes it from the request, so it is settled here, once, with
        # the branch tip it was read off for START to hold it to.
        request = await _settle_against_branch(thread_id, request, user_id)

        # Resolve workspace_id from thread if not provided
        if not workspace_id and thread_meta:
            workspace_id = str(thread_meta["workspace_id"])
            logger.debug(
                f"[CHAT] Resolved workspace_id={workspace_id} from thread_id={thread_id}"
            )

        # Validate that agent_config is initialized
        if not hasattr(setup, "agent_config") or setup.agent_config is None:
            raise HTTPException(
                status_code=503,
                detail="PTC Agent not initialized. Check server startup logs.",
            )

        # A turn naming Home's id, or any turn on a thread in Home, runs in
        # Home. The workspace guard comes first: it pairs with the thread
        # guard above. Home is bound once the admission gates below pass.
        workspace = await requested_workspace(user_id, workspace_id)
        route = turn_route(user_id, workspace)
        workspace_id = route.workspace_id

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
            f"[PTC_CHAT] New request: "
            f"workspace_id={workspace_id} thread_id={thread_id} user_id={user_id} "
            f"role={route.role}"
        )

        # Resolve LLM config eagerly — credit check must happen before SSE stream starts
        from src.server.services.llm import thread_model
        from src.server.services.llm.claude_code import require_claude_code_continues
        from src.server.services.thread_agent import thread_resolver
        from src.server.dependencies.usage_limits import enforce_credit_limit
        from ptc_agent.config.agent import CredentialSource

        # A turn naming no model runs the thread's own, the last one a client
        # named for it. Read here rather than in the resolver so automation
        # turns, which never pass through this route, keep their own model.
        requested_model = request.llm_model or None
        held_model = thread_meta.get("llm_model") if thread_meta else None
        named_model = (
            thread_model.NamedModel(requested_model, seen=held_model)
            if requested_model
            else None
        )

        # Likewise a turn naming no effort or fast mode runs the thread's own
        # (``thread_resolver`` says when the row decides).
        resolve = thread_resolver(
            user_id,
            thread_id,
            route,
            thread=thread_meta,
            is_byok=is_byok,
            reasoning_effort=getattr(request, "reasoning_effort", None),
            fast_mode=getattr(request, "fast_mode", None),
            enabled_subagents=request.subagents_enabled,
        )

        # A report-back or a channel message has nobody at a client to
        # reconnect an account, and a refusal would drop it. A person's send
        # names no model while its client has not read the thread yet, and
        # gets the refusal rather than an answer from a model it never chose.
        from_service = service_token_matches(_req_token, _svc_token) or is_dispatch
        config = await thread_model.resolve_turn_config(
            user_id,
            thread_id,
            named=requested_model,
            held=held_model,
            fall_back=from_service,
            resolve=resolve,
        )
        # Before the thread keeps the model, whichever way the turn came to it:
        # named, held, or a Claude Code default. A new thread holds no reply.
        if thread_meta:
            await require_claude_code_continues(
                thread_id, config.llm.name if config.llm else None, request.checkpoint_id
            )

        # is_byok is True only when the stamped credential_source confirms the user
        # supplied their own key (OAUTH or BYOK), not merely that a client object exists.
        is_byok = config.credential_source in (CredentialSource.OAUTH, CredentialSource.BYOK)

        # Credit check: always ask, whoever's key this is. Which pools apply to
        # an own-key turn is the quota service's call, not ours, and so is what
        # the surface the turn came from means for it.
        await enforce_credit_limit(user_id, byok=is_byok, surface=request.platform)

        # I6: Redis down at START = 503 before any durable row. Ordered after
        # the auth/authz/credit gates (their statuses are more meaningful and
        # leak nothing about infra), before anything durable happens. Without
        # the transport the run's first buffered event would kill it as
        # failed(transport_lost) anyway — refuse cheaply instead.
        await _assert_stream_transport_ready()

        request = await inline_image_urls(request)

        await route.bind(user_id, workspace)

    except BaseException:
        await scope.release_slot()
        raise

    # Resolve model name for observability labels (bounded by models.json keys).
    _model = getattr(getattr(config, "llm", None), "name", None) or ""

    # Content-Location header advertises the reconnect URL for this run.
    # Mirrors langgraph_sdk's protocol so reconnects target the exact run.
    sse_headers_with_loc = {
        **SSE_HEADERS,
        "Content-Location": f"/api/v1/threads/{thread_id}/messages/stream?run_id={run_id}",
    }

    gen = astream_ptc_workflow(
        request=request,
        thread_id=thread_id,
        run_id=run_id,
        user_input=user_input,
        user_id=user_id,
        route=route,
        is_byok=is_byok,
        config=config,
        dispatched=is_dispatch,
        from_server=is_internal,
        named_model=named_model,
        # A report-back summary owes its pair's watch clear, so its finalize
        # must see the pair.
        run_metadata=(
            {
                "report_back_ptc_thread_id": request.report_back_ptc_thread_id,
                "origin_dispatch_gen": request.origin_dispatch_gen,
            }
            if request.report_back_ptc_thread_id
            else None
        ),
        extra_configurable=_automation_configurable(request),
    )

    if not is_dispatch:
        return StreamingResponse(
            end_after_failure_frame(
                observe_chat_stream(
                    gen,
                    model=_model,
                    user_id=user_id,
                    workspace_id=workspace_id,
                    thread_id=thread_id,
                )
            ),
            media_type="text/event-stream",
            headers=sse_headers_with_loc,
        )

    return await _prime_dispatch(
        gen,
        scope,
        request,
        thread_id=thread_id,
        run_id=run_id,
        user_id=user_id,
        workspace_id=workspace_id,
        model=_model,
    )


async def _prime_dispatch(
    gen,
    scope,
    request: ChatRequest,
    *,
    thread_id: str,
    run_id: str,
    user_id: str,
    workspace_id: str,
    model: str,
) -> JSONResponse:
    """Drive a background dispatch's turn to its START, answer with the run
    and the workspace the turn resolved to, then drain ``gen`` in the
    background.

    Every exit that does not hand ``gen`` to that drain releases ``scope``'s
    slot.
    """
    from src.server.database.runs.lifecycle import DuplicateRequestError
    from src.server.handlers.chat.request_prep import (
        DISPATCH_STARTED_MARKER,
        HandOffSteered,
    )
    from src.server.services.report_back.handoff import pointer, reserve

    # Report-back idempotency: a lost-response retry of the drainer's POST
    # must NOT start a second summary run. The claim CM SET-NXs the
    # per-(dispatcher, ptc) run pointer (atomic on its own — no outer lock needed);
    # a prior admission's incumbent run_id is returned instead, and a
    # non-consummated exit (e.g. a priming failure below) releases the claim.
    # No-op unless report_back_ptc_thread_id is set.
    rb_ptc = request.report_back_ptc_thread_id
    rb_cache = None
    if rb_ptc:
        from src.utils.cache.redis_cache import get_cache_client
        rb_cache = get_cache_client()
    # A dispatch generation is gated on the PTC thread it starts;
    # a report-back summary carries its pair's generation for the claim above.
    gated_gen = request.origin_dispatch_gen if not rb_ptc else None

    async def steered(
        target_run_id: str, steer_id: str | None = None
    ) -> JSONResponse:
        # The hand-off went into a running turn as a steer: this request's
        # run never starts, so its pre-START stamp goes. The generation stays
        # admitted on the ledger, where the steer stamped the running run.
        await scope.release_slot()
        if gated_gen:
            await reserve.retract_dispatch_gen(thread_id, gated_gen, run_id)
        logger.info(
            f"[PTC_DISPATCH] Hand-off steered into running run {target_run_id} "
            f"on thread {thread_id}"
        )
        body = {
            "status": "steered",
            "thread_id": thread_id,
            "run_id": target_run_id,
            "workspace_id": workspace_id,
        }
        if steer_id:
            # A run that ends before reading the steer leaves it to a later
            # run, which only the steer's row names.
            body["steer_id"] = steer_id
        return JSONResponse(body)

    async with pointer.claim(
        rb_cache, thread_id, rb_ptc, run_id,
        request.origin_dispatch_gen,
        request.request_key,
    ) as rb_claim:
        if rb_claim.incumbent is not None:
            await scope.release_slot()
            logger.info(
                "[PTC_DISPATCH] Idempotent report-back: returning "
                f"in-flight run {rb_claim.incumbent} for ptc={rb_ptc} on "
                f"thread {thread_id} (no second run)"
            )
            return JSONResponse({
                "status": "dispatched",
                "thread_id": thread_id,
                "run_id": rb_claim.incumbent,
                "workspace_id": workspace_id,
            })
        if rb_claim.pair_gone:
            await scope.release_slot()
            logger.warning(
                f"[PTC_DISPATCH] Report-back pair for ptc={rb_ptc} on "
                f"thread {thread_id} was already settled; refusing "
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
                f"[PTC_DISPATCH] Report-back for ptc={rb_ptc} on "
                f"thread {thread_id} has a rowless incumbent "
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

        # Phantom-refusal gate BEFORE the START txn: it atomically refuses
        # generations the orphan resolver already receipted (their watch
        # state is gone; admitting would run a turn whose report-back
        # silently drops) and stamps pre-START intent on the origin, so it
        # must precede priming — a receipted gen must never reach START.
        # Admission truth is the ledger row the priming below commits (its
        # metadata carries the generation); the stamp only covers the
        # pre-START window for the resolver. Gen-less dispatches skip the
        # gate — there is nothing to receipt against.
        if gated_gen:
            admitted = await reserve.admit_dispatch_gen(thread_id, gated_gen, run_id)
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
            first = await anext(gen)
        except DuplicateRequestError as dup:
            existing = str(dup.existing_run["conversation_response_id"])
            if dup.steered:
                return await steered(existing)
            await scope.release_slot()
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
            if gated_gen:
                await reserve.retract_dispatch_gen(thread_id, gated_gen, run_id)
            raise
        except Exception as e:
            await scope.release_slot()
            if gated_gen:
                await reserve.retract_dispatch_gen(thread_id, gated_gen, run_id)
            raise HTTPException(
                status_code=503,
                detail=f"Dispatch could not start durably: {e}",
            )
        if isinstance(first, HandOffSteered):
            await gen.aclose()
            return await steered(first.run_id, first.steer_id)
        if first != DISPATCH_STARTED_MARKER:
            await gen.aclose()
            await scope.release_slot()
            raise HTTPException(
                status_code=500,
                detail="Dispatch priming yielded an unexpected event.",
            )
        rb_claim.consummate()

    _track_task(asyncio.create_task(
        observe_background_chat_turn(
            _consume_background_gen(gen, "PTC_DISPATCH", thread_id, run_id),
            model=model,
            user_id=user_id,
            workspace_id=workspace_id,
            thread_id=thread_id,
        ),
        name=f"ptc-dispatch-{thread_id}-{run_id[:8]}",
    ))
    logger.info(
        "[PTC_DISPATCH] Started background workflow: "
        f"thread_id={thread_id} run_id={run_id} workspace_id={workspace_id}"
    )
    return JSONResponse({
        "status": "dispatched",
        "thread_id": thread_id,
        "run_id": run_id,
        "workspace_id": workspace_id,
    })


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
    each time a new workflow begins on this thread (e.g. a report-back
    after a PTC completes). The connection stays open across the whole chain so
    N concurrent PTCs' report-backs are all delivered on one subscription; the
    client reconnects via ``/messages/stream`` per event and closes the watch
    when ``/status`` reports no more pending report-backs.

    Sends keepalive pings every 45 seconds.  Auto-closes after 30 minutes
    to prevent leaked connections from abandoned browser tabs.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)

    from src.utils.cache.redis_cache import get_cache_client
    from src.server.services.report_back.handoff.core import watch_wakes

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
    source: Literal["auto", "checkpoint"] = Query(
        "auto",
        description=(
            "Replay source: 'checkpoint' projects the transcript from LangGraph "
            "checkpoints and answers 409 when they cannot cover it; 'auto' "
            "replays a turn it cannot project as its user message and closing "
            "rows, and every turn that way when the thread's coverage is "
            "incomplete."
        ),
    ),
    limit: int | None = Query(
        None,
        ge=1,
        description=(
            "Paged replay: only the most recent N turns (before "
            "``before_turn`` when given), plus a leading ``history_page`` "
            "event naming the oldest turn sent and whether older ones remain."
        ),
    ),
    before_turn: int | None = Query(
        None,
        ge=0,
        description=(
            "Paged replay: only turns older than this turn_index, the "
            "previous page's ``history_page.first_turn_index``."
        ),
    ),
):
    """Replay a thread as SSE.

    Stream includes:
    - history_page: first, only when ``limit`` or ``before_turn`` is given
    - user_message: emitted once per turn_index (query content)
    - message_chunk/tool_* events: projected from checkpoints; a turn they
      cannot cover sends only its closing rows (``source=auto``)
    - snapshot: cursors for the runs replay could not project (newest page only)
    - replay_done: terminal sentinel; on the newest page it also names the
      thread's newest run (``latest_run_id``), read before the transcript
    """
    try:
        rows = await get_replay_thread_data(thread_id)

        # Preserve existing 404/403 semantics from require_thread_owner
        if rows is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        if rows.owner_id != x_user_id:
            raise HTTPException(status_code=403, detail="Forbidden")

        from src.server.services.history.replay import (
            CheckpointReplayUnavailable,
            finish_lines,
            read_replay_page,
        )

        try:
            page, resolved_source = await read_replay_page(
                rows, source=source, before_turn=before_turn, limit=limit
            )
        except CheckpointReplayUnavailable as e:
            raise HTTPException(
                status_code=409,
                detail=f"Checkpoint replay unavailable: {e}",
            )

        # After source selection and outside the stored lines: task artifacts
        # replay their spawn-time payload, so the card's status is stamped
        # here from current liveness (see history/task_status.py).
        replay_lines = page.lines
        await finish_lines(thread_id, replay_lines)
        paged = limit is not None or before_turn is not None

        async def event_generator():
            seq = 0
            if paged:
                seq += 1
                history_page = {
                    "first_turn_index": page.first_turn_index,
                    "has_more": page.has_more,
                }
                yield (
                    f"id: {seq}\n"
                    f"event: history_page\n"
                    f"data: {json.dumps(history_page, default=str)}\n\n"
                )
            for line in replay_lines:
                seq += 1
                yield f"id: {seq}\nevent: {line.event}\ndata: {line.data_json}\n\n"

            # Cursors for the runs replay could not project — an in-flight run
            # belongs to its stream, so the snapshot hands the client where to
            # resume instead of its content. Additive: a snapshot outage (None)
            # just omits the frame, and v1 clients ignore the unknown event.
            # An older page holds no in-flight run.
            if before_turn is None:
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

            # The newest page names the thread's newest run as of this read,
            # which a fork from this view sends back as expected_latest_run_id.
            done: dict[str, object] = {"thread_id": thread_id}
            if before_turn is None:
                latest_run_id = rows.thread.get("latest_run_id")
                done["latest_run_id"] = (
                    str(latest_run_id) if latest_run_id is not None else None
                )
            seq += 1
            yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps(done, default=str)}\n\n"

        return StreamingResponse(
            observe_replay_stream(event_generator(), source="private"),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Replay-Source": resolved_source,
            },
        )

    except DATABASE_UNAVAILABLE as e:
        # Replay ends the read on a database fault rather than show the
        # thread as stubs.
        raise database_unavailable(f"replaying thread {thread_id}", logger, e)
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
        from src.server.services.report_back.handoff.status import read_report_back_slice

        return await read_report_back_slice(thread_id, user_id=meta["user_id"])

    from src.server.database.runs import outbox as outbox_db
    from src.server.services.thread_status import read_thread_runtime_status

    status = await read_thread_runtime_status(
        thread_id,
        is_shared=bool(meta["is_shared"]),
        user_id=meta["user_id"],
    )
    # The user's steers no run has read yet, such as those an errored turn
    # left parked: the next turn reads them, and the view shows them queued
    # until it does.
    status["pending_steers"] = await outbox_db.list_pending_steers(thread_id)
    return status


# Upper bound on ids of each kind (threads, runs, steers) per liveness request,
# so one request stays one bounded query per kind. Ids past the cap are dropped
# for that request; the client sends at most this many of each kind per request
# and splits larger sets across requests.
_MAX_LIVENESS_IDS = 100


def _run_view(row: dict) -> dict:
    """A run's public status and times, the same for a thread's latest run
    and for a run read by id. The finish is null while the run is live; a
    client tells a live run by the status, not by these times."""
    return {
        "status": to_public(
            row["status"], cancel_requested_at=row.get("cancel_requested_at")
        ),
        "run_started_at": row["created_at"],
        "run_finished_at": run_completed_at(row),
    }


def _liveness_ids(raw: str, kind: str, user_id: str) -> list[str]:
    """A comma-separated id list, deduped in order and capped."""
    seen: set[str] = set()
    deduped: list[str] = []
    for part in raw.split(","):
        item = part.strip()
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    if len(deduped) > _MAX_LIVENESS_IDS:
        logger.warning(
            f"[LIVENESS] {len(deduped)} {kind} requested by {user_id}; capping at "
            f"{_MAX_LIVENESS_IDS} (remainder unresolved this request)"
        )
        deduped = deduped[:_MAX_LIVENESS_IDS]
    return deduped


@router.get("/dispatches/liveness")
async def get_dispatches_liveness(
    x_user_id: CurrentUserId,
    ids: str = Query(
        "",
        description="Comma-separated thread ids to read liveness for (one MGET).",
    ),
    runs: str = Query(
        "",
        description="Comma-separated run ids to read on their own, beside the threads.",
    ),
    steers: str = Query(
        "",
        description="Comma-separated steer ids to resolve to the run that took each.",
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

    ``runs`` reads named runs the same way: a hand-off is one run, and once
    it is over the thread's latest attempt describes whatever the thread did
    since. Unowned or unknown runs are omitted.

    ``steers`` follows a hand-off steered into a running turn to the run
    that took it in, which is a later run when that turn ended before
    reading it. Each says ``taken``, with that run's id, whose view joins
    ``runs`` if it was not asked for; ``queued`` until a run takes it; or
    ``dropped`` if it settled with none. A steer the running turn holds for
    its next model call is taken, and goes back to queued if that turn ends
    without reading it. A steer is known only while its outbox row lives,
    which is purged 7 days after it settles (``RETENTION_SECONDS``); after
    that it is omitted, as unowned and unknown ones are, and the client
    falls back to the run it was steered into. A reported hand-off keeps its
    link through its report's ``steer_ids``.
    """
    thread_ids = _liveness_ids(ids, "ids", x_user_id)
    run_ids = _liveness_ids(runs, "runs", x_user_id)
    steer_ids = _liveness_ids(steers, "steers", x_user_id)

    if not thread_ids and not run_ids and not steer_ids:
        return {"liveness": [], "runs": [], "steers": []}

    from src.server.database.runs import handoff_liveness
    from src.server.database.runs import lifecycle as tl_db

    async def _read(fetch, wanted: list[str]) -> dict:
        return await fetch(wanted, x_user_id) if wanted else {}

    # Independent reads, each on its own pooled connection.
    rows, run_rows, steer_rows = await asyncio.gather(
        _read(tl_db.get_latest_attempts_for_threads, thread_ids),
        _read(handoff_liveness.get_owned_runs, run_ids),
        _read(handoff_liveness.get_owned_steers, steer_ids),
    )

    liveness = []
    for tid, row in rows.items():
        # The thread's latest run, live or settled: a card opened mid-run
        # times it from its start, and a settled one says how long it took.
        view = _run_view(row)
        live = view["status"] in ("running", "stopping")
        liveness.append(
            {
                "thread_id": tid,
                **view,
                "run_id": str(row["conversation_response_id"]) if live else None,
                "can_reconnect": live,
                # When the thread's latest turn started or settled, whoever
                # sent it: how long the thread has been quiet.
                "last_activity_at": row["last_activity_at"],
                # Null until the thread is named; a view falls back to the
                # question it was handed.
                "title": row["title"],
                # Current, where a hand-off recorded the name at the time.
                "workspace_name": row["workspace_name"],
            }
        )

    # A taken steer's run is read in the same query, so a view following the
    # steer finds that run here without asking for it again.
    runs_by_id = dict(run_rows)
    for steer in steer_rows.values():
        if steer["run"] is not None:
            runs_by_id.setdefault(steer["run_id"], steer["run"])
    pinned = [
        {
            "run_id": rid,
            "thread_id": str(row["conversation_thread_id"]),
            **_run_view(row),
        }
        for rid, row in runs_by_id.items()
    ]

    resolved = [
        {
            "steer_id": sid,
            "thread_id": steer["thread_id"],
            "state": steer["state"],
            "run_id": steer["run_id"],
        }
        for sid, steer in steer_rows.items()
    ]

    return {"liveness": liveness, "runs": pinned, "steers": resolved}


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
):
    """Manually trigger context compaction for a thread, keeping the tail
    automatic compaction keeps. A ``keep_messages`` count older clients send
    is ignored.

    Endpoint path ``/summarize`` and function name preserved for REST contract
    compatibility — clients may call the older URL.
    """
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.thread_maintenance import trigger_compaction

    return await trigger_compaction(thread_id, user_id=x_user_id)


@router.post("/{thread_id}/offload", status_code=200)
async def offload_thread(thread_id: str, x_user_id: CurrentUserId):
    """Hide large tool arguments and stale Read results from the model (Tier 1 only)."""
    await auth_api.require_thread_owner(thread_id, x_user_id)
    from src.server.handlers.thread_maintenance import trigger_offload

    return await trigger_offload(thread_id, user_id=x_user_id)


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
    archived. Graph-wise the retry resumes from the last checkpoint, unless
    the failed attempt never reached the graph: then it runs that attempt's
    message again, or replays the checkpoint a regenerate named
    (``pending_input``). Returns an SSE stream.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.handlers.chat.admission_gate import admission_conflict_detail
    from src.server.handlers.checkpoint_handler import get_retry_checkpoint
    from src.server.services.runs.admission import RunScope
    from src.server.services.runs.pending_input import (
        input_rerun_request,
        read_pending_input,
        retried_wake_fields,
    )

    scope = RunScope(user_id=auth.user_id, burst_slot_id=auth.burst_slot_id)

    try:
        await auth_api.require_thread_owner(thread_id, auth.user_id)

        # I6: refuse before any durable row when the event transport is down.
        await _assert_stream_transport_ready()

        # Retransmit probe FIRST: a duplicate /retry must resolve to its
        # existing attempt, not trip the latest-attempt validation below
        # (which would mislabel it stale_retry or running).
        if body and body.request_key:
            await _reject_duplicate_request(
                thread_id, body.request_key, auth.user_id
            )

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

        # Resolve workspace_id from body or from the thread record
        workspace_id = body.workspace_id if body and body.workspace_id else None
        if not workspace_id:
            thread_record = await get_thread_by_id(thread_id)
            if not thread_record:
                raise HTTPException(
                    status_code=404, detail=f"Thread {thread_id} not found"
                )
            workspace_id = str(thread_record.get("workspace_id", ""))

        pending = read_pending_input(latest)
        if pending is not None and not pending.replay:
            # No checkpoint holds the failed message (a new thread's first
            # send has none at all), so a replay would answer the turn
            # before it. The message runs again instead.
            request = await input_rerun_request(
                thread_id, latest, pending, workspace_id=workspace_id, body=body
            )
        else:
            # A checkpoint replay carrying the attempt chain (no
            # fork_from_turn: nothing is truncated). A failed regenerate
            # replays its own fork base, which is not the newest checkpoint.
            explicit_checkpoint_id = (body.checkpoint_id if body else None) or (
                pending.checkpoint_id if pending else None
            )
            request = ChatRequest(
                workspace_id=workspace_id,
                messages=[],
                checkpoint_id=await get_retry_checkpoint(
                    thread_id, explicit_checkpoint_id
                ),
                subagents_enabled=(latest.get("metadata") or {}).get(
                    "subagents_enabled"
                ),
                request_key=(body.request_key if body else None),
                llm_model=(body.llm_model if body else None),
                reasoning_effort=(body.reasoning_effort if body else None),
                fast_mode=(body.fast_mode if body else None),
                **await retried_wake_fields(thread_id, latest["turn_index"]),
            )
    except BaseException:
        # ChatRateLimited acquired a burst slot at the dependency; every
        # early exit above bypasses _handle_send_message, whose own guard
        # normally releases it — without this, repeated stale retries
        # exhaust the user's burst allowance until TTL expiry.
        await scope.release_slot()
        raise

    # Retry provenance travels as a route-internal parameter, never in the
    # body.
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
    open_turn: bool = Query(
        False,
        description=(
            "v2 only: the client's projection holds none of the executing "
            "turn's settled runs (a fresh page), so drain them; acknowledged "
            "by an `attached` control frame"
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
        stream_thread_mux_v2(
            thread_id,
            parse_mux_cursors_v2(cursors),
            since_age_s,
            open_turn=open_turn,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
