"""Starting a question as a background turn of a workspace's own agent.

Flash's ``ptc_agent`` and the Chief of Staff's ``delegate_to_analyst`` both
hand work over through ``dispatch``. The turn starts over the chat endpoint
in the background, and a report-back slot taken before the request goes out
is kept or rolled back by what the reply proves about whether the run started.
"""

import asyncio
import json
import logging
import os
import uuid

from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.types import Command

from src.tools.secretary._commands import (
    decline_routing_command,
    error_command,
    hitl_confirm,
    success_command,
    verify_thread_owner,
    verify_workspace_owner,
)

logger = logging.getLogger(__name__)


_DISPATCH_CONFIRM_GRACE_S = 6.0
_DISPATCH_CONFIRM_POLL_S = 0.5

# Flash's ptc_agent gets this too and has no stop of its own, so the stop is
# offered as the user's choice rather than the model's.
_THREAD_BUSY = (
    "That thread is still working on an earlier message, so this was not "
    "sent. Tell the user it is busy: they can wait for that turn and ask "
    "again or stop it, or you can start a new thread with workspace_id."
)


async def _confirm_dispatch_admission(
    thread_id: str, expected_gen: str | None
) -> bool:
    """Probe the durable run ledger after an ambiguous dispatch exchange.

    The dispatched branch commits the run's in_progress row — its metadata
    stamped with the POST's dispatch generation — BEFORE replying (v4 2.4c
    eager START), and every rejection path exits without a row. POSITIVE-
    ONLY oracle: True means an attempt row provably belongs to THIS
    dispatch — an exact generation match when the dispatch carries one, or
    any attempt when it doesn't (callers pass ``expected_gen=None`` only
    for a thread id minted by this very call, so nobody else can have run
    on it). False settles NOTHING: a foreign row may predate our own
    admission (so it keeps polling to the deadline, never returns early),
    and no finite absence proves a delivered, still-processing request
    won't admit later. The caller must treat False as unproven and retain.
    """
    from src.server.database.runs import lifecycle as tl_db

    loop = asyncio.get_running_loop()
    deadline = loop.time() + _DISPATCH_CONFIRM_GRACE_S
    while True:
        try:
            row = await tl_db.get_latest_attempt(thread_id)
        except Exception:
            row = None
        if row is not None:
            if expected_gen is None:
                return True
            row_gen = (row.get("metadata") or {}).get("origin_dispatch_gen")
            if row_gen == expected_gen:
                return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(_DISPATCH_CONFIRM_POLL_S)


def _unknown_dispatch_command(
    error: str, thread_id: str, workspace_id: str | None, tool_call_id: str
) -> Command:
    """Ambiguous dispatch outcome: the reservation is retained and the run may
    already be live on ``thread_id`` — surface that id so the model checks
    agent_output before re-dispatching (a blind retry would occupy a second
    cap slot and can produce a duplicate report-back)."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps(
                        {
                            "success": False,
                            "error": error,
                            "outcome": "unknown_retained",
                            "thread_id": thread_id,
                            "workspace_id": workspace_id,
                            "note": (
                                "Dispatch outcome unknown — the analysis may "
                                "already be running on this thread. Check "
                                "agent_output with this thread_id before "
                                "re-dispatching."
                            ),
                        }
                    ),
                    tool_call_id=tool_call_id,
                ),
            ],
        }
    )


async def _cleanup_auto_created_workspace(workspace_id: str) -> None:
    """Best-effort delete of a just-created, provably-unused workspace."""
    try:
        from src.server.services.workspace_manager import WorkspaceManager

        await WorkspaceManager.get_instance().delete_workspace(workspace_id)
    except Exception as cleanup_err:
        logger.warning(
            f"Failed to delete auto-created workspace {workspace_id} "
            f"after failed dispatch: {cleanup_err}"
        )


async def _owned_workspace(workspace_id: str, user_id: str) -> dict | None:
    """The workspace row when ``user_id`` owns it, else None.

    Ownership-scoped so the new-thread dispatch HITL card can't surface another
    user's workspace name before the ownership check runs.
    """
    from src.server.database.workspace import get_workspace

    try:
        ws = await get_workspace(workspace_id)
    except Exception as e:
        logger.warning(f"Failed to read workspace {workspace_id}: {e}")
        return None
    return ws if ws and str(ws.get("user_id")) == user_id else None



# Concurrent dispatches that planned one name each take the next free number.
_AUTO_NAME_ATTEMPTS = 5


async def _free_workspace_name(user_id: str, name: str) -> str:
    """A name for a workspace nobody named, numbered past the ones the user has."""
    from src.server.database.workspace import get_workspace_name_keys
    from src.server.database.workspace_names import (
        WorkspaceNameInvalid,
        checked_workspace_name,
        first_free_name,
    )

    try:
        name = checked_workspace_name(name)
    except WorkspaceNameInvalid:
        name = "Research"
    try:
        taken = await get_workspace_name_keys(user_id)
    except Exception as e:
        # Only the number is lost: creation still refuses a taken name, and
        # the caller's retry asks again.
        logger.warning(f"Failed to read workspace names for {user_id}: {e}")
        return name
    return first_free_name(name, taken)


async def dispatch(
    question: str,
    config: RunnableConfig,
    workspace_id: str | None,
    thread_id: str | None,
    report_back: bool,
    tool_call_id: str,
    preapproved: bool = False,
) -> Command:
    """Start ``question`` as a background turn of a workspace's own agent.

    The workspace is ``workspace_id``, or ``thread_id``'s when continuing one;
    with neither, a workspace is created for the question. The dispatcher's
    own workspace is refused: a turn there is the dispatcher again, not the
    agent of the workspace it meant. A ``preapproved`` hand-off, one the user
    approved in advance, starts without asking.
    """
    import aiohttp

    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)

    # With auth enabled, the endpoint rejects an unauthenticated background
    # dispatch (403); abort before any side effect (HITL prompt, workspace
    # creation, cap reservation) so the user gets the specific error instead.
    from src.config.settings import background_dispatch_requires_token

    if background_dispatch_requires_token():
        logger.error(
            "PTC dispatch aborted: INTERNAL_SERVICE_TOKEN is not set, so the "
            "background dispatch cannot be authenticated. Set it on the "
            "backend service to enable dispatch."
        )
        return error_command("internal_service_token_missing", tool_call_id)

    from src.server.database.home_workspace import is_flash_row
    from src.server.utils.pg_sanitize import normalize_uuid

    is_continuation = thread_id is not None

    # Resolve workspace_id from existing thread or create/verify workspace
    workspace = None
    if is_continuation:
        from src.server.database.conversation.threads_read import get_thread_by_id

        # Normalize once so the owner check and every downstream bind use the
        # same canonical UUID (get_thread_owner_id and get_thread_by_id also
        # normalize internally). None means not a UUID -> not found.
        normalized_id = normalize_uuid(thread_id)
        if normalized_id is None:
            return error_command(
                "thread not found or not owned by user", tool_call_id
            )
        thread_id = normalized_id

        # Ownership lives on workspaces.user_id (conversation_threads has no
        # user_id column), so verify via the JOIN helper rather than reading
        # a user_id off the thread row, which is always None.
        if err := await verify_thread_owner(thread_id, user_id, tool_call_id):
            return err
        thread = await get_thread_by_id(thread_id)
        workspace_id = str(thread["workspace_id"])
        workspace = await _owned_workspace(workspace_id, user_id)
        workspace_name = workspace.get("name") if workspace else None
    else:
        # New thread: surface the existing workspace's real name; when
        # auto-creating (no workspace_id) use the planned name (question snippet).
        if workspace_id:
            # Canonical, as the owner check reads it, so no other spelling of
            # the dispatcher's own id gets past the check below. One that is
            # no UUID matches no row, which the owner check refuses.
            workspace_id = normalize_uuid(workspace_id) or workspace_id
            workspace = await _owned_workspace(workspace_id, user_id)
            workspace_name = workspace.get("name") if workspace else None
        else:
            workspace_name = await _free_workspace_name(user_id, question[:50])

    # Any flash row, not only the dispatcher's: a turn there runs on Flash or
    # as the Chief of Staff in Home, never as a workspace's analyst.
    if workspace_id is not None and (
        workspace_id == normalize_uuid(configurable.get("workspace_id"))
        or is_flash_row(workspace)
    ):
        return error_command(
            "That is the workspace you are working in: do it here, or "
            "dispatch to another workspace.",
            tool_call_id,
        )

    if not preapproved:
        approved, response = hitl_confirm(
            "ptc_agent",
            {
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "thread_id": thread_id,
                "question": question,
                "report_back": report_back,
                "tool_call_id": tool_call_id,
            },
        )

        if not approved:
            return decline_routing_command("the hand-off", response, tool_call_id)

        # Apply user overrides from HITL decision (e.g. toggling report_back)
        decisions = response.get("decisions", [])
        if decisions:
            overrides = decisions[0].get("overrides", {})
            if "report_back" in overrides:
                report_back = overrides["report_back"]

    auto_created_workspace = False
    if not is_continuation:
        # Create workspace or verify ownership
        if workspace_id is None:
            from src.server.services.report_back.flash.reserve import check_dispatch_capacity

            # Advisory cap check BEFORE provisioning: a dispatch reserve() is
            # certain to reject must not spin up a sandbox it would orphan.
            # reserve() below remains the atomic authority.
            cap_err = await check_dispatch_capacity(
                configurable.get("thread_id") if report_back else None, user_id
            )
            if cap_err is not None:
                return error_command(cap_err, tool_call_id)
            try:
                from src.server.database.workspace_names import WorkspaceNameTaken
                from src.server.services.workspace_manager import WorkspaceManager

                workspace_manager = WorkspaceManager.get_instance()
                name = workspace_name
                for attempt in range(_AUTO_NAME_ATTEMPTS):
                    try:
                        workspace = await workspace_manager.create_workspace(
                            user_id=user_id,
                            name=name,
                            description=f"Auto-created for: {question[:100]}",
                        )
                        break
                    except WorkspaceNameTaken:
                        # Another dispatch took it since the last read; the
                        # next free one is the same name with a later number.
                        if attempt == _AUTO_NAME_ATTEMPTS - 1:
                            raise
                        name = await _free_workspace_name(user_id, workspace_name)
                workspace_id = str(workspace["workspace_id"])
                workspace_name = name
                auto_created_workspace = True
            except Exception as e:
                logger.error(f"Failed to create workspace for PTC dispatch: {e}")
                return error_command("workspace_creation_failed", tool_call_id)
        else:
            if err := await verify_workspace_owner(workspace_id, user_id, tool_call_id):
                return err

        # New thread
        thread_id = str(uuid.uuid4())

    # reserve() takes a cap slot + records the PTC origin, rolling back on any
    # non-committed exit; a no-op when flash_thread_id is None (report_back off).
    # ``slot.wired`` (not the request flag) is echoed as report_back so we never
    # promise a report-back the completion gate would drop. The origin also
    # records where this turn came from, which the report-back turn is told.
    from src.server.services.report_back.flash import requested_from
    from src.server.services.report_back.flash.reserve import reserve

    flash_thread_id = configurable.get("thread_id") if report_back else None
    flash_workspace_id = configurable.get("workspace_id")

    # Dispatch via internal HTTP call.
    # X-Dispatch: background tells the endpoint to run the PTC workflow in a
    # background asyncio task and return JSON immediately, avoiding the
    # generator-cancelled-on-client-disconnect race.
    self_base_url = os.environ.get("GINLIXFLOW_BASE_URL", "http://localhost:8000")
    service_token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")

    async with reserve(
        flash_thread_id,
        thread_id,
        workspace_id,
        flash_workspace_id,
        user_id,
        requested_from=requested_from.of_turn(configurable, question),
    ) as slot:

        def dispatched(run_id: str | None = None) -> Command:
            # The card for a pre-approved hand-off is drawn from this result,
            # since no approval request carried the workspace's name.
            result = {
                "success": True,
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "thread_id": thread_id,
                "status": "dispatched",
                "report_back": slot.wired,
            }
            if run_id:
                # What a later stop names, so it ends this hand-off and never
                # a turn started in the thread since. Absent when the body
                # was lost; the stop then takes whatever the thread runs.
                result["run_id"] = run_id
            if preapproved:
                result["preapproved"] = True
            return success_command(result, tool_call_id)

        # Cap rejection or a fail-closed origin write — abort (reserve rolls back).
        if slot.error is not None:
            # No HTTP was sent, so a workspace auto-created above is provably
            # unused — delete it rather than leak its sandbox (the pre-check
            # narrows this to the pre-check/reserve race).
            if auto_created_workspace:
                await _cleanup_auto_created_workspace(workspace_id)
            return error_command(slot.error, tool_call_id)
        # ``rejected``: a definitive non-scheduling proof was observed — a
        # cancellation arriving during the subsequent best-effort cleanup must
        # then NOT commit. ``ambiguous_error``: delivery unproven either way;
        # settled by the admission-marker reconciliation below the try.
        rejected = False
        ambiguous_error: str | None = None
        run_id: str | None = None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self_base_url}/api/v1/threads/{thread_id}/messages",
                    json={
                        "messages": [{"role": "user", "content": question}],
                        "agent_mode": "ptc",
                        "workspace_id": workspace_id,
                        # Durable provenance on the thread row. Unconditional —
                        # unlike origin_flash_thread_id below, which is a
                        # report-back wiring token, not an initiator record.
                        "origin": {
                            "type": "agent",
                            "id": configurable.get("thread_id"),
                        },
                        # Ordering hint for finalize hooks: only a WIRED
                        # report-back binds the run to this flash thread's
                        # serialization chain.
                        "origin_flash_thread_id": (
                            flash_thread_id if slot.wired else None
                        ),
                        # Incarnation token minted by reserve(): fences this
                        # dispatch's terminal teardowns to its own generation.
                        "origin_dispatch_gen": (
                            slot.dispatch_gen if slot.wired else None
                        ),
                    },
                    headers={
                        "X-Service-Token": service_token,
                        "X-User-Id": user_id,
                        "X-Dispatch": "background",
                    },
                    timeout=aiohttp.ClientTimeout(connect=10, sock_read=30),
                    allow_redirects=False,
                ) as resp:
                    if resp.status >= 400:
                        # An error status proves the endpoint exited before
                        # scheduling the run (every raise path precedes its
                        # create_task), so an auto-created workspace is still
                        # provably unused.
                        rejected = True
                        if auto_created_workspace:
                            await _cleanup_auto_created_workspace(workspace_id)
                        if resp.status == 409:
                            # A dispatch never steers (steer_allowed), so a
                            # thread mid-turn refuses it; say so, or the model
                            # reports a failure and retries into the same 409.
                            # It sends no request_key, retry_of_run_id or
                            # steer_only, so every 409 left means busy:
                            # running, stopping or compacting.
                            return error_command(_THREAD_BUSY, tool_call_id)
                        return error_command("dispatch_failed", tool_call_id)
                    if resp.status != 200:
                        # Not the endpoint's reply (it answers exactly 200;
                        # redirects are disabled): some other hop spoke —
                        # whether the handler ran is settled below.
                        ambiguous_error = "dispatch_failed"
                    else:
                        # The endpoint's exact success status IS the
                        # scheduling proof (it replies only after its
                        # create_task) — commit BEFORE touching the body, so
                        # a lost/truncated body can't roll back a reservation
                        # whose run is already live (the run's report-back
                        # would find no origin and be dropped).
                        slot.commit()
                        try:
                            body = await resp.json()
                        except (aiohttp.ClientError, ValueError, TimeoutError):
                            logger.warning(
                                "PTC dispatch response body lost after "
                                "success status 200; treating as dispatched"
                            )
                            body = {"status": "dispatched"}
                        if (
                            not isinstance(body, dict)
                            or body.get("status") != "dispatched"
                        ):
                            # A 200 carrying a contradictory body: the status
                            # proof stands (never roll back), but don't claim
                            # success — reconcile below.
                            ambiguous_error = "dispatch_failed"
                        else:
                            run_id = body.get("run_id")
        except asyncio.CancelledError:
            # Cancellation mid-exchange (flash turn cancelled, worker
            # shutdown) is as ambiguous as a lost response: the endpoint may
            # already have scheduled the run, so commit before propagating —
            # UNLESS the outcome was already definitively rejected and the
            # cancel merely landed during the best-effort cleanup.
            if not rejected:
                slot.commit()
            raise
        except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as e:
            # The request provably never reached the endpoint — rolling the
            # reservation back is safe and an auto-created workspace is
            # provably unused. (A cancel during this cleanup propagates
            # uncommitted — the rollback is exactly what's wanted.)
            logger.error(f"PTC dispatch connection failed: {e}")
            if auto_created_workspace:
                await _cleanup_auto_created_workspace(workspace_id)
            return error_command("dispatch_failed", tool_call_id)
        except (aiohttp.ClientError, ValueError) as e:
            logger.error(f"PTC dispatch HTTP error: {e}")
            ambiguous_error = "dispatch_failed"
        except TimeoutError:
            logger.error("PTC dispatch timed out")
            ambiguous_error = "dispatch_timeout"

        if ambiguous_error is not None:
            # Settle the unknown against the endpoint's admission marker,
            # scoped to THIS dispatch's generation (the endpoint stamps the
            # POST's origin_dispatch_gen into the marker and refuses to
            # schedule if the write fails). The oracle is POSITIVE-ONLY:
            # confirmation upgrades to plain success; anything less retains
            # the reservation as unknown (TTL-bounded, orphan-reaped once
            # the origin lapses). Reconciliation NEVER rolls back — the only
            # sound rollback receipts are a definitive HTTP status (the
            # >=400 branch) or a provably-undelivered connection, both
            # handled above. In particular a continuation must not roll back
            # on a foreign/absent marker: our own admission may stamp
            # moments later, and destroying the provisional origin then
            # orphans a LIVE run's report-back (the retained-but-409'd
            # alternative merely wedges one cap slot until the origin TTL).
            if slot.wired:
                expected_gen = slot.dispatch_gen
            elif not is_continuation:
                # Unwired fresh pair: the thread id was minted by this call,
                # so ANY marker on it can only be our own admission.
                expected_gen = None
            else:
                # Unwired continuation: no identity to match — a marker
                # proves only that SOME run held the thread. Unprovable;
                # retain without probing.
                slot.commit()
                return _unknown_dispatch_command(
                    ambiguous_error, thread_id, workspace_id, tool_call_id
                )
            try:
                confirmed = await _confirm_dispatch_admission(
                    thread_id, expected_gen
                )
            except asyncio.CancelledError:
                # Cancelled mid-probe: still unknown — retain.
                slot.commit()
                raise
            slot.commit()
            if confirmed:
                # The lost reply was a real acceptance of THIS request.
                return dispatched()
            return _unknown_dispatch_command(
                ambiguous_error, thread_id, workspace_id, tool_call_id
            )

        slot.commit()
        return dispatched(run_id)
