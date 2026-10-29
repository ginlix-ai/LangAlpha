"""Starting a question as a background turn of a workspace's own agent.

The Chief of Staff's ``delegate_to_analyst`` hands work over through
``dispatch``. The turn starts over the chat endpoint in the background, and a
report-back slot taken before the request goes out is kept or rolled back by
what the reply proves about whether the run started.
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
)

logger = logging.getLogger(__name__)


_DISPATCH_CONFIRM_GRACE_S = 6.0
_DISPATCH_CONFIRM_POLL_S = 0.5

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
            metadata = row.get("metadata") or {}
            # A hand-off steered into a running turn is admitted on that run.
            if expected_gen == metadata.get("origin_dispatch_gen") or (
                expected_gen in (metadata.get("dispatch_gens") or [])
            ):
                return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(_DISPATCH_CONFIRM_POLL_S)


def _unknown_dispatch_command(
    error: str,
    thread_id: str,
    workspace_id: str | None,
    tool_call_id: str,
    handed_to: dict | None = None,
) -> Command:
    """Ambiguous dispatch outcome: the reservation is retained and the run may
    already be live on ``thread_id`` — surface that id so the model checks
    agent_output before re-dispatching (a blind retry would occupy a second
    cap slot and can produce a duplicate report-back). ``handed_to`` is what
    a pre-approved hand-off's card is drawn from."""
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content=json.dumps(
                        {
                            "success": False,
                            "error": error,
                            "outcome": "unknown_retained",
                            **(handed_to or {}),
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


async def _owned_workspace(workspace_id: str, user_id: str) -> dict | None:
    """The workspace row when ``user_id`` owns it, else None.

    Ownership-scoped so the hand-off card can't surface another user's
    workspace name. A read that fails is None as well: a continuation only
    loses the name on its card, and a new thread is refused as not found.
    """
    from src.server.database.workspace import get_workspace

    try:
        ws = await get_workspace(workspace_id)
    except Exception as e:
        logger.warning(f"Failed to read workspace {workspace_id}: {e}")
        return None
    return ws if ws and str(ws.get("user_id")) == user_id else None


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

    The workspace is ``thread_id``'s when continuing one, else ``workspace_id``;
    the caller refuses a call naming neither. The dispatcher's own workspace is
    refused: a turn there is the dispatcher again, not the agent of the
    workspace it meant. A ``preapproved`` hand-off, one the user approved in
    advance, starts without asking.
    """
    import aiohttp

    configurable = config.get("configurable", {})
    user_id = configurable.get("user_id")
    if not user_id:
        return error_command("user_id not found in config", tool_call_id)

    # With auth enabled, the endpoint rejects an unauthenticated background
    # dispatch (403); abort before any side effect (HITL prompt, cap
    # reservation) so the user gets the specific error instead.
    from src.config.settings import background_dispatch_requires_token

    if background_dispatch_requires_token():
        logger.error(
            "PTC dispatch aborted: INTERNAL_SERVICE_TOKEN is not set, so the "
            "background dispatch cannot be authenticated. Set it on the "
            "backend service to enable dispatch."
        )
        return error_command("internal_service_token_missing", tool_call_id)

    from src.server.database.home_workspace import is_home_row
    from src.server.utils.pg_sanitize import normalize_uuid

    is_continuation = thread_id is not None

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
    else:
        # Canonical, as the owner check reads it, so no other spelling of
        # the dispatcher's own id gets past the check below. One that is
        # no UUID matches no row, which the owner check refuses.
        workspace_id = normalize_uuid(workspace_id) or workspace_id
    workspace = await _owned_workspace(workspace_id, user_id)
    workspace_name = workspace.get("name") if workspace else None

    # Any Home row, not only the dispatcher's: a turn there is never a
    # workspace's analyst.
    if (
        workspace_id == normalize_uuid(configurable.get("workspace_id"))
        or is_home_row(workspace)
    ):
        return error_command(
            "That is the workspace you are working in: do it here, or "
            "dispatch to another workspace.",
            tool_call_id,
        )
    # Refused before the card, so the user is never asked to approve a
    # hand-off that cannot run. A continuation's workspace is its thread's,
    # which the owner check above already proved.
    if workspace is None and not is_continuation:
        return error_command("workspace not found", tool_call_id)

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

        # The card's report-back switch, when the user moved it. A resume
        # that names no overrides keeps what the call asked for.
        overrides = (response.get("decisions") or [{}])[0].get("overrides")
        if isinstance(overrides, dict) and isinstance(
            overrides.get("report_back"), bool
        ):
            report_back = overrides["report_back"]

    # A hand-off approved in advance has no card but the one its result
    # draws, so a failure from here on still says where it was going. A new
    # thread's id is left out: no thread has it unless the run starts.
    handed_to: dict = {}
    if preapproved:
        handed_to = {
            "preapproved": True,
            "workspace_id": workspace_id,
            "workspace_name": workspace_name,
        }
        if is_continuation:
            handed_to["thread_id"] = thread_id

    if not is_continuation:
        thread_id = str(uuid.uuid4())

    # reserve() takes a cap slot + records the PTC origin, rolling back on any
    # non-committed exit; a no-op when dispatcher_thread_id is None (report_back off).
    # ``slot.wired`` (not the request flag) is echoed as report_back so we never
    # promise a report-back the completion gate would drop. The origin also
    # records where this turn came from, which the report-back turn is told.
    from src.server.services.report_back.handoff import requested_from
    from src.server.services.report_back.handoff.keys import ORIGIN_THREAD_META
    from src.server.services.report_back.handoff.reserve import reserve

    dispatcher_thread_id = configurable.get("thread_id") if report_back else None
    dispatcher_workspace_id = configurable.get("workspace_id")

    # Dispatch via internal HTTP call.
    # X-Dispatch: background tells the endpoint to run the PTC workflow in a
    # background asyncio task and return JSON immediately, avoiding the
    # generator-cancelled-on-client-disconnect race.
    self_base_url = os.environ.get("GINLIXFLOW_BASE_URL", "http://localhost:8000")
    service_token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")

    async with reserve(
        dispatcher_thread_id,
        thread_id,
        workspace_id,
        dispatcher_workspace_id,
        user_id,
        requested_from=requested_from.of_turn(configurable, question),
    ) as slot:

        def dispatched(
            run_id: str | None = None,
            *,
            steered: bool = False,
            steer_id: str | None = None,
        ) -> Command:
            # The card for a pre-approved hand-off is drawn from this result,
            # since no approval request carried the workspace's name.
            result = {
                "success": True,
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "thread_id": thread_id,
                "status": "steered" if steered else "dispatched",
                "report_back": slot.wired,
            }
            if steered:
                result["note"] = (
                    "The analyst is mid-turn: it takes this in before its next "
                    "step, or as its next turn if that one ends first."
                )
            if run_id:
                # What a later stop names, so it ends this hand-off and never
                # a turn started in the thread since. Absent when the body
                # was lost; the stop then takes whatever the thread runs.
                result["run_id"] = run_id
            if preapproved:
                result["preapproved"] = True
            # The steer's row names the run that takes the hand-off in if
            # ``run_id`` ends before reading it. The view reads it from the
            # artifact, which the model never sees.
            artifact = {"steer_id": steer_id} if steer_id else None
            return success_command(result, tool_call_id, artifact=artifact)

        # Cap rejection or a fail-closed origin write — abort (reserve rolls back).
        if slot.error is not None:
            return error_command(slot.error, tool_call_id, **handed_to)
        # ``rejected``: a definitive non-scheduling proof was observed — a
        # cancellation landing while the response and session close must then
        # NOT commit. ``ambiguous_error``: delivery unproven either way;
        # settled by the admission-marker reconciliation below the try.
        rejected = False
        ambiguous_error: str | None = None
        run_id: str | None = None
        steered = False
        steer_id: str | None = None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self_base_url}/api/v1/threads/{thread_id}/messages",
                    json={
                        "messages": [{"role": "user", "content": question}],
                        "workspace_id": workspace_id,
                        # Durable provenance on the thread row. Unconditional —
                        # unlike ORIGIN_THREAD_META below, which is a
                        # report-back wiring token, not an initiator record.
                        "origin": {
                            "type": "agent",
                            "id": configurable.get("thread_id"),
                            "agent": "chief_of_staff",
                        },
                        # Ordering hint for finalize hooks: only a WIRED
                        # report-back binds the run to this thread's
                        # serialization chain.
                        ORIGIN_THREAD_META: (
                            dispatcher_thread_id if slot.wired else None
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
                        # create_task).
                        rejected = True
                        if resp.status == 409:
                            # A running turn takes the hand-off as a steer, so
                            # a 409 means one it cannot ride: stopping,
                            # compacting, or reporting to another Home thread.
                            # Say so, or the model reports a failure and
                            # retries into the same 409.
                            return error_command(
                                _THREAD_BUSY, tool_call_id, **handed_to
                            )
                        return error_command(
                            "dispatch_failed", tool_call_id, **handed_to
                        )
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
                        if not isinstance(body, dict) or body.get("status") not in (
                            "dispatched",
                            "steered",
                        ):
                            # A 200 carrying a contradictory body: the status
                            # proof stands (never roll back), but don't claim
                            # success — reconcile below.
                            ambiguous_error = "dispatch_failed"
                        else:
                            run_id = body.get("run_id")
                            steered = body.get("status") == "steered"
                            steer_id = body.get("steer_id") if steered else None
        except asyncio.CancelledError:
            # Cancellation mid-exchange (the dispatching turn cancelled, worker
            # shutdown) is as ambiguous as a lost response: the endpoint may
            # already have scheduled the run, so commit before propagating —
            # UNLESS the outcome was already definitively rejected and the
            # cancel merely landed as the response and session closed.
            if not rejected:
                slot.commit()
            raise
        except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as e:
            # The request provably never reached the endpoint, so rolling the
            # reservation back is safe.
            logger.error(f"PTC dispatch connection failed: {e}")
            return error_command("dispatch_failed", tool_call_id, **handed_to)
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
                    ambiguous_error, thread_id, workspace_id, tool_call_id, handed_to
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
                ambiguous_error, thread_id, workspace_id, tool_call_id, handed_to
            )

        slot.commit()
        return dispatched(run_id, steered=steered, steer_id=steer_id)
