"""What a turn takes over from the turn before it: the report-back pair of an
interrupted summary, the automation run a resumed or retried turn sends for,
and the automation firing of a turn a restart stopped."""

from __future__ import annotations

from src.server.contracts.status import is_restart_interrupted


async def carried_pair(request, thread_id: str) -> dict:
    """START metadata naming the pair this turn releases for the summary before it.

    A summary that ends interrupted (an approval card, a question) is waiting on
    the user and releases nothing, and the turn that answers it is a public
    request stripped of the pair. So whichever turn follows on the thread, a
    resume or a message past the interrupt, stamps the summary's pair: it
    releases it when it ends and carries it on when it is interrupted too. The
    dispatch generation travels with it, so the release spares a pair
    dispatched again since. A summary a restart stopped is carried on the same
    way, by the turn that resumes it. A summary names its own pair and takes
    none.
    """
    if getattr(request, "report_back_ptc_thread_id", None):
        return {}
    from src.server.database.runs import lifecycle as tl_db

    # A read failure fails the turn start: a turn stamped without the pair
    # would leave it held until its origin expires.
    prev = await tl_db.get_latest_attempt(thread_id)
    if prev is None or not (
        prev.get("status") == "interrupted" or is_restart_interrupted(prev)
    ):
        return {}
    meta = prev.get("metadata") or {}
    if not meta.get("report_back_ptc_thread_id"):
        return {}
    return {
        "report_back_ptc_thread_id": meta["report_back_ptc_thread_id"],
        "origin_dispatch_gen": meta.get("origin_dispatch_gen"),
    }


async def carried_delivery(request, thread_id: str) -> dict | None:
    """The config naming the automation run a resumed or retried turn sends for.

    A turn that sends for a run the messaging service holds (the run's own,
    or the report-back of a hand-off from it) has the run in its config, and
    its run row keeps the stamp (``automation_delivery.turn_metadata``), with
    the earlier runs a report-back of several runs' hand-offs names. The
    request that resumes it after an interrupt, or retries it after a
    failure, is not the server's own and names no run, so the turn takes the
    run from the attempt it continues. A message past the interrupt is a new
    turn and sends for nothing, and a start the service didn't take named no
    run for the turn to send for. A run a restart stopped is continued by its
    retry or by the server's own next turn, which takes its firing over too
    (``carried_firing``).
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.services import automation_delivery

    # A read failure fails the turn start, as the pair's does: a turn resumed
    # without the run would send as if it had none.
    retry_of = getattr(request, "retry_of_run_id", None)
    if retry_of:
        prev = await tl_db.get_run(retry_of)
        continues = prev is not None and (
            prev.get("status") == "error" or is_restart_interrupted(prev)
        )
    elif getattr(request, "hitl_response", None):
        prev = await tl_db.get_latest_attempt(thread_id)
        continues = prev is not None and prev.get("status") == "interrupted"
    elif getattr(request, "query_type", None) == "system":
        prev = await tl_db.get_latest_attempt(thread_id)
        continues = is_restart_interrupted(prev)
    else:
        return None
    if not continues:
        return None
    meta = prev.get("metadata") or {}
    run = automation_delivery.read_stamp(meta.get(automation_delivery.DELIVERY_KEY))
    if run is None or not run.held:
        return None
    earlier = automation_delivery.read_stamps(meta.get(automation_delivery.EARLIER_KEY))
    return automation_delivery.turn_configurable(run, earlier)


async def carried_firing(request, thread_id: str) -> dict:
    """START metadata naming the automation firing of a turn a restart
    stopped, which settled nothing, for the turn that takes it over: its
    retry, or the server's own next turn, the resume or one that took the
    resume's place. The firing settles by this turn's ending instead, as by
    its own run's, and sends what ``carried_delivery`` stamps. A user's
    message past the stop is a turn of their own."""
    from src.server.database.runs import lifecycle as tl_db

    retry_of = getattr(request, "retry_of_run_id", None)
    if retry_of:
        prev = await tl_db.get_run(retry_of)
    elif getattr(request, "query_type", None) == "system":
        prev = await tl_db.get_latest_attempt(thread_id)
    else:
        return {}
    if not is_restart_interrupted(prev):
        return {}
    meta = prev.get("metadata") or {}
    if not (meta.get("automation_execution_id") and meta.get("automation_id")):
        return {}
    return {key: meta[key] for key in ("automation_execution_id", "automation_id")}
