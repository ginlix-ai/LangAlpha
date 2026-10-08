"""What a turn takes over from the turn before it: the report-back pair of an
interrupted summary, and the automation run a resumed or retried turn sends
for."""

from __future__ import annotations


async def carried_pair(request, thread_id: str) -> dict:
    """START metadata naming the pair this turn releases for the summary before it.

    A summary that ends interrupted (an approval card, a question) is waiting on
    the user and releases nothing, and the turn that answers it is a public
    request stripped of the pair. So whichever turn follows on the thread, a
    resume or a message past the interrupt, stamps the summary's pair: it
    releases it when it ends and carries it on when it is interrupted too. The
    dispatch generation travels with it, so the release spares a pair
    dispatched again since. A summary names its own pair and takes none.
    """
    if getattr(request, "report_back_ptc_thread_id", None):
        return {}
    from src.server.database.runs import lifecycle as tl_db

    # A read failure fails the turn start: a turn stamped without the pair
    # would leave it held until its origin expires.
    prev = await tl_db.get_latest_attempt(thread_id)
    if prev is None or prev.get("status") != "interrupted":
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
    its run row keeps the stamp (``automation_delivery.turn_metadata``). The
    request that resumes it after an interrupt, or retries it after a
    failure, is not the server's own and names no run, so the turn takes the
    run from the attempt it continues. A message past the interrupt is a new
    turn and sends for nothing, and a start the service didn't take named no
    run for the turn to send for.
    """
    retry_of = getattr(request, "retry_of_run_id", None)
    if not (retry_of or getattr(request, "hitl_response", None)):
        return None
    from src.server.database.runs import lifecycle as tl_db
    from src.server.services import automation_delivery

    # A read failure fails the turn start, as the pair's does: a turn resumed
    # without the run would send as if it had none.
    if retry_of:
        prev, continued = await tl_db.get_run(retry_of), "error"
    else:
        prev, continued = await tl_db.get_latest_attempt(thread_id), "interrupted"
    if prev is None or prev.get("status") != continued:
        return None
    meta = prev.get("metadata") or {}
    run = automation_delivery.read_stamp(meta.get(automation_delivery.DELIVERY_KEY))
    if run is None or not run.held:
        return None
    return automation_delivery.turn_configurable(run)
