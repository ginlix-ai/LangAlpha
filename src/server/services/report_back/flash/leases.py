"""Retry-budget and lease timing for flash report-back delivery.

Every bound here derives from the longest legitimate server-side hold a
dispatched POST can experience, so drop deadlines can never land while the
route is still inside a lawful pre-START wait.
"""

from src.config.settings import get_workflow_timeout


def admission_hold_bound() -> float:
    """Longest legitimate server-side pre-START hold of one dispatched POST.

    ``wait_for_admission`` runs its waits SEQUENTIALLY in one call — the
    compaction backstop first, then (when the freed slot carries cancel
    intent) the stop-drain — so the bound is their SUM, not their max
    (round-4 F1: a cancel landing near a compaction window's close chains
    both). Margins come straight from ``runs.admission`` (the mirror the
    old BTM-circularity forced is gone); a unit pin still guards the
    sequential composition against drift."""
    from src.config.settings import (
        get_admission_compaction_wait_timeout,
        get_checkpoint_flush_timeout,
        get_compaction_timeout,
    )
    from src.server.services.runs.admission import (
        ADMISSION_TEARDOWN_MARGIN_S,
        COMPACTION_ADMISSION_MARGIN_S,
    )

    stopping = get_checkpoint_flush_timeout() + ADMISSION_TEARDOWN_MARGIN_S
    compaction = max(
        get_admission_compaction_wait_timeout(),
        get_compaction_timeout() + COMPACTION_ADMISSION_MARGIN_S,
    )
    return compaction + stopping


# Response/backoff slack per admission attempt (30s sock-read + 5s backoff + margin).
RB_ADMISSION_MARGIN_S = 60.0

# Cap (seconds) on retrying a 409 (flash thread busy with the user's own turn)
# for one item; derived from the workflow timeout so a long user turn is
# waited out, FLOORED so the budget structurally fits the priming lease plus
# one full post-takeover admission attempt (round-3 F1): the drop deadline
# must never land while the route is still inside a legitimate pre-START hold
# entered after takeover became legal — the admission holds derive from
# DIFFERENT config knobs than workflow_timeout, so no timeout value may be
# trusted to cover them.
RB_BUSY_WAIT_CAP = max(
    float(get_workflow_timeout()),
    2.0 * (admission_hold_bound() + RB_ADMISSION_MARGIN_S),
)

# Cap (seconds) on waiting for a POSTed report-back to reach terminal before
# force-clearing it, so a crashed run can't wedge the whole flash queue.
RB_TERMINAL_WAIT_CAP = RB_BUSY_WAIT_CAP


def derive_priming_lease(retry_budget: float) -> float:
    """Priming lease on a run pointer whose run has NO ledger row yet.

    Half the (floored) budget: the lease itself covers the longest legitimate
    pre-START admission wait, and the remaining half guarantees post-takeover
    retries — one full admission hold plus slack — before the drop deadline.
    A rowless crashed pointer therefore always becomes takeover-eligible AND
    completable while 503 retries are still coming; the cost of a small lease
    (takeover racing an unusually slow priming) is bounded by the per-thread
    in_progress slot — one live run per flash thread.
    """
    return min(900.0, retry_budget / 2)


# An incumbent pointer younger than this may still be mid-priming (admission
# wait + START txn) on another worker: retries defer instead of adopting or
# taking over.
RB_POINTER_PRIMING_LEASE_S = derive_priming_lease(RB_BUSY_WAIT_CAP)
