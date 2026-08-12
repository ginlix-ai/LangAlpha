"""Run-scoped persistence: lifecycle CAS, hook outbox, subagent run ledger.

Only the pure hook-job decision table is re-exported here — the SQL modules
stay behind their own imports so this package root never drags the connection
pool into a caller that only wants the job shapes.
"""

from src.server.database.runs.hook_jobs import (
    BurstReleasePayload,
    HookJob,
    NeedsInputWakePayload,
    ReportBackPayload,
    UserFeedPayload,
    WatchClearPayload,
    build_finalize_jobs,
    build_finalize_jobs_from_run_row,
)

__all__ = [
    "BurstReleasePayload",
    "HookJob",
    "NeedsInputWakePayload",
    "ReportBackPayload",
    "UserFeedPayload",
    "WatchClearPayload",
    "build_finalize_jobs",
    "build_finalize_jobs_from_run_row",
]
