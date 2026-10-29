"""The sandbox usage ledger: when metered computers run, and what they use.

``transitions`` opens and closes a computer's run at its status boundaries,
``sweep`` reconciles the ledger and folds provider metrics in, and
``metrics`` is the fold itself. The table is ``computer_runs`` (migration
088), whose docstring has the queries the ledger is for.
"""

from src.server.services.computer_usage.metrics import RunMetrics, fold_samples
from src.server.services.computer_usage.sweep import (
    collect_metrics,
    fold_run,
    fold_before_delete,
    reconcile_runs,
    run_usage_sweep,
)
from src.server.services.computer_usage.transitions import (
    note_computer_status,
    note_sandbox_replaced,
)

__all__ = [
    "RunMetrics",
    "collect_metrics",
    "fold_run",
    "fold_samples",
    "fold_before_delete",
    "note_computer_status",
    "note_sandbox_replaced",
    "reconcile_runs",
    "run_usage_sweep",
]
