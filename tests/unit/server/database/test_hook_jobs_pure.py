"""The hook-job decision table stays importable without the DB pool.

It runs inside the finalize transaction and must never do I/O; keeping it
pool-free is the structural guarantee, not a convention. A future import of
``database.pool`` (directly, or via a package root that re-exports the SQL
module) would let a psycopg pool be constructed by merely reading the table.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]

_PROBE = """
import sys
import src.server.database.runs.hook_jobs as hj

leaked = sorted(
    m for m in sys.modules
    if m in ("src.server.database.pool", "psycopg_pool")
)
assert not leaked, leaked
assert hj.build_finalize_jobs is not None
print("ok")
"""


def test_hook_jobs_imports_without_the_pool_module():
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout


def test_outbox_still_re_exports_the_table():
    """The split is invisible to call sites: every existing
    ``from ...runs.outbox import build_finalize_jobs`` keeps resolving."""
    from src.server.database.runs import hook_jobs, outbox

    for name in (
        "HookJob",
        "UserFeedPayload",
        "build_finalize_jobs",
        "build_finalize_jobs_from_run_row",
    ):
        assert getattr(outbox, name) is getattr(hook_jobs, name)


def test_runs_package_root_re_exports_only_the_pure_table():
    from src.server.database import runs
    from src.server.database.runs import hook_jobs

    assert runs.build_finalize_jobs is hook_jobs.build_finalize_jobs
    # The SQL modules stay behind their own import — re-exporting them here
    # would defeat the pool-free guarantee above.
    assert not hasattr(runs, "enqueue_hooks")
