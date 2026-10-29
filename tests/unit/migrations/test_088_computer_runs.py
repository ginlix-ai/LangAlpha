"""088 only adds: a table the previous build never touches, and its indexes.

The previous release keeps serving against the migrated schema until the
swap, so nothing here may alter what it reads or writes. The one open run
per computer is a partial unique index, which is what makes the open hook
idempotent; the SQL behind it is pinned in
``tests/integration/test_computer_runs_db.py``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "migration_088", _VERSIONS / "088_computer_runs.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events: list[str] = []
    op = MagicMock()
    op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))
    monkeypatch.setattr(module, "op", op)
    return module, events


def test_it_follows_087_in_a_linear_chain(migration):
    module, _events = migration
    assert (module.revision, module.down_revision) == ("088", "087")


def test_the_upgrade_creates_and_never_alters(migration):
    module, events = migration

    module.upgrade()

    assert events[0] == "SET LOCAL lock_timeout = '5s'"
    statements = events[1:]
    assert all(s.startswith(("CREATE TABLE IF NOT EXISTS computer_runs",
                             "CREATE INDEX IF NOT EXISTS",
                             "CREATE UNIQUE INDEX IF NOT EXISTS"))
               for s in statements), statements
    assert all(" ON computer_runs " in s for s in statements[1:])
    assert not any(word in s for s in statements for word in ("ALTER", "DROP"))


def test_a_computer_has_at_most_one_open_run(migration):
    module, events = migration

    module.upgrade()

    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_computer_runs_one_open"
        " ON computer_runs (computer_id) WHERE ended_at IS NULL"
    ) in events


def test_every_metric_column_is_nullable(migration):
    module, events = migration

    module.upgrade()

    table = next(e for e in events if e.startswith("CREATE TABLE"))
    for column in (
        "sample_count INTEGER,", "cpu_avg_pct REAL,", "cpu_p95_pct REAL,",
        "cpu_max_pct REAL,", "mem_peak_bytes BIGINT,",
        "mem_peak_excl_cache_bytes BIGINT,", "mem_total_bytes BIGINT,",
        "disk_peak_bytes BIGINT,", "disk_total_bytes BIGINT,",
        "metrics_through TIMESTAMPTZ,", "metrics_error TEXT,",
    ):
        assert column in table, column


def test_the_downgrade_drops_only_its_table(migration):
    module, events = migration

    module.downgrade()

    assert events == [
        "SET LOCAL lock_timeout = '5s'",
        "DROP TABLE IF EXISTS computer_runs",
    ]
