"""122 adds nullable columns and backfills only what history already settles.

The previous release keeps serving against the migrated schema until the
swap, and it names none of the new columns, so the upgrade may add and fill
them but must alter nothing it reads or writes. The backfill statements are
pinned here as written; the hold rules the sweep settles every other run by
are pinned against Postgres in ``tests/integration/test_computer_runs_db.py``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"
_NEW_COLUMNS = (
    "size_source",
    "archive_after_s",
    "held_until",
    "held_end",
    "allocation_checked_at",
    "seen_down_at",
)


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "migration_122", _VERSIONS / "122_computer_run_allocation.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events: list[str] = []
    op = MagicMock()
    op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))
    monkeypatch.setattr(module, "op", op)
    return module, events


def test_it_follows_121_in_a_linear_chain(migration):
    module, _events = migration
    assert (module.revision, module.down_revision) == ("122", "121")


def test_the_upgrade_adds_nullable_columns_and_fills_only_them(migration):
    module, events = migration

    module.upgrade()

    assert events[0] == "SET LOCAL lock_timeout = '5s'"
    alter = next(e for e in events if e.startswith("ALTER TABLE"))
    assert alter.count("ADD COLUMN IF NOT EXISTS") == len(_NEW_COLUMNS)
    assert "NOT NULL" not in alter and "DEFAULT" not in alter
    assert not any("DROP" in e for e in events)
    assert [e for e in events if e.startswith("UPDATE")] == [
        # A delete's hold may run on past a lost stop, so only a replacement
        # is settled here.
        "UPDATE computer_runs SET held_until = ended_at, held_end = 'deleted'"
        " WHERE ended_at IS NOT NULL AND held_until IS NULL"
        " AND end_reason = 'replaced'",
        "UPDATE computer_runs SET size_source = 'tier'"
        " WHERE provider_kind = 'docker' AND size_source IS NULL",
    ]


def test_the_downgrade_drops_only_what_the_upgrade_added(migration):
    module, events = migration

    module.downgrade()

    dropped = " ".join(events)
    for column in _NEW_COLUMNS:
        assert f"DROP COLUMN IF EXISTS {column}" in dropped
    assert "DROP TABLE" not in dropped
