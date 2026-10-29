"""085 commits each statement on its own, under its own lock timeout.

In one transaction, the ACCESS EXCLUSIVE lock an ALTER takes on ``computers``
stays held while the next ALTER waits on ``workspaces``, and every read of
``computers`` queues behind it for that whole wait.
"""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "migration_085", _VERSIONS / "085_owner_file_backup.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    events: list[str] = []

    @contextmanager
    def autocommit_block():
        events.append("begin")
        yield
        events.append("commit")

    op = MagicMock()
    op.get_context.return_value.autocommit_block = autocommit_block
    op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))
    monkeypatch.setattr(module, "op", op)
    return module, events


def test_it_follows_084_in_a_linear_chain(migration):
    module, _events = migration
    assert (module.revision, module.down_revision) == ("085", "084")


@pytest.mark.parametrize("step", ["upgrade", "downgrade"])
def test_each_statement_commits_on_its_own_under_its_own_lock_timeout(migration, step):
    module, events = migration

    getattr(module, step)()

    statements = [e for e in events if e not in ("begin", "commit") and "lock_timeout" not in e]
    assert len(statements) == 4
    assert events == [
        part
        for statement in statements
        for part in ("begin", "SET lock_timeout = '5s'", statement, "RESET lock_timeout", "commit")
    ]
    # Safe to run again after an upgrade or a rollback cut short.
    for statement in statements:
        clauses = statement.count("ADD COLUMN") + statement.count("DROP COLUMN")
        clauses += statement.count("CREATE INDEX") + statement.count("DROP INDEX")
        guarded = statement.count("IF NOT EXISTS") + statement.count("IF EXISTS")
        assert guarded == clauses, statement


def test_the_claims_column_comes_and_goes_with_the_hold(migration):
    module, events = migration

    module.upgrade()
    assert any(
        "ADD COLUMN IF NOT EXISTS left_folder_tried_at TIMESTAMPTZ" in e for e in events
    )

    events.clear()
    module.downgrade()
    assert any("DROP COLUMN IF EXISTS left_folder_tried_at" in e for e in events)
