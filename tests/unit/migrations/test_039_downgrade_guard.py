"""The 039 downgrade drops the only reference a moved row has to its bytes."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "039_workspace_file_blobs.py"
)


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location("migration_039", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def test_downgrade_refuses_while_any_row_points_into_storage(migration):
    module, op = migration
    op.get_bind.return_value.execute.return_value.scalar.return_value = 3

    with pytest.raises(RuntimeError, match=r"--reverse --apply"):
        module.downgrade()

    op.execute.assert_not_called()
    op.get_context.assert_not_called()


def test_downgrade_refuses_while_any_workspace_restore_is_incomplete(migration):
    """With every row inline the flag is the only prune gate a flagged workspace
    has left; dropping it lets the next sync prune the files that never arrived."""
    module, op = migration
    op.get_bind.return_value.execute.return_value.scalar.side_effect = [0, 2]

    with pytest.raises(RuntimeError, match="files_restore_incomplete_at"):
        module.downgrade()

    op.execute.assert_not_called()


def test_downgrade_runs_once_every_row_is_inline_again(migration):
    module, op = migration
    op.get_bind.return_value.execute.return_value.scalar.return_value = 0

    module.downgrade()

    dropped = [" ".join(str(c.args[0]).split()) for c in op.execute.call_args_list]
    assert "DROP TABLE IF EXISTS workspace_file_blobs" in dropped


def test_downgrade_locks_the_tables_before_it_counts(migration):
    """A count taken outside the lock is a snapshot: a still-running worker can
    publish a blob-backed row right after it, and the column drop that follows
    would strand that file's bytes. The lock has to be held through the DDL,
    so there is no autocommit block to commit it away."""
    module, op = migration
    bind = op.get_bind.return_value
    bind.execute.return_value.scalar.return_value = 0

    module.downgrade()

    statements = [" ".join(str(c.args[0]).split()) for c in bind.execute.call_args_list]
    lock = next(i for i, s in enumerate(statements) if s.startswith("LOCK TABLE"))
    first_count = next(i for i, s in enumerate(statements) if s.startswith("SELECT"))
    assert lock < first_count
    # Session-level SET would outlive this transaction and cap every later
    # migration on the same connection.
    assert any(s.startswith("SET LOCAL lock_timeout") for s in statements)
    assert "workspace_files" in statements[lock] and "workspaces" in statements[lock]
    op.get_context.assert_not_called()
