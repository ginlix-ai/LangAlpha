"""146: the downgrade would show every private thread to its workspace, and
the upgrade must not validate the sharer's key under the lock that adds it,
since every turn writes ``conversation_threads``."""

from __future__ import annotations

import importlib.util
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "146_shared_workspace_thread_visibility.py"
)


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location("migration_146", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    monkeypatch.delenv(module.ALLOW_ENV, raising=False)
    return module, op


def _private(op, threads: int, workspaces: int = 1) -> None:
    op.get_bind.return_value.execute.return_value.one.return_value = (threads, workspaces)


def _dropped(op) -> bool:
    return any("DROP COLUMN" in str(c.args[0]) for c in op.execute.call_args_list)


def test_downgrade_refuses_while_a_thread_is_private_and_says_how_many(migration):
    module, op = migration
    _private(op, 4, 2)

    with pytest.raises(RuntimeError, match=r"4 thread\(s\) in 2 shared workspace\(s\)") as e:
        module.downgrade()

    assert module.ALLOW_ENV in str(e.value)
    assert not _dropped(op)


def test_downgrade_runs_with_none_private_or_when_the_operator_accepts_it(migration, monkeypatch):
    module, op = migration
    _private(op, 0, 0)
    module.downgrade()
    assert _dropped(op)

    op.reset_mock()
    _private(op, 4, 2)
    monkeypatch.setenv(module.ALLOW_ENV, "1")
    module.downgrade()
    assert _dropped(op)


def test_downgrade_counts_under_the_lock_it_drops_under(migration):
    module, op = migration
    _private(op, 0, 0)
    module.downgrade()

    bind = op.get_bind.return_value
    statements = [" ".join(str(c.args[0]).split()) for c in bind.execute.call_args_list]
    lock = next(i for i, s in enumerate(statements) if s.startswith("LOCK TABLE conversation_threads"))
    count = next(i for i, s in enumerate(statements) if s.startswith("SELECT count(*)"))
    assert statements[0].startswith("SET LOCAL lock_timeout") and lock < count


def test_downgrade_drops_the_sharers_key_with_its_column(migration):
    module, op = migration
    _private(op, 0, 0)
    module.downgrade()

    drop = next(str(c.args[0]) for c in op.execute.call_args_list if "DROP COLUMN" in str(c.args[0]))
    assert f"DROP CONSTRAINT IF EXISTS {module._FOREIGN_KEY}" in drop


_BEGIN, _COMMIT = "<autocommit>", "</autocommit>"


@pytest.fixture
def upgrade_events(migration) -> list[str]:
    """The upgrade's statements in order, with the autocommit block marked."""
    module, op = migration
    events: list[str] = []

    @contextmanager
    def autocommit_block():
        events.append(_BEGIN)
        yield
        events.append(_COMMIT)

    op.get_context.return_value.autocommit_block = autocommit_block
    op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))
    module.upgrade()
    return events


def test_the_sharers_key_is_validated_outside_the_lock_that_adds_it(migration, upgrade_events):
    module, _op = migration
    events = upgrade_events
    key = module._FOREIGN_KEY
    add = next(i for i, e in enumerate(events) if f"ADD CONSTRAINT {key}" in e)
    begin, commit = events.index(_BEGIN), events.index(_COMMIT)

    assert events[0] == "SET LOCAL lock_timeout = '5s'"
    assert events.count(_BEGIN) == 1 and add < begin
    # Re-runnable after an abort between the add and the validation.
    assert f"DROP CONSTRAINT IF EXISTS {key}" in events[add]
    assert events[add].endswith("ON DELETE SET NULL NOT VALID")
    assert not any("VALIDATE" in e for e in events[:begin])
    assert events[begin + 1 : commit] == [
        "SET lock_timeout = 0",
        f"ALTER TABLE conversation_threads VALIDATE CONSTRAINT {key}",
    ]
    assert commit == len(events) - 1


def test_the_columns_come_without_a_key_that_would_scan_under_the_lock(upgrade_events):
    columns = next(e for e in upgrade_events if "ADD COLUMN" in e)
    assert "ADD COLUMN IF NOT EXISTS workspace_shared_at" in columns
    assert "ADD COLUMN IF NOT EXISTS workspace_shared_by" in columns
    assert "REFERENCES" not in columns


def test_the_backfill_records_no_sharer_for_a_starter_with_no_account(
    migration, upgrade_events
):
    module, _op = migration
    events = upgrade_events
    add = next(i for i, e in enumerate(events) if f"ADD CONSTRAINT {module._FOREIGN_KEY}" in e)
    backfill = next(i for i, e in enumerate(events) if e.startswith("UPDATE conversation_threads"))
    # Written after the key exists, which checks what it writes.
    assert add < backfill < events.index(_BEGIN)
    assert (
        "workspace_shared_by = ( SELECT u.user_id FROM users u WHERE u.user_id = t.created_by)"
        in events[backfill]
    )
    assert "AND t.workspace_shared_at IS NULL" in events[backfill]
