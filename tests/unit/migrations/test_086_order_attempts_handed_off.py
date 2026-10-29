"""086 added ``handed_off`` to the ``order_attempts`` status CHECK.

094 is the migration that last wrote the CHECK, and its test holds it to
``AttemptStatus``; this one holds 086 to the list 094 starts from. The CHECK
goes in NOT VALID and is validated after the migration's transaction commits: a
plain ADD CONSTRAINT scans the whole ledger under the lock every order write
waits on.
"""

from __future__ import annotations

import importlib.util
import re
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"
_BEGIN, _COMMIT = "<autocommit>", "</autocommit>"
_VALIDATE = (
    "ALTER TABLE order_attempts VALIDATE CONSTRAINT order_attempts_status_check"
)


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, _VERSIONS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def migration(monkeypatch):
    module = _load("migration_086", "086_order_attempts_handed_off.py")
    events: list[str] = []

    @contextmanager
    def autocommit_block():
        events.append(_BEGIN)
        yield
        events.append(_COMMIT)

    op = MagicMock()
    op.get_context.return_value.autocommit_block = autocommit_block
    op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))
    monkeypatch.setattr(module, "op", op)
    return module, events


def _find(events: list[str], prefix: str) -> int:
    return next(i for i, e in enumerate(events) if e.startswith(prefix))


def _alter(events: list[str]) -> int:
    return next(i for i, e in enumerate(events) if "ADD CONSTRAINT" in e)


def _allowed(statement: str) -> set[str]:
    match = re.search(r"CHECK \(status IN \(([^)]*)\)\) NOT VALID", statement)
    assert match, statement
    return set(re.findall(r"'([a-z_]+)'", match.group(1)))


def test_it_follows_085_in_a_linear_chain(migration):
    module, _events = migration
    assert (module.revision, module.down_revision) == ("086", "085")


def test_the_check_constraint_names_every_status_094_starts_from(migration):
    module, events = migration
    latest = _load("migration_094", "094_order_attempts_expired.py")

    module.upgrade()

    assert _allowed(events[_alter(events)]) == set(latest._BEFORE)


def test_the_downgrade_restores_exactly_what_043_allowed(migration):
    module, events = migration
    original = _load("migration_043", "043_order_attempts.py")

    module.downgrade()

    restored = _allowed(events[_alter(events)])
    assert restored == set(original._STATUSES)
    assert "handed_off" not in restored


@pytest.mark.parametrize("step", ["upgrade", "downgrade"])
def test_the_check_is_validated_outside_the_lock_that_adds_it(migration, step):
    module, events = migration

    getattr(module, step)()

    alter = _alter(events)
    assert events.count(_BEGIN) == 1
    begin, commit = events.index(_BEGIN), events.index(_COMMIT)
    # The ALTER waits at most 5s for ACCESS EXCLUSIVE, inside the transaction.
    assert events[0] == "SET LOCAL lock_timeout = '5s'"
    assert alter < begin
    assert "DROP CONSTRAINT IF EXISTS order_attempts_status_check" in events[alter]
    # The scan blocks no reads or writes, so it runs after commit with no cap.
    assert events[begin + 1 : commit] == ["SET lock_timeout = 0", _VALIDATE]
    assert commit == len(events) - 1


def test_unlisted_at_comes_and_goes_with_the_state(migration):
    module, events = migration

    module.upgrade()
    # Nullable, no default: NULL is an order last seen listed.
    assert "ADD COLUMN IF NOT EXISTS unlisted_at TIMESTAMPTZ," in events[_alter(events)]

    events.clear()
    module.downgrade()
    assert "DROP COLUMN IF EXISTS unlisted_at," in events[_alter(events)]


def test_the_downgrade_reopens_handed_off_rows_before_the_check_refuses_them(
    migration,
):
    module, events = migration

    module.downgrade()

    reopen = _find(events, "UPDATE")
    check = _alter(events)
    assert reopen < check
    assert "SET status = 'submitted', completed_at = NULL" in events[reopen]
    assert "WHERE status = 'handed_off'" in events[reopen]
    assert "'handed_off'" not in events[check]
