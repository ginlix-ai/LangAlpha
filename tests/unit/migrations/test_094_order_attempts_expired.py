"""094 added ``expired`` to the ``order_attempts`` status CHECK, a copy of ``AttemptStatus``.

094 is the migration that last wrote the CHECK. A state added to the enum
without a migration surfaces as a constraint violation on the order path, where
a failed write strands the attempt in ``submitting``; the next migration that
writes the CHECK moves this test's enum assertion to its own.
"""

from __future__ import annotations

import importlib.util
import re
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.server.services.brokerage_orders.models import AttemptStatus

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
    module = _load("migration_094", "094_order_attempts_expired.py")
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


def _alter(events: list[str]) -> int:
    return next(i for i, e in enumerate(events) if "ADD CONSTRAINT" in e)


def _allowed(statement: str) -> set[str]:
    match = re.search(r"CHECK \(status IN \(([^)]*)\)\) NOT VALID", statement)
    assert match, statement
    return set(re.findall(r"'([a-z_]+)'", match.group(1)))


def test_it_follows_093_in_a_linear_chain(migration):
    module, _events = migration
    assert (module.revision, module.down_revision) == ("094", "093")


def test_the_check_constraint_names_every_attempt_status(migration):
    module, events = migration

    module.upgrade()

    assert _allowed(events[_alter(events)]) == {s.value for s in AttemptStatus}


def test_the_downgrade_restores_exactly_what_086_allowed(migration):
    module, events = migration
    previous = _load("migration_086", "086_order_attempts_handed_off.py")

    module.downgrade()

    restored = _allowed(events[_alter(events)])
    assert restored == set(previous._STATUSES)
    assert "expired" not in restored


@pytest.mark.parametrize("step", ["upgrade", "downgrade"])
def test_the_check_is_validated_outside_the_lock_that_adds_it(migration, step):
    module, events = migration

    getattr(module, step)()

    alter = _alter(events)
    assert events.count(_BEGIN) == 1
    begin, commit = events.index(_BEGIN), events.index(_COMMIT)
    assert events[0] == "SET LOCAL lock_timeout = '5s'"
    assert alter < begin
    assert "DROP CONSTRAINT IF EXISTS order_attempts_status_check" in events[alter]
    assert events[begin + 1 : commit] == ["SET lock_timeout = 0", _VALIDATE]
    assert commit == len(events) - 1


def test_the_downgrade_reopens_expired_rows_before_the_check_refuses_them(
    migration,
):
    module, events = migration

    module.downgrade()

    reopen = next(i for i, e in enumerate(events) if e.startswith("UPDATE"))
    check = _alter(events)
    assert reopen < check
    assert "SET status = 'submitted', completed_at = NULL" in events[reopen]
    assert "WHERE status = 'expired'" in events[reopen]
    assert "'expired'" not in events[check]
