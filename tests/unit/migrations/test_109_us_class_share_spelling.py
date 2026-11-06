"""109 respells a dotted US class share dashed with 063's merging statements;
its downgrade leaves the rows as they are, which the previous build reads."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "109_us_class_share_spelling.py"
)


@pytest.fixture
def migration(monkeypatch):
    op = MagicMock()
    monkeypatch.setattr("alembic.op", op)
    spec = importlib.util.spec_from_file_location("migration_109", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, op


def _statements(op) -> list[str]:
    return [" ".join(str(c.args[0]).split()) for c in op.execute.call_args_list]


def test_upgrade_dashes_a_dotted_class_share_and_merges_its_collisions(migration):
    module, op = migration
    module.upgrade()

    statements = _statements(op)
    updates = [s for s in statements if s.startswith("UPDATE")]
    assert [s.split()[1] for s in updates] == [
        "watchlist_items", "user_portfolios", "chart_annotations", "automations",
        "market_watch_targets", "wakeups",
    ]
    # Armed targets on both spellings would collide on the armed-kind index.
    cancel = next(s for s in statements if "SET status = 'cancelled'" in s)
    assert statements.index(cancel) < statements.index(updates[-2])
    for sql in updates:
        assert r"~ '^[A-Z]+\.[ABC]$'" in sql
        assert r"'\.([ABC])$', '-\1'" in sql
    # Collisions with a row already dashed merge as 063 merges them, and armed
    # targets on both spellings keep the newest.
    assert sum(r"~ '^[A-Z]+-[ABC]$'" in s for s in statements) == 4


def test_downgrade_changes_nothing(migration):
    module, op = migration
    module.downgrade()
    op.execute.assert_not_called()
