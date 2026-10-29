"""063 respells Shanghai both ways; the downgrade must hand the previous code ``.SS``."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "063_shanghai_display_spelling.py"
)


@pytest.fixture
def migration(monkeypatch):
    spec = importlib.util.spec_from_file_location("migration_063", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _statements(op) -> list[str]:
    return [" ".join(str(c.args[0]).split()) for c in op.execute.call_args_list]


def test_downgrade_respells_sh_back_to_ss(migration):
    module, op = migration
    module.downgrade()

    statements = _statements(op)
    updates = [s for s in statements if s.startswith("UPDATE")]
    assert len(updates) == 4  # watchlist, holdings, charts, price triggers
    for sql in updates:
        assert r"'\.SH$', '.SS'" in sql
        assert "<> CASE WHEN upper(" in sql  # only rows whose spelling changes
    # Collisions merge into the .SS row the same way the upgrade merges into .SH.
    assert sum(r"~ '\.SS$'" in s for s in statements) == 3
    # Hong Kong stays at four digits: the previous build resolves it.
    assert not any(".HK" in s for s in statements)


def test_the_downgrade_is_the_upgrade_spelled_backwards(migration, monkeypatch):
    """Merging rules are shared, so a rollback changes spellings and nothing the
    upgrade would not; only collisions written after the upgrade merge. The
    upgrade's Hong Kong pass has no counterpart."""
    module, upgrade_op = migration
    module.upgrade()
    downgrade_op = MagicMock()
    monkeypatch.setattr(module, "op", downgrade_op)
    module.downgrade()

    swapped = [
        s.replace(".SS", "\0").replace(".SH", ".SS").replace("\0", ".SH")
        for s in _statements(upgrade_op)
    ]
    shanghai = len(_statements(downgrade_op))
    assert _statements(downgrade_op) == swapped[:shanghai]
    assert all(".HK" in s for s in swapped[shanghai:] if s.startswith(("UPDATE", "WITH")))
