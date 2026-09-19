"""What 047's one index has to keep true.

Every assertion here guards a property a plausible tidy-up would drop. Without
``NULLS NOT DISTINCT`` the index gives no uniqueness at all to the kinds whose
``connection_id`` is NULL. Without the ``computer_id IS NOT NULL`` predicate a
flash workspace and 046's backfill losers, which all carry NULL there, collapse
into one index entry and the migration cannot even build. And 025's
workspace-keyed constraint has to survive this release, because it is the guard
for exactly that NULL tail.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"
_INDEX = "idx_sandbox_egress_grants_computer_conn"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def migration(monkeypatch):
    module = _load("migration_047", _VERSIONS / "047_egress_computer_uniqueness.py")
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _flat(op) -> str:
    return re.sub(
        r"\s+", " ", " ".join(str(c.args[0]) for c in op.execute.call_args_list)
    )


def _upgrade_sql(migration) -> str:
    module, op = migration
    module.upgrade()
    return _flat(op)


def test_it_follows_044_in_a_linear_chain(migration):
    module, _op = migration
    assert module.revision == "047"
    assert module.down_revision == "046"


def test_the_grant_set_is_unique_per_machine(migration):
    sql = _upgrade_sql(migration)
    assert (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX} "
        "ON sandbox_egress_grants (computer_id, kind, connection_id, server_name)" in sql
    )


def test_two_null_connections_are_the_same_connection(migration):
    """connection_id is NULL for every kind but oauth_mcp and server_name for
    every kind but header_mcp; the default NULLS DISTINCT would leave those
    rows with no uniqueness at all."""
    sql = _upgrade_sql(migration)
    assert "NULLS NOT DISTINCT" in sql


def test_the_index_is_partial_on_a_machine_existing(migration):
    """A flash workspace has no computer, and 046's backfill left the losers of
    a shared sandbox_id with none either. Both keep the workspace-keyed path."""
    sql = _upgrade_sql(migration)
    assert "WHERE computer_id IS NOT NULL" in sql


def test_no_row_is_rewritten(migration):
    """Deduplication belongs to the consolidation job, under the machine's
    lock: a migration that revoked rows would do it without one."""
    sql = _upgrade_sql(migration)
    for verb in ("UPDATE ", "DELETE ", "INSERT "):
        assert verb not in sql.upper()


def test_045s_workspace_constraint_is_left_alone(migration):
    """It is this release's guard for the NULL tail the new index excludes."""
    module, op = migration
    module.upgrade()
    module.downgrade()
    sql = _flat(op).upper()
    assert "DROP CONSTRAINT" not in sql
    assert "WORKSPACE_ID, KIND, CONNECTION_ID" not in sql
    assert "SCOPE_KEY" not in sql


def test_the_downgrade_drops_only_the_index(migration):
    module, op = migration
    module.downgrade()
    sql = _flat(op)
    assert sql.strip() == f"DROP INDEX IF EXISTS {_INDEX}"


def test_it_touches_no_index_but_its_own(migration):
    """046's plain ``idx_sandbox_egress_grants_computer`` stays: it answers
    lookups, and it is Phase 4 that retires it as redundant with this one."""
    module, op = migration
    module.upgrade()
    module.downgrade()
    assert set(re.findall(r"idx_\w+", _flat(op))) == {_INDEX}
