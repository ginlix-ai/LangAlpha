"""048's DDL and backfill, pinned as SQL.

The claim rows cascade from both parents: a grant row that is deleted takes
its claims with it, and a workspace row that is deleted (not tombstoned, which
is the app's path) takes its own. The backfill has to be idempotent because a
blocked branch is re-applied by hand.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def migration(monkeypatch):
    module = _load("migration_048", _VERSIONS / "048_egress_grant_claims.py")
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _flat(op) -> str:
    return re.sub(
        r"\s+", " ", " ".join(str(c.args[0]) for c in op.execute.call_args_list)
    )


def test_it_follows_047_in_a_linear_chain(migration):
    module, _op = migration
    assert module.revision == "048"
    assert module.down_revision == "047"


def test_the_table_cascades_from_both_parents(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert "CREATE TABLE IF NOT EXISTS sandbox_egress_grant_claims" in sql
    assert "REFERENCES sandbox_egress_grants(grant_id) ON DELETE CASCADE" in sql
    assert "REFERENCES workspaces(workspace_id) ON DELETE CASCADE" in sql
    assert "PRIMARY KEY (grant_id, workspace_id)" in sql


def test_the_backfill_claims_provenance_and_reruns_cleanly(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert (
        "INSERT INTO sandbox_egress_grant_claims (grant_id, workspace_id) "
        "SELECT g.grant_id, g.workspace_id FROM sandbox_egress_grants g "
        "JOIN workspaces w ON w.workspace_id = g.workspace_id "
        "ON CONFLICT DO NOTHING" in sql
    )
    assert "SET LOCAL lock_timeout" in sql


def test_the_downgrade_leaves_the_grant_rows_alone(migration):
    module, op = migration
    module.downgrade()
    sql = _flat(op)
    assert "DROP TABLE IF EXISTS sandbox_egress_grant_claims" in sql
    assert "sandbox_egress_grants " not in sql.replace(
        "sandbox_egress_grant_claims", ""
    )
