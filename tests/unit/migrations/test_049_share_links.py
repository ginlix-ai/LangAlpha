"""049's DDL, pinned as SQL.

The row shape is what keeps a link honest: an app never carries a shared
state or a file list, and a file link that is shared names exactly which
paths a visitor may fetch, entry path included. The per-item unique indexes
are what the link writes' ON CONFLICT clauses land on, and the
grant key has to survive a re-run without being re-minted, or every grant in
flight would stop verifying.
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
    module = _load("migration_049", _VERSIONS / "049_share_links.py")
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _flat(op) -> str:
    return re.sub(
        r"\s+", " ", " ".join(str(c.args[0]) for c in op.execute.call_args_list)
    )


def test_it_follows_048_in_a_linear_chain(migration):
    module, _op = migration
    assert module.revision == "049"
    assert module.down_revision == "048"


def test_a_file_link_has_a_path_and_no_port(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert (
        "CONSTRAINT share_links_file_shape CHECK ( "
        "kind <> 'file' OR (path IS NOT NULL AND port IS NULL) )" in sql
    )


def test_an_app_link_has_a_user_port_and_is_never_shared(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert (
        "CONSTRAINT share_links_app_shape CHECK ( kind <> 'app' "
        "OR (port BETWEEN 3000 AND 9999 AND shared_at IS NULL AND files IS NULL) )"
        in sql
    )


def test_a_shared_link_freezes_a_file_list_that_carries_its_own_path(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert (
        "CONSTRAINT share_links_shared_files CHECK ( shared_at IS NULL "
        "OR (files IS NOT NULL AND cardinality(files) > 0 AND path = ANY(files)) )"
        in sql
    )


def test_one_link_per_item_is_enforced_by_partial_unique_indexes(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_share_links_file "
        "ON share_links (workspace_id, path) WHERE kind = 'file'" in sql
    )
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_share_links_app "
        "ON share_links (workspace_id, port) WHERE kind = 'app'" in sql
    )


def test_the_grant_key_is_minted_once_and_kept_on_rerun(migration):
    module, op = migration
    module.upgrade()
    sql = _flat(op)
    assert "CREATE TABLE IF NOT EXISTS server_keys" in sql
    assert (
        "INSERT INTO server_keys (name, secret) "
        "VALUES ('file_grant', gen_random_bytes(32)) "
        "ON CONFLICT (name) DO NOTHING" in sql
    )
    assert "SET LOCAL lock_timeout" in sql


def test_the_downgrade_drops_both_tables(migration):
    module, op = migration
    module.downgrade()
    sql = _flat(op)
    assert "DROP TABLE IF EXISTS share_links" in sql
    assert "DROP TABLE IF EXISTS server_keys" in sql
