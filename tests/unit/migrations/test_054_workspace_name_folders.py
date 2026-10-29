"""054's backfill and index, pinned.

The backfill decides which of a user's same-named workspaces keeps its name,
and it carries a frozen copy of the app's name rules: if the two drift, the
backfill writes keys the app would never compute, and a name the app thinks
is free collides with a row the backfill keyed differently.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.server.database.workspace_names import (
    WorkspaceNameInvalid,
    checked_workspace_name,
    suffixed_name,
    workspace_name_key,
)

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"


def _load():
    spec = importlib.util.spec_from_file_location(
        "migration_054", _VERSIONS / "054_workspace_name_folders.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def migration(monkeypatch):
    module = _load()
    op = MagicMock()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _sql(op) -> str:
    bind = op.get_bind.return_value
    statements = [str(c.args[0]) for c in bind.execute.call_args_list]
    statements += [str(c.args[0]) for c in op.execute.call_args_list]
    return re.sub(r"\s+", " ", " ".join(statements))


def test_it_follows_053_in_a_linear_chain(migration):
    module, _op = migration
    assert (module.revision, module.down_revision) == ("054", "053")


_NAMES = [
    "Research", "  Q3   Earnings ", "Straße", "研究", "a/b", "Q3: Earnings",
    "...", "code", "Tools", ".agents", "x" * 300, "é", "😀" * 64,
]


@pytest.mark.parametrize("name", _NAMES)
def test_the_frozen_rules_key_names_as_the_app_does(migration, name):
    module, _op = migration
    try:
        expected = workspace_name_key(name)
    except WorkspaceNameInvalid:
        expected = None
    assert module._key(name) == expected
    assert module._suffixed(name, " (2)") == suffixed_name(name, " (2)")


@pytest.mark.parametrize("name", _NAMES)
def test_every_planned_name_is_one_the_app_accepts(migration, name):
    """The backfill writes names the app would store, under the app's key."""
    module, _op = migration
    [(_id, planned, key)] = module.plan_names([("a", "u1", name)])
    assert checked_workspace_name(planned) == planned
    assert workspace_name_key(planned) == key


def test_the_oldest_keeps_the_name_and_later_ones_are_numbered(migration):
    module, _op = migration
    plan = module.plan_names(
        [("a", "u1", "Research"), ("b", "u1", "research"), ("c", "u1", "RESEARCH"), ("d", "u2", "Research")]
    )
    assert plan == [
        ("a", "Research", "research"),
        ("b", "research (2)", "research (2)"),
        ("c", "RESEARCH (3)", "research (3)"),
        ("d", "Research", "research"),
    ]


def test_a_number_the_folder_cut_would_drop_still_numbers(migration):
    """A number the byte cut dropped would key as the name, and the walk would not end."""
    module, _op = migration
    [first, second] = module.plan_names([("a", "u1", "😀" * 64), ("b", "u1", "😀" * 64)])
    assert second[1].endswith(" (2)") and second[2] != first[2]


def test_a_number_never_lands_on_a_name_the_user_typed(migration):
    """"Research (2)" is someone's real name even though it is newer than the
    duplicate that would otherwise be numbered into it."""
    module, _op = migration
    plan = module.plan_names(
        [("a", "u1", "Research"), ("b", "u1", "Research"), ("c", "u1", "Research (2)")]
    )
    assert [name for _id, name, _key in plan] == ["Research", "Research (3)", "Research (2)"]


def test_a_long_name_is_cut_and_an_unusable_one_becomes_workspace(migration):
    module, _op = migration
    plan = module.plan_names(
        [("a", "u1", "x" * 90), ("b", "u1", "..."), ("c", "u1", "Workspace")]
    )
    assert plan[0][1] == "x" * 80
    assert [plan[1][1], plan[2][1]] == ["Workspace (2)", "Workspace"]


def test_a_reserved_name_is_numbered_rather_than_lost(migration):
    """"Code" is a folder the computer owns, so the workspace becomes "Code (2)",
    or the first number free; only a name with no folder spelling at all
    falls back to "Workspace"."""
    module, _op = migration
    plan = module.plan_names(
        [("a", "u1", "Code"), ("b", "u1", "Code (2)"), ("c", "u1", "tools"), ("d", "u1", "/")]
    )
    assert [name for _id, name, _key in plan] == ["Code (3)", "Code (2)", "tools (2)", "Workspace"]


def test_the_name_index_is_per_user_over_live_non_flash_rows(migration):
    """A Flash workspace has no folder, and a deleted one hands its name back."""
    module, op = migration
    module.upgrade()
    sql = _sql(op)
    assert (
        "CREATE UNIQUE INDEX CONCURRENTLY idx_workspaces_user_name_key "
        "ON workspaces (user_id, name_key) WHERE status NOT IN ('deleted', 'flash')"
    ) in sql
    assert sql.index("DROP INDEX CONCURRENTLY IF EXISTS idx_workspaces_user_name_key") < sql.index(
        "CREATE UNIQUE INDEX CONCURRENTLY"
    )


def _events(op) -> list[str]:
    """Every statement in the order it ran, with the autocommit blocks marked."""
    events = []
    for name, args, _kwargs in op.mock_calls:
        if name.endswith("autocommit_block().__enter__"):
            events.append("<autocommit>")
        elif name.endswith("autocommit_block().__exit__"):
            events.append("</autocommit>")
        elif name.endswith("execute"):
            events.append(re.sub(r"\s+", " ", str(args[0])).strip())
    return events


def test_the_alter_commits_before_the_backfill_reads(migration):
    """ALTER TABLE holds ACCESS EXCLUSIVE until its transaction ends; kept
    with the backfill, it blocked every read of workspaces while the previous
    build was still serving."""
    module, op = migration
    module.upgrade()
    events = _events(op)
    alter = next(i for i, e in enumerate(events) if "ADD COLUMN IF NOT EXISTS name_key" in e)
    read = next(i for i, e in enumerate(events) if e.startswith("SELECT workspace_id"))
    assert events[:alter].count("<autocommit>") == 1
    assert "</autocommit>" in events[alter:read]


def test_one_guarded_write_backfills_every_row(migration):
    """A workspace the previous build renames between the read and the write
    keeps its new name rather than the one planned from the old."""
    module, op = migration
    bind = op.get_bind.return_value
    bind.execute.return_value.fetchall.return_value = [
        ("a", "u1", "Research"), ("b", "u1", "research"),
    ]
    module.upgrade()
    [(statement, params)] = [
        (re.sub(r"\s+", " ", str(c.args[0])), c.args[1])
        for c in bind.execute.call_args_list
        if "unnest" in str(c.args[0])
    ]
    assert "WHERE w.workspace_id = p.workspace_id AND w.name = p.read_name" in statement
    assert params == {
        "ids": ["a", "b"],
        "read": ["Research", "research"],
        "names": ["Research", "research (2)"],
        "keys": ["research", "research (2)"],
    }


def test_a_rerun_clears_the_first_runs_keys_before_writing(migration):
    """Rerunning after a rollback: the previous build renamed rows without
    touching their keys, so the index still holding them refused the new ones."""
    module, op = migration
    op.get_bind.return_value.execute.return_value.fetchall.return_value = [
        ("a", "u1", "Research"),
    ]
    module.upgrade()
    events = _events(op)
    clear = events.index("UPDATE workspaces SET name_key = NULL WHERE name_key IS NOT NULL")
    write = next(i for i, e in enumerate(events) if "unnest" in e)
    assert clear < write
    assert "</autocommit>" not in events[clear:write]


def test_backfill_writes_do_not_touch_updated_at(migration):
    module, op = migration
    module.upgrade()
    sql = _sql(op)
    disable = sql.index("DISABLE TRIGGER trg_workspaces_updated_at")
    enable = sql.index("ENABLE TRIGGER trg_workspaces_updated_at")
    assert disable < sql.index("folder_cleanup_pending") < enable


def test_an_old_tombstone_is_cleaned_again_rather_than_released_blind(migration):
    """Its folder may still be on disk; the cleanup finds out and then frees
    the name. Without a computer there is no disk to look at."""
    module, op = migration
    module.upgrade()
    sql = _sql(op)
    assert (
        "WHERE status = 'deleted' AND computer_id IS NOT NULL AND dir_name IS NOT NULL"
    ) in sql
    assert (
        "UPDATE workspaces SET dir_name = NULL WHERE status = 'deleted' AND computer_id IS NULL"
    ) in sql


def test_the_folder_column_widens_for_a_name(migration):
    module, op = migration
    module.upgrade()
    assert "ALTER COLUMN dir_name TYPE VARCHAR(255)" in _sql(op)
