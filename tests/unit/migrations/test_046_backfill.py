"""What 046's DDL and backfill have to keep true, as SQL rather than as prose.

Three of these lock a decision that a plausible "cleanup" would undo and that
only shows up in production. The provider-ref index must stay PARTIAL, because
``workspaces.sandbox_id`` never had a unique index and the rows this backfills
from may already share a vendor id. ``dir_name`` must stay nullable, because the
colour still serving during the rolling deploy inserts workspaces without it.
And the grant's ``computer_id`` must stay ON DELETE SET NULL while
``workspace_id`` is the relay's authority, since a cascade from a column with no
reader would delete live authorization rows.
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
    module = _load("migration_046", _VERSIONS / "046_computers.py")
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


def test_it_follows_045_in_a_linear_chain(migration):
    module, _op = migration
    assert module.revision == "046"
    assert module.down_revision == "045"


def test_the_status_set_is_the_workspace_one_minus_flash(migration):
    """A computer is a machine; 'flash' names a workspace that never had one."""
    module, _op = migration
    ws_009 = _load("migration_009", _VERSIONS / "009_add_starting_workspace_status.py")
    import inspect

    source = inspect.getsource(ws_009.upgrade)
    workspace_statuses = set(re.findall(r"'(\w+)'", source.split("ADD CONSTRAINT")[1]))
    assert set(module._STATUSES) == workspace_statuses - {"flash"}


def test_the_provider_ref_index_is_partial(migration):
    """Both halves of the predicate: NULL refs are the never-provisioned rows,
    and a tombstone must never block a rebind of the same vendor id."""
    sql = _upgrade_sql(migration)
    index = re.search(
        r"CREATE UNIQUE INDEX IF NOT EXISTS idx_computers_provider_ref[^;]*?"
        r"WHERE provider_ref IS NOT NULL AND status <> 'deleted'",
        sql,
    )
    assert index, sql
    assert "ON computers (kind, provider_ref)" in sql


def test_one_primary_per_user_is_an_index_not_a_convention(migration):
    """The tombstone half is not optional: every primary probe reads
    ``is_primary AND status <> 'deleted'``, so an index without it lets a
    deleted row hold a slot no reader can see and no insert can take."""
    sql = _upgrade_sql(migration)
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_computers_primary_by_user "
        "ON computers (user_id) WHERE is_primary AND status <> 'deleted'" in sql
    )


def test_the_backfill_leaves_layout_version_unobserved(migration):
    """0 is "no sandbox has reported", not layout v0. ``workspaces`` has no
    layout column, so there is nothing to copy; the machine's first asset sync
    stamps the manifest's value. The notice says so, because until then any
    reader comparing versions is comparing against a placeholder."""
    sql = _upgrade_sql(migration)
    assert "mcp_config_version, layout_version, origin_workspace_id" in sql
    assert "mcp_config_version, 0, workspace_id, COALESCE(artifacts" in sql
    assert "layout_version 0 (not observed) on every new row" in sql


def test_the_backfill_records_which_project_owns_the_machine_root(migration):
    """The v3 to v4 move sweeps every loose root entry into one folder. Once
    consolidation folds siblings onto an older machine, that folder has to be
    the elected project's, so the election is recorded here; inferring it later
    from whichever sibling starts first is how one project's files end up in
    another's folder."""
    sql = _upgrade_sql(migration)
    assert (
        "origin_workspace_id UUID REFERENCES workspaces(workspace_id) "
        "ON DELETE SET NULL" in sql
    )
    assert "layout_version, origin_workspace_id, artifacts" in sql
    assert "0, workspace_id, COALESCE(artifacts" in sql
    assert "origin_workspace_id names the project that owns each " in sql


def test_dir_name_is_nullable(migration):
    """NOT NULL here fails every workspace insert the draining colour makes."""
    sql = _upgrade_sql(migration)
    assert "ADD COLUMN IF NOT EXISTS dir_name VARCHAR(64)" in sql
    assert "dir_name VARCHAR(64) NOT NULL" not in sql


def test_a_backfilled_workspace_is_stamped_as_pre_split(migration):
    """The prompt tells a moved workspace where its files went, and only a
    workspace that existed before the split has anything to be told about."""
    sql = _upgrade_sql(migration)
    assert "ADD COLUMN IF NOT EXISTS layout_origin SMALLINT" in sql
    assert "layout_origin = 3" in sql
    assert "layout_origin SMALLINT NOT NULL" not in sql
    assert "layout_origin SMALLINT DEFAULT" not in sql


def test_the_workspace_link_does_not_cascade(migration):
    """A machine going away is not the project and its files going away."""
    sql = _upgrade_sql(migration)
    assert (
        "ADD COLUMN IF NOT EXISTS computer_id UUID REFERENCES "
        "computers(computer_id) ON DELETE SET NULL" in sql
    )
    assert "computers(computer_id) ON DELETE CASCADE" not in sql


def test_the_dir_name_slug_cannot_exceed_the_column(migration):
    """64 chars: 59 of slug, the separator, and a 4-char suffix."""
    sql = _upgrade_sql(migration)
    assert "left(" in sql and ", 59)" in sql
    assert "substr(md5(w.workspace_id::text), 1, 4)" in sql


def test_the_backfill_only_claims_workspaces_that_own_a_machine(migration):
    sql = _upgrade_sql(migration)
    # Flash rows carry a sandbox_id that names the shared per-user sandbox, not
    # a machine of their own; electing one would seize a sibling's computer.
    assert "WHERE sandbox_id IS NOT NULL AND status NOT IN ('deleted', 'flash')" in sql


def test_a_shared_sandbox_id_keeps_the_most_recently_active_row(migration):
    """DISTINCT ON plus that ORDER BY is the whole tie-break; losing it would
    make the insert fail on the unique index instead of choosing a winner."""
    sql = _upgrade_sql(migration)
    assert "SELECT DISTINCT ON (sandbox_id)" in sql
    assert (
        "ORDER BY sandbox_id, COALESCE(last_activity_at, updated_at, "
        "created_at) DESC NULLS LAST, workspace_id DESC" in sql
    )


def test_the_backfill_re_elects_nobody_on_a_second_run(monkeypatch, migration):
    """Every DDL statement in this migration is IF NOT EXISTS, and the backfill
    has to match, because a blocked branch gets re-applied by hand.

    Without both halves of the guard the second run elects a winner whose
    sandbox id already has a computer and dies on idx_computers_provider_ref.
    The ``computer_id IS NULL`` half also keeps the loser of a shared sandbox
    id on the pre-computer path instead of promoting it later.
    """
    monkeypatch.setenv("SANDBOX_PROVIDER", "daytona")
    sql = _upgrade_sql(migration)
    assert "FROM workspaces w WHERE sandbox_id IS NOT NULL" in sql
    assert "AND w.computer_id IS NULL" in sql
    assert (
        "AND NOT EXISTS ( SELECT 1 FROM computers c "
        "WHERE c.kind = 'daytona' AND c.provider_ref = w.sandbox_id "
        "AND c.status <> 'deleted' )" in sql
    )


def test_the_primary_is_the_users_most_recently_active_computer(migration):
    sql = _upgrade_sql(migration)
    assert "row_number() OVER ( PARTITION BY user_id" in sql
    assert "WHERE c.computer_id = r.computer_id AND r.rn = 1" in sql


def test_the_primary_election_raises_nobody_on_a_second_run(migration):
    """The other half of re-runnability, and the one a re-run reaches later.

    A re-run ranks the user's live computers again, so a machine the app has
    minted since wins the ranking and is raised beside the primary that is
    already there, which idx_computers_primary_by_user refuses.
    """
    sql = _upgrade_sql(migration)
    assert "SELECT computer_id, user_id, row_number() OVER" in sql
    assert (
        "AND NOT EXISTS ( SELECT 1 FROM computers p "
        "WHERE p.user_id = r.user_id AND p.is_primary "
        "AND p.status <> 'deleted' )" in sql
    )


def test_the_notice_names_both_skips(migration):
    """The operator reads these in the deploy log: both make the platform's
    per-user count looser than the per-workspace one it replaces."""
    sql = _upgrade_sql(migration)
    assert "sandbox id(s) shared by " in sql
    assert "with no sandbox_id" in sql


def test_grants_are_labelled_from_their_workspace(migration):
    sql = _upgrade_sql(migration)
    assert "UPDATE sandbox_egress_grants g SET computer_id = w.computer_id" in sql
    assert (
        "ADD COLUMN IF NOT EXISTS computer_id UUID REFERENCES "
        "computers(computer_id) ON DELETE SET NULL" in sql
    )


def test_the_backfill_kind_follows_the_runtimes_provider(monkeypatch, migration):
    """Provider selection is per process in this release, so the row records
    what the runtime actually uses rather than a guess."""
    module, _op = migration
    monkeypatch.setenv("SANDBOX_PROVIDER", "docker")
    assert module._backfill_kind() == "docker"
    monkeypatch.setenv("SANDBOX_PROVIDER", "")
    assert module._backfill_kind() == "daytona"
    monkeypatch.setenv("SANDBOX_PROVIDER", "something-we-do-not-ship")
    assert module._backfill_kind() == "daytona"
    monkeypatch.delenv("SANDBOX_PROVIDER", raising=False)
    assert module._backfill_kind() == "daytona"


def test_the_downgrade_removes_every_column_it_added(migration):
    module, op = migration
    module.downgrade()
    sql = _flat(op)
    for statement in (
        "ALTER TABLE sandbox_egress_grants DROP COLUMN IF EXISTS computer_id",
        "ALTER TABLE workspaces DROP COLUMN IF EXISTS dir_name",
        "ALTER TABLE workspaces DROP COLUMN IF EXISTS layout_origin",
        "ALTER TABLE workspaces DROP COLUMN IF EXISTS computer_id",
        "DROP TABLE IF EXISTS computers CASCADE",
    ):
        assert statement in sql
    # The shadow columns are untouched, so the pre-computer readers keep working
    # on the rows they already had.
    assert "workspaces DROP COLUMN IF EXISTS sandbox_id" not in sql
    assert "workspaces DROP COLUMN IF EXISTS resource_tier" not in sql
