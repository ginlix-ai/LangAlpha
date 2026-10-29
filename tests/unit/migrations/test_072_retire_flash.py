"""What 072 has to keep true.

The migration copies Home's id namespace rather than importing the app, so a
drift there would move every automation that ran outside a workspace to an
id no Home has.
Only a row the app takes for Home is renamed 'Home', or a former Home bound
at another id would take Home's folder. Each automation is stored where the
previous build runs it, since the stored workspace is where every run happens
from here on. The CHECKs are what stop 'flash' being written again, and they
and the name index rebuild have to run where Postgres allows them: added NOT
VALID in the transaction, validated and built CONCURRENTLY outside it, the
status CHECK dropped first so the rewrite can write what it refuses. Each
renamed key keeps a value already under its new name, and a preference is
renamed with its ``updated_at`` left alone. A steer parked when it runs has to
start a turn this build admits: its stamp under the new name, its sender
the Chief of Staff. The downgrade has to hand the
previous build the schema it writes and the names and status it reads:
``agent_mode`` with 'flash' as the default for a new row, 'flash' back in
every set and on every unbound Home, and the old key names.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.server.database.home_workspace import get_home_workspace_id

_VERSIONS = Path(__file__).resolve().parents[3] / "migrations" / "versions"
_INDEX = "idx_workspaces_user_name_key"
_CHECKS = {
    "workspaces_status_check",
    "conversation_threads_msg_type_check",
    "conversation_usages_msg_type_check",
}
_ADD_CHECK = re.compile(
    r"ADD CONSTRAINT (\w+) CHECK \(\w+ IN \(([^)]*)\)\) NOT VALID"
)
# A key moved to its other name, which keeps a value already stored under it.
_KEY_RENAME = re.compile(
    r"UPDATE (\w+) SET (\w+) = \(\2 - '(\w+)'\) \|\| "
    r"jsonb_build_object\('(\w+)', COALESCE\(\2->'\4', \2->'\3'\)\) "
    r"WHERE \2 \? '\3'"
)
# The same move inside a steer's origin.
_STEER_KEY_RENAME = re.compile(
    r"UPDATE hook_outbox SET payload = jsonb_set\(payload, '\{origin\}', "
    r"\(\(payload->'origin'\) - '(\w+)'\) \|\| jsonb_build_object\('(\w+)', "
    r"COALESCE\(\(payload->'origin'\)->'\2', \(payload->'origin'\)->'\1'\)\)\) "
    r"WHERE hook_type = 'steer' AND \(payload->'origin'\) \? '\1'"
)
_SLOT_RENAMES = {
    ("user_preferences", "model_preference", "preferred_flash_model", "preferred_background_model"),
    ("user_preferences", "model_preference", "flash_follows", "background_follows"),
    ("user_preferences", "other_preference", "preferred_flash_model", "preferred_background_model"),
}
_STAMP_RENAMES = {
    ("conversation_responses", "metadata", "origin_flash_thread_id", "origin_home_thread_id"),
    ("hook_outbox", "payload.origin", "origin_flash_thread_id", "origin_home_thread_id"),
}
_PREFS_TRIGGER = "user_preferences {} TRIGGER trg_user_preferences_updated_at"


def _load():
    spec = importlib.util.spec_from_file_location(
        "migration_072", _VERSIONS / "072_retire_flash.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bind() -> MagicMock:
    """A connection with nothing left to rewrite, but the column still there."""
    result = MagicMock(rowcount=0)
    result.fetchall.return_value = []
    result.first.return_value = (1,)
    bind = MagicMock()
    bind.execute.return_value = result
    return bind


@pytest.fixture
def migration(monkeypatch):
    module = _load()
    op = MagicMock()
    op.get_bind.return_value = _bind()
    monkeypatch.setattr(module, "op", op)
    return module, op


def _statements(op) -> list[str]:
    """Each statement ``op`` or its connection ran, in order, whitespace-collapsed,
    with a marker where the autocommit block opens and closes."""
    out = []
    for name, args, _ in op.mock_calls:
        if name in ("execute", "get_bind().execute"):
            out.append(re.sub(r"\s+", " ", str(args[0])).strip())
        elif name.endswith("autocommit_block().__enter__"):
            out.append("<autocommit>")
        elif name.endswith("autocommit_block().__exit__"):
            out.append("</autocommit>")
    return out


def _key_renames(statements: list[str]) -> dict[tuple[str, str, str, str], int]:
    """(table, column, from, to) of each key rename, by statement index; a
    steer's origin is the column ``payload.origin``."""
    renames = {}
    for i, s in enumerate(statements):
        if m := _KEY_RENAME.fullmatch(s):
            renames[m.groups()] = i
        elif m := _STEER_KEY_RENAME.fullmatch(s):
            renames[("hook_outbox", "payload.origin", *m.groups())] = i
    return renames


def _back(renames) -> set[tuple[str, str, str, str]]:
    return {(table, column, new, old) for table, column, old, new in renames}


def _first_lock(statements: list[str]) -> int:
    return next(
        i for i, s in enumerate(statements) if re.search(r"\b(workspaces|automations)\b", s)
    )


def _checks(statements: list[str]) -> dict[str, str]:
    return {
        m.group(1): m.group(2)
        for s in statements
        for m in _ADD_CHECK.finditer(s)
    }


def test_it_follows_071():
    module = _load()
    assert module.revision == "072"
    assert module.down_revision == "071"


@pytest.mark.parametrize("user_id", ["user-1", "a0b1c2d3-e4f5-4a6b-8c7d-9e0f1a2b3c4d"])
def test_its_home_id_is_the_apps(user_id):
    assert _load().home_workspace_id(user_id) == get_home_workspace_id(user_id)


def test_only_a_row_the_app_takes_for_home_is_renamed():
    """``flash_mode`` is not read: a former Home bound to a computer at another
    id is an ordinary workspace once 072 has run, so it keeps 'Flash'."""
    module = _load()
    rows = [
        (get_home_workspace_id("u-derived"), "u-derived", "stopped"),
        ("ws-flash", "u-flash", "flash"),
        ("ws-unbound", "u-unbound", "unbound"),
        ("ws-bound", "u-bound", "stopped"),
    ]
    result = MagicMock(rowcount=0)
    result.fetchall.return_value = rows
    bind = MagicMock()
    bind.execute.return_value = result

    module._rewrite_workspaces(bind)

    (rename,) = [
        c for c in bind.execute.call_args_list if "SET name = 'Home'" in str(c.args[0])
    ]
    assert rename.args[1]["ids"] == [rows[0][0], "ws-flash", "ws-unbound"]


_WS = "11111111-1111-1111-1111-111111111111"
_THREAD_WS = "22222222-2222-2222-2222-222222222222"


@pytest.mark.parametrize(
    "for_home, pinned, thread_ws, reachable, expected",
    [
        (False, True, _THREAD_WS, True, (_THREAD_WS, False, False)),
        (True, True, _THREAD_WS, True, (_THREAD_WS, False, False)),
        (False, True, _THREAD_WS, False, (_WS, False, True)),
        (True, True, _THREAD_WS, False, (None, True, True)),
        (False, True, None, False, (_WS, False, False)),
        (True, True, None, False, (None, True, False)),
        (True, False, None, False, (None, True, False)),
        (False, False, None, False, (_WS, False, False)),
    ],
    ids=[
        "pin-elsewhere-moves-there",
        "home-pin-elsewhere-moves-there",
        "unusable-pin-cleared-where-the-row-lives",
        "home-unusable-pin-cleared-in-home",
        "gone-thread-stays-pinned",
        "home-gone-thread-stays-pinned-in-home",
        "home-unpinned-in-home",
        "unpinned-stays",
    ],
)
def test_each_automation_is_stored_where_the_previous_build_runs_it(
    for_home, pinned, thread_ws, reachable, expected
):
    """A usable pin decides; an unusable one is cleared, as the previous build
    started a new thread for it where the row lives, which a gone thread is
    not: that build made it again there under the same id."""
    assert _load().place(
        for_home=for_home,
        stored=_WS,
        pinned=pinned,
        thread_workspace=thread_ws,
        reachable=reachable,
    ) == expected


def test_the_status_check_goes_before_the_rewrite_writes_unbound(migration):
    module, op = migration
    module.upgrade()
    statements = _statements(op)

    dropped = statements.index("ALTER TABLE workspaces DROP CONSTRAINT IF EXISTS workspaces_status_check")
    rewritten = statements.index("UPDATE workspaces SET status = 'unbound' WHERE status = 'flash'")
    (added,) = [i for i, s in enumerate(statements) if "ADD CONSTRAINT workspaces_status_check " in s]
    assert dropped < rewritten < added


def test_history_is_rewritten_before_workspaces_or_automations_are_locked(migration):
    """The history rewrite scans the conversation tables and the outbox; run
    first, it holds no lock on workspaces or automations while it does."""
    module, op = migration
    module.upgrade()
    statements = _statements(op)

    history = [
        i
        for i, s in enumerate(statements)
        if s.startswith(("UPDATE conversation_", "UPDATE hook_outbox"))
    ]
    assert len(history) == 9
    assert max(history) < _first_lock(statements)


def test_a_steer_flash_sent_reads_as_the_chief_of_staffs(migration):
    """A hand-off parked when the upgrade runs starts its own turn as its
    sender, and from here on a turn from Flash is refused."""
    module, op = migration
    module.upgrade()
    statements = _statements(op)

    assert (
        "UPDATE hook_outbox SET payload = jsonb_set(payload, '{sender}', '\"chief_of_staff\"') "
        "WHERE hook_type = 'steer' AND payload->>'sender' = 'flash'"
    ) in statements
    assert (
        "UPDATE hook_outbox SET payload = jsonb_set(payload, '{origin,agent}', "
        "'\"chief_of_staff\"') WHERE hook_type = 'steer' AND payload->'origin'->>'agent' = 'flash'"
    ) in statements


def test_no_check_admits_flash_after_the_upgrade(migration):
    module, op = migration
    module.upgrade()
    checks = _checks(_statements(op))

    assert set(checks) == _CHECKS
    for name, values in checks.items():
        assert "'flash'" not in values, name
    assert "'unbound'" in checks["workspaces_status_check"]


def test_each_check_is_added_in_the_transaction_and_validated_outside_it(migration):
    """Validating inside the transaction would hold its lock for the whole scan."""
    module, op = migration
    module.upgrade()
    statements = _statements(op)
    opened = statements.index("<autocommit>")

    for name in _CHECKS:
        (added,) = [i for i, s in enumerate(statements) if f"ADD CONSTRAINT {name} " in s]
        (validated,) = [
            i for i, s in enumerate(statements) if s.endswith(f"VALIDATE CONSTRAINT {name}")
        ]
        assert added < opened < validated, name


def test_the_name_index_leaves_out_unbound_rows_and_builds_concurrently(migration):
    """An unbound Home carries no name key; CONCURRENTLY cannot run in a
    transaction, so the build is inside the autocommit block."""
    module, op = migration
    module.upgrade()
    statements = _statements(op)
    block = statements[statements.index("<autocommit>") : statements.index("</autocommit>")]

    assert f"DROP INDEX CONCURRENTLY IF EXISTS {_INDEX}" in block
    assert (
        f"CREATE UNIQUE INDEX CONCURRENTLY {_INDEX} ON workspaces (user_id, name_key) "
        "WHERE status NOT IN ('deleted', 'unbound')"
    ) in block


def test_the_downgrade_reads_unbound_home_as_flash_again(migration):
    """The previous build knows a Home before its first computer only as
    'flash', and 071's CHECK refuses 'unbound'."""
    module, op = migration
    module.downgrade()
    statements = _statements(op)

    dropped = statements.index("ALTER TABLE workspaces DROP CONSTRAINT IF EXISTS workspaces_status_check")
    rewritten = statements.index("UPDATE workspaces SET status = 'flash' WHERE status = 'unbound'")
    (added,) = [i for i, s in enumerate(statements) if "ADD CONSTRAINT workspaces_status_check " in s]
    assert dropped < rewritten < added


def test_the_downgrade_restores_what_the_previous_build_writes(migration):
    """'ptc' fills the column on every existing row, where with Home's id it
    still means Home; a new row takes the previous build's default."""
    module, op = migration
    module.downgrade()
    statements = _statements(op)

    (added,) = [
        i
        for i, s in enumerate(statements)
        if "ADD COLUMN IF NOT EXISTS agent_mode" in s and "DEFAULT 'ptc'" in s
    ]
    (defaulted,) = [
        i
        for i, s in enumerate(statements)
        if s == "ALTER TABLE automations ALTER COLUMN agent_mode SET DEFAULT 'flash'"
    ]
    assert added < defaulted

    checks = _checks(statements)
    assert set(checks) == _CHECKS
    for name, values in checks.items():
        assert "'flash'" in values, name
    # 071's set exactly, which has no 'unbound'.
    assert checks["workspaces_status_check"] == (
        "'creating', 'running', 'starting', 'stopping', 'stopped', 'error', 'deleted', 'flash'"
    )
    assert (
        f"CREATE UNIQUE INDEX CONCURRENTLY {_INDEX} ON workspaces (user_id, name_key) "
        "WHERE status NOT IN ('deleted', 'flash')"
    ) in statements


def test_each_old_key_name_moves_to_its_new_one(migration):
    """In the preferences, and in the stamp a dispatched thread's later turns
    inherit and a hand-off's steer carries, which the upgrade renames with the
    rest of the history."""
    module, op = migration
    module.upgrade()
    statements = _statements(op)
    renames = _key_renames(statements)

    assert set(renames) == _SLOT_RENAMES | _STAMP_RENAMES
    for rename in _STAMP_RENAMES:
        assert renames[rename] < _first_lock(statements), rename


def test_a_preference_is_renamed_with_its_updated_at_left_alone(migration):
    """A rename is not the user's edit; the insights' model reads 'background'
    as a new brief's does."""
    module, op = migration
    module.upgrade()
    statements = _statements(op)
    renames = _key_renames(statements)
    off = statements.index(f"ALTER TABLE {_PREFS_TRIGGER.format('DISABLE')}")
    on = statements.index(f"ALTER TABLE {_PREFS_TRIGGER.format('ENABLE')}")

    for rename in _SLOT_RENAMES:
        assert off < renames[rename] < on, rename
    assert "UPDATE market_insights SET model = 'background' WHERE model = 'flash'" in statements


def test_the_downgrade_gives_back_every_name_the_previous_build_reads(migration):
    """The stamps first, before anything locks workspaces, as the upgrade's
    history; the preferences with their trigger off."""
    module, op = migration
    module.downgrade()
    statements = _statements(op)
    renames = _key_renames(statements)
    off = statements.index(f"ALTER TABLE {_PREFS_TRIGGER.format('DISABLE')}")
    on = statements.index(f"ALTER TABLE {_PREFS_TRIGGER.format('ENABLE')}")

    assert set(renames) == _back(_SLOT_RENAMES | _STAMP_RENAMES)
    for stamp in _back(_STAMP_RENAMES):
        assert renames[stamp] < _first_lock(statements), stamp
    for rename in _back(_SLOT_RENAMES):
        assert off < renames[rename] < on, rename
    assert "UPDATE market_insights SET model = 'flash' WHERE model = 'background'" in statements
