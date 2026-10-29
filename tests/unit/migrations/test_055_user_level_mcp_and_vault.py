"""055's plan: which user server and vault name each workspace-local one becomes.

The migration reads once, plans in pure Python and writes the plan, so these
exercise the planner directly. What they pin is what a user would notice after
the upgrade: a server running in a workspace it never ran in, a same-named user
server waking up where a local copy used to replace it, two different secret
values collapsed into one, or a reference rewritten into a longer name.
"""

from __future__ import annotations

import importlib.util
import re
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "versions"
    / "055_user_level_mcp_and_vault.py"
)
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


@pytest.fixture
def m(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("migration_055", _PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "op", MagicMock())
    # An empty operator config unless a test writes one, never whichever
    # agent_config.yaml the run starts beside.
    (tmp_path / "agent_config.yaml").write_text("")
    monkeypatch.setenv("PTC_CONFIG_FILE", str(tmp_path / "agent_config.yaml"))
    return module


def _ws(m, id_, status="running", user="u1"):
    return m.Workspace(id=id_, user_id=user, status=status)


def _fork(m, id_, ws, name, *, enabled=True, created=0, config=None):
    return m.Fork(
        id=id_,
        workspace_id=ws,
        name=name,
        enabled=enabled,
        config=config if config is not None else {"transport": "stdio", "command": "uvx"},
        created_at=created,
        updated_at=created,
    )


def _plan(
    m, forks, workspaces, *, servers=(), oauth=(), markers=None, reserved=(), running=None
):
    """``running`` is the built-ins the resolver skipped: every bundled one
    unless a test says otherwise."""
    running = m._BUNDLED_SERVERS if running is None else set(running)
    return m.plan_forks(
        forks,
        workspaces,
        user_servers={"u1": set(servers)},
        oauth_names={"u1": set(oauth)},
        markers=markers or {},
        reserved=set(reserved) | m._BUNDLED_SERVERS | m._BROKERAGES,
        skipped=running | m._BROKERAGES,
    )


def _names(plan):
    return {p.source_name + "@" + str(p.workspace_id): p.name for p in plan.promotions}


def test_it_follows_054_in_a_linear_chain(m):
    assert m.revision == "055"
    assert m.down_revision == "054"


# --- forks ------------------------------------------------------------------


def test_an_unclashing_fork_keeps_its_name_and_config(m):
    fork = _fork(m, "f1", "w1", "notes", config={
        "name": "ignored",
        "transport": "http",
        "url": "https://example.com/mcp",
        "headers": {"Authorization": "Bearer ${vault:NOTES_KEY}"},
        "description": "Team notes",
        "tool_exposure_mode": "detailed",
        "discovery_uses_secrets": True,
        "source": "workspace",
        "vault_blueprints": [{"name": "X"}],
    })
    plan = _plan(m, [fork], [_ws(m, "w1"), _ws(m, "w2")])

    (promotion,) = plan.promotions
    assert (promotion.name, promotion.user_id) == ("notes", "u1")
    assert promotion.columns == {
        "transport": "http",
        "command": None,
        "args": [],
        "url": "https://example.com/mcp",
        "env": {},
        "headers": {"Authorization": "Bearer ${vault:NOTES_KEY}"},
        "description": "Team notes",
        "instruction": "",
        "tool_exposure_mode": "detailed",
        "discovery_uses_secrets": True,
    }
    # Off where it never ran; nothing in its own workspace, where it stays on.
    assert plan.tombstones == [("w2", "notes")]


def test_a_config_with_missing_or_junk_fields_gets_the_column_defaults(m):
    columns = m._catalog_columns({"args": "not-a-list", "env": None, "transport": 7})
    assert columns == {
        "transport": "stdio",
        "command": None,
        "args": [],
        "url": None,
        "env": {},
        "headers": {},
        "description": "",
        "instruction": "",
        "tool_exposure_mode": "summary",
        "discovery_uses_secrets": False,
    }
    assert m._catalog_columns(None)["transport"] == "stdio"


def test_a_name_the_user_already_has_is_suffixed_and_the_original_tombstoned(m):
    """The local row shadowed the user server in its workspace. Without the
    tombstone that server starts running there beside its promoted copy."""
    plan = _plan(
        m, [_fork(m, "f1", "w1", "notes")], [_ws(m, "w1"), _ws(m, "w2")],
        servers={"notes"},
    )
    assert _names(plan) == {"notes@w1": "notes_2"}
    assert plan.tombstones == [("w1", "notes"), ("w2", "notes_2")]


def test_a_disabled_fork_that_shadowed_stays_off_under_both_names(m):
    plan = _plan(
        m, [_fork(m, "f1", "w1", "notes", enabled=False)], [_ws(m, "w1")],
        servers={"notes"},
    )
    assert plan.tombstones == [("w1", "notes"), ("w1", "notes_2")]


def test_an_oauth_connection_name_counts_as_taken_and_as_shadowed(m):
    """A connection is keyed by server name: a server promoted onto one would
    be bound to that connection's token."""
    plan = _plan(m, [_fork(m, "f1", "w1", "notes")], [_ws(m, "w1")], oauth={"notes"})
    assert _names(plan) == {"notes@w1": "notes_2"}
    assert plan.tombstones == [("w1", "notes")]


@pytest.mark.parametrize("name", ["price_data", "robinhood"])
def test_a_builtin_or_brokerage_name_is_suffixed_and_stays_off_where_it_never_ran(m, name):
    """The resolver skipped such a local row, enabled or not, instead of
    letting it shadow the shipped one. So it never ran and never hid the
    user's own row under that name: the copy starts nowhere, the original
    name gets no shadow tombstone."""
    plan = _plan(
        m, [_fork(m, "f1", "w1", name)], [_ws(m, "w1"), _ws(m, "w2")], servers={name}
    )
    assert _names(plan) == {f"{name}@w1": f"{name}_2"}
    assert plan.tombstones == [("w1", f"{name}_2"), ("w2", f"{name}_2")]


def test_a_fork_that_never_ran_is_promoted_switched_off(m):
    """A switched-on row is probed from the Plugins page, so the copy of a
    fork that was off, or skipped under a reserved name, would dial an
    endpoint nothing used."""
    plan = _plan(
        m,
        [
            _fork(m, "f1", "w1", "notes"),
            _fork(m, "f2", "w1", "wiki", enabled=False, created=1),
            _fork(m, "f3", "w1", "price_data", created=2),
        ],
        [_ws(m, "w1")],
    )
    assert {p.source_name: p.enabled for p in plan.promotions} == {
        "notes": True,
        "wiki": False,
        "price_data": False,
    }


def test_a_builtin_known_only_from_the_database_is_reserved(m):
    """A built-in the operator no longer lists surfaces as a marker or an
    account-wide disable. That reserves its name but proves nothing was
    running under it, so the row still ran."""
    plan = _plan(m, [_fork(m, "f1", "w1", "tavily")], [_ws(m, "w1")], reserved={"tavily"})
    assert _names(plan) == {"tavily@w1": "tavily_2"}
    assert plan.promotions[0].enabled


def test_a_row_under_a_builtin_the_operator_switched_off_ran_and_keeps_running(m):
    """The resolver skipped only a running built-in's name. The name stays
    reserved, so the copy is renamed, but it stays on where it ran."""
    plan = _plan(
        m, [_fork(m, "f1", "w1", "price_data")], [_ws(m, "w1"), _ws(m, "w2")],
        running=m._BUNDLED_SERVERS - {"price_data"},
    )
    (promotion,) = plan.promotions
    assert (promotion.name, promotion.enabled) == ("price_data_2", True)
    assert plan.tombstones == [("w2", "price_data_2")]


def test_a_row_under_a_builtin_the_operator_switched_off_kept_its_user_server_out(m):
    """The user tier skipped only a running built-in's name too, so a user
    server under a switched-off one ran everywhere but where the local row
    shadowed it; with the row renamed, it stays off there."""
    plan = _plan(
        m, [_fork(m, "f1", "w1", "price_data")], [_ws(m, "w1"), _ws(m, "w2")],
        servers={"price_data"}, running=m._BUNDLED_SERVERS - {"price_data"},
    )
    assert plan.tombstones == [("w1", "price_data"), ("w2", "price_data_2")]


def test_a_row_under_a_running_builtin_shadowed_no_user_server(m):
    plan = _plan(
        m, [_fork(m, "f1", "w1", "price_data")], [_ws(m, "w1"), _ws(m, "w2")],
        servers={"price_data"},
    )
    assert plan.tombstones == [("w1", "price_data_2"), ("w2", "price_data_2")]


def test_a_row_under_a_running_builtin_the_operator_added_never_ran(m):
    plan = _plan(
        m, [_fork(m, "f1", "w1", "tavily")], [_ws(m, "w1"), _ws(m, "w2")],
        reserved={"tavily"}, running=m._BUNDLED_SERVERS | {"tavily"},
    )
    (promotion,) = plan.promotions
    assert (promotion.name, promotion.enabled) == ("tavily_2", False)
    assert plan.tombstones == [("w1", "tavily_2"), ("w2", "tavily_2")]


def test_the_same_fork_name_in_two_workspaces_is_suffixed_by_age(m):
    plan = _plan(
        m,
        [
            _fork(m, "f2", "w2", "notes", created=2),
            _fork(m, "f1", "w1", "notes", created=1),
        ],
        [_ws(m, "w1"), _ws(m, "w2")],
    )
    assert _names(plan) == {"notes@w1": "notes", "notes@w2": "notes_2"}
    # Each runs where it ran; no shadowing, since neither was a user server.
    assert plan.tombstones == [("w1", "notes_2"), ("w2", "notes")]


def test_a_suffix_never_takes_the_name_another_fork_arrives_with(m):
    plan = _plan(
        m,
        [
            _fork(m, "f1", "w1", "notes", created=1),
            _fork(m, "f2", "w2", "notes_2", created=2),
        ],
        [_ws(m, "w1"), _ws(m, "w2")],
        servers={"notes"},
    )
    assert _names(plan) == {"notes@w1": "notes_3", "notes_2@w2": "notes_2"}


def test_a_bare_underscore_name_is_suffixed_without_a_leading_dunder(m):
    """``__2`` is never module safe, so an unbounded walk from ``_`` spun
    forever under the migration's table locks."""
    plan = _plan(
        m,
        [_fork(m, "f1", "w1", "_", created=1), _fork(m, "f2", "w2", "_", created=2)],
        [_ws(m, "w1"), _ws(m, "w2")],
        servers={"_2"},
    )
    assert _names(plan) == {"_@w1": "_", "_@w2": "_3"}
    assert _names(_plan(m, [_fork(m, "f1", "w1", "_")], [_ws(m, "w1")], servers={"_"})) == {
        "_@w1": "_2"
    }


def test_a_name_that_can_never_be_met_fails_instead_of_spinning(m, monkeypatch):
    monkeypatch.setattr(m, "_module_safe", lambda name: False)
    with pytest.raises(RuntimeError, match="no free name"):
        _plan(m, [_fork(m, "f1", "w1", "notes")], [_ws(m, "w1")])


def test_the_downgrade_bounds_its_lock_wait(m):
    """Its DROP TRIGGER takes ACCESS EXCLUSIVE, and every MCP read queues behind it."""
    m.downgrade()
    assert m.op.execute.call_args_list[0].args[0] == "SET LOCAL lock_timeout = '5s'"


def test_a_stale_row_in_the_forks_own_workspace_is_never_its_new_name(m):
    """A tombstone under the chosen name there would switch the promoted server
    off in the one workspace that had it."""
    plan = _plan(
        m, [_fork(m, "f1", "w1", "notes")], [_ws(m, "w1")],
        servers={"notes"}, markers={"w1": {"notes_2"}},
    )
    assert _names(plan) == {"notes@w1": "notes_3"}


@pytest.mark.parametrize(
    ("name", "promoted"),
    [
        ("mcp_client", "mcp_client_server"),
        ("class", "class_server"),
        ("True", "True_server"),
        ("__x", "x"),
        ("__class", "class_server"),
        ("__", "server"),
    ],
)
def test_names_that_break_the_sandbox_module_layout_get_a_legal_base(m, name, promoted):
    plan = _plan(m, [_fork(m, "f1", "w1", name)], [_ws(m, "w1")])
    assert _names(plan) == {f"{name}@w1": promoted}


@pytest.mark.parametrize("name", ["match", "case", "type", "_"])
def test_a_soft_keyword_is_a_legal_module_name_and_keeps_its_name(m, name):
    """``from tools.match import ...`` parses, so nothing about the sandbox
    layout calls for a rename."""
    plan = _plan(m, [_fork(m, "f1", "w1", name)], [_ws(m, "w1")])
    assert _names(plan) == {f"{name}@w1": name}


def test_a_legal_base_that_clashes_is_suffixed_like_any_other_name(m):
    plan = _plan(
        m,
        [
            _fork(m, "f1", "w1", "class", created=1),
            _fork(m, "f2", "w2", "class_server", created=2),
            _fork(m, "f3", "w2", "class", created=3),
        ],
        [_ws(m, "w1"), _ws(m, "w2")],
    )
    assert _names(plan) == {
        "class@w1": "class_server_2",
        "class_server@w2": "class_server",
        "class@w2": "class_server_3",
    }


def test_every_promoted_name_is_legal_and_fits_the_limit(m):
    long = "a" * 64
    plan = _plan(
        m,
        [_fork(m, "f1", "w1", long), _fork(m, "f2", "w2", "odd-name!", created=1)],
        [_ws(m, "w1"), _ws(m, "w2")],
        servers={long},
    )
    names = [p.name for p in plan.promotions]
    assert names == ["a" * 62 + "_2", "odd_name_"]
    assert all(_NAME_RE.match(n) for n in names)


def test_tombstones_reach_every_other_live_workspace_including_flash(m):
    plan = _plan(
        m,
        [_fork(m, "f1", "w1", "notes")],
        [
            _ws(m, "w1"),
            _ws(m, "w2", status="stopped"),
            _ws(m, "wf", status="flash"),
            _ws(m, "wd", status="deleted"),
            _ws(m, "wx", user="u2"),
        ],
    )
    assert plan.tombstones == [("w2", "notes"), ("wf", "notes")]


def test_forks_in_deleted_workspaces_are_dropped_not_promoted(m):
    dead = _fork(m, "f1", "wd", "notes")
    plan = _plan(m, [dead], [_ws(m, "w1"), _ws(m, "wd", status="deleted")])
    assert plan.promotions == []
    assert plan.tombstones == []
    assert plan.dropped == [dead]


def test_a_dead_forks_name_does_not_push_a_live_one_to_a_suffix(m):
    plan = _plan(
        m,
        [_fork(m, "f1", "wd", "notes", created=0), _fork(m, "f2", "w1", "notes", created=1)],
        [_ws(m, "w1"), _ws(m, "wd", status="deleted")],
    )
    assert _names(plan) == {"notes@w1": "notes"}


# --- vault ------------------------------------------------------------------


def _secret(m, id_, ws, name, digest=None, created=0):
    return m.WorkspaceSecret(
        id=id_, workspace_id=ws, name=name, created_at=created, digest=digest
    )


def _promotion(m, ws, name, **columns):
    base = m._catalog_columns({"transport": "stdio", "command": "uvx"})
    return m.Promotion(
        fork_id="f-" + name,
        user_id="u1",
        workspace_id=ws,
        source_name=name,
        name=name,
        columns={**base, **columns},
        created_at=0,
        updated_at=0,
        enabled=True,
    )


def _vault(m, secrets, workspaces, *, user=None, referenced=(), promotions=(), cap=50):
    return m.plan_vault(
        secrets,
        workspaces,
        user_secrets={"u1": user or {}},
        referenced={"u1": set(referenced)},
        promotions=list(promotions),
        cap=cap,
    )


def test_an_unclashing_secret_is_copied_under_its_name(m):
    plan = _vault(m, [_secret(m, "s1", "w1", "KEY")], [_ws(m, "w1")])
    assert plan.copies == [("s1", "u1", "KEY")]
    assert plan.renames == {}


def test_an_equal_value_collapses_into_the_users_secret(m):
    plan = _vault(
        m, [_secret(m, "s1", "w1", "KEY", digest="d1")], [_ws(m, "w1")],
        user={"KEY": "d1"},
    )
    assert plan.copies == []
    assert plan.renames == {}


def test_a_different_value_is_suffixed_and_only_that_workspaces_servers_follow(m):
    in_w1 = _promotion(
        m, "w1", "notes",
        env={"A": "${vault:KEY}", "B": "${vault:KEY_B}"},
        args=["--token=${vault:KEY}", "${vault:KEYS}"],
        headers={"X": "${vault:KEY}"},
    )
    in_w2 = _promotion(m, "w2", "wiki", env={"A": "${vault:KEY}"})
    plan = _vault(
        m,
        [_secret(m, "s1", "w1", "KEY", digest="d2")],
        [_ws(m, "w1"), _ws(m, "w2")],
        user={"KEY": "d1"},
        promotions=[in_w1, in_w2],
    )

    assert plan.copies == [("s1", "u1", "KEY_2")]
    assert plan.renames == {"w1": {"KEY": "KEY_2"}}
    rewritten, untouched = plan.promotions
    assert rewritten.refs_rewritten
    assert rewritten.columns["env"] == {"A": "${vault:KEY_2}", "B": "${vault:KEY_B}"}
    assert rewritten.columns["args"] == ["--token=${vault:KEY_2}", "${vault:KEYS}"]
    assert rewritten.columns["headers"] == {"X": "${vault:KEY_2}"}
    assert untouched == in_w2


def test_renames_apply_at_once_so_they_never_chain(m):
    promotion = _promotion(m, "w1", "notes", env={"A": "${vault:A}", "B": "${vault:B}"})
    columns = m._rewrite_refs(promotion.columns, {"A": "B", "B": "C"})
    assert columns["env"] == {"A": "${vault:B}", "B": "${vault:C}"}


def test_a_promotion_with_no_ref_to_a_renamed_secret_is_left_alone(m):
    promotion = _promotion(m, "w1", "notes", env={"A": "${vault:OTHER}"})
    plan = _vault(
        m, [_secret(m, "s1", "w1", "KEY", digest="d2")], [_ws(m, "w1")],
        user={"KEY": "d1"}, promotions=[promotion],
    )
    assert plan.promotions == [promotion]


def test_without_the_key_a_clash_is_suffixed_rather_than_guessed(m):
    plan = _vault(
        m, [_secret(m, "s1", "w1", "KEY")], [_ws(m, "w1")], user={"KEY": None}
    )
    assert plan.copies == [("s1", "u1", "KEY_2")]
    assert plan.renames == {"w1": {"KEY": "KEY_2"}}


def test_two_workspaces_holding_one_name_are_merged_by_value(m):
    workspaces = [_ws(m, "w1"), _ws(m, "w2"), _ws(m, "w3")]
    plan = _vault(
        m,
        [
            _secret(m, "s1", "w1", "KEY", digest="d1", created=1),
            _secret(m, "s2", "w2", "KEY", digest="d1", created=2),
            _secret(m, "s3", "w3", "KEY", digest="d9", created=3),
        ],
        workspaces,
    )
    assert plan.copies == [("s1", "u1", "KEY"), ("s3", "u1", "KEY_2")]
    assert plan.renames == {"w3": {"KEY": "KEY_2"}}


def test_a_value_already_copied_under_a_suffix_is_found_again(m):
    plan = _vault(
        m,
        [
            _secret(m, "s1", "w1", "KEY", digest="d2", created=1),
            _secret(m, "s2", "w2", "KEY", digest="d2", created=2),
        ],
        [_ws(m, "w1"), _ws(m, "w2")],
        user={"KEY": "d1"},
    )
    assert plan.copies == [("s1", "u1", "KEY_2")]
    assert plan.renames == {"w1": {"KEY": "KEY_2"}, "w2": {"KEY": "KEY_2"}}


def test_a_secret_suffix_never_takes_another_secrets_name(m):
    plan = _vault(
        m,
        [
            _secret(m, "s1", "w1", "KEY", digest="d2", created=1),
            _secret(m, "s2", "w2", "KEY_2", digest="d3", created=2),
        ],
        [_ws(m, "w1"), _ws(m, "w2")],
        user={"KEY": "d1"},
    )
    assert plan.copies == [("s1", "u1", "KEY_3"), ("s2", "u1", "KEY_2")]


def test_secrets_of_deleted_workspaces_are_not_copied(m):
    plan = _vault(
        m, [_secret(m, "s1", "wd", "KEY")], [_ws(m, "wd", status="deleted")]
    )
    assert plan.copies == []


def test_the_merge_may_fill_the_user_vault_to_its_limit(m):
    user = {f"U{i}": None for i in range(48)}
    plan = _vault(
        m,
        [_secret(m, "s1", "w1", "A"), _secret(m, "s2", "w1", "B", created=1)],
        [_ws(m, "w1")],
        user=user,
    )
    assert len(plan.copies) == 2


def test_a_merge_over_the_limit_copies_everything_and_reports_the_user(m):
    """The limit is a create-time check; failing the deploy over one user
    would strand every other user's move."""
    user = {f"U{i}": None for i in range(49)}
    plan = _vault(
        m,
        [_secret(m, "s1", "w1", "A"), _secret(m, "s2", "w1", "B", created=1)],
        [_ws(m, "w1")],
        user=user,
    )
    assert [name for _id, _user, name in plan.copies] == ["A", "B"]
    assert plan.over_cap == {"u1": (49, 2)}


def test_a_name_something_on_the_user_vault_asks_for_is_not_handed_a_workspace_value(m):
    """A catalog server or plugin asking for KEY read only the user vault, so
    it never saw this workspace's KEY; landing the copy there would hand it
    over. The copy is suffixed and the workspace's own servers follow."""
    own = _promotion(m, "w1", "notes", env={"A": "${vault:KEY}"})
    plan = _vault(
        m,
        [_secret(m, "s1", "w1", "KEY", digest="d1")],
        [_ws(m, "w1")],
        referenced={"KEY"},
        promotions=[own],
    )
    assert plan.copies == [("s1", "u1", "KEY_2")]
    assert plan.renames == {"w1": {"KEY": "KEY_2"}}
    assert plan.promotions[0].columns["env"] == {"A": "${vault:KEY_2}"}


def test_an_equal_value_the_user_vault_already_holds_still_collapses_when_asked_for(m):
    """Whoever asks for KEY already reads that very value."""
    plan = _vault(
        m, [_secret(m, "s1", "w1", "KEY", digest="d1")], [_ws(m, "w1")],
        user={"KEY": "d1"}, referenced={"KEY"},
    )
    assert plan.copies == []
    assert plan.renames == {}


def test_a_server_promoted_from_a_workspace_without_the_name_asks_for_it(m):
    """w2's server resolved KEY from the user vault, where there was none; it
    must not start reading w1's."""
    plan = _vault(
        m,
        [_secret(m, "s1", "w1", "KEY")],
        [_ws(m, "w1"), _ws(m, "w2")],
        promotions=[
            _promotion(m, "w1", "notes", env={"A": "${vault:KEY}"}),
            _promotion(m, "w2", "wiki", headers={"X": "Bearer ${vault:KEY}"}),
        ],
    )
    assert plan.copies == [("s1", "u1", "KEY_2")]
    notes, wiki = plan.promotions
    assert notes.columns["env"] == {"A": "${vault:KEY_2}"}
    assert wiki.columns["headers"] == {"X": "Bearer ${vault:KEY}"}


def test_servers_whose_own_workspaces_hold_the_name_do_not_push_it_to_a_suffix(m):
    plan = _vault(
        m,
        [
            _secret(m, "s1", "w1", "KEY", digest="d1", created=1),
            _secret(m, "s2", "w2", "KEY", digest="d1", created=2),
        ],
        [_ws(m, "w1"), _ws(m, "w2")],
        promotions=[
            _promotion(m, "w1", "notes", env={"A": "${vault:KEY}"}),
            _promotion(m, "w2", "wiki", env={"A": "${vault:KEY}"}),
        ],
    )
    assert plan.copies == [("s1", "u1", "KEY")]
    assert plan.renames == {}


def test_only_users_with_a_clash_have_their_values_decrypted(m):
    workspaces = [
        _ws(m, "w1"),
        _ws(m, "w2"),
        _ws(m, "v1", user="u2"),
        _ws(m, "x1", user="u3"),
        _ws(m, "x2", user="u3"),
        _ws(m, "d1", user="u4", status="deleted"),
        _ws(m, "d2", user="u4"),
    ]
    secrets = [
        _secret(m, "s1", "w1", "KEY"),
        _secret(m, "s2", "v1", "OTHER"),
        _secret(m, "s3", "x1", "SHARED"),
        _secret(m, "s4", "x2", "SHARED"),
        _secret(m, "s5", "d1", "DEAD"),
        _secret(m, "s6", "d2", "DEAD"),
    ]
    needing = m.secrets_needing_compare(
        secrets, workspaces, {"u1": {"KEY"}, "u2": {"MINE"}}
    )
    assert needing == {"u1", "u3"}


# --- discovery cache ----------------------------------------------------------


def test_snapshots_follow_a_rename_and_go_when_refs_moved_or_the_workspace_died(m):
    renamed = replace(_promotion(m, "w1", "notes"), name="notes_2")
    rewritten = replace(_promotion(m, "w2", "wiki"), refs_rewritten=True)
    kept = _promotion(m, "w3", "docs")
    dead = _fork(m, "f9", "wd", "old")

    deletes, renames = m.plan_tool_schemas([renamed, rewritten, kept], [dead])

    assert renames == [("w1", "notes", "notes_2")]
    assert deletes == [("w2", "wiki"), ("wd", "old")]


# --- the SQL boundary ---------------------------------------------------------


def test_the_encryption_key_is_a_bound_parameter_never_sql_text(m):
    bind = MagicMock()
    bind.execute.return_value.mappings.return_value.all.return_value = [
        {"owner": "u1", "name": "KEY", "id": None, "digest": "a"},
        {"owner": "u1", "name": "KEY", "id": "s1", "digest": "b"},
    ]

    user_digests, workspace_digests = m._secret_digests(bind, ["u1"], "k3y-value")

    (statement, params), _ = bind.execute.call_args
    assert "k3y-value" not in str(statement)
    assert params["key"] == "k3y-value"
    assert params["users"] == ["u1"]
    assert user_digests == {("u1", "KEY"): "a"}
    assert workspace_digests == {"s1": "b"}


def test_catalog_rows_and_plugins_both_count_as_asking(m):
    """Every string of a catalog row's resolved fields, a plugin's documents,
    and the names its manifest declares, which are granted at install before
    any row references them."""
    bind = MagicMock()
    bind.execute.return_value.mappings.return_value.all.return_value = [
        {
            "owner": "u1",
            "doc": [{"A": "${vault:ENV_KEY}"}, {}, ["--t=${vault:ARG_KEY}"], None],
            "manifest": None,
        },
        {
            "owner": "u1",
            "doc": {"mcpServers": {"x": {"headers": {"H": "${vault:DOC_KEY}"}}}},
            "manifest": {"extensions": {"ai.langalpha": {"secrets": [
                {"name": "DECLARED", "bind": []},
                "junk",
            ]}}},
        },
        {"owner": "u2", "doc": None, "manifest": {"extensions": []}},
    ]

    assert m._user_tier_refs(bind, ["u1", "u2"]) == {
        "u1": {"ENV_KEY", "ARG_KEY", "DOC_KEY", "DECLARED"},
        "u2": set(),
    }


def test_no_key_means_no_decryption_at_all(m):
    bind = MagicMock()
    assert m._secret_digests(bind, ["u1"], None) == ({}, {})
    bind.execute.assert_not_called()


def test_a_database_with_nothing_to_move_only_gains_the_guard_and_the_column(m):
    """The previous build can still write a local row after an empty upgrade,
    so the retired tables refuse it either way. It can create a workspace too,
    once the new build has added a server that starts off in new ones.

    The column is added in a transaction of its own, before the main one sets
    its timeout: ADD COLUMN holds ACCESS EXCLUSIVE until commit, which inside
    the main transaction no NOWAIT retry of the table locks could let go of.
    """
    bind = m.op.get_bind.return_value
    bind.execute.return_value.mappings.return_value.all.return_value = []
    events = []
    block = m.op.get_context.return_value.autocommit_block.return_value
    block.__enter__.side_effect = lambda *a: events.append("autocommit")
    block.__exit__.side_effect = lambda *a: events.append("end autocommit")
    m.op.execute.side_effect = lambda sql: events.append(" ".join(str(sql).split()))

    m.upgrade()

    statements = [" ".join(str(c.args[0]).split()) for c in bind.execute.call_args_list]
    assert not any(s.startswith(("DELETE", "INSERT", "UPDATE")) for s in statements)
    assert events[:6] == [
        "autocommit",
        "SET lock_timeout = '5s'",
        "ALTER TABLE user_mcp_servers ADD COLUMN IF NOT EXISTS "
        "enabled_in_new_workspaces BOOLEAN NOT NULL DEFAULT TRUE",
        "RESET lock_timeout",
        "end autocommit",
        "SET LOCAL lock_timeout = '5s'",
    ]
    assert [s.split()[2] for s in events if s.startswith("CREATE TRIGGER")] == [
        "trg_workspace_mcp_servers_retired",
        "trg_workspace_vault_secrets_retired",
        "trg_workspaces_start_mcp_selection",
    ]


def _recording_bind(reads):
    """A bind answering each read from ``reads`` and recording batched writes."""
    writes = []

    def execute(statement, params=None):
        sql = str(statement)
        if isinstance(params, list):
            writes.append((sql, params))
        result = MagicMock()
        result.mappings.return_value.all.return_value = reads.get(sql, [])
        return result

    bind = MagicMock()
    bind.execute.side_effect = execute
    return bind, writes


def test_a_promoted_server_starts_off_in_workspaces_created_later(m):
    """It was added in one workspace, as a server added from a workspace is
    now, so a workspace made after the upgrade must not start it. Rows the
    user already had keep the column's default."""
    bind, writes = _recording_bind({
        m._FORKS_SQL: [
            {"id": "f1", "workspace_id": "w1", "name": "notes", "enabled": True,
             "config": {"transport": "stdio", "command": "uvx"},
             "created_at": 0, "updated_at": 0},
            {"id": "f2", "workspace_id": "w2", "name": "wiki", "enabled": False,
             "config": {"transport": "stdio", "command": "uvx"},
             "created_at": 1, "updated_at": 1},
        ],
        m._WORKSPACES_SQL: [
            {"id": "w1", "user_id": "u1", "status": "running"},
            {"id": "w2", "user_id": "u1", "status": "stopped"},
        ],
        m._SERVERS_LANDED_SQL: [{"n": 2}],
    })

    m._move_to_user_tier(bind)

    (inserted,) = [params for sql, params in writes if sql == m._INSERT_SERVER_SQL]
    assert [
        (row["name"], row["enabled"], row["enabled_in_new_workspaces"])
        for row in inserted
    ] == [
        ("notes", True, False),
        ("wiki", False, False),
    ]


def test_the_operators_config_says_which_builtin_names_ran(m, tmp_path):
    """Switching a bundled server off let a local row under its name run; a
    server the operator added and left on skipped one. A name known only from
    a marker is reserved but proves nothing ran."""
    (tmp_path / "agent_config.yaml").write_text(
        "mcp:\n"
        "  servers:\n"
        "    - name: price_data\n"
        "      enabled: false\n"
        "    - name: tavily\n"
        "      command: uvx\n"
    )
    bind, writes = _recording_bind({
        m._FORKS_SQL: [
            {"id": f"f{i}", "workspace_id": "w1", "name": name, "enabled": True,
             "config": {"transport": "stdio", "command": "uvx"},
             "created_at": i, "updated_at": i}
            for i, name in enumerate(["price_data", "tavily", "legacy"])
        ],
        m._WORKSPACES_SQL: [{"id": "w1", "user_id": "u1", "status": "running"}],
        m._BUILTIN_NAMES_SQL: [{"name": "legacy"}],
        m._SERVERS_LANDED_SQL: [{"n": 3}],
    })

    m._move_to_user_tier(bind)

    (inserted,) = [params for sql, params in writes if sql == m._INSERT_SERVER_SQL]
    assert [(row["name"], row["enabled"]) for row in inserted] == [
        ("price_data_2", True),
        ("tavily_2", False),
        ("legacy_2", True),
    ]
    (tombstones,) = [params for sql, params in writes if sql == m._TOMBSTONE_SQL]
    assert tombstones == [{"workspace_id": "w1", "name": "tavily_2"}]


def test_the_config_lays_the_operators_servers_over_the_bundled_ones(m, tmp_path):
    """An entry without ``enabled`` keeps what the name had, as the runtime's
    merge does; one under a new name runs unless it says otherwise."""
    (tmp_path / "agent_config.yaml").write_text(
        "mcp:\n"
        "  servers:\n"
        "    - name: price_data\n"
        "      enabled: false\n"
        "    - name: price_data\n"
        "      description: retuned\n"
        "    - name: tavily\n"
        "    - name: parked\n"
        "      enabled: false\n"
    )
    bundled = m._bundled_server_names()
    runs = m._builtin_servers()
    assert set(runs) == bundled | {"tavily", "parked"}
    assert {name for name, on in runs.items() if on} == (
        (bundled - {"price_data"}) | {"tavily"}
    )


def _config_server(tmp_path, enabled, name="tavily"):
    (tmp_path / "agent_config.yaml").write_text(
        f"mcp:\n  servers:\n    - name: {name}\n      enabled: {enabled}\n"
    )


@pytest.mark.parametrize(
    ("enabled", "env", "runs"),
    [
        ("${X}", {"X": "false"}, False),
        ("${X}", {"X": "0"}, False),
        ("$X", {"X": "off"}, False),
        ('"false"', {}, False),
        ("n", {}, False),
        ("0", {}, False),
        ("true", {}, True),
        ("false", {}, False),
    ],
)
def test_enabled_reads_as_the_backend_read_it(
    m, tmp_path, monkeypatch, enabled, env, runs
):
    """Substituted, then read as Pydantic's lax bool: a string or a 0 that
    switched the server off is not taken for one that ran."""
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    _config_server(tmp_path, enabled)
    assert m._builtin_servers()["tavily"] is runs


def test_a_substituted_name_is_the_server_it_names(m, tmp_path, monkeypatch):
    monkeypatch.setenv("N", "price_data")
    _config_server(tmp_path, "false", name="${N}")
    runs = m._builtin_servers()
    assert runs["price_data"] is False and "${N}" not in runs


@pytest.mark.parametrize("enabled", ["${X}", "null"])
def test_an_enabled_the_backend_refuses_stops_the_upgrade(
    m, tmp_path, monkeypatch, enabled
):
    """The backend refused this at startup, so the environment here is not the
    one it ran in, and neither answer is the one it acted on."""
    monkeypatch.delenv("X", raising=False)
    _config_server(tmp_path, enabled)
    with pytest.raises(RuntimeError, match="'tavily'"):
        m._builtin_servers()
    # Only a fork's plan reads the config, so a vault-only upgrade goes ahead.
    bind, _ = _recording_bind({
        m._SECRETS_SQL: [
            {"id": "s1", "workspace_id": "w1", "name": "K", "created_at": 0},
        ],
        m._WORKSPACES_SQL: [{"id": "w1", "user_id": "u1", "status": "running"}],
        m._SECRETS_LANDED_SQL: [{"n": 1}],
    })
    m._move_to_user_tier(bind)


def test_an_app_env_overlay_decides_which_servers_ran(m, tmp_path, monkeypatch):
    """The backend read the overlay over the base, and its server list replaced
    the base's outright."""
    _config_server(tmp_path, "false", name="price_data")
    (tmp_path / "agent_config.prod.yaml").write_text(
        "mcp:\n  servers:\n    - name: parked\n      enabled: false\n"
    )
    monkeypatch.setenv("APP_ENV", "prod")
    runs = m._builtin_servers()
    assert runs["price_data"] is True and runs["parked"] is False


def test_without_a_config_file_every_bundled_server_ran(m, tmp_path, monkeypatch):
    """A PTC_CONFIG_FILE that does not exist is passed over, as the runtime
    passes over it."""
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.setenv("PTC_CONFIG_FILE", str(empty / "missing.yaml"))
    monkeypatch.chdir(empty)
    monkeypatch.setenv("HOME", str(empty))
    assert m._builtin_servers() == dict.fromkeys(m._bundled_server_names(), True)


@pytest.mark.parametrize(
    "config",
    [
        "",
        "- not a mapping\n",
        "mcp: [1, 2]\n",
        "mcp:\n  servers:\n",
        "mcp:\n  servers: {name: price_data, enabled: false}\n",
        "mcp:\n  servers:\n    - price_data\n    - {enabled: false}\n"
        "    - {name: 7, enabled: false}\n",
    ],
)
def test_a_malformed_mcp_section_overrides_nothing(m, tmp_path, config):
    (tmp_path / "agent_config.yaml").write_text(config)
    assert m._builtin_servers() == dict.fromkeys(m._bundled_server_names(), True)


def test_a_copy_that_lands_short_aborts_before_its_source_is_deleted(m):
    """An INSERT ... SELECT whose source row is not found inserts nothing and
    raises nothing, so only a count tells, and it has to come first."""
    reads = {
        m._SECRETS_SQL: [
            {"id": "s1", "workspace_id": "w1", "name": "KEY", "created_at": 0},
        ],
        m._WORKSPACES_SQL: [{"id": "w1", "user_id": "u1", "status": "running"}],
        m._SECRETS_LANDED_SQL: [{"n": 0}],
    }
    statements = []

    def execute(statement, params=None):
        statements.append(" ".join(str(statement).split()))
        result = MagicMock()
        result.mappings.return_value.all.return_value = reads.get(str(statement), [])
        return result

    bind = MagicMock()
    bind.execute.side_effect = execute

    with pytest.raises(RuntimeError, match="planned 1 vault secret"):
        m._move_to_user_tier(bind)
    assert not any(s.startswith("DELETE FROM workspace_vault_secrets") for s in statements)
