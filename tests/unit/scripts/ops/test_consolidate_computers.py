"""The consolidation job's rules, and the app functions it has to agree with.

The job cannot import the app layer: every route into ``src.server.database``
beyond the fences reaches ``src.config``, whose ``load_dotenv()`` would retarget
a mutating operator script at whatever ``.env`` is on disk. The fences, the
column list and the folder-name rule therefore live in the zero-import leaf
``src.server.database.sql_fences`` that both sides import, and these tests pin
that sharing; the sandbox config hash and the key derivation are still copies,
and these tests import both sides and compare.

The rest pins the decisions the job makes on its own, in the shape a re-run has
to reproduce exactly, because a second run over a consolidated database is
supposed to write nothing.
"""

from __future__ import annotations

import inspect
import typing
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional

import pytest

from scripts.ops.consolidate_computers import (
    CURRENT_LAYOUT_VERSION,
    FENCE_LIVE_WORKSPACE,
    FENCE_NOT_DELETED,
    TIER_RANK,
    _adopt_grants,
    _reap,
    _adopt_machine_sandbox,
    _consolidate,
    _consolidate_user,
    _free_dir_name,
    _presync_targets,
    _rebind,
    _rebind_folding_workspaces,
    _retire_computer,
    _settle_creating,
    _Sink,
    _workspace_identity,
    _write_primary_columns,
    advisory_key,
    computer_advisory_key,
    computer_status_channel,
    elect_primary,
    highest_tier,
    pick_grant_survivor,
    sandbox_config_hash,
    sandbox_config_stamp,
    user_egress_lock_key,
    workspace_dir_name,
    workspace_status_channel,
)
from src.server.database.sql_fences import DIR_NAME_MAX

PRIMARY = "11111111-1111-4111-8111-111111111111"
FOLDING = "22222222-2222-4222-8222-222222222222"
WORKSPACE = "44444444-4444-4444-8444-444444444444"


def _computer(computer_id: str, *, is_primary: bool = False, tier: str = "standard"):
    return {"computer_id": computer_id, "is_primary": is_primary, "resource_tier": tier}


def _grant(grant_id: str, computer_id: str | None):
    return {"grant_id": grant_id, "computer_id": computer_id}


# ── The app definitions the script no longer copies ──────────────────────────


def test_the_app_and_the_job_read_one_copy_of_the_fences():
    from src.server.database import computer, sql_fences

    assert FENCE_NOT_DELETED is sql_fences.FENCE_NOT_DELETED
    assert FENCE_LIVE_WORKSPACE is sql_fences.FENCE_LIVE_WORKSPACE
    assert computer.FENCE_NOT_DELETED is sql_fences.FENCE_NOT_DELETED
    assert computer.FENCE_LIVE_WORKSPACE is sql_fences.FENCE_LIVE_WORKSPACE
    assert computer._COMPUTER_COLS is sql_fences.COMPUTER_COLS


def test_the_folder_name_rule_is_the_one_the_app_writes():
    from src.server.database import sql_fences

    assert workspace_dir_name is sql_fences.workspace_dir_name


def test_the_job_names_the_shared_fence_rather_than_spelling_it():
    # Four statements carried an inline copy of a predicate a constant already
    # held, so grepping the constant missed a third of the statements it governs.
    for fn in (
        _rebind,
        _adopt_machine_sandbox,
        _retire_computer,
        _settle_creating,
        _write_primary_columns,
    ):
        source = inspect.getsource(fn)
        assert "status NOT IN ('deleted', 'flash')" not in source
        assert "w.status <> 'deleted'" not in source
        assert "{FENCE_LIVE_WORKSPACE}" in source
    # The sandbox adoption fenced two of its three predicates differently from
    # the statement it mirrors, so a flash row was reachable by the repair.
    assert "FENCE_NOT_DELETED" not in inspect.getsource(_adopt_machine_sandbox)


# ── The app functions the script still copies ────────────────────────────────


def test_advisory_key_agrees_with_the_writer_guard():
    from src.server.services.writer_guard import advisory_key as app_advisory_key

    for domain, parts in [
        ("C", ("abc",)),
        ("EGU", ("user-1",)),
        ("W", ("a", "b")),
        ("C", ("",)),
        ("C", ("éàü",)),
    ]:
        assert advisory_key(domain, *parts) == app_advisory_key(domain, *parts)


def test_advisory_key_fits_a_signed_bigint():
    # pg_advisory_xact_lock takes a bigint, so an unsigned read of the digest
    # would raise on roughly half of all ids.
    for n in range(200):
        key = computer_advisory_key(f"computer-{n}")
        assert -(2**63) <= key < 2**63


def test_computer_and_egress_keys_agree_with_the_app():
    from src.server.database.computer import (
        computer_advisory_key as app_computer_key,
    )
    from src.server.database.egress_grants import lock_user_egress_state

    for value in ["computer-1", PRIMARY, "x"]:
        assert computer_advisory_key(value) == app_computer_key(value)

    # The egress lock's key is computed inline rather than exported, so the
    # domain string is read off the one function that takes it.
    assert 'advisory_key("EGU"' in inspect.getsource(lock_user_egress_state)
    assert user_egress_lock_key("u") == advisory_key("EGU", "u")


def test_the_two_domains_do_not_collide():
    assert computer_advisory_key("shared-id") != user_egress_lock_key("shared-id")


def test_a_widened_suffix_still_fits_the_column():
    # dir_name is VARCHAR(64) and a re-slug widens the hex, so a long project
    # name must lose slug, not overflow: truncation is not a UniqueViolation.
    workspace_id = "33333333-3333-4333-8333-333333333333"
    for hex_chars in (4, 8, 12, 16, 32):
        assert (
            len(workspace_dir_name("x" * 120, workspace_id, hex_chars=hex_chars))
            <= DIR_NAME_MAX
        )


def test_a_nameless_workspace_still_gets_a_folder():
    folder = workspace_dir_name(None, PRIMARY)
    assert folder.startswith("workspace-")


def test_sandbox_config_hash_agrees_with_the_app():
    from src.server.services.computer_manager import (
        ComputerBinding,
        ComputerManager,
    )

    rows = [
        {"kind": "daytona", "root_dir": "/home/workspace", "provider_config": None},
        {"kind": "docker", "root_dir": "/work", "provider_config": None},
        {
            "kind": "daytona",
            "root_dir": "/home/workspace",
            "provider_config": {"target": "us", "snapshot": "s1"},
        },
        {"kind": "daytona", "root_dir": "/home/workspace", "provider_config": {}},
    ]
    for row in rows:
        binding = ComputerBinding(
            workspace_id="w",
            computer_id=PRIMARY,
            kind=row["kind"],
            root_dir=row["root_dir"],
            provider_config=row["provider_config"],
        )
        # A bound row names both fields, so the deployment config the app falls
        # back to is unreachable and any object will do for it.
        app_hash = ComputerManager._compute_sandbox_config_hash(
            _unreachable_config(), binding
        )
        assert sandbox_config_hash(row) == app_hash


def test_the_stamp_carries_the_hash_and_the_values_behind_it():
    row = {"kind": "daytona", "root_dir": "/home/workspace", "provider_config": None}
    assert sandbox_config_stamp(row) == {
        "sandbox_config_hash": sandbox_config_hash(row),
        "sandbox_provider": "daytona",
        "sandbox_working_dir": "/home/workspace",
    }


def test_an_empty_provider_config_hashes_as_no_override():
    # `if computer.get("provider_config")` on both sides: a row that overrides
    # nothing has to hash as the deployment's own, or the whole fleet takes the
    # sandbox migration path on its next start.
    bare = {"kind": "daytona", "root_dir": "/w", "provider_config": None}
    empty = {"kind": "daytona", "root_dir": "/w", "provider_config": {}}
    assert sandbox_config_hash(bare) == sandbox_config_hash(empty)


def test_tier_rank_covers_every_tier_the_api_accepts():
    from src.server.models.computer import ComputerCreate

    field = ComputerCreate.model_fields["resource_tier"]
    assert set(typing.get_args(field.annotation)) == set(TIER_RANK)


# ── Primary selection ────────────────────────────────────────────────────────


def test_an_existing_primary_wins_over_the_most_active():
    # `_user_computers` hands this list in activity order, so the most recently
    # used machine is first and the flagged one is not.
    computers = [_computer(FOLDING), _computer(PRIMARY, is_primary=True)]
    assert elect_primary(computers)["computer_id"] == PRIMARY


def test_the_most_active_wins_when_no_primary_exists():
    computers = [_computer(FOLDING), _computer(PRIMARY)]
    assert elect_primary(computers)["computer_id"] == FOLDING


def test_no_computers_elects_nothing():
    assert elect_primary([]) is None


def test_election_is_stable_across_a_rerun():
    computers = [_computer(FOLDING), _computer(PRIMARY)]
    first = elect_primary(computers)
    # The apply flags the winner, which is the only row that changes.
    after = [
        {**c, "is_primary": c["computer_id"] == first["computer_id"]} for c in computers
    ]
    assert elect_primary(after)["computer_id"] == first["computer_id"]


# ── Tier ─────────────────────────────────────────────────────────────────────


def test_the_primary_takes_the_largest_tier_the_user_spans():
    computers = [
        _computer(PRIMARY, is_primary=True, tier="standard"),
        _computer(FOLDING, tier="performance"),
    ]
    assert highest_tier(computers) == "performance"


def test_a_larger_primary_is_never_folded_down():
    computers = [
        _computer(PRIMARY, is_primary=True, tier="max"),
        _computer(FOLDING, tier="standard"),
    ]
    assert highest_tier(computers) == "max"


def test_an_unknown_tier_never_wins():
    computers = [
        _computer(PRIMARY, is_primary=True, tier="standard"),
        _computer(FOLDING, tier="titanium"),
    ]
    assert highest_tier(computers) == "standard"


def test_highest_tier_of_nothing_is_nothing():
    assert highest_tier([]) is None


# ── Egress grant survivor ────────────────────────────────────────────────────


def test_the_primarys_incumbent_beats_a_newer_challenger():
    # The caller orders by `updated_at DESC`, so the folding row is first here
    # precisely because it is newer.
    group = [_grant("newer", FOLDING), _grant("incumbent", PRIMARY)]
    assert pick_grant_survivor(group, PRIMARY)["grant_id"] == "incumbent"


def test_the_freshest_folding_grant_wins_when_the_primary_holds_none():
    group = [
        _grant("newer", FOLDING),
        _grant("older", "33333333-3333-4333-8333-333333333333"),
    ]
    assert pick_grant_survivor(group, PRIMARY)["grant_id"] == "newer"


def test_a_grant_the_backfill_left_unlabelled_can_win():
    group = [_grant("unlabelled", None)]
    assert pick_grant_survivor(group, PRIMARY)["grant_id"] == "unlabelled"


def test_an_unlabelled_grant_loses_to_the_primarys_own():
    group = [_grant("unlabelled", None), _grant("incumbent", PRIMARY)]
    assert pick_grant_survivor(group, PRIMARY)["grant_id"] == "incumbent"


def test_the_survivor_is_the_same_on_a_rerun():
    group = [_grant("newer", FOLDING), _grant("older", FOLDING)]
    survivor = pick_grant_survivor(group, PRIMARY)
    # The apply adopts the survivor onto the primary and revokes the rest, so a
    # re-run sees one active grant, now labelled with the primary.
    after = [{**survivor, "computer_id": PRIMARY}]
    assert pick_grant_survivor(after, PRIMARY)["grant_id"] == survivor["grant_id"]


# ── Folder names on the surviving computer ───────────────────────────────────


def test_a_free_name_is_kept_so_a_rerun_writes_nothing():
    workspace = {"workspace_id": PRIMARY, "name": "Alpha", "dir_name": "alpha-1234"}
    assert _free_dir_name(workspace, {"other-9999"}) == ("alpha-1234", False)


def test_a_taken_name_is_reslugged_with_more_hex():
    workspace = {"workspace_id": PRIMARY, "name": "Alpha", "dir_name": "alpha-1234"}
    name, reslugged = _free_dir_name(workspace, {"alpha-1234"})
    assert name == workspace_dir_name("Alpha", PRIMARY, hex_chars=4)
    assert reslugged is True


def test_the_suffix_widens_until_it_is_free():
    workspace = {"workspace_id": PRIMARY, "name": "Alpha", "dir_name": "alpha-1234"}
    taken = {
        "alpha-1234",
        workspace_dir_name("Alpha", PRIMARY, hex_chars=4),
        workspace_dir_name("Alpha", PRIMARY, hex_chars=8),
    }
    assert _free_dir_name(workspace, taken)[0] == workspace_dir_name(
        "Alpha", PRIMARY, hex_chars=12
    )


def test_a_workspace_with_no_folder_yet_gets_one_without_counting_a_rename():
    workspace = {"workspace_id": PRIMARY, "name": "Alpha", "dir_name": None}
    assert _free_dir_name(workspace, set()) == (
        workspace_dir_name("Alpha", PRIMARY),
        False,
    )


def test_every_workspace_on_one_computer_ends_up_with_its_own_folder():
    # The collision the fold actually produces: the same project name, and the
    # same folder name, on machines that are about to become one.
    workspaces = [
        {
            "workspace_id": str(uuid.uuid4()),
            "name": "Research",
            "dir_name": "research-dupe",
        }
        for _ in range(25)
    ]
    taken: set[str] = set()
    for workspace in workspaces:
        name, _ = _free_dir_name(workspace, taken)
        assert name not in taken
        taken.add(name)
    assert len(taken) == len(workspaces)


def test_an_exhausted_suffix_raises_rather_than_colliding():
    workspace = {"workspace_id": PRIMARY, "name": "Alpha", "dir_name": "alpha-1234"}
    taken = {"alpha-1234"} | {
        workspace_dir_name("Alpha", PRIMARY, hex_chars=4 + 4 * n) for n in range(4)
    }
    with pytest.raises(RuntimeError, match="No free folder name"):
        _free_dir_name(workspace, taken)


# ── The SQL the migrations and the runtime pin ───────────────────────────────


def test_the_adoption_update_carries_the_runtimes_not_exists_guard():
    # 047's index has no status predicate and a revoked row on the primary is
    # steady state, so without this guard one folded grant rolls the user back.
    statement = _statement(_adopt_grants, "UPDATE sandbox_egress_grants g")
    assert "NOT EXISTS" in statement
    for clause in (
        "o.kind = g.kind",
        "o.connection_id IS NOT DISTINCT FROM g.connection_id",
        "o.server_name IS NOT DISTINCT FROM g.server_name",
    ):
        assert clause in statement

    from src.server.database.egress_grants import _adopt_grants_onto_computer

    app = inspect.getsource(_adopt_grants_onto_computer)
    assert "NOT EXISTS" in app


def test_a_revoked_losers_claimants_move_to_the_survivor():
    # 048 spares a grant while any live project claims it, so revoking a loser
    # without carrying its claims over would let the survivor's next sweep
    # read those projects as having left.
    statement = _statement(_adopt_grants, "INSERT INTO sandbox_egress_grant_claims")
    assert "SELECT %(survivor)s::uuid, workspace_id" in statement
    assert "WHERE grant_id = %(grant_id)s" in statement
    assert "ON CONFLICT DO NOTHING" in statement


def test_the_reap_spares_a_sandbox_a_legacy_row_still_names():
    # A workspace 046 left with computer_id NULL names its sandbox by id until
    # it resolves through the app; its files are only in that sandbox.
    statement = _statement(_reap, "SELECT c.computer_id, c.user_id")
    assert "w.sandbox_id = c.provider_ref" in statement
    assert statement.count("{FENCE_LIVE_WORKSPACE}") == 2


def test_the_taken_folder_names_do_not_filter_on_status():
    # idx_workspaces_computer_dir has no status predicate and a delete is a
    # tombstone, so a deleted project still owns its folder name.
    statement = _statement(
        _rebind_folding_workspaces, "SELECT dir_name FROM workspaces"
    )
    assert FENCE_NOT_DELETED not in statement
    assert "status" not in statement


def test_the_rebind_moves_binding_shadow_and_stamp_in_one_statement():
    # The runtime's rebind_workspace_to_computer moves the binding and the shadow
    # in one UPDATE against a comp CTE; a second statement for the shadow can miss.
    statement = _statement(_rebind, "WITH comp AS (")
    assert statement.count("UPDATE workspaces") == 1
    for clause in (
        "computer_id = comp.computer_id",
        "status = comp.status",
        "sandbox_id = comp.provider_ref",
        "resource_tier = comp.resource_tier",
        "is_always_on = comp.is_always_on",
        "platform_secret_version = comp.platform_secret_version",
        "config = COALESCE(w.config, '{{}}'::jsonb) || %(stamp)s::jsonb",
        "w.computer_id IS NOT DISTINCT FROM %(expected)s",
    ):
        assert clause in statement
    # mcp_config_version is the column every config write advances and the grant
    # CAS gates on, so the shadow copy must leave it alone.
    assert "mcp_config_version" not in statement


def test_the_retirement_update_stops_the_row_billing():
    statement = _statement(_retire_computer, "UPDATE computers c")
    assert "is_always_on = FALSE" in statement
    assert "is_primary = FALSE" in statement
    assert "stopped_at = NOW()" in statement


def test_the_status_channels_agree_with_the_app():
    from src.server.services.workspace_status_pubsub import (
        status_channel,
        workspace_status_channel as app_workspace_channel,
    )

    assert computer_status_channel(PRIMARY) == status_channel(PRIMARY)
    assert workspace_status_channel("w-1") == app_workspace_channel("w-1")


def test_the_layout_version_agrees_with_the_app():
    from ptc_agent.core.sandbox.migration import (
        CURRENT_LAYOUT_VERSION as app_layout,
    )

    assert CURRENT_LAYOUT_VERSION == app_layout


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "has_computers,present,absent",
    [
        (True, "count(DISTINCT w.computer_id)", "w.sandbox_id IS NOT NULL"),
        (False, "count(DISTINCT w.sandbox_id)", "JOIN computers"),
    ],
)
async def test_presync_reads_whichever_schema_is_on_disk(
    has_computers, present, absent
):
    # Presync runs before 046, and again after it when a window lands the
    # migration and then aborts before the fold, so both bodies are reachable.
    conn = _FakeConn([])
    await _presync_targets(conn, None, has_computers=has_computers)
    sql, _ = conn.statements[-1]
    assert present in sql
    assert absent not in sql


# ── Safety against a running backend ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_dry_run_takes_no_advisory_lock():
    # A fleet-wide dry run would otherwise hold EGU(user) and C(computer) against
    # every live grant sync in turn.
    conn = _FakeConn([("LEFT JOIN LATERAL", [_machine(PRIMARY, live=1)])])
    sink = _RecordingSink()
    await _consolidate_user(conn, "user-1", apply=False, sink=sink, watermarks={})
    assert conn.locks() == []
    assert not conn.wrote()
    # The run still reports what it would do.
    await sink.flush()
    assert sink.tally["primary_elected"] == 1


@pytest.mark.asyncio
async def test_an_apply_locks_the_user_the_primary_and_every_folding_machine():
    third = "33333333-3333-4333-8333-333333333333"
    machines = [
        _machine(PRIMARY, is_primary=True, live=1),
        _machine(third),
        _machine(FOLDING),
    ]
    conn = _FakeConn(
        [
            ("LEFT JOIN LATERAL", machines),
            ("SET status = 'deleted'", [_retired(FOLDING)]),
        ]
    )
    sink = _RecordingSink()
    await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    assert conn.locks() == [
        user_egress_lock_key("user-1"),
        computer_advisory_key(PRIMARY),
        # Sorted by id, so two runs cannot take the same pair in two orders.
        *[computer_advisory_key(cid) for cid in sorted([FOLDING, third])],
    ]
    await sink.flush()
    assert sink.tally["computer_retired"] == 2
    # Every retired machine wakes the workers that cached it.
    assert {cid for cid, _ in sink.published} == {FOLDING, third}
    assert {status for _, status in sink.published} == {"deleted"}


@pytest.mark.asyncio
async def test_the_primarys_columns_are_folded_in_one_statement():
    # Four UPDATEs against one row, two of them dragging a shadow UPDATE along,
    # became one pair: the decisions are pure functions of the rows already read.
    machines = [
        _machine(PRIMARY, live=1),
        _machine(FOLDING, tier="max", always_on=True),
    ]
    conn = _FakeConn(
        [
            ("LEFT JOIN LATERAL", machines),
            ("SET status = 'deleted'", [_retired(FOLDING)]),
        ]
    )
    sink = _RecordingSink()
    await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    fold = [sql for sql, _ in conn.statements if "is_primary = %(is_primary)s" in sql]
    assert len(fold) == 1
    for column in ("is_primary", "resource_tier", "is_always_on"):
        assert f"{column} = %({column})s" in fold[0]
    assert fold[0].count("UPDATE computers") == 1
    assert fold[0].count("UPDATE workspaces") == 1
    # is_primary is not a workspaces column, so only the two shadowed ones copy.
    shadow = fold[0].split("UPDATE workspaces")[1]
    assert "is_primary" not in shadow
    assert "resource_tier = %(resource_tier)s" in shadow
    assert "is_always_on = %(is_always_on)s" in shadow
    await sink.flush()
    for action in ("primary_elected", "tier_raised", "always_on_folded"):
        assert sink.tally[action] == 1


@pytest.mark.asyncio
async def test_a_retired_sandbox_is_stopped_and_the_audit_says_so():
    machines = [
        _machine(PRIMARY, is_primary=True, live=1),
        _machine(FOLDING, provider_ref="sbx-1", status="running"),
    ]
    conn = _FakeConn(
        [
            ("LEFT JOIN LATERAL", machines),
            ("SET status = 'deleted'", [_retired(FOLDING)]),
        ]
    )
    sink = _RecordingSink()
    await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    assert sink.stopped == ["sbx-1"]
    assert sink.tally["provider.sandbox_stopped"] == 1
    await sink.flush()
    record = sink.one("computer_retired")
    assert record["stopped"] is True
    assert record["after"]["is_always_on"] is False


@pytest.mark.asyncio
async def test_a_sandbox_that_will_not_stop_does_not_fail_the_fold():
    machines = [
        _machine(PRIMARY, is_primary=True, live=1),
        _machine(FOLDING, provider_ref="sbx-1", status="running"),
    ]
    conn = _FakeConn(
        [
            ("LEFT JOIN LATERAL", machines),
            ("SET status = 'deleted'", [_retired(FOLDING)]),
        ]
    )
    sink = _RecordingSink(outcome="failed")
    await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    await sink.flush()
    assert sink.tally["computer_retired"] == 1
    assert sink.tally["provider.sandbox_stop_failed"] == 1
    assert "provider.sandbox_stopped" not in sink.tally
    # The reap pass deletes it after the soak, so the run itself still succeeds.
    assert sink.failed() is False
    assert sink.one("computer_retired")["stopped"] is False


# ── What a rolled-back user may not leave behind ─────────────────────────────


@pytest.mark.asyncio
async def test_a_committed_user_lands_its_records_its_tally_and_its_wakeups():
    sink = _RecordingSink()
    sink.queue("workspace_rebound", also=("dir_name_reslugged",), workspace_id="w-1")
    sink.queue_status(FOLDING, "deleted")
    sink.hold_ref("daytona:sbx-1")
    await sink.flush()
    assert [r["action"] for r in sink.records] == ["workspace_rebound"]
    assert sink.tally == {"workspace_rebound": 1, "dir_name_reslugged": 1}
    assert sink.refs == ["daytona:sbx-1"]
    assert len(sink.flushed) == 1


@pytest.mark.asyncio
async def test_a_rolled_back_user_leaves_no_record_no_tally_and_no_wakeup():
    # The audit's stated job is manual reversal, so a record for a write that
    # never landed would have a human undo something that never happened.
    sink = _RecordingSink()
    sink.queue("workspace_rebound", workspace_id="w-1")
    sink.queue_status(FOLDING, "deleted")
    sink.hold_ref("daytona:sbx-1")
    sink.drop_pending()
    await sink.flush()
    assert sink.records == []
    assert sink.tally == {}
    assert sink.refs == []
    assert sink.flushed == []


def test_a_provider_call_is_recorded_before_any_commit():
    # Stopping a sandbox is irreversible, so a rollback must not erase it, and
    # the provider. prefix keeps it apart from the committed row counts.
    sink = _RecordingSink()
    sink.note(
        "provider.sandbox_stopped", record="sandbox_stopped", provider_ref="sbx-1"
    )
    sink.drop_pending()
    assert [r["action"] for r in sink.records] == ["sandbox_stopped"]
    assert sink.tally == {"provider.sandbox_stopped": 1}


@pytest.mark.asyncio
async def test_a_fold_that_aborts_mid_way_records_nothing_it_rolled_back():
    machines = [
        _machine(PRIMARY, is_primary=True, live=1),
        _machine(FOLDING, live=1),
    ]
    conn = _FakeConn(
        [
            ("LEFT JOIN LATERAL", machines),
            (
                "ORDER BY created_at, workspace_id",
                lambda params: [_workspace()]
                if params["computer_id"] == FOLDING
                else [],
            ),
            (
                "RETURNING w.workspace_id, w.computer_id, w.dir_name",
                [{"workspace_id": WORKSPACE}],
            ),
            # No rule answers the retirement, so _retire_computer finds no row.
        ]
    )
    sink = _RecordingSink()
    with pytest.raises(RuntimeError, match="not retirable"):
        await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    # What the driver does when a user's transaction rolls back.
    sink.drop_pending()
    await sink.flush()
    assert sink.records == []
    assert sink.tally == {}


# ── The workspace-identity invariant ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_identity_digest_only_covers_the_users_being_folded():
    # Unscoped, any unrelated user's concurrent project insert flips the digest
    # and aborts the run accusing the job of re-keying a workspace.
    conn = _FakeConn([("md5(", [{"n": 1, "deleted": 0, "digest": "d"}])])
    await _workspace_identity(conn, ["user-1", "user-2"])
    sql, params = conn.statements[-1]
    assert "WHERE user_id = ANY(%(users)s::text[])" in sql
    assert params == {"users": ["user-1", "user-2"]}


@pytest.mark.asyncio
async def test_a_dry_run_never_digests_the_workspaces_table():
    # A dry run writes nothing, so the invariant it checks is vacuous there.
    conn = _FakeConn(
        [
            ("SELECT user_id FROM (", [{"user_id": "user-1"}]),
            ("LEFT JOIN LATERAL", []),
        ]
    )
    sink = _RecordingSink()
    await _consolidate(conn, apply=False, user_id=None, sink=sink, presync_audit=None)
    assert not any("md5(" in sql for sql, _ in conn.statements)


# ── Layout version ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_fold_onto_a_primary_of_unknown_layout_is_skipped():
    # Neither fact is available: layout_version 0 is 046 never stamping it, and
    # with no origin workspace nothing says which folder the root sweep fills.
    machines = [
        _machine(PRIMARY, is_primary=True, layout_version=0, live=1),
        _machine(FOLDING, layout_version=0, live=1),
    ]
    conn = _FakeConn([("LEFT JOIN LATERAL", machines)])
    sink = _RecordingSink()
    await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    await sink.flush()
    assert sink.tally == {"user_skipped": 1}
    assert not conn.wrote()
    assert sink.one("user_skipped")["reason"] == "layout_unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "layout_version,origin_workspace_id",
    [
        # Already v4, wherever it came from.
        (CURRENT_LAYOUT_VERSION, None),
        # Still v3 or unstamped, but 046 recorded the folder its root sweep
        # fills, so the sweep cannot land in a folded sibling's folder.
        (0, "a1111111-1111-4111-8111-111111111111"),
        (3, "a1111111-1111-4111-8111-111111111111"),
    ],
)
async def test_a_primary_that_can_prove_its_layout_folds(
    layout_version, origin_workspace_id
):
    machines = [
        _machine(
            PRIMARY,
            is_primary=True,
            layout_version=layout_version,
            origin_workspace_id=origin_workspace_id,
            live=1,
        ),
        _machine(FOLDING, layout_version=layout_version, live=1),
    ]
    conn = _FakeConn(
        [
            ("LEFT JOIN LATERAL", machines),
            (
                "ORDER BY created_at, workspace_id",
                lambda params: [_workspace()]
                if params["computer_id"] == FOLDING
                else [],
            ),
            (
                "RETURNING w.workspace_id, w.computer_id, w.dir_name",
                [{"workspace_id": WORKSPACE}],
            ),
            ("SET status = 'deleted'", [_retired(FOLDING)]),
        ]
    )
    sink = _RecordingSink()
    await _consolidate_user(conn, "user-1", apply=True, sink=sink, watermarks={})
    await sink.flush()
    assert "user_skipped" not in sink.tally
    assert sink.tally["workspace_rebound"] == 1
    assert sink.tally["computer_retired"] == 1


# ── Helpers ──────────────────────────────────────────────────────────────────


def _statement(fn, first_line: str) -> str:
    """The one SQL literal inside *fn* that starts with *first_line*."""
    source = inspect.getsource(fn)
    start = source.index(first_line)
    return source[start : source.index('"""', start)]


def _machine(
    computer_id: str,
    *,
    is_primary: bool = False,
    status: str = "stopped",
    tier: str = "standard",
    always_on: bool = False,
    layout_version: int = CURRENT_LAYOUT_VERSION,
    origin_workspace_id: Optional[str] = None,
    live: int = 0,
    provider_ref: Optional[str] = None,
) -> dict[str, Any]:
    """A computers row in the shape `_user_computers` returns."""
    return {
        "computer_id": computer_id,
        "user_id": "user-1",
        "kind": "daytona",
        "provider_ref": provider_ref,
        "name": "My computer",
        "is_primary": is_primary,
        "status": status,
        "resource_tier": tier,
        "is_always_on": always_on,
        "platform_secret_version": 0,
        "mcp_config_version": 0,
        "root_dir": "/home/workspace",
        "layout_version": layout_version,
        "origin_workspace_id": origin_workspace_id,
        "provider_config": None,
        "artifacts": None,
        "last_activity_at": None,
        "stopped_at": None,
        "created_at": None,
        "updated_at": None,
        "config": None,
        "activity_at": None,
        "live_workspaces": live,
    }


def _workspace() -> dict[str, Any]:
    """A workspaces row in the shape `_live_workspaces` returns."""
    return {
        "workspace_id": WORKSPACE,
        "user_id": "user-1",
        "name": "Alpha",
        "dir_name": "alpha-1234",
        "status": "stopped",
        "computer_id": FOLDING,
        "last_activity_at": None,
        "config": None,
    }


def _retired(computer_id: str) -> dict[str, Any]:
    """What `_retire_computer` returns: the row, and no stranded workspace."""
    return {**_machine(computer_id), "shadowed_workspace_ids": []}


class _RecordingSink(_Sink):
    """The real sink with the two halves that dial the deployment stubbed out.

    Records land in a list rather than a file, so a test can assert both what a
    commit wrote and what a rollback did not.
    """

    def __init__(self, outcome: str = "stopped") -> None:
        super().__init__(None)
        self._outcome = outcome
        self.records: list[dict[str, Any]] = []
        self.published: list[tuple[str, str]] = []
        self.flushed: list[tuple[str, dict[str, Any]]] = []
        self.stopped: list[str] = []

    def queue_status(self, computer_id, status, *, workspace_ids=()) -> None:
        super().queue_status(computer_id, status, workspace_ids=workspace_ids)
        self.published.append((computer_id, status))

    async def stop_sandbox(self, row) -> str:
        self.stopped.append(row["provider_ref"])
        return self._outcome

    async def _publish(self) -> None:
        self.flushed.extend(self._publishes)
        self._publishes.clear()

    def _emit(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def one(self, action: str) -> dict[str, Any]:
        matching = [r for r in self.records if r["action"] == action]
        assert len(matching) == 1, f"{action}: {len(matching)} record(s)"
        return matching[0]


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self._conn = conn
        self._rows: list[Any] = []

    async def execute(self, sql, params=None):
        self._rows = self._conn.record(sql, params)
        return self

    async def fetchall(self):
        return list(self._rows)

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    @property
    def rowcount(self) -> int:
        return len(self._rows)


class _FakeConn:
    """Answers the job's reads by SQL substring and keeps every statement.

    The job's guards live in the statements it does and does not send, which is
    what these rules make assertable without a Postgres.
    """

    def __init__(self, rules) -> None:
        self._rules = list(rules)
        self.statements: list[tuple[str, Any]] = []

    def record(self, sql: str, params: Any) -> list[Any]:
        self.statements.append((sql, params))
        for needle, answer in self._rules:
            if needle in sql:
                return answer(params) if callable(answer) else list(answer)
        return []

    @asynccontextmanager
    async def cursor(self, row_factory=None):
        yield _FakeCursor(self)

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def execute(self, sql, params=None):
        cursor = _FakeCursor(self)
        await cursor.execute(sql, params)
        return cursor

    def locks(self) -> list[int]:
        return [
            params[0]
            for sql, params in self.statements
            if "pg_advisory_xact_lock" in sql
        ]

    def wrote(self) -> bool:
        return any("UPDATE" in sql for sql, _ in self.statements)


class _unreachable_config:
    """Stands in for the deployment config the bound-row branch never reads."""

    def __getattr__(self, name):
        raise AssertionError(
            f"a bound computer row must not fall back to the deployment config "
            f"(reached .{name})"
        )
