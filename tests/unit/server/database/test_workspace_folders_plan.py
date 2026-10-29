"""Which folders a settle moves: the plan is where every data-loss case is decided.

The planner is pure, so each case the move script can meet on disk is pinned
here as rows in, moves out. The script only renames what the plan says; a plan
that lands two rows on one folder, or a row on a folder a staying row holds,
is the bug that mixes two workspaces' files.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import replace

from src.server.database.workspace_folders import (
    TOMBSTONES_PER_PASS,
    FolderRow,
    _row,
    accepted_landings,
    leftovers_path,
    moving_path,
    plan_folder_moves,
)

A, B, C = "aaaaaaaa-0000-4000-8000-000000000001", "bbbbbbbb-0000-4000-8000-000000000002", "cccccccc-0000-4000-8000-000000000003"
D = "dddddddd-0000-4000-8000-000000000004"


def row(workspace_id, name, dir_name, *, deleted=False, previous=()):
    return FolderRow(workspace_id, name, dir_name, deleted, tuple(previous))


def moves(plan):
    return {m.workspace_id: (m.source, m.target) for m in plan.moves}


def test_a_folder_already_named_after_its_workspace_stays():
    assert not plan_folder_moves([row(A, "Research", "Research")])


def home(dir_name, *, previous=()):
    return FolderRow(A, "Home", dir_name, False, tuple(previous), home=True)


def test_home_keeps_the_folder_it_was_bound_to():
    """A workspace named Home before the name was reserved stays out of it."""
    assert not plan_folder_moves([home("Home")])
    assert not plan_folder_moves([home("Home"), row(B, "Home", "Research")])


def test_home_waits_on_a_placeholder_while_a_workspace_holds_its_folder():
    """A workspace named Home before the name was reserved keeps its folder."""
    assert not plan_folder_moves([home("Home-7d59"), row(B, "Home", "Home")])


def test_home_moves_in_once_the_workspace_holding_its_folder_is_renamed():
    plan = plan_folder_moves([home("Home-7d59"), row(B, "Research", "Home")])
    assert moves(plan) == {A: ("Home-7d59", "Home"), B: ("Home", "Research")}


def test_home_bound_on_a_placeholder_moves_in_when_nothing_holds_its_folder():
    """A computer on the old layout, or a deleted workspace's folder since
    cleared, left Home on a placeholder with no live workspace in the way."""
    assert moves(plan_folder_moves([home("Home-7d59")])) == {A: ("Home-7d59", "Home")}
    assert not plan_folder_moves([home("Home-7d59")], busy={A})


def test_staged_home_that_cannot_land_goes_to_a_home_placeholder():
    rows = [
        home(moving_path(A), previous=["Research"]),
        row(B, "Home", "Home"),
        row(C, "Research", "Research"),
    ]
    (move,) = plan_folder_moves(rows).moves
    assert move.target.startswith("Home-")


def test_a_renamed_workspace_moves_to_its_new_name():
    plan = plan_folder_moves([row(A, "Macro", "Research")])
    assert moves(plan) == {A: ("Research", "Macro")}


def test_a_case_only_rename_moves_too():
    """The folder is the spelling the user sees; staging makes the rename
    work on a case-insensitive disk, where Research and research are one entry."""
    plan = plan_folder_moves([row(A, "Research", "research")])
    assert moves(plan) == {A: ("research", "Research")}


def test_a_busy_workspace_keeps_its_folder():
    """A running turn has absolute paths into it."""
    assert not plan_folder_moves([row(A, "Macro", "Research")], busy={A})


def test_two_workspaces_can_trade_names():
    """Every mover leaves for staging before any lands, so a swap is two moves."""
    plan = plan_folder_moves([row(A, "Y", "X"), row(B, "X", "Y")])
    assert moves(plan) == {A: ("X", "Y"), B: ("Y", "X")}


def test_a_target_held_by_a_staying_row_holds_the_mover_back():
    plan = plan_folder_moves([row(A, "Y", "X"), row(B, "X", "Y")], busy={B})
    assert not plan


def test_holding_back_cascades_until_nothing_changes():
    """C stays because it is busy, so B cannot take C's folder, so B stays,
    so A cannot take B's folder: a single pass would move A onto B."""
    rows = [row(A, "B-folder", "A-folder"), row(B, "C-folder", "B-folder"), row(C, "D", "C-folder")]
    assert not plan_folder_moves(rows, busy={C})


def test_a_target_held_case_insensitively_is_held():
    plan = plan_folder_moves([row(A, "Notes", "Old"), row(B, "Other", "notes")], busy={B})
    assert not plan


def test_a_staged_row_lands_on_its_target_when_free():
    """A crash between staging and landing leaves the row pointed at staging;
    the next settle finishes the move."""
    plan = plan_folder_moves([row(A, "Macro", moving_path(A), previous=("Research",))])
    assert moves(plan) == {A: ("Research", "Macro")}
    assert plan.moves[0].staged


def test_a_staged_row_whose_target_is_held_goes_back_to_its_old_folder():
    """A folder under _internal is one the agent cannot use, so a staged row
    always moves, even when its name cannot be had yet."""
    rows = [row(A, "Macro", moving_path(A), previous=("Research",)), row(B, "Macro", "Macro")]
    plan = plan_folder_moves(rows, busy={B})
    assert moves(plan) == {A: ("Research", "Research")}


def test_a_staged_row_with_no_room_to_copy_out_waits_while_its_backoff_lasts():
    """Staged across an archive, it leaves staging only by a copy: moved on
    every acquisition, it would copy until the disk filled, then delete, on
    whichever target the last pass tried."""
    staged = FolderRow(A, "Macro", moving_path(A), False, ("Research",), blocked="Macro")
    assert not plan_folder_moves([staged])
    assert not plan_folder_moves([replace(staged, blocked="Research")])
    assert moves(plan_folder_moves([replace(staged, blocked=None)])) == {A: ("Research", "Macro")}


def test_a_staged_row_falls_back_to_a_placeholder_when_its_old_folder_is_landed_on():
    """Its old folder is another mover's target this pass: going back there
    would put two workspaces in one folder."""
    rows = [
        row(A, "Macro", moving_path(A), previous=("Research",)),
        row(B, "Macro", "Macro"),
        row(C, "Research", "Elsewhere"),
    ]
    plan = plan_folder_moves(rows, busy={B})
    placeholder = "Macro-" + hashlib.md5(A.encode()).hexdigest()[:8]
    assert moves(plan)[A] == ("Research", placeholder)
    assert moves(plan)[C] == ("Elsewhere", "Research")


def test_a_staged_rows_placeholder_is_never_a_folder_another_row_keeps():
    """A workspace can be named exactly like the placeholder; landing there
    would set its files aside as unowned."""
    placeholder = "Macro-" + hashlib.md5(A.encode()).hexdigest()[:8]
    rows = [
        row(A, "Macro", moving_path(A), previous=("Research",)),
        row(B, "Macro", "Macro"),
        row(C, "Research", "Elsewhere"),
        row(D, placeholder, placeholder),
    ]
    plan = plan_folder_moves(rows, busy={B})
    landed = moves(plan)[A][1]
    assert landed.startswith("Macro-")
    assert landed.casefold() not in {placeholder.casefold(), "macro", "research"}


def test_two_rows_folding_to_one_folder_move_one_at_a_time():
    """Only an older build could write both; the second waits for a later settle."""
    plan = plan_folder_moves([row(A, "Macro", "a"), row(B, "MACRO", "b")])
    assert list(moves(plan)) == [A]


def test_a_staged_row_claims_its_target_before_a_row_folding_alike():
    """The staged row moves either way; letting the other take the name too
    would land both on one folder."""
    rows = [row(A, "Macro", "a"), row(B, "MACRO", moving_path(B), previous=("Research",))]
    assert moves(plan_folder_moves(rows)) == {B: ("Research", "MACRO")}


def test_two_staged_rows_folding_alike_never_share_a_landing():
    rows = [
        row(A, "Macro", moving_path(A), previous=("Old A",)),
        row(B, "MACRO", moving_path(B), previous=("Old B",)),
    ]
    assert moves(plan_folder_moves(rows)) == {A: ("Old A", "Macro"), B: ("Old B", "Old B")}


def test_a_name_with_no_folder_spelling_keeps_its_folder():
    assert not plan_folder_moves([row(A, "code", "code-1a2b")])


def test_top_level_tombstones_are_cleared_first_and_leftovers_are_not():
    """A deleted workspace's folder still holds the name until it is moved
    under _internal; one already there is only waiting for its rm."""
    rows = [
        row(A, "Research", "Research", deleted=True),
        row(B, "Old", "_internal/leftovers/" + B, deleted=True),
        row(C, "Research", "Research-1a2b"),
    ]
    plan = plan_folder_moves(rows)
    assert [t.workspace_id for t in plan.tombstones] == [A]
    assert moves(plan) == {C: ("Research-1a2b", "Research")}


def test_a_folder_its_owner_kept_stays_until_its_name_is_wanted():
    """Kept on the owner's machine, it is theirs: no pass touches it, nor runs
    for it, while no live row targets its name."""
    kept = replace(row(A, "Research", "Research", deleted=True), kept=True)
    plan = plan_folder_moves([kept, row(B, "Macro", "Macro")])
    assert not plan
    assert plan.deferred == (A,)


def test_a_kept_folder_left_past_the_cap_holds_its_name():
    """Landing on one the pass leaves would push it aside unrecorded."""
    n = TOMBSTONES_PER_PASS + 5
    rows = [
        replace(row(f"{i:08d}-0000-4000-8000-00000000000d", f"N{i}", f"N{i}", deleted=True), kept=True)
        for i in range(n)
    ]
    rows += [row(f"{i:08d}-0000-4000-8000-00000000000a", f"N{i}", f"N{i}-{i:04x}") for i in range(n)]
    plan = plan_folder_moves(rows)
    assert len(plan.tombstones) == TOMBSTONES_PER_PASS
    assert {m.target for m in plan.moves} == {t.dir_name for t in plan.tombstones}


def test_a_kept_folder_whose_name_a_new_workspace_wants_is_set_aside():
    """The new workspace lands on the name; the kept folder goes under
    leftovers, where the settle records it and nothing removes it."""
    kept = replace(row(A, "Research", "Research", deleted=True), kept=True)
    plan = plan_folder_moves([kept, row(B, "Research", "Research-1a2b")])
    assert [t.workspace_id for t in plan.tombstones] == [A]
    assert moves(plan) == {B: ("Research-1a2b", "Research")}


def test_a_tombstone_does_not_hold_a_live_row_back():
    """It is cleared before any move lands, in the same pass."""
    rows = [row(A, "Research", "Research", deleted=True), row(B, "Research", "Research-1a2b")]
    assert moves(plan_folder_moves(rows)) == {B: ("Research-1a2b", "Research")}


def test_a_long_tombstone_backlog_clears_over_passes_blockers_first():
    """A pass holds the folder lock until it ends, so it carries a bounded
    number; the one a landing needs cannot wait behind the rest."""
    dead = [
        row(f"{i:08d}-0000-4000-8000-000000000000", f"Old {i}", f"Old {i}", deleted=True)
        for i in range(TOMBSTONES_PER_PASS + 5)
    ]
    rows = [*dead, row(A, "Research", "Research", deleted=True), row(B, "Research", "Research-1a2b")]
    plan = plan_folder_moves(rows)
    assert len(plan.tombstones) == TOMBSTONES_PER_PASS
    assert plan.tombstones[0].workspace_id == A
    assert moves(plan) == {B: ("Research-1a2b", "Research")}


def test_a_move_waits_for_a_tombstone_past_the_pass_cap():
    """Landing on a folder the pass leaves to its tombstone would set the dead
    content aside now, and the tombstone's own clearing would take the landed
    content next pass."""
    n = TOMBSTONES_PER_PASS + 5
    rows = [row(f"{i:08d}-0000-4000-8000-00000000000d", f"N{i}", f"N{i}", deleted=True) for i in range(n)]
    rows += [row(f"{i:08d}-0000-4000-8000-00000000000a", f"N{i}", f"N{i}-{i:04x}") for i in range(n)]
    plan = plan_folder_moves(rows)
    assert len(plan.tombstones) == TOMBSTONES_PER_PASS
    assert {m.target for m in plan.moves} == {t.dir_name for t in plan.tombstones}
    # The move script keeps what it journaled for the ones this pass leaves.
    cleared = {t.workspace_id for t in plan.tombstones}
    assert set(plan.deferred) == {r.workspace_id for r in rows if r.deleted} - cleared


def test_a_tombstone_a_file_change_holds_keeps_its_folder():
    """A change that read the folder before the delete is still writing to
    it; clearing it now would let that write recreate the folder."""
    rows = [row(A, "Research", "Research", deleted=True), row(B, "Research", "Research-1a2b")]
    plan = plan_folder_moves(rows, busy={A})
    assert not plan.tombstones
    assert not plan.moves
    assert plan.deferred == (A,)


def test_a_tombstone_deleted_mid_move_is_never_deferred():
    """Only the pass's journal knows where its last landing went."""
    dead = [
        row(f"{i:08d}-0000-4000-8000-000000000000", f"Old {i}", f"Old {i}", deleted=True)
        for i in range(TOMBSTONES_PER_PASS)
    ]
    staged = row(A, "Macro", moving_path(A), deleted=True, previous=("Research",))
    plan = plan_folder_moves([*dead, staged])
    assert len(plan.tombstones) == TOMBSTONES_PER_PASS
    assert plan.tombstones[0].workspace_id == A


def test_a_workspace_deleted_mid_move_is_cleared_like_any_tombstone():
    """Its content is in staging or where its unrecorded landing put it."""
    plan = plan_folder_moves([row(A, "Macro", moving_path(A), deleted=True, previous=("Research",))])
    assert [t.workspace_id for t in plan.tombstones] == [A]
    assert not plan.moves


def test_a_report_is_held_to_what_the_plan_asked():
    """The script runs beside the workspace's own code: a row it was not
    asked about, or a folder that is none of the row's own, never reaches the
    database."""
    plan = plan_folder_moves([
        row(A, "Macro", "Research"),
        row(B, "Old", "Old", deleted=True),
    ])
    moves, tombstones = accepted_landings(plan, {
        "moves": {A: "../../elsewhere", C: "Macro"},
        "tombstones": {B: "Research", C: None},
    })
    assert (moves, tombstones) == ({}, {})
    moves, tombstones = accepted_landings(plan, {
        "moves": {A: "Macro"}, "tombstones": {B: leftovers_path(B)},
    })
    assert (moves, tombstones) == ({A: "Macro"}, {B: leftovers_path(B)})
    for folder in ("tools", ".agents", "mcp_servers", "_internal"):
        assert accepted_landings(plan, {"moves": {A: folder}}) == ({}, {})


def test_a_reported_folder_is_one_the_database_gave_the_row():
    """A staging failure reports the journal's folder, which code in the
    sandbox can write: its staging, its old folder, its target, or a landing a
    pass recorded planning for it. Never a sibling's, and never one that
    merely spells a name, as the disk may fold case and a planner never did."""
    rows = [
        FolderRow(A, "Macro", moving_path(A), False, ("Research",), ("Q3 Notes", "Final")),
        row(B, "Notes", "Notes"),
        row(D, "Old", "Old", deleted=True),
    ]
    plan = plan_folder_moves(rows)
    [move] = plan.moves
    assert (move.source, move.target, move.landings) == ("Research", "Macro", ("Q3 Notes", "Final"))
    for folder in (moving_path(A), "Research", "Macro", "Q3 Notes", "Final"):
        assert accepted_landings(plan, {"moves": {A: folder}}) == ({A: folder}, {})
    for folder in (
        "Notes", "Old", "q3 notes", "Drafts", moving_path(B), "../../elsewhere", ".agents", 5,
    ):
        assert accepted_landings(plan, {"moves": {A: folder}}) == ({}, {})
    # Cleared in the same report, the deleted workspace's folder is still not A's.
    assert accepted_landings(
        plan, {"moves": {A: "Old"}, "tombstones": {D: leftovers_path(D)}}
    ) == ({}, {D: leftovers_path(D)})
    # A planned landing on a folder another mover leaves is still the plan's.
    swap = plan_folder_moves([row(A, "Y", "X"), row(B, "X", "Y")])
    assert accepted_landings(swap, {"moves": {A: "Y", B: "X"}}) == ({A: "Y", B: "X"}, {})


def test_only_a_staged_rows_recorded_landings_are_read():
    """Staging writes a fresh list, so what a settled row still carries was
    planned for a move that ended, and ``config`` is a client's to write too:
    anything in it that is no folder name was never planned."""
    base = {"workspace_id": uuid.UUID(A), "name": "Macro", "deleted": False,
            "previous_dir_names": ["Research"]}
    staged = {**base, "dir_name": moving_path(A)}
    assert _row({**staged, "landings": ["Macro", 5, None, {"x": 1}, "Final"]}).landings == (
        "Macro", "Final",
    )
    for junk in ({"Macro": True}, "Macro", None):
        assert _row({**staged, "landings": junk}).landings == ()
    assert _row(staged).landings == ()
    settled = _row({**base, "dir_name": "Research", "landings": ["Macro"]})
    assert settled.landings == ()
    [move] = plan_folder_moves([settled]).moves
    assert move.landings == ()
