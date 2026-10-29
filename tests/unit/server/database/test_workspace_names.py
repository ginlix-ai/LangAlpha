"""The name rules: one spelling as a folder, one key two names may not share."""

from __future__ import annotations

import pytest

from src.server.database.workspace_names import (
    WorkspaceNameInvalid,
    checked_workspace_name,
    copy_name,
    first_free_name,
    candidate_dir_names,
    placeholder_dir_name,
    suffixed_name,
    workspace_folder_name,
    workspace_name_key,
)


@pytest.mark.parametrize(
    ("name", "folder"),
    [
        ("Q3 Earnings", "Q3 Earnings"),
        ("  Q3 \t Earnings  ", "Q3 Earnings"),
        ("研究 notes", "研究 notes"),
        ("a/b\\c", "a-b-c"),
        ("Q3: Earnings", "Q3- Earnings"),
        ('say "hi" $HOME `x`', "say -hi- -HOME -x-"),
        ("..hidden", "hidden"),
        ("-flag", "flag"),
        ("é", "é"),
    ],
)
def test_the_folder_keeps_the_name_and_drops_only_what_breaks_a_path(name, folder):
    assert workspace_folder_name(name) == folder


@pytest.mark.parametrize("name", ["", "   ", "...", "/", "code", "TOOLS", "_internal", "mcp_servers"])
def test_a_name_with_no_folder_of_its_own_is_refused(name):
    """Empty, or a folder that belongs to the computer beside the workspaces."""
    with pytest.raises(WorkspaceNameInvalid):
        workspace_folder_name(name)


def test_the_folder_fits_the_filesystem_in_bytes_not_characters():
    folder = workspace_folder_name("研" * 100)
    assert len(folder.encode("utf-8")) <= 255
    assert folder == "研" * 85


def test_the_key_folds_case_the_way_a_case_insensitive_disk_does():
    assert workspace_name_key("Research") == workspace_name_key("RESEARCH")
    assert workspace_name_key("Straße") == workspace_name_key("STRASSE")
    assert workspace_name_key("Q3  Earnings") == workspace_name_key("q3 earnings")


def test_a_stored_name_is_trimmed_and_at_most_80_characters():
    assert checked_workspace_name("  Research ") == "Research"
    assert checked_workspace_name("x" * 80) == "x" * 80
    with pytest.raises(WorkspaceNameInvalid):
        checked_workspace_name("x" * 81)


def test_a_suffix_trims_the_name_rather_than_overflowing_it():
    name = suffixed_name("x" * 80, " (2)")
    assert name == "x" * 76 + " (2)"


def test_a_suffix_survives_the_folders_byte_cut():
    """64 four-byte characters cut to 252 bytes, which dropped a suffix whole and
    left every numbered spelling on the name's own key."""
    name = "😀" * 64
    assert workspace_name_key(suffixed_name(name, " (copy)")) != workspace_name_key(name)
    assert copy_name(name, {workspace_name_key(name)}).endswith(" (copy)")
    assert first_free_name(name, {workspace_name_key(name)}).endswith(" (2)")


def test_the_first_free_name_counts_past_every_taken_one():
    taken = {"research", "research (2)"}
    assert first_free_name("Research", set()) == "Research"
    assert first_free_name("Research", taken) == "Research (3)"


def test_a_copy_is_named_copy_then_counts_on():
    assert copy_name("Research", {"research"}) == "Research (copy)"
    assert copy_name("Research", {"research", "research (copy)"}) == "Research (copy 2)"


def test_a_copy_of_a_copy_counts_on_instead_of_nesting():
    taken = {"research", "research (copy)", "research (copy 2)"}
    assert copy_name("Research (copy 2)", taken) == "Research (copy 3)"


def test_a_placeholder_is_the_folder_plus_a_suffix_the_id_fixes():
    a = placeholder_dir_name("Research", "id-1")
    assert a.startswith("Research-") and a == placeholder_dir_name("Research", "id-1")
    assert a != placeholder_dir_name("Research", "id-2")
    assert placeholder_dir_name("code", "id-1").startswith("workspace-")


@pytest.mark.parametrize(
    ("name", "reason", "held"),
    [
        (" ./ ", "empty", None),
        ("Tools", "reserved", "Tools"),
        # Home's folder: a workspace taking it would push Home to a placeholder.
        (" home ", "reserved", "home"),
        ("x" * 81, "too_long", None),
    ],
)
def test_a_refusal_says_why_so_a_client_can_word_it(name, reason, held):
    with pytest.raises(WorkspaceNameInvalid) as caught:
        checked_workspace_name(name)
    assert (caught.value.reason, caught.value.name) == (reason, held)


def test_a_folder_held_under_any_case_is_not_a_candidate():
    """One folder on a case-insensitive disk, so adoption and create both skip it."""
    candidates = candidate_dir_names("Research", "id-1", held=["research"])
    assert candidates[0] == placeholder_dir_name("Research", "id-1")
    assert "Research" not in candidates
