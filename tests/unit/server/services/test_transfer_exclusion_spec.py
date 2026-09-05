"""The scan spec the server hands the runtime: what is reserved, and where."""

from __future__ import annotations

from src.server.services.persistence.transfer import SYNC_MARKER_NAME, exclusion_spec


def test_the_sync_marker_is_reserved_at_the_root_only_and_by_exact_name():
    """A basename exclusion would drop a user's ``results/.file_sync_marker``
    from every scan, and a prefix would drop a root ``.file_sync_marker.bak``;
    either way the next sync prunes the row."""
    spec = exclusion_spec(1)
    assert spec["exclude_root_basenames"] == [SYNC_MARKER_NAME]
    assert SYNC_MARKER_NAME not in spec["exclude_basenames"]
    assert all(not SYNC_MARKER_NAME.startswith(p) for p in spec["exclude_root_basename_prefixes"])


def test_the_skill_reconciler_scratch_is_reserved_under_its_own_directory_only():
    """A global ``.staging`` name would drop ``work/model/.staging`` from
    every scan and prune its rows."""
    spec = exclusion_spec(1)
    assert ".agents/skills/.staging" in spec["exclude_rel_dirs"]
    assert spec["exclude_rel_dir_prefixes"] == [".agents/skills/.trash-"]
    assert ".staging" not in spec["exclude_dir_names"]
    assert "exclude_dir_name_prefixes" not in spec


def test_the_reconciler_lock_is_reserved_at_its_own_path_only():
    """A basename exclusion would drop a user's ``results/.skills-sync.flock``
    from every scan and prune its row on the next sync."""
    spec = exclusion_spec(1)
    assert spec["exclude_rel_files"] == [".agents/skills/.skills-sync.flock"]
    assert ".skills-sync.flock" not in spec["exclude_basenames"]
