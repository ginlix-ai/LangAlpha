"""The scan spec the server hands the runtime: what is reserved, and where."""

from __future__ import annotations

from ptc_agent.core.paths import ALWAYS_HIDDEN_DIR_NAMES
from src.server.services.persistence.transfer import (
    EXCLUDE_ROOT_DIRS,
    SYNC_MARKER_NAME,
    exclusion_spec,
)


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


def test_package_markers_are_the_users_own_files():
    """``__init__.py`` is what makes a directory a Python package. Dropping it
    by basename cost the user their own packages on every sandbox rebuild, and
    restored a tree that no longer imported."""
    spec = exclusion_spec(1)
    assert "__init__.py" not in spec["exclude_basenames"]
    assert spec["exclude_basenames"] == [".DS_Store", "Thumbs.db"]


def test_our_own_directories_are_reserved_only_where_we_put_them():
    """``model``, ``tools`` and ``mcp_servers`` are ordinary words. Matching
    them at any depth silently dropped a user's ``work/model/tools/`` from
    every scan and then pruned the rows for everything under it."""
    spec = exclusion_spec(1)
    for name in ("tools", "mcp_servers", ".system", ".self-improve"):
        assert name not in spec["exclude_dir_names"], name
    for reserved in EXCLUDE_ROOT_DIRS:
        assert reserved in spec["exclude_rel_dirs"], reserved


def test_git_repositories_are_excluded_deliberately_and_at_any_depth():
    """A repository's object store is routinely larger than the tree it
    belongs to, and half a repository restores worse than none; ``git clone``
    is what puts the history back."""
    spec = exclusion_spec(1)
    assert ".git" in spec["exclude_dir_names"]


def test_only_derivable_trees_are_pruned_by_name_at_any_depth():
    """Everything matched by name alone has to be re-derivable by re-running
    the install that made it, because it is matched wherever it appears. The
    set is derived rather than restated so a name added to one is added to
    both, and nothing of ours joins it by hand."""
    spec = exclusion_spec(1)
    assert set(spec["exclude_dir_names"]) == set(ALWAYS_HIDDEN_DIR_NAMES) | {
        "__pycache__"
    }
