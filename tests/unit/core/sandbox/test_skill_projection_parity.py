"""Host and sandbox must project a skill tree identically.

The sandbox decides a tree is dirty by hashing what ``projected_files`` walks;
the host decides what an archive contains by what ``_ignored`` keeps. Any
disagreement makes a clean skill look permanently edited, so the reconciler
would pull the same tree up forever.

The sandbox half lives inside a script string that never reaches an import, so
this test lifts the function out of that source and runs both halves over one
real directory tree.
"""

from __future__ import annotations

import ast
import hashlib
import os
import stat
from pathlib import Path

from ptc_agent.core.sandbox.skill_sync import _SCRIPT
from src.server.services.user_skills.validate import _ignored


def _sandbox_projected_files():
    """Compile ``projected_files`` out of the embedded sandbox script."""
    module = ast.parse(_SCRIPT)
    fn = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "projected_files"
    )
    ns: dict = {"os": os, "stat": stat}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<skill_sync>", "exec"), ns)
    return ns["projected_files"]


def _sandbox_tree_state():
    """Compile ``tree_state`` and the two helpers it calls out of the script."""
    module = ast.parse(_SCRIPT)
    wanted = {"projected_files", "hash_file", "tree_state"}
    fns = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    ns: dict = {"os": os, "stat": stat, "hashlib": hashlib}
    exec(compile(ast.Module(body=fns, type_ignores=[]), "<skill_sync>", "exec"), ns)
    return ns["tree_state"]


def _build_tree(root: Path) -> None:
    for rel in (
        "SKILL.md",
        "LICENSE.txt",
        "scripts/run.py",
        "scripts/__pycache__/run.cpython-313.pyc",
        "reference/notes.md",
        "reference/LICENSE.txt",
        "reference/__pycache__",
        "data/nested/deep/values.csv",
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")


def test_sandbox_and_host_projections_agree(tmp_path):
    skill_dir = tmp_path / "demo"
    _build_tree(skill_dir)

    projected_files = _sandbox_projected_files()
    files, special = projected_files(str(skill_dir))
    sandbox_view = {rel for rel, _ in files}

    on_disk = {
        os.path.relpath(os.path.join(root, name), skill_dir).replace(os.sep, "/")
        for root, _, names in os.walk(skill_dir)
        for name in names
    }
    host_view = {rel for rel in on_disk if not _ignored(rel)}

    assert special is None
    assert sandbox_view == host_view
    # Guard the guard: a tree where nothing is dropped would pass vacuously.
    assert on_disk - host_view == {
        "LICENSE.txt",
        "reference/LICENSE.txt",
        "reference/__pycache__",
        "scripts/__pycache__/run.cpython-313.pyc",
    }


def test_sandbox_reports_a_symlink_as_unsyncable(tmp_path):
    skill_dir = tmp_path / "demo"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("x")
    (skill_dir / "link.md").symlink_to(skill_dir / "SKILL.md")

    files, special = _sandbox_projected_files()(str(skill_dir))

    assert {rel for rel, _ in files} == {"SKILL.md"}
    assert special and "non-regular file" in special


def test_sandbox_reports_a_symlinked_directory_as_unsyncable(tmp_path):
    """os.walk files a symlink-to-directory under ``dirs`` and never descends
    it, so without an explicit lstat it leaves no trace at all and the tree
    reads as clean.
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("x")
    skill_dir = tmp_path / "demo"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("x")
    (skill_dir / "data").symlink_to(outside, target_is_directory=True)

    files, special = _sandbox_projected_files()(str(skill_dir))

    assert {rel for rel, _ in files} == {"SKILL.md"}
    assert special == "symlinked directory: data"


def test_the_stat_cache_is_keyed_on_ctime_as_well(tmp_path):
    """A restored mtime plus an equal size is a reachable collision.

    ``cp -p``, an archive extract, and editors that preserve times all produce
    it, and the cached hash would then describe content that is no longer
    there. The forged entries below stand in for that: each asserts whether
    the cache is trusted, by seeding it with a hash the file does not have.
    """
    skill_dir = tmp_path / "demo"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("aaaa")

    tree_state = _sandbox_tree_state()
    true_hash, cache, special = tree_state(str(skill_dir), {})
    assert special is None
    (rel,) = cache
    mtime, size, ctime, _ = cache[rel]

    # A full match is still trusted, so the cache has not simply been disabled.
    assert tree_state(str(skill_dir), {rel: [mtime, size, ctime, "forged"]})[0] != true_hash
    # mtime and size agree, ctime does not: the collision this key exists for.
    assert tree_state(str(skill_dir), {rel: [mtime, size, ctime - 1, "forged"]})[0] == true_hash
    # A pre-ctime entry recomputes once rather than being read as a match.
    assert tree_state(str(skill_dir), {rel: [mtime, size, "forged"]})[0] == true_hash
