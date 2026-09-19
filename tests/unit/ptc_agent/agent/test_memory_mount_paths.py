"""Where each memory tier mounts once a computer serves several workspaces.

The user tier is one per person, so it stays at the computer root. Workspace
memory is per workspace, so it mounts under the turn's own folder: two
workspaces on one computer must not resolve `.agents/memory/memory.md` to the
same file, and neither may reach the other's. Only the prefix moves -- the
namespace behind each mount is what holds the bytes and is unchanged, so no
existing memory is stranded by the move.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.filesystem_routes import IdentityGates, build_filesystem_backend
from ptc_agent.core.paths import (
    MEMO_USER_DIR,
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    SandboxLayout,
)

ROOT = "/home/workspace"
USER = "user-1"
WORKSPACE = "ws-1"


def _gates(**overrides) -> IdentityGates:
    base = {
        "user_memory": True,
        "workspace_memory": True,
        "memo": False,
        "user_data": False,
        "workflow": False,
        "workflow_fs": False,
        "workflow_tool": False,
    }
    return IdentityGates(**{**base, **overrides})


def _backend(root: str = ROOT):
    sb = MagicMock()
    sb.root_dir = root
    sb.computer_root = root
    sb.normalize_path.side_effect = lambda p: p if p.startswith("/") else f"{root}/{p}"
    return sb


def _build(*, dir_name: str | None, gates: IdentityGates | None = None):
    layout = SandboxLayout(ROOT).for_workspace(dir_name)
    backend, sources = build_filesystem_backend(
        backend=_backend(),
        gates=gates or _gates(),
        store=MagicMock(),
        user_id=USER,
        workspace_id=WORKSPACE,
        layout=layout,
    )
    return backend, sources


def _prefixes(backend) -> set[str]:
    return {route.root_prefix for route in backend._routes}


class TestWorkspaceMemoryMount:
    def test_workspace_memory_mounts_under_the_turns_own_folder(self):
        backend, _ = _build(dir_name="acme-ab12")

        assert f"{ROOT}/acme-ab12/.agents/memory/" in _prefixes(backend)

    def test_the_old_computer_level_path_is_no_longer_mounted(self):
        backend, _ = _build(dir_name="acme-ab12")

        assert f"{ROOT}/.agents/workspace/memory/" not in _prefixes(backend)

    def test_the_user_tier_stays_at_the_computer_root(self):
        backend, _ = _build(dir_name="acme-ab12")

        assert f"{ROOT}/{MEMORY_USER_DIR}/" in _prefixes(backend)

    def test_two_workspaces_on_one_computer_get_different_mounts(self):
        alpha, _ = _build(dir_name="alpha-aa11")
        beta, _ = _build(dir_name="beta-bb22")

        assert f"{ROOT}/alpha-aa11/.agents/memory/" in _prefixes(alpha)
        assert f"{ROOT}/beta-bb22/.agents/memory/" in _prefixes(beta)
        assert not (_prefixes(alpha) & {f"{ROOT}/beta-bb22/.agents/memory/"})
        assert not (_prefixes(beta) & {f"{ROOT}/alpha-aa11/.agents/memory/"})

    def test_a_sibling_workspaces_memory_is_not_mounted(self):
        backend, _ = _build(dir_name="alpha-aa11")

        memory_mounts = {p for p in _prefixes(backend) if p.endswith("/.agents/memory/")}
        assert memory_mounts == {f"{ROOT}/alpha-aa11/.agents/memory/"}

    def test_a_computer_holding_one_workspace_at_its_root_still_mounts(self):
        backend, _ = _build(dir_name=None)

        assert f"{ROOT}/.agents/memory/" in _prefixes(backend)

    def test_an_omitted_layout_falls_back_to_the_root(self):
        backend, _ = build_filesystem_backend(
            backend=_backend(),
            gates=_gates(),
            store=MagicMock(),
            user_id=USER,
            workspace_id=WORKSPACE,
        )

        assert f"{ROOT}/.agents/memory/" in _prefixes(backend)


class TestNamespacesAreUnmoved:
    def test_the_workspace_namespace_is_untouched_by_the_new_prefix(self):
        _, sources = _build(dir_name="acme-ab12")

        assert sources.memory["workspace"].namespace_factory() == (
            USER,
            "workspaces",
            WORKSPACE,
            "memory",
        )

    def test_the_user_namespace_is_untouched(self):
        _, sources = _build(dir_name="acme-ab12")

        assert sources.memory["user"].namespace_factory() == (USER, "memory")

    def test_two_workspaces_keep_distinct_namespaces_under_one_computer(self):
        layout = SandboxLayout(ROOT)
        namespaces = set()
        for dir_name, ws_id in (("alpha-aa11", "ws-a"), ("beta-bb22", "ws-b")):
            _, sources = build_filesystem_backend(
                backend=_backend(),
                gates=_gates(),
                store=MagicMock(),
                user_id=USER,
                workspace_id=ws_id,
                layout=layout.for_workspace(dir_name),
            )
            namespaces.add(sources.memory["workspace"].namespace_factory())
        assert namespaces == {
            (USER, "workspaces", "ws-a", "memory"),
            (USER, "workspaces", "ws-b", "memory"),
        }


class TestBaselineDisplayPaths:
    """What the memory index tells the model to open, relative to its cwd."""

    def test_the_workspace_index_is_named_relative_to_the_workspace(self):
        _, sources = _build(dir_name="acme-ab12")

        assert (
            sources.memory["workspace"].display_path
            == f".agents/memory/{MEMORY_INDEX_FILENAME}"
        )

    def test_the_display_path_does_not_vary_with_the_folder(self):
        _, alpha = _build(dir_name="alpha-aa11")
        _, beta = _build(dir_name="beta-bb22")

        assert (
            alpha.memory["workspace"].display_path
            == beta.memory["workspace"].display_path
        )

    def test_the_user_index_keeps_its_computer_rooted_name(self):
        _, sources = _build(dir_name="acme-ab12")

        assert (
            sources.memory["user"].display_path
            == f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}"
        )


class TestOtherRoutesStayOnTheComputer:
    @pytest.mark.parametrize(
        ("gate", "expected"),
        [
            ("memo", f"{ROOT}/{MEMO_USER_DIR}/"),
            ("user_data", f"{ROOT}/.agents/user/profile/"),
        ],
    )
    def test_user_scoped_routes_are_not_moved_into_the_workspace(self, gate, expected):
        backend, _ = _build(
            dir_name="acme-ab12",
            gates=_gates(user_memory=False, workspace_memory=False, **{gate: True}),
        )

        assert _prefixes(backend) == {expected}
