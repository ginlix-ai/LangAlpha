"""Locks the two sandbox layout tiers to the paths that exist on disk.

Layout v4 split the one layout object in two: ``SandboxLayout`` is the
computer, ``WorkspaceLayout`` is one workspace's folder on it. These tests pin
both tables, hold the copies that cannot import the layout (the
sandbox-resident sources, the frontend module) equal to them, and gate on new
derived paths so a later move cannot happen silently in only one of the tiers.
"""

import asyncio
import contextvars
import json
import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox import path_resolution as _paths
from ptc_agent.core.paths import (
    AGENT_SYSTEM_DIRS,
    BACKUP_EXCLUDE_DIRS,
    COMPUTER_ROOT_ENTRIES,
    DEFAULT_SANDBOX_ROOT,
    LEGACY_ROOT_TOOLS_DIR,
    SANDBOX_ROOTS,
    SandboxLayout,
    WorkspaceLayout,
    resolve_agent_path,
    workspace_root,
)
from ptc_agent.core.project_context import (
    ProjectContext,
    current_project,
    require_project,
    run_with_project,
    set_project,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _relative_snapshot(cls: type) -> dict[str, str]:
    """Every relative name a layout class spells, read off the class.

    Derived rather than listed so a constant added without a pinned value
    fails the comparison below instead of arriving unnoticed. ``as_constants``
    cannot answer this question any more: it emits only the subset the
    in-sandbox client reads back.
    """
    return {
        name: value
        for name, value in vars(cls).items()
        if name.isupper() and isinstance(value, str)
    }


# Every path the default computer layout produces. A move updates this table
# and the layout together; nothing else should ever need to change it.
EXPECTED_ABSOLUTE = {
    "root": "/home/workspace",
    "tools": "/home/workspace/_internal/tools",
    "tools_docs": "/home/workspace/.agents/tools/docs",
    "tools_init": "/home/workspace/_internal/tools/__init__.py",
    "mcp_client": "/home/workspace/_internal/tools/mcp_client.py",
    "union_ledger": "/home/workspace/_internal/tools/.union.json",
    "union_lock": "/home/workspace/_internal/tools/.union.lock",
    "mcp_servers": "/home/workspace/mcp_servers",
    "mcp_manifest": "/home/workspace/mcp_servers/.mcp_manifest.json",
    "agents": "/home/workspace/.agents",
    "skills": "/home/workspace/.agents/skills",
    "skills_manifest": "/home/workspace/.agents/skills/.skills_manifest.json",
    "user": "/home/workspace/.agents/user",
    "memory_user": "/home/workspace/.agents/user/memory",
    "memo_user": "/home/workspace/.agents/user/memo",
    "user_profile": "/home/workspace/.agents/user/profile",
    "workflows": "/home/workspace/.agents/workflows",
    "system": "/home/workspace/.system",
    "system_code": "/home/workspace/.system/code",
    "system_trace": "/home/workspace/.system/trace",
    "internal": "/home/workspace/_internal",
    "internal_src": "/home/workspace/_internal/src",
    "packs": "/home/workspace/_internal/packs",
    "wsfiles": "/home/workspace/_internal/.wsfiles",
    "manifest": "/home/workspace/_internal/.sandbox_manifest.json",
    "mcp_tokens": "/home/workspace/_internal/.mcp_tokens.json",
    "egress_relay": "/home/workspace/_internal/.egress_relay.json",
    "vault_secrets": "/home/workspace/_internal/.vault_secrets.json",
}

# Every relative name SandboxLayout spells, not only the emitted subset.
EXPECTED_RELATIVE = {
    "TOOLS_DIR": "_internal/tools",
    "TOOLS_DOCS_DIR": ".agents/tools/docs",
    "TOOLS_INIT_FILE": "_internal/tools/__init__.py",
    "MCP_CLIENT_FILE": "_internal/tools/mcp_client.py",
    "UNION_LEDGER_FILE": "_internal/tools/.union.json",
    "UNION_LOCK_FILE": "_internal/tools/.union.lock",
    "MCP_SERVERS_DIR": "mcp_servers",
    "MCP_MANIFEST_FILE": "mcp_servers/.mcp_manifest.json",
    "AGENTS_DIR": ".agents",
    "SKILLS_DIR": ".agents/skills",
    "SKILLS_MANIFEST_FILE": ".agents/skills/.skills_manifest.json",
    "USER_DIR": ".agents/user",
    "MEMORY_USER_DIR": ".agents/user/memory",
    "MEMO_USER_DIR": ".agents/user/memo",
    "USER_PROFILE_DIR": ".agents/user/profile",
    "WORKFLOWS_DIR": ".agents/workflows",
    "TMP_DIR": ".agents/tmp",
    "SYSTEM_DIR": ".system",
    "SYSTEM_CODE_DIR": ".system/code",
    "SYSTEM_TRACE_DIR": ".system/trace",
    "INTERNAL_DIR": "_internal",
    "INTERNAL_SRC_DIR": "_internal/src",
    "PACKS_DIR": "_internal/packs",
    "WSFILES_DIR": "_internal/.wsfiles",
    "MANIFEST_FILE": "_internal/.sandbox_manifest.json",
    "MCP_TOKENS_FILE": "_internal/.mcp_tokens.json",
    "EGRESS_RELAY_FILE": "_internal/.egress_relay.json",
    "VAULT_SECRETS_FILE": "_internal/.vault_secrets.json",
}

# The workspace tier, relative to the workspace folder rather than the root.
# data is here rather than on the computer: v4 puts a project's deliverables
# inside its own folder, and task directories sit directly in it under
# whatever name the agent picks, so only the shared dataset dir is fixed.
EXPECTED_WORKSPACE_RELATIVE = {
    "AGENTS_DIR": ".agents",
    "SKILLS_DIR": ".agents/skills",
    "MEMORY_DIR": ".agents/memory",
    "TOOLS_DIR": ".agents/tools",
    "MCP_CLIENT_CONFIG_FILE": ".agents/tools/mcp_client_config.json",
    "THREADS_DIR": ".agents/threads",
    "LARGE_TOOL_RESULTS_DIR": ".agents/large_tool_results",
    "AGENT_MD_FILE": "agent.md",
    "DATA_DIR": "data",
}

EXPECTED_WORKSPACE_ABSOLUTE = {
    "workspace": "/home/workspace/acme-ab12",
    "agents": "/home/workspace/acme-ab12/.agents",
    "skills": "/home/workspace/acme-ab12/.agents/skills",
    "memory": "/home/workspace/acme-ab12/.agents/memory",
    "tools": "/home/workspace/acme-ab12/.agents/tools",
    "tools_docs": "/home/workspace/acme-ab12/.agents/tools/docs",
    "mcp_client_config": (
        "/home/workspace/acme-ab12/.agents/tools/mcp_client_config.json"
    ),
    "threads": "/home/workspace/acme-ab12/.agents/threads",
    "large_tool_results": (
        "/home/workspace/acme-ab12/.agents/large_tool_results"
    ),
    "agent_md": "/home/workspace/acme-ab12/agent.md",
}


class TestDefaultLayout:
    def test_absolute_paths_are_todays_literals(self):
        layout = SandboxLayout.default()
        actual = {name: getattr(layout, name) for name in EXPECTED_ABSOLUTE}
        assert actual == EXPECTED_ABSOLUTE

    def test_relative_names_are_todays_literals(self):
        # Two questions now, because as_constants answers only the second.
        # The table pins every relative name the class spells, so a new one
        # has to be added here deliberately.
        assert _relative_snapshot(SandboxLayout) == EXPECTED_RELATIVE
        # And the emission is exactly the names the in-sandbox client reads
        # back. Over-emitting is the bug the allowlist fixed: the block rides
        # MCP_CLIENT_CODEGEN_VERSION, so renaming a constant the runtime never
        # reads used to re-sync every warm sandbox in the fleet.
        constants = SandboxLayout.default().as_constants()
        assert constants["SandboxLayout"] == {
            name: EXPECTED_RELATIVE[name] for name in SandboxLayout.RUNTIME_CONSTANTS
        }

    def test_every_absolute_path_has_a_snapshot(self):
        # A new derived path without a pinned value would let a later move
        # happen silently, which is the whole failure mode this file stops.
        derived = {
            name
            for name, attr in vars(SandboxLayout).items()
            if isinstance(attr, property)
        }
        assert derived - set(EXPECTED_ABSOLUTE) == {
            "setup_dirs",
            "allowed_directories",
            "denied_directories",
        }

    def test_the_wrapper_tiers_live_under_internal(self):
        # The generated wrapper modules moved under _internal in v4 so one
        # computer serves every workspace from one copy. Denied to the agent,
        # reached by import path rather than by cwd.
        layout = SandboxLayout.default()
        for path in (layout.tools, layout.tools_init, layout.mcp_client):
            assert path.startswith(layout.internal + "/")

    def test_the_tool_docs_are_readable(self):
        # Docs are reference the agent quotes and the file panel serves, so
        # they sit outside _internal while the importable wrappers stay in it.
        layout = SandboxLayout.default()
        assert not layout.tools_docs.startswith(layout.internal + "/")
        assert layout.tools_docs.startswith(layout.agents + "/")
        # One spelling, and it is the machine's. A folder that wants its own
        # doc tree joins the computer's constant rather than declaring a
        # second copy that could drift from it.
        assert not hasattr(WorkspaceLayout, "TOOLS_DOCS_DIR")
        assert layout.for_workspace("acme-ab12").tools_docs == (
            f"{layout.root}/acme-ab12/{SandboxLayout.TOOLS_DOCS_DIR}"
        )

    def test_computer_root_entries_cover_every_machine_path(self):
        # The migration's reserved list is derived from this set, so anything
        # the machine owns that is missing from it gets swept into the first
        # workspace folder. Read the segments off the class: a hand list is
        # how .system and .agents came to be absent, saved only by a shell
        # glob that happens to skip dot entries.
        segments = {
            value.split("/", 1)[0]
            for value in _relative_snapshot(SandboxLayout).values()
        }
        assert segments <= COMPUTER_ROOT_ENTRIES
        # The one entry with no constant behind it is the pre-v4 root
        # directory, which a reused sandbox still has and the migration has
        # to leave where it found it.
        assert COMPUTER_ROOT_ENTRIES - segments == {LEGACY_ROOT_TOOLS_DIR}

    def test_setup_dirs_are_the_machines_own_directories(self):
        # Deliverables belong to a workspace folder, so data is no longer
        # created at the computer root.
        assert SandboxLayout.default().setup_dirs == (
            "/home/workspace/_internal/tools",
            "/home/workspace/.agents/tools/docs",
            "/home/workspace/.system/code",
            "/home/workspace/.system/trace",
            "/home/workspace/.agents/skills",
            "/home/workspace/_internal/src",
        )

    def test_filesystem_config_defaults(self):
        # The config's deny list stays computer-wide: the sibling half is per
        # turn and cannot live on an object one sandbox shares.
        layout = SandboxLayout.default()
        assert layout.allowed_directories == ["/home/workspace", "/tmp"]
        assert layout.denied_directories == ["/home/workspace/_internal"]

    def test_for_root_rebases_everything(self):
        layout = SandboxLayout.for_root("/srv/box/")
        assert layout.root == "/srv/box"
        assert layout.skills == "/srv/box/.agents/skills"
        assert layout.mcp_tokens == "/srv/box/_internal/.mcp_tokens.json"
        assert SandboxLayout.for_root(None) == SandboxLayout.default()
        assert SandboxLayout.for_root("") == SandboxLayout.default()

    def test_bare_constructor_keeps_the_root_verbatim(self):
        # Call sites that already hold a real working dir use the constructor,
        # which must join exactly as their f-strings did, including for the
        # degenerate roots a misconfigured deployment can produce.
        assert SandboxLayout("").tools == "/_internal/tools"
        assert SandboxLayout("/a/").tools == "/a//_internal/tools"

    def test_layouts_are_frozen(self):
        # One sandbox serves concurrent turns for different workspaces, so a
        # layout that could be mutated in place would leak across them.
        with pytest.raises(FrozenInstanceError):
            SandboxLayout.default().root = "/elsewhere"
        with pytest.raises(FrozenInstanceError):
            SandboxLayout.default().for_workspace("a").dir_name = "b"

    def test_agent_system_dirs_are_root_level_names(self):
        # Root-level directory NAMES, not paths. "tools" left this set with
        # the directory: the v3-to-v4 migration moves it under _internal,
        # which HIDDEN_DIR_NAMES already covers, and it runs before any turn
        # can list the root.
        assert AGENT_SYSTEM_DIRS == frozenset(
            {".system", "mcp_servers", ".agents", ".self-improve"}
        )
        assert "tools" not in BACKUP_EXCLUDE_DIRS
        # The name itself stays: reads of a path a pre-v4 sandbox wrote are
        # still classified by it.
        assert LEGACY_ROOT_TOOLS_DIR == "tools"

    def test_sandbox_roots_list_current_then_legacy(self):
        assert DEFAULT_SANDBOX_ROOT == "/home/workspace"
        assert SANDBOX_ROOTS == ("/home/workspace", "/home/daytona")


class TestWorkspaceLayout:
    def test_absolute_paths_are_todays_literals(self):
        layout = SandboxLayout.default().for_workspace("acme-ab12")
        actual = {name: getattr(layout, name) for name in EXPECTED_WORKSPACE_ABSOLUTE}
        assert actual == EXPECTED_WORKSPACE_ABSOLUTE

    def test_relative_names_are_todays_literals(self):
        # Same split as the computer tier: the whole table, then the subset
        # the emitted block carries.
        assert _relative_snapshot(WorkspaceLayout) == EXPECTED_WORKSPACE_RELATIVE
        layout = SandboxLayout.default().for_workspace("acme-ab12")
        assert layout.as_constants() == {
            name: EXPECTED_WORKSPACE_RELATIVE[name]
            for name in WorkspaceLayout.RUNTIME_CONSTANTS
        }

    def test_every_absolute_path_has_a_snapshot(self):
        derived = {
            name
            for name, attr in vars(WorkspaceLayout).items()
            if isinstance(attr, property)
        }
        assert derived - set(EXPECTED_WORKSPACE_ABSOLUTE) == {"setup_dirs"}

    def test_setup_dirs_land_in_the_folder(self):
        # What the v3-to-v4 migration creates, so a folder made fresh and one
        # the migration moved are the same shape.
        computer = SandboxLayout.default()
        assert computer.for_workspace("acme-ab12").setup_dirs == (
            "/home/workspace/acme-ab12",
            "/home/workspace/acme-ab12/data",
            "/home/workspace/acme-ab12/.agents/skills",
            "/home/workspace/acme-ab12/.agents/memory",
            "/home/workspace/acme-ab12/.agents/tools",
            "/home/workspace/acme-ab12/.agents/threads",
        )
        # Without a folder they land at the root, which is where an unsplit
        # computer has always kept them.
        assert computer.for_workspace().setup_dirs[:2] == (
            "/home/workspace",
            "/home/workspace/data",
        )

    def test_only_a_folder_gets_its_own_tool_docs(self):
        # The bug this shape fixes: with an empty dir_name the two tiers
        # produced the identical string, so a per-workspace prune of "the
        # folder's docs" deleted the computer's shared docs out from under
        # every sibling. None is what makes that unwritable.
        computer = SandboxLayout.default()
        for dir_name in (None, ""):
            assert computer.for_workspace(dir_name).tools_docs is None
        for dir_name in ("acme-ab12", "beta-cd34"):
            docs = computer.for_workspace(dir_name).tools_docs
            assert docs is not None
            assert docs != computer.tools_docs

    def test_thread_state_is_workspace_tier(self):
        # A turn's scratch follows the project it ran for, so deleting the
        # workspace folder takes it along. Naming it on the computer would put
        # every project's threads back in one flat pile at the root.
        for name in ("THREADS_DIR", "LARGE_TOOL_RESULTS_DIR"):
            assert hasattr(WorkspaceLayout, name)
            assert not hasattr(SandboxLayout, name)
        layout = SandboxLayout.default().for_workspace("acme-ab12")
        assert layout.threads.startswith(layout.workspace + "/")
        assert layout.large_tool_results.startswith(layout.workspace + "/")

    def test_no_folder_means_the_workspace_owns_the_root(self):
        computer = SandboxLayout.default()
        for layout in (computer.for_workspace(), computer.for_workspace("")):
            assert layout.workspace == computer.root
            assert layout.agents == computer.agents
            assert layout.agent_md == "/home/workspace/agent.md"

    def test_workspace_root_helper_agrees_with_the_layout(self):
        # WP7 and WP13 fold paths with the helper rather than the object, so
        # the two spellings have to produce one answer.
        assert workspace_root("/home/workspace", None) == "/home/workspace"
        assert workspace_root("/home/workspace", "") == "/home/workspace"
        assert workspace_root("/home/workspace", "a-1") == "/home/workspace/a-1"
        computer = SandboxLayout("/srv/box")
        for name in (None, "", "a-1"):
            assert computer.for_workspace(name).workspace == workspace_root(
                "/srv/box", name
            )

    def test_pythonpath_is_workspace_then_runtime(self):
        computer = SandboxLayout.default()
        layout = computer.for_workspace("acme-ab12")
        assert layout.pythonpath(computer) == [
            "/home/workspace/acme-ab12/.agents",
            "/home/workspace/_internal",
            "/home/workspace/_internal/src",
        ]

    def test_denied_directories_carve_out_siblings_tools(self):
        computer = SandboxLayout.default()
        layout = computer.for_workspace("acme-ab12")
        assert layout.denied_directories(computer) == ["/home/workspace/_internal"]
        assert layout.denied_directories(
            computer, ("beta-cd34", "acme-ab12", "")
        ) == [
            "/home/workspace/_internal",
            # The workspace's own tools are not denied, and neither is any
            # other file a sibling owns, only its wrapper package.
            "/home/workspace/beta-cd34/.agents/tools",
        ]

    def test_rebasing_the_computer_moves_the_workspace(self):
        layout = SandboxLayout("/srv/box").for_workspace("acme-ab12")
        assert layout.workspace == "/srv/box/acme-ab12"
        assert layout.skills == "/srv/box/acme-ab12/.agents/skills"


class TestProjectContext:
    def _project(self, dir_name="acme-ab12", siblings=()):
        SandboxLayout.default()
        return ProjectContext(
            workspace_id="ws-1",
            dir_name=dir_name,
            sibling_dir_names=siblings,
        )

    def test_is_frozen(self):
        with pytest.raises(FrozenInstanceError):
            self._project().dir_name = "other"

    def test_unset_reads_none_and_require_raises(self):
        assert current_project() is None
        with pytest.raises(LookupError):
            require_project()

    def test_set_and_read_back(self):
        ctx = self._project()
        token = set_project(ctx)
        try:
            assert current_project() is ctx
            assert require_project() is ctx
        finally:
            ctx_var_reset(token)

    def test_a_child_task_inherits_the_project(self):
        # The fan-out relies on this: a task created inside a turn sees the
        # turn's project without being handed it.
        ctx = self._project()

        async def main():
            token = set_project(ctx)
            try:
                return await asyncio.create_task(_read_project())
            finally:
                ctx_var_reset(token)

        assert asyncio.run(main()) is ctx

    def test_a_task_created_before_the_set_does_not_inherit(self):
        # Why the workflow driver re-sets rather than relying on inheritance.
        ctx = self._project()

        async def main():
            started = asyncio.Event()
            task = asyncio.create_task(_read_after(started))
            set_project(ctx)
            started.set()
            return await task

        assert asyncio.run(main()) is None

    def test_run_with_project_binds_for_the_stream(self):
        ctx = self._project()
        seen = []

        async def source():
            for _ in range(2):
                seen.append(current_project())
                yield 1

        async def main():
            async for _ in run_with_project(ctx, source()):
                pass
            return current_project()

        assert asyncio.run(main()) is None
        assert seen == [ctx, ctx]

    def test_run_with_project_passes_none_through(self):
        async def source():
            yield "a"
            yield "b"

        async def main():
            return [e async for e in run_with_project(None, source())]

        assert asyncio.run(main()) == ["a", "b"]


def _fake_workspace(sandbox):
    """``PTCSandbox.workspace``, which is how path resolution reaches the folder.

    Mirrored rather than stubbed to a constant: the ambient-project fallback is
    what makes a call outside a turn resolve against the whole computer, and
    two of these tests depend on exactly that.
    """

    def workspace(project=None):
        ctx = project if project is not None else current_project()
        return sandbox.layout.for_workspace(ctx.dir_name if ctx is not None else None)

    return workspace


class TestComputerTierFolding:
    """Which base a relative agent path folds onto, and that it folds back.

    The store mounts are keyed on a root-anchored prefix, so a user-tier path
    that folded into the turn's folder would miss every route and write a real
    file the store never sees. These pin the split both ways: the tier the
    workspace owns stays in the folder, and a path the agent names has to come
    back the way it went in.
    """

    ROOT = "/home/workspace"
    DIR = "acme-ab12"

    def _sandbox(self):
        sandbox = SimpleNamespace(_work_dir=self.ROOT)
        sandbox.config = SimpleNamespace(
            filesystem=SimpleNamespace(
                allowed_directories=[self.ROOT, "/tmp"],
                denied_directories=[f"{self.ROOT}/_internal"],
                enable_path_validation=True,
            )
        )
        sandbox.layout = SandboxLayout(self.ROOT)
        sandbox.workspace = _fake_workspace(sandbox)
        return sandbox

    def _project(self):
        SandboxLayout(self.ROOT)
        return ProjectContext(
            workspace_id="ws-1",
            dir_name=self.DIR,
        )

    @pytest.mark.parametrize(
        "spelling",
        [
            ".agents/user/memory/note.md",
            "/.agents/user/memory/note.md",
            "./.agents/user/memory/note.md",
        ],
    )
    def test_user_tier_folds_onto_the_computer(self, spelling):
        sandbox, project = self._sandbox(), self._project()
        assert (
            _paths.normalize_path(sandbox, spelling, project)
            == f"{self.ROOT}/.agents/user/memory/note.md"
        )

    @pytest.mark.parametrize(
        "relative",
        [
            ".agents/user/memo/memo.md",
            ".agents/user/profile/preference.json",
            ".agents/workflows/daily.js",
        ],
    )
    def test_every_computer_tier_subtree_folds_onto_the_root(self, relative):
        sandbox, project = self._sandbox(), self._project()
        assert (
            _paths.normalize_path(sandbox, relative, project)
            == f"{self.ROOT}/{relative}"
        )

    @pytest.mark.parametrize(
        "relative",
        [
            WorkspaceLayout.MEMORY_DIR + "/note.md",
            WorkspaceLayout.SKILLS_DIR + "/dcf-model/SKILL.md",
            WorkspaceLayout.TOOLS_DIR + "/yf_price.py",
            WorkspaceLayout.THREADS_DIR + "/abcd1234/request.md",
            WorkspaceLayout.LARGE_TOOL_RESULTS_DIR + "/r1.json",
            "work/task/out.csv",
        ],
    )
    def test_the_workspaces_own_tier_stays_in_its_folder(self, relative):
        sandbox, project = self._sandbox(), self._project()
        assert (
            _paths.normalize_path(sandbox, relative, project)
            == f"{self.ROOT}/{self.DIR}/{relative}"
        )

    @pytest.mark.parametrize(
        "relative",
        [
            ".agents/user/memory/note.md",
            ".agents/workflows/daily.js",
            ".agents/memory/note.md",
            "work/task/out.csv",
        ],
    )
    def test_normalize_and_virtualize_round_trip(self, relative):
        sandbox, project = self._sandbox(), self._project()
        absolute = _paths.normalize_path(sandbox, relative, project)
        virtual = _paths.virtualize_path(sandbox, absolute, project)
        assert virtual == f"/{relative}"
        assert _paths.normalize_path(sandbox, virtual, project) == absolute

    def test_a_real_file_under_a_computer_tier_name_keeps_its_absolute_path(self):
        """The spelling belongs to the mount, so the stray file cannot borrow it."""
        sandbox, project = self._sandbox(), self._project()
        stray = f"{self.ROOT}/{self.DIR}/.agents/user/memory/stray.md"
        assert _paths.virtualize_path(sandbox, stray, project) == stray

    def test_an_unsplit_computer_folds_everything_onto_the_root(self):
        sandbox = self._sandbox()
        assert (
            _paths.normalize_path(sandbox, ".agents/user/memory/note.md", None)
            == f"{self.ROOT}/.agents/user/memory/note.md"
        )
        assert (
            _paths.normalize_path(sandbox, "task/out.csv", None)
            == f"{self.ROOT}/task/out.csv"
        )


class TestTraversalContainment:
    """A `..` must not buy a spelling the same checks refuse by name.

    Both lists in ``validate_path`` are textual prefixes, and the deny entries
    are canonical, so an uncollapsed `..` used to pass every one of them: a
    sibling's wrapper directory and the machine's own runtime directory were
    both reachable from inside a workspace folder. These pin the collapse, and
    the two surfaces the design does keep open, so a later reader can tell one
    from the other.
    """

    ROOT = "/home/workspace"
    OWN = "beta-c3d4"
    SIBLING = "alpha-a1b2"

    def _sandbox(self):
        computer = SandboxLayout(self.ROOT)
        sandbox = SimpleNamespace(_work_dir=self.ROOT, layout=computer)
        sandbox.config = SimpleNamespace(
            filesystem=SimpleNamespace(
                allowed_directories=computer.allowed_directories,
                denied_directories=computer.denied_directories,
                enable_path_validation=True,
            )
        )
        sandbox.workspace = _fake_workspace(sandbox)
        # The delegators PTCSandbox exposes; the validators reach back through them.
        sandbox.normalize_path = lambda path, project=None: _paths.normalize_path(
            sandbox, path, project
        )
        sandbox._normalize_search_path = lambda path: _paths._normalize_search_path(
            sandbox, path
        )
        sandbox.validate_path = lambda path, project=None: _paths.validate_path(
            sandbox, path, project
        )
        return sandbox

    def _project(self):
        SandboxLayout(self.ROOT)
        return ProjectContext(
            workspace_id="ws-b",
            dir_name=self.OWN,
            sibling_dir_names=(self.SIBLING, self.OWN),
        )

    @pytest.mark.parametrize(
        "spelling",
        [
            "../alpha-a1b2/.agents/tools/mcp_client_config.json",
            "./../alpha-a1b2/.agents/tools/mcp_client_config.json",
            "work/../../alpha-a1b2/.agents/tools/yf_price.py",
            "/home/workspace/beta-c3d4/../alpha-a1b2/.agents/tools/yf_price.py",
            "/tmp/../home/workspace/alpha-a1b2/.agents/tools/yf_price.py",
        ],
    )
    def test_a_siblings_wrappers_are_denied_by_every_spelling(self, spelling):
        sandbox, project = self._sandbox(), self._project()
        normalized = _paths.normalize_path(sandbox, spelling, project)
        assert normalized.startswith(f"{self.ROOT}/{self.SIBLING}/.agents/tools/")
        assert _paths.validate_path(sandbox, spelling, project) is False

    @pytest.mark.parametrize(
        "spelling",
        [
            "../_internal/.manifest",
            "work/../../_internal/tools/yf_price.py",
            "/home/workspace/beta-c3d4/../_internal/tools/yf_price.py",
            "/tmp/../home/workspace/_internal/.manifest",
        ],
    )
    def test_the_runtime_directory_is_denied_by_every_spelling(self, spelling):
        sandbox, project = self._sandbox(), self._project()
        normalized = _paths.normalize_path(sandbox, spelling, project)
        assert normalized.startswith(f"{self.ROOT}/_internal/")
        assert _paths.validate_path(sandbox, spelling, project) is False

    def test_a_path_off_the_computer_is_refused_rather_than_clamped(self):
        """Collapse first, then check: the allowlist is what rejects it."""
        sandbox, project = self._sandbox(), self._project()
        assert (
            _paths.normalize_path(sandbox, "../../../etc/passwd", project)
            == "/etc/passwd"
        )
        assert _paths.validate_path(sandbox, "../../../etc/passwd", project) is False

    @pytest.mark.parametrize(
        "spelling",
        [
            f"{WorkspaceLayout.MCP_CLIENT_CONFIG_FILE}",
            f"{WorkspaceLayout.TOOLS_DIR}/yf_price.py",
        ],
    )
    def test_the_turns_own_wrappers_stay_readable(self, spelling):
        sandbox, project = self._sandbox(), self._project()
        assert _paths.validate_path(sandbox, spelling, project) is True

    def test_a_siblings_deliverables_stay_readable(self):
        """Open decision 1: one computer exists so a turn can read prior work.

        The prompt is what confines writes to the turn's own folder, so this
        allowance covers writes too; the allowlist is the computer root.
        """
        sandbox, project = self._sandbox(), self._project()
        spelling = f"../{self.SIBLING}/work/report.md"
        assert (
            _paths.normalize_path(sandbox, spelling, project)
            == f"{self.ROOT}/{self.SIBLING}/work/report.md"
        )
        assert _paths.validate_path(sandbox, spelling, project) is True

    @pytest.mark.parametrize(
        "real",
        [
            "/home/workspace/beta-c3d4/work/x.md",
            "/home/workspace/beta-c3d4/.agents/memory/n.md",
            "/home/workspace/beta-c3d4/.agents/user/memory/stray.md",
            "/home/workspace/.agents/user/memory/n.md",
            "/home/workspace/alpha-a1b2/work/x.md",
            "/home/workspace/_internal/.manifest",
            "/home/workspace",
            "/tmp/x.md",
        ],
    )
    def test_every_real_path_has_a_virtual_spelling_that_folds_back(self, real):
        """The inverse has to hold for paths outside the folder too.

        A virtual `/x` folds onto the turn's folder, so the root's own files
        keep their absolute path rather than borrowing a spelling that would
        come back as this workspace's.
        """
        sandbox, project = self._sandbox(), self._project()
        virtual = _paths.virtualize_path(sandbox, real, project)
        assert _paths.normalize_path(sandbox, virtual, project) == real

    def test_a_sibling_keeps_its_absolute_path_and_the_folder_owns_the_slash(self):
        sandbox, project = self._sandbox(), self._project()
        sibling = f"{self.ROOT}/{self.SIBLING}/work/x.md"
        assert _paths.virtualize_path(sandbox, sibling, project) == sibling
        assert (
            _paths.virtualize_path(
                sandbox, f"{self.ROOT}/{self.OWN}/work/x.md", project
            )
            == "/work/x.md"
        )
        assert (
            _paths.virtualize_path(sandbox, f"{self.ROOT}/{self.OWN}", project) == "/"
        )

    def test_an_unsplit_computer_still_owns_the_virtual_root(self):
        sandbox = self._sandbox()
        assert _paths.virtualize_path(sandbox, self.ROOT, None) == "/"
        assert (
            _paths.virtualize_path(sandbox, f"{self.ROOT}/work/x.md", None)
            == "/work/x.md"
        )

    def test_validation_reads_the_callers_spelling_not_its_own_output(self):
        """Normalizing twice launders an escape into an allowed path.

        An absolute path outside the allowed roots is a virtual path by
        ``normalize_path``'s rule, so a second pass folds ``/etc/passwd`` back
        under the folder and answers True while the caller still acts on the
        path that left. The paired helper has to ask about the original.
        """
        sandbox, project = self._sandbox(), self._project()
        escape = "../../../etc/passwd"
        normalized = _paths.normalize_path(sandbox, escape, project)

        assert normalized == "/etc/passwd"
        assert _paths.validate_path(sandbox, escape, project) is False
        # The trap itself: re-reading the result is a different question.
        assert _paths.validate_path(sandbox, normalized, project) is True

        path, error = _paths.validate_and_normalize_path(sandbox, escape, project)
        assert path == "/etc/passwd"
        assert error is not None

    def test_the_search_path_a_listing_walks_is_collapsed_too(self):
        """``_normalize_search_path`` feeds the allow-only validator."""
        sandbox = self._sandbox()
        assert (
            _paths._normalize_search_path(sandbox, "work/../.agents")
            == f"{self.ROOT}/.agents"
        )
        assert _paths._normalize_search_path(sandbox, "/tmp/../etc") == "/etc"
        assert _paths._validate_path_allow_denied(sandbox, "/tmp/../etc") is False


class TestPreSplitRootPaths:
    """An absolute root path the folder move left behind lands in the folder.

    The root's real entries are the reserved tiers and one directory per
    project. A turn resumed from before the split still writes
    ``/home/workspace/report.md``, and a stored v3 link still names one; at the
    root that file is served by no route and scanned by no mirror, so the
    unowned root entry is read as this project's own. A sibling's folder and a
    reserved entry are spelled as themselves.
    """

    ROOT = "/home/workspace"
    OWN = "beta-c3d4"
    SIBLING = "alpha-a1b2"

    def _sandbox(self):
        computer = SandboxLayout(self.ROOT)
        sandbox = SimpleNamespace(_work_dir=self.ROOT, layout=computer)
        sandbox.config = SimpleNamespace(
            filesystem=SimpleNamespace(
                allowed_directories=computer.allowed_directories,
                denied_directories=computer.denied_directories,
                enable_path_validation=True,
            )
        )
        sandbox.workspace = _fake_workspace(sandbox)
        sandbox.normalize_path = lambda path, project=None: _paths.normalize_path(
            sandbox, path, project
        )
        sandbox.validate_path = lambda path, project=None: _paths.validate_path(
            sandbox, path, project
        )
        return sandbox

    def _project(self):
        return ProjectContext(
            workspace_id="ws-b",
            dir_name=self.OWN,
            sibling_dir_names=(self.SIBLING,),
        )

    @pytest.mark.parametrize(
        "relative",
        [
            "report.md",
            "charts/x.png",
            "task/out.csv",
            WorkspaceLayout.AGENT_MD_FILE,
        ],
    )
    def test_an_unowned_root_entry_becomes_the_folders_own(self, relative):
        sandbox, project = self._sandbox(), self._project()
        assert (
            _paths.normalize_path(sandbox, f"{self.ROOT}/{relative}", project)
            == f"{self.ROOT}/{self.OWN}/{relative}"
        )

    def test_the_folded_write_is_allowed_and_virtualizes_back(self):
        sandbox, project = self._sandbox(), self._project()
        spelling = f"{self.ROOT}/report.md"
        resolved = _paths.normalize_path(sandbox, spelling, project)
        assert _paths.validate_path(sandbox, spelling, project) is True
        assert _paths.virtualize_path(sandbox, resolved, project) == "/report.md"

    @pytest.mark.parametrize(
        "absolute",
        [
            # A sibling project's folder, which a turn may write into.
            f"/home/workspace/{SIBLING}/work/report.md",
            f"/home/workspace/{SIBLING}",
            # The folder this turn already runs in.
            f"/home/workspace/{OWN}/work/report.md",
            # Every reserved root entry keeps its tier.
            "/home/workspace/_internal/.sandbox_manifest.json",
            "/home/workspace/.agents/user/memory/note.md",
            "/home/workspace/.agents/skills/dcf-model/SKILL.md",
            "/home/workspace/mcp_servers/.mcp_manifest.json",
            "/home/workspace/.system/code/c.py",
            "/home/workspace/tools/yf_price.py",
            # The root itself, and the other allowed directory.
            "/home/workspace",
            "/tmp/x.md",
        ],
    )
    def test_an_owned_entry_is_spelled_as_itself(self, absolute):
        sandbox, project = self._sandbox(), self._project()
        assert _paths.normalize_path(sandbox, absolute, project) == absolute

    def test_an_unsplit_computer_has_no_root_entry_to_fold(self):
        """With no folder the root is the base, so there is nothing to move."""
        sandbox = self._sandbox()
        assert (
            _paths.normalize_path(sandbox, f"{self.ROOT}/report.md", None)
            == f"{self.ROOT}/report.md"
        )

    def test_a_caller_that_cannot_name_the_siblings_folds_nothing(self):
        """The pinned backend resolves against a named base outside any turn,
        where a sibling's folder is indistinguishable from a loose root file."""
        absolute = f"{self.ROOT}/{self.SIBLING}/work/report.md"
        assert (
            resolve_agent_path(
                absolute,
                workspace=f"{self.ROOT}/{self.OWN}",
                root=self.ROOT,
                allowed=[self.ROOT, "/tmp"],
            )
            == absolute
        )


def ctx_var_reset(token: contextvars.Token) -> None:
    from ptc_agent.core import project_context

    project_context._current.reset(token)


async def _read_project():
    return current_project()


async def _read_after(started: asyncio.Event):
    await started.wait()
    return current_project()


class TestSandboxResidentCopies:
    """Sources uploaded into a sandbox cannot import the layout, so they carry
    transcribed constants. These hold the transcriptions equal to the object."""

    def test_mcp_client_runtime_fallbacks(self):
        from ptc_agent.core.sandbox import mcp_client_runtime as runtime

        layout = SandboxLayout.default()
        assert runtime._DEFAULT_ROOT == layout.root
        assert runtime._LAYOUT_CLASS == SandboxLayout.__name__
        for name, value in runtime._DEFAULT_LAYOUT.items():
            assert value == getattr(SandboxLayout, name), name

    def test_generated_client_config_carries_both_tiers(self):
        from ptc_agent.core.tool_generator import ToolFunctionGenerator

        cfg = ToolFunctionGenerator().generate_client_config([], working_dir="/srv/box")
        assert cfg["working_dir"] == "/srv/box"
        assert cfg["layout"] == SandboxLayout.default().as_constants()
        assert set(cfg["layout"]) == {"SandboxLayout", "WorkspaceLayout"}

    def test_generated_client_resolves_paths_from_the_emitted_layout(self):
        # End to end: the epilogue the host emits must drive the runtime's
        # paths, not the runtime's own fallbacks.
        from ptc_agent.core.tool_generator import ToolFunctionGenerator

        source = ToolFunctionGenerator().generate_mcp_client_code(
            [], working_dir="/srv/box"
        )
        namespace: dict = {"__name__": "generated_mcp_client"}
        exec(compile(source, "mcp_client.py", "exec"), namespace)  # noqa: S102
        assert namespace["_WORK_DIR"] == "/srv/box"
        assert namespace["_INTERNAL_ROOT"] == "/srv/box/_internal"
        assert namespace["_VAULT_SECRETS_FILE"] == (
            "/srv/box/_internal/.vault_secrets.json"
        )
        assert namespace["_EGRESS_RELAY_FILE"] == (
            "/srv/box/_internal/.egress_relay.json"
        )

    def test_data_client_token_file_default(self):
        from data_client.ginlix_data import mcp_client

        assert str(mcp_client.TOKEN_FILE) == SandboxLayout.default().mcp_tokens

    def test_transfer_runtime_pack_dir(self):
        from ptc_agent.core.sandbox import wsfiles_transfer_runtime

        assert wsfiles_transfer_runtime._PACK_DIR == SandboxLayout.PACKS_DIR


class TestConfigLiterals:
    """The config package sits below ptc_agent.core in the import graph, so a
    few field defaults are spelled out rather than imported. Pinned here."""

    def test_filesystem_and_docker_defaults(self):
        from ptc_agent.config.core import DockerConfig, FilesystemConfig

        layout = SandboxLayout.default()
        fs = FilesystemConfig()
        assert fs.working_directory == layout.root
        assert fs.allowed_directories == layout.allowed_directories
        assert fs.denied_directories == layout.denied_directories
        assert DockerConfig().working_dir == layout.root

    def test_skills_config_default(self):
        from ptc_agent.config.agent import SkillsConfig

        assert SkillsConfig().sandbox_skills_base == SandboxLayout.default().skills


class TestGeneratedFrontendModule:
    def test_committed_file_matches_the_emitter(self):
        # Regenerating in-process would test the emitter against itself; the
        # script is what a developer runs, so run the script.
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts/gen_agent_paths.py"), "--check"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        assert result.returncode == 0, (
            "web/src/pages/ChatAgent/utils/agentPaths.generated.ts is stale, "
            "run: uv run python scripts/gen_agent_paths.py\n"
            f"{result.stdout}{result.stderr}"
        )

    def test_emitted_values_are_todays_frontend_literals(self):
        # How the browser classifies agent paths. A change here is a real
        # change to what the file panel shows.
        text = (
            REPO_ROOT / "web/src/pages/ChatAgent/utils/agentPaths.generated.ts"
        ).read_text(encoding="utf-8")
        for literal in (
            "export const MEMORY_USER_DIR = '.agents/user/memory';",
            # The workspace memory tier no longer has a second spelling under
            # .agents/workspace/, so the browser classifies the one path the
            # folder actually carries.
            "export const MEMORY_WORKSPACE_DIR = '.agents/memory';",
            "export const MEMO_USER_DIR = '.agents/user/memo';",
            "export const USER_PROFILE_DIR = '.agents/user/profile';",
            "export const SKILLS_DIR = '.agents/skills';",
            "export const MEMORY_INDEX_FILENAME = 'memory.md';",
            "export const MEMO_INDEX_FILENAME = 'memo.md';",
            "  'home/workspace/',",
            "  'home/daytona/',",
            "  portfolio: 'portfolio.json',",
            "  watchlist: 'watchlist.json',",
            "  preference: 'preference.json',",
            # Root directories an earlier layout owned. A sandbox reused
            # across its migration still has them, so the file panel filters
            # on them by name.
            "export const LEGACY_ROOT_TOOLS_DIR = 'tools';",
            "export const LEGACY_ROOT_CODE_DIR = 'code';",
            "export const LEGACY_ROOT_DIRS = [\n  'code',\n  'tools',\n] as const;",
        ):
            assert literal in text, literal

    def test_json_round_trip_of_the_layout_is_stable(self):
        # The layout crosses into the sandbox as JSON; a non-string value would
        # break the runtime's dict lookups with no other signal.
        constants = SandboxLayout.default().as_constants()
        assert json.loads(json.dumps(constants)) == constants
