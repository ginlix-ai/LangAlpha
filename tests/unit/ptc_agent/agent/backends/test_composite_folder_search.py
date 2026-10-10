"""Glob and Grep from the turn's own folder reach the store routes as files.

A turn runs in its workspace's folder, but the user tier -- memory, memo,
workflows, profile, automations -- is mounted at the computer root, and a
relative name such as ``.agents/user/profile/portfolio.json`` folds onto it.
A search from the folder has to treat those files as files at that name:
Glob matches them as the sandbox glob matches the disk, dot-names included,
and Grep walks to them as rg walks the disk, skipping a dot-name below the
path it is given and matching its filter from the folder, where rg runs. The
one exception is the folder's own memory, which a Grep from the
folder searches by default. Each prints them in a form a Read accepts and
reaches nothing else: no other workspace's folder and nothing at the root
that is not a route.

The folder here is distinct from the computer root, which is what the
composite's own tests leave out: there the two are one directory.
"""

from __future__ import annotations

import functools
import json
import os
import posixpath
import random
import shlex
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends import workflow_namespace
from ptc_agent.agent.backends.automations import AutomationsBackend
from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.backends.search_match import (
    GLOB_EXCLUDED_DIRS,
    GLOB_HISTORY_CHILDREN,
    RgWalk,
)
from ptc_agent.agent.backends.user_data import UserDataBackend
from ptc_agent.agent.filesystem_routes import (
    USER_DATA_ROUTES,
    IdentityGates,
    build_filesystem_backend,
)
from ptc_agent.agent.tools.glob import create_glob_tool
from ptc_agent.agent.tools.grep import create_grep_tool
from ptc_agent.core.paths import (
    SandboxLayout,
    grep_working_dir,
    resolve_agent_path,
    virtual_agent_path,
)
from ptc_agent.core.sandbox import files as sandbox_files
from ptc_agent.core.sandbox.files import aglob_files
from ptc_agent.core.sandbox.glob_runtime import agent_glob_pattern
from ptc_agent.core.sandbox.grep_render import GrepLine

ROOT = "/home/workspace"
FOLDER = f"{ROOT}/Home"
SIBLING = f"{ROOT}/Research"
USER = "user-1"
WORKSPACE = "ws-1"

PROFILE = f"{ROOT}/.agents/user/profile/"
PROFILE_FILES = {
    f"{PROFILE}README.md",
    f"{PROFILE}portfolio.json",
    f"{PROFILE}watchlist.json",
    f"{PROFILE}preference.json",
    f"{PROFILE}user.json",
}
AUTOMATIONS = f"{ROOT}/.agents/user/automations/"
USER_MEMORY = f"{ROOT}/.agents/user/memory/memory.md"
MEMO = f"{ROOT}/.agents/user/memo/q1-thesis.md"
WORKFLOW = f"{ROOT}/.agents/workflows/brief.js"
WORKSPACE_MEMORY_ROOT = f"{FOLDER}/.agents/memory/"
WORKSPACE_MEMORY = f"{WORKSPACE_MEMORY_ROOT}memory.md"

# What the sandbox finds on disk in the folder: a file of the project's own.
ON_DISK = f"{FOLDER}/report.md"

# What a Grep for ZEPH finds in the routes, once it reaches them.
ROUTE_HITS = {f"{PROFILE}portfolio.json", USER_MEMORY, f"{AUTOMATIONS}daily-brief.json"}


def _envelope(content: str) -> dict[str, str]:
    return {
        "content": content,
        "encoding": "utf-8",
        "created_at": "2026-10-01T00:00:00Z",
        "modified_at": "2026-10-01T00:00:00Z",
    }


async def _on_disk(pattern: str, path: str) -> list[str]:
    """The sandbox's own glob, over a folder whose one file is ``ON_DISK``."""
    pattern = agent_glob_pattern(pattern)
    if not ON_DISK.startswith(f"{path}/"):
        return []
    return (
        [ON_DISK] if PurePosixPath(ON_DISK[len(path) + 1 :]).full_match(pattern) else []
    )


def _sandbox(folder: str = FOLDER, root: str = ROOT) -> MagicMock:
    """The sandbox as a turn in ``folder`` sees it: the same fold and the
    same printed spelling the real one uses."""
    sb = MagicMock()
    sb.computer_root = root
    sb.workspace_dir = folder
    sb.normalize_path.side_effect = lambda p: resolve_agent_path(
        p, workspace=folder, root=root, allowed=[root]
    )
    sb.virtualize_path.side_effect = lambda p: virtual_agent_path(
        p, workspace=folder, root=root
    )
    sb.validate_path.return_value = True
    sb.filesystem_config.enable_path_validation = True
    sb.aglob_paths = AsyncMock(side_effect=_on_disk)
    sb.agrep_rich = AsyncMock(return_value=[])
    sb.aread_text = AsyncMock(
        side_effect=AssertionError("read fell through to the sandbox")
    )
    return sb


@pytest.fixture
def automations(monkeypatch) -> MagicMock:
    """The automations table, holding one automation. A Glob lists it with
    ``names`` and a Grep reads it with ``rendered``, so each records whether
    a search touched the table."""
    table = MagicMock()
    table.names = AsyncMock(return_value=["daily-brief.json"])
    table.rendered = AsyncMock(
        return_value={"daily-brief.json": ('{"prompt": "Summarize ZEPH"}', "v1")}
    )
    monkeypatch.setattr(
        AutomationsBackend,
        "names",
        classmethod(lambda cls, user_id: table.names(user_id)),
    )
    monkeypatch.setattr(
        AutomationsBackend,
        "rendered",
        classmethod(lambda cls, user_id: table.rendered(user_id)),
    )
    return table


@pytest.fixture
def profile_rows(monkeypatch) -> dict[str, AsyncMock]:
    """The profile's rows, each file fetched by a ``fetch`` of its own, so a
    test can tell which files a search fetched."""
    contents = {
        "portfolio.json": '{"holdings": [{"symbol": "ZEPH"}]}',
        "watchlist.json": '{"symbols": ["MSFT"]}',
        "preference.json": '{"risk": "moderate"}',
        "user.json": '{"timezone": "UTC"}',
    }
    fetches: dict[str, AsyncMock] = {}
    for name, content in contents.items():
        file = UserDataBackend.files[name]
        fetches[name] = AsyncMock(return_value=content)
        monkeypatch.setattr(file, "fetch", fetches[name])
        monkeypatch.setattr(file, "render", lambda rows: (rows, "v1"))
    return fetches


@pytest.fixture
def sandbox() -> MagicMock:
    return _sandbox()


@pytest.fixture
def store() -> InMemoryStore:
    return InMemoryStore()


def _composite(sandbox, store, folder_name: str | None, root: str = ROOT):
    """Every route mounted over ``sandbox``, the turn's folder being
    ``folder_name`` under the computer root, or the root itself for None."""
    store.put((USER, "memory"), "memory.md", _envelope("Prefers ZEPH over MSFT."))
    store.put((USER, "memos"), "q1-thesis.md", _envelope("Q1 thesis."))
    store.put(workflow_namespace(USER), "brief.js", _envelope("// morning brief"))
    store.put(
        (USER, "workspaces", WORKSPACE, "memory"), "memory.md", _envelope("Home notes.")
    )
    composite, _ = build_filesystem_backend(
        backend=sandbox,
        gates=IdentityGates(
            user_memory=True,
            workspace_memory=True,
            memo=True,
            user_data=True,
            workflow=True,
            workflow_fs=True,
            workflow_tool=False,
        ),
        store=store,
        user_id=USER,
        workspace_id=WORKSPACE,
        layout=SandboxLayout(root).for_workspace(folder_name),
    )
    return composite


@pytest.fixture
def files(sandbox, store, automations, profile_rows):
    return _composite(sandbox, store, "Home")


@pytest.fixture
def route_greps(files, monkeypatch) -> dict[str, AsyncMock]:
    """Each route's ``agrep_rich``, by the route's root, recording every
    path a Grep reads it at."""
    spies: dict[str, AsyncMock] = {}
    for route in files._routes:
        spies[route.root_prefix] = AsyncMock(side_effect=route.agrep_rich)
        monkeypatch.setattr(route, "agrep_rich", spies[route.root_prefix])
    return spies


def _routes_read(route_greps: dict[str, AsyncMock]) -> list[str]:
    return [root for root, spy in route_greps.items() if spy.await_count]


def _computer_routes_read(route_greps: dict[str, AsyncMock]) -> list[str]:
    """The routes a search read, but for the folder's own memory."""
    return [root for root in _routes_read(route_greps) if root != WORKSPACE_MEMORY_ROOT]


async def _glob(files, pattern: str, path: str = ".") -> list[str]:
    return await files.aglob_paths(pattern, files.normalize_path(path))


async def _grep(files, pattern: str, path: str = ".", **kwargs) -> list:
    return await files.agrep_rich(pattern, path=files.normalize_path(path), **kwargs)


def _computer_routes(files) -> list[str]:
    """Each route's root as the folder spells it, for every route mounted
    at the computer root rather than in the folder."""
    return [
        route.root_prefix[len(ROOT) + 1 :].rstrip("/")
        for route in files._routes
        if not route.root_prefix.startswith(f"{FOLDER}/")
    ]


class TestGlobFromTheFolder:
    @pytest.mark.asyncio
    async def test_a_computer_tier_spelling_reaches_its_route(self, files):
        found = await _glob(files, ".agents/user/profile/*")

        assert set(found) == PROFILE_FILES

    @pytest.mark.asyncio
    async def test_every_route_at_the_computer_root_is_reached_by_its_spelling(
        self, files
    ):
        spellings = _computer_routes(files)
        assert {".agents/user/memory", ".agents/user/memo", ".agents/workflows"} <= set(
            spellings
        )
        for spelled in spellings:
            route = files.route_for(spelled)
            found = await _glob(files, f"{spelled}/*")

            assert found, spelled
            assert set(found) == set(await route.aglob_paths("*", route.root_prefix))

    @pytest.mark.asyncio
    async def test_the_printed_paths_read_back(self, files):
        glob = create_glob_tool(files)

        printed = await glob.ainvoke({"pattern": ".agents/user/memory/*"})

        assert printed.splitlines()[1:] == ["/.agents/user/memory/memory.md"]
        assert await files.aread_text("/.agents/user/memory/memory.md") == (
            "Prefers ZEPH over MSFT."
        )

    @pytest.mark.asyncio
    async def test_a_pattern_from_a_parent_spells_the_rest(self, files):
        assert set(await _glob(files, "profile/*", ".agents/user")) == PROFILE_FILES
        assert await _glob(files, "memory/*", ".agents/user") == [USER_MEMORY]
        assert await _glob(files, "user/memo/*", ".agents") == [MEMO]

    @pytest.mark.asyncio
    async def test_the_computer_root_spells_from_the_root(self, files):
        found = await _glob(files, ".agents/user/profile/*", ROOT)

        assert PROFILE_FILES <= set(found)

    @pytest.mark.asyncio
    async def test_the_routes_own_folder_is_unchanged(self, files, sandbox):
        found = await _glob(files, "*", ".agents/user/profile")

        assert set(found) == PROFILE_FILES
        sandbox.aglob_paths.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_wildcard_passes_the_mounts_links_by(self, files, automations):
        """``.agents/user`` and ``.agents/workflows`` are the file mount's
        links, in the folder and at the root, which the sandbox glob enters
        only by a pattern's literal prefix, so a wildcard reads none of their
        stores."""
        assert await _glob(files, "**/*.json") == []
        assert await _glob(files, "**/*.json", ROOT) == []
        assert await _glob(files, "*/user/**/*.json", ".agents") == []
        assert await _glob(files, "**/*.js") == []
        automations.names.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_literal_prefix_goes_through_a_link(self, files):
        json_files = PROFILE_FILES - {f"{PROFILE}README.md"} | {
            f"{AUTOMATIONS}daily-brief.json"
        }

        assert set(await _glob(files, ".agents/user/**/*.json")) == json_files
        assert set(await _glob(files, "**/*.json", ".agents/user")) == json_files
        assert set(await _glob(files, f"{ROOT}/.agents/user/**/*.json")) == json_files
        assert await _glob(files, "**/*.js", ".agents/workflows") == [WORKFLOW]

    @pytest.mark.asyncio
    async def test_a_bare_name_matches_at_any_depth(self, files):
        found = await _glob(files, "*.md")

        assert {ON_DISK, WORKSPACE_MEMORY} <= set(found)
        assert not {USER_MEMORY, MEMO, f"{PROFILE}README.md"} & set(found)


class TestGlobReachesNothingElse:
    @pytest.mark.asyncio
    async def test_a_subfolder_reaches_no_route(self, files, sandbox, automations):
        assert await _glob(files, "**/*", "data") == []
        sandbox.aglob_paths.assert_awaited_once_with("**/*", f"{FOLDER}/data")
        automations.names.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_another_workspaces_folder_reaches_no_route(
        self, files, sandbox, automations
    ):
        assert await _glob(files, "**/*", SIBLING) == []
        sandbox.aglob_paths.assert_awaited_once_with("**/*", SIBLING)
        automations.names.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_folder_reaches_only_itself_and_its_routes(self, files, sandbox):
        found = await _glob(files, "**/*")

        sandbox.aglob_paths.assert_awaited_once_with("**/*", FOLDER)
        roots = tuple(route.root_prefix for route in files._routes)
        assert all(p == ON_DISK or p.startswith(roots) for p in found)

    @pytest.mark.asyncio
    async def test_a_route_the_pattern_cannot_reach_is_never_listed(
        self, files, automations
    ):
        assert await _glob(files, "data/*.csv") == []
        assert await _glob(files, ".agents/user/profile/*.json")
        automations.names.assert_not_awaited()

        await _glob(files, ".agents/user/*/*.json")
        automations.names.assert_awaited_once()


class TestGlobListsEachFileOnce:
    @pytest.mark.asyncio
    async def test_the_folders_link_to_the_mount_yields_to_the_route(
        self, files, sandbox
    ):
        """With the file mount linked into the folder, the sandbox lists a
        route's file under the folder too; the route's copy is kept."""
        sandbox.aglob_paths = AsyncMock(
            return_value=[f"{FOLDER}/.agents/user/profile/portfolio.json", ON_DISK]
        )

        found = await _glob(files, ".agents/user/profile/*")

        assert set(found) == PROFILE_FILES | {ON_DISK}
        assert len(found) == len(set(found))

    @pytest.mark.asyncio
    async def test_the_roots_link_to_the_mount_yields_to_the_route(
        self, files, sandbox
    ):
        sandbox.aglob_paths = AsyncMock(return_value=[f"{PROFILE}portfolio.json"])

        found = await _glob(files, "*", ".agents/user")

        assert found.count(f"{PROFILE}portfolio.json") == 1


class TestGrepWalksAsRg:
    """rg skips a name that starts with a dot below the path it is given,
    unless the glob filter matches that entry itself. Every route sits under
    ``.agents``, so a Grep reaches the routes with a path into ``.agents``,
    or with a filter that matches the dot-folder's own name, and not
    otherwise: from the folder, from the computer root, or anywhere. The
    folder's own memory is the one exception, which a Grep from the folder
    reads by default (``TestGrepSearchesTheFoldersMemoryByDefault``)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".", ROOT])
    async def test_a_bare_grep_reads_no_route_of_the_computers(
        self, files, sandbox, route_greps, profile_rows, automations, path
    ):
        sandbox.agrep_rich.return_value = [ON_DISK]

        assert await _grep(files, "ZEPH", path) == [ON_DISK]
        assert _computer_routes_read(route_greps) == []
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()
        automations.rendered.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".", ROOT])
    @pytest.mark.parametrize(
        "glob",
        [
            "*.json",
            "**/*.json",
            ".agents/**",
            ".agents/user/profile/*",
            "**/profile/*",
            "portfolio.json",
        ],
    )
    async def test_a_filter_that_names_no_dot_folder_does_not_bring_it_back(
        self, files, route_greps, profile_rows, path, glob
    ):
        assert await _grep(files, "ZEPH", path, glob=glob) == []
        assert _computer_routes_read(route_greps) == []
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("glob", ["*", "**"])
    async def test_a_filter_that_matches_the_dot_folder_brings_it_back(
        self, files, sandbox, glob
    ):
        """rg enters ``.agents`` under ``--glob '*'`` or ``--glob '**'``, as
        the filter matches the folder's own name. The sandbox then also
        shows the folder's link to the mount, which yields to the route."""
        sandbox.agrep_rich.return_value = [
            f"{FOLDER}/.agents/user/profile/portfolio.json"
        ]

        found = await _grep(files, "ZEPH", glob=glob)

        assert sorted(found) == sorted(ROUTE_HITS)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path", [".agents", f"{ROOT}/.agents", ".agents/user", f"{ROOT}/.agents/user"]
    )
    async def test_a_path_into_the_dot_folder_searches_every_route_below(
        self, files, path
    ):
        assert set(await _grep(files, "ZEPH", path)) == ROUTE_HITS

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path", [".agents/user/profile", ".agents/user/profile/portfolio.json"]
    )
    async def test_a_path_at_a_route_or_one_of_its_files_searches_it(self, files, path):
        assert await _grep(files, "ZEPH", path) == [f"{PROFILE}portfolio.json"]

    @pytest.mark.asyncio
    async def test_a_hit_rg_shows_under_the_folders_link_stands_at_the_route(
        self, files, sandbox, route_greps
    ):
        """rg's own hits stand, a route's file among them printed at the
        route's root, and no route of the computer's is read for them."""
        sandbox.agrep_rich.return_value = [
            f"{FOLDER}/.agents/user/profile/portfolio.json",
            ON_DISK,
        ]

        assert await _grep(files, "ZEPH") == [PORTFOLIO, ON_DISK]
        assert _computer_routes_read(route_greps) == []


class TestGrepSearchesTheFoldersMemoryByDefault:
    """A Grep from the folder searches the folder's own memory,
    ``.agents/memory/``, as though ``path`` named it beside the folder, so
    its files are found by default and only a dot-name inside it is skipped.
    The user tier's memory is the computer's, and walks as rg walks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".", "/", FOLDER])
    async def test_a_bare_grep_from_the_folder_finds_it(self, files, route_greps, path):
        assert await _grep(files, "Home notes", path) == [WORKSPACE_MEMORY]
        assert _routes_read(route_greps) == [WORKSPACE_MEMORY_ROOT]

    @pytest.mark.asyncio
    async def test_not_the_user_tiers_memory(self, files, route_greps):
        assert await _grep(files, "Prefers") == []
        assert _computer_routes_read(route_greps) == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [ROOT, "data", SIBLING])
    async def test_no_other_path_searches_it_by_default(self, files, route_greps, path):
        assert await _grep(files, "Home notes", path) == []
        assert _routes_read(route_greps) == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".agents", ".agents/memory"])
    async def test_a_path_into_it_searches_it(self, files, path):
        assert await _grep(files, "Home notes", path) == [WORKSPACE_MEMORY]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("glob", "found"),
        [
            ("*.md", [WORKSPACE_MEMORY]),
            ("memory.md", [WORKSPACE_MEMORY]),
            (".agents/memory/*", [WORKSPACE_MEMORY]),
            ("/.agents/memory/*", [WORKSPACE_MEMORY]),
            ("*.json", []),
            ("Home/.agents/memory/*", []),
        ],
    )
    async def test_its_files_meet_the_filter_as_the_folders_do(
        self, files, glob, found
    ):
        """rg matches a filter that names a folder from the folder, where it
        runs: ``.agents/memory/*`` matches the folder's memory as
        ``reports/*`` matches the folder's ``reports``."""
        assert await _grep(files, "Home notes", glob=glob) == found

    @pytest.mark.asyncio
    async def test_a_dot_name_inside_it_is_still_skipped(self, files, store):
        store.put(
            (USER, "workspaces", WORKSPACE, "memory"),
            ".draft.md",
            _envelope("Home notes"),
        )
        draft = f"{WORKSPACE_MEMORY_ROOT}.draft.md"

        assert await _grep(files, "Home notes") == [WORKSPACE_MEMORY]
        assert set(await _grep(files, "Home notes", glob="*.md")) == {
            WORKSPACE_MEMORY,
            draft,
        }

    @pytest.mark.asyncio
    async def test_a_folder_at_the_computer_root_searches_only_its_memory(
        self, store, automations, profile_rows
    ):
        """A workspace that owns the computer root holds the user tier under
        its folder by its real path. The user tier is still the computer's,
        so a bare Grep searches the workspace's memory alone."""
        files = _composite(_sandbox(ROOT), store, None)

        assert await _grep(files, "Home notes|Prefers") == [
            f"{ROOT}/.agents/memory/memory.md"
        ]
        automations.rendered.assert_not_awaited()


class TestGrepFilterMatchesFromTheFolder:
    """rg runs in the turn's folder and matches its filter against each path
    from there, whatever path it searches but the computer root's
    ``.agents``, which it searches from the root: ``--glob 'reports/*'``
    matches the folder's ``reports``, as does ``/reports/*``. A route's files
    meet the filter where the agent finds them, so one that names a folder
    spells it from the folder, not from ``path``. Any other path outside the
    folder is matched whole, which only a filter that starts with ``**`` or
    names no folder can match."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("path", "glob", "found"),
        [
            (".agents", ".agents/user/profile/w*", f"{PROFILE}watchlist.json"),
            (".agents/user", ".agents/user/profile/w*", f"{PROFILE}watchlist.json"),
            (".agents/user", "/.agents/user/profile/w*", f"{PROFILE}watchlist.json"),
            (".agents/user", "**/profile/w*", f"{PROFILE}watchlist.json"),
            (
                ".agents/user/profile",
                ".agents/user/profile/w*",
                f"{PROFILE}watchlist.json",
            ),
            (".agents/user/memory", ".agents/user/memory/*.md", USER_MEMORY),
        ],
    )
    async def test_a_filter_that_names_a_folder_matches_from_the_folder(
        self, files, path, glob, found
    ):
        assert await _grep(files, "ZEPH|MSFT", path, glob=glob) == [found]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("path", "glob"),
        [
            (".agents", "user/profile/w*"),
            (".agents/user", "profile/w*"),
            (".agents/user/profile", "profile/w*"),
            (".agents/user/memory", "memory/*.md"),
        ],
    )
    async def test_a_filter_spelled_from_the_path_matches_nothing(
        self, files, profile_rows, path, glob
    ):
        assert await _grep(files, "ZEPH|MSFT", path, glob=glob) == []
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("path", "found"),
        [
            (".agents/user/profile/portfolio.json", f"{PROFILE}portfolio.json"),
            (".agents/user/memory/memory.md", USER_MEMORY),
        ],
    )
    async def test_a_file_the_path_names_is_searched_whatever_the_filter(
        self, files, path, found
    ):
        """rg searches a file it is given, whatever ``--glob`` says."""
        assert await _grep(files, "ZEPH", path, glob="*.csv") == [found]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("glob", "found"),
        [
            ("{**/.agents,**/profile/w*}", [f"{PROFILE}watchlist.json"]),
            ("{.agents,w*}", [f"{PROFILE}watchlist.json"]),
            ("{.agents,.agents/user/profile/w*}", []),
            ("{.agents,/.agents/user/profile/w*}", []),
        ],
    )
    async def test_a_path_outside_the_folder_is_matched_whole(
        self, files, profile_rows, glob, found
    ):
        """The computer root is no name in the folder, so rg, which runs in
        the folder for it, matches what it finds there by the whole path, as
        it matches ``../data/a.csv``: once a filter brings back the dot
        folder, ``**/profile/*`` or ``w*`` match, a filter anchored at the
        folder cannot, and neither can one whose leading ``/`` rg reads as
        the folder."""
        assert await _grep(files, "ZEPH|MSFT", ROOT, glob=glob) == found
        if not found:
            for fetch in profile_rows.values():
                fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("glob", "found"),
        [
            (".agents/user/profile/w*", [f"{PROFILE}watchlist.json"]),
            ("/.agents/user/profile/w*", [f"{PROFILE}watchlist.json"]),
            ("**/profile/w*", [f"{PROFILE}watchlist.json"]),
            ("user/profile/w*", []),
            (f"{PROFILE}w*", []),
        ],
    )
    async def test_the_roots_agents_matches_from_the_root(
        self, files, profile_rows, glob, found
    ):
        """rg searches the computer root's ``.agents`` from the root, as it
        searches the computer tier below it, so a filter matches a file there
        as Grep prints it, ``/.agents/...``, and not as spelled from the path."""
        assert await _grep(files, "ZEPH|MSFT", f"{ROOT}/.agents", glob=glob) == found
        if not found:
            for fetch in profile_rows.values():
                fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_folder_at_the_computer_root_matches_everything_from_there(
        self, store, automations, profile_rows
    ):
        """A workspace that owns the computer root runs rg there, so every
        route is inside its folder however the path names it."""
        files = _composite(_sandbox(ROOT), store, None)

        for path in (".agents", f"{ROOT}/.agents"):
            found = await _grep(
                files, "ZEPH|MSFT", path, glob=".agents/user/profile/w*"
            )

            assert found == [f"{PROFILE}watchlist.json"]


class TestGrepSkipsDotNamesInsideARoute:
    """Below the path a Grep is given, a dot-name inside a route is skipped
    as one outside it is, and the path itself is searched whatever its
    name."""

    @pytest.fixture
    def drafts(self, store) -> dict[str, str]:
        """Memory files rg would skip below the memory folder."""
        names = {
            "draft": ".draft.md",
            "in_dot_folder": ".old/notes.md",
            "nested": "notes/.scratch.md",
        }
        for name in names.values():
            store.put((USER, "memory"), name, _envelope(f"ZEPH in {name}"))
        return {
            key: f"{ROOT}/.agents/user/memory/{name}" for key, name in names.items()
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".agents", ".agents/user", ".agents/user/memory"])
    async def test_a_grep_above_them_skips_them(self, files, drafts, path):
        found = await _grep(files, "ZEPH", path)

        assert USER_MEMORY in found
        assert not set(drafts.values()) & set(found)

    @pytest.mark.asyncio
    async def test_a_path_at_one_searches_it(self, files, drafts):
        assert await _grep(files, "ZEPH", ".agents/user/memory/.draft.md") == [
            drafts["draft"]
        ]
        assert await _grep(files, "ZEPH", ".agents/user/memory/.old") == [
            drafts["in_dot_folder"]
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".agents/user", ".agents/user/memory"])
    async def test_a_filter_that_matches_a_dot_file_brings_it_back(
        self, files, drafts, path
    ):
        """rg lists ``.draft.md`` under ``--glob '*.md'``, which matches the
        file itself, but not ``.old/notes.md``, whose folder it does not
        match."""
        found = await _grep(files, "ZEPH", path, glob="*.md")

        assert set(found) == {USER_MEMORY, drafts["draft"], drafts["nested"]}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".agents/user", ".agents/user/memory"])
    async def test_glob_lists_them_as_the_disk_glob_does(self, files, drafts, path):
        found = await _glob(files, "**/*.md", path)

        assert {USER_MEMORY, *drafts.values()} <= set(found)


class TestGrepFromAboveARoute:
    @pytest.mark.asyncio
    async def test_a_bare_glob_filter_matches_file_names(self, files):
        assert await _grep(files, "ZEPH", ".agents/user", glob="*.md") == [USER_MEMORY]

    @pytest.mark.asyncio
    async def test_the_filter_reads_the_hit_in_every_output_mode(self, files):
        portfolio = ".agents/user/profile/portfolio.json"
        lines = await _grep(
            files, "ZEPH|MSFT", ".agents/user", glob=portfolio, output_mode="content"
        )
        counts = await _grep(
            files, "ZEPH|MSFT", ".agents/user", glob=portfolio, output_mode="count"
        )

        assert lines == [
            f'{PROFILE}portfolio.json:1:{{"holdings": [{{"symbol": "ZEPH"}}]}}'
        ]
        assert counts == [(f"{PROFILE}portfolio.json", 1)]

    @pytest.mark.asyncio
    async def test_the_routes_own_folder_is_unchanged(self, files, sandbox):
        assert await _grep(files, "ZEPH", ".agents/user/profile") == [
            f"{PROFILE}portfolio.json"
        ]
        sandbox.agrep_rich.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_another_workspaces_folder_reaches_no_route(self, files, automations):
        assert await _grep(files, "ZEPH", SIBLING) == []
        automations.rendered.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_subfolder_reaches_no_route(self, files, automations):
        assert await _grep(files, "ZEPH", "data") == []
        automations.rendered.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_route_the_glob_filter_cannot_reach_is_never_read(
        self, files, automations
    ):
        assert await _grep(files, "ZEPH", ".agents/user", glob="data/*.csv") == []
        automations.rendered.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_roots_link_to_the_mount_yields_to_the_route(
        self, files, sandbox
    ):
        sandbox.agrep_rich.return_value = [f"{PROFILE}portfolio.json", ON_DISK]

        found = await _grep(files, "ZEPH", ".agents/user")

        assert found.count(f"{PROFILE}portfolio.json") == 1
        assert ON_DISK in found

    @pytest.mark.asyncio
    async def test_the_printed_paths_read_back(self, files):
        grep = create_grep_tool(files)

        printed = await grep.ainvoke({"pattern": "Prefers", "path": ".agents/user"})

        assert printed.splitlines()[1:] == ["/.agents/user/memory/memory.md"]
        assert await files.aread_text("/.agents/user/memory/memory.md") == (
            "Prefers ZEPH over MSFT."
        )


class TestSearchSkipsWhatCannotMatch:
    """A route whose file names are fixed is never read for a file a pattern
    can't match, since its names say so without a read."""

    @pytest.mark.asyncio
    async def test_a_glob_that_matches_no_fixed_name_never_lists_the_route(
        self, files, monkeypatch
    ):
        route = files.route_for(".agents/user/profile")
        listing = AsyncMock(side_effect=route.aglob_paths)
        monkeypatch.setattr(route, "aglob_paths", listing)

        assert await _glob(files, "**/*.csv", ".agents/user") == []
        listing.assert_not_awaited()

        assert await _glob(files, "**/watchlist.json", ".agents/user") == [
            f"{PROFILE}watchlist.json"
        ]
        listing.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_filter_that_matches_no_fixed_name_fetches_nothing(
        self, files, profile_rows, automations
    ):
        assert await _grep(files, "ZEPH", ".agents/user", glob="*.csv") == []
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()
        # The automations' rows name their files, so they are read as before.
        automations.rendered.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_filter_fetches_only_the_files_it_can_match(
        self, files, profile_rows
    ):
        found = await _grep(files, "ZEPH|MSFT", ".agents/user", glob="watchlist.json")

        assert found == [f"{PROFILE}watchlist.json"]
        profile_rows["watchlist.json"].assert_awaited_once()
        profile_rows["portfolio.json"].assert_not_awaited()
        profile_rows["preference.json"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_filter_in_the_routes_own_folder_fetches_only_what_it_matches(
        self, files, profile_rows
    ):
        found = await _grep(files, "ZEPH|MSFT", ".agents/user/profile", glob="w*")

        assert found == [f"{PROFILE}watchlist.json"]
        profile_rows["portfolio.json"].assert_not_awaited()
        profile_rows["preference.json"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_path_at_one_file_searches_that_file_alone(
        self, files, profile_rows
    ):
        found = await _grep(files, "ZEPH|MSFT", ".agents/user/profile/portfolio.json")

        assert found == [f"{PROFILE}portfolio.json"]
        profile_rows["watchlist.json"].assert_not_awaited()
        assert await _grep(files, "ZEPH", ".agents/user/profile/missing.json") == []


class TestGlobInsideARoute:
    """With ``path`` at or inside a route, Glob matches the route's files as
    the sandbox glob matches a folder on disk, not as the route would: ``*``
    stays within one name and ``**/`` takes the top level too."""

    @pytest.fixture
    def nested(self, store) -> str:
        store.put((USER, "memory"), "notes/q3.md", _envelope("Q3 notes."))
        return f"{ROOT}/.agents/user/memory/notes/q3.md"

    @pytest.mark.asyncio
    async def test_a_recursive_pattern_takes_the_top_level_files(self, files, nested):
        found = await _glob(files, "**/*.md", ".agents/user/memory")

        assert set(found) == {USER_MEMORY, nested}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("pattern", "found"),
        [
            ("*", ["memory.md", "notes/q3.md"]),
            ("*/*.md", ["notes/q3.md"]),
            ("notes/*", ["notes/q3.md"]),
            ("n*", []),
            ("*.{md,json}", []),
        ],
    )
    async def test_a_star_stays_within_one_name(self, files, nested, pattern, found):
        """``n*`` names the folder ``notes``, and no file in it."""
        listed = await _glob(files, pattern, ".agents/user/memory")

        assert sorted(listed) == [
            f"{ROOT}/.agents/user/memory/{name}" for name in found
        ]

    @pytest.mark.asyncio
    async def test_a_class_reads_as_the_sandbox_glob_reads_it(self, files):
        """Glob is not rg: ``[^p]`` is a ``^`` or a ``p``."""
        found = await _glob(files, "[^p]*.json", ".agents/user/profile")

        assert sorted(found) == [
            f"{PROFILE}portfolio.json",
            f"{PROFILE}preference.json",
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path", [".agents/user/memory/memory.md", ".agents/user/memory/missing"]
    )
    async def test_a_path_at_a_file_or_at_nothing_finds_nothing(self, files, path):
        assert await _glob(files, "*", path) == []


# What a Grep for ZEPH or the memo's text finds below `.agents/user`.
USER_TIER_HITS = ROUTE_HITS | {MEMO}
PORTFOLIO = f"{PROFILE}portfolio.json"
DAILY_BRIEF = f"{AUTOMATIONS}daily-brief.json"


class TestGrepFilterAsRgReadsIt:
    """Grep's ``glob`` is rg's: a leading ``!`` negates it, skipping each file
    or folder it matches below the path with everything inside, a trailing
    ``/`` keeps it to folders, ``{a,b}`` alternates and ``[^a]`` is
    ``[!a]``. A filter with no ``/`` matches at any depth, decided on the
    whole filter before its alternatives are expanded."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("glob", "found"),
        [
            ("!*.json", {USER_MEMORY, MEMO}),
            ("!*.md", {PORTFOLIO, DAILY_BRIEF}),
            ("!memory", USER_TIER_HITS - {USER_MEMORY}),
            ("!memory/", USER_TIER_HITS - {USER_MEMORY}),
            ("!**/memory/**", USER_TIER_HITS - {USER_MEMORY}),
            ("!.agents/user/memory", USER_TIER_HITS - {USER_MEMORY}),
            ("!/memory", USER_TIER_HITS),
            ("!memory.md/", USER_TIER_HITS),
            ("!", set()),
            ("*.{md,json}", USER_TIER_HITS),
            ("{portfolio.json,.agents/user/memo/*}", {MEMO}),
            (".agents/user/{memo,memory}/*", {MEMO, USER_MEMORY}),
            ("[^p]*.json", {DAILY_BRIEF}),
            ("[!p]*.json", {DAILY_BRIEF}),
            ("{,d}*.json", {DAILY_BRIEF}),
            ("memory.md}", {USER_MEMORY}),
            ("*.json ", {PORTFOLIO, DAILY_BRIEF}),
            ("#*.json", USER_TIER_HITS),
        ],
    )
    async def test_route_files_meet_it_as_files_on_disk(self, files, glob, found):
        """``/memory`` and the alternative ``portfolio.json`` are anchored at
        the folder, where no such file is. A trailing space is dropped, an
        empty alternative is no alternative, a ``}`` outside braces is
        dropped, and a ``#`` comment is no filter, as rg reads them."""
        assert (
            set(await _grep(files, "ZEPH|Q1 thesis", ".agents/user", glob=glob))
            == found
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("path", "glob", "found"),
        [
            (".agents/user/profile", "!profile", [PORTFOLIO]),
            (".agents/user/memory", "!memory", [USER_MEMORY]),
            (".agents/user/memory", "!.agents", [USER_MEMORY]),
            (".agents/user/profile/portfolio.json", "!*.json", [PORTFOLIO]),
        ],
    )
    async def test_a_negated_filter_never_skips_the_path(
        self, files, path, glob, found
    ):
        assert await _grep(files, "ZEPH", path, glob=glob) == found

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("glob", "found"),
        [
            ("!memory", [WORKSPACE_MEMORY]),
            ("!.agents", [WORKSPACE_MEMORY]),
            ("!*.md", []),
        ],
    )
    async def test_the_folders_memory_is_searched_as_a_path(
        self, files, route_greps, glob, found
    ):
        """From the folder, ``.agents/memory/`` is searched as though ``path``
        named it, so a negated filter skips its files and never the folder,
        and brings back none of the computer's under ``.agents``."""
        assert await _grep(files, "Home notes", glob=glob) == found
        assert _computer_routes_read(route_greps) == []

    @pytest.mark.asyncio
    async def test_a_file_a_negated_filter_skips_is_never_fetched(
        self, files, profile_rows
    ):
        found = await _grep(
            files, "ZEPH|MSFT", ".agents/user/profile", glob="!portfolio.json"
        )

        assert found == [f"{PROFILE}README.md", f"{PROFILE}watchlist.json"]
        profile_rows["portfolio.json"].assert_not_awaited()
        profile_rows["watchlist.json"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_folder_a_negated_filter_skips_is_never_read(
        self, files, profile_rows
    ):
        found = await _grep(files, "ZEPH", ".agents/user", glob="!profile")

        assert set(found) == {USER_MEMORY, DAILY_BRIEF}
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("glob", ["{a,{b,c}}", "{a,b", "[a", "[z-a]*", "x\\"])
    async def test_a_filter_rg_refuses_finds_nothing(
        self, files, sandbox, profile_rows, automations, glob
    ):
        """The sandbox's rg (14.1, which also refuses nested braces) searches
        nothing for these, not even a file its path names. Inside a route or
        above it, the sandbox is asked, and its answer, which is rg's
        error, stands."""
        sandbox.agrep_rich.return_value = ["rg: error parsing glob"]

        assert await _grep(
            files, "ZEPH", ".agents/user/profile/portfolio.json", glob=glob
        ) == ["rg: error parsing glob"]
        assert await _grep(files, "ZEPH", ".agents/user", glob=glob) == [
            "rg: error parsing glob"
        ]
        assert [call.kwargs["path"] for call in sandbox.agrep_rich.await_args_list] == [
            PORTFOLIO,
            f"{ROOT}/.agents/user",
        ]
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()
        automations.rendered.assert_not_awaited()


class TestGrepFilterTokensAsRg14:
    """The sandbox's rg (14.1) reads a filter token by token, and the
    composite reads it the same way rather than respelling it as a Python
    glob, which loses where a token ends."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("glob", "found"),
        [
            # Each alternative of `{*,c}` stays within one name.
            (".agents/user/{*,c}*", set()),
            # A `}` outside braces is dropped (nested braces are refused,
            # `test_a_filter_rg_refuses_finds_nothing`).
            (".agents/user/memory/*}*", {USER_MEMORY}),
            # A chained range extends the one before it.
            ("[a-c-e]*.json", {DAILY_BRIEF}),
            ("[a-c-b]*.json", set()),
            # `**` before an escaped `/` is two stars, not any number of
            # folders, and a class may hold the `/`.
            (".agents/user/memory/**\\/memory.md", set()),
            (".agents/user/memory[/]memory.md", {USER_MEMORY}),
            # As many alternatives as rg can take.
            (
                "{" + ",".join(f"q{n}" for n in range(300)) + ",memory}.md",
                {USER_MEMORY},
            ),
            ("{d,p,q}{a,o,x}{i,r,x}{l,t,x}{y,f,x}*", {PORTFOLIO, DAILY_BRIEF}),
        ],
    )
    async def test_route_files_meet_it_as_rg_14_does(self, files, glob, found):
        """Checked against ripgrep 14.1.0; the ones 15 reads otherwise sit
        here rather than in the on-disk table."""
        hits = await _grep(files, "ZEPH|Q1 thesis", ".agents/user", glob=glob)

        assert set(hits) == found


class TestGrepTypeAsRg:
    """Grep's ``type`` holds route files to rg's type table: a filter that
    matches decides first, a plain filter ignoring what it does not match,
    then the type selects by name, which also brings back a dot-file."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("file_type", "glob", "found"),
        [
            ("json", None, {PORTFOLIO, DAILY_BRIEF}),
            ("md", None, {USER_MEMORY, MEMO}),
            ("all", None, USER_TIER_HITS),
            ("make", None, set()),
            ("json", "*.md", {USER_MEMORY, MEMO}),
            ("json", "!*.md", {PORTFOLIO, DAILY_BRIEF}),
            ("json", "!*.json", set()),
            ("md", "!portfolio.json", {USER_MEMORY, MEMO}),
        ],
    )
    async def test_route_files_meet_it_as_files_on_disk(
        self, files, route_greps, file_type, glob, found
    ):
        hits = await _grep(
            files, "ZEPH|Q1 thesis", ".agents/user", glob=glob, type=file_type
        )

        assert set(hits) == found
        for spy in route_greps.values():
            assert all(call.kwargs.get("type") is None for call in spy.await_args_list)

    @pytest.mark.asyncio
    async def test_a_type_brings_back_a_dot_file_it_selects(self, files, store):
        store.put((USER, "memory"), ".draft.md", _envelope("ZEPH draft"))

        assert await _grep(files, "ZEPH", ".agents/user/memory") == [USER_MEMORY]
        assert set(await _grep(files, "ZEPH", ".agents/user/memory", type="md")) == {
            USER_MEMORY,
            f"{ROOT}/.agents/user/memory/.draft.md",
        }

    @pytest.mark.asyncio
    async def test_a_file_the_path_names_is_searched_whatever_the_type(self, files):
        assert await _grep(files, "ZEPH", PORTFOLIO, type="md") == [PORTFOLIO]

    @pytest.mark.asyncio
    async def test_a_type_that_selects_no_fixed_name_fetches_nothing(
        self, files, profile_rows
    ):
        assert await _grep(files, "ZEPH", ".agents/user/profile", type="md") == []
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [PORTFOLIO, ".agents/user"])
    async def test_a_type_rg_does_not_know_gets_rgs_answer(
        self, files, sandbox, profile_rows, automations, path
    ):
        sandbox.agrep_rich.return_value = ["rg: unrecognized file type: nope"]

        assert await _grep(files, "ZEPH", path, type="nope") == [
            "rg: unrecognized file type: nope"
        ]
        sandbox.agrep_rich.assert_awaited_once()
        for fetch in profile_rows.values():
            fetch.assert_not_awaited()
        automations.rendered.assert_not_awaited()


class TestGrepKeepsRgsOwnHits:
    """rg's hits stand, each route file among them printed at its route's
    root however rg reached it, and a route adds only the files rg did not
    list, once each, in every output mode."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "listed", [USER_MEMORY, f"{FOLDER}/.agents/user/memory/memory.md"]
    )
    async def test_a_file_rg_lists_is_listed_once(self, files, sandbox, listed):
        sandbox.agrep_rich.return_value = [listed]

        found = await _grep(files, "ZEPH", ".agents/user")

        assert found[0] == USER_MEMORY
        assert sorted(found) == sorted(ROUTE_HITS)

    @pytest.mark.asyncio
    async def test_a_count_rg_gives_stands(self, files, sandbox):
        sandbox.agrep_rich.return_value = [
            (f"{FOLDER}/.agents/user/memory/memory.md", 3)
        ]

        found = await _grep(files, "ZEPH", ".agents/user", output_mode="count")

        assert found[0] == (USER_MEMORY, 3)
        assert sorted(path for path, _ in found) == sorted(ROUTE_HITS)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "spelled", [USER_MEMORY, f"{FOLDER}/.agents/user/memory/memory.md"]
    )
    async def test_lines_rg_gives_stand_with_their_context(
        self, files, sandbox, spelled
    ):
        """rg's context and multiline matches are what a route cannot give,
        so its lines for a file are the file's lines."""
        sandbox.agrep_rich.return_value = [
            GrepLine(f"{spelled}:1:Prefers ZEPH over MSFT.", spelled),
            GrepLine(f"{spelled}-2-and then some", spelled),
        ]

        found = await _grep(
            files, "ZEPH", ".agents/user", output_mode="content", lines_after=1
        )

        assert found[:2] == [
            f"{USER_MEMORY}:1:Prefers ZEPH over MSFT.",
            f"{USER_MEMORY}-2-and then some",
        ]
        assert not [line for line in found[2:] if line.startswith(USER_MEMORY)]
        assert {line.split(":", 1)[0] for line in found[2:]} == ROUTE_HITS - {
            USER_MEMORY
        }

    @pytest.mark.asyncio
    async def test_a_hit_rg_gives_stands_when_the_route_fails_to_read(
        self, files, sandbox, profile_rows
    ):
        profile_rows["portfolio.json"].side_effect = RuntimeError("database down")
        sandbox.agrep_rich.return_value = [PORTFOLIO]

        assert sorted(await _grep(files, "ZEPH", ".agents/user")) == sorted(ROUTE_HITS)
        profile_rows["portfolio.json"].assert_awaited_once()


class TestGlobThroughTheFolderSpelling:
    """An absolute Glob pattern finds a route's file by either of its names:
    at the route's root, or where the file mount links it into the folder."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "path", [".", ".agents/user/profile", ".agents/user/memory"]
    )
    @pytest.mark.parametrize(
        "pattern",
        [f"{FOLDER}/.agents/user/profile/*.json", f"{PROFILE}*.json"],
    )
    async def test_either_name_finds_it_from_anywhere(self, files, path, pattern):
        found = await _glob(files, pattern, path)

        assert sorted(found) == sorted(PROFILE_FILES - {f"{PROFILE}README.md"})

    @pytest.mark.asyncio
    async def test_a_hit_the_sandbox_gives_at_the_link_is_listed_once(
        self, files, sandbox
    ):
        sandbox.aglob_paths = AsyncMock(
            return_value=[f"{FOLDER}/.agents/user/profile/portfolio.json"]
        )

        found = await _glob(files, f"{FOLDER}/.agents/user/profile/*.json")

        assert found[0] == PORTFOLIO
        assert sorted(found) == sorted(f for f in PROFILE_FILES if f.endswith(".json"))


class TestGlobMatchesNamesAsTheDiskGlob:
    """Glob matches one name at a time, as the sandbox's glob does: a class
    never matches ``/``, and the folders the sandbox glob drops are dropped
    in a route too."""

    @pytest.fixture
    def tree(self, store):
        for name in ("sub/s.json", "as.json", "a/c.json", "vendor/v.md"):
            store.put((USER, "memory"), name, _envelope("x"))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("pattern", "found"),
        [
            ("*[!s]s.json", {"as.json"}),
            ("a[!s]**", set()),
            ("a?**", {"as.json"}),
            ("**/*.md", {"memory.md"}),
            ("vendor/*", set()),
        ],
    )
    async def test_inside_a_route(self, files, tree, pattern, found):
        hits = await _glob(files, pattern, ".agents/user/memory")

        assert {hit[len(USER_MEMORY) - len("memory.md") :] for hit in hits} == found

    @pytest.mark.asyncio
    async def test_an_excluded_folder_is_searched_when_the_path_names_it(
        self, files, tree
    ):
        assert await _glob(files, "*.md", ".agents/user/memory/vendor") == [
            f"{ROOT}/.agents/user/memory/vendor/v.md"
        ]


class TestMatchingTakesPolynomialTime:
    """A pattern is matched by a state set, one character at a time, so no
    pattern takes the server's event loop for longer than the path is long."""

    PATTERNS = ("/".join(["**", "*a*"] * 8) + "/b", "**/a/" * 16 + "MISSING")

    @pytest.fixture
    def deep(self, store):
        for name in ("/".join(["aaaa"] * 14) + "/c.md", "a/" * 32 + "x"):
            store.put((USER, "memory"), name, _envelope("ZEPH"))

    @pytest.mark.asyncio
    async def test_glob_and_grep_answer_at_once(self, files, deep):
        started = time.perf_counter()
        for pattern in self.PATTERNS:
            assert await _glob(files, pattern, ".agents/user/memory") == []
            assert await _glob(files, pattern) == []
            assert await _grep(files, "ZEPH", ".agents/user/memory", glob=pattern) == []
            assert await _grep(files, "ZEPH", ".agents/user", glob=pattern) == []

        assert time.perf_counter() - started < 2.0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pattern", ["[" * 30000, "**/" * 3000 + "*.md"], ids=["open-class", "globstars"]
    )
    async def test_a_long_glob_answers_at_once(self, files, sandbox, deep, pattern):
        """A ``[`` left open is a character, found so without a search to
        the end of the pattern for each, and a run of ``**`` is passed once
        a name, not once for each ``**`` before it."""
        sandbox.aglob_paths = AsyncMock(return_value=[])
        same = "**/*.md" if pattern.endswith(".md") else "[" * 3

        started = time.perf_counter()
        in_route = await _glob(files, pattern, ".agents/user/memory")
        found = await _glob(files, pattern)

        assert time.perf_counter() - started < 2.0
        assert in_route == await _glob(files, same, ".agents/user/memory")
        assert found == await _glob(files, same)

    @pytest.mark.asyncio
    async def test_a_filter_of_a_thousand_alternatives_answers_at_once(
        self, files, store
    ):
        """A state set is matched whole a byte at a time, so a filter of a
        thousand alternatives costs a step about what one does, even on names
        that carry hundreds of them part way."""
        letters = "abcdefghijklmnopqrstuvwxyz"
        draw = random.Random(1)
        for i in range(300):
            name = "".join(draw.choice(letters[:13]) for _ in range(40))
            store.put((USER, "memos"), f"{name}-{i:03d}.md", _envelope("ZEPH"))
        firsts, lasts = letters[:10], letters[-10:]
        alternatives = [
            f"**/*{a}*{b}*{c}*" for a in firsts for b in firsts for c in lasts
        ]
        glob = "{" + ",".join([*alternatives, "**/*-1*"]) + "}"

        started = time.perf_counter()
        found = await _grep(files, "ZEPH", ".agents/user/memo", glob=glob)

        assert time.perf_counter() - started < 1.0
        assert len(found) == 100

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("alternatives", "off_loop"), [(10, False), (200, True)], ids=["short", "long"]
    )
    async def test_a_long_filter_is_matched_off_the_event_loop(
        self, files, monkeypatch, alternatives, off_loop
    ):
        loop_thread = threading.current_thread()
        on_loop: set[bool] = set()
        finds = RgWalk.finds

        def spy(walk: RgWalk, start: str, spelled: str) -> bool:
            on_loop.add(threading.current_thread() is loop_thread)
            return finds(walk, start, spelled)

        monkeypatch.setattr(RgWalk, "finds", spy)
        glob = "{" + ",".join(f"**/*{i}*" for i in range(alternatives)) + ",**/*.md}"

        assert await _grep(files, "ZEPH", ".agents/user/memory", glob=glob)
        assert await _grep(files, "ZEPH", ".agents/user", glob=glob)

        assert on_loop == {not off_loop}

    @pytest.mark.asyncio
    async def test_rg_lines_are_told_from_a_routes_by_their_file_at_once(
        self, files, sandbox, store
    ):
        """Whether rg listed a route's file is one lookup, not a pass over
        every line rg gave for each file a route holds: over thousands of
        short files, that pass takes seconds."""
        lines = 40
        names = [f"m{i:04d}.md" for i in range(3000)]
        body = "\n".join(["ZEPH"] * lines)
        for name in names:
            store.put((USER, "memos"), name, _envelope(body))
        sandbox.agrep_rich.return_value = [
            GrepLine(f"{MEMO_ROOT}{name}:{number}:ZEPH", f"{MEMO_ROOT}{name}")
            for name in names
            for number in range(1, lines + 1)
        ]

        started = time.perf_counter()
        found = await _grep(files, "ZEPH", ".agents/user", output_mode="content")

        assert time.perf_counter() - started < 3.0
        assert [line.path for line in found[: len(names) * lines]] == [
            f"{MEMO_ROOT}{name}" for name in names for _ in range(lines)
        ]
        assert not [
            line
            for line in found[len(names) * lines :]
            if line.path.startswith(MEMO_ROOT)
        ]


MEMO_ROOT = f"{ROOT}/.agents/user/memo/"
OUTPUT_MODES = ("files_with_matches", "count", "content")


def _answer(paths: list[str], output_mode: str, *, numbered: bool = True) -> list:
    """rg's answer naming ``paths`` in ``output_mode``, as the sandbox gives it."""
    if output_mode == "count":
        return [(path, 1) for path in paths]
    if output_mode == "content":
        return [
            GrepLine(f"{path}:2:ZEPH" if numbered else f"{path}:ZEPH", path)
            for path in paths
        ]
    return list(paths)


def _named(found: list, output_mode: str) -> list[str]:
    """The file each entry of a Grep's answer names."""
    if output_mode == "count":
        return [path for path, _ in found]
    if output_mode == "content":
        return [line.path for line in found]
    return list(found)


class TestGrepNamesAFileByItsWholeName:
    """A file's name may hold the ``:`` or ``-`` that ends it in a content
    line, and may begin another's, so an answer names each file by its
    path, never by what comes before a separator: in every output mode,
    inside a route and above one, a filter meets the whole name and a file
    rg lists is listed once."""

    NAMES = ("Q1: thesis.md", "notes", "notes-old.md", "notes-1-x.md")

    @pytest.fixture
    def memos(self, store) -> list[str]:
        for name in self.NAMES:
            store.put((USER, "memos"), name, _envelope("a\nZEPH"))
        return [f"{MEMO_ROOT}{name}" for name in self.NAMES]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("output_mode", OUTPUT_MODES)
    @pytest.mark.parametrize("path", [".agents/user/memo", ".agents/user"])
    @pytest.mark.parametrize(
        ("glob", "kept"),
        [
            (None, NAMES),
            ("*.md", ("Q1: thesis.md", "notes-old.md", "notes-1-x.md")),
            ("Q1*", ("Q1: thesis.md",)),
            ("notes", ("notes",)),
        ],
    )
    async def test_a_filter_meets_the_whole_name(
        self, files, memos, output_mode, path, glob, kept
    ):
        found = await _grep(files, "ZEPH", path, output_mode=output_mode, glob=glob)

        named = [hit for hit in _named(found, output_mode) if hit.startswith(MEMO_ROOT)]
        assert sorted(named) == sorted(f"{MEMO_ROOT}{name}" for name in kept)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("output_mode", "numbered"),
        [(mode, True) for mode in OUTPUT_MODES] + [("content", False)],
    )
    async def test_a_file_rg_lists_is_listed_once(
        self, files, sandbox, memos, output_mode, numbered
    ):
        listed = [f"{MEMO_ROOT}{name}" for name in ("Q1: thesis.md", "notes-1-x.md")]
        sandbox.agrep_rich.return_value = _answer(
            listed, output_mode, numbered=numbered
        )

        found = await _grep(
            files,
            "ZEPH",
            ".agents/user",
            output_mode=output_mode,
            show_line_numbers=numbered,
        )

        named = _named(found, output_mode)
        assert named[:2] == listed
        routed = sorted(hit for hit in named if hit.startswith(MEMO_ROOT))
        assert routed == sorted(memos)

    @pytest.mark.asyncio
    async def test_the_tool_prints_each_line_at_its_files_spelling(
        self, files, sandbox, memos
    ):
        own = f"{FOLDER}/q-1-a: b.md"
        sandbox.agrep_rich.return_value = [
            GrepLine(f"{own}-1-a", own),
            GrepLine(f"{own}:2:ZEPH", own),
        ]
        grep = create_grep_tool(files)

        printed = await grep.ainvoke(
            {"pattern": "ZEPH", "output_mode": "content", "B": 1}
        )
        in_route = await grep.ainvoke(
            {"pattern": "ZEPH", "path": ".agents/user/memo", "output_mode": "content"}
        )

        assert printed.splitlines()[2:] == ["/q-1-a: b.md-1-a", "/q-1-a: b.md:2:ZEPH"]
        assert sorted(in_route.splitlines()[2:]) == sorted(
            f"/.agents/user/memo/{name}:2:ZEPH" for name in self.NAMES
        )


class TestGrepAsksOfEachFileOnce:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [".agents/user/memo", ".agents/user"])
    async def test_every_line_of_a_file_meets_the_filter_with_it(
        self, files, store, monkeypatch, path
    ):
        """A file's lines are its own, so the walk is asked of the file once
        however many of them match."""
        asked: list[str] = []
        finds = RgWalk.finds

        def spy(walk: RgWalk, start: str, spelled: str) -> bool:
            asked.append(posixpath.join(start, spelled))
            return finds(walk, start, spelled)

        monkeypatch.setattr(RgWalk, "finds", spy)
        for i in range(4):
            store.put((USER, "memos"), f"m{i}.md", _envelope("\n".join(["ZEPH"] * 50)))

        found = await _grep(files, "ZEPH", path, output_mode="content", glob="m?.md")

        assert len(found) == 200
        assert len(asked) == len(set(asked))


class TestFixedNamesCoverEveryFile:
    @pytest.mark.asyncio
    async def test_every_name_a_user_data_route_can_list(self, files):
        """A search skips a file its ``fixed_names`` leaves out, so the
        names have to cover every file the route lists."""
        checked = 0
        for route in files._routes:
            if not isinstance(route, USER_DATA_ROUTES) or route.fixed_names is None:
                continue
            listed = await route.aglob_paths("*", route.root_prefix)
            assert {
                path[len(route.root_prefix) :] for path in listed
            } <= route.fixed_names
            assert set(await type(route).names(USER)) <= route.fixed_names
            checked += 1

        assert checked


# Below, each route's files are copied to disk and the sandbox's own glob and
# rg run over the copies, which every search through the routes must agree
# with. The routes gain names that exercise each rule a walk goes by:
# dot-names, a folder named like a file, a folder named like the route's own.
FOLDER_MEMORY_TREE = (
    "a.json",
    "b.md",
    ".hidden.md",
    "sub/c.json",
    "sub/d.md",
    "a/c.json",
    "user/e.md",
    ".old/o.md",
    "notes/.d.md",
    "memory/m.md",
    "vendor/v.md",
    "sub/node_modules/n.json",
    "threads/t.json",
    "as.json",
    "sub/s.json",
)
USER_MEMORY_TREE = ("sub/c.json", ".draft.md", "a/c.json", "vendor/u.md")
# The folder's own files, which only the sandbox holds.
FOLDER_TREE = ("report.md", ".hidden.md", "sub/c.json", "data/a.json")


def _rg(
    folder: str, glob: str | None, paths: list[str], file_type: str | None = None
) -> list[str]:
    """The files rg lists as holding any character, run from ``folder`` as
    the sandbox runs it."""
    filters = ["--glob", glob] if glob else []
    if file_type:
        filters += ["--type", file_type]
    done = subprocess.run(
        ["rg", "--no-config", "-l", *filters, ".", *paths],
        cwd=folder,
        capture_output=True,
        text=True,
        check=False,
    )
    return done.stdout.splitlines()


def _found_in(files, folder: str, path: str) -> str:
    """Where the agent finds ``path``: at its spelling in ``folder``, or,
    where it has none, at ``path`` itself."""
    virtual = files.virtualize_path(path)
    return path if virtual == path else f"{folder}{virtual}".rstrip("/")


def _rg_on(disk: str, root: str):
    """The sandbox's Grep: rg over ``disk``, which stands in for the
    computer at ``root`` and holds only the folder's own files, run from
    where the sandbox runs it."""

    async def grep(pattern, path, glob=None, type=None, **_):  # noqa: A002
        cwd = grep_working_dir(path, folder=f"{root}/Home", root=root)
        hits = _rg(disk + cwd[len(root) :], glob, [disk + path[len(root) :]], type)
        return [root + hit[len(disk) :] for hit in hits]

    return grep


class _LocalRuntime:
    """Runs the sandbox's glob script with this interpreter."""

    async def exec(self, command: str, timeout: int = 60):
        command = command.replace("python3", sys.executable, 1)
        done = subprocess.run(
            ["/bin/sh", "-c", command], capture_output=True, text=True
        )
        return SimpleNamespace(
            stdout=done.stdout, stderr=done.stderr, exit_code=done.returncode
        )


def _agents_glob(runtime_call):
    """The agent's Glob in the sandbox (``SandboxBackend.aglob_paths``),
    whose command goes to ``runtime_call``."""
    sandbox = SimpleNamespace(
        _wait_ready=AsyncMock(),
        config=SimpleNamespace(
            filesystem=SimpleNamespace(enable_path_validation=False)
        ),
        _normalize_search_path=lambda searched: searched,
        runtime=_LocalRuntime(),
        _runtime_call=runtime_call,
    )
    backend = SimpleNamespace(
        sandbox=SimpleNamespace(aglob_files=functools.partial(aglob_files, sandbox))
    )
    return functools.partial(SandboxBackend.aglob_paths, backend)


async def _disk_glob(pattern: str, path: str) -> list[str]:
    """The sandbox's own Glob, run here over the disk at ``path``, with the
    folders it drops and its history rule."""

    async def call(fn, *args, retry_policy=None, **kwargs):
        return await fn(*args, **kwargs)

    return await _agents_glob(call)(pattern, path)


def _glob_on(disk: str, root: str):
    """The sandbox's Glob over ``disk``, standing in for the computer at
    ``root`` with only the folder's own files."""

    async def glob(pattern, path):
        if pattern.startswith(root):
            pattern = disk + pattern[len(root) :]
        hits = await _disk_glob(pattern, disk + path[len(root) :])
        return [root + hit[len(disk) :] for hit in hits]

    return glob


async def _mirrored(tmp_path: Path, store):
    """Every route over a computer at ``tmp_path``, and a copy of that
    computer on disk: the folder's own files, and each route's files where
    the folder spells them and, for a route at the computer root, there too.
    Grep's answer from the copy is what the composite has to give."""
    root, disk = str(tmp_path / "root"), str(tmp_path / "disk")
    folder = f"{root}/Home"
    sandbox = _sandbox(folder, root)
    sandbox.agrep_rich = AsyncMock(side_effect=_rg_on(disk, root))
    sandbox.aglob_paths = AsyncMock(side_effect=_glob_on(disk, root))
    files = _composite(sandbox, store, "Home", root)
    for name in FOLDER_MEMORY_TREE:
        store.put((USER, "workspaces", WORKSPACE, "memory"), name, _envelope("x"))
    for name in USER_MEMORY_TREE:
        store.put((USER, "memory"), name, _envelope("x"))
    own = [f"{folder}/{name}" for name in FOLDER_TREE]
    copies = {*own, *(disk + path[len(root) :] for path in own)}
    for route in files._routes:
        for path in await route.aglob_paths("*", route.root_prefix):
            copies |= {path, _found_in(files, folder, path)}
    for copy in copies:
        Path(copy).parent.mkdir(parents=True, exist_ok=True)
        Path(copy).write_text("x\n")
    return files, root, folder


async def _disk_found(files, folder: str, pattern: str, base: str) -> set[str]:
    """What the sandbox's own glob lists on the copy, each route's file at
    its route's root."""
    renamed = {
        _found_in(files, folder, route.root_prefix): route.root_prefix.rstrip("/")
        for route in files._routes
    }
    found = set()
    for hit in await _disk_glob(pattern, base):
        hit = posixpath.normpath(hit)
        alias = next((a for a in renamed if hit.startswith(f"{a}/")), None)
        found.add(renamed[alias] + hit[len(alias) :] if alias else hit)
    return found


def _link_the_mount(tmp_path: Path, root: str, folder: str, monkeypatch) -> None:
    """Lay the copy's computer folders out as the file mount does, at the
    root and in the folder: links into the mount, which the sandbox glob
    enters only by a pattern's literal prefix. The folder's own memory stays
    a folder, since Glob lists it at any depth."""
    mount = tmp_path / "mnt" / "livefs"
    mount.mkdir(parents=True)
    monkeypatch.setattr(sandbox_files, "MOUNT", str(mount))
    for linked in (".agents/user", ".agents/workflows"):
        served = mount / posixpath.basename(linked)
        shutil.move(f"{root}/{linked}", served)
        shutil.rmtree(f"{folder}/{linked}")
        for at in (root, folder):
            os.symlink(served, f"{at}/{linked}")


GLOB_PATHS = (
    ".",
    ".agents",
    "{root}",
    "{root}/.agents",
    ".agents/user",
    ".agents/memory",
    ".agents/memory/sub",
    ".agents/memory/.old",
    ".agents/memory/b.md",
    ".agents/memory/missing",
    ".agents/user/memory",
    ".agents/user/profile",
    ".agents/user/automations",
)
GLOB_PATTERNS = (
    "*",
    "**",
    "**/*",
    "*.md",
    "**/*.md",
    "*.json",
    "*/*.json",
    "*/c.json",
    "**/c.json",
    "sub/*",
    "sub/**",
    ".*",
    "**/.*",
    "*.{md,json}",
    "[!a]*.json",
    "[^a]*",
    "?.json",
    "a",
    "a/",
    "memory/*",
    "m*",
    "**/README.md",
    "*[!s]s.json",
    "a[!s]**",
    "a?**",
    "**/vendor/*",
    "vendor/*",
    "**/threads/*",
    "./*.md",
    "sub//*",
    "*.md/",
    "**/sub/**/*.json",
    "{folder}/.agents/user/memory/*",
    "{folder}/.agents/user/memory/**/*.json",
    "{root}/.agents/user/*/*.md",
    "{folder}/.agents/memory/*.md",
    "{root}/.agents/user/memory/vendor/*",
)


class TestGlobInARouteMatchesTheDisk:
    """Glob finds what the sandbox's own glob finds in a copy of the route's
    files on disk, linked in as the file mount links them, a file it finds by
    both of its names listed once, at the route's root."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pattern", GLOB_PATTERNS)
    async def test_the_composite_finds_what_the_disk_glob_finds(
        self, tmp_path, store, automations, profile_rows, monkeypatch, pattern
    ):
        files, root, folder = await _mirrored(tmp_path, store)
        _link_the_mount(tmp_path, root, folder, monkeypatch)
        pattern = pattern.replace("{root}", root).replace("{folder}", folder)
        differences = {}
        for path in GLOB_PATHS:
            base = files.normalize_path(path.replace("{root}", root))
            found = await files.aglob_paths(pattern, base)
            expected = await _disk_found(files, folder, pattern, base)
            if len(found) != len(set(found)):
                differences[path] = found
            # The sandbox's own hits stand as it spells them.
            found = {posixpath.normpath(hit) for hit in found}
            if found != expected:
                differences[path] = (sorted(found - expected), sorted(expected - found))

        assert differences == {}


CLIMBING_PATTERNS = (
    "missing/../.agents/user/profile/*.json",
    "missing/../.agents/user/profile/portfolio.json",
    "report.md/../.agents/memory/*.md",
    "data/../.agents/user/profile/*.json",
    "sub/../*.md",
    "*/../.agents/memory/*.md",
    ".agents/user/../memory/*.md",
    ".agents/user/../workflows/*",
    ".agents/user/profile/../memory/*.md",
    "../Home/.agents/user/profile/*.json",
)


class TestAGlobThatClimbsIsTheDisks:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("pattern", CLIMBING_PATTERNS)
    async def test_with_the_mount_up(
        self, tmp_path, store, automations, profile_rows, monkeypatch, pattern
    ):
        """A `..` steps back out of whatever the name before it is on disk,
        a folder, a link into the mount, a file or nothing, so Glob lists
        what the sandbox's own glob lists there."""
        files, root, folder = await _mirrored(tmp_path, store)
        _link_the_mount(tmp_path, root, folder, monkeypatch)
        files._sandbox.aglob_paths = AsyncMock(side_effect=_disk_glob)
        differences = {}
        for path in (".", ".agents", ".agents/user", ".agents/user/profile"):
            base = files.normalize_path(path)
            found = {
                posixpath.normpath(hit)
                for hit in await files.aglob_paths(pattern, base)
            }
            expected = await _disk_found(files, folder, pattern, base)
            if found != expected:
                differences[path] = (sorted(found - expected), sorted(expected - found))

        assert differences == {}


class TestGlobDropsWhatTheSandboxGlobDrops:
    @pytest.mark.asyncio
    async def test_with_the_lists_the_sandbox_glob_is_sent(self):
        """A route's files are held to the folders the sandbox's glob drops
        and its history rule, as the sandbox is sent them."""
        sent: dict = {}

        async def call(fn, command, **_):
            sent.update(json.loads(shlex.split(command)[-1]))
            return SimpleNamespace(stdout="", stderr="", exit_code=0)

        await _agents_glob(call)("*", FOLDER)

        assert sent["excluded_dirs"] == GLOB_EXCLUDED_DIRS
        assert sent["history_children"] == GLOB_HISTORY_CHILDREN


RG_PATHS = (
    ".",
    ".agents",
    ".agents/memory",
    ".agents/memory/sub",
    ".agents/memory/.old",
    ".agents/memory/b.md",
    ".agents/user",
    ".agents/user/memory",
    ".agents/user/profile",
    ".agents/user/profile/portfolio.json",
)
# No filter, every rule rg's filter goes by, and the readings of rg it was
# checked against. Nested braces and a `}` outside braces are left out:
# ripgrep 15 reads them otherwise than the sandbox's 14 does
# (`TestGrepFilterTokensAsRg14`).
RG_FILTERS = (
    None,
    "!*.json",
    "!sub",
    "!sub/",
    "!/sub",
    "!**/memory/**",
    "!memory",
    "!memory/*",
    "!a/",
    "!a",
    "!.agents",
    "*.{md,json}",
    "{c.json,user/e.md}",
    "{sub,user}/*.md",
    "!{*.json,sub}",
    "{*.md,!*.json}",
    "[^a]*.json",
    "[!a]*.json",
    "\\{a,b\\}*",
    "sub",
    "!",
    "!!x",
    "{,a}*.json",
    "b{}.md",
    "!{}",
    "*.md ",
    " ",
    "#*.md",
    "!*/",
    "!**/",
    "*.md",
    "!*.md",
    "*",
    "**",
    ".*",
    "!.*",
    ".agents/memory/*",
    "/.agents/memory/*.md",
    "!.agents/memory/*",
    "!/.agents/memory",
    ".agents/**",
    ".agents/user/**",
    "**/profile/*",
    "!**/profile/w*",
    "!portfolio.json",
    "!profile",
    "!user/",
    "{a,b}{.md,.json}",
    "*.[^m]*",
    "!*.[^m]*",
    "[!]a]*",
    "[]a]*",
    "/{sub,user}/*.md",
    "{/sub,user}/*.md",
    "!{sub,a}/",
    "**/sub/**",
    "!**/.old/**",
    "a\\,b",
    "*.m?",
    "{*,c}*",
    ".agents/memory/{*,c}*",
    "**/sub/{*,x}*",
    ".agents/memory/sub/{**,x}/c.json",
    "[a-c-e]*",
    "[a-c-b]*",
    "**\\/c.json",
    ".agents/memory/**\\/c.json",
    "sub[/]c.json",
    ".agents/memory/sub[/]c.json",
    "*[!s]s.json",
    "{" + ",".join(f"q{n}" for n in range(300)) + ",a}.json",
    "{a,b,s}{.,u}{j,b,x}{s,x,y}{o,x,y}{n,x,y}",
    # rg refuses these and finds nothing anywhere.
    "{a,b",
    "[z-a]*",
    "[a",
    "x\\",
)


# Each type with a filter, as rg weighs the two. rg knows no `nope`.
RG_TYPES = (
    ("md", None),
    ("json", None),
    ("all", None),
    ("json", "*.md"),
    ("json", "!*.md"),
    ("md", "!sub"),
    ("md", ".*"),
    ("nope", None),
)


@pytest.mark.skipif(shutil.which("rg") is None, reason="needs ripgrep")
class TestGrepFilterMatchesRgOnDisk:
    """A route's files meet Grep's ``glob`` as rg meets a copy of them on
    disk: for each filter and path, the composite finds what rg finds, run
    where the sandbox runs it at the path as the agent spells it. The one
    difference is the folder's own memory, which a Grep from the folder
    searches as though ``path`` named it too."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("file_type", "glob"), [(None, glob) for glob in RG_FILTERS] + list(RG_TYPES)
    )
    async def test_the_composite_finds_what_rg_finds(
        self, tmp_path, store, automations, profile_rows, file_type, glob
    ):
        files, root, folder = await _mirrored(tmp_path, store)
        differences = {}
        for path in (*RG_PATHS, f"{root}/.agents"):
            base = files.normalize_path(path)
            where = _found_in(files, folder, base)
            searched = (
                [where, f"{folder}/.agents/memory"] if base == folder else [where]
            )
            hits = await files.agrep_rich(".", path=base, glob=glob, type=file_type)
            # A hit is spelled from the folder when the search was.
            if where == folder or where.startswith(f"{folder}/"):
                hits = [_found_in(files, folder, hit) for hit in hits]
            found = set(hits)
            cwd = grep_working_dir(where, folder=folder, root=root)
            expected = set(_rg(cwd, glob, searched, file_type))
            if found != expected:
                differences[path] = (sorted(found - expected), sorted(expected - found))

        assert differences == {}
