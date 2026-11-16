"""Which folder a reconcile pass materialises, and how it reaches the rest.

One computer serves several workspaces, so the pass has to be told which one
it is running for. It writes only that workspace's own skill directory; the
shared directory at the computer root serves every sibling, which is why a
workspace's disables turn into an absent link there rather than a deletion
here.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.project_context import ProjectContext
from src.server.services.user_skills import reconcile as R
from src.server.services.workspace_layout import WorkspaceLayoutUnavailable

ROOT = "/home/workspace"
WS = "ws-1"


@pytest.fixture
def sandbox():
    return SimpleNamespace(
        working_dir=ROOT,
        _work_dir=ROOT,
        workspace=lambda project=None: SandboxLayout(ROOT).for_workspace(
            project.dir_name if project is not None else None
        ),
    )


def _project(workspace_id: str, dir_name: str | None) -> ProjectContext:
    return ProjectContext(
        workspace_id=workspace_id,
        dir_name=dir_name,
        sibling_dir_names=(),
    )


@pytest.fixture
def no_project(monkeypatch):
    monkeypatch.setattr(R, "current_project", lambda: None)


@pytest.fixture
def binding(monkeypatch):
    """Stand in for the binding read `_resolve_layout` falls back to.

    A row names a computer and the folder it owns there. Pass ``computer_id``
    None for a project bound to no computer, which is the one shape that owns
    the whole root.
    """

    def _set(row, *, error: Exception | None = None):
        import src.server.database.workspace_binding as wsb

        monkeypatch.setattr(
            wsb,
            "get_project_binding",
            AsyncMock(return_value=row, side_effect=error),
        )

    return _set


class TestResolveLayout:
    @pytest.mark.asyncio
    async def test_the_callers_project_wins(self, sandbox, monkeypatch):
        monkeypatch.setattr(R, "current_project", lambda: _project(WS, "other-zz99"))
        wanted = _project(WS, "acme-ab12")

        got = await R._resolve_layout(sandbox, WS, wanted)

        assert got.dir_name == "acme-ab12"

    @pytest.mark.asyncio
    async def test_the_bound_project_is_used_when_it_is_this_workspace(
        self, sandbox, monkeypatch
    ):
        monkeypatch.setattr(R, "current_project", lambda: _project(WS, "acme-ab12"))

        got = await R._resolve_layout(sandbox, WS, None)

        assert got.skills == f"{ROOT}/acme-ab12/.agents/skills"

    @pytest.mark.asyncio
    async def test_another_workspaces_project_is_ignored(
        self, sandbox, monkeypatch, binding
    ):
        monkeypatch.setattr(R, "current_project", lambda: _project("ws-other", "zz-99"))
        binding({"computer_id": "c-1", "dir_name": "acme-ab12"})

        got = await R._resolve_layout(sandbox, WS, None)

        assert got.skills == f"{ROOT}/acme-ab12/.agents/skills"

    @pytest.mark.asyncio
    async def test_outside_a_turn_the_binding_answers(
        self, sandbox, no_project, binding
    ):
        binding({"computer_id": "c-1", "dir_name": "acme-ab12"})

        got = await R._resolve_layout(sandbox, WS, None)

        assert got.skills == f"{ROOT}/acme-ab12/.agents/skills"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "row",
        [
            # The shape the old code read as "this project owns the machine".
            {"computer_id": "c-1", "dir_name": None},
            # And the one it read as "nothing shares this root", although the
            # sandbox such a row still names may already carry its siblings'
            # folders: that is how migration 046 leaves a shared sandbox.
            {"computer_id": None, "dir_name": None},
        ],
    )
    async def test_a_row_without_a_folder_refuses_to_guess(
        self, sandbox, no_project, binding, row
    ):
        binding(row)

        with pytest.raises(WorkspaceLayoutUnavailable):
            await R._resolve_layout(sandbox, WS, None)

    @pytest.mark.asyncio
    async def test_a_failed_read_refuses_to_guess(
        self, sandbox, no_project, binding
    ):
        binding(None, error=RuntimeError("connection reset"))

        with pytest.raises(WorkspaceLayoutUnavailable):
            await R._resolve_layout(sandbox, WS, None)

    @pytest.mark.asyncio
    async def test_a_missing_row_refuses_to_guess(
        self, sandbox, no_project, binding
    ):
        binding(None)

        with pytest.raises(WorkspaceLayoutUnavailable):
            await R._resolve_layout(sandbox, WS, None)


class TestPassSkippedWithoutAFolder:
    @pytest.mark.asyncio
    async def test_an_unresolvable_folder_runs_no_pass(
        self, sandbox, no_project, binding, monkeypatch
    ):
        """It prunes the skill dirs it does not recognise, and from the computer
        root every sibling's looks unrecognised."""
        binding(None, error=RuntimeError("connection reset"))
        ran = AsyncMock()
        monkeypatch.setattr(R, "_run_pass", ran)
        sandbox.runtime = SimpleNamespace()

        stats = await R.reconcile_workspace_skills(
            sandbox, user_id="u-1", workspace_id=WS, source="test"
        )

        assert stats is None
        ran.assert_not_awaited()


def _ctx(sandbox, base: str) -> R._Pass:
    return R._Pass(
        sandbox=sandbox,
        user_id="user-1",
        workspace_id=WS,
        scope_id=WS,
        report={},
        ws_rows={},
        user_rows={},
        base=base,
    )


@pytest.fixture
def linker(monkeypatch):
    """Capture the link call the pass makes, with a settled empty result."""
    calls: list[dict] = []

    async def _link(sandbox, *, base, user_base, disabled=(), own=()):
        calls.append(
            {
                "base": base,
                "user_base": user_base,
                "disabled": list(disabled),
                "own": sorted(own),
            }
        )
        return {
            "linked": [],
            "relinked": [],
            "pruned": [],
            "blocked": [],
            "unpinned": [],
        }

    monkeypatch.setattr(R.skill_sync, "link_shared_skills", _link)
    return calls


@pytest.fixture
def disables(monkeypatch):
    def _set(names):
        monkeypatch.setattr(
            R, "list_workspace_skill_disables", AsyncMock(return_value=names)
        )

    return _set


class TestLinkShared:
    @pytest.mark.asyncio
    async def test_it_links_the_workspace_tier_at_the_computers_shared_one(
        self, sandbox, linker, disables
    ):
        disables([])

        await R._link_shared(_ctx(sandbox, f"{ROOT}/acme-ab12/.agents/skills"))

        assert linker == [
            {
                "base": f"{ROOT}/acme-ab12/.agents/skills",
                "user_base": f"{ROOT}/.agents/skills",
                "disabled": [],
                "own": [],
            }
        ]

    @pytest.mark.asyncio
    async def test_the_workspaces_disables_ride_along(
        self, sandbox, linker, disables
    ):
        disables(["xlsx"])

        await R._link_shared(_ctx(sandbox, f"{ROOT}/acme-ab12/.agents/skills"))

        assert linker[0]["disabled"] == ["xlsx"]

    @pytest.mark.asyncio
    async def test_the_workspaces_own_rows_are_never_handed_back(
        self, sandbox, linker, disables
    ):
        """A workspace skill with a pre-sync managed entry reads as shared by
        its entry alone; its bytes are the skill sync's to decide."""
        disables([])
        ctx = _ctx(sandbox, f"{ROOT}/acme-ab12/.agents/skills")
        ctx.ws_rows["house-style"] = {"name": "house-style"}

        await R._link_shared(ctx)

        assert linker[0]["own"] == ["house-style"]

    @pytest.mark.asyncio
    async def test_a_workspace_at_the_computer_root_links_nothing(
        self, sandbox, linker, disables
    ):
        disables([])

        await R._link_shared(_ctx(sandbox, f"{ROOT}/.agents/skills"))

        assert linker == []

    @pytest.mark.asyncio
    async def test_link_counts_reach_the_stats(self, sandbox, monkeypatch, disables):
        disables([])

        async def _link(sandbox, *, base, user_base, disabled=(), own=()):
            return {
                "linked": ["pdf"],
                "relinked": ["xlsx"],
                "pruned": ["docx", "pptx"],
                "blocked": [],
                "unpinned": [],
            }

        monkeypatch.setattr(R.skill_sync, "link_shared_skills", _link)
        ctx = _ctx(sandbox, f"{ROOT}/acme-ab12/.agents/skills")

        await R._link_shared(ctx)

        assert (ctx.stats.linked, ctx.stats.unlinked) == (2, 2)
        assert ctx.stats.changed

    @pytest.mark.asyncio
    async def test_a_link_failure_is_counted_not_raised(
        self, sandbox, monkeypatch, disables
    ):
        disables([])
        monkeypatch.setattr(
            R.skill_sync,
            "link_shared_skills",
            AsyncMock(side_effect=RuntimeError("exec died")),
        )
        ctx = _ctx(sandbox, f"{ROOT}/acme-ab12/.agents/skills")

        await R._link_shared(ctx)

        assert ctx.stats.failures == 1
        assert ctx.stats.linked == 0


class TestHomeStandIn:
    """While Home's machine is away its turns run in a hidden stand-in folder.
    The rows and the row lock follow Home, so a skill made there is not lost;
    the folder's own state (restore flag, lock) stays on the stand-in's id."""

    USER = "user-1"

    @pytest.fixture
    def ids(self):
        from src.server.database.home_workspace import (
            get_home_stand_in_id,
            get_home_workspace_id,
        )

        return get_home_stand_in_id(self.USER), get_home_workspace_id(self.USER)

    @pytest.fixture
    def locks(self, monkeypatch):
        taken: list[str] = []

        class _Lock:
            def __init__(self, ws):
                self.ws = ws

            async def __aenter__(self):
                taken.append(self.ws)

            async def __aexit__(self, *exc):
                return False

        monkeypatch.setattr(R, "workspace_skill_sync_lock", _Lock)
        return taken

    @pytest.mark.asyncio
    async def test_both_ids_are_locked_in_sorted_order(
        self, sandbox, no_project, locks, ids, monkeypatch
    ):
        stand_in, home = ids
        monkeypatch.setattr(R, "_resolve_layout", AsyncMock(return_value=object()))
        monkeypatch.setattr(R, "_run_pass", AsyncMock(return_value="stats"))
        sandbox.runtime = SimpleNamespace()

        got = await R.reconcile_workspace_skills(
            sandbox, user_id=self.USER, workspace_id=stand_in, source="test"
        )

        assert got == "stats"
        assert locks == sorted({stand_in, home})
        assert len(locks) == 2

    @pytest.mark.asyncio
    async def test_a_normal_workspace_takes_one_lock(
        self, sandbox, no_project, locks, monkeypatch
    ):
        monkeypatch.setattr(R, "_resolve_layout", AsyncMock(return_value=object()))
        monkeypatch.setattr(R, "_run_pass", AsyncMock(return_value="stats"))
        sandbox.runtime = SimpleNamespace()

        await R.reconcile_workspace_skills(
            sandbox, user_id=self.USER, workspace_id=WS, source="test"
        )

        assert locks == [WS]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stand_in", [True, False])
    async def test_rows_follow_home_and_the_folder_stays_put(
        self, monkeypatch, ids, stand_in
    ):
        folder = ids[0] if stand_in else WS
        scope = ids[1] if stand_in else WS
        listed: list = []
        created: list = []
        restore: list = []
        linked: list = []

        async def _rows(user_id, *, workspace_id=None):
            listed.append(workspace_id)
            return []

        async def _create(user_id, name, **kw):
            created.append(kw["workspace_id"])
            return {"user_skill_id": "sk-new", "name": name}

        async def _flag(workspace_id):
            restore.append(workspace_id)
            return False

        async def _link(ctx):
            linked.append((ctx.scope_id, ctx.workspace_id, sorted(ctx.ws_rows)))

        async def _download(ctx, name):
            return (
                SimpleNamespace(
                    command=None,
                    description="d",
                    license=None,
                    frontmatter={},
                    allowed_tools=[],
                    content_hash="h",
                    canonical_zip=b"z",
                    file_count=1,
                    skill_md="",
                ),
                "tree",
            )

        report = {
            "fresh": {
                "present": True,
                "syncable": True,
                "wellFormed": True,
                "treeHash": "t",
                "entry": None,
            }
        }
        monkeypatch.setattr(
            R.skill_sync, "report", AsyncMock(return_value=report)
        )
        monkeypatch.setattr(R.skill_sync, "apply_actions", AsyncMock(return_value=[]))
        monkeypatch.setattr(R, "list_user_skills", _rows)
        monkeypatch.setattr(R, "create_user_skill", _create)
        monkeypatch.setattr(R, "files_restore_incomplete", _flag)
        monkeypatch.setattr(R, "_link_shared", _link)
        monkeypatch.setattr(R, "_download_validated", _download)
        monkeypatch.setattr(R, "_store", AsyncMock(return_value=(None, b"z")))

        await R._run_pass(
            object(),
            self.USER,
            folder,
            "test",
            SimpleNamespace(skills=f"{ROOT}/acme-ab12/.agents/skills"),
        )

        assert listed == [scope, None]
        assert created == [scope]
        assert restore == [folder]
        assert linked == [(scope, folder, ["fresh"])]

    @pytest.mark.asyncio
    async def test_disables_are_read_for_the_scope(
        self, sandbox, linker, ids, monkeypatch
    ):
        stand_in, home = ids
        read = AsyncMock(return_value=[])
        monkeypatch.setattr(R, "list_workspace_skill_disables", read)
        ctx = _ctx(sandbox, f"{ROOT}/acme-ab12/.agents/skills")
        ctx.workspace_id, ctx.scope_id = stand_in, home

        await R._link_shared(ctx)

        read.assert_awaited_once_with(home)
