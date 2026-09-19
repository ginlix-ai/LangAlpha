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
        report={},
        ws_rows={},
        user_rows={},
        base=base,
    )


@pytest.fixture
def linker(monkeypatch):
    """Capture the link call the pass makes, with a settled empty result."""
    calls: list[dict] = []

    async def _link(sandbox, *, base, user_base, disabled=()):
        calls.append(
            {"base": base, "user_base": user_base, "disabled": list(disabled)}
        )
        return {"linked": [], "relinked": [], "pruned": [], "blocked": []}

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
    async def test_a_workspace_at_the_computer_root_links_nothing(
        self, sandbox, linker, disables
    ):
        disables([])

        await R._link_shared(_ctx(sandbox, f"{ROOT}/.agents/skills"))

        assert linker == []

    @pytest.mark.asyncio
    async def test_link_counts_reach_the_stats(self, sandbox, monkeypatch, disables):
        disables([])

        async def _link(sandbox, *, base, user_base, disabled=()):
            return {
                "linked": ["pdf"],
                "relinked": ["xlsx"],
                "pruned": ["docx", "pptx"],
                "blocked": [],
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
