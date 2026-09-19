"""
Tests for ptc_agent.core.sandbox.migration — versioned sandbox layout migrations.

Covers:
- Zero cost when already current (no API calls)
- v1→v2 migration moves directories, creates skills dir, removes old skills/
- Idempotency (safe to re-run)
- Skipping missing source directories
- Sequential migration execution via run_layout_migrations
- v3→v4 folder split: the script's guards, the names it leaves alone, and
  what it does against a real directory tree
"""

import shlex
import subprocess
from pathlib import Path

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from ptc_agent.core.sandbox.migration import (
    CURRENT_LAYOUT_VERSION,
    build_v3_to_v4_script,
    migrate_layout_v1_to_v2,
    migrate_layout_v2_to_v3,
    migrate_layout_v3_to_v4,
    run_layout_migrations,
)


@pytest.fixture
def mock_runtime():
    runtime = AsyncMock()
    runtime.exec = AsyncMock(return_value="")
    return runtime


WORK_DIR = "/home/user/project"
DIR_NAME = "acme-ab12"


# ---------------------------------------------------------------------------
# Zero cost when current
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zero_cost_when_current(mock_runtime):
    """current_version >= CURRENT_LAYOUT_VERSION -> returns immediately, no API calls."""
    result = await run_layout_migrations(
        mock_runtime, WORK_DIR, current_version=CURRENT_LAYOUT_VERSION, dir_name=DIR_NAME
    )
    assert result == CURRENT_LAYOUT_VERSION
    mock_runtime.exec.assert_not_called()


@pytest.mark.asyncio
async def test_zero_cost_when_ahead(mock_runtime):
    """current_version > CURRENT_LAYOUT_VERSION -> also returns immediately."""
    result = await run_layout_migrations(
        mock_runtime, WORK_DIR, current_version=CURRENT_LAYOUT_VERSION + 1, dir_name=DIR_NAME
    )
    assert result == CURRENT_LAYOUT_VERSION + 1
    mock_runtime.exec.assert_not_called()


# ---------------------------------------------------------------------------
# v1 → v2 migration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migration_v1_to_v2_moves_dirs(mock_runtime):
    """Verify the migration runs shell commands to move .agent/ subdirs to .agents/."""
    await migrate_layout_v1_to_v2(mock_runtime, WORK_DIR, dir_name=DIR_NAME)

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]

    # Should create .agents/skills/ first
    assert any("mkdir -p" in c and ".agents/skills" in c for c in calls), (
        f"Expected mkdir for .agents/skills/ in calls: {calls}"
    )

    # Should move .agent/threads -> .agents/threads
    assert any(
        ".agent/threads" in c and ".agents/threads" in c and "cp -a" in c
        for c in calls
    ), f"Expected move of .agent/threads in calls: {calls}"

    # Should move .agent/user -> .agents/user
    assert any(
        ".agent/user" in c and ".agents/user" in c and "cp -a" in c
        for c in calls
    ), f"Expected move of .agent/user in calls: {calls}"

    # Should move .agent/large_tool_results -> .agents/large_tool_results
    assert any(
        ".agent/large_tool_results" in c
        and ".agents/large_tool_results" in c
        and "cp -a" in c
        for c in calls
    ), f"Expected move of .agent/large_tool_results in calls: {calls}"

    # Should clean up old .agent/ directory
    assert any("rmdir" in c and ".agent" in c for c in calls), (
        f"Expected rmdir of .agent/ in calls: {calls}"
    )

    # Should remove old skills/ directory
    assert any("rm -rf" in c and "/skills" in c for c in calls), (
        f"Expected rm -rf of skills/ in calls: {calls}"
    )


@pytest.mark.asyncio
async def test_migration_v1_to_v2_idempotent(mock_runtime):
    """Running migration twice should not fail — shell commands are idempotent."""
    await migrate_layout_v1_to_v2(mock_runtime, WORK_DIR, dir_name=DIR_NAME)
    first_call_count = mock_runtime.exec.call_count

    # Run again — same commands should be issued without error
    await migrate_layout_v1_to_v2(mock_runtime, WORK_DIR, dir_name=DIR_NAME)
    second_call_count = mock_runtime.exec.call_count - first_call_count

    # Same number of exec calls both times (deterministic, not skipping)
    assert first_call_count == second_call_count


@pytest.mark.asyncio
async def test_migration_v1_to_v2_creates_skills_dir(mock_runtime):
    """.agents/skills/ is created via mkdir -p."""
    await migrate_layout_v1_to_v2(mock_runtime, WORK_DIR, dir_name=DIR_NAME)

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]

    expected_path = shlex.quote(f"{WORK_DIR}/.agents/skills")
    assert any(f"mkdir -p {expected_path}" in c for c in calls), (
        f"Expected mkdir -p for .agents/skills/ in calls: {calls}"
    )


@pytest.mark.asyncio
async def test_migration_v1_to_v2_skips_missing_source(mock_runtime):
    """If .agent/ doesn't exist, the `if [ -d ... ]` check skips the move."""
    await migrate_layout_v1_to_v2(mock_runtime, WORK_DIR, dir_name=DIR_NAME)

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]

    # Each move command is guarded by `if [ -d <src> ]`
    move_calls = [c for c in calls if "cp -a" in c]
    for cmd in move_calls:
        assert "if [ -d " in cmd, (
            f"Move command not guarded by existence check: {cmd}"
        )


@pytest.mark.asyncio
async def test_migration_v1_to_v2_uses_quoted_paths(mock_runtime):
    """Paths are passed through shlex.quote() for safety."""
    work_dir_with_space = "/home/user/my project"
    await migrate_layout_v1_to_v2(mock_runtime, work_dir_with_space, dir_name=DIR_NAME)

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]

    # shlex.quote wraps paths containing spaces in single quotes
    quoted_skills = shlex.quote(f"{work_dir_with_space}/.agents/skills")
    assert any(quoted_skills in c for c in calls), (
        f"Expected quoted path {quoted_skills} in calls: {calls}"
    )


# ---------------------------------------------------------------------------
# run_layout_migrations orchestration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_layout_migrations_sequential(mock_runtime):
    """With current_version=1, runs both v1->v2 and v2->v3 in order."""
    result = await run_layout_migrations(
        mock_runtime, WORK_DIR, current_version=1, dir_name=DIR_NAME
    )

    assert result == CURRENT_LAYOUT_VERSION
    assert mock_runtime.exec.call_count > 0

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]

    # Verify it ran the v1->v2 migration (check for characteristic commands)
    assert any(".agents/skills" in c for c in calls)
    assert any(".agent/threads" in c for c in calls)
    # Verify it also ran the v2->v3 migration (legacy .md removal)
    assert any(".agents/user/portfolio.md" in c for c in calls)


@pytest.mark.asyncio
async def test_run_layout_migrations_returns_current_version(mock_runtime):
    """After running all migrations, returns CURRENT_LAYOUT_VERSION."""
    result = await run_layout_migrations(
        mock_runtime, WORK_DIR, current_version=1, dir_name=DIR_NAME
    )
    assert result == CURRENT_LAYOUT_VERSION
    assert result >= 3


@pytest.mark.asyncio
async def test_run_layout_migrations_skips_unknown_versions(mock_runtime):
    """If a version has no registered migrator, it's silently skipped."""
    # Version 0 has no registered migration, so it should skip 0->1
    # but still run 1->2 and 2->3
    result = await run_layout_migrations(
        mock_runtime, WORK_DIR, current_version=0, dir_name=DIR_NAME
    )
    assert result == CURRENT_LAYOUT_VERSION

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]
    assert any(".agents/skills" in c for c in calls)
    assert any(".agents/user/portfolio.md" in c for c in calls)


# ---------------------------------------------------------------------------
# v2 → v3 migration (legacy user-data .md cleanup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migration_v2_to_v3_removes_legacy_md(mock_runtime):
    """v2→v3 deletes the three legacy user-data markdown files."""
    await migrate_layout_v2_to_v3(mock_runtime, WORK_DIR, dir_name=DIR_NAME)

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]
    # Single `rm -f` containing all three files (no per-file round-trip)
    assert len(calls) == 1
    rm_cmd = calls[0]
    assert rm_cmd.startswith("rm -f ")
    assert ".agents/user/portfolio.md" in rm_cmd
    assert ".agents/user/watchlist.md" in rm_cmd
    assert ".agents/user/preference.md" in rm_cmd


@pytest.mark.asyncio
async def test_migration_v2_to_v3_idempotent(mock_runtime):
    """rm -f is idempotent; running twice issues identical commands."""
    await migrate_layout_v2_to_v3(mock_runtime, WORK_DIR, dir_name=DIR_NAME)
    first = mock_runtime.exec.call_args_list[-1].args[0]
    await migrate_layout_v2_to_v3(mock_runtime, WORK_DIR, dir_name=DIR_NAME)
    second = mock_runtime.exec.call_args_list[-1].args[0]
    assert first == second


@pytest.mark.asyncio
async def test_migration_v2_to_v3_uses_quoted_paths(mock_runtime):
    """Paths flow through shlex.quote so spaces don't break the shell command."""
    work_dir_with_space = "/home/user/my project"
    await migrate_layout_v2_to_v3(mock_runtime, work_dir_with_space, dir_name=DIR_NAME)
    rm_cmd = mock_runtime.exec.call_args_list[-1].args[0]
    quoted = shlex.quote(f"{work_dir_with_space}/.agents/user/portfolio.md")
    assert quoted in rm_cmd


@pytest.mark.asyncio
async def test_run_layout_migrations_from_v2_runs_only_v2_to_v3(mock_runtime):
    """A sandbox already at v2 only runs the v2->v3 migration, not v1->v2."""
    result = await run_layout_migrations(
        mock_runtime, WORK_DIR, current_version=2, dir_name=DIR_NAME
    )
    assert result == CURRENT_LAYOUT_VERSION

    calls = [call.args[0] for call in mock_runtime.exec.call_args_list]
    # No v1->v2 commands
    assert not any(".agent/threads" in c for c in calls)
    # Only the v2->v3 rm -f
    assert any(".agents/user/portfolio.md" in c for c in calls)


# ---------------------------------------------------------------------------
# v3 → v4 migration (the workspace folder, and the runtime's own root)
# ---------------------------------------------------------------------------


class TestV3ToV4Script:
    """The script text: one shell run, every clause guarded, nothing else moved."""

    @pytest.mark.asyncio
    async def test_it_is_one_exec(self, mock_runtime):
        await migrate_layout_v3_to_v4(mock_runtime, WORK_DIR, dir_name=DIR_NAME)
        assert mock_runtime.exec.await_count == 1

    def test_every_move_is_guarded(self):
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        # A re-run after a partial failure must not nest a folder inside
        # itself, so nothing moves without first testing both ends.
        for line in script.splitlines():
            if line.startswith("merge() "):
                continue
            if " mv " in line or "merge " in line:
                assert "[ -e " in line or "[ -d " in line, line
        assert '[ ! -e "$dst" ]; then mv' in script

    def test_a_failed_move_stops_the_script(self):
        # The step that moves the user's own data used to report success when
        # a copy failed: set -e does not abort on a failing AND-OR list, so
        # ``cp -a src && rm -rf src`` carried on and the version was stamped.
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        assert script.splitlines()[0] == "set -e"
        assert 'merge() { mkdir -p "$2"; cp -a "$1/." "$2/"; rm -rf "$1"; }' in script
        assert "&& rm -rf" not in script
        assert "&& continue" not in script
        # The conflict arm says so rather than skipping in silence.
        assert 'echo "layout-v4: kept $dst, left $src at the root" >&2' in script

    def test_the_legacy_memory_mirror_is_dropped(self):
        # Nothing mounts <root>/.agents/workspace/memory any more, and leaving
        # it gives the tier a second spelling for a Glob to surface.
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        assert f"rm -rf {WORK_DIR}/.agents/workspace" in script

    def test_the_computer_keeps_its_own_directories(self):
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        # Named reservations, plus the glob itself: "$root"/* never matches a
        # dot-entry, which is what leaves .agents, .system and the sandbox
        # user's own home files where they are.
        assert 'case "$name" in _internal|mcp_servers|tools) continue ;; esac' in script
        assert 'for src in "$root"/*; do' in script
        assert ".[!.]*" not in script and "dotglob" not in script

    def test_the_wrappers_move_under_internal(self):
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        assert f"if [ -d {WORK_DIR}/tools ]" in script
        assert f"merge {WORK_DIR}/tools {WORK_DIR}/_internal/tools" in script

    def test_the_docs_split_off_before_the_wrappers_move(self):
        # The legacy directory has two destinations now, and the wrapper clause
        # takes whatever is left, so the docs clause has to run first.
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        docs = f"merge {WORK_DIR}/tools/docs {WORK_DIR}/.agents/tools/docs"
        wrappers = f"merge {WORK_DIR}/tools {WORK_DIR}/_internal/tools"
        assert docs in script
        assert script.index(docs) < script.index(wrappers)

    def test_thread_state_follows_the_workspace(self):
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        ws = f"{WORK_DIR}/{DIR_NAME}"
        for name in (".agents/threads", ".agents/large_tool_results"):
            assert f"merge {WORK_DIR}/{name} {ws}/{name}" in script

    def test_the_workspace_tier_is_created(self):
        script = build_v3_to_v4_script(WORK_DIR, DIR_NAME)
        ws = f"{WORK_DIR}/{DIR_NAME}"
        assert script.splitlines()[-1] == (
            f"mkdir -p {ws}/.agents/skills {ws}/.agents/memory "
            f"{ws}/.agents/tools {ws}/.agents/threads"
        )

    def test_without_a_folder_nothing_moves(self):
        # The workspace IS the root: only the runtime relocation runs, so a
        # computer that has not been split keeps every path it published.
        script = build_v3_to_v4_script(WORK_DIR, None)
        assert "for src in" not in script
        assert f"ws={WORK_DIR}\n" in script
        assert build_v3_to_v4_script(WORK_DIR, "") == script

    def test_paths_with_spaces_stay_one_word(self):
        script = build_v3_to_v4_script("/home/my box", DIR_NAME)
        assert "root='/home/my box'" in script
        assert "ws='/home/my box/acme-ab12'" in script


class TestV3ToV4AgainstRealFiles:
    """Run the script against a directory tree, twice."""

    @staticmethod
    def _seed(root: Path) -> None:
        for d in (
            "work/task1",
            "results",
            "data",
            "tools/docs/probe",
            "mcp_servers",
            ".agents/skills/platform-skill",
            ".agents/threads/abc",
            ".agents/large_tool_results",
            ".system/code",
            "_internal/src",
            "my-analysis",
        ):
            (root / d).mkdir(parents=True, exist_ok=True)
        (root / "agent.md").write_text("notes")
        (root / ".bashrc").write_text("rc")
        (root / "tools/probe.py").write_text("wrapper")
        (root / "tools/docs/probe/get_price.md").write_text("doc")
        (root / "work/task1/out.csv").write_text("1,2")
        (root / ".agents/threads/abc/offload.md").write_text("spill")
        (root / ".agents/large_tool_results/r1.json").write_text("{}")

    def _run(self, root: Path, dir_name: str | None) -> None:
        script = build_v3_to_v4_script(str(root), dir_name)
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr

    def test_the_folder_gets_the_files_and_a_rerun_changes_nothing(self, tmp_path):
        root = tmp_path / "home"
        root.mkdir()
        self._seed(root)
        ws = root / DIR_NAME

        self._run(root, DIR_NAME)
        after_first = sorted(p.relative_to(root).as_posix() for p in root.rglob("*"))
        self._run(root, DIR_NAME)

        assert (ws / "work/task1/out.csv").read_text() == "1,2"
        assert (ws / "agent.md").read_text() == "notes"
        assert (ws / "my-analysis").is_dir()
        assert (ws / ".agents/memory").is_dir()
        assert (root / "_internal/tools/probe.py").read_text() == "wrapper"
        assert not (root / "tools").exists()
        # The docs land in the readable tier, the wrappers do not.
        assert (
            root / ".agents/tools/docs/probe/get_price.md"
        ).read_text() == "doc"
        assert not (root / "_internal/tools/docs").exists()
        # Thread state follows the one workspace a v3 machine holds.
        assert (ws / ".agents/threads/abc/offload.md").read_text() == "spill"
        assert (ws / ".agents/large_tool_results/r1.json").read_text() == "{}"
        assert not (root / ".agents/threads").exists()
        assert not (root / ".agents/large_tool_results").exists()
        # Untouched: the computer's own tiers and the sandbox user's dotfiles.
        assert (root / ".agents/skills/platform-skill").is_dir()
        assert (root / ".system/code").is_dir()
        assert (root / "mcp_servers").is_dir()
        assert (root / ".bashrc").read_text() == "rc"
        assert not (ws / DIR_NAME).exists()
        assert (
            sorted(p.relative_to(root).as_posix() for p in root.rglob("*"))
            == after_first
        )

    def test_a_half_finished_move_converges(self, tmp_path):
        # The failure this guards: an interrupted run that already created the
        # folder and moved part of a directory into it.
        root = tmp_path / "home"
        root.mkdir()
        self._seed(root)
        ws = root / DIR_NAME
        (ws / "work/task2").mkdir(parents=True)
        (ws / "work/task2/done.csv").write_text("3,4")

        self._run(root, DIR_NAME)

        assert (ws / "work/task1/out.csv").read_text() == "1,2"
        assert (ws / "work/task2/done.csv").read_text() == "3,4"
        assert not (root / "work").exists()

    def test_an_unsplit_computer_only_gains_the_runtime_move(self, tmp_path):
        root = tmp_path / "home"
        root.mkdir()
        self._seed(root)

        self._run(root, None)

        assert (root / "work/task1/out.csv").read_text() == "1,2"
        assert (root / "agent.md").read_text() == "notes"
        assert (root / "_internal/tools/probe.py").read_text() == "wrapper"
        assert (root / ".agents/memory").is_dir()
        # Its workspace already owns the root, so the thread state is in place.
        assert (root / ".agents/threads/abc/offload.md").read_text() == "spill"


class TestMigrationHonesty:
    """A step that failed must leave the version behind it.

    The v3 to v4 move is the one migration that relocates the user's own
    files, and it used to discard the exit code: the manifest recorded 4, the
    retry never happened, and the files sat at a root nothing scanned.
    """

    @pytest.mark.asyncio
    async def test_a_nonzero_exit_raises(self):
        runtime = AsyncMock()
        runtime.exec = AsyncMock(
            return_value=SimpleNamespace(exit_code=1, stderr="cp: no space left")
        )
        with pytest.raises(RuntimeError, match="no space left"):
            await migrate_layout_v3_to_v4(runtime, WORK_DIR, dir_name=DIR_NAME)

    @pytest.mark.asyncio
    async def test_the_chain_returns_the_last_version_it_completed(self):
        runtime = AsyncMock()

        async def exec_(cmd, *a, **kw):
            if "merge()" in cmd:
                return SimpleNamespace(exit_code=1, stderr="boom")
            return SimpleNamespace(exit_code=0, stderr="")

        runtime.exec = AsyncMock(side_effect=exec_)
        reached = await run_layout_migrations(
            runtime, WORK_DIR, 1, dir_name=DIR_NAME
        )
        assert reached == 3
