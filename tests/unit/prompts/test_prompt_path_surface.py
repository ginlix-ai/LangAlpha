"""The paths the agent is told about, pinned against layout drift.

Every path in a prompt template or a skill doc is read by the model as an
instruction, and a wrong one fails silently: the write lands somewhere nobody
reads. Layout v4 retired five spellings, and each of them is one careless
copy-paste away from coming back, so they are banned by pattern rather than by
review. The canonical table itself is checked against ``paths.py`` so the one
surface that must not drift transcribes the classes instead of restating them.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ptc_agent.agent.prompts import get_loader, reset_loader, workspace_path_vars
from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATES_DIR = REPO_ROOT / "src" / "ptc_agent" / "agent" / "prompts" / "templates"
PLUGINS_DIR = REPO_ROOT / "plugins"

# Each entry: (name, compiled pattern, why it is banned).
BANNED: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "_internal/",
        re.compile(r"_internal/"),
        "the computer's runtime tree is denied to the agent, so naming it in a "
        "prompt can only produce a refused tool call",
    ),
    (
        "/home/workspace/",
        re.compile(r"/home/workspace/"),
        "the sandbox root is deployment configuration, and a turn's paths are "
        "relative to its workspace folder",
    ),
    (
        "$WORK_DIR",
        re.compile(r"\$\{?WORK_DIR\}?"),
        "nothing ever exported it, so it expanded to a leading slash",
    ),
    (
        ".agents/workspace/memory/",
        re.compile(r"\.agents/workspace/memory/"),
        "workspace memory moved to .agents/memory/ under the workspace folder",
    ),
    (
        "root tools/docs",
        re.compile(r"(?<!\.agents/)tools/docs"),
        "wrapper docs are reached through the workspace's own .agents/tools/docs",
    ),
)


def _corpus() -> list[Path]:
    """Every file whose text reaches the model as a path instruction."""
    return sorted([*TEMPLATES_DIR.rglob("*.j2"), *PLUGINS_DIR.glob("*/skills/**/*.md")])


def _rendered_prompts() -> dict[str, str]:
    """The prompts as the model actually receives them.

    A static scan misses anything a Python formatter contributes, and the MCP
    roster's doc path is built there rather than in a template.
    """
    reset_loader()
    loader = get_loader()
    stub_summary = (
        "\nfinancial_data:\n"
        f"  - Module: {WorkspaceLayout.TOOLS_DIR}/financial_data.py\n"
        "  - Import: from tools.financial_data import <tool_name>\n"
        f"  - Documentation: {SandboxLayout.TOOLS_DOCS_DIR}/financial_data/*.md"
    )
    common = {
        # A root that is not the stock one: a template may spell the real
        # working directory through its variable (the output guidelines show
        # an absolute link as the wrong form), and that has to render clean
        # while a literal root contributed by a formatter still trips the scan.
        **workspace_path_vars(None, root="/srv/sandbox"),
        "subagent_summary": "- general-purpose: does things",
        "tool_summary": stub_summary,
        "thread_id": "a1b2c3d4",
        "current_time": "2026-01-01 00:00:00 UTC",
        "guidance": "detailed",
        "max_concurrent_task_units": 3,
        "memory_enabled": True,
        "memo_enabled": True,
        "ask_user_enabled": True,
        "crawl_enabled": True,
        "user_profile": {"name": "Demo", "timezone": "UTC", "locale": "en-US"},
    }
    return {
        "system": loader.get_system_prompt(**common),
        "mcp_roster": loader.render(
            "envelope/baseline_mcp_servers.md.j2", content=stub_summary
        ),
    }


class TestNoRetiredPathSpelling:
    """The five spellings layout v4 retired must not reappear anywhere."""

    @pytest.mark.parametrize("name,pattern,reason", BANNED, ids=[b[0] for b in BANNED])
    def test_corpus_is_clean(self, name, pattern, reason):
        offenders = [
            f"{path.relative_to(REPO_ROOT)}:{n}: {line.strip()}"
            for path in _corpus()
            for n, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), start=1
            )
            if pattern.search(line)
        ]
        assert not offenders, f"{name} is banned ({reason}):\n" + "\n".join(offenders)

    @pytest.mark.parametrize("name,pattern,reason", BANNED, ids=[b[0] for b in BANNED])
    def test_rendered_prompts_are_clean(self, name, pattern, reason):
        offenders = [
            f"{which}: {line.strip()}"
            for which, text in _rendered_prompts().items()
            for line in text.splitlines()
            if pattern.search(line)
        ]
        assert not offenders, f"{name} is banned ({reason}):\n" + "\n".join(offenders)


class TestImportSpellingSurvives:
    """The wrapper package is imported by name, not by path.

    ``<ws>/.agents`` is on PYTHONPATH, so the package is ``tools`` however the
    directory is spelled on disk. A sweep that rewrites doc paths must not
    follow through into the import line.
    """

    def test_tool_guide_keeps_the_bare_import(self):
        reset_loader()
        text = get_loader().render(
            "components/tool_guide.md.j2", guidance="detailed", tool_summary=""
        )
        assert "from tools.{server_name} import {tool_name}" in text
        assert "from .agents.tools" not in text


class TestCanonicalTableTranscribesTheLayout:
    """The path table is rendered from the layout classes, not retyped."""

    def test_every_layout_name_reaches_the_table(self):
        reset_loader()
        layout = WorkspaceLayout("/home/workspace", "acme-research-1a2b")
        text = get_loader().render(
            "components/workspace_paths.md.j2",
            guidance="detailed",
            **workspace_path_vars(layout, root="/home/workspace"),
        )
        assert "Working directory: `/home/workspace/acme-research-1a2b`" in text
        for name in (
            WorkspaceLayout.AGENT_MD_FILE,
            WorkspaceLayout.DATA_DIR,
            WorkspaceLayout.SKILLS_DIR,
            WorkspaceLayout.MEMORY_DIR,
            SandboxLayout.TOOLS_DOCS_DIR,
            SandboxLayout.USER_DIR,
        ):
            assert f"`{name}" in text, f"{name} is missing from the path table"
        assert "`<task_name>/`" in text
        assert "work/" not in text
        assert "results/" not in text

    def test_the_legacy_note_renders_only_for_a_pre_split_workspace(self):
        """A workspace stamped layout_origin=3 has files that used to live at
        the computer root, so its prompt says where they went."""
        reset_loader()
        layout = WorkspaceLayout("/home/workspace", "acme-research-1a2b")
        render = lambda legacy: get_loader().render(  # noqa: E731
            "components/workspace_paths.md.j2",
            guidance="detailed",
            **workspace_path_vars(layout, root="/home/workspace", legacy_layout=legacy),
        )
        fresh, legacy = render(False), render(True)
        assert "Older Files Here" not in fresh
        assert "Older Files Here" in legacy
        assert "used to live directly under `/home/workspace/`" in legacy
        assert "now under `/home/workspace/acme-research-1a2b/`" in legacy
        assert "`tools/docs/` to `.agents/tools/docs/`" in legacy
        assert legacy.startswith(fresh.rstrip())

    def test_unbound_build_falls_back_to_the_computer_root(self):
        """A preview or a component rendered on its own still renders a table."""
        reset_loader()
        text = get_loader().render(
            "components/workspace_paths.md.j2", guidance="detailed"
        )
        assert "Working directory: `/home/workspace`" in text
        assert "{{" not in text
