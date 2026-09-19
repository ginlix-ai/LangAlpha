"""The two skill tiers a computer serves, exercised as real directories.

A computer keeps one shared skill dir at its root and one per workspace. The
workspace tier owns its own skills as real directories and reaches the shared
ones through relative symlinks, so a skill's ``.agents/skills/<name>/...``
cross-reference resolves the same from either cwd. The script that maintains
that view is run here for real: the command the host builds is executed by a
subprocess against temp dirs, so the base64 envelope, the flock paths and the
JSON contract are all under test, not just the link logic.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import skill_sync

ROOT = "/home/workspace"

#: A minimal authoritative ledger entry; the merge only reads owner/sourceType.
_ENTRY = {"owner": "platform", "sourceType": "builtin"}


class _ShellRuntime:
    """Runs the command the host builds, on this machine's own filesystem."""

    def __init__(self) -> None:
        self.commands: list[str] = []

    async def exec(self, command: str, *args, **kwargs):
        self.commands.append(command)
        proc = subprocess.run(
            command, shell=True, capture_output=True, text=True, check=False
        )
        return SimpleNamespace(
            stdout=proc.stdout, stderr=proc.stderr, exit_code=proc.returncode
        )


class _FakeSandbox:
    def __init__(self, working_dir: str) -> None:
        self.working_dir = working_dir
        self._work_dir = working_dir
        self.runtime = _ShellRuntime()

    async def _runtime_call(self, fn, *args, retry_policy=None, **kwargs):
        return await fn(*args, **kwargs)


def _skill(base: Path, name: str, body: str = "do the thing") -> Path:
    d = base / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "SKILL.md").write_text(f"---\nname: {name}\n---\n{body}\n")
    return d


def _entries(path: Path) -> dict[str, str]:
    """``{name: 'link:<target>' | 'dir' | 'file'}`` for one tier's skills.

    Dot-names are the tier's own bookkeeping (flock, staging, trash), never a
    skill, so they are left out.
    """
    out: dict[str, str] = {}
    for name in sorted(os.listdir(path)):
        if name.startswith("."):
            continue
        p = path / name
        if p.is_symlink():
            out[name] = f"link:{os.readlink(p)}"
        elif p.is_dir():
            out[name] = "dir"
        else:
            out[name] = "file"
    return out


@pytest.fixture
def tiers(tmp_path):
    """A computer root with a shared tier and one workspace tier under it."""
    layout = SandboxLayout(str(tmp_path))
    ws = layout.for_workspace("acme-ab12")
    user_base = Path(layout.skills)
    ws_base = Path(ws.skills)
    user_base.mkdir(parents=True)
    ws_base.mkdir(parents=True)
    return SimpleNamespace(
        sandbox=_FakeSandbox(str(tmp_path)),
        user_base=str(user_base),
        ws_base=str(ws_base),
        user=user_base,
        ws=ws_base,
    )


def _link(tiers, disabled=()):
    return asyncio.run(
        skill_sync.link_shared_skills(
            tiers.sandbox,
            base=tiers.ws_base,
            user_base=tiers.user_base,
            disabled=disabled,
        )
    )


# --------------------------------------------------------------------------
# The relative target
# --------------------------------------------------------------------------


class TestSharedLinkTarget:
    def test_target_is_relative_and_climbs_to_the_computer_root(self):
        layout = SandboxLayout(ROOT)
        target = skill_sync.shared_link_target(
            layout.for_workspace("acme-ab12").skills, layout.skills
        )
        assert target == "../../../.agents/skills"
        assert not target.startswith("/")

    def test_target_resolves_back_to_the_shared_dir(self, tmp_path):
        layout = SandboxLayout(str(tmp_path))
        ws_base = layout.for_workspace("acme-ab12").skills
        target = skill_sync.shared_link_target(ws_base, layout.skills)
        assert os.path.normpath(os.path.join(ws_base, target)) == layout.skills

    def test_coincident_tiers_target_themselves(self):
        layout = SandboxLayout(ROOT)
        assert skill_sync.shared_link_target(layout.skills, layout.skills) == "."


# --------------------------------------------------------------------------
# Materialising the workspace tier
# --------------------------------------------------------------------------


class TestWorkspaceTierMaterialisation:
    def test_shared_skills_arrive_as_relative_links(self, tiers):
        _skill(tiers.user, "financial-modeling")
        _skill(tiers.user, "pdf")

        result = _link(tiers)

        assert result["linked"] == ["financial-modeling", "pdf"]
        assert _entries(tiers.ws) == {
            "financial-modeling": "link:../../../.agents/skills/financial-modeling",
            "pdf": "link:../../../.agents/skills/pdf",
        }

    def test_a_linked_skill_reads_through(self, tiers):
        _skill(tiers.user, "pdf", body="fill forms")
        _link(tiers)

        assert (tiers.ws / "pdf" / "SKILL.md").read_text().endswith("fill forms\n")

    def test_a_cross_reference_resolves_from_the_workspace_tier(self, tiers):
        _skill(tiers.user, "financial-modeling")
        ref = _skill(tiers.user, "equity-research")
        (ref / "SKILL.md").write_text(
            "---\nname: equity-research\n---\nSee .agents/skills/financial-modeling/SKILL.md\n"
        )
        _link(tiers)

        # The relative reference as the agent would resolve it: from the
        # workspace cwd, through the link, back into the shared tree.
        workspace = tiers.ws.parent.parent
        cited = workspace / ".agents/skills/financial-modeling/SKILL.md"
        assert cited.is_file()
        assert "name: financial-modeling" in cited.read_text()

    def test_workspace_tier_skills_stay_real_directories(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.ws, "acme-house-style")

        _link(tiers)

        assert _entries(tiers.ws) == {
            "acme-house-style": "dir",
            "pdf": "link:../../../.agents/skills/pdf",
        }

    def test_workspace_tier_shadows_a_shared_skill_of_the_same_name(self, tiers):
        _skill(tiers.user, "pdf", body="shared version")
        _skill(tiers.ws, "pdf", body="workspace version")

        result = _link(tiers)

        assert result["linked"] == []
        assert not (tiers.ws / "pdf").is_symlink()
        assert "workspace version" in (tiers.ws / "pdf" / "SKILL.md").read_text()

    def test_a_disabled_skill_gets_no_link(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.user, "xlsx")

        result = _link(tiers, disabled=["xlsx"])

        assert result["linked"] == ["pdf"]
        assert set(_entries(tiers.ws)) == {"pdf"}

    def test_disabling_does_not_touch_the_shared_tier(self, tiers):
        _skill(tiers.user, "xlsx")

        _link(tiers, disabled=["xlsx"])

        assert (tiers.user / "xlsx" / "SKILL.md").is_file()

    def test_a_dir_without_skill_md_is_not_linked(self, tiers):
        (tiers.user / ".staging").mkdir()
        (tiers.user / "leftovers").mkdir()
        _skill(tiers.user, "pdf")

        _link(tiers)

        assert set(_entries(tiers.ws)) == {"pdf"}

    def test_a_removed_shared_skill_is_pruned_on_the_next_pass(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.user, "xlsx")
        _link(tiers)

        import shutil

        shutil.rmtree(tiers.user / "xlsx")
        result = _link(tiers)

        assert result["pruned"] == ["xlsx"]
        assert set(_entries(tiers.ws)) == {"pdf"}

    def test_a_newly_disabled_skill_is_pruned(self, tiers):
        _skill(tiers.user, "pdf")
        _link(tiers)

        result = _link(tiers, disabled=["pdf"])

        assert result["pruned"] == ["pdf"]
        assert _entries(tiers.ws) == {}

    def test_a_stale_absolute_link_is_repointed_relative(self, tiers):
        _skill(tiers.user, "pdf")
        os.symlink(str(tiers.user / "pdf"), str(tiers.ws / "pdf"))

        result = _link(tiers)

        assert result["relinked"] == ["pdf"]
        assert os.readlink(tiers.ws / "pdf") == "../../../.agents/skills/pdf"

    def test_a_second_pass_changes_nothing(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.ws, "acme-house-style")
        _link(tiers)
        before = _entries(tiers.ws)

        result = _link(tiers)

        assert result == {"linked": [], "relinked": [], "pruned": [], "blocked": []}
        assert _entries(tiers.ws) == before

    def test_a_file_holding_the_name_is_reported_not_clobbered(self, tiers):
        _skill(tiers.user, "pdf")
        (tiers.ws / "pdf").write_text("not a skill")

        result = _link(tiers)

        assert result["blocked"] == ["pdf"]
        assert (tiers.ws / "pdf").read_text() == "not a skill"

    def test_coincident_tiers_are_a_no_op(self, tiers):
        _skill(tiers.user, "pdf")

        result = asyncio.run(
            skill_sync.link_shared_skills(
                tiers.sandbox, base=tiers.user_base, user_base=tiers.user_base
            )
        )

        assert result == {"linked": [], "relinked": [], "pruned": [], "blocked": []}
        assert tiers.sandbox.runtime.commands == []
        assert _entries(tiers.user) == {"pdf": "dir"}


# --------------------------------------------------------------------------
# Tier isolation
# --------------------------------------------------------------------------


class TestTierIsolation:
    def test_a_sibling_workspace_sees_only_what_it_enabled(self, tmp_path):
        layout = SandboxLayout(str(tmp_path))
        sandbox = _FakeSandbox(str(tmp_path))
        user_base = Path(layout.skills)
        user_base.mkdir(parents=True)
        _skill(user_base, "pdf")
        _skill(user_base, "xlsx")

        views = {}
        for dir_name, disabled in (("alpha-aa11", ()), ("beta-bb22", ("xlsx",))):
            base = Path(layout.for_workspace(dir_name).skills)
            base.mkdir(parents=True)
            if dir_name == "alpha-aa11":
                _skill(base, "alpha-only")
            asyncio.run(
                skill_sync.link_shared_skills(
                    sandbox,
                    base=str(base),
                    user_base=str(user_base),
                    disabled=disabled,
                )
            )
            views[dir_name] = set(_entries(base))

        assert views["alpha-aa11"] == {"pdf", "xlsx", "alpha-only"}
        assert views["beta-bb22"] == {"pdf"}

    def test_report_on_the_workspace_tier_ignores_links(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.ws, "acme-house-style")
        _link(tiers)

        seen = asyncio.run(skill_sync.report(tiers.sandbox, base=tiers.ws_base))

        assert set(seen) == {"acme-house-style"}

    def test_report_defaults_to_the_shared_tier(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.ws, "acme-house-style")

        seen = asyncio.run(skill_sync.report(tiers.sandbox))

        assert set(seen) == {"pdf"}


# --------------------------------------------------------------------------
# Flock and lock file placement
# --------------------------------------------------------------------------


class TestPerTierLocks:
    def test_each_tier_locks_and_ledgers_in_its_own_directory(self, tiers):
        _skill(tiers.user, "pdf")
        _link(tiers)
        asyncio.run(
            skill_sync.merge_authoritative_entries(
                tiers.sandbox, {"shared-one": _ENTRY}
            )
        )
        asyncio.run(
            skill_sync.merge_authoritative_entries(
                tiers.sandbox, {"ws-one": _ENTRY}, base=tiers.ws_base
            )
        )

        assert (tiers.ws / ".skills-sync.flock").is_file()
        assert (tiers.ws / "skills-lock.json").is_file()
        assert (tiers.user / ".skills-sync.flock").is_file()
        assert (tiers.user / "skills-lock.json").is_file()

    def test_the_two_ledgers_are_independent(self, tiers):
        asyncio.run(
            skill_sync.merge_authoritative_entries(
                tiers.sandbox, {"shared-one": _ENTRY}
            )
        )
        asyncio.run(
            skill_sync.merge_authoritative_entries(
                tiers.sandbox, {"ws-one": _ENTRY}, base=tiers.ws_base
            )
        )

        ws_lock = json.loads((tiers.ws / "skills-lock.json").read_text())
        user_lock = json.loads((tiers.user / "skills-lock.json").read_text())
        assert set(ws_lock["skills"]) == {"ws-one"}
        assert set(user_lock["skills"]) == {"shared-one"}

    def test_the_link_pass_locks_only_the_workspace_tier(self, tiers):
        _skill(tiers.user, "pdf")

        _link(tiers)

        assert (tiers.ws / ".skills-sync.flock").is_file()
        assert not (tiers.user / ".skills-sync.flock").exists()

    def test_the_tiers_bookkeeping_is_never_linked_as_a_skill(self, tiers):
        _skill(tiers.user, "pdf")
        asyncio.run(skill_sync.report(tiers.sandbox))

        _link(tiers)

        names = set(os.listdir(tiers.ws))
        assert names == {"pdf", ".skills-sync.flock"}
        assert not (tiers.ws / ".skills-sync.flock").is_symlink()
