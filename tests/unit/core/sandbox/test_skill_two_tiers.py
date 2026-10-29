"""The two skill tiers a computer serves, exercised as real directories.

A computer keeps one shared skill dir at its root and one per workspace. The
workspace tier owns its own skills as real directories and reaches the shared
ones through relative symlinks, so a skill's ``.agents/skills/<name>/...``
cross-reference resolves the same from either cwd. The script that maintains
that view is run here for real: the command the host builds is executed by a
subprocess against temp dirs, so the uploaded script, the flock paths and the
JSON contract are all under test, not just the link logic.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest

from ptc_agent.agent.middleware.skills.lock import (
    MANAGED_SOURCE_TYPE,
    is_shared_tier,
)
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import skill_sync
from ptc_agent.core.sandbox.retry import async_retry_with_backoff
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from ptc_agent.core.sandbox.tree_upload import TreeUploadError
from tests.unit.restored_overlay import archive, python3_on_path

ROOT = "/home/workspace"

#: A minimal authoritative ledger entry; the merge only reads owner/sourceType.
_ENTRY = {"owner": "platform", "sourceType": "builtin"}


class _ShellRuntime:
    """Runs the command the host builds, on this machine's own filesystem."""

    def __init__(self) -> None:
        self.commands: list[str] = []

    async def upload_file(self, content: bytes, dest_path: str):
        path = Path(dest_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

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
        self.layout = SandboxLayout(working_dir)
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

    Dot-names and the ledger are the tier's own bookkeeping (flock, staging,
    trash), never a skill, so they are left out.
    """
    out: dict[str, str] = {}
    for name in sorted(os.listdir(path)):
        if name.startswith(".") or name == "skills-lock.json":
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


def _link(tiers, disabled=(), own=()):
    return asyncio.run(
        skill_sync.link_shared_skills(
            tiers.sandbox,
            base=tiers.ws_base,
            user_base=tiers.user_base,
            disabled=disabled,
            own=own,
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

        assert result == {
            "linked": [],
            "relinked": [],
            "pruned": [],
            "blocked": [],
            "unpinned": [],
        }
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

        assert result == {
            "linked": [],
            "relinked": [],
            "pruned": [],
            "blocked": [],
            "unpinned": [],
        }
        assert tiers.sandbox.runtime.commands == []
        assert _entries(tiers.user) == {"pdf": "dir"}


# --------------------------------------------------------------------------
# Stale copies of shared skills
# --------------------------------------------------------------------------

_PLATFORM = {"owner": "platform", "sourceType": "platform"}


def _ledger(base: Path, entries: dict) -> None:
    (base / "skills-lock.json").write_text(
        json.dumps({"version": 1, "skills": entries})
    )


def _ledgered(base: Path) -> set[str]:
    return set(json.loads((base / "skills-lock.json").read_text())["skills"])


class TestStaleSharedCopies:
    """A folder restored from a backup taken at the computer root holds real
    copies of the shared skills, ledgered as the root's were. Left in place,
    each copy outranks the shared skill and never updates again."""

    def test_a_platform_copy_gives_way_to_a_link(self, tiers):
        _skill(tiers.user, "pdf", body="current")
        _skill(tiers.ws, "pdf", body="stale")
        _ledger(tiers.ws, {"pdf": _PLATFORM})

        result = _link(tiers)

        assert result["unpinned"] == ["pdf"]
        assert result["linked"] == ["pdf"]
        assert (tiers.ws / "pdf" / "SKILL.md").read_text().endswith("current\n")
        assert _ledgered(tiers.ws) == set()

    def test_a_copy_a_restored_sandbox_cannot_rename_still_gives_way(
        self, tiers, monkeypatch, tmp_path
    ):
        """Restored from its archive, the sandbox refuses to rename the copy
        aside (EXDEV), so it is deleted where it is."""
        _skill(tiers.user, "pdf", body="current")
        (_skill(tiers.ws, "pdf", body="stale") / "scripts").mkdir()
        _ledger(tiers.ws, {"pdf": _PLATFORM})
        python3_on_path(monkeypatch, tmp_path / "bin", archive(tiers.ws))

        result = _link(tiers)

        assert result["unpinned"] == ["pdf"]
        assert _entries(tiers.ws) == {"pdf": "link:../../../.agents/skills/pdf"}
        assert _ledgered(tiers.ws) == set()

    def test_a_delivered_user_skill_copy_gives_way_to_a_link(self, tiers):
        _skill(tiers.user, "house-style")
        _skill(tiers.ws, "house-style")
        _ledger(
            tiers.ws,
            {"house-style": {"owner": "user", "sourceType": "langalpha-user"}},
        )

        result = _link(tiers)

        assert result["unpinned"] == ["house-style"]
        assert _entries(tiers.ws) == {
            "house-style": "link:../../../.agents/skills/house-style"
        }

    def test_the_workspace_s_own_skills_keep_their_names(self, tiers):
        for name in ("mine", "synced", "unledgered"):
            _skill(tiers.user, name, body="shared")
            _skill(tiers.ws, name, body="own")
        _ledger(
            tiers.ws,
            {
                "mine": {"owner": "user", "sourceType": "local"},
                "synced": {
                    "owner": "user",
                    "sourceType": "langalpha-user",
                    "sync": {"linkedSkillId": "row-1"},
                },
            },
        )

        result = _link(tiers)

        assert result["unpinned"] == []
        assert _entries(tiers.ws) == {
            "mine": "dir",
            "synced": "dir",
            "unledgered": "dir",
        }
        assert _ledgered(tiers.ws) == {"mine", "synced"}

    def test_a_copy_waits_until_the_shared_tier_serves_its_name(self, tiers):
        """A skill promoted to the user tier is ledgered as delivered before the
        asset sync delivers it; dropping the copy then leaves neither."""
        _skill(tiers.ws, "house-style")
        _skill(tiers.ws, "retired")
        _ledger(
            tiers.ws,
            {
                "house-style": {"owner": "user", "sourceType": "langalpha-user"},
                "retired": _PLATFORM,
            },
        )

        result = _link(tiers)

        assert result["unpinned"] == []
        assert _entries(tiers.ws) == {"house-style": "dir", "retired": "dir"}
        assert _ledgered(tiers.ws) == {"house-style", "retired"}

    def test_a_disabled_shared_skill_s_copy_is_dropped_unlinked(self, tiers):
        _skill(tiers.user, "pdf")
        _skill(tiers.user, "xlsx")
        _skill(tiers.ws, "xlsx")
        _ledger(tiers.ws, {"xlsx": _PLATFORM})

        result = _link(tiers, disabled=["xlsx"])

        assert result["unpinned"] == ["xlsx"]
        assert _entries(tiers.ws) == {"pdf": "link:../../../.agents/skills/pdf"}

    def test_a_workspace_row_keeps_its_copy_whatever_its_entry_says(self, tiers):
        _skill(tiers.user, "house-style", body="shared")
        _skill(tiers.ws, "house-style", body="own")
        _ledger(
            tiers.ws,
            {"house-style": {"owner": "user", "sourceType": "langalpha-user"}},
        )

        result = _link(tiers, own=["house-style"])

        assert result["unpinned"] == []
        assert _entries(tiers.ws) == {"house-style": "dir"}
        assert _ledgered(tiers.ws) == {"house-style"}

    def test_a_workspace_row_without_its_copy_is_never_linked(self, tiers):
        """While a restore is still bringing the copy back, a link at the name
        would turn the retry away and let the shared skill stand in for it."""
        _skill(tiers.user, "house-style")
        _skill(tiers.user, "pdf")
        _link(tiers)

        result = _link(tiers, own=["house-style"])

        assert result["pruned"] == ["house-style"]
        assert _entries(tiers.ws) == {"pdf": "link:../../../.agents/skills/pdf"}

    def test_a_copy_that_cannot_move_aside_keeps_its_entry(self, tiers):
        """Dropped alone, the entry would leave the copy unledgered, which the
        next pass adopts as the workspace's own skill."""
        name = "p" * 240  # too long to take the trash prefix
        _skill(tiers.user, name)
        _skill(tiers.ws, name)
        _ledger(tiers.ws, {name: _PLATFORM})

        result = _link(tiers)

        assert result["unpinned"] == []
        assert _entries(tiers.ws) == {name: "dir"}
        assert _ledgered(tiers.ws) == {name}

    def test_the_script_unpins_exactly_what_the_server_calls_shared(self, tiers):
        """The restore filter and the in-sandbox heal each classify the ledger;
        where they disagree, one of them deletes a workspace's own skill."""
        table = {
            "plat": _PLATFORM,
            "managed": {"owner": "user", "sourceType": MANAGED_SOURCE_TYPE},
            "linked": {
                "owner": "user",
                "sourceType": MANAGED_SOURCE_TYPE,
                "sync": {"linkedSkillId": "row-1"},
            },
            "odd-sync": {"owner": "platform", "sync": "not-a-ref"},
            "local": {"owner": "user", "sourceType": "local"},
            "other": {"owner": "someone", "sourceType": "git"},
        }
        for name in table:
            _skill(tiers.user, name)
            _skill(tiers.ws, name)
        _ledger(tiers.ws, table)

        result = _link(tiers)

        assert result["unpinned"] == sorted(
            name for name, entry in table.items() if is_shared_tier(entry)
        )

    def test_an_entry_over_a_link_is_dropped_and_the_shared_skill_kept(self, tiers):
        """A restore leaves out a shared skill's files but not the ledger that
        names it, so the folder already reaches the skill through a link."""
        _skill(tiers.user, "pdf")
        _link(tiers)
        _ledger(tiers.ws, {"pdf": _PLATFORM})

        result = _link(tiers)

        assert result["unpinned"] == ["pdf"]
        assert result["linked"] == []
        assert _entries(tiers.ws) == {"pdf": "link:../../../.agents/skills/pdf"}
        assert _ledgered(tiers.ws) == set()
        assert (tiers.user / "pdf" / "SKILL.md").is_file()

    def test_the_shared_tier_never_unpins_itself(self, tiers):
        """The wrapper skips coincident tiers by name; the script compares real
        paths too, since there the copies are the shared skills."""
        _skill(tiers.user, "pdf")
        _ledger(tiers.user, {"pdf": _PLATFORM})

        result = asyncio.run(
            skill_sync.link_shared_skills(
                tiers.sandbox, base=f"{tiers.user_base}/.", user_base=tiers.user_base
            )
        )

        assert result["unpinned"] == []
        assert _entries(tiers.user) == {"pdf": "dir"}
        assert _ledgered(tiers.user) == {"pdf"}


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


def _merge(tiers, entries, base=None):
    """An install with nothing staged: the asset sync's lock merge alone."""
    staging = f"{base or tiers.user_base}/.staging/sync-test"
    return asyncio.run(
        skill_sync.install_staged_skills(
            tiers.sandbox, staging, [], entries, base=base
        )
    )


class TestPerTierLocks:
    def test_each_tier_locks_and_ledgers_in_its_own_directory(self, tiers):
        _skill(tiers.user, "pdf")
        _link(tiers)
        _merge(tiers, {"shared-one": _ENTRY})
        _merge(tiers, {"ws-one": _ENTRY}, base=tiers.ws_base)

        assert (tiers.ws / ".skills-sync.flock").is_file()
        assert (tiers.ws / "skills-lock.json").is_file()
        assert (tiers.user / ".skills-sync.flock").is_file()
        assert (tiers.user / "skills-lock.json").is_file()

    def test_the_two_ledgers_are_independent(self, tiers):
        _merge(tiers, {"shared-one": _ENTRY})
        _merge(tiers, {"ws-one": _ENTRY}, base=tiers.ws_base)

        ws_lock = json.loads((tiers.ws / "skills-lock.json").read_text())
        user_lock = json.loads((tiers.user / "skills-lock.json").read_text())
        assert set(ws_lock["skills"]) == {"ws-one"}
        assert set(user_lock["skills"]) == {"shared-one"}

    def test_the_link_pass_waits_out_a_swap_in_the_shared_tier(self, tiers):
        """Mid-swap, under the shared flock, the skill is briefly missing."""
        import fcntl
        import shutil
        import threading
        import time

        _skill(tiers.user, "pdf")
        _skill(tiers.user, "xlsx")
        _link(tiers)
        held = open(tiers.user / ".skills-sync.flock", "w")
        fcntl.flock(held, fcntl.LOCK_EX)
        shutil.move(tiers.user / "xlsx", tiers.user / ".trash-swap-xlsx")
        result = {}
        thread = threading.Thread(target=lambda: result.update(_link(tiers)))
        thread.start()
        time.sleep(0.5)
        shutil.move(tiers.user / ".trash-swap-xlsx", tiers.user / "xlsx")
        fcntl.flock(held, fcntl.LOCK_UN)
        held.close()
        thread.join(timeout=20)

        assert result["pruned"] == []
        assert set(_entries(tiers.ws)) == {"pdf", "xlsx"}

    def test_the_tiers_bookkeeping_is_never_linked_as_a_skill(self, tiers):
        _skill(tiers.user, "pdf")
        asyncio.run(skill_sync.report(tiers.sandbox))

        _link(tiers)

        names = set(os.listdir(tiers.ws))
        assert names == {"pdf", ".skills-sync.flock"}
        assert not (tiers.ws / ".skills-sync.flock").is_symlink()


class TestScriptTransport:
    @pytest.mark.parametrize("count", [150, 3200])
    @pytest.mark.parametrize("mode", ["install", "apply"])
    def test_payloads_beyond_argv_limits_round_trip(self, tiers, count, mode):
        entries = {
            f"skill-{i:04}": {
                **_ENTRY, "description": "x" * 1000 + "\n'\"\\雪$(false)"
            }
            for i in range(count)
        }
        if mode == "install":
            merged, skipped = _merge(tiers, entries)
            assert merged["skills"] == entries
            assert skipped == []
        else:
            results = asyncio.run(
                skill_sync.apply_actions(
                    tiers.sandbox,
                    [
                        {"op": "set_entry", "name": name, "entry": entry}
                        for name, entry in entries.items()
                    ],
                )
            )
            assert results == [
                {"op": "set_entry", "name": name, "ok": True} for name in entries
            ]
        lock = json.loads((tiers.user / "skills-lock.json").read_text())
        assert lock["skills"] == entries
        assert max(len(c.encode()) for c in tiers.sandbox.runtime.commands) < 4096
        assert not list(Path(tiers.sandbox.layout.internal).glob(".skill-sync-*"))

    def test_many_long_staging_paths_use_an_archive(self, tmp_path):
        sandbox = _FakeSandbox(str(tmp_path / "root with 'quotes'"))
        deep = "/".join(["a" * 70] * 30)
        files = [(f"dir-{i}/{deep}/file.txt", f"file-{i}".encode()) for i in range(64)]

        staged = Path(asyncio.run(skill_sync.stage_skill_files(sandbox, files)))

        for rel, content in files:
            assert (staged / rel).read_bytes() == content
        assert asyncio.run(skill_sync.report(sandbox)) == {}
        assert max(len(c.encode()) for c in sandbox.runtime.commands) < 4096
        assert not list(Path(sandbox.layout.internal).glob(".tree-upload-*"))

    @pytest.mark.parametrize("mode", ["install", "apply"])
    def test_script_survives_safe_retries_without_retrying_unsafe_actions(
        self, tiers, monkeypatch, mode
    ):
        sandbox = tiers.sandbox
        original = sandbox.runtime.exec
        attempts = 0

        async def lost_reply(command, *args, **kwargs):
            nonlocal attempts
            result = await original(command, *args, **kwargs)
            if command.startswith("python3 -I "):
                attempts += 1
                if attempts == 1:
                    raise SandboxTransientError("reply lost")
            return result

        async def retry(fn, *args, retry_policy=None, **kwargs):
            return await async_retry_with_backoff(
                fn,
                *args,
                retry_policy=retry_policy,
                is_transient=lambda e: isinstance(e, SandboxTransientError),
                initial_delay_s=0.001,
                **kwargs,
            )

        monkeypatch.setattr(sandbox.runtime, "exec", lost_reply)
        monkeypatch.setattr(sandbox, "_runtime_call", retry)
        if mode == "install":
            assert _merge(tiers, {"pdf": _ENTRY})[0]["skills"] == {"pdf": _ENTRY}
            assert attempts == 2
        else:
            with pytest.raises(SandboxTransientError, match="not retrying automatically"):
                asyncio.run(
                    skill_sync.apply_actions(
                        sandbox, [{"op": "set_entry", "name": "pdf", "entry": _ENTRY}]
                    )
                )
            assert attempts == 1
        assert not list(Path(sandbox.layout.internal).glob(".skill-sync-*"))

    @pytest.mark.parametrize("phase", ["upload", "exec", "parse", "cancel"])
    def test_failed_calls_remove_the_uploaded_script(self, tiers, monkeypatch, phase):
        runtime = tiers.sandbox.runtime
        upload = runtime.upload_file
        execute = runtime.exec

        async def fail_upload(content, path):
            await upload(content, path)
            raise RuntimeError("upload failed")

        async def fail_exec(command, *args, **kwargs):
            if not command.startswith("python3 -I "):
                return await execute(command, *args, **kwargs)
            if phase == "parse":
                return SimpleNamespace(stdout="not JSON", exit_code=1)
            if phase == "cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("exec failed")

        monkeypatch.setattr(
            runtime, "upload_file", fail_upload if phase == "upload" else upload
        )
        monkeypatch.setattr(runtime, "exec", fail_exec)
        expected = {
            "upload": RuntimeError,
            "exec": RuntimeError,
            "parse": skill_sync.SkillSyncError,
            "cancel": asyncio.CancelledError,
        }[phase]
        with pytest.raises(expected):
            asyncio.run(skill_sync.report(tiers.sandbox))
        assert not list(Path(tiers.sandbox.layout.internal).glob(".skill-sync-*"))

    def test_concurrent_calls_have_separate_scripts(self, tiers, monkeypatch):
        runtime = tiers.sandbox.runtime
        upload = runtime.upload_file
        paths = []

        async def interleave(content, path):
            paths.append(path)
            await upload(content, path)
            await asyncio.sleep(0)

        async def reports():
            return await asyncio.gather(
                skill_sync.report(tiers.sandbox, base=tiers.user_base),
                skill_sync.report(tiers.sandbox, base=tiers.ws_base),
            )

        monkeypatch.setattr(runtime, "upload_file", interleave)
        assert asyncio.run(reports()) == [{}, {}]
        assert len(set(paths)) == 2
        assert all(not Path(path).exists() for path in paths)

    def test_cleanup_outlasts_an_anyio_cancel_scope(self, tiers, monkeypatch):
        runtime = tiers.sandbox.runtime
        execute = runtime.exec

        async def cancelled_call():
            with anyio.CancelScope() as scope:
                async def cancel_exec(command, *args, **kwargs):
                    if command.startswith("python3 -I "):
                        scope.cancel()
                    await anyio.lowlevel.checkpoint()
                    return await execute(command, *args, **kwargs)

                monkeypatch.setattr(runtime, "exec", cancel_exec)
                await skill_sync.report(tiers.sandbox)

        asyncio.run(cancelled_call())
        assert not list(Path(tiers.sandbox.layout.internal).glob(".skill-sync-*"))

    @pytest.mark.parametrize("outcome", ["ok", "error", "cancel"])
    def test_stalled_cleanup_preserves_the_sync_outcome(self, tiers, monkeypatch, outcome):
        runtime = tiers.sandbox.runtime
        execute = runtime.exec

        async def stall_cleanup(command, *args, **kwargs):
            if command.startswith("rm -f "):
                await anyio.sleep_forever()
            if outcome == "error":
                raise RuntimeError("sync failed")
            if outcome == "cancel":
                raise asyncio.CancelledError()
            return await execute(command, *args, **kwargs)

        async def bounded_call():
            return await asyncio.wait_for(skill_sync.report(tiers.sandbox), timeout=1)

        monkeypatch.setattr(skill_sync, "_CLEANUP_TIMEOUT_SECONDS", 0.01)
        monkeypatch.setattr(runtime, "exec", stall_cleanup)
        if outcome == "ok":
            assert asyncio.run(bounded_call()) == {}
        else:
            error = RuntimeError if outcome == "error" else asyncio.CancelledError
            with pytest.raises(error):
                asyncio.run(bounded_call())

        (orphan,) = Path(tiers.sandbox.layout.internal).glob(".skill-sync-*")
        os.utime(orphan, (0, 0))
        monkeypatch.setattr(runtime, "exec", execute)
        assert asyncio.run(skill_sync.report(tiers.sandbox)) == {}
        assert not orphan.exists()

    @pytest.mark.parametrize("name", [".skill-sync-stale.py", ".tree-upload-stale.tar.gz"])
    def test_report_sweeps_only_abandoned_sync_uploads(self, tiers, name):
        internal = Path(tiers.sandbox.layout.internal)
        internal.mkdir(parents=True)
        old = internal / name
        recent = internal / name.replace("stale", "running")
        unrelated = internal / "keep.py"
        for path in (old, recent, unrelated):
            path.write_text("not ours to execute")
        for path in (old, unrelated):
            os.utime(path, (0, 0))

        asyncio.run(skill_sync.report(tiers.sandbox))

        assert not old.exists()
        assert recent.exists()
        assert unrelated.exists()

    def test_lost_unpack_reply_defers_the_stage_until_the_next_pass(self, tiers, monkeypatch):
        sandbox = tiers.sandbox
        call = sandbox._runtime_call

        async def replay_unpack(fn, *args, retry_policy=None, **kwargs):
            result = await call(fn, *args, retry_policy=retry_policy, **kwargs)
            if fn == sandbox.runtime.exec and args[0].startswith("[ -f "):
                return await call(fn, *args, retry_policy=retry_policy, **kwargs)
            return result

        monkeypatch.setattr(sandbox, "_runtime_call", replay_unpack)
        with pytest.raises(TreeUploadError, match="exit 97"):
            asyncio.run(skill_sync.stage_skill_files(sandbox, [("SKILL.md", b"new")]))
        assert not (tiers.user / "skills-lock.json").exists()
        monkeypatch.setattr(sandbox, "_runtime_call", call)
        staged = asyncio.run(skill_sync.stage_skill_files(sandbox, [("SKILL.md", b"new")]))
        assert (Path(staged) / "SKILL.md").read_bytes() == b"new"
