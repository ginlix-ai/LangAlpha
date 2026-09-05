"""Integration tests for skills-lock.json lifecycle.

Covers:
- Layout: .agents/threads and .agents/skills created after setup
- Lock file: written by _upload_skills, contains platform entries
- User preservation: user lock entries survive platform resync
- Prune protection: user-installed skill dirs survive prune
- Discovery: user-installed skills in manifest after cache build

NOTE: Tests exercise skills methods directly (not full sync_sandbox_assets)
because integration sandbox lacks mcp_registry for tool module install.
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


def _make_test_skill(name: str, description: str = "Test skill") -> str:
    """Create a minimal SKILL.md content string."""
    return (
        f"---\n"
        f"name: {name}\n"
        f"description: {description}\n"
        f"---\n"
        f"# {name}\n"
    )


def _make_lock_entry(
    name: str,
    *,
    owner: str = "user",
    description: str = "User skill",
) -> dict:
    """Build a minimal lock entry dict."""
    return {
        "name": name,
        "description": description,
        "owner": owner,
        "source": "local",
        "sourceType": "local",
        "computedHash": "",
        "confirmed": True,
        "license": None,
        "metadata": {},
        "allowed_tools": [],
        "installedAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z",
    }


def _write_lock_to_sandbox(runtime, lock_path: str, lock_data: dict):
    """Coroutine: upload a lock file to the sandbox."""
    return runtime.upload_file(
        json.dumps(lock_data, indent=2).encode("utf-8"),
        lock_path,
    )


async def _read_lock_from_sandbox(runtime, lock_path: str) -> dict | None:
    """Read and parse the lock file from sandbox, or None if missing."""
    result = await runtime.exec(f"cat {lock_path} 2>/dev/null")
    if result.exit_code != 0 or not result.stdout.strip():
        return None
    return json.loads(result.stdout)


class TestSkillsLayout:
    """Verify .agents/ directory structure after setup."""

    async def test_setup_creates_agents_dirs(self, shared_sandbox):
        """After setup, .agents/threads and .agents/skills exist."""
        work_dir = shared_sandbox._work_dir
        for subdir in (".agents/threads", ".agents/skills"):
            result = await shared_sandbox.runtime.exec(
                f"test -d {work_dir}/{subdir} && echo EXISTS"
            )
            assert "EXISTS" in result.stdout, f"{subdir} not created after setup"

    async def test_system_code_dir_exists(self, shared_sandbox):
        """After setup, .system/code/ exists."""
        result = await shared_sandbox.runtime.exec(
            f"test -d {shared_sandbox._work_dir}/.system/code && echo EXISTS"
        )
        assert "EXISTS" in result.stdout


class TestSkillsLockLifecycle:
    """Lock file creation and platform entries via _upload_skills."""

    async def test_upload_skills_creates_lock_file(self, shared_sandbox):
        """_upload_skills() writes skills-lock.json with platform entries."""
        sb = shared_sandbox
        work_dir = sb._work_dir
        skills_base = f"{work_dir}/.agents/skills"
        lock_path = f"{skills_base}/skills-lock.json"

        # Create a local platform skill in a temp dir
        with tempfile.TemporaryDirectory() as tmpdir:
            skill_dir = os.path.join(tmpdir, "test-plat")
            os.makedirs(skill_dir)
            with open(os.path.join(skill_dir, "SKILL.md"), "w") as f:
                f.write(_make_test_skill("test-plat", "Platform skill"))

            # Compute skills module (generates lock entries)
            from ptc_agent.config.agent import SkillsConfig

            sb._agent_config = type("Cfg", (), {
                "skills": SkillsConfig(
                    enabled=True,
                    user_skills_dir=tmpdir,
                    sandbox_skills_base=skills_base,
                ),
            })()

            skills_mod = await sb._compute_skills_module([tmpdir])

            # Upload skills + write lock
            merged_lock, collisions = await sb._upload_skills(
                [(tmpdir, skills_base)],
                manifest=skills_mod,
                existing_lock=None,
            )
            assert not collisions, 'no name here is agent-installed'

        # Verify lock file written
        lock_data = await _read_lock_from_sandbox(sb.runtime, lock_path)
        assert lock_data is not None, "skills-lock.json not created"
        assert lock_data["version"] == 1

        # Verify platform skill entry
        skills = lock_data.get("skills", {})
        assert "test-plat" in skills, f"test-plat not in lock: {list(skills.keys())}"
        assert skills["test-plat"]["owner"] == "platform"
        assert skills["test-plat"]["description"] == "Platform skill"

        # Verify merged_lock was returned
        assert merged_lock is not None
        assert "test-plat" in merged_lock.get("skills", {})

    async def test_lock_preserves_user_entries_on_resync(self, shared_sandbox):
        """User lock entries survive when platform skills are re-uploaded."""
        sb = shared_sandbox
        work_dir = sb._work_dir
        skills_base = f"{work_dir}/.agents/skills"
        lock_path = f"{skills_base}/skills-lock.json"

        # Create user skill dir + lock entry
        user_dir = f"{skills_base}/my-custom"
        await sb.runtime.exec(f"mkdir -p {user_dir}")
        await sb.runtime.upload_file(
            _make_test_skill("my-custom", "Custom user skill").encode(),
            f"{user_dir}/SKILL.md",
        )

        # Seed the lock FILE with the user entry, the way this state arises in
        # prod (existing_lock is always _download_skills_lock's read of it).
        # The merge re-reads the live file under the sandbox flock, so an
        # entry that exists only as a host-side dict would rightly not
        # survive — the file is the ledger, the parameter is a snapshot.
        existing_lock = {
            "my-custom": _make_lock_entry("my-custom", description="Custom user skill"),
        }
        await sb.runtime.upload_file(
            json.dumps({"version": 1, "skills": existing_lock}).encode(),
            lock_path,
        )

        # Upload platform skills with existing_lock containing user entry
        with tempfile.TemporaryDirectory() as tmpdir:
            skill_dir = os.path.join(tmpdir, "plat-skill")
            os.makedirs(skill_dir)
            with open(os.path.join(skill_dir, "SKILL.md"), "w") as f:
                f.write(_make_test_skill("plat-skill", "Platform"))

            skills_mod = await sb._compute_skills_module([tmpdir])
            merged_lock, collisions = await sb._upload_skills(
                [(tmpdir, skills_base)],
                manifest=skills_mod,
                existing_lock=existing_lock,
            )
            assert not collisions, 'the user entry has a different name'

        # Verify user entry preserved in merged result
        assert merged_lock is not None
        merged_skills = merged_lock.get("skills", {})
        assert "my-custom" in merged_skills, "User entry lost during merge"
        assert merged_skills["my-custom"]["owner"] == "user"
        assert "plat-skill" in merged_skills, "Platform entry missing"
        assert merged_skills["plat-skill"]["owner"] == "platform"

        # Verify on-disk lock also has both
        lock_data = await _read_lock_from_sandbox(sb.runtime, lock_path)
        assert lock_data is not None
        assert "my-custom" in lock_data["skills"]
        assert "plat-skill" in lock_data["skills"]


class TestPruneProtection:
    """User-installed skill directories survive platform prune."""

    async def test_user_skill_survives_prune(self, shared_sandbox):
        """Skill dir with owner=user in lock is NOT pruned."""
        sb = shared_sandbox
        work_dir = sb._work_dir
        skills_base = f"{work_dir}/.agents/skills"

        # Create user skill dir
        user_dir = f"{skills_base}/user-survivor"
        await sb.runtime.exec(f"mkdir -p {user_dir}")
        await sb.runtime.upload_file(
            _make_test_skill("user-survivor").encode(),
            f"{user_dir}/SKILL.md",
        )

        existing_lock = {
            "user-survivor": _make_lock_entry("user-survivor"),
        }

        # Prune with empty local_skill_names (all platform skills removed)
        await sb._prune_remote_skills(
            skills_base, local_skill_names=set(), existing_lock=existing_lock
        )

        # Verify user skill dir still exists
        result = await sb.runtime.exec(
            f"test -d {user_dir} && echo EXISTS"
        )
        assert "EXISTS" in result.stdout, "User skill was pruned!"

    async def test_stale_platform_skill_pruned(self, shared_sandbox):
        """Skill dir with owner=platform NOT in local set IS pruned."""
        sb = shared_sandbox
        work_dir = sb._work_dir
        skills_base = f"{work_dir}/.agents/skills"

        # Create stale platform skill dir
        stale_dir = f"{skills_base}/old-platform"
        await sb.runtime.exec(f"mkdir -p {stale_dir}")
        await sb.runtime.upload_file(
            _make_test_skill("old-platform").encode(),
            f"{stale_dir}/SKILL.md",
        )

        existing_lock = {
            "old-platform": _make_lock_entry(
                "old-platform", owner="platform", description="Stale"
            ),
        }

        await sb._prune_remote_skills(
            skills_base, local_skill_names=set(), existing_lock=existing_lock
        )

        # Verify stale dir was removed
        result = await sb.runtime.exec(
            f"test -d {stale_dir} && echo EXISTS || echo GONE"
        )
        assert "GONE" in result.stdout, "Stale platform skill was not pruned"
