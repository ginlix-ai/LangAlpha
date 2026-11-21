"""Tests for lock-aware skill discovery in adiscover_skills().

Verifies that:
- Cache hits return immediately without downloads.
- Cache misses use lock entries when available (no SKILL.md download).
- Cache misses fall back to SKILL.md download when no lock entry exists.
- A dir with no lock entry is read, and no lock entry is written for it.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deepagents.backends.protocol import LsResult

from ptc_agent.agent.middleware.skills.discovery import (
    adiscover_skills,
)


def _make_download_response(content: bytes | None = None, error: bool = False):
    """Build a minimal response object matching what backend.adownload_files returns."""
    return SimpleNamespace(content=content, error=error)


def _make_lock_json(entries: dict) -> str:
    """Serialize a valid skills-lock.json string."""
    import json

    return json.dumps({"version": 1, "skills": entries})


def _make_lock_entry(
    name: str,
    *,
    owner: str = "user",
    description: str = "A test skill",
    confirmed: bool = True,
) -> dict:
    """Build a minimal lock entry dict."""
    return {
        "name": name,
        "description": description,
        "owner": owner,
        "source": "local",
        "sourceType": "local",
        "computedHash": "sha256:abc123",
        "confirmed": confirmed,
        "license": None,
        "metadata": {},
        "allowed_tools": [],
        "installedAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z",
    }


SKILLS_PATH = "/home/workspace/.agents/skills"


class TestDiscoverSkillsWithLock:
    """adiscover_skills() lock-file integration."""

    @pytest.mark.asyncio
    async def test_cache_miss_uses_lock_entry(self):
        """Unknown skill dir + lock entry exists -> uses lock, SKILL.md NOT downloaded."""
        lock_entry = _make_lock_entry("my-skill", description="From lock")
        lock_json = _make_lock_json({"my-skill": lock_entry})

        backend = AsyncMock()
        backend.als = AsyncMock(
            return_value=LsResult(
                entries=[
                    {"path": f"{SKILLS_PATH}/my-skill", "is_dir": True},
                ]
            )
        )
        # First download call is for skills-lock.json
        backend.adownload_files = AsyncMock(
            return_value=[_make_download_response(lock_json.encode("utf-8"))]
        )

        results = await adiscover_skills(
            backend, SKILLS_PATH, known_skills={}
        )

        assert len(results) == 1
        assert results[0]["name"] == "my-skill"
        assert results[0]["description"] == "From lock"

        # adownload_files called once for the lock file, NOT for SKILL.md
        assert backend.adownload_files.call_count == 1
        downloaded_paths = backend.adownload_files.call_args_list[0][0][0]
        assert any("skills-lock.json" in p for p in downloaded_paths)

    @pytest.mark.asyncio
    async def test_hidden_directory_is_not_a_skill(self):
        """The sync's `.staging` dir sits under the skills root; it is never listed."""
        backend = AsyncMock()
        backend.als = AsyncMock(
            return_value=LsResult(
                entries=[
                    {"path": f"{SKILLS_PATH}/.staging", "is_dir": True},
                    {"path": f"{SKILLS_PATH}/my-skill", "is_dir": True},
                ]
            )
        )
        lock_json = _make_lock_json({"my-skill": _make_lock_entry("my-skill")})
        backend.adownload_files = AsyncMock(
            return_value=[_make_download_response(lock_json.encode("utf-8"))]
        )

        results = await adiscover_skills(backend, SKILLS_PATH, known_skills={})

        assert [r["name"] for r in results] == ["my-skill"]
        downloaded = [
            p
            for call in backend.adownload_files.call_args_list
            for p in call[0][0]
        ]
        assert not any(".staging" in p for p in downloaded)

    @pytest.mark.asyncio
    async def test_cache_miss_falls_back_to_skill_md(self):
        """Unknown skill dir + no lock entry + no lock file -> downloads SKILL.md."""
        skill_md_content = (
            "---\n"
            "name: orphan-skill\n"
            "description: Discovered via SKILL.md\n"
            "---\n"
            "# Orphan Skill\n"
        )

        backend = AsyncMock()
        backend.als = AsyncMock(
            return_value=LsResult(
                entries=[
                    {"path": f"{SKILLS_PATH}/orphan-skill", "is_dir": True},
                ]
            )
        )

        # First call: lock file download fails
        # Second call: SKILL.md download succeeds
        backend.adownload_files = AsyncMock(
            side_effect=[
                [_make_download_response(error=True)],  # lock file
                [
                    _make_download_response(
                        skill_md_content.encode("utf-8")
                    )
                ],  # SKILL.md
            ]
        )
        results = await adiscover_skills(
            backend, SKILLS_PATH, known_skills={}
        )

        assert len(results) == 1
        assert results[0]["name"] == "orphan-skill"
        assert results[0]["description"] == "Discovered via SKILL.md"
        assert results[0]["confirmed"] is True

        # Two download rounds: lock file + SKILL.md
        assert backend.adownload_files.call_count == 2

    @pytest.mark.asyncio
    async def test_a_dir_with_no_lock_entry_is_read_and_the_lock_left_alone(self):
        """The lock is merged under its flock by the sync and the reconciler;
        a whole-file write from the listing could land over what they merged."""
        skill_md_content = (
            "---\n"
            "name: orphan-skill\n"
            "description: No entry\n"
            "---\n"
            "# Orphan\n"
        )

        # A lock that already holds another skill's entry is the one a
        # whole-file write could clobber.
        lock = _make_lock_json({"saved-skill": _make_lock_entry("saved-skill")})
        backend = AsyncMock()
        backend.als = AsyncMock(
            return_value=LsResult(
                entries=[
                    {"path": f"{SKILLS_PATH}/saved-skill", "is_dir": True},
                    {"path": f"{SKILLS_PATH}/orphan-skill", "is_dir": True},
                ]
            )
        )
        backend.adownload_files = AsyncMock(
            side_effect=[
                [_make_download_response(lock.encode("utf-8"))],
                [_make_download_response(skill_md_content.encode("utf-8"))],
            ]
        )

        results = await adiscover_skills(backend, SKILLS_PATH, known_skills={})

        assert {skill["name"] for skill in results} == {"saved-skill", "orphan-skill"}
        backend.aupload_files.assert_not_called()
