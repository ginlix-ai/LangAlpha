"""A skill name the asset sync may not write over is settled once, not per start.

The sync stamps the skipped names into the skills version so a freed name is
retried. That stamp never matches a freshly computed version, so before this
every start of such a computer re-ran the whole skills upload to skip the same
names again. A stamp whose names are all still held now carries over.
"""

from __future__ import annotations

import copy
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.migration import CURRENT_LAYOUT_VERSION
from tests.unit.core.sandbox.test_sync_partial_failure import _make_sandbox

AGENT_INSTALLED = {"owner": "user", "source": "local", "sourceType": "local"}
PLATFORM = {"owner": "platform", "source": "platform", "sourceType": "platform"}


def _skills(tmp_path, body: str = "Draw.") -> list[str]:
    root = tmp_path / "skills"
    for name in ("charting", "run-workflow"):
        (root / name).mkdir(parents=True, exist_ok=True)
        (root / name / "SKILL.md").write_text(f"---\nname: {name}\n---\n{body}\n")
    return [str(root)]


class _Computer:
    """One sandbox across syncs: what a sync writes, the next one reads."""

    def __init__(self, lock: dict) -> None:
        self.sandbox = _make_sandbox([])
        self.manifest: dict | None = None
        self.sandbox._read_unified_manifest = AsyncMock(side_effect=self._read)
        self.sandbox._write_unified_manifest = AsyncMock(side_effect=self._write)
        self.sandbox._download_skills_lock = AsyncMock(side_effect=lambda: lock)
        self.sandbox._upload_skills = AsyncMock(return_value=({}, {"run-workflow"}))

    async def _read(self):
        return copy.deepcopy(self.manifest)

    async def _write(self, manifest):
        self.manifest = copy.deepcopy(manifest)

    async def sync(self, skill_dirs):
        self.sandbox._skills_manifest = None  # a start in a fresh session
        with patch(
            "ptc_agent.core.sandbox.assets.run_layout_migrations",
            AsyncMock(return_value=CURRENT_LAYOUT_VERSION),
        ):
            await self.sandbox.sync_sandbox_assets(
                reusing_sandbox=True, skill_dirs=skill_dirs
            )


@pytest.mark.asyncio
async def test_a_name_still_held_is_not_uploaded_again(tmp_path):
    computer = _Computer({"run-workflow": AGENT_INSTALLED})
    await computer.sync(_skills(tmp_path))
    assert computer.sandbox._upload_skills.await_count == 1
    stamped = computer.manifest["modules"]["skills"]
    assert stamped["collisions"] == ["run-workflow"]

    for _ in range(3):
        await computer.sync(_skills(tmp_path))

    assert computer.sandbox._upload_skills.await_count == 1
    assert computer.manifest["modules"]["skills"]["version"] == stamped["version"]


@pytest.mark.asyncio
async def test_a_freed_name_is_uploaded(tmp_path):
    lock = {"run-workflow": AGENT_INSTALLED}
    computer = _Computer(lock)
    await computer.sync(_skills(tmp_path))

    lock["run-workflow"] = PLATFORM
    computer.sandbox._upload_skills.return_value = ({}, set())
    await computer.sync(_skills(tmp_path))

    assert computer.sandbox._upload_skills.await_count == 2
    assert "collisions" not in computer.manifest["modules"]["skills"]


@pytest.mark.asyncio
async def test_a_name_taken_after_the_stamp_is_uploaded(tmp_path):
    """Its skip joins the stamp, so freeing it later reaches the upload."""
    lock = {"run-workflow": AGENT_INSTALLED}
    computer = _Computer(lock)
    await computer.sync(_skills(tmp_path))

    lock["charting"] = AGENT_INSTALLED
    computer.sandbox._upload_skills.return_value = ({}, {"run-workflow", "charting"})
    await computer.sync(_skills(tmp_path))

    assert computer.sandbox._upload_skills.await_count == 2
    assert computer.manifest["modules"]["skills"]["collisions"] == [
        "charting",
        "run-workflow",
    ]


@pytest.mark.asyncio
async def test_a_changed_skill_is_uploaded_beside_a_held_name(tmp_path):
    computer = _Computer({"run-workflow": AGENT_INSTALLED})
    await computer.sync(_skills(tmp_path))

    await computer.sync(_skills(tmp_path, body="Draw better."))

    assert computer.sandbox._upload_skills.await_count == 2


@pytest.mark.asyncio
async def test_an_unreadable_lock_uploads(tmp_path):
    computer = _Computer({"run-workflow": AGENT_INSTALLED})
    await computer.sync(_skills(tmp_path))

    computer.sandbox._download_skills_lock = AsyncMock(return_value=None)
    await computer.sync(_skills(tmp_path))

    assert computer.sandbox._upload_skills.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["no_version", "entry_not_a_dict"])
async def test_a_damaged_manifest_or_lock_uploads_rather_than_failing_the_sync(
    tmp_path, damage
):
    lock = {"run-workflow": AGENT_INSTALLED}
    computer = _Computer(lock)
    await computer.sync(_skills(tmp_path))
    if damage == "no_version":
        del computer.manifest["modules"]["skills"]["version"]
    else:
        lock["run-workflow"] = "agent"

    await computer.sync(_skills(tmp_path))

    assert computer.sandbox._upload_skills.await_count == 2


@pytest.mark.asyncio
async def test_a_carried_stamp_keeps_the_agents_own_skills_known(tmp_path):
    mine = {**AGENT_INSTALLED, "name": "my-notes", "description": "Notes I keep."}
    lock = {"run-workflow": {**mine, "name": "run-workflow"}, "my-notes": mine}
    computer = _Computer(lock)
    await computer.sync(_skills(tmp_path))
    computer.sandbox._download_skills_lock.reset_mock()

    await computer.sync(_skills(tmp_path))

    assert "my-notes" in computer.sandbox._skills_manifest["skills"]
    assert computer.sandbox._download_skills_lock.await_count == 1
