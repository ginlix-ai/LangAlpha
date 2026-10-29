"""GET /api/v1/skills/limits: the upload ceiling follows the deployment."""

import pytest

from src.server.app import skills as skills_app
from src.server.services.user_skills.limits import (
    MAX_SKILL_ARCHIVE_BYTES,
    MAX_SKILL_FILES,
    MAX_SKILL_INLINE_BLOB_BYTES,
)


@pytest.mark.asyncio
async def test_with_object_storage_the_wire_cap_applies(skills_client, monkeypatch):
    monkeypatch.setattr(skills_app.skill_archive_storage, "is_configured", lambda: True)
    resp = await skills_client.get("/api/v1/skills/limits")
    assert resp.status_code == 200
    body = resp.json()
    assert body["max_upload_bytes"] == MAX_SKILL_ARCHIVE_BYTES
    assert body["max_stored_bytes"] is None
    assert body["max_files"] == MAX_SKILL_FILES


@pytest.mark.asyncio
async def test_without_object_storage_the_inline_cap_binds_what_is_kept(skills_client, monkeypatch):
    # The inline cap weighs the re-zip after hidden files and dependencies are
    # set aside, not the zip as sent, so the two are reported apart: one cap
    # would refuse a zip that only its .git folder puts over.
    monkeypatch.setattr(skills_app.skill_archive_storage, "is_configured", lambda: False)
    resp = await skills_client.get("/api/v1/skills/limits")
    assert resp.status_code == 200
    body = resp.json()
    assert body["max_upload_bytes"] == MAX_SKILL_ARCHIVE_BYTES
    assert body["max_stored_bytes"] == MAX_SKILL_INLINE_BLOB_BYTES
