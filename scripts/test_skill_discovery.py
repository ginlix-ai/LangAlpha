#!/usr/bin/env python3
"""Test skill discovery optimization against a real Daytona sandbox.

Verifies:
1. sync_sandbox_assets enriches manifest with parsed skill metadata
2. adiscover_skills uses known_skills cache (0 downloads for known skills)
3. adiscover_skills downloads SKILL.md only for unknown skills
4. End-to-end flow: sandbox.skills_manifest -> SkillsMiddleware known_skills

Usage:
    uv run python scripts/test_skill_discovery.py
"""

import asyncio
import sys
import time
from pathlib import Path

# Add project root to path so we can import from src/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# ── Config ──────────────────────────────────────────────────────────────────


async def load_config():
    from ptc_agent.config.loaders import load_from_files

    return await load_from_files()


# ── Helpers ─────────────────────────────────────────────────────────────────


def section(title: str) -> None:
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def ok(msg: str) -> None:
    print(f"  [PASS] {msg}")


def fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")


def info(msg: str) -> None:
    print(f"  [INFO] {msg}")


# ── Tests ───────────────────────────────────────────────────────────────────


async def test_manifest_has_skills_metadata(sandbox, skill_dirs):
    """Test 1: sync_sandbox_assets populates skills_manifest with parsed metadata."""
    section("Test 1: sync_sandbox_assets enriches manifest with skill metadata")

    await sandbox.sync_sandbox_assets(
        skill_dirs=skill_dirs, reusing_sandbox=False, force_refresh=True
    )

    manifest = sandbox.skills_manifest
    assert manifest is not None, (
        "skills_manifest should not be None after sync_sandbox_assets"
    )
    ok("skills_manifest is set after sync_sandbox_assets")

    assert "version" in manifest, "manifest should have 'version' key"
    assert "files" in manifest, "manifest should have 'files' key"
    assert "skills" in manifest, "manifest should have 'skills' key"
    ok("manifest has version, files, and skills keys")

    skills = manifest["skills"]
    info(f"Found {len(skills)} skills in manifest")

    for name, meta in skills.items():
        assert "name" in meta, f"skill {name} missing 'name'"
        assert "description" in meta, f"skill {name} missing 'description'"
        assert "path" in meta, f"skill {name} missing 'path'"
        assert meta["name"] == name, f"skill name mismatch: {meta['name']} != {name}"
        info(f"  {name}: {meta['description'][:60]}...")

    assert len(skills) > 0, "should have at least one skill with metadata"
    ok(f"All {len(skills)} skills have valid metadata")

    return skills


async def test_manifest_preserved_on_no_upload(sandbox, skill_dirs):
    """Test 2: skills_manifest is preserved when no upload occurs (reuse path)."""
    section("Test 2: skills_manifest preserved on no-upload path")

    # First sync populates the manifest
    await sandbox.sync_sandbox_assets(
        skill_dirs=skill_dirs, reusing_sandbox=False, force_refresh=True
    )
    first_manifest = sandbox.skills_manifest
    assert first_manifest is not None

    # Second sync should detect no changes and take the early-return path
    result = await sandbox.sync_sandbox_assets(
        skill_dirs=skill_dirs, reusing_sandbox=True
    )
    assert not result.refreshed_modules, "second sync should not upload (no changes)"
    ok("Second sync correctly detected no changes")

    second_manifest = sandbox.skills_manifest
    assert second_manifest is not None, "skills_manifest should still be set"
    assert second_manifest["version"] == first_manifest["version"]
    ok("Manifest version matches after no-upload path")


async def test_discover_all_known(sandbox, skill_dirs):
    """Test 3: adiscover_skills with all skills known → 0 downloads."""
    section("Test 3: adiscover_skills with known_skills (0 downloads expected)")

    from ptc_agent.agent.backends.daytona import DaytonaBackend
    from ptc_agent.agent.middleware.skills.discovery import (
        SkillMetadata,
        adiscover_skills,
    )

    backend = DaytonaBackend(sandbox=sandbox)

    # Build known_skills from manifest
    manifest = sandbox.skills_manifest
    assert manifest and manifest.get("skills"), "need skills in manifest"

    known_skills = {
        name: SkillMetadata(**meta) for name, meta in manifest["skills"].items()
    }
    info(f"Passing {len(known_skills)} known skills")

    # Wrap adownload_files to count calls
    original_download = backend.adownload_files

    download_call_count = 0
    download_paths: list[list[str]] = []

    async def counting_download(paths):
        nonlocal download_call_count
        download_call_count += 1
        download_paths.append(paths)
        return await original_download(paths)

    backend.adownload_files = counting_download  # type: ignore[assignment]

    t0 = time.monotonic()
    results = await adiscover_skills(backend, "/home/daytona/skills/", known_skills)
    elapsed = time.monotonic() - t0

    info(f"Discovered {len(results)} skills in {elapsed:.3f}s")
    info(f"adownload_files called {download_call_count} time(s)")
    if download_paths:
        for i, paths in enumerate(download_paths):
            info(f"  download call {i}: {paths}")

    assert download_call_count == 0, (
        f"Expected 0 download calls with all known skills, got {download_call_count}"
    )
    ok("Zero downloads when all skills are known")

    assert len(results) == len(known_skills), (
        f"Expected {len(known_skills)} results, got {len(results)}"
    )
    ok(f"All {len(results)} known skills returned from cache")

    return results


async def test_discover_empty_known(sandbox, skill_dirs):
    """Test 4: adiscover_skills with no known skills → downloads all."""
    section("Test 4: adiscover_skills with empty known_skills (downloads expected)")

    from ptc_agent.agent.backends.daytona import DaytonaBackend
    from ptc_agent.agent.middleware.skills.discovery import adiscover_skills

    backend = DaytonaBackend(sandbox=sandbox)

    # Wrap adownload_files to count calls
    original_download = backend.adownload_files
    download_call_count = 0
    downloaded_paths: list[str] = []

    async def counting_download(paths):
        nonlocal download_call_count
        download_call_count += 1
        downloaded_paths.extend(paths)
        return await original_download(paths)

    backend.adownload_files = counting_download  # type: ignore[assignment]

    t0 = time.monotonic()
    results = await adiscover_skills(backend, "/home/daytona/skills/", {})
    elapsed = time.monotonic() - t0

    info(f"Discovered {len(results)} skills in {elapsed:.3f}s")
    info(f"adownload_files called {download_call_count} time(s)")
    info(f"Downloaded {len(downloaded_paths)} SKILL.md files: {downloaded_paths}")

    assert download_call_count == 1, (
        f"Expected 1 batch download call, got {download_call_count}"
    )
    ok("Exactly 1 batch download call with empty known_skills")

    assert len(results) > 0, "Should discover at least some skills"
    ok(f"Discovered {len(results)} skills via download")

    # Verify result shape
    for skill in results:
        assert "name" in skill
        assert "description" in skill
        assert "path" in skill
        assert "allowed_tools" in skill
    ok("All discovered skills have correct metadata shape")


async def test_discover_partial_known(sandbox, skill_dirs):
    """Test 5: adiscover_skills with partial known → downloads only unknown."""
    section("Test 5: adiscover_skills with partial known_skills")

    from ptc_agent.agent.backends.daytona import DaytonaBackend
    from ptc_agent.agent.middleware.skills.discovery import (
        SkillMetadata,
        adiscover_skills,
    )

    backend = DaytonaBackend(sandbox=sandbox)
    manifest = sandbox.skills_manifest
    assert manifest and manifest.get("skills")

    all_skills = {
        name: SkillMetadata(**meta) for name, meta in manifest["skills"].items()
    }

    if len(all_skills) < 2:
        info(f"Only {len(all_skills)} skill(s) — skipping partial test")
        return

    # Keep only the first skill as "known"
    first_name = next(iter(all_skills))
    partial_known = {first_name: all_skills[first_name]}
    unknown_count = len(all_skills) - 1

    info(f"Passing 1 known skill ({first_name}), {unknown_count} unknown")

    original_download = backend.adownload_files
    downloaded_paths: list[str] = []

    async def counting_download(paths):
        downloaded_paths.extend(paths)
        return await original_download(paths)

    backend.adownload_files = counting_download  # type: ignore[assignment]

    results = await adiscover_skills(backend, "/home/daytona/skills/", partial_known)

    info(f"Discovered {len(results)} skills total")
    info(f"Downloaded {len(downloaded_paths)} SKILL.md files")

    # Should download only the unknown skills' SKILL.md
    assert len(downloaded_paths) == unknown_count, (
        f"Expected {unknown_count} downloads, got {len(downloaded_paths)}"
    )
    ok(f"Downloaded exactly {unknown_count} unknown skill(s)")

    # Known skill should not appear in downloads
    known_path = f"/home/daytona/skills/{first_name}/SKILL.md"
    assert known_path not in downloaded_paths, (
        f"Known skill {first_name} should not be downloaded"
    )
    ok(f"Known skill '{first_name}' was NOT re-downloaded")


async def test_middleware_integration(sandbox, skill_dirs):
    """Test 6: SkillsMiddleware receives known_skills from manifest."""
    section("Test 6: SkillsMiddleware integration with known_skills")

    from ptc_agent.agent.backends.daytona import DaytonaBackend
    from ptc_agent.agent.middleware.skills.discovery import SkillMetadata
    from ptc_agent.agent.middleware.skills.middleware import SkillsMiddleware

    manifest = sandbox.skills_manifest
    assert manifest and manifest.get("skills")

    known_skills = {
        name: SkillMetadata(**meta) for name, meta in manifest["skills"].items()
    }

    backend = DaytonaBackend(sandbox=sandbox)

    middleware = SkillsMiddleware(
        mode="ptc",
        backend=backend,
        sources=["/home/daytona/skills/"],
        known_skills=known_skills,
    )

    assert len(middleware._known_skills) == len(known_skills)
    ok(f"Middleware initialized with {len(known_skills)} known skills")

    # Run abefore_agent with a mock state (no discovered_skills yet)
    state = {}

    # Wrap backend to verify no downloads
    original_download = backend.adownload_files
    download_count = 0

    async def counting_download(paths):
        nonlocal download_count
        download_count += 1
        return await original_download(paths)

    backend.adownload_files = counting_download  # type: ignore[assignment]

    result = await middleware.abefore_agent(state, None)

    info(
        f"abefore_agent returned {len(result.get('discovered_skills', []))} discovered skills"
    )
    info(f"Downloads during abefore_agent: {download_count}")

    assert download_count == 0, f"Expected 0 downloads, got {download_count}"
    ok("abefore_agent made 0 downloads with known_skills populated")


# ── Main ────────────────────────────────────────────────────────────────────


async def main():
    print("Loading config...")
    config = await load_config()
    core_config = config.to_core_config()

    from ptc_agent.core.sandbox import PTCSandbox

    sandbox = PTCSandbox(config=core_config)

    skill_dirs = config.skills.local_skill_dirs_with_sandbox()
    info(f"Skill dirs: {skill_dirs}")

    try:
        section("Sandbox Setup")
        print("  Creating sandbox (may take 30-60s)...")
        t0 = time.monotonic()
        await sandbox.setup_sandbox_workspace()
        elapsed = time.monotonic() - t0
        ok(f"Sandbox created in {elapsed:.1f}s: {sandbox.sandbox_id}")

        # Run tests
        await test_manifest_has_skills_metadata(sandbox, skill_dirs)
        await test_manifest_preserved_on_no_upload(sandbox, skill_dirs)
        await test_discover_all_known(sandbox, skill_dirs)
        await test_discover_empty_known(sandbox, skill_dirs)
        await test_discover_partial_known(sandbox, skill_dirs)
        await test_middleware_integration(sandbox, skill_dirs)

        section("ALL TESTS PASSED")

    except AssertionError as e:
        fail(str(e))
        sys.exit(1)
    except Exception as e:
        fail(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup: delete sandbox
        section("Cleanup")
        if sandbox.sandbox_id:
            try:
                await sandbox.daytona_client.delete(sandbox.sandbox)
                ok(f"Sandbox {sandbox.sandbox_id} deleted")
            except Exception as e:
                info(f"Cleanup failed (non-fatal): {e}")


if __name__ == "__main__":
    asyncio.run(main())
