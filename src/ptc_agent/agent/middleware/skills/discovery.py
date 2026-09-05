"""Skill discovery — types, parsing, and optimized sandbox scanning.

In-house implementation of skill metadata parsing and filesystem scanning,
replacing the deepagents dependency. Follows the Agent Skills specification
(https://agentskills.io/specification).
"""

import hashlib
import re
from pathlib import PurePosixPath
from typing import Any, TypedDict

import structlog
import yaml

logger = structlog.get_logger(__name__)

# --- Constants ---
MAX_SKILL_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_SKILL_NAME_LENGTH = 64
MAX_SKILL_DESCRIPTION_LENGTH = 1024
MAX_SKILL_COMPATIBILITY_LENGTH = 500


# --- Types ---


class SkillMetadata(TypedDict):
    """Metadata for a skill per Agent Skills specification."""

    path: str
    """Path to the SKILL.md file."""

    name: str
    """Skill identifier (lowercase alphanumeric + hyphens, 1-64 chars)."""

    description: str
    """What the skill does (1-1024 chars)."""

    license: str | None
    """License name or reference to bundled license file."""

    compatibility: str | None
    """Environment requirements (1-500 chars if provided)."""

    metadata: dict[str, str]
    """Arbitrary key-value mapping for additional metadata."""

    allowed_tools: list[str]
    """Tool names the skill recommends using."""

    confirmed: bool
    """True when valid frontmatter was parsed; False for fallback-discovered skills."""


# --- Validation helpers ---


def validate_skill_name(name: str, directory_name: str) -> tuple[bool, str]:
    """Validate skill name per Agent Skills specification."""
    if not name:
        return False, "name is required"
    if len(name) > MAX_SKILL_NAME_LENGTH:
        return False, "name exceeds 64 characters"
    if name.startswith("-") or name.endswith("-") or "--" in name:
        return False, "name must be lowercase alphanumeric with single hyphens only"
    for c in name:
        # ASCII only: str.isalpha()/isdigit() accept Unicode letters and
        # digits, which would admit names that look identical to a platform
        # skill's and paths no other layer here expects.
        if c == "-" or "a" <= c <= "z" or "0" <= c <= "9":
            continue
        return False, "name must be lowercase alphanumeric with single hyphens only"
    if name != directory_name:
        return False, f"name '{name}' must match directory name '{directory_name}'"
    return True, ""


def _validate_metadata(raw: object, skill_path: str) -> dict[str, str]:
    """Validate and normalize the metadata field from YAML frontmatter."""
    if not isinstance(raw, dict):
        if raw:
            logger.warning(
                "Ignoring non-dict metadata in %s (got %s)",
                skill_path,
                type(raw).__name__,
            )
        return {}
    return {str(k): str(v) for k, v in raw.items()}


# --- Parsing ---


def _make_unconfirmed_metadata(
    skill_path: str, directory_name: str
) -> SkillMetadata:
    """Build a fallback SkillMetadata for skills without valid frontmatter."""
    return SkillMetadata(
        name=directory_name,
        description="",
        path=skill_path,
        metadata={},
        license=None,
        compatibility=None,
        allowed_tools=[],
        confirmed=False,
    )


def _parse_allowed_tools(raw_tools: object, skill_path: str) -> list[str]:
    """Parse allowed-tools from frontmatter (list or legacy string format)."""
    if isinstance(raw_tools, list):
        return [str(t).strip() for t in raw_tools if str(t).strip()]
    elif isinstance(raw_tools, str):
        return [t.strip(",") for t in raw_tools.split() if t.strip(",")]
    else:
        if raw_tools is not None:
            logger.warning(
                "Ignoring unsupported 'allowed-tools' type in %s (got %s)",
                skill_path,
                type(raw_tools).__name__,
            )
        return []


def parse_skill_metadata(
    content: str,
    skill_path: str,
    directory_name: str,
) -> SkillMetadata:
    """Parse YAML frontmatter from ``SKILL.md`` content.

    Always returns a SkillMetadata — skills without valid frontmatter get a
    fallback entry with ``confirmed=False`` so they are still discoverable.

    Args:
        content: Raw text content of the SKILL.md file.
        skill_path: Path to the SKILL.md (used in log messages and returned metadata).
        directory_name: Name of the parent directory (validated against ``name`` field).

    Returns:
        Parsed SkillMetadata. ``confirmed`` is True when valid frontmatter was
        parsed, False for fallback/incomplete entries.
    """
    if len(content) > MAX_SKILL_FILE_SIZE:
        logger.warning(
            "Skipping %s: content too large (%d bytes)", skill_path, len(content)
        )
        return _make_unconfirmed_metadata(skill_path, directory_name)

    frontmatter_pattern = r"^---\s*\n(.*?)\n---\s*\n"
    match = re.match(frontmatter_pattern, content, re.DOTALL)

    if not match:
        logger.debug("No YAML frontmatter in %s; using fallback metadata", skill_path)
        return _make_unconfirmed_metadata(skill_path, directory_name)

    frontmatter_str = match.group(1)

    try:
        frontmatter_data = yaml.safe_load(frontmatter_str)
    except yaml.YAMLError as e:
        logger.warning("Invalid YAML in %s: %s", skill_path, e)
        return _make_unconfirmed_metadata(skill_path, directory_name)

    if not isinstance(frontmatter_data, dict):
        logger.warning("Frontmatter is not a mapping in %s; using fallback", skill_path)
        return _make_unconfirmed_metadata(skill_path, directory_name)

    name = str(frontmatter_data.get("name", "")).strip()
    description = str(frontmatter_data.get("description", "")).strip()

    confirmed = True
    if not name:
        name = directory_name
        confirmed = False
    if not description:
        confirmed = False

    is_valid, error = validate_skill_name(name, directory_name)
    if not is_valid:
        logger.warning(
            "Skill '%s' in %s: %s — using directory name",
            name,
            skill_path,
            error,
        )
        name = directory_name
        confirmed = False

    description_str = description
    if len(description_str) > MAX_SKILL_DESCRIPTION_LENGTH:
        logger.warning(
            "Description exceeds %d characters in %s, truncating",
            MAX_SKILL_DESCRIPTION_LENGTH,
            skill_path,
        )
        description_str = description_str[:MAX_SKILL_DESCRIPTION_LENGTH]

    allowed_tools = _parse_allowed_tools(
        frontmatter_data.get("allowed-tools"), skill_path
    )

    compatibility_str = str(frontmatter_data.get("compatibility", "")).strip() or None
    if compatibility_str and len(compatibility_str) > MAX_SKILL_COMPATIBILITY_LENGTH:
        logger.warning(
            "Compatibility exceeds %d characters in %s, truncating",
            MAX_SKILL_COMPATIBILITY_LENGTH,
            skill_path,
        )
        compatibility_str = compatibility_str[:MAX_SKILL_COMPATIBILITY_LENGTH]

    return SkillMetadata(
        name=name,
        description=description_str,
        path=skill_path,
        metadata=_validate_metadata(frontmatter_data.get("metadata", {}), skill_path),
        license=str(frontmatter_data.get("license", "")).strip() or None,
        compatibility=compatibility_str,
        allowed_tools=allowed_tools,
        confirmed=confirmed,
    )


# --- Optimized discovery ---


async def adiscover_skills(
    backend: Any,
    source_path: str,
    known_skills: dict[str, SkillMetadata],
) -> list[SkillMetadata]:
    """Discover skills from sandbox filesystem, downloading only new ones.

    For skills already present in ``known_skills`` (keyed by directory name),
    the cached metadata is returned without downloading. Cache misses check
    the skills-lock.json first (single file), falling back to individual
    SKILL.md downloads only when no lock entry exists.

    Self-healing: orphaned skill dirs (no lock entry) get a lock entry
    written back after SKILL.md parse.
    """
    result = await backend.als(source_path)  # 1 API call
    items = result.entries or []
    skill_dirs = [item["path"] for item in items if item.get("is_dir")]

    if not skill_dirs:
        return []

    results: list[SkillMetadata] = []
    unknown_dirs: list[str] = []

    for dir_path in skill_dirs:
        dir_name = PurePosixPath(dir_path).name
        if dir_name in known_skills:
            results.append(known_skills[dir_name])  # Cache hit
        else:
            unknown_dirs.append(dir_path)

    if not unknown_dirs:
        return results

    # Try lock file first for all unknown skills (1 download vs N)
    lock_entries: dict[str, Any] | None = None
    try:
        from ptc_agent.agent.middleware.skills.lock import (
            LOCK_FILENAME,
            parse_skills_lock,
            lock_entry_to_skill_metadata,
        )

        lock_path = str(PurePosixPath(source_path) / LOCK_FILENAME)
        lock_responses = await backend.adownload_files([lock_path])
        if lock_responses and not lock_responses[0].error and lock_responses[0].content:
            lock_text = lock_responses[0].content.decode("utf-8")
            lock_entries = parse_skills_lock(lock_text)
    except Exception:
        logger.debug("Could not download skills-lock.json for discovery")

    # Resolve unknown skills: lock entry -> SKILL.md download -> unconfirmed
    to_download: list[tuple[str, str]] = []  # (dir_path, skill_md_path)
    new_lock_entries: dict[str, Any] = {}  # Self-healing entries to write back

    for dir_path in unknown_dirs:
        dir_name = PurePosixPath(dir_path).name
        if lock_entries and dir_name in lock_entries:
            # Lock entry exists — use it directly (no SKILL.md download)
            skill_md_path = str(PurePosixPath(dir_path) / "SKILL.md")
            meta = lock_entry_to_skill_metadata(lock_entries[dir_name], skill_md_path)
            results.append(meta)
        else:
            # No lock entry — fall back to SKILL.md download
            skill_md_path = str(PurePosixPath(dir_path) / "SKILL.md")
            to_download.append((dir_path, skill_md_path))

    # Download SKILL.md for dirs without lock entries
    if to_download:
        paths = [p for _, p in to_download]
        responses = await backend.adownload_files(paths)

        for (dir_path, skill_md_path), response in zip(
            to_download, responses, strict=True
        ):
            directory_name = PurePosixPath(dir_path).name
            if response.error or response.content is None:
                results.append(
                    _make_unconfirmed_metadata(skill_md_path, directory_name)
                )
                continue
            try:
                content = response.content.decode("utf-8")
            except UnicodeDecodeError:
                results.append(
                    _make_unconfirmed_metadata(skill_md_path, directory_name)
                )
                continue
            meta = parse_skill_metadata(content, skill_md_path, directory_name)
            results.append(meta)

            # Self-healing: create lock entry for orphaned skill
            try:
                from ptc_agent.agent.middleware.skills.lock import build_lock_entry

                entry = build_lock_entry(
                    meta,
                    owner="user",
                    source="local",
                    source_type="local",
                    content_hash=f"sha256:{hashlib.sha256(content.encode()).hexdigest()}",
                )
                new_lock_entries[directory_name] = entry
            except Exception:
                pass  # Best-effort self-healing

    # Write back self-healing lock entries if any
    if new_lock_entries:
        try:
            from ptc_agent.agent.middleware.skills.lock import (
                LOCK_FILENAME,
                LOCK_FILE_VERSION,
                serialize_skills_lock,
            )

            all_entries = dict(lock_entries or {})
            all_entries.update(new_lock_entries)
            lock_data = {"version": LOCK_FILE_VERSION, "skills": all_entries}
            lock_content = serialize_skills_lock(lock_data)
            lock_path = str(PurePosixPath(source_path) / LOCK_FILENAME)
            await backend.aupload_files([(lock_path, lock_content.encode("utf-8"))])
            logger.info(
                "Self-healing: wrote lock entries for orphaned skills",
                count=len(new_lock_entries),
                skills=list(new_lock_entries.keys()),
            )
        except Exception:
            logger.debug("Self-healing lock write failed (non-critical)")

    # NOTE: Full lock ↔ filesystem reconciliation (adds + removes) is handled
    # post-turn by reconcile_workspace_skills. The self-healing above is a
    # fallback for the window before that pass runs, or when it failed.

    return results
