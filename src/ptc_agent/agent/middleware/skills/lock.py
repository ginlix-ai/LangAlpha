"""Skills lock file (skills-lock.json) — types and helpers.

The lock file lives at ``.agents/skills/skills-lock.json`` in the sandbox and
tracks ownership, versioning, and parsed metadata for every skill (platform
and user-installed).  Compatible with the vercel-labs/skills lock format with
our extensions (``owner``, ``confirmed``, etc.).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal, NotRequired, TypedDict

import structlog

from ptc_agent.agent.middleware.skills.discovery import SkillMetadata

logger = structlog.get_logger(__name__)

LOCK_FILE_VERSION = 1
LOCK_FILENAME = "skills-lock.json"

# sourceType stamped on user-tier skills the server materializes and syncs.
# ``owner`` says whose *content* a skill is (never destroy user content);
# ``sourceType`` says who owns the *bytes*: server-managed entries have a DB
# source of truth, so the sync may replace or prune them like platform skills.
MANAGED_SOURCE_TYPE = "langalpha-user"


def is_agent_installed(entry: SkillLockEntry) -> bool:
    """Installed by the agent inside the sandbox — no server-side source of
    truth, so the sync may never replace or remove it."""
    return entry.get("owner") == "user" and entry.get("sourceType") != MANAGED_SOURCE_TYPE


def is_linked(entry: SkillLockEntry) -> bool:
    """Two-way synced with a workspace-tier DB row.

    The reconciler is the sole writer of linked entries and their skill dirs;
    the generic upload/prune path must treat them like agent-installed content
    and never touch them.
    """
    sync = entry.get("sync")
    return bool(sync and sync.get("linkedSkillId"))


# --- Types ---


class SkillSyncState(TypedDict, total=False):
    """Two-way sync tracking ref stored on a linked lock entry.

    The git analogy: the sandbox dir is the working copy, the DB row is the
    remote, and this block is the tracking ref — each side is compared only
    against its own recorded base, so no cross-environment hash is needed.
    """

    linkedSkillId: str  # noqa: N815 — user_skills row UUID
    syncedTreeHash: str  # noqa: N815 — sandbox tree hash at last sync
    syncedDbHash: str  # noqa: N815 — row content_hash at last sync
    lastFailedSync: dict[str, Any]  # noqa: N815 — {treeHash, dbHash, kind}
    statCache: dict[str, list]  # noqa: N815 — relpath → [mtime_ns, size, ctime_ns, sha256]


class SkillLockEntry(TypedDict):
    """A single skill entry in the lock file."""

    name: str
    description: str
    owner: Literal["platform", "user"]
    source: str
    sourceType: str  # noqa: N815 — matches vercel-labs/skills JSON key
    computedHash: str  # noqa: N815
    confirmed: bool
    license: str | None
    metadata: dict[str, Any]
    allowed_tools: list[str]
    installedAt: str  # noqa: N815 — ISO 8601
    updatedAt: str  # noqa: N815 — ISO 8601
    sync: NotRequired[SkillSyncState]


class SkillsLockFile(TypedDict):
    """Top-level lock file structure."""

    version: int
    skills: dict[str, SkillLockEntry]


# --- Builders ---


def build_lock_entry(
    meta: SkillMetadata,
    *,
    owner: Literal["platform", "user"] = "platform",
    source: str = "platform",
    source_type: str = "platform",
    content_hash: str = "",
) -> SkillLockEntry:
    """Build a lock entry from parsed SkillMetadata.

    Args:
        meta: Parsed skill metadata from discovery.
        owner: Who installed this skill.
        source: Origin URL or identifier.
        source_type: One of "platform", "github", "local".
        content_hash: SHA-256 hash of SKILL.md content (``sha256:...`` prefix).
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return SkillLockEntry(
        name=meta["name"],
        description=meta["description"],
        owner=owner,
        source=source,
        sourceType=source_type,
        computedHash=content_hash,
        confirmed=meta["confirmed"],
        license=meta.get("license"),
        metadata=dict(meta.get("metadata", {})),
        allowed_tools=list(meta.get("allowed_tools", [])),
        installedAt=now,
        updatedAt=now,
    )


def lock_entry_to_skill_metadata(
    entry: SkillLockEntry, skill_path: str
) -> SkillMetadata:
    """Convert a lock entry back to a SkillMetadata for the discovery cache.

    Args:
        entry: Lock file entry.
        skill_path: Sandbox path to the SKILL.md file.
    """
    return SkillMetadata(
        path=skill_path,
        name=entry["name"],
        description=entry["description"],
        license=entry.get("license"),
        compatibility=None,  # Not stored in lock file
        metadata=dict(entry.get("metadata", {})),
        allowed_tools=list(entry.get("allowed_tools", [])),
        confirmed=entry.get("confirmed", False),
    )


# --- Parsing ---


def parse_skills_lock(content: str) -> dict[str, SkillLockEntry]:
    """Parse a skills-lock.json string into a skill-name → entry dict.

    Returns an empty dict on any error (corrupt JSON, wrong version).
    """
    if not content or not content.strip():
        return {}
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        logger.warning("Corrupt skills-lock.json — returning empty dict")
        return {}

    if not isinstance(data, dict):
        logger.warning("skills-lock.json is not a dict — returning empty dict")
        return {}

    version = data.get("version")
    if version != LOCK_FILE_VERSION:
        logger.warning(
            "skills-lock.json version mismatch: expected %d, got %s",
            LOCK_FILE_VERSION,
            version,
        )
        return {}

    skills = data.get("skills")
    if not isinstance(skills, dict):
        return {}

    return skills


# --- Merging ---


def merge_lock_files(
    authoritative_entries: dict[str, SkillLockEntry],
    existing_lock: dict[str, SkillLockEntry] | None,
) -> SkillsLockFile:
    """Merge server-authoritative entries into an existing lock file.

    Rules:
    - Authoritative entries (platform + server-managed user tier) overwrite
      their existing counterparts.
    - Agent-installed and linked (two-way synced workspace) entries are always
      preserved — the reconciler is the sole writer of linked entries, and the
      physical delivery view keeps workspace names out of the authoritative
      set, so the two can never collide.
    - Stale authoritative entries (present in existing lock but not in
      ``authoritative_entries``) are purged — for a managed user skill this is
      exactly when its row was deleted or disabled in the DB.

    Args:
        authoritative_entries: Current platform + managed skills (authoritative).
        existing_lock: Previously downloaded lock entries, or None for fresh sandbox.

    Returns:
        Complete SkillsLockFile ready to write.
    """
    merged: dict[str, SkillLockEntry] = {}

    # Preserve agent-installed and reconciler-owned skills from existing lock
    if existing_lock:
        for name, entry in existing_lock.items():
            if is_agent_installed(entry) or is_linked(entry):
                merged[name] = entry

    # Add/overwrite all current authoritative entries
    merged.update(authoritative_entries)

    return SkillsLockFile(version=LOCK_FILE_VERSION, skills=merged)


# --- Serialization ---


def serialize_skills_lock(lock: SkillsLockFile) -> str:
    """Serialize a lock file to deterministic JSON (sorted keys, 2-space indent)."""
    return json.dumps(lock, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
