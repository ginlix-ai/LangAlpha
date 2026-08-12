"""Tests for ptc_agent.agent.middleware.skills.lock module."""

import json


from ptc_agent.agent.middleware.skills.discovery import SkillMetadata
from ptc_agent.agent.middleware.skills.lock import (
    LOCK_FILE_VERSION,
    build_lock_entry,
    lock_entry_to_skill_metadata,
    merge_lock_files,
    parse_skills_lock,
    serialize_skills_lock,
)


def _make_skill_metadata(name: str = "test-skill", **overrides) -> SkillMetadata:
    base = SkillMetadata(
        path=f"/home/workspace/.agents/skills/{name}/SKILL.md",
        name=name,
        description=f"Test skill {name}",
        license="MIT",
        compatibility=None,
        metadata={"author": "test", "version": "1.0.0"},
        allowed_tools=["Read"],
        confirmed=True,
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# build_lock_entry
# ---------------------------------------------------------------------------


class TestBuildLockEntry:
    def test_build_lock_entry_platform(self):
        meta = _make_skill_metadata("analytics")
        entry = build_lock_entry(
            meta,
            owner="platform",
            source="platform",
            source_type="platform",
            content_hash="sha256:abc123",
        )

        assert entry["name"] == "analytics"
        assert entry["description"] == "Test skill analytics"
        assert entry["owner"] == "platform"
        assert entry["source"] == "platform"
        assert entry["sourceType"] == "platform"
        assert entry["computedHash"] == "sha256:abc123"
        assert entry["confirmed"] is True
        assert entry["license"] == "MIT"
        assert entry["metadata"] == {"author": "test", "version": "1.0.0"}
        assert entry["allowed_tools"] == ["Read"]
        # Timestamps are ISO 8601 UTC strings
        assert entry["installedAt"].endswith("Z")
        assert entry["updatedAt"].endswith("Z")
        assert entry["installedAt"] == entry["updatedAt"]

    def test_build_lock_entry_user(self):
        meta = _make_skill_metadata("my-custom-skill")
        entry = build_lock_entry(
            meta,
            owner="user",
            source="https://github.com/user/skill-repo",
            source_type="github",
            content_hash="sha256:def456",
        )

        assert entry["owner"] == "user"
        assert entry["source"] == "https://github.com/user/skill-repo"
        assert entry["sourceType"] == "github"
        assert entry["computedHash"] == "sha256:def456"
        assert entry["name"] == "my-custom-skill"

    def test_build_lock_entry_defaults(self):
        """Default kwargs produce a platform entry with empty hash."""
        meta = _make_skill_metadata("default-skill")
        entry = build_lock_entry(meta)

        assert entry["owner"] == "platform"
        assert entry["source"] == "platform"
        assert entry["sourceType"] == "platform"
        assert entry["computedHash"] == ""

    def test_build_lock_entry_missing_optional_fields(self):
        """Metadata with no license or metadata dict still works."""
        meta = _make_skill_metadata(
            "bare-skill", license=None, metadata={}, allowed_tools=[]
        )
        entry = build_lock_entry(meta, owner="platform", content_hash="sha256:000")

        assert entry["license"] is None
        assert entry["metadata"] == {}
        assert entry["allowed_tools"] == []


# ---------------------------------------------------------------------------
# parse_skills_lock
# ---------------------------------------------------------------------------


class TestParseSkillsLock:
    def test_parse_skills_lock_valid(self):
        skill_entry = {
            "name": "research",
            "description": "Research skill",
            "owner": "platform",
            "source": "platform",
            "sourceType": "platform",
            "computedHash": "sha256:aaa",
            "confirmed": True,
            "license": "MIT",
            "metadata": {},
            "allowed_tools": ["Read", "Bash"],
            "installedAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z",
        }
        lock_json = json.dumps(
            {"version": LOCK_FILE_VERSION, "skills": {"research": skill_entry}}
        )

        result = parse_skills_lock(lock_json)

        assert "research" in result
        assert result["research"]["name"] == "research"
        assert result["research"]["owner"] == "platform"
        assert result["research"]["allowed_tools"] == ["Read", "Bash"]

    def test_parse_skills_lock_corrupt_json(self):
        result = parse_skills_lock("{this is not valid json!!!")
        assert result == {}

    def test_parse_skills_lock_wrong_version(self):
        lock_json = json.dumps({"version": 99, "skills": {}})
        result = parse_skills_lock(lock_json)
        assert result == {}

    def test_parse_skills_lock_empty_string(self):
        assert parse_skills_lock("") == {}

    def test_parse_skills_lock_whitespace_only(self):
        assert parse_skills_lock("   \n  ") == {}

    def test_parse_skills_lock_not_a_dict(self):
        result = parse_skills_lock(json.dumps([1, 2, 3]))
        assert result == {}

    def test_parse_skills_lock_skills_not_a_dict(self):
        lock_json = json.dumps({"version": LOCK_FILE_VERSION, "skills": "bad"})
        result = parse_skills_lock(lock_json)
        assert result == {}

    def test_parse_skills_lock_missing_skills_key(self):
        lock_json = json.dumps({"version": LOCK_FILE_VERSION})
        result = parse_skills_lock(lock_json)
        assert result == {}


# ---------------------------------------------------------------------------
# lock_entry_to_skill_metadata
# ---------------------------------------------------------------------------


class TestLockEntryToSkillMetadata:
    def test_lock_entry_to_skill_metadata(self):
        """Round-trip: SkillMetadata -> lock entry -> SkillMetadata."""
        original_meta = _make_skill_metadata("round-trip")
        entry = build_lock_entry(
            original_meta,
            owner="platform",
            source="platform",
            source_type="platform",
            content_hash="sha256:rtrip",
        )

        skill_path = "/sandbox/.agents/skills/round-trip/SKILL.md"
        restored = lock_entry_to_skill_metadata(entry, skill_path)

        assert restored["path"] == skill_path
        assert restored["name"] == "round-trip"
        assert restored["description"] == original_meta["description"]
        assert restored["license"] == original_meta["license"]
        assert restored["compatibility"] is None  # Not stored in lock
        assert restored["metadata"] == original_meta["metadata"]
        assert restored["allowed_tools"] == original_meta["allowed_tools"]
        assert restored["confirmed"] is True

    def test_lock_entry_to_skill_metadata_unconfirmed(self):
        meta = _make_skill_metadata("unconf", confirmed=False)
        entry = build_lock_entry(meta, owner="user", content_hash="sha256:x")

        restored = lock_entry_to_skill_metadata(entry, "/some/path")
        assert restored["confirmed"] is False

    def test_lock_entry_to_skill_metadata_copies_collections(self):
        """Returned metadata/allowed_tools are new list/dict instances."""
        meta = _make_skill_metadata("copy-test")
        entry = build_lock_entry(meta, content_hash="sha256:copy")

        restored = lock_entry_to_skill_metadata(entry, "/path")

        assert restored["metadata"] == entry["metadata"]
        assert restored["metadata"] is not entry["metadata"]
        assert restored["allowed_tools"] == entry["allowed_tools"]
        assert restored["allowed_tools"] is not entry["allowed_tools"]


# ---------------------------------------------------------------------------
# merge_lock_files
# ---------------------------------------------------------------------------


def _make_lock_entry(
    name: str, owner: str = "platform", **overrides
) -> dict:
    """Convenience builder for test lock entries."""
    base = {
        "name": name,
        "description": f"Skill {name}",
        "owner": owner,
        "source": owner,
        "sourceType": owner,
        "computedHash": f"sha256:{name}",
        "confirmed": True,
        "license": None,
        "metadata": {},
        "allowed_tools": [],
        "installedAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


class TestMergeLockFiles:
    def test_merge_preserves_user_entries(self):
        user_entry = _make_lock_entry("my-custom", owner="user")
        platform_entry = _make_lock_entry("analytics", owner="platform")

        existing = {"my-custom": user_entry, "analytics": platform_entry}
        new_platform = {"analytics": _make_lock_entry("analytics", owner="platform")}

        result = merge_lock_files(new_platform, existing)

        assert result["version"] == LOCK_FILE_VERSION
        assert "my-custom" in result["skills"]
        assert result["skills"]["my-custom"]["owner"] == "user"

    def test_merge_overwrites_platform_entries(self):
        old_platform = _make_lock_entry(
            "analytics", owner="platform", description="Old description"
        )
        new_platform = _make_lock_entry(
            "analytics", owner="platform", description="New description"
        )

        existing = {"analytics": old_platform}
        result = merge_lock_files({"analytics": new_platform}, existing)

        assert result["skills"]["analytics"]["description"] == "New description"

    def test_merge_adds_new_platform_alongside_user(self):
        user_entry = _make_lock_entry("my-skill", owner="user")
        new_platform_entry = _make_lock_entry("brand-new", owner="platform")

        existing = {"my-skill": user_entry}
        result = merge_lock_files({"brand-new": new_platform_entry}, existing)

        assert "my-skill" in result["skills"]
        assert result["skills"]["my-skill"]["owner"] == "user"
        assert "brand-new" in result["skills"]
        assert result["skills"]["brand-new"]["owner"] == "platform"

    def test_merge_empty_existing_lock(self):
        platform_a = _make_lock_entry("skill-a", owner="platform")
        platform_b = _make_lock_entry("skill-b", owner="platform")

        result = merge_lock_files(
            {"skill-a": platform_a, "skill-b": platform_b}, None
        )

        assert result["version"] == LOCK_FILE_VERSION
        assert len(result["skills"]) == 2
        assert "skill-a" in result["skills"]
        assert "skill-b" in result["skills"]

    def test_merge_purges_removed_platform_skills(self):
        user_entry = _make_lock_entry("user-plugin", owner="user")
        old_platform = _make_lock_entry("deprecated-skill", owner="platform")
        kept_platform = _make_lock_entry("kept-skill", owner="platform")

        existing = {
            "user-plugin": user_entry,
            "deprecated-skill": old_platform,
            "kept-skill": kept_platform,
        }
        # Platform no longer ships "deprecated-skill"
        current_platform = {"kept-skill": _make_lock_entry("kept-skill", owner="platform")}
        result = merge_lock_files(current_platform, existing)

        assert "user-plugin" in result["skills"], "User entry must survive"
        assert "kept-skill" in result["skills"], "Active platform entry must survive"
        assert "deprecated-skill" not in result["skills"], (
            "Removed platform entry must be purged"
        )

    def test_merge_empty_platform_entries_preserves_user(self):
        """If platform ships zero skills, user entries still survive."""
        user_entry = _make_lock_entry("user-only", owner="user")
        existing = {"user-only": user_entry}

        result = merge_lock_files({}, existing)

        assert len(result["skills"]) == 1
        assert result["skills"]["user-only"]["owner"] == "user"

    def test_merge_both_empty(self):
        result = merge_lock_files({}, None)
        assert result == {"version": LOCK_FILE_VERSION, "skills": {}}


# ---------------------------------------------------------------------------
# serialize_skills_lock
# ---------------------------------------------------------------------------


class TestSerializeSkillsLock:
    def test_serialize_sorted_keys(self):
        lock = {
            "version": LOCK_FILE_VERSION,
            "skills": {
                "zebra-skill": _make_lock_entry("zebra-skill"),
                "alpha-skill": _make_lock_entry("alpha-skill"),
            },
        }
        output = serialize_skills_lock(lock)

        # Must be valid JSON
        parsed = json.loads(output)
        assert parsed["version"] == LOCK_FILE_VERSION
        assert len(parsed["skills"]) == 2

        # Keys must be sorted in the output string
        zebra_pos = output.index('"zebra-skill"')
        alpha_pos = output.index('"alpha-skill"')
        assert alpha_pos < zebra_pos, "Keys must be sorted alphabetically"

        # Must end with trailing newline
        assert output.endswith("\n")

        # Must use 2-space indent
        assert "\n  " in output
        assert "\t" not in output

    def test_serialize_deterministic(self):
        """Same input always produces identical output."""
        lock = {
            "version": LOCK_FILE_VERSION,
            "skills": {
                "b-skill": _make_lock_entry("b-skill"),
                "a-skill": _make_lock_entry("a-skill"),
            },
        }
        first = serialize_skills_lock(lock)
        second = serialize_skills_lock(lock)
        assert first == second

    def test_serialize_roundtrip(self):
        """Serialized output can be parsed back."""
        meta = _make_skill_metadata("roundtrip-ser")
        entry = build_lock_entry(meta, content_hash="sha256:rt")
        lock = merge_lock_files({"roundtrip-ser": entry}, None)

        serialized = serialize_skills_lock(lock)
        parsed_skills = parse_skills_lock(serialized)

        assert "roundtrip-ser" in parsed_skills
        assert parsed_skills["roundtrip-ser"]["name"] == "roundtrip-ser"
        assert parsed_skills["roundtrip-ser"]["computedHash"] == "sha256:rt"
