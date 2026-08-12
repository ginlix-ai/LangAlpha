"""Unit tests for scripts/ci/generate_release_notes.py."""

from __future__ import annotations

import sys
from pathlib import Path


# Add scripts/ci to path so we can import the module
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts" / "ci"))
from generate_release_notes import (
    extract_new_contributors,
    format_notes,
    parse_commits,
)


class TestParseCommits:
    """Test parse_commits with various conventional commit formats."""

    def test_conventional_types(self):
        """All conventional commit types are parsed into correct categories."""
        commits = [
            ("a" * 40, "feat: add login page"),
            ("b" * 40, "fix: resolve null pointer"),
            ("c" * 40, "perf: optimize query"),
            ("d" * 40, "refactor: extract helper"),
            ("e" * 40, "docs: update readme"),
            ("f" * 40, "chore: bump deps"),
        ]
        parsed = parse_commits(commits)
        assert "New Features" in parsed
        assert "Bug Fixes" in parsed
        assert "Performance" in parsed
        assert "Improvements" in parsed
        assert "Documentation" in parsed
        assert "Maintenance" in parsed

    def test_scoped_commits(self):
        """Scoped commits like feat(auth): msg produce scope='auth'."""
        commits = [("a" * 40, "feat(auth): add OAuth support")]
        parsed = parse_commits(commits)
        entry = parsed["New Features"][0]
        assert entry["scope"] == "auth"
        assert entry["message"] == "add OAuth support"

    def test_unscoped_commits(self):
        """Unscoped commits like feat: msg have scope=None."""
        commits = [("a" * 40, "feat: add dark mode")]
        parsed = parse_commits(commits)
        entry = parsed["New Features"][0]
        assert entry["scope"] is None
        assert entry["message"] == "add dark mode"

    def test_non_conventional_commits(self):
        """Non-conventional commits go to 'Other Changes'."""
        commits = [("a" * 40, "Update dependencies")]
        parsed = parse_commits(commits)
        assert "Other Changes" in parsed
        assert parsed["Other Changes"][0]["message"] == "Update dependencies"

    def test_empty_commit_list(self):
        """Empty commit list produces empty dict."""
        parsed = parse_commits([])
        assert parsed == {}

    def test_sha_truncation(self):
        """SHAs are truncated to 7 characters."""
        full_sha = "abcdef1234567890abcdef1234567890abcdef12"
        commits = [(full_sha, "feat: something")]
        parsed = parse_commits(commits)
        assert parsed["New Features"][0]["sha"] == "abcdef1"

    def test_extra_colons_in_message(self):
        """Commit messages with extra colons split on first colon only."""
        commits = [("a" * 40, "feat: foo: bar baz")]
        parsed = parse_commits(commits)
        entry = parsed["New Features"][0]
        assert entry["message"] == "foo: bar baz"

    def test_feature_alias(self):
        """'feature:' is an alias for 'feat:'."""
        commits = [("a" * 40, "feature(ui): new button")]
        parsed = parse_commits(commits)
        assert "New Features" in parsed
        assert parsed["New Features"][0]["scope"] == "ui"


class TestFormatNotes:
    """Test format_notes markdown rendering."""

    def test_empty_parsed_produces_no_changes(self):
        """Empty parsed dict produces 'No changes since last release'."""
        result = format_notes({})
        assert "No changes since last release" in result

    def test_category_display_order(self):
        """Categories appear in the defined display order."""
        parsed = {
            "Bug Fixes": [{"scope": None, "message": "fix it", "sha": "abc1234"}],
            "New Features": [
                {"scope": None, "message": "add it", "sha": "def5678"}
            ],
            "Other Changes": [
                {"scope": None, "message": "misc", "sha": "ghi9012"}
            ],
        }
        result = format_notes(parsed)
        feat_pos = result.index("## New Features")
        fix_pos = result.index("## Bug Fixes")
        other_pos = result.index("## Other Changes")
        assert feat_pos < fix_pos < other_pos

    def test_scoped_entry_formatting(self):
        """Scoped entries render as **scope:** message (sha)."""
        parsed = {
            "New Features": [
                {"scope": "auth", "message": "add login", "sha": "abc1234"}
            ]
        }
        result = format_notes(parsed)
        assert "- **auth:** add login (abc1234)" in result

    def test_unscoped_entry_formatting(self):
        """Unscoped entries render as message (sha) without bold prefix."""
        parsed = {
            "New Features": [
                {"scope": None, "message": "add dark mode", "sha": "abc1234"}
            ]
        }
        result = format_notes(parsed)
        assert "- add dark mode (abc1234)" in result

    def test_footer_counts(self):
        """Footer shows per-category counts and total."""
        parsed = {
            "New Features": [
                {"scope": None, "message": "a", "sha": "1234567"},
                {"scope": None, "message": "b", "sha": "2345678"},
            ],
            "Bug Fixes": [
                {"scope": None, "message": "c", "sha": "3456789"},
            ],
        }
        result = format_notes(parsed)
        assert "3 changes total" in result
        assert "2 features" in result
        assert "1 fix" in result

    def test_footer_pluralization(self):
        """Footer pluralizes correctly (fixes not fixs)."""
        parsed = {
            "Bug Fixes": [
                {"scope": None, "message": "a", "sha": "1234567"},
                {"scope": None, "message": "b", "sha": "2345678"},
            ],
        }
        result = format_notes(parsed)
        assert "2 fixes" in result
        assert "fixs" not in result

    def test_no_h1_header(self):
        """Notes carry no H1 — the release page supplies the title."""
        parsed = {
            "New Features": [
                {"scope": None, "message": "a", "sha": "1234567"}
            ]
        }
        result = format_notes(parsed)
        assert not result.startswith("# ")
        assert result.startswith("## New Features")

    def test_new_contributors_before_footer(self):
        """Contributors section and changelog link render before the footer."""
        parsed = {
            "New Features": [
                {"scope": None, "message": "a", "sha": "1234567"}
            ]
        }
        contributors = ["* @someone made their first contribution in #1"]
        link = "**Full Changelog**: https://example.com/compare/a...b"
        result = format_notes(parsed, contributors, link)
        section_pos = result.index("## New Contributors")
        link_pos = result.index(link)
        footer_pos = result.index("---")
        assert result.index("## New Features") < section_pos < link_pos < footer_pos
        assert contributors[0] in result

    def test_no_contributors_omits_section(self):
        """No contributors and no link → neither appears."""
        parsed = {
            "New Features": [
                {"scope": None, "message": "a", "sha": "1234567"}
            ]
        }
        result = format_notes(parsed)
        assert "New Contributors" not in result
        assert "Full Changelog" not in result


class TestOutputFile:
    """Test file output mode."""

    def test_output_to_file(self, tmp_path):
        """format_notes output can be written to a file."""
        parsed = {
            "New Features": [
                {"scope": None, "message": "a", "sha": "1234567"}
            ]
        }
        content = format_notes(parsed)
        out_file = tmp_path / "notes.md"
        out_file.write_text(content)
        assert out_file.read_text() == content


class TestExtractNewContributors:
    """Test parsing of GitHub generate-notes bodies."""

    BODY = """## What's Changed
* feat: thing by @owner in https://example.com/pull/1
* fix: other by @newbie in https://example.com/pull/2

## New Contributors
* @newbie made their first contribution in https://example.com/pull/2

**Full Changelog**: https://example.com/compare/v1...v2"""

    def test_extracts_contributors_and_link(self):
        contributors, link = extract_new_contributors(self.BODY)
        assert contributors == [
            "* @newbie made their first contribution in https://example.com/pull/2"
        ]
        assert link == "**Full Changelog**: https://example.com/compare/v1...v2"

    def test_body_without_contributors(self):
        """What's Changed bullets are not mistaken for contributors."""
        body = (
            "## What's Changed\n"
            "* feat: thing by @owner in https://example.com/pull/1\n\n"
            "**Full Changelog**: https://example.com/compare/v1...v2"
        )
        contributors, link = extract_new_contributors(body)
        assert contributors == []
        assert link == "**Full Changelog**: https://example.com/compare/v1...v2"
