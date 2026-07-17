#!/usr/bin/env python3
"""Generate structured release notes from conventional commits.

Usage:
    python3 scripts/ci/generate_release_notes.py --version v2026.04.02
    python3 scripts/ci/generate_release_notes.py --range v2026.04.01..HEAD --version v2026.04.02
    python3 scripts/ci/generate_release_notes.py --range v2026.04.01..HEAD --version v2026.04.02 --output release_notes.md
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

# Ordered: first match wins display position
CATEGORIES = [
    ("feat", "New Features"),
    ("feature", "New Features"),
    ("fix", "Bug Fixes"),
    ("perf", "Performance"),
    ("refactor", "Improvements"),
    ("docs", "Documentation"),
    ("chore", "Maintenance"),
]

# For the footer summary: (singular, plural)
CATEGORY_LABELS = {
    "New Features": ("feature", "features"),
    "Bug Fixes": ("fix", "fixes"),
    "Performance": ("performance improvement", "performance improvements"),
    "Improvements": ("improvement", "improvements"),
    "Documentation": ("doc change", "doc changes"),
    "Maintenance": ("maintenance change", "maintenance changes"),
    "Other Changes": ("other change", "other changes"),
}

# Pattern: type(scope): message  OR  type: message
CONVENTIONAL_RE = re.compile(
    r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]+)\))?:\s*(?P<message>.+)$"
)


def get_commits(commit_range: str | None) -> list[tuple[str, str]]:
    """Run git log and return (full_sha, subject) pairs.

    First-parent only: a PR's individual commits are implementation detail, so
    the walk yields one entry per PR — the merge commit under merge-commit PRs,
    the squashed commit under squash PRs — plus any direct push to main.
    """
    cmd = ["git", "log", "--first-parent", "--format=%H %s"]
    if commit_range:
        cmd.append(commit_range)
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    commits = []
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        sha, subject = line.split(" ", 1)
        commits.append((sha, subject))
    return commits


def parse_commits(
    commits: list[tuple[str, str]],
) -> dict[str, list[dict[str, str]]]:
    """Parse commits into categorized groups.

    Returns dict mapping category name to list of
    {"scope": str|None, "message": str, "sha": str}.
    """
    groups: dict[str, list[dict[str, str]]] = {}

    for sha, subject in commits:
        short_sha = sha[:7]
        match = CONVENTIONAL_RE.match(subject)

        if match:
            commit_type = match.group("type")
            scope = match.group("scope")
            message = match.group("message")

            category = None
            for prefix, cat_name in CATEGORIES:
                if commit_type == prefix:
                    category = cat_name
                    break

            if category is None:
                category = "Other Changes"
        else:
            category = "Other Changes"
            scope = None
            message = subject

        if category not in groups:
            groups[category] = []
        groups[category].append(
            {"scope": scope, "message": message, "sha": short_sha}
        )

    return groups


def fetch_generated_notes(version: str, previous_tag: str | None) -> str | None:
    """Fetch GitHub's auto-generated notes body via the generate-notes API.

    Requires GITHUB_REPOSITORY and GH_TOKEN/GITHUB_TOKEN env vars; returns
    None when they are absent (e.g. local runs).
    """
    repo = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        return None

    payload: dict[str, str] = {"tag_name": version, "target_commitish": "main"}
    if previous_tag:
        payload["previous_tag_name"] = previous_tag

    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases/generate-notes",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())["body"]


def extract_new_contributors(body: str) -> tuple[list[str], str | None]:
    """Pull the New Contributors bullets and Full Changelog line from a
    generate-notes body."""
    contributors: list[str] = []
    full_changelog = None
    in_section = False
    for line in body.splitlines():
        if line.startswith("## New Contributors"):
            in_section = True
        elif line.startswith("## "):
            in_section = False
        elif line.startswith("**Full Changelog**"):
            in_section = False
            full_changelog = line.strip()
        elif in_section and line.startswith("*"):
            contributors.append(line.strip())
    return contributors, full_changelog


def format_notes(
    parsed: dict[str, list[dict[str, str]]],
    new_contributors: list[str] | None = None,
    full_changelog: str | None = None,
) -> str:
    """Render parsed commits as markdown release notes.

    No H1 header — the release page already shows the tag as its title.
    """
    if not parsed:
        return "No changes since last release.\n"

    lines: list[str] = []

    # Ordered output: follow CATEGORIES order, then "Other Changes" last
    seen_categories: list[str] = []
    for _, cat_name in CATEGORIES:
        if cat_name in parsed and cat_name not in seen_categories:
            seen_categories.append(cat_name)
    if "Other Changes" in parsed:
        seen_categories.append("Other Changes")

    for category in seen_categories:
        entries = parsed[category]
        lines.append(f"## {category}")
        for entry in entries:
            if entry["scope"]:
                lines.append(
                    f"- **{entry['scope']}:** {entry['message']} ({entry['sha']})"
                )
            else:
                lines.append(f"- {entry['message']} ({entry['sha']})")
        lines.append("")

    if new_contributors:
        lines.append("## New Contributors")
        lines.append("")
        lines.append(
            "A warm welcome and thank you to our new contributors this release:"
        )
        lines.append("")
        lines.extend(new_contributors)
        lines.append("")
    if full_changelog:
        lines.append(full_changelog)
        lines.append("")

    # Footer with counts
    lines.append("---")
    count_parts = []
    total = 0
    for category in seen_categories:
        n = len(parsed[category])
        total += n
        singular, plural = CATEGORY_LABELS.get(
            category, (category.lower(), category.lower() + "s")
        )
        count_parts.append(f"{n} {singular if n == 1 else plural}")

    lines.append(f"*{', '.join(count_parts)} — {total} changes total*")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate release notes from conventional commits"
    )
    parser.add_argument(
        "--range",
        dest="commit_range",
        help="Git commit range (e.g. v1.0.0..HEAD). Omit for full history.",
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Version tag of the release being cut (e.g. v2026.04.02)",
    )
    parser.add_argument(
        "--previous-tag",
        help="Previous release tag, used to detect new contributors.",
    )
    parser.add_argument(
        "--output",
        help="Output file path. Prints to stdout if omitted.",
    )
    args = parser.parse_args()

    commits = get_commits(args.commit_range)
    parsed = parse_commits(commits)

    new_contributors: list[str] = []
    full_changelog = None
    try:
        body = fetch_generated_notes(args.version, args.previous_tag)
        if body:
            new_contributors, full_changelog = extract_new_contributors(body)
    except Exception as exc:  # noqa: BLE001 — never fail the release on this
        print(f"warning: could not fetch new contributors: {exc}", file=sys.stderr)

    notes = format_notes(parsed, new_contributors, full_changelog)

    if args.output:
        Path(args.output).write_text(notes)
    else:
        print(notes, end="")


if __name__ == "__main__":
    main()
