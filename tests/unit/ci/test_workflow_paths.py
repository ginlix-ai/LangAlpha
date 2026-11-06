"""The Daytona tier's path filter is written twice: once for same-repo PRs and
pushes, once for forks. A drift would quietly stop Daytona running for a change
that can reach it, so the two lists are held equal here."""

from pathlib import Path

import yaml

_WORKFLOWS = Path(__file__).resolve().parents[3] / ".github" / "workflows"


def _paths(workflow: str, event: str) -> set[str]:
    doc = yaml.safe_load((_WORKFLOWS / workflow).read_text())
    # PyYAML reads the bare key `on` as True.
    triggers = doc.get("on", doc.get(True))
    return set(triggers[event]["paths"])


def _shared(paths: set[str]) -> set[str]:
    # Each workflow lists its own file, and only the same-repo one posts the
    # PR comments its scripts render.
    return {p for p in paths if not p.startswith((".github/workflows/", "scripts/ci/"))}


def test_pushes_to_main_run_daytona_for_the_same_paths_as_prs():
    pr = _paths("sandbox-integration.yml", "pull_request")
    assert _paths("sandbox-integration.yml", "push") == pr


def test_forks_run_daytona_for_the_same_paths_as_same_repo_prs():
    same_repo = _shared(_paths("sandbox-integration.yml", "pull_request"))
    fork = _shared(_paths("fork-integration.yml", "pull_request_target"))
    assert fork == same_repo
