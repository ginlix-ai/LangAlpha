"""Contract lock: vendored Agent Plugins schemas vs the canonical URLs.

The validators run against vendored copies (never fetched at runtime), so
upstream edits to the published 1.0.0 schemas would silently diverge from
what this deployment enforces. This opt-in check diffs the vendored bytes
against the live canonical documents.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.integration

SCHEMA_DIR = (
    Path(__file__).parents[2]
    / "src" / "server" / "services" / "plugins" / "schemas"
)

CANONICAL = [
    (
        "plugin.schema.1.0.0.json",
        "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
    ),
    (
        "mcp.schema.1.0.0.json",
        "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
    ),
]


@pytest.mark.parametrize(("filename", "url"), CANONICAL)
def test_vendored_schema_matches_canonical(filename: str, url: str):
    response = httpx.get(url, follow_redirects=True, timeout=30)
    response.raise_for_status()
    vendored = (SCHEMA_DIR / filename).read_bytes()
    assert vendored == response.content, (
        f"{filename} has drifted from {url}; re-vendor the canonical bytes "
        f"and re-run the conformance suite before shipping"
    )
