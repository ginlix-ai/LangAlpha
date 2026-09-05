"""Config tests run against no bundled plugins unless they ask for some.

``create_mcp_config`` composes the shipped MCP servers out of ``plugins/``
before it reads a line of YAML, so a test about the YAML half would otherwise
assert against whatever the repo happens to ship that week — and, worse, an
assertion on ``servers[0]`` would pass by landing on a bundled server instead
of the one the test declared. A test that wants the composition points
``BUNDLES_DIR`` somewhere itself; its own monkeypatch runs after this one.
"""

import pytest


@pytest.fixture(autouse=True)
def _no_bundled_plugins(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "ptc_agent.config.plugins.BUNDLES_DIR", tmp_path / "no-bundles"
    )
