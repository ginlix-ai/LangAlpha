"""Tests for the Glob tool's result cap.

A recursive glob over a project with a dependency tree can match tens of
thousands of files; without a cap the single tool message can exceed the model
context window. These pin down the cap and the truncation banner. Directory
exclusion (node_modules, .git, …) happens in the sandbox backend and is covered
by the integration suite.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from ptc_agent.agent.tools.glob import GLOB_MATCH_LIMIT, create_glob_tool


def _make_backend(matches: list[str]) -> Any:
    """Build a minimal backend stub the Glob tool will accept.

    Only the surface the tool uses is mocked: `normalize_path`,
    `virtualize_path`, `validate_path`, `filesystem_config`, `aglob_paths`.
    """
    backend = SimpleNamespace()
    backend.normalize_path = lambda p: p
    backend.virtualize_path = lambda p: p
    backend.validate_path = lambda p: True
    backend.filesystem_config = SimpleNamespace(enable_path_validation=False)
    backend.aglob_paths = AsyncMock(return_value=matches)
    return backend


class TestGlobCap:
    @pytest.mark.asyncio
    async def test_result_capped_and_banner_present(self):
        total = GLOB_MATCH_LIMIT + 500
        backend = _make_backend([f"/w/f{i}.py" for i in range(total)])
        glob = create_glob_tool(backend)

        result = await glob.ainvoke({"pattern": "**/*.py"})

        # Header reports the true total, not the capped count.
        assert f"Found {total} file(s)" in result
        # Only GLOB_MATCH_LIMIT paths are listed.
        listed = [ln for ln in result.splitlines() if ln.startswith("/w/f")]
        assert len(listed) == GLOB_MATCH_LIMIT
        # Truncation banner tells the agent how to narrow.
        assert f"showing first {GLOB_MATCH_LIMIT} of {total}" in result
        assert "narrow the pattern" in result

    @pytest.mark.asyncio
    async def test_under_cap_has_no_banner(self):
        backend = _make_backend([f"/w/f{i}.py" for i in range(3)])
        glob = create_glob_tool(backend)

        result = await glob.ainvoke({"pattern": "**/*.py"})

        assert "Found 3 file(s)" in result
        assert "showing first" not in result
        listed = [ln for ln in result.splitlines() if ln.startswith("/w/f")]
        assert len(listed) == 3

    @pytest.mark.asyncio
    async def test_no_matches_message(self):
        backend = _make_backend([])
        glob = create_glob_tool(backend)

        result = await glob.ainvoke({"pattern": "**/*.py"})

        assert "No files matching" in result
