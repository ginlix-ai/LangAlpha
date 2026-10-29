from __future__ import annotations

import pytest

from ptc_agent.core.sandbox.runtime import RuntimeState

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


class TestWorkspaceSetup:
    """setup_sandbox_workspace() creates the runtime and directory skeleton."""

    async def test_setup_creates_runtime(self, shared_sandbox):
        assert shared_sandbox.runtime is not None
        assert shared_sandbox.sandbox_id is not None
        state = await shared_sandbox.runtime.get_state()
        assert state == RuntimeState.RUNNING

    async def test_setup_creates_directories(self, shared_sandbox):
        """Verify the computer runtime and workspace directory skeletons."""
        expected_dirs = [
            "_internal/tools",
            ".agents/tools/docs",
            "data",
            ".system/code",
            ".agents/memory",
            ".agents/threads",
            ".agents/skills",
            "_internal/src",
        ]
        for d in expected_dirs:
            result = await shared_sandbox.runtime.exec(f"test -d {shared_sandbox._work_dir}/{d} && echo EXISTS")
            assert "EXISTS" in result.stdout, f"Directory {d} was not created"

    async def test_setup_idempotent_structure(self, shared_sandbox):
        """Calling _setup_workspace again should not fail."""
        await shared_sandbox._setup_workspace()
        result = await shared_sandbox.runtime.exec(f"test -d {shared_sandbox._work_dir}/_internal/tools && echo OK")
        assert "OK" in result.stdout
