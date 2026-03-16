from __future__ import annotations

import asyncio

import pytest

from tests.integration.sandbox.conftest import REQUESTED_PROVIDERS

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TestResourceCleanup:
    async def test_delete_makes_sandbox_unretrievable(self, sandbox_provider, provider_name):
        """After delete(), the sandbox should not be retrievable or in a deleted state."""
        runtime = await sandbox_provider.create()
        sid = runtime.id
        await runtime.delete()

        if provider_name == "daytona":
            # Daytona SDK's get() may return a stale reference after deletion.
            # Verify the sandbox is at least in a non-running state.
            retrieved = await sandbox_provider.get(sid)
            state = await retrieved.get_state()
            assert state.value != "running", f"Deleted sandbox still running: {state}"
        else:
            with pytest.raises(Exception):
                await sandbox_provider.get(sid)

    @pytest.mark.skipif(
        "docker" not in REQUESTED_PROVIDERS, reason="Docker only"
    )
    async def test_no_leaked_docker_containers(self):
        """Verify no langalpha-sandbox-docker-* containers remain.

        Allows a brief wait for container teardown from prior test fixtures.
        """
        import subprocess

        # Give fixture teardown a moment to finish container cleanup
        await asyncio.sleep(2)

        result = subprocess.run(
            [
                "docker", "ps", "-a",
                "--filter", "name=langalpha-sandbox-docker",
                "--format", "{{.Names}}",
            ],
            capture_output=True,
            text=True,
        )
        containers = [c for c in result.stdout.strip().split("\n") if c]
        assert len(containers) == 0, f"Leaked containers: {containers}"
