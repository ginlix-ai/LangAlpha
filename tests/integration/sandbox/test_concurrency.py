from __future__ import annotations
import asyncio
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]


class TestConcurrentSandboxes:
    async def test_parallel_create(self, sandbox_provider, timed):
        """Create 3 sandboxes concurrently, verify unique IDs."""
        runtimes = []
        try:
            async with timed("concurrency", "parallel_create"):
                results = await asyncio.gather(
                    *[sandbox_provider.create() for _ in range(3)],
                    return_exceptions=True,
                )
            runtimes = [r for r in results if not isinstance(r, Exception)]
            assert len({r.id for r in runtimes}) == 3
        finally:
            await asyncio.gather(
                *[r.delete() for r in runtimes], return_exceptions=True
            )

    async def test_parallel_exec(self, sandbox_runtime, timed):
        """5 concurrent exec commands on same sandbox."""
        async with timed("concurrency", "parallel_exec"):
            results = await asyncio.gather(
                *[sandbox_runtime.exec(f"echo {i}") for i in range(5)]
            )
        assert all(r.exit_code == 0 for r in results)

    async def test_concurrent_file_io(self, sandbox_runtime, timed):
        """Upload + download 10 files concurrently."""
        wd = await sandbox_runtime.fetch_working_dir()
        files = [
            (f"content_{i}".encode(), f"{wd}/concurrent_{i}.txt")
            for i in range(10)
        ]
        async with timed("concurrency", "parallel_upload"):
            await asyncio.gather(
                *[sandbox_runtime.upload_file(c, p) for c, p in files]
            )
        async with timed("concurrency", "parallel_download"):
            results = await asyncio.gather(
                *[sandbox_runtime.download_file(p) for _, p in files]
            )
        for i, data in enumerate(results):
            assert data == f"content_{i}".encode()
