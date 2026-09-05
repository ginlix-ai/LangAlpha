"""The shipped workflow tier: a read-only StoreBackend seeded from the repo."""

from __future__ import annotations

import pytest

from ptc_agent.agent.backends import (
    PREBUILT_READ_ONLY_ERROR,
    ReadOnlyStoreError,
    StoreBackend,
    prebuilt_workflow_backend,
)

from .conftest import FakeSandbox


@pytest.fixture
def backend() -> StoreBackend:
    return prebuilt_workflow_backend(
        files={
            "alpha.js": "export const meta = { name: 'alpha' };\nline two",
            "beta.js": "export const meta = { name: 'beta' };",
        },
        root_prefix="/home/workspace/.agents/workflows/",
        sandbox_backend=FakeSandbox(),
    )


@pytest.mark.asyncio
async def test_read_hit_miss_and_range(backend: StoreBackend) -> None:
    path = "/home/workspace/.agents/workflows/alpha.js"
    assert "name: 'alpha'" in await backend.aread_text(path)
    assert await backend.aread_text(
        "/home/workspace/.agents/workflows/missing.js"
    ) is None
    assert await backend.aread_range(path, offset=1, limit=1) == "line two"


@pytest.mark.asyncio
async def test_glob_and_grep_surface(backend: StoreBackend) -> None:
    root = "/home/workspace/.agents/workflows/"
    assert await backend.aglob_paths("*.js", root) == [
        f"{root}alpha.js",
        f"{root}beta.js",
    ]
    assert await backend.aglob_paths("alpha*", root) == [
        f"{root}alpha.js"
    ]
    assert await backend.agrep_rich("alpha", root) == [
        f"{root}alpha.js"
    ]
    content = await backend.agrep_rich("line", root, output_mode="content")
    assert content == [f"{root}alpha.js:2:line two"]


@pytest.mark.asyncio
async def test_relative_mount_root_resolves_under_the_sandbox_root(
    backend: StoreBackend,
) -> None:
    """Every other test passes an absolute path, which takes ``normalize_path``'s
    early return — so nothing else here exercises mount translation. A dot-prefix
    eaten on the way through lands outside the mount and silently finds nothing."""
    root = "/home/workspace/.agents/workflows/"
    assert await backend.aglob_paths("*.js", ".agents/workflows/") == [
        f"{root}alpha.js",
        f"{root}beta.js",
    ]
    assert await backend.agrep_rich("alpha", "./.agents/workflows/") == [
        f"{root}alpha.js"
    ]


@pytest.mark.asyncio
async def test_mutations_are_refused(backend: StoreBackend) -> None:
    path = "/home/workspace/.agents/workflows/alpha.js"
    with pytest.raises(ReadOnlyStoreError, match="Pre-built workflows"):
        await backend.awrite_text(path, "changed")
    assert await backend.aedit_text(path, "alpha", "changed") == {
        "success": False,
        "error": PREBUILT_READ_ONLY_ERROR,
    }
