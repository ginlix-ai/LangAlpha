"""One workflow mount: shipped scripts overlaid by the user's own tier."""

from __future__ import annotations

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends import (
    StoreBackend,
    WorkflowsBackend,
    prebuilt_workflow_backend,
)

from .conftest import FakeSandbox

ROOT = "/home/workspace/.agents/workflows/"


class RefusingStore(InMemoryStore):
    """A store whose writes never land — the case the fork path used to hide."""

    async def aput(self, *args, **kwargs) -> None:
        raise RuntimeError("store unavailable")


@pytest.fixture
def overlay() -> WorkflowsBackend:
    return _overlay(InMemoryStore())


def _overlay(user_store: InMemoryStore) -> WorkflowsBackend:
    sandbox = FakeSandbox()
    return WorkflowsBackend(
        store_backend=StoreBackend(
            store=user_store,
            namespace_factory=lambda: ("user-1", "workflows"),
            root_prefix=ROOT,
            sandbox_backend=sandbox,
        ),
        prebuilt_backend=prebuilt_workflow_backend(
            files={
                "shipped.js": "const meta = { name: 'shipped' };\nreturn 1;",
                "other.js": "const meta = { name: 'other' };",
            },
            root_prefix=ROOT,
            sandbox_backend=sandbox,
        ),
    )


@pytest.mark.asyncio
async def test_prebuilt_readable_until_a_save_shadows_it(overlay) -> None:
    path = f"{ROOT}shipped.js"
    assert "name: 'shipped'" in await overlay.aread_text(path)

    await overlay.awrite_text(path, "const meta = { name: 'shipped' };\nreturn 2;")

    # The user copy wins everywhere, and the name appears once, not twice.
    assert "return 2;" in await overlay.aread_text(path)
    assert await overlay.aglob_paths("*.js", ROOT) == [
        f"{ROOT}other.js",
        f"{ROOT}shipped.js",
    ]


@pytest.mark.asyncio
async def test_editing_a_prebuilt_forks_it_into_the_user_tier(overlay) -> None:
    """The move the split mounts made impossible: edit in place, keep running
    under the same name."""
    path = f"{ROOT}shipped.js"

    result = await overlay.aedit_text(path, "return 1;", "return 42;")

    # Reported exactly like a user-tier edit — the tool logs `occurrences` and
    # returns `message`, so a fork that reported its own shape would go blank.
    assert result == {"success": True, "occurrences": 1, "message": f"Edited {path}"}
    assert "return 42;" in await overlay.aread_text(path)
    # The shipped script itself is untouched for anyone without a fork.
    assert "return 1;" in await overlay._prebuilt.aread_text(path)


@pytest.mark.asyncio
async def test_a_fork_that_cannot_persist_is_not_reported_as_edited() -> None:
    """The failure the hand-rolled fork swallowed: a store write that never
    persists must not come back as a successful edit."""
    overlay = _overlay(RefusingStore())

    result = await overlay.aedit_text(f"{ROOT}shipped.js", "return 1;", "return 42;")

    assert result["success"] is False


@pytest.mark.asyncio
async def test_failed_edit_of_a_prebuilt_writes_nothing(overlay) -> None:
    path = f"{ROOT}shipped.js"

    result = await overlay.aedit_text(path, "not-present", "x")

    assert result["success"] is False
    assert await overlay._store.aread_text(path) is None


@pytest.mark.asyncio
async def test_grep_reports_the_shadowing_copy_once(overlay) -> None:
    path = f"{ROOT}shipped.js"
    await overlay.awrite_text(path, "const meta = { name: 'shipped' };\nreturn 7;")

    hits = await overlay.agrep_rich("meta", ROOT)

    assert sorted(hits) == [f"{ROOT}other.js", f"{ROOT}shipped.js"]
    content = await overlay.agrep_rich("return", ROOT, output_mode="content")
    assert content == [f"{path}:2:return 7;"]


@pytest.mark.asyncio
async def test_mismatched_roots_are_rejected() -> None:
    sandbox = FakeSandbox()
    with pytest.raises(ValueError, match="one root_prefix"):
        WorkflowsBackend(
            store_backend=StoreBackend(
                store=InMemoryStore(),
                namespace_factory=lambda: ("user-1", "workflows"),
                root_prefix=ROOT,
                sandbox_backend=sandbox,
            ),
            prebuilt_backend=prebuilt_workflow_backend(
                files={},
                root_prefix="/home/workspace/.agents/elsewhere/",
                sandbox_backend=sandbox,
            ),
        )


@pytest.mark.asyncio
async def test_an_unreadable_saved_row_never_resolves_to_the_shipped_script() -> None:
    """`StoreBackend` answers unreadable and absent with the same `None`, so a
    straight fall-through hands back the shipped script of the same name.

    The sharp cost is not the wrong run: read falls through to the built-in
    while write always lands in the user tier, so an ordinary
    read-modify-write replaces the user's workflow with a derivative of ours.
    """
    store = InMemoryStore()
    # An envelope carrying no `content` string — what `aread_text` reports
    # as absent and `workflow_script_from_value` rejects outright.
    store.put(("user-1", "workflows"), "shipped.js", {"encoding": "utf-8"})
    overlay = _overlay(store)

    assert await overlay.aread_text(f"{ROOT}shipped.js") is None
    assert await overlay.aread_range(f"{ROOT}shipped.js") is None


@pytest.mark.asyncio
async def test_a_path_the_user_never_saved_still_reads_the_shipped_script(
    overlay: WorkflowsBackend,
) -> None:
    """The shadow check must not swallow the overlay's whole reason to exist."""
    assert "shipped" in (await overlay.aread_text(f"{ROOT}shipped.js") or "")
    assert "other" in (await overlay.aread_range(f"{ROOT}other.js") or "")
