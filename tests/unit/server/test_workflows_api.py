from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends.langgraph_store import StoreBackend
from ptc_agent.agent.backends.workflows import (
    WorkflowsBackend,
    prebuilt_workflow_backend,
    workflow_namespace,
)
from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    PrebuiltWorkflowRegistry,
)
import src.config.settings as settings_mod
from src.config.models import WorkflowOrchestrationConfig
from src.server.app import setup as setup_mod
from src.server.app import workflows as workflows_mod
from src.server.app.workflows import (
    WorkflowWriteRequest,
    delete_workflow,
    get_workflow,
    include_workflow_router,
    list_workflows,
    put_workflow,
)

USER_ID = "user-1"
NAMESPACE = workflow_namespace(USER_ID)
MOUNT_ROOT = "/home/workspace/.agents/workflows/"
# Synthetic builtins: the shipped set is deliberately empty, and these tests are
# about the merge rules rather than about whatever happens to ship.
BUILTIN_SHADOWED = "shared-name"
BUILTIN_ONLY = "builtin-only"


def _script(name: str, description: str = "description") -> str:
    return (
        f"export const meta = {{ name: '{name}', description: '{description}' }};\n"
        "return args;"
    )


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> InMemoryStore:
    value = InMemoryStore()
    monkeypatch.setattr(setup_mod, "store", value, raising=False)
    for name in (BUILTIN_SHADOWED, BUILTIN_ONLY):
        directory = tmp_path / "workflows" / name
        directory.mkdir(parents=True)
        (directory / "workflow.js").write_text(_script(name, "shipped"))
    registry = PrebuiltWorkflowRegistry(tmp_path)
    monkeypatch.setattr(workflows_mod, "get_prebuilt_workflows", lambda: registry)
    return value


async def _seed(store: InMemoryStore, name: str, content: str) -> None:
    await store.aput(
        NAMESPACE,
        f"{name}.js",
        {
            "content": content,
            "encoding": "utf-8",
            "created_at": "2026-01-01T00:00:00Z",
            "modified_at": "2026-01-01T00:00:00Z",
        },
    )


@pytest.fixture
def agent_mount(store: InMemoryStore) -> WorkflowsBackend:
    """The other writer of these rows — the agent's `.agents/workflows/` mount."""
    sandbox = MagicMock()
    sandbox.normalize_path.side_effect = lambda p: (
        p if p.startswith("/") else f"/home/workspace/{p}"
    )
    sandbox.virtualize_path.side_effect = lambda p: p
    sandbox.validate_path.return_value = True
    return WorkflowsBackend(
        store_backend=StoreBackend(
            store=store,
            namespace_factory=lambda: workflow_namespace(USER_ID),
            root_prefix=MOUNT_ROOT,
            sandbox_backend=sandbox,
        ),
        prebuilt_backend=prebuilt_workflow_backend(
            files={}, root_prefix=MOUNT_ROOT, sandbox_backend=sandbox
        ),
    )


@pytest.mark.asyncio
async def test_list_merges_users_builtins_and_shadowing(store: InMemoryStore) -> None:
    await _seed(store, BUILTIN_SHADOWED, _script(BUILTIN_SHADOWED, "mine"))
    await _seed(store, "broken", "not valid JavaScript {")
    # A row with no readable script at all: invalid, not a crash and not a
    # silent fall-through to the builtin of the same name.
    await store.aput(NAMESPACE, "shapeless.js", {"encoding": "utf-8"})

    entries = await list_workflows(user_id=USER_ID)
    by_name = {entry.name: entry for entry in entries}

    assert by_name[BUILTIN_SHADOWED].builtin is False
    assert by_name[BUILTIN_SHADOWED].shadows_builtin is True
    assert by_name[BUILTIN_SHADOWED].description == "mine"
    assert by_name[BUILTIN_ONLY].builtin is True
    assert by_name["broken"].valid is False
    assert by_name["shapeless"].valid is False
    assert len([entry for entry in entries if entry.name == BUILTIN_SHADOWED]) == 1


@pytest.mark.asyncio
async def test_rest_and_the_agent_mount_write_rows_each_other_can_read(
    store: InMemoryStore, agent_mount: WorkflowsBackend
) -> None:
    """Two doors write this tier. They stay interchangeable only while the
    namespace, the key and the stored envelope have one owner between them."""
    await put_workflow(
        "via-rest",
        WorkflowWriteRequest(content=_script("via-rest")),
        user_id=USER_ID,
    )
    assert await agent_mount.aread_text(f"{MOUNT_ROOT}via-rest.js") == _script(
        "via-rest"
    )

    written = await agent_mount.awrite_text(
        f"{MOUNT_ROOT}via-mount.js", _script("via-mount")
    )
    assert written is True
    read_back = await get_workflow("via-mount", user_id=USER_ID)
    assert read_back.builtin is False
    assert read_back.content == _script("via-mount")
    assert read_back.description == "description"


@pytest.mark.asyncio
async def test_get_user_builtin_and_missing(store: InMemoryStore) -> None:
    await _seed(store, BUILTIN_SHADOWED, _script(BUILTIN_SHADOWED, "mine"))

    user = await get_workflow(BUILTIN_SHADOWED, user_id=USER_ID)
    builtin = await get_workflow(BUILTIN_ONLY, user_id=USER_ID)

    assert user.builtin is False
    assert user.description == "mine"
    assert builtin.builtin is True
    with pytest.raises(HTTPException) as exc:
        await get_workflow("missing", user_id=USER_ID)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_get_and_delete_reject_invalid_names(store: InMemoryStore) -> None:
    for name in ("..%2Fetc", "bad name!", ""):
        with pytest.raises(HTTPException) as exc:
            await get_workflow(name, user_id=USER_ID)
        assert exc.value.status_code == 400
        with pytest.raises(HTTPException) as exc:
            await delete_workflow(name, user_id=USER_ID)
        assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_put_valid_workflow_preserves_created_at(store: InMemoryStore) -> None:
    await _seed(store, "demo", _script("demo", "old"))

    result = await put_workflow(
        "demo",
        WorkflowWriteRequest(content=_script("demo", "new")),
        user_id=USER_ID,
    )

    assert result.description == "new"
    item = await store.aget(NAMESPACE, "demo.js")
    assert item.value["created_at"] == "2026-01-01T00:00:00Z"
    assert item.value["modified_at"] != item.value["created_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "content", "message"),
    [
        ("bad name", _script("bad-name"), "name must match"),
        ("expected", _script("different"), "must match workflow name"),
        ("syntax", "export const meta = {", "Invalid workflow script"),
    ],
)
async def test_put_rejects_invalid_workflows(
    store: InMemoryStore, name: str, content: str, message: str
) -> None:
    with pytest.raises(HTTPException) as exc:
        await put_workflow(
            name,
            WorkflowWriteRequest(content=content),
            user_id=USER_ID,
        )
    assert exc.value.status_code == 400
    assert message in exc.value.detail


@pytest.mark.asyncio
async def test_put_rejects_oversized_workflow(
    store: InMemoryStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Patches the config source rather than this router's re-export: the cap
    is resolved by ``workflow_script_byte_cap``, which reads it there."""
    monkeypatch.setattr(
        settings_mod,
        "get_workflow_orchestration_config",
        lambda: WorkflowOrchestrationConfig().model_copy(
            update={"max_script_bytes": 100}
        ),
    )
    with pytest.raises(HTTPException) as exc:
        await put_workflow(
            "large",
            WorkflowWriteRequest(content=_script("large") + "x" * 200),
            user_id=USER_ID,
        )
    assert exc.value.status_code == 400
    assert "max is 100" in exc.value.detail


@pytest.mark.asyncio
async def test_list_skips_rows_the_router_cannot_address(
    store: InMemoryStore,
) -> None:
    """A key holding a '/' predates the write-time guard and is unreachable:
    every route for it 400s or 404s. Offering the name would only mislead."""
    await store.aput(
        NAMESPACE,
        "nested/stranded.js",
        {
            "content": _script("stranded"),
            "encoding": "utf-8",
            "created_at": "2026-01-01T00:00:00Z",
            "modified_at": "2026-01-01T00:00:00Z",
        },
    )
    await _seed(store, "reachable", _script("reachable"))

    names = {entry.name for entry in await list_workflows(user_id=USER_ID)}

    assert "reachable" in names
    assert not any("/" in name for name in names)


@pytest.mark.asyncio
async def test_delete_user_and_reject_builtin_only(store: InMemoryStore) -> None:
    await _seed(store, "demo", _script("demo"))
    response = await delete_workflow("demo", user_id=USER_ID)
    assert response.status_code == 204
    assert await store.aget(NAMESPACE, "demo.js") is None

    with pytest.raises(HTTPException) as exc:
        await delete_workflow(BUILTIN_ONLY, user_id=USER_ID)
    assert exc.value.status_code == 404
    assert "cannot be deleted" in exc.value.detail


@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.asyncio
async def test_router_is_registered_only_when_enabled(
    monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    """Both arms, because a bare ``FastAPI()`` 404s on every path — the disabled
    case alone passes just as well with ``include_workflow_router`` deleted.

    Only mounted-or-not is asserted. Which rejection a mounted route answers with
    depends on process-wide auth and store state this test does not own, so
    pinning the specific code makes the test pass or fail on what ran before it.
    """
    monkeypatch.setattr(
        workflows_mod,
        "get_workflow_orchestration_config",
        lambda: SimpleNamespace(enabled=enabled),
    )
    app = FastAPI()
    include_workflow_router(app)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v1/workflows/")
    if enabled:
        assert response.status_code != 404
    else:
        assert response.status_code == 404


@pytest.mark.asyncio
async def test_listing_bounds_the_compiles_a_single_request_can_spend(
    store: InMemoryStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The agent mount writes scripts this router never compiled, so a
    namespace can hold more uncompiled rows than the memo holds entries —
    without a bound every listing recompiles all of them on the shared
    executor. Past the budget a row is reported without a verdict, which is
    not the same as reporting it invalid.
    """
    from src.server.app import workflows as workflows_router

    monkeypatch.setattr(workflows_router, "_MAX_LIST_COMPILES", 2)
    names = [f"row{index}" for index in range(5)]
    for name in names:
        await _seed(store, name, _script(name, f"description {name}"))

    entries = {entry.name: entry for entry in await list_workflows(user_id=USER_ID)}
    described = [entries[name] for name in names if entries[name].valid is True]
    unchecked = [entries[name] for name in names if entries[name].valid is None]

    assert len(described) == 2
    assert len(unchecked) == 3
    # Unchecked is silent, not wrong: no row is branded invalid for having
    # arrived after the budget ran out.
    assert all(entry.valid is not False for entry in entries.values() if entry.name in names)
    assert all(entry.description is None for entry in unchecked)

    # A second listing is free for everything the first one compiled.
    again = {entry.name: entry for entry in await list_workflows(user_id=USER_ID)}
    assert len([e for e in again.values() if e.name in names and e.valid is True]) >= 4
