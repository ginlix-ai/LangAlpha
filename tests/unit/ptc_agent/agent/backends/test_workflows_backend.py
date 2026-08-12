"""Write-time guards on the workflow mount.

The mount is the only door that creates rows in the user's workflow tier, and
every row it creates has to stay reachable from ``/api/v1/workflows/{name}``.
"""

from __future__ import annotations

import asyncio

from unittest.mock import MagicMock

import pytest
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends.langgraph_store import (
    MAX_CONTENT_BYTES,
    InvalidStoreKeyError,
    StoreBackend,
    StoreContentTooLargeError,
)
from ptc_agent.agent.backends.workflows import (
    WorkflowsBackend,
    prebuilt_workflow_backend,
    workflow_name_from_key,
    workflow_script_byte_cap,
)
from src.config.models import WorkflowOrchestrationConfig

WORKING_DIR = "/home/workspace"
WORKFLOW_PREFIX = f"{WORKING_DIR}/.agents/workflows/"
NAMESPACE = ("user_abc", "workflows")


@pytest.fixture
def sandbox():
    sb = MagicMock()
    sb.normalize_path.side_effect = lambda p: (
        p if p.startswith("/") else f"{WORKING_DIR}/{p}"
    )
    sb.virtualize_path.side_effect = lambda p: p
    sb.validate_path.return_value = True
    return sb


@pytest.fixture
def store():
    return InMemoryStore()


@pytest.fixture
def overlay(sandbox, store):
    return WorkflowsBackend(
        store_backend=StoreBackend(
            store=store,
            namespace_factory=lambda: NAMESPACE,
            root_prefix=WORKFLOW_PREFIX,
            sandbox_backend=sandbox,
        ),
        prebuilt_backend=prebuilt_workflow_backend(
            files={}, root_prefix=WORKFLOW_PREFIX, sandbox_backend=sandbox
        ),
    )


class TestAddressableNames:
    """A row the REST surface cannot name again can never be deleted."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "key",
        [
            "nested/flow.js",          # the permanent one: `{name}` never matches a '/'
            "a/b/c.js",
            "-leading-dash.js",
            "has space.js",
            "flow.txt",
            "flow",
            ".js",
            "x" * 65 + ".js",
        ],
    )
    async def test_unaddressable_name_is_refused_before_the_row_exists(
        self, overlay, store, key
    ):
        with pytest.raises(InvalidStoreKeyError, match="workflow file name"):
            await overlay.awrite_text(WORKFLOW_PREFIX + key, "export const meta = {}")
        assert await store.asearch(NAMESPACE) == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["flow", "my-flow_2", "A1", "x" * 64])
    async def test_addressable_name_is_accepted(self, overlay, store, name):
        assert await overlay.awrite_text(f"{WORKFLOW_PREFIX}{name}.js", "body") is True
        assert [item.key for item in await store.asearch(NAMESPACE)] == [f"{name}.js"]

    def test_name_from_key_rejects_what_the_router_cannot_address(self):
        assert workflow_name_from_key("flow.js") == "flow"
        assert workflow_name_from_key("nested/flow.js") is None
        assert workflow_name_from_key("flow.txt") is None


class TestScriptByteCap:
    @pytest.mark.asyncio
    async def test_write_over_the_cap_is_refused(self, overlay, store):
        cap = workflow_script_byte_cap()
        with pytest.raises(StoreContentTooLargeError, match=f"max is {cap}"):
            await overlay.awrite_text(WORKFLOW_PREFIX + "big.js", "x" * (cap + 1))
        assert await store.asearch(NAMESPACE) == []

    @pytest.mark.asyncio
    async def test_an_edit_cannot_grow_a_script_past_the_cap(self, overlay):
        """The store's own value cap is the looser of the two, so an edit left
        to it lands a saved workflow every later run refuses as oversized."""
        cap = workflow_script_byte_cap()
        assert cap < MAX_CONTENT_BYTES
        path = f"{WORKFLOW_PREFIX}big.js"
        seed = "// seed\nGROW"
        assert await overlay.awrite_text(path, seed) is True

        result = await overlay.aedit_text(path, "GROW", "y" * (cap + 4096))

        assert result["success"] is False
        assert f"past {cap} bytes" in result["error"]
        assert await overlay.aread_text(path) == seed

    def test_the_tighter_cap_wins_when_the_script_cap_is_raised_above_it(
        self, monkeypatch
    ):
        """Configuring ``max_script_bytes`` above the store's own value cap used
        to leave the two disagreeing about what was storable."""
        import src.config.settings as settings

        monkeypatch.setattr(
            settings,
            "get_workflow_orchestration_config",
            lambda: WorkflowOrchestrationConfig().model_copy(
                update={"max_script_bytes": MAX_CONTENT_BYTES * 4}
            ),
        )
        assert workflow_script_byte_cap() == MAX_CONTENT_BYTES

    def test_the_script_cap_wins_when_it_is_the_tighter_one(self, monkeypatch):
        import src.config.settings as settings

        monkeypatch.setattr(
            settings,
            "get_workflow_orchestration_config",
            lambda: WorkflowOrchestrationConfig().model_copy(
                update={"max_script_bytes": 4096}
            ),
        )
        assert workflow_script_byte_cap() == 4096


class TestUnreadableIsNotAbsent:
    """A store that cannot answer must not be read as "the user saved nothing".

    Read falls through to the shipped tier while write always lands in the
    user tier, so guessing "absent" under store pressure serves the builtin
    and lets an ordinary read-modify-write replace the user's own script.
    """

    @staticmethod
    def _stalled_overlay(sandbox, store, prebuilt: dict[str, str]):
        class StalledStore(InMemoryStore):
            async def asearch(self, *args, **kwargs):  # noqa: ANN002, ANN003
                await asyncio.sleep(3600)

        return WorkflowsBackend(
            store_backend=StoreBackend(
                store=StalledStore(),
                namespace_factory=lambda: NAMESPACE,
                root_prefix=WORKFLOW_PREFIX,
                sandbox_backend=sandbox,
            ),
            prebuilt_backend=prebuilt_workflow_backend(
                files=prebuilt, root_prefix=WORKFLOW_PREFIX, sandbox_backend=sandbox
            ),
        )

    @pytest.mark.asyncio
    async def test_a_stalled_listing_refuses_rather_than_serving_the_prebuilt(
        self, sandbox, store, monkeypatch
    ):
        import ptc_agent.agent.backends.langgraph_store as store_module

        monkeypatch.setattr(store_module, "_STORE_OP_TIMEOUT_S", 0.05)
        overlay = self._stalled_overlay(
            sandbox, store, {"flow.js": "export const meta = { name: 'shipped' }"}
        )

        assert await overlay.aread_text(WORKFLOW_PREFIX + "flow.js") is None
        assert await overlay.aread_range(WORKFLOW_PREFIX + "flow.js") is None

    @pytest.mark.asyncio
    async def test_a_completed_listing_still_falls_through(self, overlay, sandbox):
        """The guard only fires on an unreadable listing — an empty user tier
        that answered is still proof there is no shadow."""
        served = WorkflowsBackend(
            store_backend=StoreBackend(
                store=InMemoryStore(),
                namespace_factory=lambda: NAMESPACE,
                root_prefix=WORKFLOW_PREFIX,
                sandbox_backend=sandbox,
            ),
            prebuilt_backend=prebuilt_workflow_backend(
                files={"flow.js": "shipped body"},
                root_prefix=WORKFLOW_PREFIX,
                sandbox_backend=sandbox,
            ),
        )
        assert await served.aread_text(WORKFLOW_PREFIX + "flow.js") == "shipped body"
