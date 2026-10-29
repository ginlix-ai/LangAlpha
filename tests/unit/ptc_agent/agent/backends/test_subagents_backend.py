"""``SubagentsBackend``: the folder lists main.md and one file per subagent, and main.md and the README take no change."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ptc_agent.agent.backends import ReadOnlyStoreError
from ptc_agent.agent.backends.db_json_route import UserDataValidationError
from ptc_agent.agent.backends.subagents import README_FILE, SubagentsBackend
from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import BaseSubagent
from ptc_agent.core.sandbox.livefs_mount import CallContext
from tests.unit.server.services.subagents._fakes import THREAD, USER, install, views

pytestmark = pytest.mark.asyncio

ROOT = "/home/workspace/.agents/user/agents/"
MAIN = f"{ROOT}main.md"
README = f"{ROOT}{README_FILE}"


@pytest.fixture
def world(monkeypatch):
    world = install(monkeypatch)
    world.custom("alpha")
    world.override("ghost", base_key="builtin:ghost", enabled=False)
    # A deployment may name a subagent no file name can carry.
    world.bases["Deep_Research"] = BaseSubagent("deployment:Deep_Research", SubagentDefinition(
        name="Deep_Research", description="Digs deep.", role_prompt="Dig.", tools=["web_search"], source="user",
    ))
    world.offered += ("Deep_Research",)
    return world


@pytest.fixture
def backend(world) -> SubagentsBackend:
    return SubagentsBackend(user_id=USER, call=CallContext(thread_id=THREAD), sandbox_backend=MagicMock(), root_prefix=ROOT)


async def test_the_folder_lists_main_and_each_subagent_with_a_file(world, backend):
    """An orphan's settings are the pane's to delete, so they get no file to
    edit, and a name no file can carry gets none either."""
    listed = {entry["name"]: entry["writable"] for entry in await backend.alist(ROOT)}

    assert listed == {
        README_FILE: False, "main.md": False, "alpha.md": True, "data-prep.md": True, "research.md": True,
    }
    assert (await views(world))["Deep_Research"]["file"] is None


async def test_main_shows_what_the_agent_runs_on(backend):
    content, _ = await backend.aread_versioned(MAIN)

    assert content.startswith("---\nmodel: model-a\neffort: medium\n")


@pytest.mark.parametrize("path", [MAIN, README])
async def test_main_and_the_readme_take_no_change(backend, path):
    # Unread first: a write the agent sends without a Read takes the refusal
    # too, not the path that makes a new file.
    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_text(path, "---\nmodel: model-flat\n---\n")
    assert exc.value.error_type == "schema_error"
    before = await backend.aread_versioned(path)

    with pytest.raises(UserDataValidationError) as exc:
        await backend.awrite_versioned(path, "---\nmodel: model-flat\n---\n", before[1])
    assert exc.value.error_type == "schema_error"
    assert (await backend.aedit_text(path, "\n", "\n\n"))["success"] is False
    with pytest.raises(ReadOnlyStoreError):
        await backend.adelete_versioned(path)
    with pytest.raises(ReadOnlyStoreError):
        await backend.arename_versioned(path, f"{ROOT}moved.md")
    assert backend.is_writable(path) is False
    assert await backend.aread_versioned(path) == before
