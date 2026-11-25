"""POST /skills/{name}/save: which workspace it means and what it refuses
before touching the computer."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.app import skills as skills_app
from src.server.database.home_workspace import get_home_stand_in_id, get_home_workspace_id
from src.server.dependencies.principal import _CLI_CALL, CliCall, Principal
from src.server.services.user_skills.reconcile import SkillSaved

USER = "test-user-123"
WS = "ws-1"


@pytest.fixture
def world(monkeypatch):
    state = SimpleNamespace(
        workspace={"workspace_id": WS, "user_id": USER, "status": "running"},
        sandbox=object(),
        save=AsyncMock(return_value=SkillSaved(True, changed=True)),
    )
    state.acquire = AsyncMock(side_effect=lambda *_: state.sandbox)
    state.home_computer = None
    monkeypatch.setattr(skills_app, "db_get_workspace", AsyncMock(side_effect=lambda _: state.workspace))
    monkeypatch.setattr(skills_app, "sandbox_if_running", state.acquire)
    monkeypatch.setattr(
        skills_app,
        "get_computer_for_workspace",
        AsyncMock(side_effect=lambda _: state.home_computer),
    )
    monkeypatch.setattr(
        skills_app.WorkspaceManager,
        "get_instance",
        lambda: SimpleNamespace(save_skill=state.save),
    )
    return state


@pytest.mark.asyncio
async def test_no_workspace_and_no_command_context_is_a_400(skills_client, world):
    resp = await skills_client.post("/api/v1/skills/demo/save")

    assert resp.status_code == 400
    world.acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_name_that_is_not_a_skill_name_is_a_422(skills_client, world):
    resp = await skills_client.post(
        "/api/v1/skills/Bad_Name/save", params={"workspace_id": WS}
    )

    assert resp.status_code == 422
    world.acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_another_users_workspace_is_a_403(skills_client, world):
    world.workspace["user_id"] = "someone-else"

    resp = await skills_client.post("/api/v1/skills/demo/save", params={"workspace_id": WS})

    assert resp.status_code == 403
    world.acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_workspace_that_is_not_running_is_a_409_and_is_not_started(
    skills_client, world
):
    world.sandbox = None

    resp = await skills_client.post("/api/v1/skills/demo/save", params={"workspace_id": WS})

    assert resp.status_code == 409
    world.save.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_saved_skill_is_mirrored_in_the_body(skills_client, world):
    resp = await skills_client.post("/api/v1/skills/demo/save", params={"workspace_id": WS})

    assert resp.status_code == 200
    assert resp.json() == {"name": "demo", "saved": True, "changed": True, "reason": None}
    world.acquire.assert_awaited_once_with(WS, USER)
    world.save.assert_awaited_once_with(WS, USER, world.sandbox, "demo")


@pytest.mark.asyncio
async def test_a_refused_skill_is_a_200_with_the_reason(skills_client, world):
    world.save.return_value = SkillSaved(False, reason="no frontmatter")

    resp = await skills_client.post("/api/v1/skills/demo/save", params={"workspace_id": WS})

    assert resp.status_code == 200
    assert resp.json() == {
        "name": "demo",
        "saved": False,
        "changed": False,
        "reason": "no frontmatter",
    }


@pytest.mark.asyncio
async def test_without_a_workspace_the_calling_commands_workspace_is_saved(world):
    principal = Principal(USER, "computer", computer_id="c-1")
    call = CliCall(principal, "call-0123456789", "t-1", WS)
    request = SimpleNamespace(scope={"state": {_CLI_CALL: call}})

    got = await skills_app.save_workspace_skill_folder("demo", request, USER, None)

    assert got.saved
    world.save.assert_awaited_once_with(WS, USER, world.sandbox, "demo")


def _home_call(computer_id: str) -> tuple[str, SimpleNamespace]:
    home = get_home_workspace_id(USER)
    call = CliCall(Principal(USER, "computer", computer_id=computer_id), "call-0123456789", "t-1", home)
    return home, SimpleNamespace(scope={"state": {_CLI_CALL: call}})


@pytest.mark.asyncio
async def test_a_home_command_on_homes_own_computer_saves_homes_folder(world):
    home, request = _home_call("c-home")
    world.home_computer = {"computer_id": "c-home"}

    await skills_app.save_workspace_skill_folder("demo", request, USER, None)

    world.save.assert_awaited_once_with(home, USER, world.sandbox, "demo")


@pytest.mark.asyncio
@pytest.mark.parametrize("home_computer", [None, {"computer_id": "c-home"}])
async def test_a_home_command_on_another_computer_saves_the_stand_ins_folder(
    world, home_computer
):
    # The command files Home, the workspace its turn works for, while the
    # folder it wrote is the stand-in's, on the computer that ran it.
    _, request = _home_call("c-cloud")
    world.home_computer = home_computer

    await skills_app.save_workspace_skill_folder("demo", request, USER, None)

    stand_in = get_home_stand_in_id(USER)
    world.save.assert_awaited_once_with(stand_in, USER, world.sandbox, "demo")
