"""The file panel serves the user's data files through the agent's own routes.

A data file's path reads through its route class, which renders the rows as
the agent reads them and names the panel's refusals. The panel never writes or
deletes one, since that would skip the route's schema and version checks. An
automation's file is there only while its row is, so the panel lists and
resolves the ones the user has. Every other path, a README or a file under the
other route's folder included, falls through to the sandbox.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from ptc_agent.agent.backends.automations import AutomationsBackend
from ptc_agent.agent.backends.user_data import UserDataBackend
from ptc_agent.agent.filesystem_routes import route_for
from src.server.app.workspace_files import crud
from src.server.app.workspace_files._shared import (
    _present_virtual_file,
    _virtual_file,
    _virtual_files_in_scope,
)
from src.server.services import user_data_io
from src.server.services.automations import file as automations_file
from tests.unit.server.services.automations.file._support import _row

USER = "user-fake-1"
WORKSPACE_ID = "00000000-0000-4000-8000-00000000aaaa"
WORKSPACE = {
    "workspace_id": WORKSPACE_ID,
    "user_id": USER,
    "status": "running",
    "config": None,
    "sandbox_id": "sb-fake",
}
ROOT = "/home/workspace"
AUTOMATIONS = ".agents/user/automations/morning-brief.json"
PORTFOLIO = ".agents/user/profile/portfolio.json"
PROFILE = [
    PORTFOLIO,
    ".agents/user/profile/watchlist.json",
    ".agents/user/profile/preference.json",
    ".agents/user/profile/user.json",
]
CATALOG = {**dict.fromkeys(PROFILE, UserDataBackend), AUTOMATIONS: AutomationsBackend}


@pytest.fixture
def filed(monkeypatch) -> list[str]:
    """The user's automations' file names; the test sets which."""
    names: list[str] = ["morning-brief.json"]
    monkeypatch.setattr(
        automations_file.auto_db, "list_automation_file_names", AsyncMock(side_effect=lambda _user: list(names))
    )
    return names


def test_each_data_file_maps_to_the_route_that_serves_it():
    assert {path: route_for(path) for path in CATALOG} == CATALOG
    assert route_for(".agents/user/automations/any-name.json") is AutomationsBackend


@pytest.mark.parametrize(
    "path",
    [
        ".agents/user/profile/README.md",
        ".agents/user/automations/README.md",
        ".agents/user/automations/notes.txt",
        ".agents/user/automations/brief.json.tmp",
        ".agents/user/profile/automations.json",
        ".agents/user/automations/sub/brief.json",
        ".agents/user/profile",
        "portfolio.json",
    ],
)
def test_no_other_path_has_a_route(path):
    assert route_for(path) is None
    assert _virtual_file(path) is None


@pytest.mark.parametrize(
    ("scope", "listed"),
    [
        ("", [*PROFILE, AUTOMATIONS]),
        (".agents/user", [*PROFILE, AUTOMATIONS]),
        (".agents/user/automations", [AUTOMATIONS]),
        (AUTOMATIONS, [AUTOMATIONS]),
        ("research", []),
    ],
)
@pytest.mark.asyncio
async def test_a_listing_shows_the_automations_the_user_has(filed, scope, listed):
    assert sorted(await _virtual_files_in_scope(scope, USER)) == sorted(listed)


@pytest.mark.asyncio
async def test_a_listing_that_cannot_read_the_names_still_shows_the_rest(monkeypatch):
    monkeypatch.setattr(
        automations_file.auto_db, "list_automation_file_names", AsyncMock(side_effect=RuntimeError("database down"))
    )

    assert sorted(await _virtual_files_in_scope("", USER)) == sorted(PROFILE)


@pytest.mark.asyncio
async def test_a_reference_resolves_to_an_automation_only_while_it_is_there(filed):
    assert await _present_virtual_file(AUTOMATIONS, USER)
    assert not await _present_virtual_file(".agents/user/automations/gone.json", USER)
    assert await _present_virtual_file(PORTFOLIO, USER)
    assert not await _present_virtual_file("notes.md", USER)


@pytest.mark.asyncio
async def test_a_reference_the_names_cannot_be_read_for_resolves_to_its_route(monkeypatch):
    """The read that follows says what failed, which a not-found would not."""
    monkeypatch.setattr(
        automations_file.auto_db, "list_automation_file_names", AsyncMock(side_effect=RuntimeError("database down"))
    )

    assert await _present_virtual_file(AUTOMATIONS, USER)


@pytest.fixture
def workspace(monkeypatch):
    monkeypatch.setattr(crud, "db_get_workspace", AsyncMock(return_value=WORKSPACE))
    monkeypatch.setattr(crud, "owner_work_dir", lambda _ws: ROOT)
    monkeypatch.setattr(crud, "owner_layout", lambda _ws: SimpleNamespace(workspace=ROOT))


async def _read(path: str) -> dict:
    return await crud.read_workspace_file(
        workspace_id=WORKSPACE_ID, x_user_id=USER, path=path, offset=0, limit=100, unlimited=True
    )


@pytest.mark.parametrize("status", ["running", "stopped"])
@pytest.mark.asyncio
async def test_the_panel_lists_the_data_files_running_or_stopped(workspace, filed, monkeypatch, status):
    """A stopped workspace lists its stored copy, which a backup keeps no data
    file in, and a row an older backup left at one of their paths is not what a
    read serves: a deleted automation there would list and then 404."""
    monkeypatch.setattr(crud, "db_get_workspace", AsyncMock(return_value={**WORKSPACE, "status": status}))
    sandbox = MagicMock()
    sandbox.is_ready.return_value = True
    sandbox.aglob_files = AsyncMock(return_value=[f"{ROOT}/notes.md"])

    @asynccontextmanager
    async def _acquire(*_):
        yield sandbox, WORKSPACE

    monkeypatch.setattr(crud, "_acquire_sandbox_to_change", _acquire)
    monkeypatch.setattr(crud, "contained_sandbox_path", AsyncMock(return_value=ROOT))
    stored = ["notes.md", AUTOMATIONS, ".agents/user/automations/gone.json"]
    monkeypatch.setattr(
        crud.FilePersistenceService, "get_file_tree", AsyncMock(return_value=[{"path": p} for p in stored])
    )

    result = await crud.list_workspace_files(
        workspace_id=WORKSPACE_ID,
        x_user_id=USER,
        path=".",
        include_system=False,
        pattern="**/*",
        wait_for_sandbox=False,
        auto_start=False,
    )

    assert sorted(result["files"]) == sorted(["notes.md", *PROFILE, AUTOMATIONS])


@pytest.mark.asyncio
async def test_the_panel_reads_an_automation_as_the_agent_does(workspace, monkeypatch):
    row = _row()
    fetch = AsyncMock(return_value=row)
    monkeypatch.setattr(automations_file.auto_db, "get_automation_file", fetch)

    result = await _read(AUTOMATIONS)

    assert (result["content"], result["source"]) == (automations_file._render(row)[0], "automations_backend")
    fetch.assert_awaited_once_with(USER, "morning-brief.json", conn=None)


@pytest.mark.asyncio
async def test_the_panel_finds_no_automation_under_a_name_none_has(workspace, monkeypatch):
    monkeypatch.setattr(automations_file.auto_db, "get_automation_file", AsyncMock(return_value=None))

    with pytest.raises(HTTPException) as exc:
        await _read(".agents/user/automations/gone.json")

    assert (exc.value.status_code, exc.value.detail) == (404, "File not found")


@pytest.mark.asyncio
async def test_the_panel_reads_a_profile_file_as_the_agent_does(workspace, monkeypatch):
    fetch = AsyncMock(return_value=[])
    monkeypatch.setattr(user_data_io, "fetch_portfolio_for_user", fetch)

    result = await _read(PORTFOLIO)

    assert (result["content"], result["source"]) == ('{\n  "holdings": []\n}', "user_data_backend")
    fetch.assert_awaited_once_with(USER)


@pytest.mark.parametrize(("path", "route"), [(AUTOMATIONS, AutomationsBackend), (PORTFOLIO, UserDataBackend)])
@pytest.mark.asyncio
async def test_a_failed_read_answers_with_its_routes_message(workspace, monkeypatch, path, route):
    down = AsyncMock(side_effect=RuntimeError("database down"))
    monkeypatch.setattr(automations_file.auto_db, "get_automation_file", down)
    monkeypatch.setattr(user_data_io, "fetch_portfolio_for_user", down)

    with pytest.raises(HTTPException) as exc:
        await _read(path)

    assert (exc.value.status_code, exc.value.detail) == (500, route.read_failure)


@pytest.fixture
def held_elsewhere(monkeypatch):
    """A data file's rows are the only place it is: the sandbox and the stored
    copy are never asked for one, running or stopped."""

    def _status(status: str) -> None:
        monkeypatch.setattr(crud, "db_get_workspace", AsyncMock(return_value={**WORKSPACE, "status": status}))

    @asynccontextmanager
    async def _acquire(*_):
        raise AssertionError("a data file reached the sandbox")
        yield

    unreachable = AsyncMock(side_effect=AssertionError("a data file reached the stored copy"))
    monkeypatch.setattr(crud, "_acquire_sandbox_to_change", _acquire)
    monkeypatch.setattr(crud.FilePersistenceService, "get_file_content", unreachable)
    monkeypatch.setattr(crud, "mirror_download_link", unreachable)
    return _status


async def _download(path: str):
    return await crud.download_workspace_file(
        workspace_id=WORKSPACE_ID, x_user_id=USER, request=SimpleNamespace(headers={}), path=path, attachment=True
    )


@pytest.mark.parametrize("status", ["running", "stopped"])
@pytest.mark.parametrize("path", [AUTOMATIONS, PORTFOLIO])
@pytest.mark.asyncio
async def test_the_panel_downloads_a_data_file_as_the_agent_reads_it(
    workspace, held_elsewhere, monkeypatch, path, status
):
    held_elsewhere(status)
    row = _row()
    monkeypatch.setattr(automations_file.auto_db, "get_automation_file", AsyncMock(return_value=row))
    monkeypatch.setattr(user_data_io, "fetch_portfolio_for_user", AsyncMock(return_value=[]))
    rendered = {AUTOMATIONS: automations_file._render(row)[0], PORTFOLIO: '{\n  "holdings": []\n}'}

    response = await _download(path)

    assert response.body == rendered[path].encode()
    assert response.media_type == "application/json"
    assert response.headers["content-disposition"].startswith("attachment;")
    assert path.rsplit("/", 1)[-1] in response.headers["content-disposition"]


@pytest.mark.asyncio
async def test_the_panel_downloads_no_automation_under_a_name_none_has(workspace, held_elsewhere, monkeypatch):
    held_elsewhere("running")
    monkeypatch.setattr(automations_file.auto_db, "get_automation_file", AsyncMock(return_value=None))

    with pytest.raises(HTTPException) as exc:
        await _download(".agents/user/automations/gone.json")

    assert (exc.value.status_code, exc.value.detail) == (404, "File not found")


@pytest.mark.parametrize("status", ["running", "stopped"])
@pytest.mark.parametrize("path", list(CATALOG))
@pytest.mark.asyncio
async def test_a_data_file_has_no_download_link(workspace, held_elsewhere, path, status):
    """The client falls back to /files/download, which renders the rows."""
    held_elsewhere(status)

    result = await crud.workspace_file_download_url(workspace_id=WORKSPACE_ID, x_user_id=USER, path=path)

    assert result == {"url": None}


@pytest.mark.parametrize(("path", "route"), list(CATALOG.items()))
@pytest.mark.asyncio
async def test_the_panel_never_writes_a_data_file(workspace, path, route):
    with pytest.raises(HTTPException) as exc:
        await crud.write_workspace_file(
            workspace_id=WORKSPACE_ID, x_user_id=USER, path=path, body=crud.WriteFileRequest(content="{}")
        )

    assert (exc.value.status_code, exc.value.detail) == (400, route.read_only)


@pytest.mark.asyncio
async def test_the_panel_never_deletes_a_data_file(workspace, monkeypatch):
    sandbox = MagicMock()
    sandbox.validate_path.return_value = True
    sandbox.execute_bash_command = AsyncMock(side_effect=AssertionError("a data file reached rm"))

    @asynccontextmanager
    async def _acquire(*_):
        yield sandbox, WORKSPACE

    monkeypatch.setattr(crud, "_acquire_sandbox_to_change", _acquire)
    monkeypatch.setattr(
        crud, "contained_sandbox_paths", AsyncMock(side_effect=lambda _sb, paths, work_dir: paths)
    )

    result = await crud.delete_workspace_files(
        workspace_id=WORKSPACE_ID, x_user_id=USER, body=crud.DeleteFilesRequest(paths=list(CATALOG))
    )

    assert result == {
        "deleted": [],
        "errors": [{"path": path, "detail": route.undeletable} for path, route in CATALOG.items()],
    }
