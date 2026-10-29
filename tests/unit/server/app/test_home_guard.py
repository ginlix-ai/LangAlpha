"""Home before its first computer: every route that reaches its files or
sandbox refuses it the same way, through one guard."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from src.server.app.share_links import router as share_links_router
from src.server.app.workspace_files import router as files_router
from src.server.app.workspace_sandbox import router as sandbox_router
from src.server.services.workspace_files import (
    HOME_NOT_ON_COMPUTER,
    owned_workspace_on_computer,
)
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

USER = "test-user-123"
WS = "ws-home-guard"
BASE = f"/api/v1/workspaces/{WS}"
_READ = "src.server.services.workspace_files.db_get_workspace"


def _row(**overrides) -> dict:
    return {"workspace_id": WS, "user_id": USER, "status": "running", **overrides}


@pytest.mark.parametrize(
    "row, status, detail",
    [
        (None, 404, "Workspace not found"),
        (_row(user_id="someone-else"), 403, "Forbidden"),
        (_row(status="unbound"), 400, HOME_NOT_ON_COMPUTER),
    ],
    ids=["missing", "another-users", "home-not-on-a-computer"],
)
async def test_the_guard_refuses_as_the_routes_always_have(row, status, detail):
    with patch(_READ, AsyncMock(return_value=row)):
        with pytest.raises(HTTPException) as exc:
            await owned_workspace_on_computer(WS, USER)
    assert (exc.value.status_code, exc.value.detail) == (status, detail)


async def test_the_guard_returns_the_row_it_read():
    row = _row()
    with patch(_READ, AsyncMock(return_value=row)):
        assert await owned_workspace_on_computer(WS, USER) is row


ROUTES = [
    ("GET", "/files/read", {"params": {"path": "a.txt"}}),
    ("PUT", "/files/write", {"params": {"path": "a.txt"}, "json": {"content": "x"}}),
    ("GET", "/files/download", {"params": {"path": "a.txt"}}),
    ("GET", "/files/download-url", {"params": {"path": "a.txt"}}),
    (
        "POST",
        "/files/upload",
        {"params": {"path": "a.txt"}, "files": {"file": ("a.txt", b"x", "text/plain")}},
    ),
    ("POST", "/files/uploads", {"json": {"path": "a.bin", "size": 1}}),
    ("POST", "/files/backup", {}),
    ("DELETE", "/files", {"json": {"paths": ["a.txt"]}}),
    ("GET", "/sandbox/stats", {}),
    ("POST", "/sandbox/packages", {"json": {"packages": ["pandas"]}}),
    ("POST", "/file-grant", {}),
    ("GET", "/share-links", {}),
    ("POST", "/share-links", {"json": {"kind": "file", "path": "a.txt"}}),
]


@pytest.mark.parametrize("method, path, kwargs", ROUTES, ids=[f"{m} {p}" for m, p, _ in ROUTES])
async def test_every_route_that_needs_the_computer_refuses_home_before_it(method, path, kwargs):
    app = create_test_app(files_router, sandbox_router, share_links_router)
    with patch(_READ, AsyncMock(return_value=_row(status="unbound"))):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.request(method, f"{BASE}{path}", **kwargs)
    assert (resp.status_code, resp.json()) == (400, {"detail": HOME_NOT_ON_COMPUTER})
