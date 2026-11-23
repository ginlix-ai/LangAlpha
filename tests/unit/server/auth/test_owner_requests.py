"""With an owner secret set, only a request carrying it is an open-source server's owner."""

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import Depends, FastAPI, WebSocketException
from fastapi.testclient import TestClient

from src.server.auth import owner_requests
from src.server.auth.jwt_bearer import get_current_auth_info, verify_jwt_token
from src.server.auth.ws_auth import authenticate_websocket
from src.server.utils.api import get_current_user_id, get_optional_user_id

OWNER = "local-dev-user"
SECRET = "s3cret"
FROM_APP = {"X-LangAlpha-Owner": SECRET}


def _client() -> TestClient:
    app = FastAPI()

    @app.get("/user")
    async def user(user_id: str = Depends(get_current_user_id)):
        return user_id

    @app.get("/jwt")
    async def jwt(user_id: str = Depends(verify_jwt_token)):
        return user_id

    @app.get("/info")
    async def info(auth=Depends(get_current_auth_info)):
        return auth.user_id

    @app.get("/optional")
    async def optional(user_id=Depends(get_optional_user_id)):
        return user_id

    return TestClient(app)


@pytest.fixture
def oss():
    with ExitStack() as stack:
        stack.enter_context(patch("src.server.utils.api.HOST_MODE", "oss"))
        stack.enter_context(patch("src.server.utils.api.LOCAL_DEV_USER_ID", OWNER))
        stack.enter_context(patch("src.server.auth.jwt_bearer.HOST_MODE", "oss"))
        stack.enter_context(patch("src.server.auth.jwt_bearer.LOCAL_DEV_USER_ID", OWNER))
        stack.enter_context(patch("src.server.utils.api.admit_caller", AsyncMock()))
        yield


@pytest.mark.parametrize("path", ["/user", "/jwt", "/info"])
def test_the_owner_fallback_answers_only_the_app(oss, monkeypatch, path):
    monkeypatch.setenv(owner_requests.OWNER_SECRET_ENV, SECRET)
    client = _client()
    assert client.get(path, headers=FROM_APP).json() == OWNER
    for headers in ({}, {"X-LangAlpha-Owner": "guess"}, {"Host": "127.0.0.1"}):
        refused = client.get(path, headers=headers)
        assert refused.status_code == 401
        assert refused.json() == {"detail": owner_requests.REFUSAL}


@pytest.mark.parametrize("path", ["/user", "/jwt", "/info"])
def test_with_no_secret_set_every_request_is_the_owner(oss, monkeypatch, path):
    monkeypatch.delenv(owner_requests.OWNER_SECRET_ENV, raising=False)
    monkeypatch.delenv("LANGALPHA_DESKTOP_APP", raising=False)
    assert _client().get(path).json() == OWNER


def test_the_app_s_server_without_its_secret_answers_no_one(oss, monkeypatch):
    monkeypatch.delenv(owner_requests.OWNER_SECRET_ENV, raising=False)
    monkeypatch.setenv("LANGALPHA_DESKTOP_APP", "1")
    refused = _client().get("/user")
    assert refused.status_code == 401


def test_an_optional_caller_without_the_secret_is_anonymous(oss, monkeypatch):
    monkeypatch.setenv(owner_requests.OWNER_SECRET_ENV, SECRET)
    client = _client()
    assert client.get("/optional", headers=FROM_APP).json() == OWNER
    assert client.get("/optional").json() is None


@pytest.mark.asyncio
async def test_a_websocket_without_the_secret_is_closed(monkeypatch):
    monkeypatch.setenv(owner_requests.OWNER_SECRET_ENV, SECRET)
    socket = SimpleNamespace(scope={"headers": []}, close=AsyncMock())
    with patch("src.server.auth.ws_auth.HOST_MODE", "oss"), pytest.raises(WebSocketException):
        await authenticate_websocket(socket)
    socket.close.assert_awaited_once()

    socket = SimpleNamespace(scope={"headers": [(b"x-langalpha-owner", SECRET.encode())]})
    with patch("src.server.auth.ws_auth.HOST_MODE", "oss"), patch(
        "src.server.auth.ws_auth.LOCAL_DEV_USER_ID", OWNER
    ):
        assert await authenticate_websocket(socket) == OWNER
