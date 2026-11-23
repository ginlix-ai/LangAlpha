"""A web page must not reach a server that can run code on its own machine."""

import re

import pytest

from src.server.app.loopback_guard import LoopbackOriginGuard

_ROUTES = re.compile(r"/api/v1/artifacts/g/")


async def _ask(
    *,
    host: str = "127.0.0.1:9000",
    origin: str | None = None,
    method: str = "POST",
    path: str = "/api/v1/threads/messages",
    kind: str = "http",
) -> tuple[bool, list[dict]]:
    reached: list[bool] = []
    sent: list[dict] = []

    async def app(scope, receive, send):
        reached.append(True)

    async def send(message):
        sent.append(message)

    headers = [(b"host", host.encode())]
    if origin is not None:
        headers.append((b"origin", origin.encode()))
    scope = {
        "type": kind,
        "scheme": "ws" if kind == "websocket" else "http",
        "method": method,
        "path": path,
        "headers": headers,
    }
    guard = LoopbackOriginGuard(
        app, app_origins=("http://localhost:5173/",), opaque_origin_routes=_ROUTES
    )
    await guard(scope, None, send)
    return bool(reached), sent


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_",
    [
        {},
        {"origin": "http://127.0.0.1:9000"},
        {"host": "localhost:9000", "origin": "http://localhost:9000"},
        {"origin": "http://localhost:5173"},
        {"host": "[::1]:9000"},
        {"origin": "null", "method": "GET", "path": "/api/v1/artifacts/g/abc"},
        {"kind": "websocket", "origin": "http://127.0.0.1:9000"},
        # A Docker computer dialing the host: no browser in it, so no Origin.
        {"host": "host.docker.internal:9000"},
        {"host": "host.docker.internal:9000", "method": "PUT", "path": "/api/v1/livefs/x"},
        {"host": "host.docker.internal:9000", "kind": "websocket"},
    ],
)
async def test_the_app_and_local_processes_pass(request_):
    passed, _ = await _ask(**request_)
    assert passed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_",
    [
        # Any site the user visits.
        {"origin": "https://evil.example"},
        # A domain resolving to loopback, which is same-origin to the browser.
        {"host": "rebind.evil.example:9000"},
        {"host": "rebind.evil.example:9000", "origin": "http://rebind.evil.example:9000"},
        # Something the agent serves on another port of this machine.
        {"origin": "http://127.0.0.1:18732"},
        # A sandboxed frame, outside the routes whose URL is the credential.
        {"origin": "null"},
        {"origin": "null", "path": "/api/v1/artifacts/g/abc"},
        {"host": ""},
        # The Docker alias says nothing for a page that names itself.
        {"host": "host.docker.internal:9000", "origin": "https://evil.example"},
        {"host": "host.docker.internal:9000", "origin": "http://host.docker.internal:9000"},
        {"host": "host.docker.internal:9000", "origin": "http://localhost:5173"},
        {
            "host": "host.docker.internal:9000",
            "origin": "null",
            "method": "GET",
            "path": "/api/v1/artifacts/g/abc",
        },
    ],
)
async def test_a_web_page_is_refused(request_):
    passed, sent = await _ask(**request_)
    assert not passed
    assert sent[0]["status"] == 403


@pytest.mark.asyncio
async def test_a_websocket_from_a_web_page_is_closed():
    passed, sent = await _ask(kind="websocket", origin="https://evil.example")
    assert not passed
    assert sent == [{"type": "websocket.close", "code": 1008}]
