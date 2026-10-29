"""What the proxy itself answers instead of passing the sandbox's reply on.

E2B's gateway names the sandbox in its own error pages, and some requests
would hand the injected traffic token back to the page; neither may reach a
browser. A stream the browser has left is closed rather than relayed to no one.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from src.server.preview_proxy import app as proxy_app
from src.server.preview_proxy.app import PreviewProxy
from src.server.services.preview_base import PreviewBase
from src.server.services.preview_capabilities import Capability, Upstream

SANDBOX = "isbx0123456789abcdef"
HOST = "8080-" + "a" * 32 + ".preview.example.com"


@pytest.fixture
def proxy(monkeypatch):
    capability = Capability(
        sandbox_id=SANDBOX,
        port=8080,
        upstream=Upstream(
            f"https://8080-{SANDBOX}.e2b.app", {"e2b-traffic-access-token": "tok-1"}
        ),
        workspace_id="ws-1",
        generation=1,
        expires_at=int(time.time()) + 600,
    )

    async def resolve_host(host, *, base):
        return capability

    monkeypatch.setattr(proxy_app, "resolve_host", resolve_host)
    proxy = PreviewProxy(PreviewBase("https", "preview.example.com"))
    monkeypatch.setattr(proxy.activity, "note", lambda capability: None)
    return proxy


def _reply(status: int, body: str, content_type: str = "text/plain") -> httpx.Response:
    # A stream rather than content, which httpx would read up front.
    return httpx.Response(
        status,
        headers={"content-type": content_type},
        stream=httpx.ByteStream(body.encode()),
    )


def _upstream(proxy: PreviewProxy, handler) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    proxy._client = httpx.AsyncClient(transport=httpx.MockTransport(record))
    return seen


def _scope(method: str = "GET") -> dict:
    return {
        "type": "http",
        "method": method,
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": [(b"host", HOST.encode())],
    }


async def _call(proxy: PreviewProxy, method: str = "GET") -> tuple[int, bytes]:
    scope = _scope(method)
    sent: list[dict] = []
    requested = False

    async def receive():
        # As uvicorn's does: the body, then a wait for the browser to leave.
        nonlocal requested
        if requested:
            await asyncio.Event().wait()
        requested = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    try:
        await proxy(scope, receive, send)
    finally:
        await proxy.aclose()
    return sent[0]["status"], b"".join(m.get("body", b"") for m in sent[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "shown"), [(403, 502), (409, 503), (429, 503), (502, 502), (503, 503), (504, 502)]
)
@pytest.mark.parametrize("content_type", ["application/json", "text/html"])
async def test_e2bs_own_error_page_never_reaches_the_browser(proxy, status, shown, content_type):
    # E2B's HTML page names the sandbox after about 12 KiB of styles.
    body = (
        f"<style>{'x' * 12_000}</style>"
        f"<p>Sandbox {SANDBOX}: port 8080 is not open at 8080-{SANDBOX}.e2b.app</p>"
    )
    _upstream(proxy, lambda r: _reply(status, body, content_type))

    got, page = await _call(proxy)

    assert got == shown
    assert SANDBOX.encode() not in page


@pytest.mark.asyncio
async def test_an_apps_own_error_passes_through(proxy):
    _upstream(proxy, lambda r: _reply(502, "upstream app is down"))

    assert await _call(proxy) == (502, b"upstream app is down")


@pytest.mark.asyncio
async def test_a_full_upstream_pool_is_not_a_stopped_app(proxy):
    def full(request):
        raise httpx.PoolTimeout("no connection free", request=request)

    _upstream(proxy, full)

    status, page = await _call(proxy)

    assert status == 503
    assert b"Preview unavailable" in page


@pytest.mark.asyncio
async def test_trace_never_reaches_the_sandbox(proxy):
    # A TRACE reply echoes the request, which carries the injected token.
    seen = _upstream(proxy, lambda r: _reply(200, r.headers.get("e2b-traffic-access-token", "")))

    status, _ = await _call(proxy, "TRACE")

    assert status == 405
    assert seen == []


class _Endless(httpx.AsyncByteStream):
    """An event stream that never ends, as an idle SSE app's does."""

    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self):
        yield b"data: 1\n\n"
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_a_stream_the_browser_left_is_closed(proxy):
    stream = _Endless()
    _upstream(
        proxy,
        lambda r: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=stream
        ),
    )

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        # uvicorn's send once the browser has gone: it returns, raising nothing.
        return None

    try:
        await asyncio.wait_for(proxy(_scope(), receive, send), timeout=5)
    finally:
        await proxy.aclose()

    assert stream.closed
