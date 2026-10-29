"""The preview proxy: an ASGI app for every request under the preview base.

Selected by Host ahead of everything else the server runs, so a preview
origin never reaches an app route, and CORS, compression, request ids and
tracing never touch preview bytes or record a capability host. The proxy's
own calls upstream and to Redis run with instrumentation suppressed, so the
client instrumentations record none of them either.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from http.cookiejar import CookieJar
from typing import Any

import httpx
from opentelemetry import context as otel_context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY

from src.server.preview_proxy.activity import PreviewActivity
from src.server.preview_proxy.headers import request_headers, response_headers
from src.server.preview_proxy.limits import SandboxSlots
from src.server.preview_proxy.ws import relay_websocket
from src.server.services.preview_base import PreviewBase
from src.server.services.preview_capabilities import (
    Capability,
    PreviewProxyUnavailable,
    Upstream,
    resolve_host,
    until_revoked,
)
from src.server.services.preview_upstreams import upstream_for

logger = logging.getLogger(__name__)

_MAX_BODY_BYTES = 64 * 1024 * 1024
# Read is long for apps that stream (SSE); the rest fail fast.
_TIMEOUT = httpx.Timeout(connect=10.0, read=300.0, write=60.0, pool=10.0)
# Enough of an error body to tell E2B's own gateway errors from an app's. Its
# HTML page names the sandbox only near the end, about 12 KiB in.
_PEEK_BYTES = 64 * 1024
# The statuses E2B's gateway answers with itself, as JSON or, to a browser
# navigating, as an HTML page; either names the sandbox. Each maps to the page
# shown instead: 409 is a sandbox changing state and 429 a limit, both passing.
_GATEWAY_STATUSES = {403: 502, 409: 503, 429: 503, 502: 502, 503: 503, 504: 502}
# TRACE would echo the injected traffic token back to the page.
_REFUSED_METHODS = frozenset({"TRACE", "CONNECT"})
# Per sandbox, per worker. A page loading many modules at once queues for a
# slot rather than failing; one sandbox cannot take the whole upstream pool.
_REQUESTS_PER_SANDBOX = 32
_REQUEST_SLOT_WAIT_S = 10.0
_SOCKETS_PER_SANDBOX = 16
# Try Again Later; before the accept, any close answers the handshake with 403.
_SOCKETS_FULL = 1013

_PAGE_HEADERS = [
    (b"content-type", b"text/html; charset=utf-8"),
    (b"cache-control", b"no-store"),
    (b"referrer-policy", b"no-referrer"),
    (b"x-robots-tag", b"noindex"),
]
_PAGES = {
    400: ("Bad request", "This address is not one a preview can serve."),
    405: ("Method not allowed", "A preview does not accept this kind of request."),
    404: (
        "Preview not found",
        "This preview link has expired or was never valid. Open the app again "
        "from its workspace to get a fresh one.",
    ),
    413: ("Request too large", "The request is larger than a preview accepts."),
    502: (
        "The app isn't running",
        "Its computer may be stopped, or nothing is listening on this port. "
        "Start it from its workspace, then reload.",
    ),
    503: ("Preview unavailable", "Previews can't be reached right now. Try again shortly."),
}


class _NoCookies(CookieJar):
    """The upstream client keeps no cookies: one jar would mix every user's."""

    def extract_cookies(self, response: Any, request: Any) -> None:
        return None

    def add_cookie_header(self, request: Any) -> None:
        return None


class _BodyTooLarge(Exception):
    pass


class _ClientGone(Exception):
    pass


def _header(scope: dict[str, Any], name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return None


async def _page(send: Any, status: int) -> None:
    title, text = _PAGES[status]
    body = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        f"<title>{title}</title></head>"
        "<body style=\"font-family:system-ui,sans-serif;max-width:32rem;"
        "margin:4rem auto;padding:0 1rem;color:#333\">"
        f"<h1 style=\"font-size:1.25rem\">{title}</h1><p>{text}</p></body></html>"
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [*_PAGE_HEADERS, (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _pump(
    chunks: AsyncIterator[bytes],
    send: Any,
    capability: Capability,
    activity: PreviewActivity,
) -> None:
    async for chunk in chunks:
        await send({"type": "http.response.body", "body": chunk, "more_body": True})
        # A stream the browser only reads, such as an event feed, is use too.
        activity.note(capability)


async def _until_disconnect(receive: Any) -> None:
    # uvicorn's send returns quietly once the browser has gone, so a relay
    # that only sends would hold an abandoned stream open. With the body
    # read, receive waits for exactly that.
    while (await receive())["type"] != "http.disconnect":
        pass


class PreviewProxy:
    """Serves ``<port>-<token>.<base>`` by forwarding to the sandbox port the
    capability opens, with the header the sandbox requires. Every request it
    does not claim goes on to *app*."""

    def __init__(
        self,
        base: PreviewBase,
        *,
        frame_ancestors: str | None = None,
        app: Any = None,
    ):
        self.base = base
        self.app = app
        self._frame_ancestors = frame_ancestors or None
        self._client: httpx.AsyncClient | None = None
        self.activity = PreviewActivity()
        self._requests = SandboxSlots(_REQUESTS_PER_SANDBOX)
        self._sockets = SandboxSlots(_SOCKETS_PER_SANDBOX)

    def claims(self, scope: dict[str, Any]) -> bool:
        return scope.get("type") in ("http", "websocket") and self.base.claims(
            _header(scope, b"host")
        )

    def _http_client(self) -> httpx.AsyncClient:
        # Built on first use, on the worker's own loop; a pool is execution
        # context and nothing here is read as truth.
        if self._client is None:
            client = httpx.AsyncClient(
                timeout=_TIMEOUT,
                follow_redirects=False,
                cookies=_NoCookies(),
                limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
            )
            # Only what the browser sent goes upstream. httpx's own defaults
            # would ask for compression a client never offered to decode, as
            # the body is relayed undecoded.
            for name in ("accept", "accept-encoding", "user-agent"):
                del client.headers[name]
            self._client = client
        return self._client

    async def aclose(self) -> None:
        await self.activity.aclose()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _capability_origin(self, host: str) -> str:
        """The origin the browser is on, as its Origin header names it."""
        host = host.strip().lower()
        default = ":443" if self.base.scheme == "https" else ":80"
        if host.endswith(default):
            host = host[: -len(default)]
        return f"{self.base.scheme}://{host}"

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if not self.claims(scope):
            await self.app(scope, receive, send)
            return
        # The client instrumentations would record each upstream URL, query
        # and all, and each capability lookup.
        token = otel_context.attach(
            otel_context.set_value(_SUPPRESS_INSTRUMENTATION_KEY, True)
        )
        try:
            await self._serve(scope, receive, send)
        finally:
            otel_context.detach(token)

    async def _serve(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        websocket = scope["type"] == "websocket"
        host = _header(scope, b"host") or ""
        capability: Capability | None = None
        status = 404
        try:
            capability = await resolve_host(host, base=self.base)
        except PreviewProxyUnavailable as e:
            logger.warning("Preview proxy could not resolve a capability: %s", e)
            status = 503
        raw_path = scope.get("raw_path") or scope.get("path", "").encode()
        if capability is not None and not raw_path.startswith(b"/"):
            capability, status = None, 400
        if capability is None:
            if websocket:
                await send({"type": "websocket.close", "code": 1008})
            else:
                await _page(send, status)
            return

        query = scope.get("query_string") or b""
        target = raw_path + (b"?" + query if query else b"")
        capability_origin = self._capability_origin(host)
        if websocket:
            if not await self._sockets.acquire(capability.sandbox_id):
                await send({"type": "websocket.close", "code": _SOCKETS_FULL})
                return
            try:
                await self._dial(scope, receive, send, capability, capability_origin, target)
            finally:
                self._sockets.release(capability.sandbox_id)
            return
        if scope["method"] in _REFUSED_METHODS:
            await _page(send, 405)
            return
        if not await self._requests.acquire(
            capability.sandbox_id, wait=_REQUEST_SLOT_WAIT_S
        ):
            await _page(send, 503)
            return
        try:
            await self._dial(scope, receive, send, capability, capability_origin, target)
        finally:
            self._requests.release(capability.sandbox_id)

    async def _dial(
        self,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
        capability: Capability,
        capability_origin: str,
        target: bytes,
    ) -> None:
        """Open the capability's port from this worker, then relay to it."""
        websocket = scope["type"] == "websocket"
        try:
            upstream = await upstream_for(capability)
            status = 502
        except PreviewProxyUnavailable as e:
            logger.warning("Preview proxy could not open an upstream: %s", e)
            upstream, status = None, 503
        if upstream is None:
            if websocket:
                # Before the accept, a close answers the handshake with a 403.
                await send({"type": "websocket.close", "code": 1011})
            else:
                await _page(send, status)
            return
        if websocket:
            await relay_websocket(
                scope,
                receive,
                send,
                capability,
                upstream=upstream,
                capability_origin=capability_origin,
                target=target.decode("latin-1"),
                activity=self.activity,
            )
        else:
            await self._forward(
                scope, receive, send, capability, upstream, capability_origin, target
            )

    async def _forward(
        self,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
        capability: Capability,
        upstream: Upstream,
        capability_origin: str,
        target: bytes,
    ) -> None:
        method = scope["method"]
        headers = request_headers(
            scope.get("headers", []),
            capability_origin=capability_origin,
            upstream_origin=upstream.origin,
            inject=upstream.headers,
        )
        declared = _header(scope, b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > _MAX_BODY_BYTES:
            await _page(send, 413)
            return
        has_body = declared is not None or _header(scope, b"transfer-encoding") is not None

        async def body():
            total = 0
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    raise _ClientGone
                chunk = message.get("body", b"")
                total += len(chunk)
                if total > _MAX_BODY_BYTES:
                    raise _BodyTooLarge
                if chunk:
                    yield chunk
                if not message.get("more_body", False):
                    return

        client = self._http_client()
        request = client.build_request(
            method,
            httpx.URL(upstream.origin).copy_with(raw_path=target),
            headers=[(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers],
            content=body() if has_body else None,
        )
        try:
            response = await client.send(request, stream=True)
        except _BodyTooLarge:
            await _page(send, 413)
            return
        except _ClientGone:
            return
        except httpx.PoolTimeout:
            # Every upstream connection on this worker is in use: the proxy is
            # full, not the app down.
            await _page(send, 503)
            return
        except httpx.HTTPError as e:
            logger.debug("Preview upstream unreachable: %s", type(e).__name__)
            await _page(send, 502)
            return

        try:
            chunks = response.aiter_raw()
            head = b""
            page = _GATEWAY_STATUSES.get(response.status_code)
            if page is not None:
                async for chunk in chunks:
                    head += chunk
                    if len(head) >= _PEEK_BYTES:
                        break
                # E2B's page names the sandbox, which the browser never learns.
                if capability.sandbox_id.encode() in head:
                    await _page(send, page)
                    return
            await send(
                {
                    "type": "http.response.start",
                    "status": response.status_code,
                    "headers": response_headers(
                        response.headers.raw,
                        upstream_origin=upstream.origin,
                        capability_origin=capability_origin,
                        frame_ancestors=self._frame_ancestors,
                        withheld=upstream.headers.values(),
                    ),
                }
            )
            self.activity.note(capability)
            if method != "HEAD":
                if head:
                    await send({"type": "http.response.body", "body": head, "more_body": True})
                # A stream outlives its request, so it ends on whichever comes
                # first: its last byte, the browser leaving, or a revocation.
                relay = asyncio.create_task(_pump(chunks, send, capability, self.activity))
                gone = asyncio.create_task(_until_disconnect(receive))
                revoked = asyncio.create_task(until_revoked(capability))
                tasks = (relay, gone, revoked)
                try:
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                if not relay.cancelled():
                    relay.result()
                elif not gone.cancelled():
                    gone.result()
                    return
                else:
                    revoked.result()
                    # A revocation ends a stream as it closes an open socket.
                    # With a declared length there is no clean end, so the
                    # connection drops.
                    if "content-length" in response.headers:
                        return
            await send({"type": "http.response.body", "body": b""})
        except httpx.HTTPError as e:
            # The upstream broke mid-body; the browser sees a truncated reply.
            logger.debug("Preview upstream failed mid-response: %s", type(e).__name__)
        finally:
            await response.aclose()


def install_preview_proxy(app: Any) -> PreviewProxy | None:
    """Put the preview proxy in front of *app*, when the deployment sets a base.

    Wraps the built middleware stack rather than adding a middleware, so the
    proxy sits outside even the tracing that instruments the stack: a
    capability host is never recorded as a span attribute. Call it after the
    last ``add_middleware`` and after instrumenting the app.
    """
    from src.config.env import PREVIEW_PROXY_FRAME_ANCESTORS
    from src.server.services.preview_base import preview_base

    base = preview_base()
    if base is None:
        return None
    proxy = PreviewProxy(base, frame_ancestors=PREVIEW_PROXY_FRAME_ANCESTORS)
    app.state.preview_proxy = proxy
    build = app.build_middleware_stack

    def build_middleware_stack() -> Any:
        proxy.app = build()
        return proxy

    app.build_middleware_stack = build_middleware_stack
    return proxy
