"""Keeps web pages away from a server that can run code on its own machine.

An open-source server has no sign-in, and with computers on its own machine a
request can end in code running as the user. Binding to loopback does not stop
a browser: any page the user visits can send requests to 127.0.0.1, and a page
on a domain that resolves to 127.0.0.1 is same-origin with this server to the
browser's CORS checks. So with such computers allowed, a request must name a
loopback host and, where a browser says which page sent it, come from the app.

Processes on the machine send no Origin and pass. They run as a user of this
machine, which is outside what this guards.

Docker computers dial this server as ``host.docker.internal``, and that name
passes only with no Origin. A browser puts an Origin on every write and every
websocket, so a page cannot send one of those without it. Rebinding gets a page
no further: it can only rebind a name of its own, which the Host check refuses,
and a no-cors read of this name is as blind as one of 127.0.0.1. Any container
on the machine can use that name, and name any host, so who is the owner is not
decided here but by the secret the app holds (``owner_requests``).
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from urllib.parse import urlsplit

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_DOCKER_HOST_ALIAS = "host.docker.internal"
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_OPAQUE_ORIGIN = "null"


def _hostname(host_header: str) -> str | None:
    """The host a Host header names, without its port."""
    try:
        return urlsplit(f"//{host_header}").hostname
    except ValueError:
        return None


def _origin_of(url: str) -> str | None:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}" if parts.scheme and parts.netloc else None


class LoopbackOriginGuard:
    """Refuses what a web page could send, before anything else handles it.

    ``app_origins`` names where the web app is served when that is not this
    server's own origin (a dev server). ``opaque_origin_routes`` are the routes
    whose URL is the whole credential: their documents run on an opaque origin
    and read through them with ``Origin: null``, which nothing else may use.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        app_origins: Iterable[str],
        opaque_origin_routes: re.Pattern[str],
    ) -> None:
        self.app = app
        self._app_origins = frozenset(filter(None, map(_origin_of, app_origins)))
        self._opaque_origin_routes = opaque_origin_routes

    def _refusal(self, scope: Scope) -> str | None:
        headers = {k: v for k, v in scope.get("headers") or ()}
        host = headers.get(b"host", b"").decode("latin-1")
        origin = headers.get(b"origin", b"").decode("latin-1")
        hostname = _hostname(host)
        if hostname == _DOCKER_HOST_ALIAS:
            return "origin" if origin else None
        if hostname not in _LOOPBACK_HOSTS:
            return "host"
        if not origin:
            return None
        if origin == _OPAQUE_ORIGIN:
            allowed = (
                scope["type"] == "http"
                and scope.get("method") in _SAFE_METHODS
                and self._opaque_origin_routes.match(scope["path"]) is not None
            )
            return None if allowed else "origin"
        scheme = {"wss": "https", "ws": "http"}.get(scope["scheme"], scope["scheme"])
        if origin == f"{scheme}://{host}" or origin in self._app_origins:
            return None
        return "origin"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        refused = self._refusal(scope)
        if refused is None:
            await self.app(scope, receive, send)
            return
        logger.warning(
            "Refused a request from outside this machine's app: %s (%s %s)",
            refused,
            scope["type"],
            scope["path"],
        )
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        body = json.dumps({"detail": "Not allowed from this origin."}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 403,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
