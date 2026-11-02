"""Serving the built web app from this server, so one origin serves both.

For a deployment with no separate web server: the desktop app runs this with
``LANGALPHA_WEB_DIST`` pointing at its bundled ``web/dist``. Unset, none of this
is installed.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI
from starlette.responses import FileResponse, Response
from starlette.types import Receive, Scope, Send

logger = logging.getLogger(__name__)

#: Paths that are never the web app's. A miss here stays a plain 404: a client
#: calling an API that does not exist must not be handed a page of HTML.
_BACKEND_ROOTS = frozenset({"api", "ws", "v1", "health", "docs", "redoc", "openapi.json"})

_NO_CACHE = {"Cache-Control": "no-cache"}


def _file_under(root: Path, path: str) -> Path | None:
    """The file ``path`` names inside ``root``; anything else is None."""
    try:
        candidate = (root / path).resolve()
    except (OSError, ValueError):
        return None
    if candidate.is_relative_to(root) and candidate.is_file():
        return candidate
    return None


def _respond(root: Path, path: str) -> Response | None:
    """What the web app answers ``path`` with, or None to leave it a 404.

    ``version.json`` is how a running tab notices a new build, so it must be
    fresh and must not turn into the app shell on a miss. A missing hashed asset
    must fail as a load error rather than parse HTML as code, so it stays a miss
    too. Only ``assets/`` is content-hashed, so everything else revalidates.
    """
    if path.split("/", 1)[0] in _BACKEND_ROOTS:
        return None
    file = _file_under(root, path) if path else None
    if file is not None:
        headers = None if path.startswith("assets/") else _NO_CACHE
        return FileResponse(file, headers=headers)
    if path == "version.json" or path.startswith("assets/"):
        return None
    return FileResponse(root / "index.html", headers=_NO_CACHE)


def serve_web_dist(app: FastAPI) -> None:
    """Answer requests no route matched from ``LANGALPHA_WEB_DIST``.

    On the router's fallback rather than a catch-all route, which would match
    first and so end the router's own handling of a wrong method or a trailing
    slash. By construction this never shadows a route, whenever one is added.
    """
    configured = os.getenv("LANGALPHA_WEB_DIST")
    if not configured:
        return
    root = Path(configured).resolve()
    if not (root / "index.html").is_file():
        logger.warning("LANGALPHA_WEB_DIST has no index.html, not serving it: %s", root)
        return

    not_found = app.router.default

    async def fallback(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["method"] in ("GET", "HEAD"):
            response = _respond(root, scope["path"].lstrip("/"))
            if response is not None:
                await response(scope, receive, send)
                return
        await not_found(scope, receive, send)

    app.router.default = fallback
    logger.info("Serving the web app from %s", root)
