"""The endpoint a computer's file mount reads and saves the user's files through.

Authenticated by the computer's mount token alone, never the app's user
auth: the caller is a machine, and its token names the one user and computer
it acts for (``services/livefs/tokens.py``). Paths are mount-relative
(``user/memory/notes.md``); errors carry a ``code`` the daemon turns into an
errno and a ``message`` that reaches the agent through the tool result.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import JSONResponse

from ptc_agent.core.sandbox.livefs_runtime.protocol import (
    MAX_FILE_BYTES,
    MOUNT,
    PREFIX,
    PROVISIONAL_HEADER,
    etag,
)
from src.server.app import setup
from src.server.app.mount_calls import caller, mutation
from src.server.services.livefs.routes import LivefsError
from src.server.services.livefs.tokens import KIND, LivefsIdentity
from src.server.services.livefs.tree import LivefsTree
from src.server.utils.uploads import read_body

logger = logging.getLogger(__name__)

router = APIRouter(prefix=PREFIX, tags=["Livefs"])


_caller = caller(KIND, "file")


def _error(exc: LivefsError) -> JSONResponse:
    return JSONResponse(
        {"code": exc.code, "message": exc.message, "path": exc.path},
        status_code=exc.status,
    )


async def _mutation(
    request: Request,
    identity: LivefsIdentity,
    op: str,
    path: str,
    change: Callable[[LivefsTree], Awaitable[tuple[dict[str, Any], Any]]],
) -> Any:
    """Make one change and file its outcome for the command that made it. A
    provisional save's refusal is only logged: the daemon sends it again at
    release unless real bytes follow, and that retry is the command's."""

    def refusal(exc: Exception) -> tuple[dict[str, Any], Response] | None:
        if not isinstance(exc, LivefsError):
            return None
        return {"path": exc.path, "code": exc.code, "error": exc.message}, _error(exc)

    provisional = "provisional" if request.headers.get(PROVISIONAL_HEADER) else None
    return await mutation(
        request,
        identity,
        op,
        lambda _, context: change(LivefsTree(identity, setup.store, context)),
        log=logger,
        label="livefs",
        refusal=refusal,
        failed_path=f"{MOUNT}/{path.strip('/')}",
        resent=lambda _: provisional,
    )


@router.get("/list")
async def list_dir(
    path: str = Query(""),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    try:
        listing = await LivefsTree(identity, setup.store).list(path)
    except LivefsError as exc:
        return _error(exc)
    return listing._asdict()


@router.get("/read")
async def read_file(
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Response:
    try:
        content, version, _ = await LivefsTree(identity, setup.store).read(path)
    except LivefsError as exc:
        return _error(exc)
    return Response(
        content.encode(),
        media_type="application/octet-stream",
        # The next save is checked against this tag, which a CDN or proxy
        # that re-encodes the body weakens or drops; it must not cache one
        # user's file either.
        headers={"ETag": etag(version), "Cache-Control": "no-store, no-transform"},
    )


@router.put("/write")
async def write_file(
    request: Request,
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    async def save(tree: LivefsTree) -> tuple[dict[str, Any], Any]:
        saved = await tree.write(
            path,
            await read_body(request, MAX_FILE_BYTES),
            if_match=request.headers.get("if-match"),
            if_none_match=request.headers.get("if-none-match"),
        )
        if saved.removed:
            return {"path": saved.path, "report": saved.report}, {"removed": True}
        return (
            {"path": saved.path, "size": saved.size, "report": saved.report},
            {"version": saved.version, "size": saved.size, "as_sent": saved.as_sent},
        )

    return await _mutation(request, identity, "write", path, save)


@router.post("/delete")
async def delete_file(
    request: Request,
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    async def remove(tree: LivefsTree) -> tuple[dict[str, Any], Any]:
        deleted, report = await tree.delete(path)
        return {"path": deleted, "report": report}, Response(status_code=204)

    return await _mutation(request, identity, "delete", path, remove)


@router.post("/rename")
async def rename_file(
    request: Request,
    path: str = Query(...),
    to: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    async def move(tree: LivefsTree) -> tuple[dict[str, Any], Any]:
        source, target, report = await tree.rename(path, to)
        return {"path": target, "from": source, "report": report}, Response(status_code=204)

    return await _mutation(request, identity, "rename", path, move)
