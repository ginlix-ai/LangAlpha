"""The endpoint a computer's Drive mount reads and saves the user's Drive through.

The "Mount protocol" of the Drive contract: under the livefs prefix and
authenticated by the same computer mount token, with a request rate of its
own (``tokens.DRIVE``). Every action needs the ``drive`` feature on for the
token's user (404 otherwise) and reaches its space through ``drive.open``.
Each change is the agent's, as the command that made it was filed
(``outcomes.call_context``), and its outcome is filed under the command's
call id as livefs's are, so a refused save reaches the tool result.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import asdict
from typing import Any, TypeVar

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ValidationError

from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.livefs_runtime.drive_protocol import (
    ACTION_PREFIX,
    FEED_WAIT_S,
    PROCESS_HEADER,
    ROW_FIELDS,
    TEXTS_MAX_BYTES,
    TEXTS_MAX_IDS,
    WireEntry,
    encode_frame,
)
from ptc_agent.core.sandbox.livefs_runtime.protocol import CALL_HEADER, PREFIX
from src.server.app.drive_http import byte_range, etag_of, receive, status_of
from src.server.app.mount_calls import caller, mutation
from src.server.database.computer import get_computer
from src.server.models.drive import UploadIn, UploadSignIn
from src.server.services import drive
from src.server.services.drive import Actor, DriveBusy, DriveError, Entry, Space, uploads
from src.server.services.livefs import drive_notices, outcomes
from src.server.services.livefs.tokens import DRIVE, LivefsIdentity
from src.server.services.persistence.transfer import kind_pulls_from_store
from src.server.utils.uploads import read_body
from src.utils.storage import multipart_enabled

logger = logging.getLogger(__name__)

router = APIRouter(prefix=f"{PREFIX}/{ACTION_PREFIX}", tags=["Drive mount"])

#: Past this many changed entries a ``changes`` answer asks for a resync: one
#: change (a folder move) can touch a whole space, and the manifest is the
#: answer built for that size.
CHANGES_LIMIT = 5000

_PROCESS = re.compile(r"\d{1,10}-\d{1,20}")
#: Seconds a save the server had no room for is asked to wait.
_BUSY_RETRY_S = 1
#: The most a JSON body on these routes carries (``texts``' ids, a batch of
#: notices, an upload's begin or its parts to sign), read no further.
_JSON_BODY_MAX = 1024 * 1024

#: A change's outcome fields and the response it answers with.
_Change = Callable[[Space, Actor], Awaitable[tuple[dict[str, Any], Any]]]


_caller = caller(DRIVE, "Drive")


def _error(exc: DriveError, path: str = "") -> JSONResponse:
    body: dict[str, Any] = {"code": exc.code, "message": exc.message, "path": path, **exc.details}
    return JSONResponse(body, status_code=status_of(exc))


async def _json_body(request: Request) -> bytes:
    """The raw JSON body, refused ``too_large`` past ``_JSON_BODY_MAX``
    before more of it is read."""
    raw = await read_body(request, _JSON_BODY_MAX)
    if len(raw) > _JSON_BODY_MAX:
        raise DriveError("too_large", f"A request body is at most {_JSON_BODY_MAX // 1024} KB")
    return raw


def _upload_error(exc: DriveError, path: str = "") -> JSONResponse:
    """A refusal of an upload step; one worth sending again says when."""
    response = _error(exc, path)
    if exc.code == "unavailable":
        response.headers["Retry-After"] = str(_BUSY_RETRY_S)
    return response


def _wire(entry: Entry) -> WireEntry:
    folder = entry.kind == "folder"
    wire: WireEntry = {
        "id": entry.entry_id,
        "path": entry.path,
        "kind": entry.kind,
        "size": 0 if folder else int(entry.size or 0),
        "sha256": None if folder else entry.sha256,
        "is_text": False if folder else bool(entry.is_text),
        "modified_ms": int(entry.modified_at.timestamp() * 1000),
        "seq": entry.changed_seq,
    }
    if entry.deleted_at is not None:
        wire["deleted"] = True
    return wire


def _actor(
    identity: LivefsIdentity,
    context: CallContext | None,
    call_id: str | None,
    process: str | None = None,
) -> Actor:
    """The agent's, as its command was filed. A change no command filed (a
    process started outside any tool call, or a filing lost to Redis) is
    still the agent's, for no thread. The sitting, which folds one file's
    saves into a version and groups a burst of deletes into one trash item,
    is the turn's run, else the call, else the session of the process that
    made the change (``X-Drive-Process``); with none of them, each change
    stands alone."""
    sitting = context.sitting if context is not None else None
    if sitting is None and call_id:
        sitting = f"call:{call_id}"
    if sitting is None and process:
        sitting = f"process:{identity.computer_id}:{process}"
    return Actor.of_call(
        context,
        user_id=identity.user_id,
        via="mount",
        sitting=sitting,
        computer_id=identity.computer_id,
    )


def _busy(exc: Exception) -> str | None:
    return "busy" if isinstance(exc, DriveBusy) else None


def _save_resent(exc: Exception) -> str | None:
    """A save the server had no room for, or failed on, is sent again until
    it lands or is refused; the daemon tells the agent meanwhile that it is
    not saved yet (``drive_notices``). The daemon sends every ``unavailable``
    again (a store that did not answer as much as a busy worker), so only
    the other codes are refusals filed here."""
    if isinstance(exc, DriveError):
        return _busy(exc) or ("unavailable" if exc.code == "unavailable" else None)
    return "failed"


async def _mutation(
    request: Request,
    identity: LivefsIdentity,
    op: str,
    space_ref: str,
    path: str,
    change: _Change,
    resent: Callable[[Exception], str | None] = _busy,
) -> Any:
    """Make one change as the agent and file its outcome for the command
    that made it. A refusal is reported once: the daemon never resends."""
    process = request.headers.get(PROCESS_HEADER) or ""
    process = process if _PROCESS.fullmatch(process) else None
    shown = drive_notices.shown(identity.root_dir, space_ref, path)

    async def run(call_id: str | None, context: CallContext | None) -> tuple[dict[str, Any], Any]:
        space = await drive.open(identity.user_id, space_ref)
        fields, response = await change(space, _actor(identity, context, call_id, process))
        return {"path": shown, **fields}, response

    def refusal(exc: Exception) -> tuple[dict[str, Any], Response] | None:
        if not isinstance(exc, DriveError):
            return None
        response = _error(exc, path)
        if isinstance(exc, DriveBusy):
            response.headers["Retry-After"] = str(_BUSY_RETRY_S)
        return {"path": shown, "code": exc.code, "error": exc.message}, response

    return await mutation(
        request,
        identity,
        op,
        run,
        log=logger,
        label="drive mount",
        refusal=refusal,
        failed_path=shown,
        resent=resent,
    )


# --- reads -------------------------------------------------------------------


@router.get("/spaces")
async def list_spaces(identity: LivefsIdentity = Depends(_caller)) -> Any:
    try:
        if not await drive.enabled(identity.user_id):
            raise DriveError("not_found", "The Drive is off")
        found = await drive.spaces(identity.user_id)
    except DriveError as exc:
        return _error(exc)
    return {
        "spaces": [
            {"ref": s.kind, "name": s.kind, "id": s.space_id}
            for s in found
            if s.kind == "personal"
        ]
    }


@router.get("/manifest")
async def manifest(
    space: str = Query(...), identity: LivefsIdentity = Depends(_caller)
) -> Any:
    try:
        sp = await drive.open(identity.user_id, space)
        # Read before the rows: a change between the two is in the rows and
        # comes again on the feed, which the daemon applies idempotently.
        cursor = await drive.cursor(sp)
        entries = await drive.tree(sp)
        usage = await drive.usage(sp)
    except DriveError as exc:
        return _error(exc)
    rows = [[wire[f] for f in ROW_FIELDS] for wire in map(_wire, entries)]
    return {
        "space": {"ref": space, "id": sp.space_id},
        "cursor": cursor,
        "limits": {"file_bytes": usage.max_file_bytes, "upload_bytes": usage.max_upload_bytes},
        "usage": asdict(usage),
        "fields": list(ROW_FIELDS),
        "entries": rows,
    }


@router.post("/texts")
async def texts(
    request: Request,
    space: str = Query(...),
    every: int = Query(0, alias="all"),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    """Text files' bytes as frames. ``all=1`` streams every one the space
    holds in one answer, a page at a time, for a mount starting empty;
    otherwise the ``ids`` asked for, up to about ``TEXTS_MAX_BYTES``."""
    try:
        sp = await drive.open(identity.user_id, space)
        ids: list[str] | None = None
        if not every:
            try:
                asked = json.loads(await _json_body(request) or b"{}").get("ids")
            except (ValueError, AttributeError):
                asked = None
            if not isinstance(asked, list) or len(asked) > TEXTS_MAX_IDS:
                raise DriveError("invalid_path", f"Send at most {TEXTS_MAX_IDS} ids")
            ids = [str(i) for i in asked]
    except DriveError as exc:
        return _error(exc)

    async def frames() -> AsyncIterator[bytes]:
        sent = 0
        async for entry, data in drive.text_bytes(sp, ids):
            yield encode_frame(entry.entry_id, entry.sha256 or "", data)
            sent += len(data)
            if ids is not None and sent >= TEXTS_MAX_BYTES:
                return

    return StreamingResponse(frames(), media_type="application/octet-stream")


@router.get("/content")
async def content(
    request: Request,
    space: str = Query(...),
    sha256: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    try:
        sp = await drive.open(identity.user_id, space)
        found = await drive.content_by_sha(sp, sha256)
    except DriveError as exc:
        return _error(exc)
    start, end, status = 0, found.size, 200
    headers = {"ETag": f'"{found.sha256}"', "Cache-Control": "no-store, no-transform"}
    try:
        span = byte_range(request.headers.get("range"), found.size)
    except DriveError:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{found.size}"})
    if span is not None:
        (start, end), status = span, 206
        headers["Content-Range"] = f"bytes {start}-{end - 1}/{found.size}"
    headers["Content-Length"] = str(end - start)
    return StreamingResponse(
        drive.stream(found, start, end),
        status_code=status,
        media_type="application/octet-stream",
        headers=headers,
    )


@router.get("/entry")
async def entry(
    space: str = Query(...),
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    try:
        sp = await drive.open(identity.user_id, space)
        found = await drive.stat(sp, path)
    except DriveError as exc:
        return _error(exc, path)
    return {"entry": _wire(found)}


@router.get("/changes")
async def changes(
    space: str = Query(...),
    since: int = Query(0),
    wait: float = Query(0),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    # The feed asks at least every FEED_WAIT_S: while it does, the daemon can
    # hand the server its own notices, and the host need not ask the computer.
    await drive_notices.mark_seen(identity.computer_id)
    try:
        sp = await drive.open(identity.user_id, space)
        found = await drive.changes(
            sp, since, wait=min(max(wait, 0.0), FEED_WAIT_S + 5), limit=CHANGES_LIMIT
        )
        answer: dict[str, Any] = {
            "entries": [_wire(e) for e in found.entries],
            "cursor": found.cursor,
            "resync": found.resync,
        }
        if found.entries or found.cursor != since:
            # A cursor that moved with no entry behind it (a purge, a
            # sweep's trim) changed the usage too, which ``statfs`` reads.
            answer["usage"] = asdict(await drive.usage(sp))
    except DriveError as exc:
        return _error(exc)
    return answer


@router.post("/notices")
async def notices(request: Request, identity: LivefsIdentity = Depends(_caller)) -> Any:
    """The daemon's notices about saves the server never answered
    (``drive_notices``), filed under their calls, each once."""
    if not await drive.enabled(identity.user_id):
        return _error(DriveError("not_found", "No such space"))
    try:
        body = json.loads(await _json_body(request))
    except DriveError as exc:
        return _error(exc)
    except ValueError:
        return _error(DriveError("invalid_path", "A JSON body is required"))
    notes = body.get("notices") if isinstance(body, dict) else None
    if not isinstance(notes, list) or len(notes) > drive_notices.MAX_NOTICES:
        return _error(DriveError("invalid_path", "notices must be a list"))
    await drive_notices.mark_seen(identity.computer_id)
    await drive_notices.file(identity.computer_id, identity.root_dir, notes)
    return {"filed": True}


# --- changes -----------------------------------------------------------------


@router.put("/write")
async def write(
    request: Request,
    space: str = Query(...),
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    async def save(sp: Space, actor: Actor) -> tuple[dict[str, Any], Any]:
        bounds = await drive.limits(sp)
        async with receive(request, bounds) as data:
            saved = await drive.write(
                sp,
                path,
                data,
                actor=actor,
                if_match=etag_of(request.headers.get("if-match")),
                if_none_match=(request.headers.get("if-none-match") or "").strip() == "*",
                bounds=bounds,
            )
        return {"size": saved.size}, {"entry": _wire(saved), "cursor": await drive.cursor(sp)}

    return await _mutation(request, identity, "write", space, path, save, _save_resent)


@router.post("/mkdir")
async def mkdir(
    request: Request,
    space: str = Query(...),
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    async def make(sp: Space, actor: Actor) -> tuple[dict[str, Any], Any]:
        made = await drive.mkdir(sp, path)
        return {}, {"entry": _wire(made), "cursor": await drive.cursor(sp)}

    return await _mutation(request, identity, "mkdir", space, path, make)


@router.post("/move")
async def move(
    request: Request,
    space: str = Query(...),
    src: str = Query(..., alias="from"),
    dst: str = Query(..., alias="to"),
    replace: int = Query(0),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    async def rename(sp: Space, actor: Actor) -> tuple[dict[str, Any], Any]:
        removed: list[str] = []
        if replace:
            moved, ended = await drive.replace(sp, src, dst, actor=actor)
            removed = [ended] if ended else []
        else:
            moved = await drive.move(sp, src, dst)
        answer = {"entry": _wire(moved), "removed": removed, "cursor": await drive.cursor(sp)}
        return {"from": drive_notices.shown(identity.root_dir, space, src)}, answer

    return await _mutation(request, identity, "move", space, dst, rename)


@router.post("/trash")
async def trash(
    request: Request,
    space: str = Query(...),
    path: str = Query(...),
    kind: str | None = Query(None),
    empty: int = Query(0),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    """``If-Match: "<sha256>"`` deletes the file only while its head is
    that version (412 ``changed`` otherwise), as a save's does."""
    if_match = etag_of(request.headers.get("if-match"))

    async def remove(sp: Space, actor: Actor) -> tuple[dict[str, Any], Any]:
        if kind not in (None, "file", "folder"):
            raise DriveError("invalid_path", "kind is file or folder")
        if if_match == "*":
            raise DriveError("invalid_path", "If-Match on a delete names a sha256")
        trash_id = await drive.trash(
            sp, path, actor=actor, kind=kind, empty=bool(empty), if_match=if_match
        )
        return {}, {"trash_id": trash_id, "cursor": await drive.cursor(sp)}

    return await _mutation(request, identity, "trash", space, path, remove)


# --- large files ------------------------------------------------------------------

#: How long a signed read address works; the daemon asks again before then.
SOURCE_TTL_S = 900


async def _reaches_store(identity: LivefsIdentity) -> bool:
    """Whether this computer fetches from the store itself, as its workspace
    files do (the computer's transfer mode)."""
    computer = await get_computer(identity.computer_id)
    return computer is not None and kind_pulls_from_store(computer.get("kind"))


@router.get("/source")
async def source(
    space: str = Query(...),
    sha256: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    """Where to read the bytes of ``sha256`` by range: a signed address on
    the store, or this endpoint's ``content``. Addressed by the bytes, so a
    reader keeps its version after the file changes."""
    try:
        sp = await drive.open(identity.user_id, space)
        found = await drive.content_by_sha(sp, sha256)
    except DriveError as exc:
        return _error(exc)
    answer: dict[str, Any] = {"via": "api", "sha256": found.sha256, "size": found.size}
    if found.blob_user_id is not None and await _reaches_store(identity):
        url = await drive.signed_url(found, ttl=SOURCE_TTL_S, for_browser=False)
        if url is not None:
            answer.update(via="link", url=url, expires_in=SOURCE_TTL_S)
    return answer


def _upload_json(out: dict[str, Any], cursor: int | None = None) -> dict[str, Any]:
    """An upload's answer in the mount's terms: its entry as a wire entry,
    with the cursor, and a failure as ``{code, message}``."""
    out = dict(out)
    if isinstance(out.get("entry"), Entry):
        out["entry"] = _wire(out["entry"])
        if cursor is not None:
            out["cursor"] = cursor
    if isinstance(out.get("error"), dict):
        out["error"] = {"code": out["error"].get("type"), "message": out["error"].get("message")}
    return out


async def _file(
    identity: LivefsIdentity,
    call_id: str | None,
    context: CallContext | None,
    outcome: dict[str, Any],
) -> None:
    logger.info(
        "drive mount write",
        extra={"computer_id": identity.computer_id, "call_id": call_id, "ok": outcome.get("ok")},
    )
    await outcomes.record(identity.computer_id, call_id, outcome, context)


@router.post("/uploads")
async def begin_upload(
    request: Request,
    space: str = Query(...),
    path: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    """Begin a save in parts. A refusal here is the save's outcome and is
    filed for the command as ``write``'s is; the upload files its own end
    for the command (``uploads._report``)."""
    call_id = outcomes.valid_call_id(request.headers.get(CALL_HEADER))
    context = await outcomes.call_context(identity.computer_id, call_id)
    filed = call_id if context is not None else None
    process = request.headers.get(PROCESS_HEADER) or ""
    process = process if _PROCESS.fullmatch(process) else None
    shown = drive_notices.shown(identity.root_dir, space, path)
    size = None
    try:
        body = _model(UploadIn, await _json_body(request), {
            "path": path,
            "if_match": etag_of(request.headers.get("if-match")),
            "if_none_match": (request.headers.get("if-none-match") or "").strip() == "*",
        })
        size = body.size
        sp = await drive.open(identity.user_id, space)
        out = await uploads.begin(
            sp,
            _actor(identity, context, call_id, process),
            body,
            direct=await _reaches_store(identity) and multipart_enabled(),
            report={"computer_id": identity.computer_id, "call_id": filed, "path": shown},
        )
    except DriveError as exc:
        if exc.code != "unavailable":
            await _file(
                identity,
                filed,
                context,
                {"op": "write", "ok": False, "path": shown, "code": exc.code, "error": exc.message},
            )
        return _upload_error(exc, path)
    if out["mode"] == "held":
        await _file(
            identity, filed, context, {"op": "write", "ok": True, "path": shown, "size": size}
        )
        return _upload_json(out, await drive.cursor(sp))
    return _upload_json(out)


async def _answer(sp: Space, out: dict[str, Any]) -> dict[str, Any]:
    return _upload_json(out, await drive.cursor(sp) if out.get("entry") else None)


_Model = TypeVar("_Model", bound=BaseModel)


def _model(model: type[_Model], raw: bytes, fields: dict[str, Any] | None = None) -> _Model:
    """A JSON body (plus ``fields`` from the query and headers) checked by
    the person's route's own model; a body it refuses reads as ``invalid_path``,
    the mount's code for a request it cannot act on."""
    try:
        body = json.loads(raw or b"{}")
        if not isinstance(body, dict):
            raise ValueError("not an object")
        return model.model_validate({**body, **(fields or {})})
    except (ValueError, ValidationError) as e:
        message = e.errors()[0]["msg"] if isinstance(e, ValidationError) else "The body is not JSON"
        raise DriveError("invalid_path", message) from None


@router.post("/uploads/{upload_id}/parts")
async def sign_upload_parts(
    request: Request,
    upload_id: str,
    space: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    try:
        body = _model(UploadSignIn, await _json_body(request))
        sp = await drive.open(identity.user_id, space)
        return await uploads.sign(
            sp, upload_id, [p.model_dump() for p in body.parts], for_browser=False
        )
    except DriveError as exc:
        return _upload_error(exc)


@router.put("/uploads/{upload_id}/parts/{number}")
async def send_upload_part(
    request: Request,
    upload_id: str,
    number: int,
    space: str = Query(...),
    identity: LivefsIdentity = Depends(_caller),
) -> Any:
    try:
        sp = await drive.open(identity.user_id, space)
        return await uploads.relay_part(
            sp,
            upload_id,
            number,
            request.headers.get("content-md5"),
            lambda cap: read_body(request, cap),
        )
    except DriveError as exc:
        return _upload_error(exc)


@router.post("/uploads/{upload_id}/complete", status_code=202)
async def complete_upload(
    upload_id: str, space: str = Query(...), identity: LivefsIdentity = Depends(_caller)
) -> Any:
    try:
        sp = await drive.open(identity.user_id, space)
        out = await uploads.complete(sp, upload_id)
    except DriveError as exc:
        return _upload_error(exc)
    return JSONResponse(await _answer(sp, out), status_code=202)


@router.get("/uploads/{upload_id}")
async def get_upload(
    upload_id: str, space: str = Query(...), identity: LivefsIdentity = Depends(_caller)
) -> Any:
    try:
        sp = await drive.open(identity.user_id, space)
        out = await uploads.poll(await uploads.owned(sp, upload_id))
    except DriveError as exc:
        return _upload_error(exc)
    return await _answer(sp, out)


@router.delete("/uploads/{upload_id}")
async def cancel_upload(
    upload_id: str, space: str = Query(...), identity: LivefsIdentity = Depends(_caller)
) -> Any:
    """Ends the upload while it is sent or read back, so its version is
    never recorded: the status, ``aborted``. 409 ``upload_ended`` with its
    ``state`` once it has ended (``done``: the save stands)."""
    try:
        sp = await drive.open(identity.user_id, space)
        out = await uploads.cancel(sp, upload_id)
    except DriveError as exc:
        return _upload_error(exc)
    return await _answer(sp, out)
