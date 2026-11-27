"""The Drive API: the user's server-held files, for the web app and the CLI.

Every route answers 404 unless the ``drive`` feature is on for the caller,
and 404 for a space the caller cannot reach, so neither tells a caller
anything it may not see. Tokens reach the routes through their scopes
(``drive:read``, ``drive:write``); a computer's token holds both, and what
it writes is recorded as its agent's, and the agent changes a memo only
inside a check-out (``memo/checkout``), whose reason each version carries,
or with a note sent with the save (``checkout_required``). Moving to the
trash is undoable, so a token may do it, as it may mark a memo and undo a
save. A person may say afterwards why they made a version; an agent says
why only through its check-out. Emptying the trash is for a signed-in
person only, and clearing a file's earlier versions for the person (signed
in, or their own CLI login), never a computer's token. The memo, record and
undo routes are ``drive_memo``'s, under the same prefix.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable
from typing import Annotated, Any, TypeVar
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from src.config.core import get_infrastructure_config
from src.server.app.drive_http import byte_range, etag_of, receive
from src.server.database.pool import DATABASE_UNAVAILABLE
from src.server.dependencies import cli_approval
from src.server.dependencies.principal import Principal, computer_call, require_scopes
from src.server.models.drive import (
    ChangesOut,
    EntriesOut,
    EntryOut,
    HistoryClearedOut,
    HistoryClearIn,
    ModifiedByOut,
    MoveIn,
    PathIn,
    PurgedOut,
    RestoredOut,
    SpaceOut,
    SpacesOut,
    TrashedOut,
    TrashIdIn,
    TrashOut,
    UploadIn,
    UploadSignIn,
    VersionOut,
    VersionRestoreIn,
    VersionsOut,
)
from src.server.services import drive
from src.server.services.drive.errors import http_status
from src.server.services.drive import Actor, Changes, DriveError, Entry, Version
from src.server.services.drive import extract, uploads
from src.server.services.shared_workspaces import access as shared_access
from src.server.utils.api import database_unavailable
from src.server.utils.uploads import read_body
from src.utils.storage import browser_uploads_enabled
from src.server.utils.http_headers import content_disposition

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/drive", tags=["Drive"])

ReadPrincipal = Annotated[Principal, Depends(require_scopes("drive:read"))]
WritePrincipal = Annotated[Principal, Depends(require_scopes("drive:write"))]

SITTING_HEADER = "X-Drive-Sitting"
_SITTING_MAX = 200

# Header types a client sends for a body it did not label, which say nothing
# about the file.
_UNLABELLED = {"", "application/octet-stream", "application/x-www-form-urlencoded"}


def _http(e: DriveError, headers: dict[str, str] | None = None) -> HTTPException:
    if e.code == "unavailable":
        wait = e.retry_after or 2
        headers = {"Retry-After": str(wait), **(headers or {})}
    detail: dict[str, Any] = {"type": e.code, "message": e.message, **e.details}
    return HTTPException(status_code=http_status(e.code), detail=detail, headers=headers)


class _Errors:
    """Answers a route's Drive and database failures as the API's errors."""

    def __init__(self, action: str) -> None:
        self.action = action

    def __enter__(self) -> None:
        return None

    def __exit__(self, kind: Any, error: BaseException | None, tb: Any) -> bool:
        if isinstance(error, DriveError):
            raise _http(error) from None
        if isinstance(error, DATABASE_UNAVAILABLE):
            raise database_unavailable(self.action, logger, error) from None
        return False


def _actor(principal: Principal, request: Request) -> Actor:
    sitting = (request.headers.get(SITTING_HEADER) or "").strip()[:_SITTING_MAX] or None
    if principal.computer_id is not None:
        call = computer_call(request.scope)
        return Actor(
            kind="agent",
            user_id=principal.user_id,
            via="cli",
            thread_id=call.thread_id if call is not None else None,
            computer_id=principal.computer_id,
            sitting=sitting,
        )
    via = {"session": "app", "service": "api"}.get(principal.auth, "cli")
    return Actor(kind="person", user_id=principal.user_id, via=via, sitting=sitting)


def _one_of(path: str | None, entry_id: str | None) -> None:
    if (path is None) == (entry_id is None):
        raise _http(DriveError("invalid_path", "Send a path or an id"))


@router.get("/spaces", response_model=SpacesOut)
async def list_spaces(principal: ReadPrincipal) -> SpacesOut:
    with _Errors("list Drive spaces"):
        if not await drive.enabled(principal.user_id):
            raise DriveError("not_found", "No such space")
        trash_days = get_infrastructure_config().drive.trash_days
        return SpacesOut(
            spaces=[
                SpaceOut(
                    id=s.space_id,
                    kind=s.kind,
                    created_at=s.created_at,
                    usage=await drive.usage(s),
                    change_seq=await drive.cursor(s),
                    trash_days=trash_days,
                )
                for s in await drive.spaces(principal.user_id)
            ]
        )


@router.get("/{space}/entries", response_model=EntriesOut)
async def list_entries(
    space: str, principal: ReadPrincipal, path: str = "", recursive: bool = False
) -> EntriesOut:
    with _Errors("list Drive entries"):
        sp = await drive.open(principal.user_id, space)
        found = await (drive.tree(sp, path) if recursive else drive.list_dir(sp, path))
        return EntriesOut(entries=found)


@router.get("/{space}/entry", response_model=EntryOut)
async def get_entry(
    space: str, principal: ReadPrincipal, path: str | None = None, id: str | None = None
) -> EntryOut:
    _one_of(path, id)
    with _Errors("read a Drive entry"):
        entry = await drive.stat(await drive.open(principal.user_id, space), path, entry_id=id)
        return await _entry_out(principal.user_id, entry)


async def _titled(user_id: str, actors: Iterable[ModifiedByOut | None]) -> None:
    """Give each of ``actors`` the title of the thread it wrote from, where
    the caller may see that thread; one lookup for the whole answer. Left
    off when the lookup fails: the answer may be of a change already made."""
    shown = [a for a in actors if a is not None and a.thread_id]
    try:
        titles = await shared_access.thread_titles(user_id, (a.thread_id for a in shown))
    except Exception:  # noqa: BLE001 - a title is never worth the answer
        logger.warning("[Drive] thread titles not read", exc_info=True)
        return
    for a in shown:
        a.thread_title = titles.get(a.thread_id)


async def _entries_out(user_id: str, entries: list[Entry]) -> list[EntryOut]:
    """``entries`` with the title of the thread each head was saved in."""
    out = [EntryOut.model_validate(e) for e in entries]
    await _titled(user_id, (e.modified_by for e in out))
    return out


async def _entry_out(user_id: str, entry: Entry) -> EntryOut:
    return (await _entries_out(user_id, [entry]))[0]


_V = TypeVar("_V", bound=VersionOut)


def _version_out(
    entry: Entry, version: Version, model: type[_V] = VersionOut, **extra: Any
) -> _V:
    """``version`` of ``entry`` as the API shows it, with how its text is
    read; ``extra``, a record's line counts."""
    kind = extract.text_kind(entry.name, version.content_type, version.is_text)
    return model.model_validate(dataclasses.asdict(version) | {"text": kind} | extra)


@router.get("/{space}/content")
async def get_content(
    space: str,
    request: Request,
    principal: ReadPrincipal,
    path: str | None = None,
    id: str | None = None,
    seq: int | None = Query(None, ge=1),
    download: bool = False,
) -> Response:
    _one_of(path, id)
    with _Errors("read a Drive file"):
        sp = await drive.open(principal.user_id, space)
        entry, found = await drive.content(sp, path, entry_id=id, seq=seq)
    media = found.content_type
    if found.is_text and media.startswith("text/") and "charset" not in media:
        media += "; charset=utf-8"
    headers = {
        "ETag": f'"{found.sha256}"',
        "Accept-Ranges": "bytes",
        "Cache-Control": "private, no-cache",
        # The bytes are anyone's who can write to the Drive, an agent's
        # included: opened from this origin, they must not run.
        "Content-Security-Policy": "sandbox; default-src 'none'",
        "X-Content-Type-Options": "nosniff",
    }
    if download:
        headers["Content-Disposition"] = content_disposition(entry.name)
    if etag_of(request.headers.get("if-none-match")) == found.sha256:
        return Response(status_code=304, headers=headers)
    span = None
    if_range = request.headers.get("if-range")
    if if_range is None or etag_of(if_range) == found.sha256:
        try:
            span = byte_range(request.headers.get("range"), found.size)
        except DriveError as e:
            raise _http(e, {"Content-Range": f"bytes */{found.size}"}) from None
    start, end = span if span is not None else (0, found.size)
    headers["Content-Length"] = str(end - start)
    status = 200
    if span is not None:
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end - 1}/{found.size}"
    return StreamingResponse(
        drive.stream(found, start, end), status_code=status, media_type=media, headers=headers
    )


@router.put("/{space}/content", response_model=EntryOut)
async def put_content(
    space: str,
    request: Request,
    principal: WritePrincipal,
    path: str = Query(..., min_length=1, max_length=2048),
    note: str | None = Query(None, max_length=500),
    as_memo: bool | None = Query(None, alias="memo"),
    status: str | None = Query(None, max_length=24),
    tickers: str | None = Query(None, max_length=600),
) -> EntryOut:
    """Save a file. ``memo=true`` with ``if-none-match: *`` makes a new file
    a memo as it is saved, with ``status`` and ``tickers`` (comma separated);
    a bad ticker refuses the save."""
    given = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    mark: dict[str, Any] | None = None
    if as_memo is not None or status is not None or tickers is not None:
        mark = {"memo": as_memo}
        if status is not None:
            mark["status"] = status
        if tickers is not None:
            mark["tickers"] = tickers.split(",")
    with _Errors("save a Drive file"):
        sp = await drive.open(principal.user_id, space)
        bounds = await drive.limits(sp)
        async with receive(request, bounds) as data:
            saved = await drive.write(
                sp,
                path,
                data,
                actor=_actor(principal, request),
                if_match=etag_of(request.headers.get("if-match")),
                if_none_match=(request.headers.get("if-none-match") or "").strip() == "*",
                note=note,
                content_type=None if given in _UNLABELLED else given,
                bounds=bounds,
                mark=mark,
            )
        return await _entry_out(principal.user_id, saved)


@router.post("/{space}/folders", response_model=EntryOut)
async def make_folder(space: str, body: PathIn, principal: WritePrincipal) -> EntryOut:
    with _Errors("make a Drive folder"):
        made = await drive.mkdir(await drive.open(principal.user_id, space), body.path)
        return await _entry_out(principal.user_id, made)


@router.post("/{space}/move", response_model=EntryOut)
async def move_entry(space: str, body: MoveIn, principal: WritePrincipal) -> EntryOut:
    with _Errors("move a Drive entry"):
        sp = await drive.open(principal.user_id, space)
        return await _entry_out(principal.user_id, await drive.move(sp, body.src, body.dst))


@router.post("/{space}/trash", response_model=TrashedOut)
async def trash_entry(
    space: str, body: PathIn, request: Request, principal: WritePrincipal
) -> TrashedOut:
    with _Errors("trash a Drive entry"):
        sp = await drive.open(principal.user_id, space)
        trash_id = await drive.trash(sp, body.path, actor=_actor(principal, request))
        return TrashedOut(trash_id=trash_id)


@router.get("/{space}/trash", response_model=TrashOut)
async def list_trash(
    space: str, principal: ReadPrincipal, limit: int = Query(200, ge=1, le=1000)
) -> TrashOut:
    with _Errors("list the Drive trash"):
        sp = await drive.open(principal.user_id, space)
        found = await drive.trashed(sp, limit=limit + 1)
        return TrashOut(items=found[:limit], more=len(found) > limit)


@router.post("/{space}/trash/restore", response_model=RestoredOut)
async def restore_trash(space: str, body: TrashIdIn, principal: WritePrincipal) -> RestoredOut:
    with _Errors("restore from the Drive trash"):
        sp = await drive.open(principal.user_id, space)
        back = await drive.restore(sp, body.trash_id)
        return RestoredOut(entries=await _entries_out(principal.user_id, back))


@router.delete("/{space}/trash", response_model=PurgedOut)
async def empty_trash(
    space: str, principal: WritePrincipal, trash_id: str | None = None
) -> PurgedOut:
    if principal.auth != "session":
        raise _http(DriveError("forbidden", "Only a signed-in person empties the trash"))
    with _Errors("empty the Drive trash"):
        sp = await drive.open(principal.user_id, space)
        return PurgedOut(purged=await drive.purge(sp, trash_id))


@router.get("/{space}/versions", response_model=VersionsOut)
async def list_versions(
    space: str, principal: ReadPrincipal, path: str | None = None, id: str | None = None
) -> VersionsOut:
    _one_of(path, id)
    with _Errors("list Drive versions"):
        sp = await drive.open(principal.user_id, space)
        entry, found, too_large, dropped, freed = await drive.versions(sp, path, entry_id=id)
        shown = [_version_out(entry, v) for v in found]
        await _titled(principal.user_id, (v.actor for v in shown))
        return VersionsOut(
            versions=shown,
            history_bytes=freed,
            clearable=dropped,
            too_large_for_history=too_large,
            memo=entry.memo,
        )


#: What a computer's agent is told when it asks to clear a file's history.
CLEAR_REFUSED = (
    "Only the user clears a file's earlier versions, from the Drive page or "
    "their own la login. Ask them to."
)


@router.post("/{space}/versions/clear", response_model=HistoryClearedOut)
async def clear_history(
    space: str, body: HistoryClearIn, request: Request, principal: WritePrincipal
) -> HistoryClearedOut:
    """Drop a file's old versions, all or those below ``before``; the head
    stays. Bytes another version or file still uses are kept. Nothing brings
    them back, so a computer's token is refused rather than asked for: an
    approval kind an older app does not know would break it."""
    if cli_approval.held_for_approval(principal):
        raise _http(DriveError("forbidden", CLEAR_REFUSED))
    _one_of(body.path, body.id)
    with _Errors("clear a Drive file's history"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, body.path, entry_id=body.id)
        dropped, freed = await drive.clear_history(
            sp,
            entry.entry_id,
            before=body.before,
            pinned=body.final_seq if "final_seq" in body.model_fields_set else ...,
            actor=_actor(principal, request),
        )
        return HistoryClearedOut(dropped=dropped, freed_bytes=freed)


@router.post("/{space}/versions/restore", response_model=EntryOut)
async def restore_version(
    space: str, body: VersionRestoreIn, request: Request, principal: WritePrincipal
) -> EntryOut:
    with _Errors("restore a Drive version"):
        sp = await drive.open(principal.user_id, space)
        back = await drive.restore_version(
            sp, body.id, body.seq, actor=_actor(principal, request), note=body.note
        )
        return await _entry_out(principal.user_id, back)


@router.get("/{space}/changes", response_model=ChangesOut)
async def list_changes(
    space: str,
    principal: ReadPrincipal,
    since: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
) -> Changes:
    """Past ``limit`` changed entries the answer is ``resync``: list the
    space again instead."""
    with _Errors("list Drive changes"):
        sp = await drive.open(principal.user_id, space)
        return await drive.changes(sp, since, limit=limit)


# --- files sent in parts, and signed downloads --------------------------------


def _upload_json(out: dict[str, Any]) -> dict[str, Any]:
    if isinstance(out.get("entry"), Entry):
        out["entry"] = EntryOut.model_validate(out["entry"]).model_dump(mode="json")
    return out


@router.post("/{space}/uploads")
async def begin_upload(
    space: str, body: UploadIn, request: Request, principal: WritePrincipal
) -> dict[str, Any]:
    """How to send a file: ``held`` (recorded, nothing to send), ``single``
    (``PUT /content``), or ``direct`` / ``relay`` in parts."""
    with _Errors("begin a Drive upload"):
        sp = await drive.open(principal.user_id, space)
        out = await uploads.begin(
            sp,
            _actor(principal, request),
            body,
            direct=principal.auth != "session" or browser_uploads_enabled(),
        )
    return _upload_json(out)


@router.post("/{space}/uploads/{upload_id}/parts")
async def sign_upload_parts(
    space: str, upload_id: str, body: UploadSignIn, principal: WritePrincipal
) -> dict[str, Any]:
    with _Errors("sign Drive upload parts"):
        sp = await drive.open(principal.user_id, space)
        return await uploads.sign(
            sp, upload_id, [p.model_dump() for p in body.parts], for_browser=True
        )


@router.put("/{space}/uploads/{upload_id}/parts/{number}")
async def send_upload_part(
    space: str, upload_id: str, number: int, request: Request, principal: WritePrincipal
) -> dict[str, Any]:
    """One part through the server, held here only until the store has it."""
    with _Errors("send a Drive upload part"):
        sp = await drive.open(principal.user_id, space)
        return await uploads.relay_part(
            sp,
            upload_id,
            number,
            request.headers.get("content-md5"),
            lambda cap: read_body(request, cap),
        )


@router.post("/{space}/uploads/{upload_id}/complete", status_code=202)
async def complete_upload(
    space: str, upload_id: str, principal: WritePrincipal
) -> dict[str, Any]:
    with _Errors("complete a Drive upload"):
        sp = await drive.open(principal.user_id, space)
        return _upload_json(await uploads.complete(sp, upload_id))


@router.get("/{space}/uploads/{upload_id}")
async def get_upload(space: str, upload_id: str, principal: ReadPrincipal) -> dict[str, Any]:
    with _Errors("read a Drive upload"):
        sp = await drive.open(principal.user_id, space)
        return _upload_json(await uploads.poll(await uploads.owned(sp, upload_id)))


@router.delete("/{space}/uploads/{upload_id}")
async def cancel_upload(space: str, upload_id: str, principal: WritePrincipal) -> dict[str, Any]:
    """The upload's status, ``aborted``; 409 ``upload_ended`` (with its
    ``state``) once it has ended."""
    with _Errors("cancel a Drive upload"):
        sp = await drive.open(principal.user_id, space)
        return _upload_json(await uploads.cancel(sp, upload_id))


@router.get("/{space}/content/link")
async def content_link(
    space: str,
    principal: ReadPrincipal,
    path: str | None = None,
    id: str | None = None,
    seq: int | None = Query(None, ge=1),
) -> dict[str, Any]:
    """A short-lived address for a file's bytes: signed on the store when it
    holds them, else this API's own download route."""
    _one_of(path, id)
    with _Errors("link a Drive file"):
        sp = await drive.open(principal.user_id, space)
        entry, found = await drive.content(sp, path, entry_id=id, seq=seq)
        url = await drive.signed_url(found, disposition=content_disposition(entry.name))
    signed = url is not None
    if url is None:
        query = urlencode({"id": entry.entry_id, "seq": seq or entry.head_seq, "download": "true"})
        url = f"{router.prefix}/{space}/content?{query}"
    return {
        "url": url,
        "signed": signed,
        "expires_in": drive.LINK_TTL_S,
        "size": found.size,
        "sha256": found.sha256,
        "name": entry.name,
    }
