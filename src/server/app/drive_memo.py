"""The Drive's memo routes: a file's mark, the agent's check-out, the record
of its versions and mark, a version's text and the attribution of a memo's
lines, a person's why after the save, and the one-step undo. Mounted beside
``drive``'s router, under the same prefix and the same rules of reach.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Query, Request

from src.server.app.drive import (
    ReadPrincipal,
    WritePrincipal,
    _actor,
    _entries_out,
    _entry_out,
    _Errors,
    _one_of,
    _titled,
    _version_out,
)
from src.server.app.drive_http import etag_of
from src.server.database.drive.changes import Checked
from src.server.database.drive.records import MemoEvent
from src.server.models.drive import (
    AttributionOut,
    AttributionRunOut,
    ChangeOut,
    CheckinIn,
    CheckoutIn,
    EntryOut,
    MemoIn,
    MemoOut,
    MemosOut,
    RecordEventOut,
    RecordOut,
    RecordVersionOut,
    UndoIn,
    VersionNoteIn,
    VersionOut,
    VersionTextOut,
)
from src.server.services import drive
from src.server.services.drive import extract, memo, texts

router = APIRouter(prefix="/api/v1/drive", tags=["Drive"])


@router.post("/{space}/versions/note", response_model=VersionOut)
async def explain_version(
    space: str, body: VersionNoteIn, request: Request, principal: WritePrincipal
) -> VersionOut:
    """Say why a version (the head by default) was made, where it does not
    say yet; only the person who made it may."""
    _one_of(body.path, body.id)
    with _Errors("say why a Drive version was made"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, body.path, entry_id=body.id)
        made = await memo.explain(
            sp, entry.entry_id, body.seq, body.note, actor=_actor(principal, request)
        )
        shown = _version_out(entry, made)
        await _titled(principal.user_id, [shown.actor])
        return shown


@router.post("/{space}/memo", response_model=EntryOut)
async def set_memo(
    space: str, body: MemoIn, request: Request, principal: WritePrincipal
) -> EntryOut:
    """Mark a file a memo, un-mark it, or set its status or tickers: only
    the fields the body carries change (``status: null`` clears it)."""
    _one_of(body.path, body.id)
    with _Errors("mark a Drive memo"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, body.path, entry_id=body.id)
        marked = await memo.set_memo(
            sp,
            entry.entry_id,
            actor=_actor(principal, request),
            change=body.model_dump(exclude_unset=True, include={"memo", "status", "tickers"}),
            note=body.note,
            if_match=etag_of(body.if_match),
        )
        return await _entry_out(principal.user_id, marked)


def _change_out(checked: Checked) -> ChangeOut:
    change = checked.change
    return ChangeOut(
        change_id=change.change_id,
        path=checked.entry.path,
        reason=change.reason,
        continued=checked.continued,
        versions=change.versions,
        first_seq=change.first_seq,
        last_seq=change.last_seq,
        head_seq=checked.entry.head_seq,
        adopted_seq=checked.adopted_seq,
    )


@router.post("/{space}/memo/checkout", response_model=ChangeOut)
async def checkout_memo(
    space: str, body: CheckoutIn, request: Request, principal: WritePrincipal
) -> ChangeOut:
    """Open the agent's change of a memo with a ``reason``, or ``continue``
    its chat's newest one; a save to the memo by this chat's agent that
    carried no reason, still its head, is put under it. Only a computer's
    agent checks out (``forbidden``)."""
    _one_of(body.path, body.id)
    with _Errors("check out a Drive memo"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, body.path, entry_id=body.id)
        checked = await memo.checkout(
            sp,
            entry.entry_id,
            actor=_actor(principal, request),
            reason=body.reason,
            carry_on=body.continue_,
        )
        return _change_out(checked)


@router.post("/{space}/memo/checkin", response_model=ChangeOut)
async def checkin_memo(
    space: str, body: CheckinIn, request: Request, principal: WritePrincipal
) -> ChangeOut:
    """End the agent's change of a memo, rewording it for every version
    made under it when a ``reason`` is sent."""
    _one_of(body.path, body.id)
    with _Errors("check in a Drive memo"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, body.path, entry_id=body.id)
        checked = await memo.checkin(
            sp, entry.entry_id, actor=_actor(principal, request), reason=body.reason
        )
        return _change_out(checked)


@router.get("/{space}/memos", response_model=MemosOut)
async def list_memos(
    space: str,
    principal: ReadPrincipal,
    sort: Literal["revised", "name", "path", "status"] = "revised",
    order: Literal["desc", "asc"] = "desc",
    status: str | None = Query(None, max_length=24),
    ticker: str | None = Query(None, max_length=24),
    limit: int = Query(100, ge=1, le=500),
    cursor: str | None = Query(None, max_length=200),
) -> MemosOut:
    """The space's memos, a page at a time, in the order and with the
    filters asked for."""
    with _Errors("list Drive memos"):
        sp = await drive.open(principal.user_id, space)
        page = await memo.memos(
            sp,
            sort=sort,
            descending=order == "desc",
            status=status,
            ticker=ticker,
            limit=limit,
            cursor=cursor,
        )
        shown = await _entries_out(principal.user_id, [entry for entry, _ in page.items])
    return MemosOut(
        memos=[
            MemoOut(entry=entry, versions=count)
            for entry, (_, count) in zip(shown, page.items, strict=True)
        ],
        next=page.next,
    )


@router.get("/{space}/record", response_model=RecordOut)
async def get_record(
    space: str,
    principal: ReadPrincipal,
    path: str | None = None,
    id: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    before: str | None = Query(None, max_length=200),
) -> RecordOut:
    """A file's versions and the changes to its mark, newest first."""
    _one_of(path, id)
    with _Errors("read a Drive file's record"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, path, entry_id=id)
        page = await memo.record(sp, entry.entry_id, before=before, limit=limit)
        items: list[RecordVersionOut | RecordEventOut] = []
        for item in page.items:
            if isinstance(item, MemoEvent):
                items.append(RecordEventOut.model_validate(item))
            else:
                items.append(
                    _version_out(
                        entry, item.version, RecordVersionOut,
                        added=item.added, removed=item.removed,
                    )
                )
        await _titled(principal.user_id, (i.actor for i in items))
    return RecordOut(items=items, next=page.next, counting=page.counting)


@router.get("/{space}/text", response_model=VersionTextOut)
async def get_version_text(
    space: str, principal: ReadPrincipal, id: str, seq: int = Query(..., ge=1)
) -> VersionTextOut:
    """A version's text: as stored, or read out of a Word or PDF file
    (503 with ``Retry-After`` while that is under way)."""
    with _Errors("read a Drive version's text"):
        sp = await drive.open(principal.user_id, space)
        return VersionTextOut.model_validate(await extract.version_text(sp, id, seq))


@router.get("/{space}/attribution", response_model=AttributionOut)
async def get_attribution(space: str, principal: ReadPrincipal, id: str) -> AttributionOut:
    """Which version wrote each line of a memo's current text."""
    with _Errors("attribute a Drive memo's lines"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, entry_id=id)
        found = await texts.attribution(sp, id)
        shown = [_version_out(entry, v) for v in found.versions]
        await _titled(principal.user_id, (v.actor for v in shown))
    return AttributionOut(
        seq=found.seq,
        sha256=found.sha256,
        line_count=found.line_count,
        runs=[AttributionRunOut(start=a, count=n, seq=s) for a, n, s in found.runs],
        versions=shown,
    )


@router.post("/{space}/undo", response_model=EntryOut)
async def undo_save(
    space: str, body: UndoIn, request: Request, principal: WritePrincipal
) -> EntryOut:
    """Put a file back as it was before its last save, as a new version."""
    _one_of(body.path, body.id)
    with _Errors("undo a Drive save"):
        sp = await drive.open(principal.user_id, space)
        entry = await drive.stat(sp, body.path, entry_id=body.id)
        back = await memo.undo(
            sp,
            entry.entry_id,
            actor=_actor(principal, request),
            if_match=etag_of(body.if_match),
            note=body.note,
        )
        return await _entry_out(principal.user_id, back)
