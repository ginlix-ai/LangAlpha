"""What the record and a memo's "who wrote what" make of its versions'
text: lines added and removed against the version before, and which
version wrote each line of the head.

Line counts are cached by the two contents and attribution by version
under the file's version epoch (``database/drive/texts``). Both are worked
out outside the space's lock; a request that outlasts its wait answers
``unavailable`` while the work goes on and is cached.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace

from src.server.database.drive import reads
from src.server.database.drive import texts as rows
from src.server.database.drive.records import (
    Attribution,
    DriveError,
    Entry,
    RecordPage,
    RecordVersion,
    Space,
)
from src.server.database.drive.texts import NO_BASE, Chain, Stored
from src.server.services.drive import extract
from src.server.services.drive.extract import Kind, TextKind, pending, shared, text_kind
from src.server.services.drive.extract_child import EMPTY, OK
from src.server.services.drive.linediff import counts, lines_of, packed, runs_of, step, unpacked

__all__ = ["attribution", "fill_counts"]

#: How long a request waits for attribution before ``unavailable``; the
#: walk goes on to ``WALK_BUDGET_S`` and caches each step, so a later call
#: starts where it got to.
ATTRIBUTION_WAIT_S = 20.0
WALK_BUDGET_S = 60.0
#: Time a record page spends counting lines; the rest stay null for the
#: next read.
COUNT_BUDGET_S = 10.0
#: Text fetched from the database at once while reading many versions.
_BATCH_BYTES = 8 * 1024 * 1024
_READ_OK = (OK, EMPTY)


class _Texts:
    """Versions' lines in the order they are needed, fetched a batch at a
    time ahead of need and dropped once passed, so a long read holds a
    bounded slice. An extracted version's lines come only from its cached
    extract: reading never starts an extraction."""

    def __init__(self, space_id: str, order: list[tuple[Stored, Kind | None]]) -> None:
        #: Each version with the reader that extracts it, None for its own text.
        self.space_id = space_id
        self.order = order
        self.held: dict[str, list[str] | None] = {}

    async def lines(self, index: int) -> list[str] | None:
        sha = self.order[index][0].sha256
        if sha not in self.held:
            texts: list[str] = []
            extracted: dict[str, Kind] = {}
            size = 0
            for v, kind in self.order[index:]:
                if v.sha256 in self.held or v.sha256 in texts or v.sha256 in extracted:
                    continue
                # An extract's cached text can outgrow its upload, up to the cap.
                cost = v.size if kind is None else extract.TEXT_CAP_BYTES
                if (texts or extracted) and size + cost > _BATCH_BYTES:
                    break
                if kind is None:
                    texts.append(v.sha256)
                else:
                    extracted[v.sha256] = kind
                size += cost
            keep = {v.sha256 for v, _ in self.order[max(0, index - 1) :]}
            self.held = {k: v for k, v in self.held.items() if k in keep}
            for k, text in (await rows.held_texts(self.space_id, texts)).items():
                self.held[k] = lines_of(text)
            for k, got in (await rows.extracts(self.space_id, list(extracted))).items():
                # Nothing is counted from another reader's empty answer, nor
                # from text cut at the cap, whose lines are not the file's.
                if extract.answers(got, extracted[k]) and got.status in _READ_OK and not got.truncated:
                    self.held[k] = lines_of(got.text or "")
            for k in extracted:
                self.held.setdefault(k, None)
        return self.held.get(sha)


# --- record counts --------------------------------------------------------


async def fill_counts(entry: Entry, page: RecordPage) -> RecordPage:
    """``page`` with each version's lines added and removed against the one
    before it, cached ones read and missing ones counted, for at most
    ``COUNT_BUDGET_S``, past which the page says it is still ``counting``.
    A Word or PDF version is counted only when both extracts are cached:
    listing a record never starts an extraction."""
    shown = [i for i in page.items if isinstance(i, RecordVersion)]
    if not shown:
        return page
    links = await rows.with_bases(entry.entry_id, [i.version.seq for i in shown])

    def how(v: Stored) -> TextKind:
        return text_kind(entry.name, v.content_type, v.is_text)

    countable = [
        (v, b) for v, b in links if how(v) != "none" and (b is None or how(b) != "none")
    ]
    key = {v.seq: (b.sha256 if b else NO_BASE, v.sha256) for v, b in countable}
    cached = await rows.line_counts(entry.space_id, list(set(key.values())))
    found = {seq: cached[pair] for seq, pair in key.items() if pair in cached}
    missing = [(v, b) for v, b in countable if v.seq not in found]
    def reader(v: Stored) -> Kind | None:
        return extract.reader(entry.name, v.content_type, v.is_text)

    order: list[tuple[Stored, Kind | None]] = []
    for v, b in missing:
        order += ([(b, reader(b))] if b is not None else []) + [(v, reader(v))]
    texts = _Texts(entry.space_id, order)
    deadline = time.monotonic() + COUNT_BUDGET_S
    at = 0
    cut = False
    for v, b in missing:
        start, at = at, at + (2 if b is not None else 1)
        if time.monotonic() > deadline:
            cut = True
            continue
        if b is not None and b.sha256 == v.sha256:
            n = (0, 0)
        else:
            before = [] if b is None else await texts.lines(start)
            after = await texts.lines(at - 1)
            if before is None or after is None:
                continue
            n = await asyncio.to_thread(counts, before, after)
        await rows.put_line_counts(entry.space_id, *key[v.seq], *n)
        found[v.seq] = n
    items = []
    for item in page.items:
        if isinstance(item, RecordVersion):
            added, removed = found.get(item.version.seq, (None, None))
            item = replace(item, added=added, removed=removed)
        items.append(item)
    return RecordPage(items=items, next=page.next, counting=cut)


# --- attribution ----------------------------------------------------------

_attributing: dict[tuple[str, str], asyncio.Task[Attribution]] = {}


async def _walk(entry: Entry, found: Chain) -> Attribution:
    """Attribution of the head: from the newest version with one cached
    under the file's current epoch, forward a version at a time, caching
    each step. Attribution begins again at the first version and at the
    first after one with no text."""
    deadline = time.monotonic() + WALK_BUDGET_S
    vs = found.versions
    texty = [text_kind(entry.name, v.content_type, v.is_text) == "text" for v in vs]
    if not vs or not texty[-1]:
        raise DriveError("not_text", f"{entry.path} is not text")
    head = len(vs) - 1
    begin = head
    while begin > 0 and texty[begin - 1]:
        begin -= 1
    texts = _Texts(entry.space_id, [(v, None) for v in vs[begin:]])

    async def lines(i: int) -> list[str]:
        got = await texts.lines(i - begin)
        if got is None:
            # Dropped and freed since the chain was read: read it again.
            raise pending("The file changed while it was read; try again shortly")
        return got

    cached = [i for i in range(begin, head + 1) if vs[i].seq in found.attributed]
    start, by = begin, None
    if cached:
        start = cached[-1]
        pairs = await rows.blame(entry.entry_id, vs[start].seq, found.epoch)
        by = unpacked(pairs) if pairs is not None else None
    if by is None:
        start = begin
        by = [vs[begin].seq] * len(await lines(begin))
        await rows.put_attribution(entry.entry_id, vs[begin].seq, found.epoch, packed(by))
    prev = await lines(start)
    for i in range(start + 1, head + 1):
        if time.monotonic() > deadline:
            raise pending("Still working out who wrote what; try again shortly")
        now = await lines(i)
        added, removed, by = await asyncio.to_thread(step, prev, now, by, vs[i].seq)
        await rows.put_attribution(entry.entry_id, vs[i].seq, found.epoch, packed(by))
        await rows.put_line_counts(entry.space_id, vs[i - 1].sha256, vs[i].sha256, added, removed)
        prev = now
    runs = runs_of(packed(by))
    named = sorted({seq for _, _, seq in runs})
    return Attribution(
        seq=vs[head].seq,
        sha256=vs[head].sha256,
        line_count=len(by),
        runs=runs,
        versions=await rows.versions(entry.entry_id, named) if named else [],
    )


async def attribution(space: Space, entry_id: str) -> Attribution:
    """Which version wrote each line of a memo's current text. A live memo
    only (``not_memo``) whose head is text (``not_text``); ``unavailable``
    while the walk takes longer than ``ATTRIBUTION_WAIT_S``."""
    entry = await reads.live_file(space.space_id, entry_id)
    if not entry.is_memo:
        raise DriveError("not_memo", f"{entry.path} is not a memo")
    if text_kind(entry.name, entry.content_type, bool(entry.is_text)) != "text":
        raise DriveError("not_text", f"{entry.path} is not text")
    # By head as well as bytes: a restore brings back a digest under a new
    # head, whose lines are credited differently. By epoch too: a clear
    # keeps the head, and a walk of the chain before it credits versions
    # that are gone.
    found = await rows.chain(entry.entry_id)
    head = f"{entry.head_seq}:{entry.sha256 or ''}:{found.epoch}"
    task = shared(_attributing, (entry.entry_id, head), lambda: _walk(entry, found))
    try:
        return await asyncio.wait_for(asyncio.shield(task), ATTRIBUTION_WAIT_S)
    except TimeoutError:
        raise pending("Still working out who wrote what; try again shortly") from None
