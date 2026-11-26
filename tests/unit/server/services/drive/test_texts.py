"""Record line counts and the cached attribution walk, which must always
equal working attribution out whole (``attribute``, the oracle here).

The cache's rules are SQL (``database/drive/texts`` and the epoch trigger
in migration 154); ``_Store`` keeps the same rules in memory: counts by the
two contents, attributions kept under the file's epoch, which moves when a
version's content changes (a fold) or a version goes, and a write made
under an epoch that has moved is not kept."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.server.database.drive.records import (
    Actor,
    Entry,
    MemoMark,
    RecordPage,
    RecordVersion,
    Version,
)
from src.server.database.drive.texts import NO_BASE, Chain, Extract, Stored
from src.server.services.drive import texts
from src.server.services.drive.linediff import lines_of, packed, runs_of, step

_NOW = datetime.now(timezone.utc)
_ACTOR = Actor(kind="person", user_id="u", via="app")


def attribute(chain: list[tuple[int, list[str] | None]]) -> list[int]:
    """Which version wrote each line of the last of ``chain`` (``(seq,
    lines)`` oldest first, ``lines`` None for a version with no text),
    worked out whole."""
    by: list[int] = []
    prev: list[str] | None = None
    for seq, lines in chain:
        if lines is None:
            prev = None
            continue
        if prev is None:
            by = [seq] * len(lines)
        else:
            _, _, by = step(prev, lines, by, seq)
        prev = lines
    return by


class _Store:
    def __init__(self) -> None:
        #: seq -> (sha, text or None for a binary version, content type)
        self.versions: dict[int, tuple[str, str | None, str]] = {}
        self.epoch = 0
        self.attributions: dict[int, tuple[int, list[int]]] = {}
        self.counts: dict[tuple[str, str], tuple[int, int]] = {}
        self.extracted: dict[str, Extract] = {}
        #: Bytes outlive their version for a while (released, not freed).
        self.held: dict[str, str] = {}
        self.writes = 0
        self.text_reads = 0
        self.extract_reads = 0

    def save(self, seq: int, text: str | None, content_type: str | None = None) -> None:
        if seq in self.versions:
            self.epoch += 1  # a fold
        sha = f"{seq}:{text}:{content_type}"
        kind = content_type or ("text/markdown" if text is not None else "image/png")
        self.versions[seq] = (sha, text, kind)
        if text is not None:
            self.held[sha] = text

    def drop(self, *seqs: int) -> None:
        for seq in seqs:
            del self.versions[seq]
            self.attributions.pop(seq, None)
        self.epoch += 1

    def _stored(self, seq: int) -> Stored:
        sha, text, kind = self.versions[seq]
        is_text = text is not None
        return Stored(seq=seq, sha256=sha, is_text=is_text, content_type=kind, size=len(text or ""))

    async def chain(self, entry_id):
        seqs = sorted(self.versions)
        return Chain(
            epoch=self.epoch,
            versions=[self._stored(s) for s in seqs],
            attributed={s for s, (e, _) in self.attributions.items() if e == self.epoch},
        )

    async def blame(self, entry_id, seq, epoch):
        row = self.attributions.get(seq)
        return list(row[1]) if row and row[0] == epoch else None

    async def put_attribution(self, entry_id, seq, epoch, blame):
        if epoch != self.epoch or seq not in self.versions:
            return False
        self.attributions[seq] = (epoch, list(blame))
        self.writes += 1
        return True

    async def put_line_counts(self, space_id, base, sha, added, removed):
        self.counts.setdefault((base, sha), (added, removed))

    async def line_counts(self, space_id, pairs):
        return {p: self.counts[p] for p in pairs if p in self.counts}

    async def with_bases(self, entry_id, seqs):
        order = sorted(self.versions)
        return [
            (self._stored(s), self._stored(order[i - 1]) if i else None)
            for i, s in enumerate(order)
            if s in seqs
        ]

    async def held_texts(self, space_id, shas):
        self.text_reads += 1
        return {sha: text for sha, text in self.held.items() if sha in shas}

    async def extracts(self, space_id, shas):
        self.extract_reads += 1
        return {sha: e for sha, e in self.extracted.items() if sha in shas}

    async def versions_of(self, entry_id, seqs):
        return [_version(self._stored(s)) for s in seqs if s in self.versions]

    def full(self) -> list[tuple[int, int, int]]:
        chain = [
            (seq, lines_of(text) if text is not None and kind != "application/pdf" else None)
            for seq, (_, text, kind) in sorted(self.versions.items())
        ]
        return runs_of(packed(attribute(chain)))


def _version(v: Stored) -> Version:
    return Version(
        seq=v.seq, sha256=v.sha256, size=v.size, content_type=v.content_type,
        created_at=_NOW, note=None, restored_from=None, actor=_ACTOR, is_text=v.is_text,
    )


@pytest.fixture
def store(monkeypatch):
    found = _Store()
    for name in (
        "chain", "blame", "put_attribution", "put_line_counts", "line_counts",
        "with_bases", "held_texts", "extracts",
    ):
        monkeypatch.setattr(texts.rows, name, getattr(found, name))
    monkeypatch.setattr(texts.rows, "versions", found.versions_of)
    return found


_ENTRY = Entry(
    entry_id="e", space_id="s", path="memo.md", parent="", name="memo.md", kind="file",
    head_seq=None, sha256=None, size=None, content_type="text/markdown", is_text=True,
    created_at=_NOW, modified_at=_NOW, changed_seq=0, deleted_at=None, trash_id=None,
    deleted_by=None, modified_by=None,
    memo=MemoMark(marked=True, status=None, tickers=(), final_seq=None),
)


def _text(n: int) -> str:
    lines = [f"para {i}" for i in range(12)]
    for k in range(1, n + 1):
        i = (k * 7) % len(lines)
        if k % 3 == 0:
            lines.insert(i, f"added at {k}")
        elif k % 3 == 1:
            lines[i] = f"edited at {k}"
        elif len(lines) > 5:
            del lines[i]
    return "\n".join(lines) + "\n"


async def _walked(store: _Store) -> list[tuple[int, int, int]]:
    return (await texts._walk(_ENTRY, await texts.rows.chain(_ENTRY.entry_id))).runs


# --- attribution ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_incremental_attribution_equals_a_full_recompute(store):
    for seq in range(1, 31):
        store.save(seq, _text(seq))
        if seq % 4 == 0:
            assert await _walked(store) == store.full()
    assert await _walked(store) == store.full()
    writes = store.writes
    assert await _walked(store) == store.full()
    assert store.writes == writes, "a warm walk writes nothing"


@pytest.mark.asyncio
async def test_a_restored_head_does_not_join_the_walk_of_the_head_it_copies(monkeypatch):
    """v3 restores v1's bytes while v1's walk still runs: its lines are
    credited differently, so it gets a walk of its own."""
    import asyncio
    import dataclasses

    release, walked = asyncio.Event(), []

    async def walk(entry, found):
        walked.append(entry.head_seq)
        await release.wait()
        return entry.head_seq

    heads = iter([dataclasses.replace(_ENTRY, head_seq=seq, sha256="sha-a") for seq in (1, 3)])

    async def live_file(space_id, entry_id):
        return next(heads)

    async def chain(entry_id):
        return Chain(epoch=0, versions=[], attributed=set())

    monkeypatch.setattr(texts, "_walk", walk)
    monkeypatch.setattr(texts.reads, "live_file", live_file)
    monkeypatch.setattr(texts.rows, "chain", chain)
    monkeypatch.setattr(texts, "_attributing", {})
    space = type("S", (), {"space_id": "s"})()
    first = asyncio.create_task(texts.attribution(space, "e"))
    await asyncio.sleep(0)
    second = asyncio.create_task(texts.attribution(space, "e"))
    await asyncio.sleep(0)
    release.set()
    assert (await first, await second) == (1, 3) and walked == [1, 3]


@pytest.mark.asyncio
async def test_a_request_after_a_clear_does_not_join_the_walk_from_before_it(store, monkeypatch):
    """A clear keeps the head and its bytes, so only the epoch tells the
    walk begun on the old chain from the one a later request wants."""
    import asyncio
    import dataclasses

    for seq in range(1, 11):
        store.save(seq, _text(seq))
    entry = dataclasses.replace(_ENTRY, head_seq=10, sha256=store.versions[10][0])
    real, read, cleared = store.held_texts, asyncio.Event(), asyncio.Event()

    async def held_texts(space_id, shas):
        if not read.is_set():
            read.set()
            await cleared.wait()
        return await real(space_id, shas)

    async def live_file(space_id, entry_id):
        return entry

    monkeypatch.setattr(texts.rows, "held_texts", held_texts)
    monkeypatch.setattr(texts.reads, "live_file", live_file)
    monkeypatch.setattr(texts, "_attributing", {})
    space = type("S", (), {"space_id": "s"})()
    before = asyncio.create_task(texts.attribution(space, "e"))
    await read.wait()
    store.drop(*range(1, 6))
    after = asyncio.create_task(texts.attribution(space, "e"))
    await asyncio.sleep(0)
    cleared.set()
    await before
    assert (await after).runs == store.full()
    assert {v.seq for v in (await after).versions} <= set(range(6, 11))


@pytest.mark.asyncio
async def test_new_versions_extend_the_cached_walk(store):
    for seq in range(1, 11):
        store.save(seq, _text(seq))
    await _walked(store)
    writes = store.writes
    store.save(11, _text(11))
    assert await _walked(store) == store.full()
    assert store.writes == writes + 1


@pytest.mark.asyncio
async def test_after_a_fold_the_walk_starts_again_and_still_equals(store):
    for seq in range(1, 11):
        store.save(seq, _text(seq))
    await _walked(store)
    store.save(10, _text(10) + "folded\n")
    assert await _walked(store) == store.full()


@pytest.mark.asyncio
async def test_after_a_clear_no_line_is_credited_to_a_dropped_version(store):
    for seq in range(1, 21):
        store.save(seq, _text(seq))
    await _walked(store)
    store.drop(*range(1, 10))
    assert await _walked(store) == store.full()
    assert min(seq for _, _, seq in await _walked(store)) == 10


@pytest.mark.asyncio
async def test_a_dropped_middle_version_rebases_its_successor(store):
    for seq in range(1, 16):
        store.save(seq, _text(seq))
    await _walked(store)
    store.drop(8, 9)
    assert await _walked(store) == store.full()


@pytest.mark.asyncio
async def test_a_version_with_no_text_starts_attribution_again(store):
    for seq in range(1, 6):
        store.save(seq, _text(seq))
    store.save(6, None)
    store.save(7, "%PDF ascii but a pdf\n", "application/pdf")
    for seq in range(8, 12):
        store.save(seq, _text(seq))
    assert await _walked(store) == store.full()
    assert {seq for _, _, seq in await _walked(store)} <= set(range(8, 12))


@pytest.mark.asyncio
async def test_a_walker_that_read_a_changed_chain_caches_nothing(store, monkeypatch):
    """A clear between reading the chain and writing a step leaves every
    step the walk writes unkept: they were worked out on the old chain."""
    for seq in range(1, 11):
        store.save(seq, _text(seq))
    real = store.chain

    async def stale(entry_id):
        found = await real(entry_id)
        store.drop(5)
        return found

    monkeypatch.setattr(texts.rows, "chain", stale)
    await texts._walk(_ENTRY, await texts.rows.chain(_ENTRY.entry_id))
    assert store.attributions == {}, "a stale walk writes no attribution under any epoch"
    monkeypatch.setattr(texts.rows, "chain", real)
    assert await _walked(store) == store.full()


# --- record counts -------------------------------------------------------------


def _page(store: _Store) -> RecordPage:
    return RecordPage(
        items=[
            RecordVersion(version=_version(store._stored(s)), added=None, removed=None)
            for s in sorted(store.versions, reverse=True)
        ],
        next=None,
    )


def _shown(page: RecordPage) -> list[tuple[int, int | None, int | None]]:
    return [(i.version.seq, i.added, i.removed) for i in page.items]


@pytest.mark.asyncio
async def test_counts_are_cached_by_content_and_read_in_batches(store):
    for seq in range(1, 41):
        store.save(seq, _text(seq))
    page = await texts.fill_counts(_ENTRY, _page(store))
    assert all(a is not None for _, a, _ in _shown(page))
    assert _shown(page)[-1] == (1, len(lines_of(_text(1))), 0)
    assert store.text_reads == 1, "one batch for forty versions"
    store.text_reads = 0
    assert _shown(await texts.fill_counts(_ENTRY, _page(store))) == _shown(page)
    assert store.text_reads == 0, "cached counts read no text"


@pytest.mark.asyncio
async def test_extracted_versions_are_batched_by_their_cap_not_their_upload(store):
    """A Word file's text can be far larger than its upload, so a run of
    tiny uploads is not one batch: 8 MiB holds 8 extracts at the 1 MiB cap."""
    entry = Entry(**{**_ENTRY.__dict__, "name": "note.pdf", "content_type": "application/pdf"})
    for seq in range(1, 21):
        store.save(seq, f"%PDF {seq}", "application/pdf")
    await texts.fill_counts(entry, _page(store))
    assert store.extract_reads == 3, "20 extracts at 8 a batch"


@pytest.mark.asyncio
async def test_a_page_out_of_time_says_it_is_still_counting(store, monkeypatch):
    for seq in range(1, 4):
        store.save(seq, _text(seq))
    with monkeypatch.context() as short:
        short.setattr(texts, "COUNT_BUDGET_S", -1.0)
        page = await texts.fill_counts(_ENTRY, _page(store))
    assert page.counting and all(a is None for _, a, _ in _shown(page))
    page = await texts.fill_counts(_ENTRY, _page(store))
    assert not page.counting and all(a is not None for _, a, _ in _shown(page))


@pytest.mark.asyncio
async def test_extracted_versions_count_only_with_both_extracts_cached(store):
    entry = Entry(**{**_ENTRY.__dict__, "name": "note.pdf", "content_type": "application/pdf"})
    store.save(1, "%PDF one", "application/pdf")
    store.save(2, "%PDF two", "application/pdf")
    first, second = (store.versions[s][0] for s in (1, 2))
    page = await texts.fill_counts(entry, _page(store))
    assert _shown(page) == [(2, None, None), (1, None, None)]
    assert not page.counting, "nothing a later read of the record would fill in"
    store.extracted[second] = Extract("pdf", "ok", "a\nb\n", False, False)
    store.extracted[first] = Extract("pdf", "ok", "a\n", False, False)
    page = await texts.fill_counts(entry, _page(store))
    assert _shown(page) == [(2, 1, 0), (1, 1, 0)]
    assert (NO_BASE, first) in store.counts


@pytest.mark.asyncio
async def test_another_readers_empty_answer_is_not_counted_from(store):
    """The cache is by digest: the Word reader finding no text in these bytes
    says nothing of what the PDF reader finds, though text it read does."""
    entry = Entry(**{**_ENTRY.__dict__, "name": "note.pdf", "content_type": "application/pdf"})
    store.save(1, "%PDF one", "application/pdf")
    store.save(2, "%PDF two", "application/pdf")
    first, second = (store.versions[s][0] for s in (1, 2))
    store.extracted[first] = Extract("pdf", "ok", "a\n", False, False)
    store.extracted[second] = Extract("docx", "empty", None, False, False)
    assert _shown(await texts.fill_counts(entry, _page(store))) == [(2, None, None), (1, 1, 0)]
    store.extracted[second] = Extract("docx", "ok", "a\nb\n", False, False)
    assert _shown(await texts.fill_counts(entry, _page(store))) == [(2, 1, 0), (1, 1, 0)]


@pytest.mark.asyncio
async def test_an_extract_cut_at_the_cap_is_not_counted_from(store):
    """Two versions alike as far as the cap reaches may differ past it."""
    entry = Entry(**{**_ENTRY.__dict__, "name": "note.docx", "content_type": "application/pdf"})
    store.save(1, "%PDF one", "application/pdf")
    store.save(2, "%PDF two", "application/pdf")
    first, second = (store.versions[s][0] for s in (1, 2))
    store.extracted[first] = Extract("pdf", "ok", "a\n", False, False)
    store.extracted[second] = Extract("pdf", "ok", "a\n", True, False)
    page = await texts.fill_counts(entry, _page(store))
    assert _shown(page) == [(2, None, None), (1, 1, 0)]
    assert (first, second) not in store.counts


@pytest.mark.asyncio
async def test_a_lost_race_with_a_drop_is_not_cached(monkeypatch):
    from psycopg import errors

    from src.server.database.drive import texts as table

    async def dropped(sql, params):
        raise errors.ForeignKeyViolation("gone")

    monkeypatch.setattr(table, "fetch", dropped)
    await table.put_line_counts("s", "a", "b", 1, 0)

    async def moved(sql, params):
        return []  # the epoch moved: nothing matched, nothing written

    monkeypatch.setattr(table, "fetch", moved)
    assert await table.put_attribution("e", 2, 0, [2, 1]) is False
