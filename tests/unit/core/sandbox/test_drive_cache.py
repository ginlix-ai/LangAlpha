"""The Drive mount's cache: what it claims, holds, evicts and gives back, the
fetches that fill a large file a block at a time, the read-ahead that plans
them, and the version a large file's readers are held on."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import os
import sys
import threading
from collections import Counter
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.livefs_runtime import drive_cache, drive_fetch, drive_holds, drive_readahead, drive_tree
from ptc_agent.core.sandbox.livefs_runtime.drive_cache import GRACE_S, Cache
from ptc_agent.core.sandbox.livefs_runtime.drive_effects import Effects
from ptc_agent.core.sandbox.livefs_runtime.drive_fetch import Fetcher
from ptc_agent.core.sandbox.livefs_runtime.drive_ops import DriveFS
from ptc_agent.core.sandbox.livefs_runtime.drive_readahead import ReadAhead
from ptc_agent.core.sandbox.livefs_runtime.drive_reads import Reads
from ptc_agent.core.sandbox.livefs_runtime.drive_source import Gone
from ptc_agent.core.sandbox.livefs_runtime.drive_tree import Entry, Space

MiB = 1 << 20


class FakeSource:
    """Versions by sha256, read by range; ``fail_after`` cuts a fetch part
    way with an error once it has handed over that many bytes."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.fail_after: int | None = None
        self.fetched = 0

    def add(self, data: bytes) -> str:
        sha = hashlib.sha256(data).hexdigest()
        self.files[sha] = data
        return sha

    def request_max(self, space, sha) -> int:
        return 8 * MiB

    def fetch(self, space, sha, start, end, sink, buf) -> None:
        data = self.files.get(sha)
        if data is None:
            raise Gone()
        at = start
        while at < end:
            n = min(len(buf), end - at)
            if self.fail_after is not None and at - start + n > self.fail_after:
                sink(at, memoryview(data[at : at + self.fail_after - (at - start)]))
                raise OSError(errno.ECONNRESET, "cut")
            sink(at, memoryview(data[at : at + n]))
            at += n
            self.fetched += n


def _cache(tmp_path, budget: int) -> Cache:
    cache = Cache(str(tmp_path / "store"), budget, reserve=0)
    cache.scan()
    return cache


def _entry(sha: str, size: int, entry_id: str = "e1") -> Entry:
    return Entry(id=entry_id, path="big.bin", kind="file", size=size, sha256=sha, is_text=False,
                 mtime_ns=0, seq=1)


def _settled(fetcher: Fetcher) -> None:
    """Wait for the fetches under way to finish."""
    cache = fetcher.cache
    for _ in range(200):
        with cache.lock:
            busy = any(i.fetching for i in cache._items.values())
        if fetcher._queue.empty() and cache.claimed == 0 and not busy:
            return
        threading.Event().wait(0.02)


def _allocated(path: str) -> int:
    return os.stat(path).st_blocks * 512


def _partials(cache: Cache) -> int:
    return sum(_allocated(os.path.join(cache.partials, n)) for n in os.listdir(cache.partials))


def test_claims_keep_to_one_budget(tmp_path):
    """Claims count against the budget with what the cache holds, and a
    claim past it fails unless something can be given back."""
    cache = _cache(tmp_path, 10 * MiB)
    assert cache.claim(6 * MiB)
    assert not cache.claim(6 * MiB), "two claims together pass the budget"
    cache.unclaim(6 * MiB)
    data = os.urandom(6 * MiB)
    assert cache.put(hashlib.sha256(data).hexdigest(), data)
    assert not cache.claim(6 * MiB), "a binary written within the grace stays, and counts"
    for item in cache._items.values():
        item.used -= GRACE_S + 1
    assert cache.claim(6 * MiB), "an older binary goes for the claim"
    assert cache.files() == 0
    cache.unclaim(6 * MiB)
    assert cache.claimed == 0 and cache.binaries == 0


def test_large_file_through_a_small_budget(tmp_path):
    """A file eight times the budget reads right, evicting least recently
    read blocks; held and claimed never pass the budget."""
    cache = _cache(tmp_path, 4 * MiB)
    source = FakeSource()
    data = os.urandom(32 * MiB)
    sha = source.add(data)
    fetcher = Fetcher(cache, source, cache.counts, connections=4)
    seen = []
    real_claim = cache.claim

    def claim(n, **kw):
        ok = real_claim(n, **kw)
        seen.append(cache.binaries + cache.claimed)
        return ok

    cache.claim = claim
    out = b"".join(fetcher.read(None, sha, len(data), MiB, at) for at in range(0, len(data), MiB))
    assert out == data
    _settled(fetcher)
    assert max(seen) <= 4 * MiB + 8 * MiB  # one request counted twice at most
    assert cache.binaries <= 4 * MiB
    assert cache.counts["block_evicted"] + fetcher.counts["block_refused"] > 0
    assert cache.claimed == 0


def test_cut_fetch_leaves_nothing(tmp_path):
    """A fetch cut off part way: the read fails, the bytes it wrote are
    punched out, nothing is counted held and the claim goes back."""
    cache = _cache(tmp_path, 64 * MiB)
    source = FakeSource()
    data = os.urandom(8 * MiB)
    sha = source.add(data)
    fetcher = Fetcher(cache, source, Counter(), connections=2)
    source.fail_after = 3 * MiB
    with pytest.raises(OSError) as raised:
        fetcher.read(None, sha, len(data), 4 * MiB, 0)  # one 4 MiB fetch, cut at 3
    assert raised.value.errno == errno.EIO
    _settled(fetcher)
    item = cache._items[sha]
    # Only what the read ran ahead to (blocks 4 to 6) is held; the cut
    # fetch (blocks 0 to 3) left nothing.
    assert not any(item.have[:4])
    assert cache.binaries == sum(item.have) * MiB + item.dark
    assert cache.claimed == 0
    if sys.platform.startswith("linux"):
        assert _allocated(item.path) == cache.binaries
    source.fail_after = None
    assert fetcher.read(None, sha, len(data), MiB, 0) == data[:MiB]


def test_a_version_gone_fails_never_another(tmp_path):
    cache = _cache(tmp_path, 64 * MiB)
    fetcher = Fetcher(cache, FakeSource(), Counter(), connections=2)
    with pytest.raises(OSError) as raised:
        fetcher.read(None, "0" * 64, 16 * MiB, MiB, 0)
    assert raised.value.errno == errno.ESTALE


def test_a_file_read_whole_becomes_a_whole_file(tmp_path):
    """The last block in: checked and renamed into place, counted once, and
    read from there from then on."""
    cache = _cache(tmp_path, 64 * MiB)
    source = FakeSource()
    data = os.urandom(6 * MiB + 5)
    sha = source.add(data)
    fetcher = Fetcher(cache, source, Counter(), connections=2)
    assert b"".join(fetcher.read(None, sha, len(data), MiB, at) for at in range(0, len(data), MiB)) == data
    _settled(fetcher)
    for _ in range(100):
        if cache.has(sha):
            break
        threading.Event().wait(0.02)
    assert cache.has(sha) and not os.listdir(cache.partials)
    assert cache.binaries == len(data)
    assert cache.pread(sha, 10, MiB) == data[MiB : MiB + 10]


def test_read_ahead_of_a_quiet_reader_is_cancelled(tmp_path, monkeypatch):
    """Fetches queued ahead of a stream that went quiet are not sent, and
    their claims never taken."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    data = os.urandom(64 * MiB)
    sha = source.add(data)
    read_to = 24 * MiB
    parked, release = threading.Event(), threading.Event()
    sent: list[int] = []
    real_fetch = source.fetch

    def fetch(space, sha_, start, *args):
        sent.append(start)
        # The one connection stops on the first fetch wholly past what is
        # read: every block the reader needs came before it in the queue, so
        # the reader ends, and the fetches ahead behind it stay queued.
        if start >= read_to and not parked.is_set():
            parked.set()
            assert release.wait(10)
        real_fetch(space, sha_, start, *args)

    source.fetch = fetch
    fetcher = Fetcher(cache, source, Counter(), connections=1, readahead=32 * MiB)
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_fetch.time, "monotonic", lambda: clock["t"])
    reader = threading.Thread(target=lambda: [fetcher.read(None, sha, len(data), MiB, at)
                                              for at in range(0, read_to, MiB)])
    reader.start()
    reader.join(10)
    assert not reader.is_alive()
    assert parked.wait(10), f"no fetch ahead past the reader: {dict(fetcher.counts)}"
    queued = fetcher._queue.qsize()
    assert queued > 0, "the fetches ahead behind the parked one are still queued"
    clock["t"] += drive_readahead.AHEAD_IDLE_S + 1
    release.set()
    _settled(fetcher)
    # The parked fetch is cut at its first chunk; the queued ones are never sent.
    assert [s for s in sent if s >= read_to] == [sent[-1]]
    assert fetcher.counts["fetch_cancelled"] >= queued + 1
    assert cache.claimed == 0


def test_readers_hold_the_version_they_are_on(tmp_path, monkeypatch):
    """A large file replaced while it is read: the path keeps the version
    being read while its readers go on, then shows the new one and asks the
    kernel to forget what it cached."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    v1, v2 = os.urandom(8 * MiB), os.urandom(8 * MiB)
    sha1, sha2 = source.add(v1), source.add(v2)
    fetcher = Fetcher(cache, source, Counter(), connections=2)
    forgot = []

    class Remember(Effects):
        def forget(self, path):
            forgot.append(path)

    space = Space("personal", "personal", "s1")
    reads = Reads(cache, None, None, Remember(), Counter(), fetcher, caller=lambda: 4242)
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_holds.time, "monotonic", lambda: clock["t"])
    old, new = _entry(sha1, len(v1)), _entry(sha2, len(v2))
    key = "/personal/big.bin"
    space.load([old], 1)
    assert reads.read(space, key, old, MiB, 0) == v1[:MiB]
    space.load([new], 2)
    clock["t"] += 1
    assert reads.shown(space, new).sha256 == sha1, "getattr shows the version being read"
    assert reads.read(space, key, new, MiB, MiB) == v1[MiB : 2 * MiB]
    assert reads.read_version(space, new, MiB, MiB) == v2[MiB : 2 * MiB], "a writer's base is never the held one"
    assert sha1 in reads.holds.kept()
    clock["t"] += drive_holds.HOLD_IDLE_S + 1
    reads.holds.tick()
    assert forgot == [key]
    assert reads.shown(space, new).sha256 == sha2
    assert reads.read(space, key, new, MiB, 0) == v2[:MiB]


def _small(text: bytes, entry_id: str = "e1") -> Entry:
    return Entry(id=entry_id, path="big.bin", kind="file", size=len(text), sha256=hashlib.sha256(text).hexdigest(),
                 is_text=True, mtime_ns=0, seq=3)


@pytest.mark.parametrize("then", ["small", "recreated", "deleted"])
def test_a_reader_gets_the_file_it_opened_or_an_error(tmp_path, monkeypatch, then):
    """Whether a reader is held depends on the file it began on: replaced by
    a small version, deleted and made again at the same path, or deleted,
    a reader part way through still gets the bytes it began on."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    v1 = os.urandom(8 * MiB)
    sha1 = source.add(v1)
    fetcher = Fetcher(cache, source, Counter(), connections=2)
    space = Space("personal", "personal", "s1")
    reads = Reads(cache, None, None, Effects(), Counter(), fetcher, caller=lambda: 4242)
    monkeypatch.setattr(drive_holds.time, "monotonic", lambda: 1000.0)
    key = "/personal/big.bin"
    old = _entry(sha1, len(v1))
    space.load([old], 1)
    assert reads.read(space, key, old, MiB, 0) == v1[:MiB]
    after = {"small": _small(b"tiny"), "recreated": _small(b"other file", "e2"), "deleted": None}[then]
    space.load([after] if after else [], 2)
    assert reads.read(space, key, after, MiB, MiB) == v1[MiB : 2 * MiB]


def test_a_cancelled_fetch_never_punches_a_newer_fetchs_blocks(tmp_path, monkeypatch):
    """Fetch A is cancelled off its blocks, fetch B fetches and lands one of
    them, and only then does A's cut run: B's bytes stay."""

    def zero(fd, start, length):  # a punch on any disk
        os.pwrite(fd, bytes(length), start)
        return True

    monkeypatch.setattr(drive_cache, "punch", zero)
    cache = _cache(tmp_path, 64 * MiB)
    data = os.urandom(2 * MiB)
    sha = hashlib.sha256(data).hexdigest()
    a, b = object(), object()
    with cache.lock:
        item = cache.item_for(sha, len(data))
        item.fetching.update({0: a, 1: a})
    os.pwrite(item.fd, b"x" * 4096, 0)  # A part way through block 0
    with cache.lock:  # A is cancelled: its blocks let go
        del item.fetching[0], item.fetching[1]
        item.fetching[0] = b
    os.pwrite(item.fd, data[:MiB], 0)
    with cache.lock:
        del item.fetching[0]
        cache.landed(item, 0, 0, 0)
    cache.cut(item, 0, 1, a, 0)
    assert os.pread(item.fd, MiB, 0) == data[:MiB]
    assert item.have[0] and not item.have[1]


def test_blocks_held_from_random_reads_do_not_stop_read_ahead(tmp_path, monkeypatch):
    """Random reads earlier leave blocks held, and streams, across the file;
    a sequential reader passing them later still has every block fetched
    ahead of it."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    data = os.urandom(64 * MiB)
    sha = source.add(data)
    fetcher = Fetcher(cache, source, Counter(), connections=2, readahead=8 * MiB)
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_fetch.time, "monotonic", lambda: clock["t"])
    for at in range(3 * MiB, 60 * MiB, 6 * MiB):
        fetcher.read(None, sha, len(data), MiB // 4, at)
    clock["t"] += drive_readahead.STREAM_FORGET_S + 1
    _settled(fetcher)
    demand = fetcher.counts["fetch_demand"]
    out = b"".join(fetcher.read(None, sha, len(data), MiB, at) for at in range(0, 32 * MiB, MiB))
    assert out == data[: 32 * MiB]
    assert fetcher.counts["fetch_demand"] - demand <= 2  # the first reads only


def test_a_reader_joining_a_fetch_as_it_is_cancelled_gets_its_bytes(tmp_path, monkeypatch):
    """The worker decides a fetch ahead is stale just as a reader joins it:
    the reader gets its bytes, never EIO."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    data = os.urandom(32 * MiB)
    sha = source.add(data)
    gate = threading.Event()
    real_fetch = source.fetch

    def gated(space, sha_, start, *args):
        if start >= 8 * MiB:
            gate.wait(5)  # the read-ahead past what is read waits here
        real_fetch(space, sha_, start, *args)

    source.fetch = gated
    fetcher = Fetcher(cache, source, Counter(), connections=1, readahead=8 * MiB)
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_fetch.time, "monotonic", lambda: clock["t"])
    for at in range(0, 8 * MiB, MiB):
        fetcher.read(None, sha, len(data), MiB, at)
    clock["t"] += drive_readahead.AHEAD_IDLE_S + 1  # its stream is quiet: stale
    joined: dict = {}
    real_stale = fetcher._stale

    def stale_then_join(fetch):
        verdict = real_stale(fetch)
        if verdict and fetch.stream is not None and "out" not in joined:
            block = fetch.first * MiB

            def join():
                try:
                    joined["out"] = fetcher.read(None, sha, len(data), MiB, block) == data[block : block + MiB]
                except OSError as exc:
                    joined["out"] = exc.errno

            t = threading.Thread(target=join)
            t.start()
            threading.Event().wait(0.3)  # the reader finds the fetch and waits on it
            joined["thread"] = t
        return verdict

    monkeypatch.setattr(fetcher, "_stale", stale_then_join)
    gate.set()
    for _ in range(100):
        if "thread" in joined:
            break
        threading.Event().wait(0.05)
    assert "thread" in joined, f"no fetch ahead was cancelled: {dict(fetcher.counts)}"
    joined["thread"].join(10)
    assert joined["out"] is True


def test_partial_files_no_daemon_holds_go_at_start(tmp_path):
    """A partial file an earlier daemon left goes whatever pid that daemon
    had; one a running daemon still holds stays."""
    cache = _cache(tmp_path, 64 * MiB)
    left = os.path.join(cache.partials, f"{'a' * 64}.0001")
    held = os.path.join(cache.partials, f"{'b' * 64}.0002")
    for path in (left, held):
        with open(path, "wb") as f:
            f.write(os.urandom(MiB))
    holder = os.open(held, os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        Cache(cache.root, 64 * MiB, reserve=0).scan()
        assert not os.path.exists(left)
        assert os.path.exists(held)
    finally:
        os.close(holder)


def test_eviction_frees_the_disk_where_holes_cannot_be_punched(tmp_path, monkeypatch):
    """On a filesystem with no hole punching, eviction still gives the disk
    back, and what is counted held is what the disk holds."""
    monkeypatch.setattr(drive_cache, "punch", lambda fd, offset, length: False)
    cache = _cache(tmp_path, 4 * MiB)
    source = FakeSource()
    data = os.urandom(32 * MiB)
    sha = source.add(data)
    fetcher = Fetcher(cache, source, Counter(), connections=2, readahead=0)
    for at in range(0, len(data), MiB):
        assert fetcher.read(None, sha, len(data), MiB, at) == data[at : at + MiB]
    _settled(fetcher)
    assert _partials(cache) <= 4 * MiB + MiB, f"{_partials(cache) / MiB:.0f} MiB on disk under a 4 MiB budget"
    assert cache.binaries <= 4 * MiB


def _stepped(ra: ReadAhead, reads: list[tuple[int, int]], held: set[int] | None = None, most: int = 8 * MiB):
    held = held if held is not None else set()
    asked: list[int] = []
    for t, (offset, end) in enumerate(reads):
        _, ahead = ra.observe(offset, end, float(t) / 100, lambda i: i not in held, most)
        held.update(ahead)
        asked += ahead
    return asked


def test_read_ahead_follows_a_sequential_reader_in_request_sized_batches():
    ra = ReadAhead(64 * MiB, MiB, 8 * MiB)
    asked = _stepped(ra, [(at, at + MiB) for at in range(0, 16 * MiB, MiB)])
    assert asked and asked == sorted(set(asked)) and asked[0] >= 2
    assert max(asked) >= 16, "the window stays ahead of the reader"


def test_read_ahead_leaves_a_sparse_reader_alone():
    """A columnar reader taking a small chunk every few MB moves forward but
    reads a tenth of what it passes: nothing is fetched ahead of it."""
    ra = ReadAhead(256 * MiB, MiB, 64 * MiB)
    assert _stepped(ra, [(at, at + MiB // 2) for at in range(0, 64 * MiB, 5 * MiB)]) == []


def test_read_ahead_sends_a_near_gap_left_by_earlier_reads():
    """Blocks held through the window (earlier random reads) keep it from
    missing a request's worth; the nearer missing block goes anyway."""
    ra = ReadAhead(64 * MiB, MiB, 8 * MiB)
    held = set(range(0, 64)) - {9}
    asked = _stepped(ra, [(at, at + MiB) for at in range(0, 8 * MiB, MiB)], held, most=8 * MiB)
    assert asked == [9]


def test_read_ahead_of_a_stream_gone_quiet_is_not_live():
    ra = ReadAhead(64 * MiB, MiB, 8 * MiB)
    stream, _ = ra.observe(0, MiB, 0.0, lambda i: True, 8 * MiB)
    assert ra.live(stream, drive_readahead.AHEAD_IDLE_S - 0.1)
    assert not ra.live(stream, drive_readahead.AHEAD_IDLE_S + 0.1)


@pytest.mark.parametrize("sha", ["../../../../etc/passwd", "A" * 64, "0" * 63, "/" + "0" * 63])
def test_a_digest_that_is_not_one_never_becomes_a_path(tmp_path, sha):
    """A sha256 the server sent that is not 64 lowercase hex digits is
    dropped where the answer is parsed, and the cache refuses it anyway."""
    entry = drive_tree.entry_of({"id": "e1", "path": "x.bin", "kind": "file", "size": 9, "sha256": sha,
                                  "is_text": False, "modified_ms": 0, "seq": 1})
    assert entry.sha256 is None
    cache = _cache(tmp_path, 64 * MiB)
    with pytest.raises(OSError):
        with cache.lock:
            cache.item_for(sha, 9)
    reads = Reads(cache, None, None, Effects(), Counter())
    with pytest.raises(OSError) as raised:
        reads.read_version(Space("personal", "personal", "s1"), entry, 9, 0)
    assert raised.value.errno == errno.EIO
    assert os.listdir(cache.partials) == []


def test_a_reader_still_holding_the_file_open_keeps_its_hold(tmp_path, monkeypatch):
    """A reader answered from the kernel's cache goes quiet past the idle
    time but still has the file open: its hold stays, and it goes on
    getting the version it began on."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    v1, v2 = os.urandom(8 * MiB), os.urandom(8 * MiB)
    sha1, sha2 = source.add(v1), source.add(v2)
    fetcher = Fetcher(cache, source, Counter(), connections=2)
    space = Space("personal", "personal", "s1")
    reads = Reads(cache, None, None, Effects(), Counter(), fetcher, caller=lambda: 4242, root="/mnt/drive")
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_holds.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(drive_holds, "_proc", lambda pid: (pid, 1))
    monkeypatch.setattr(drive_holds, "_alive", lambda proc: True)
    still_open = {"yes": True}
    monkeypatch.setattr(drive_holds, "_open_at", lambda proc, target: still_open["yes"]
                        and target == "/mnt/drive/personal/big.bin")
    key = "/personal/big.bin"
    old, new = _entry(sha1, len(v1)), _entry(sha2, len(v2))
    assert reads.read(space, key, old, MiB, 0) == v1[:MiB]
    clock["t"] += 1
    assert reads.read(space, key, new, MiB, MiB) == v1[MiB : 2 * MiB]
    clock["t"] += drive_holds.HOLD_IDLE_S + 1  # its reads answered by the kernel's cache
    reads.holds.tick()
    assert reads.read(space, key, new, MiB, 2 * MiB) == v1[2 * MiB : 3 * MiB]
    clock["t"] += drive_holds.HOLD_IDLE_S + 1  # quiet again, and a read comes before the tick
    assert reads.read(space, key, new, MiB, 3 * MiB) == v1[3 * MiB : 4 * MiB]
    still_open["yes"] = False
    clock["t"] += drive_holds.HOLD_IDLE_S + 1
    reads.holds.tick()
    assert reads.read(space, key, new, MiB, 0) == v2[:MiB], "closed and quiet: the new version shows"


def test_a_descriptor_is_found_by_where_it_points(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"x")
    me = drive_holds._proc(os.getpid())
    if me is None:
        pytest.skip("no /proc here")
    with open(path, "rb"):
        assert drive_holds._open_at(me, str(path))
    assert not drive_holds._open_at(me, str(path))


def test_a_read_naming_no_path_never_answers_another_files_bytes(tmp_path, monkeypatch):
    """The kernel names no path for a read of a file removed while open, and
    nothing in the request says which file it is. One process holds large
    B, replaced at its path from elsewhere, and reads small A, which held
    nothing and is then removed: its read and fstat fail rather than answer
    with B's bytes or size."""
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    v1, v2 = os.urandom(8 * MiB), os.urandom(8 * MiB)
    sha1, sha2 = source.add(v1), source.add(v2)
    fetcher = Fetcher(cache, source, Counter(), connections=2)
    space = Space("personal", "personal", "s1")
    reads = Reads(cache, None, None, Effects(), Counter(), fetcher, caller=lambda: 4242)
    monkeypatch.setattr(drive_holds, "_proc", lambda pid: (pid, 1))
    small = Entry(id="a", path="a.txt", kind="file", size=3, sha256=None, is_text=True, mtime_ns=0, seq=1)
    b = _entry(sha1, len(v1), "b")
    space.load([small, b], 1)
    reads.read(space, "/personal/big.bin", b, MiB, 0)  # B held, partway
    space.load([small, _entry(sha2, len(v2), "b")], 2)  # B replaced from elsewhere
    space.load([_entry(sha2, len(v2), "b")], 3)  # A removed
    fs = SimpleNamespace(counts=Counter(), reads=reads)
    with pytest.raises(OSError) as raised:
        DriveFS.read(fs, None, MiB, MiB, 0)
    assert raised.value.errno == errno.EIO
    with pytest.raises(OSError):
        DriveFS.getattr(fs, None)


def test_a_removed_file_still_open_holds_nothing(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"x")
    me = drive_holds._proc(os.getpid())
    if me is None:
        pytest.skip("no /proc here")
    with open(path, "rb"):
        path.unlink()
        assert not drive_holds._open_at(me, str(path)), "its reads fail: nothing is held for it"


def test_eviction_past_a_partial_dropped_whole_goes_on(tmp_path, monkeypatch):
    """Where holes cannot be punched, a partial goes whole at its first
    block, taking its other blocks with it: the pass goes on past them."""
    monkeypatch.setattr(drive_cache, "punch", lambda fd, offset, length: False)
    cache = _cache(tmp_path, 256 * MiB)
    source = FakeSource()
    data = os.urandom(3 * cache.block)
    sha = source.add(data)
    fetcher = Fetcher(cache, source, Counter(), connections=2, readahead=0)
    for i in (0, 1):
        at = i * cache.block
        assert fetcher.read(None, sha, len(data), MiB, at) == data[at : at + MiB]
    _settled(fetcher)
    with cache.lock:
        freed = cache._evict(10 * cache.block, text=False)
    assert freed == 2 * cache.block and cache.binaries == 0


def test_a_partial_being_finished_keeps_its_blocks(tmp_path, monkeypatch):
    """Eviction running between a partial's digest check and its rename into
    place must not punch a block: the whole file would hold zeros there."""

    def zero(fd, start, length):  # a punch on any disk
        os.pwrite(fd, bytes(length), start)
        return True

    monkeypatch.setattr(drive_cache, "punch", zero)
    cache = _cache(tmp_path, 256 * MiB)
    data = os.urandom(2 * cache.block)
    sha = hashlib.sha256(data).hexdigest()
    with cache.lock:
        item = cache.item_for(sha, len(data))
    os.pwrite(item.fd, data, 0)
    with cache.lock:
        assert cache.landed(item, 0, 1, 0)
    real = cache.release

    def release_then_evict(it):
        real(it)
        with cache.lock:
            cache._evict(10 * cache.block, text=False)  # another thread, in the gap

    monkeypatch.setattr(cache, "release", release_then_evict)
    finished = cache.finish(item)
    monkeypatch.setattr(cache, "release", real)
    if finished:
        with open(cache._path(sha), "rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == sha, "a whole file that is not its digest"


def test_a_hold_that_showed_the_old_size_ends_with_a_forget(monkeypatch):
    """``getattr`` shown the held version's size while its reader reads: once
    the hold ends the kernel is told to forget it, or it keeps that size."""
    clock = {"t": 1000.0}
    monkeypatch.setattr(drive_holds.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(drive_holds, "_proc", lambda pid: (pid, 1))
    monkeypatch.setattr(drive_holds, "_alive", lambda proc: True)
    forgot: list[str] = []

    class Recorded(Effects):
        def forget(self, path: str) -> None:
            forgot.append(path)

    holds = drive_holds.Holds(Recorded(), Counter())
    space = Space("personal", "personal", "s1")
    old, new = _entry("a" * 64, 8 * MiB), _entry("b" * 64, 9 * MiB)
    holds.resolve(space, "/personal/big.bin", old, 4242, large=True)
    assert holds.shown(space, new) is old
    clock["t"] += drive_holds.HOLD_IDLE_S + 1
    holds.tick()
    assert forgot == ["/personal/big.bin"]


def test_an_extension_a_folder_carries_is_not_ignored_whole():
    """``*.csv`` would hide a folder named ``reports.csv`` from a search too:
    where a folder carries the extension, the binary files go by path."""
    space = Space("personal", "personal", "s1")
    space.load([
        Entry(id="b", path="data.csv", kind="file", size=9, sha256="a" * 64, is_text=False, mtime_ns=0, seq=1),
        Entry(id="d", path="reports.csv", kind="folder", size=0, sha256=None, is_text=False, mtime_ns=0, seq=1),
        Entry(id="t", path="reports.csv/notes.txt", kind="file", size=3, sha256="b" * 64, is_text=True,
              mtime_ns=0, seq=1),
    ], 1)
    rules = space.ignore_text().decode().splitlines()
    assert "*.csv" not in rules and "/data.csv" in rules


def test_a_start_leaves_a_running_daemons_temp_files(tmp_path):
    """Two daemons share the store while one replaces the other: a start
    clears only temp files old enough that no write is still on them."""
    first = _cache(tmp_path, 64 * MiB)
    live = os.path.join(first.tmp, "being-written")
    old = os.path.join(first.tmp, "cut-short")
    for path in (live, old):
        with open(path, "wb") as f:
            f.write(b"x")
    os.utime(old, (0, 0))
    _cache(tmp_path, 64 * MiB)  # the replacing daemon's scan
    assert os.path.exists(live) and not os.path.exists(old)


def test_files_finished_from_parts_keep_to_the_open_descriptor_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(drive_cache, "FD_OPEN", 1)
    cache = _cache(tmp_path, 256 * MiB)
    for _ in range(3):
        data = os.urandom(cache.block + 7)
        sha = hashlib.sha256(data).hexdigest()
        with cache.lock:
            item = cache.item_for(sha, len(data))
        os.pwrite(item.fd, data, 0)
        with cache.lock:
            assert cache.landed(item, 0, 1, 0)
        assert cache.finish(item)
    assert len(cache._open) <= 1


def test_a_fresh_link_is_not_the_old_one_when_the_shared_refresh_failed():
    """Two readers need a new link; the first to ask fails: the second asks
    itself rather than taking the link both were refused."""
    from ptc_agent.core.sandbox.livefs_runtime import drive_source

    asked = threading.Event()
    release = threading.Event()
    answers = iter(["fail", "new"])

    class Client:
        def source(self, ref, sha):
            what = next(answers)
            if what == "fail":
                asked.set()
                release.wait(5)
                raise OSError(errno.EIO, "no answer")
            return {"via": "link", "url": "http://store/new", "expires_in": 900}

    source = drive_source.Source(Client(), Counter())
    space = Space("personal", "personal", "s1")
    old = {"via": "link", "url": "http://store/old", "expires_in": 900}
    source._answers[(space.ref, "a" * 64)] = (old, float("inf"))
    first = threading.Thread(target=lambda: pytest.raises(OSError, source.where, space, "a" * 64, fresh=True))
    first.start()
    assert asked.wait(5)
    got = {}
    second = threading.Thread(target=lambda: got.update(a=source.where(space, "a" * 64, fresh=True)))
    second.start()
    release.set()
    first.join(5)
    second.join(5)
    assert got["a"]["url"] == "http://store/new"


def test_a_feed_answer_older_than_a_delete_here_does_not_bring_the_file_back():
    space = Space("personal", "personal", "s1")
    space.load([Entry(id="e1", path="a.txt", kind="file", size=1, sha256="a" * 64, is_text=True, mtime_ns=0,
                      seq=5)], 5)
    space.drop("e1", 10)  # removed here; the server's answer named cursor 10
    wire = {"id": "e1", "path": "a.txt", "kind": "file", "size": 1, "sha256": "a" * 64, "is_text": True,
            "modified_ms": 0, "seq": 5}
    assert space.apply([wire]) == [] and space.get("a.txt") is None
    assert space.apply([{**wire, "seq": 11}]) and space.get("a.txt") is not None, "a later one still comes"


def test_a_delete_answered_after_a_newer_version_arrived_keeps_it():
    """The feed brought a version newer than the delete before its answer
    was committed here: the newer one stays."""
    space = Space("personal", "personal", "s1")
    space.load([Entry(id="e1", path="a.txt", kind="file", size=1, sha256="a" * 64, is_text=True, mtime_ns=0,
                      seq=12)], 12)
    assert space.drop("e1", 10) is None and space.get("a.txt") is not None
    assert space.drop("e1", 13) is not None and space.get("a.txt") is None


def test_a_cursor_only_batch_brings_the_usage_statfs_reads():
    """A purge or a sweep's trim moves the cursor with no entry behind it and
    frees room, which ``statfs`` answers from the usage the feed last gave."""
    from ptc_agent.core.sandbox.livefs_runtime.drive_sync import Sync

    taken = []
    sync = object.__new__(Sync)
    sync.state = {"feed": {}}
    sync.names = SimpleNamespace(apply=lambda *a: taken.append(a))
    space = Space("personal", "personal", "s1")
    sync.apply(space, {"entries": [], "cursor": 9, "resync": False, "usage": {"bytes": 1}})
    assert taken == [(space, [], 9, {"bytes": 1})]


@pytest.mark.parametrize(
    ("usage", "blocks", "files"),
    [
        ({}, (1 << 40) // 4096, 1 << 20),
        ({"max_bytes": None, "max_entries": 10}, (1 << 40) // 4096, 10),
        ({"max_bytes": 0, "max_entries": 0}, 0, 0),
        ({"max_bytes": 8192, "bytes": 4096, "max_entries": 5, "entries": 2}, 2, 5),
    ],
    ids=["nothing known yet", "no cap on bytes", "an allowance of nothing", "a cap partly used"],
)
def test_statfs_reads_an_allowance_of_nothing_as_a_full_drive(usage, blocks, files):
    fs = object.__new__(DriveFS)
    fs.counts = Counter()
    fs.names = SimpleNamespace(spaces={"personal": SimpleNamespace(usage=usage)})
    found = fs.statfs("/")
    assert (found["f_blocks"], found["f_files"]) == (blocks, files)
    assert found["f_bavail"] == max(blocks - int(usage.get("bytes") or 0) // 4096, 0)
