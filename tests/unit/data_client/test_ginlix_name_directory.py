"""The CN/HK name directory under a slow or busy ginlix-data.

Quote reads wait on it for every CN/HK row, so a stalled lookup must cost
them a bounded moment once, not the client's full timeout per reader.
"""

from __future__ import annotations

import asyncio

import pytest

from src.data_client.ginlix_data import directory as directory_mod
from src.data_client.ginlix_data.directory import NameDirectory

_MOUTAI = {"600519.XSHG": {"name": "Kweichow Moutai", "name_local": "贵州茅台"}}


@pytest.mark.asyncio
async def test_concurrent_readers_share_one_lookup():
    release = asyncio.Event()
    calls: list[list[str]] = []

    async def lookup(keys: list[str]):
        calls.append(keys)
        await release.wait()
        return _MOUTAI

    directory = NameDirectory(lookup)
    readers = [
        asyncio.create_task(directory.display_names_many(["600519.SH"])),
        asyncio.create_task(directory.display_names_many(["600519.sh", "AAPL"])),
        asyncio.create_task(directory.display_names("600519.SH")),
    ]
    await asyncio.sleep(0)
    release.set()
    first, second, third = await asyncio.gather(*readers)

    assert calls == [["600519.SH"]]
    assert first == {"600519.SH": ("贵州茅台", "Kweichow Moutai")}
    assert second == {"600519.sh": ("贵州茅台", "Kweichow Moutai"), "AAPL": (None, None)}
    assert third == ("贵州茅台", "Kweichow Moutai")


@pytest.mark.asyncio
async def test_a_cancelled_reader_leaves_the_shared_lookup_running():
    release = asyncio.Event()

    async def lookup(keys: list[str]):
        await release.wait()
        return _MOUTAI

    directory = NameDirectory(lookup)
    owner = asyncio.create_task(directory.display_names("600519.SH"))
    await asyncio.sleep(0)
    waiter = asyncio.create_task(directory.display_names("600519.SH"))
    await asyncio.sleep(0)
    owner.cancel()
    release.set()

    assert await waiter == ("贵州茅台", "Kweichow Moutai")


@pytest.mark.asyncio
async def test_a_lookup_past_the_deadline_fails_softly_and_is_not_retried():
    calls = 0

    async def stalled(keys: list[str]):
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()

    directory = NameDirectory(stalled, deadline=0.01)

    assert await asyncio.wait_for(directory.display_names("600519.SH"), 1.0) == (None, None)
    # The failure is remembered for the fail TTL, so the next reader does not stall.
    assert await asyncio.wait_for(directory.display_names("600519.SH"), 1.0) == (None, None)
    assert calls == 1


class _Clock:
    """Stands in for the directory module's ``time`` so TTLs can lapse instantly."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


def _symbols(n: int) -> list[str]:
    return [f"{600000 + i}.SH" for i in range(n)]


@pytest.mark.asyncio
async def test_expired_entries_are_reclaimed(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(directory_mod, "time", clock)

    async def unknown(keys: list[str]):
        return {}

    directory = NameDirectory(unknown)
    await directory.display_names_many(_symbols(1000))
    assert len(directory._cache) == 1000

    clock.now += directory_mod._MISS_TTL + 1
    await directory.display_names("000001.SZ")

    assert list(directory._cache) == ["000001.SZ"]


@pytest.mark.asyncio
async def test_the_cache_keeps_at_most_the_cap_dropping_the_oldest(monkeypatch):
    monkeypatch.setattr(directory_mod, "_MAX_ENTRIES", 3)
    calls: list[list[str]] = []

    async def lookup(keys: list[str]):
        calls.append(keys)
        return _MOUTAI

    directory = NameDirectory(lookup)
    for sym in _symbols(5):
        await directory.display_names(sym)

    oldest = _symbols(5)[0]
    assert list(directory._cache) == _symbols(5)[2:]
    # The evicted entry had not expired, so only the cap explains a second lookup.
    await directory.display_names(oldest)
    assert calls[-1] == [oldest] and len(calls) == 6
    assert len(directory._cache) == 3
