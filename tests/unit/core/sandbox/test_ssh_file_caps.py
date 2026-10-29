"""What a machine sends over SFTP is held only up to a cap.

A backend worker serves every user's turns, so a machine that claims a file
of 64 GiB, or a directory listing without end, must not take the worker's
memory with it.
"""

from __future__ import annotations

import errno
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.sandbox.providers import ssh_runtime
from ptc_agent.core.sandbox.providers._ssh_transport import HostFacts, Link
from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime


class _File:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.asked: list[int] = []

    async def __aenter__(self) -> _File:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def read(self, size: int = -1) -> bytes:
        self.asked.append(size)
        return self._data[:size] if size >= 0 else self._data


class _SFTP:
    def __init__(self, *, claims: int, holds: bytes = b"", names: int = 0) -> None:
        self._claims = claims
        self.file = _File(holds)
        self._names = names

    async def stat(self, path: str) -> SimpleNamespace:
        return SimpleNamespace(size=self._claims)

    async def open(self, path: str, mode: str, encoding: None = None) -> _File:
        return self.file

    async def scandir(self, path: str):
        n = 0
        while self._names == 0 or n < self._names:
            n += 1
            yield SimpleNamespace(
                filename=f"f{n}",
                longname="",
                attrs=SimpleNamespace(
                    permissions=0o100644, size=0, uid=None, gid=None, mtime=None
                ),
            )


def _runtime(sftp: _SFTP) -> tuple[SSHRuntime, Link]:
    # The constructor needs a target and a key loader; these paths read neither.
    runtime = object.__new__(SSHRuntime)
    runtime._id = "c1"
    runtime._target = SimpleNamespace(host="m", label="u@m")
    link = Link(runtime._target, MagicMock())
    link.facts = HostFacts(uid=1000, user="u", home="/home/u", os="Linux", arch="x86_64")
    link._sftp = sftp
    runtime._ensure_running = AsyncMock(return_value=link)
    return runtime, link


@pytest.mark.asyncio
async def test_a_file_claimed_past_the_cap_is_refused_unread():
    sftp = _SFTP(claims=64 << 30)
    runtime, link = _runtime(sftp)

    with pytest.raises(OSError) as e:
        await runtime._read_bytes(link, "/home/u/env.json", cap=1024)

    assert e.value.errno == errno.EFBIG
    assert sftp.file.asked == []


@pytest.mark.asyncio
async def test_a_file_bigger_than_its_stat_says_is_read_only_past_the_cap():
    sftp = _SFTP(claims=0, holds=b"x" * 4096)
    runtime, link = _runtime(sftp)

    with pytest.raises(OSError) as e:
        await runtime._read_bytes(link, "/proc/self/environ", cap=1024)

    assert e.value.errno == errno.EFBIG
    assert sftp.file.asked == [1025]


@pytest.mark.asyncio
async def test_an_env_file_past_its_cap_reads_as_none():
    runtime, link = _runtime(_SFTP(claims=64 << 30))

    assert await runtime._read_env(link) == {}


@pytest.mark.asyncio
async def test_a_listing_without_end_is_cut_off(monkeypatch):
    monkeypatch.setattr(ssh_runtime, "_LIST_MAX_ENTRIES", 5)
    runtime, _ = _runtime(_SFTP(claims=0))

    entries = await runtime.list_files("/home/u")

    assert [e["name"] for e in entries] == ["f1", "f2", "f3", "f4", "f5"]
