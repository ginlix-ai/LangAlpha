"""An SFTP channel the machine ends is forgotten as it ends.

A pooled connection counts as idle when no channel but its own SFTP one is
open, and an idle one is closed after a while. An SFTP channel that ended
but was still counted would make one live channel, a socket held through a
preview's forward, read as none, and the connection would be closed under it.
Its open has a deadline, so a hung sftp-server fails file calls rather than
holding them, and every other computer on the login with them.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.providers import _ssh_transport
from ptc_agent.core.sandbox.providers._ssh_transport import Link


class _SFTP:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn
        self._ended = asyncio.Event()

    def end(self) -> None:
        self._conn._channels.pop(id(self))
        self._ended.set()

    async def wait_closed(self) -> None:
        await self._ended.wait()


class _Conn:
    def __init__(self) -> None:
        self._channels: dict[int, object] = {}

    async def start_sftp_client(self) -> _SFTP:
        sftp = _SFTP(self)
        self._channels[id(sftp)] = sftp
        return sftp


@pytest.mark.asyncio
async def test_an_ended_sftp_channel_no_longer_hides_a_live_one():
    conn = _Conn()
    link = Link(SimpleNamespace(label="u@m"), conn)
    first = await link.sftp()
    assert link.idle()

    first.end()
    conn._channels[1] = object()  # a socket through a forward
    async with asyncio.timeout(5):
        while link._tasks:
            await asyncio.sleep(0)

    assert not link.idle()
    assert await link.sftp() is not first


class _HungSFTP(_Conn):
    """A machine whose sftp-server answers only once the connection closes,
    as on a stalled NFS home."""

    def __init__(self) -> None:
        super().__init__()
        self.closed = asyncio.Event()

    async def start_sftp_client(self) -> _SFTP:
        await self.closed.wait()
        raise ConnectionError("connection closed")


@pytest.mark.asyncio
async def test_an_sftp_server_that_never_answers_fails_the_call(monkeypatch):
    monkeypatch.setattr(_ssh_transport, "_SFTP_OPEN_S", 0.05)
    conn = _HungSFTP()
    link = Link(SimpleNamespace(label="u@m"), conn)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(link.sftp(), timeout=5)

    conn.closed.set()
    async with asyncio.timeout(5):
        await asyncio.gather(*link._tasks)
