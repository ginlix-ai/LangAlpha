"""A large upload rides an SFTP channel of its own, in a command slot.

The kept SFTP channel serves every file call on the connection, so a send of
hundreds of MB on it would hold them all. A send runs on a channel it opens in
a command slot, gives both back when it ends, and leaves no partial file when
it fails. A long script can take a command slot too, leaving the control slot
to management scripts.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.providers._ssh_transport import _COMMAND_SLOTS, Link


class _SFTP:
    def __init__(self, fail: bool) -> None:
        self.fail = fail
        self.files: dict[str, bytes] = {}
        self.exited = False

    @contextlib.asynccontextmanager
    async def open(self, path: str, mode: str) -> AsyncIterator[SimpleNamespace]:
        self.files[path] = b""

        async def write(data: bytes) -> None:
            self.files[path] += data
            if self.fail:
                raise ConnectionError("lost mid-send")

        yield SimpleNamespace(write=write)

    async def remove(self, path: str) -> None:
        self.files.pop(path)

    def exit(self) -> None:
        self.exited = True

    async def wait_closed(self) -> None:
        return None


class _Conn:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.opened: list[_SFTP] = []

    async def start_sftp_client(self) -> _SFTP:
        sftp = _SFTP(self.fail)
        self.opened.append(sftp)
        return sftp


async def _settled(link: Link) -> None:
    async with asyncio.timeout(5):
        while link._tasks:
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_an_upload_uses_its_own_channel_and_gives_its_slot_back(tmp_path):
    local = tmp_path / "claude"
    local.write_bytes(b"claude")
    conn = _Conn()
    link = Link(SimpleNamespace(label="u@m"), conn)
    await link.upload(str(local), "/remote/.upload.x", timeout=5)
    await _settled(link)

    assert link._sftp is None, "the kept channel is not used"
    assert conn.opened[0].files == {"/remote/.upload.x": b"claude"}
    assert conn.opened[0].exited
    assert link.slots._value == _COMMAND_SLOTS
    assert link.active == 0


@pytest.mark.asyncio
async def test_a_failed_upload_leaves_no_partial_file(tmp_path):
    local = tmp_path / "claude"
    local.write_bytes(b"claude")
    conn = _Conn(fail=True)
    link = Link(SimpleNamespace(label="u@m"), conn)
    with pytest.raises(ConnectionError):
        await link.upload(str(local), "/remote/.upload.x", timeout=5)
    await _settled(link)

    assert conn.opened[0].files == {}
    assert conn.opened[0].exited
    assert link.slots._value == _COMMAND_SLOTS


class _Process:
    def __init__(self) -> None:
        self.stdin = SimpleNamespace(write=lambda data: None, write_eof=lambda: None)
        self.stdout = self.stderr = SimpleNamespace(read=self._eof)
        self.exit_status, self.exit_signal = 0, None

    async def _eof(self, n: int) -> bytes:
        return b""

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


@pytest.mark.asyncio
async def test_a_script_given_a_command_slot_leaves_the_control_slot_free():
    async def create_process(command: str, encoding: None = None) -> _Process:
        return _Process()

    link = Link(SimpleNamespace(label="u@m"), SimpleNamespace(create_process=create_process))
    await link.control_slot.acquire()
    result = await link.run("true", timeout=5, slot=link.slots)
    assert result.exit_code == 0
