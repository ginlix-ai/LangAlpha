"""A worker's children get none of its descriptors, the listening socket uvicorn
leaves inheritable included."""

import asyncio
import socket
import sys

import pytest

from src.server.utils.child_fds import keep_descriptors_from_children

pytestmark = [
    pytest.mark.skipif(sys.platform == "win32", reason="no fork or uvloop"),
    pytest.mark.enable_socket,  # a loopback listener, nothing beyond it
]


@pytest.fixture
def listener():
    with socket.create_server(("127.0.0.1", 0)) as sock:
        sock.set_inheritable(True)  # as uvicorn hands it to each worker
        yield sock


def test_an_inheritable_socket_is_no_longer_inheritable(listener):
    keep_descriptors_from_children()

    assert not listener.get_inheritable()


def test_a_child_started_on_uvloop_does_not_get_it(listener):
    uvloop = pytest.importorskip("uvloop")
    keep_descriptors_from_children()
    fd = listener.fileno()

    async def child_sees() -> str:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            f"import os; print(os.path.exists('/dev/fd/{fd}'))",
            stdout=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        return out.decode().strip()

    assert uvloop.run(child_sees()) == "False"
