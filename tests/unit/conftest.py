"""Socket tripwire: unit tests must not reach real network endpoints.

A mock that silently stops intercepting (e.g. after a module move breaks a
patch target) fails OPEN — the test goes green against live I/O. Blocking
socket creation turns that failure mode into a loud error. AF_UNIX stays
allowed: asyncio's event loop self-pipe is a unix socketpair.
"""

import pytest
import pytest_socket


@pytest.fixture(autouse=True)
def _no_real_sockets(request):
    if request.node.get_closest_marker("enable_socket"):
        yield
        return
    pytest_socket.disable_socket(allow_unix_socket=True)
    yield
    pytest_socket.enable_socket()
