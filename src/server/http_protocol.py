"""The HTTP protocol the server runs under uvicorn.

uvicorn's own, with ``TCP_NODELAY`` set on every connection. asyncio sets it
on an accepted socket only when that socket's protocol is ``IPPROTO_TCP``,
and the listening socket uvicorn makes itself (``bind_socket``, which every
``--workers`` or ``--reload`` run uses) is created with protocol 0, so the
check fails and the option is never set. Without it, a response written in
two parts (its head, then its body) on a kept-alive connection waits for the
client's delayed acknowledgement: about 40 ms on every request after a
connection's first, which a file mount making one request per file call pays
on each.
"""

from __future__ import annotations

import contextlib
import socket
from typing import Any

from uvicorn.protocols.http.auto import AutoHTTPProtocol


class NoDelayHTTPProtocol(AutoHTTPProtocol):  # type: ignore[misc, valid-type]
    def connection_made(self, transport: Any) -> None:
        sock = transport.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError, AttributeError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        super().connection_made(transport)
