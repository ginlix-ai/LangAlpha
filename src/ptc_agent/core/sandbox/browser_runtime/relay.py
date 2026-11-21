"""How a viewer reaches the browser's live stream, from inside the computer.

The stream server binds loopback with no option to change that and refuses any
Origin that is not its own machine, so nothing outside the computer can open it.
This listens where a preview port can be reached and relays a WebSocket upgrade
with the Origin removed. That check is what stops any web page a person has open
from driving a browser it can reach, so it is replaced rather than dropped: the
path must name a token made when the bridge starts, which only the server reads
off this machine and hands to the computer's owner. A preview capability or
signed URL guards the way here as well, but not every computer has one, and a
published Docker port answers anything on its host.

The bridge ends once the stream has been gone long enough that a restart is not
what it is watching, so it stops with the browser however the browser ended.
"""

from __future__ import annotations

import asyncio
import hmac
import secrets

from . import care, daemon
from .protocol import BRIDGE_PORT, PATH_PREFIX, STREAM_PORT, TOKEN_FILE, write_state

_LOG = "bridge.log"
_HEAD_LIMIT = 16 * 1024
_HEAD_TIMEOUT_S = 5.0
_DIAL_TIMEOUT_S = 3.0
# Only these reach the stream. The rest are for the hops before this one, and
# include the provider's traffic credential.
_FORWARDED = frozenset(
    {
        b"host",
        b"connection",
        b"upgrade",
        b"sec-websocket-key",
        b"sec-websocket-version",
        b"sec-websocket-extensions",
        b"sec-websocket-protocol",
    }
)
# Only these of a viewer's query reach the stream, which reads its settings there.
_STREAM_SETTINGS = frozenset({b"pacing", b"maxFps"})


def ensure(wait_s: float = 0.0) -> bool:
    """Whether the bridge is up, starting it when it is not."""
    return daemon.ensure("serve", BRIDGE_PORT, _LOG, wait_s)


def rewrite_head(head: bytes, token: str) -> tuple[bytes | None, bytes]:
    """The upgrade to send the stream, or None and the status to answer with.

    The request line's target loses its token, and its query keeps only the
    stream's own settings: on some providers the signed preview credential
    rides there too. Only the headers an upgrade needs go on, and a head a
    parser could read two ways is refused rather than guessed at.
    """
    lines = daemon.split_head(head)
    if lines is None:
        return None, b"400 Bad Request"
    parts = lines[0].split(b" ")
    if len(parts) != 3 or parts[0] != b"GET":
        return None, b"400 Bad Request"
    path, _, query = parts[1].partition(b"?")
    expected = (PATH_PREFIX + token).encode()
    if not hmac.compare_digest(path, expected):
        return None, b"403 Forbidden"
    kept = []
    upgrade = False
    hosts = 0
    for line in lines[1:]:
        name, colon, value = line.partition(b":")
        if not colon or not name or name != name.strip() or line[:1] in (b" ", b"\t"):
            return None, b"400 Bad Request"
        name = name.lower()
        if name == b"host":
            hosts += 1
        if name == b"upgrade" and value.strip().lower() == b"websocket":
            upgrade = True
        if name in _FORWARDED:
            kept.append(line)
    if hosts != 1:
        return None, b"400 Bad Request"
    if not upgrade:
        return None, b"426 Upgrade Required"
    query = b"&".join(
        pair for pair in query.split(b"&") if pair.partition(b"=")[0] in _STREAM_SETTINGS
    )
    target = b"/" + (b"?" + query if query else b"")
    return b"\r\n".join([b"GET " + target + b" " + parts[2], *kept]) + b"\r\n\r\n", b""


def serve() -> int:
    listener = daemon.bind("0.0.0.0", BRIDGE_PORT, 64)
    if listener is None:
        # One is already up, whose token this must not overwrite.
        return 0
    # Written by the one that listens, never by a start that lost the race.
    token = secrets.token_urlsafe(32)
    write_state(TOKEN_FILE, token)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), _HEAD_TIMEOUT_S)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, OSError):
            await daemon.answer(writer, b"400 Bad Request")
            return
        upgrade, refusal = rewrite_head(head, token)
        if upgrade is None:
            await daemon.answer(writer, refusal)
            return
        try:
            up_reader, up_writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", STREAM_PORT, limit=_HEAD_LIMIT),
                _DIAL_TIMEOUT_S,
            )
        except (OSError, TimeoutError):
            # The browser is restarting or has gone: a viewer reconnects.
            await daemon.answer(writer, b"502 Bad Gateway")
            return
        up_writer.write(upgrade)
        await asyncio.gather(daemon.pipe(reader, up_writer), daemon.pipe(up_reader, writer))

    async def tick(up: bool) -> None:
        care.tend(up)

    asyncio.run(daemon.serve_while_stream(listener, handle, _HEAD_LIMIT, tick))
    return 0
