"""WebSocket passthrough for previews: Streamlit, Jupyter, dev-server reloads.

The capability is checked at the handshake, then re-read once a minute so a
revocation reaches an open socket on whichever worker holds it. Messages are
relayed whole, so each hop negotiates its own compression.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from src.server.preview_proxy.activity import PreviewActivity
from src.server.preview_proxy.headers import request_headers
from src.server.services.preview_capabilities import (
    Capability,
    Upstream,
    until_revoked,
)

logger = logging.getLogger(__name__)

_OPEN_TIMEOUT_S = 10
# What one socket may buffer from a sandbox is about one message times the
# queue, so these bound a socket's memory on the worker to around 8 MiB.
_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
_MAX_QUEUE = 2
# Policy violation: what a revoked capability closes with.
_REVOKED = 1008
# Codes that describe a close but may not be sent in one.
_UNSENDABLE = {1005: 1000, 1006: 1011, 1015: 1011}


class _connect_once(connect):
    """The upstream handshake, failing on a redirect as a browser's does.

    Following one would resend the sandbox's traffic token, the cookies and
    the rewritten Origin to whatever host the app names.
    """

    def process_redirect(self, exc: Exception) -> Exception:
        return exc


def _sendable(code: int | None) -> int:
    if code is None:
        return 1000
    if code in _UNSENDABLE:
        return _UNSENDABLE[code]
    return code if 1000 <= code <= 4999 else 1011


async def relay_websocket(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
    capability: Capability,
    *,
    upstream: Upstream,
    capability_origin: str,
    target: str,
    activity: PreviewActivity,
) -> None:
    message = await receive()
    if message["type"] != "websocket.connect":
        return
    url = "ws" + upstream.origin[len("http") :] + target
    # Origin and User-Agent ride along as the browser sent them, Origin
    # rebased, so the library adds neither.
    headers = request_headers(
        scope.get("headers", []),
        capability_origin=capability_origin,
        upstream_origin=upstream.origin,
        inject=upstream.headers,
        websocket=True,
    )
    try:
        upstream = await _connect_once(
            url,
            additional_headers=headers,
            subprotocols=scope.get("subprotocols") or None,
            open_timeout=_OPEN_TIMEOUT_S,
            max_size=_MAX_MESSAGE_BYTES,
            max_queue=_MAX_QUEUE,
            user_agent_header=None,
        )
    except (OSError, TimeoutError, WebSocketException) as e:
        logger.debug("Preview WebSocket did not open upstream: %s", e)
        # Closing before accepting answers the handshake with a 403.
        await send({"type": "websocket.close", "code": 1011})
        return

    try:
        await send({"type": "websocket.accept", "subprotocol": upstream.subprotocol})
        activity.note(capability)
        tasks = [
            asyncio.create_task(_client_to_upstream(receive, upstream, capability, activity)),
            asyncio.create_task(_upstream_to_client(upstream, send, capability, activity)),
            asyncio.create_task(_watch_revocation(capability, send)),
        ]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        with contextlib.suppress(Exception):
            await upstream.close()


async def _client_to_upstream(
    receive: Any,
    upstream: ClientConnection,
    capability: Capability,
    activity: PreviewActivity,
) -> None:
    while True:
        message = await receive()
        kind = message["type"]
        if kind == "websocket.disconnect":
            with contextlib.suppress(Exception):
                await upstream.close(code=_sendable(message.get("code")))
            return
        if kind != "websocket.receive":
            continue
        try:
            if message.get("text") is not None:
                await upstream.send(message["text"])
            elif message.get("bytes") is not None:
                await upstream.send(message["bytes"])
        except ConnectionClosed:
            return
        activity.note(capability)


async def _upstream_to_client(
    upstream: ClientConnection,
    send: Any,
    capability: Capability,
    activity: PreviewActivity,
) -> None:
    try:
        async for data in upstream:
            if isinstance(data, str):
                await send({"type": "websocket.send", "text": data})
            else:
                await send({"type": "websocket.send", "bytes": data})
            # A live feed the browser only watches is use too.
            activity.note(capability)
    except ConnectionClosed:
        pass
    except OSError:
        # The browser went away mid-send; the receive side reports it.
        return
    with contextlib.suppress(Exception):
        await send(
            {
                "type": "websocket.close",
                "code": _sendable(upstream.close_code),
                "reason": upstream.close_reason or "",
            }
        )


async def _watch_revocation(capability: Capability, send: Any) -> None:
    await until_revoked(capability)
    with contextlib.suppress(Exception):
        await send({"type": "websocket.close", "code": _REVOKED})
