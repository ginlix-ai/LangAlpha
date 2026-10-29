"""A WebSocket upgrade the sandbox answers with a redirect fails, as a browser's does.

Following it would resend the traffic token, the cookies and the Origin to
whatever host the app names.
"""

from __future__ import annotations

import time

import pytest
from websockets.asyncio.server import serve

from src.server.preview_proxy.activity import PreviewActivity
from src.server.preview_proxy.ws import relay_websocket
from src.server.services.preview_capabilities import Capability, Upstream


@pytest.mark.asyncio
@pytest.mark.enable_socket
async def test_a_redirect_on_the_upgrade_closes_and_reaches_no_other_host():
    reached: list[str | None] = []

    async def elsewhere(ws):
        reached.append(ws.request.headers.get("e2b-traffic-access-token"))
        await ws.close()

    async def never(ws):
        await ws.close()

    async with serve(elsewhere, "127.0.0.1", 0) as other:
        other_port = other.sockets[0].getsockname()[1]

        def redirect(connection, request):
            response = connection.respond(302, "")
            response.headers["Location"] = f"ws://127.0.0.1:{other_port}/"
            return response

        async with serve(never, "127.0.0.1", 0, process_request=redirect) as sandbox:
            port = sandbox.sockets[0].getsockname()[1]
            capability = Capability(
                sandbox_id="sbx-1",
                port=8080,
                upstream=None,
                workspace_id="ws-1",
                generation=1,
                expires_at=int(time.time()) + 600,
            )
            sent: list[dict] = []
            # The browser connects, then leaves, so a relay that did open
            # ends rather than waiting on a socket nobody uses.
            messages = iter(
                [{"type": "websocket.connect"}, {"type": "websocket.disconnect", "code": 1000}]
            )

            async def receive():
                return next(messages)

            async def send(message):
                sent.append(message)

            await relay_websocket(
                {"type": "websocket", "headers": [], "subprotocols": []},
                receive,
                send,
                capability,
                upstream=Upstream(
                    f"http://127.0.0.1:{port}", {"e2b-traffic-access-token": "tok-1"}
                ),
                capability_origin="http://8080-x.preview.localhost",
                target="/",
                activity=PreviewActivity(),
            )

    assert sent == [{"type": "websocket.close", "code": 1011}]
    assert reached == []
