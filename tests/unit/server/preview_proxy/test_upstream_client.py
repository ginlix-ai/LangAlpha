"""The proxy's upstream client, which every user's previews share.

One client per worker serves every user, so it must never carry state from
one request into another, and must send upstream only what the browser sent.
"""

from __future__ import annotations

import httpx
import pytest

from src.server.preview_proxy.app import PreviewProxy, _NoCookies
from src.server.services.preview_base import PreviewBase


@pytest.mark.asyncio
async def test_a_cookie_one_user_was_set_is_never_sent_for_another():
    sent: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request.headers.get("cookie"))
        return httpx.Response(200, headers={"set-cookie": "session=user-a; Path=/"})

    async with httpx.AsyncClient(
        cookies=_NoCookies(), transport=httpx.MockTransport(handler)
    ) as client:
        await client.get("https://8080-sbx-1.e2b.app/")
        await client.get("https://8080-sbx-1.e2b.app/")

    assert sent == [None, None]


@pytest.mark.asyncio
async def test_the_shared_client_adds_nothing_the_browser_did_not_send():
    proxy = PreviewProxy(PreviewBase("https", "preview.example.com"))
    try:
        client = proxy._http_client()

        assert isinstance(client.cookies.jar, _NoCookies)
        assert "accept-encoding" not in client.headers
        assert "user-agent" not in client.headers
        assert "accept" not in client.headers
        assert client.follow_redirects is False
    finally:
        await proxy.aclose()
