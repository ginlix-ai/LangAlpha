"""What status the connect route gives a refusal, and why the order matters.

``McpServerMoved`` is an ``McpOAuthError``, so the two ``except`` clauses in
``oauth_start`` are ordered rather than exclusive: swap them and every moved row
answers 422 with the service's own wording, which the page shows verbatim
instead of telling the user to reload. Nothing else notices -- the request still
fails, just uselessly -- so the order is pinned here.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx2
import pytest
from fastapi import HTTPException, Response
from mcp.shared.auth import OAuthMetadata

from src.server.app import mcp_oauth as mod
from src.server.services.mcp_oauth import connect
from src.server.services.mcp_oauth.connect import McpOAuthError, McpServerMoved
from src.server.utils.egress_guard import PinnedTarget


def _request():
    return SimpleNamespace(headers={}, cookies={})


async def _call(monkeypatch, error: Exception) -> HTTPException:
    async def _start(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(mod, "start_connect", _start)
    with pytest.raises(HTTPException) as caught:
        await mod.oauth_start("srv", "user-1", _request(), Response(), {})
    return caught.value


@pytest.mark.asyncio
async def test_a_row_that_moved_is_a_conflict(monkeypatch):
    raised = await _call(monkeypatch, McpServerMoved("the address changed"))

    assert raised.status_code == 409


@pytest.mark.asyncio
async def test_every_other_refusal_still_reads_as_unprocessable(monkeypatch):
    # The control for the ordering above: the general clause is untouched, so a
    # 409 means the specific one was reached rather than that both moved.
    raised = await _call(monkeypatch, McpOAuthError("not a remote server"))

    assert raised.status_code == 422


@pytest.mark.asyncio
async def test_a_registration_the_server_refused_is_unprocessable_not_a_crash(
    monkeypatch,
):
    # Seen live: an AS advertising a registration endpoint that answers 404
    # with an HTML page. The SDK's error for it is not ours, so the route used
    # to answer a bare 500, and its message carried the page.
    issuer = "https://auth.vendor.test"
    metadata = OAuthMetadata.model_validate(
        {
            "issuer": issuer,
            "authorization_endpoint": f"{issuer}/authorize",
            "token_endpoint": f"{issuer}/token",
            "registration_endpoint": f"{issuer}/register",
        }
    )

    @asynccontextmanager
    async def _client():
        yield object()

    monkeypatch.setattr(
        connect,
        "get_catalog_server",
        AsyncMock(
            return_value={"url": "https://mcp.vendor.test/mcp", "transport": "http"}
        ),
    )
    monkeypatch.setattr(connect, "oauth_http_client", _client)
    monkeypatch.setattr(
        connect, "_discover", AsyncMock(return_value=(None, metadata, issuer, None))
    )
    monkeypatch.setattr(connect, "get_connection", AsyncMock(return_value=None))
    monkeypatch.setattr(
        connect,
        "pinned_send",
        AsyncMock(
            return_value=httpx2.Response(
                404,
                headers={"content-type": "text/html"},
                content=b"<html><body><h1>Not Found</h1></body></html>",
            )
        ),
    )

    with pytest.raises(HTTPException) as caught:
        await mod.oauth_start("srv", "user-1", _request(), Response(), {})

    assert caught.value.status_code == 422
    assert "HTTP 404" in caught.value.detail
    assert "<" not in caught.value.detail


@pytest.mark.asyncio
async def test_a_blocked_discovery_hop_is_unprocessable_and_keeps_the_query_out(
    monkeypatch, caplog
):
    # A server whose /mcp redirects to /mcp/ trips the no-redirect rule on the
    # very first hop. That used to reach the route as a bare 500, logged at
    # ERROR quoting the row's URL, credential and all.
    url = "https://mcp.vendor.test/mcp?api_key=sk-live-123"

    async def _pin(target, *, require_https=True, allow_non_global=False):
        host = "mcp.vendor.test"
        return PinnedTarget(url=target, host=host, ip="203.0.113.10", authority=host)

    def _answer(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/mcp":
            return httpx2.Response(
                307, headers={"location": "https://mcp.vendor.test/mcp/"}
            )
        return httpx2.Response(404)

    monkeypatch.setattr(
        connect,
        "get_catalog_server",
        AsyncMock(return_value={"url": url, "transport": "http"}),
    )
    monkeypatch.setattr(
        connect,
        "oauth_http_client",
        lambda: httpx2.AsyncClient(transport=httpx2.MockTransport(_answer)),
    )
    monkeypatch.setattr("src.server.services.mcp_oauth.http.pin_public_url", _pin)

    with pytest.raises(HTTPException) as caught:
        await mod.oauth_start("srv", "user-1", _request(), Response(), {})

    assert caught.value.status_code == 422
    assert "redirect (307)" in caught.value.detail
    assert "mcp.vendor.test" in caught.value.detail
    assert "sk-live-123" not in caught.value.detail
    assert "sk-live-123" not in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
