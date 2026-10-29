"""A preview probe never hands the sandbox's credential to another host."""

from __future__ import annotations

import httpx
import pytest

from ptc_agent.core.sandbox.preview_probe import head_preview

_HEADERS = {"e2b-traffic-access-token": "tok-1"}


def _client(routes: dict[str, httpx.Response], seen: list[httpx.Request]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return routes[str(request.url)]

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_a_redirect_on_the_same_origin_is_followed_with_the_credential():
    seen: list[httpx.Request] = []
    routes = {
        "https://8080-sbx.e2b.app/": httpx.Response(302, headers={"location": "/app"}),
        "https://8080-sbx.e2b.app/app": httpx.Response(200),
    }
    async with _client(routes, seen) as client:
        response = await head_preview(client, "https://8080-sbx.e2b.app/", _HEADERS)

    assert response.status_code == 200
    assert [r.headers.get("e2b-traffic-access-token") for r in seen] == ["tok-1", "tok-1"]


@pytest.mark.asyncio
async def test_a_redirect_to_another_host_is_the_answer_not_a_hop():
    seen: list[httpx.Request] = []
    routes = {
        "https://8080-sbx.e2b.app/": httpx.Response(
            302, headers={"location": "https://accounts.example.com/login"}
        ),
    }
    async with _client(routes, seen) as client:
        response = await head_preview(client, "https://8080-sbx.e2b.app/", _HEADERS)

    assert response.status_code == 302
    assert [r.url.host for r in seen] == ["8080-sbx.e2b.app"]


@pytest.mark.asyncio
async def test_a_redirect_loop_ends():
    seen: list[httpx.Request] = []
    routes = {
        "https://8080-sbx.e2b.app/": httpx.Response(302, headers={"location": "/"}),
    }
    async with _client(routes, seen) as client:
        response = await head_preview(client, "https://8080-sbx.e2b.app/", _HEADERS)

    assert response.status_code == 302
    assert len(seen) == 6
