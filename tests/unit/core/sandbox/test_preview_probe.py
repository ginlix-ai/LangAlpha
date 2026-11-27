"""A preview probe never hands the sandbox's credential to another host."""

from __future__ import annotations

import httpx
import pytest

from ptc_agent.core.sandbox.preview_probe import app_answered, app_opens, head_preview

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


@pytest.mark.parametrize(
    ("status", "answered", "opens"),
    [
        (200, True, True),
        (302, True, True),
        # The app is up; the root is just not a page.
        (404, True, False),
        # A server with no HEAD handler is up: Python's BaseHTTPRequestHandler
        # answers 501, a route that names its methods 405.
        (405, True, True),
        (501, True, True),
        # An app that errors is still up; restarting it would lose its state.
        (500, True, False),
        # The gateway found nothing listening.
        (502, False, False),
        (503, False, False),
        (504, False, False),
    ],
)
def test_a_refused_head_is_the_app_answering(status, answered, opens):
    assert app_answered(status) is answered
    assert app_opens(status) is opens
