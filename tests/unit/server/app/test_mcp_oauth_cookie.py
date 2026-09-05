"""The OAuth CSRF nonce cookie is per-state, so concurrent connects from one
browser don't clobber each other's cookie.

The bug this pins: a single fixed cookie name meant a second connect overwrote
the first flow's nonce cookie (same name, same path), so the first flow's
callback read the second flow's nonce and failed ``state_mismatch``. Naming the
cookie for the single-use ``state`` isolates the flows.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import Response

from src.server.app import mcp_oauth as mod
from src.server.services.mcp_oauth import StartedConnect


def _request(*, origin: str | None = None, cookies: dict | None = None):
    return SimpleNamespace(
        headers={"origin": origin} if origin else {},
        cookies=cookies or {},
    )


def _set_cookies(response: Response) -> list[str]:
    return [
        v.decode() for k, v in response.raw_headers if k == b"set-cookie"
    ]


@pytest.mark.asyncio
async def test_start_names_the_cookie_for_its_state(monkeypatch):
    async def _start(
        user_id, name, *, return_to, web_origin, loopback_redirect, expected_url,
        **_rest,
    ):
        return StartedConnect(
            authorize_url="https://as.test/authorize?state=state-A",
            state="state-A",
            browser_nonce="nonce-A",
            redirect_uri="https://api.example.com/api/v1/mcp/oauth/callback",
        )

    monkeypatch.setattr(mod, "start_connect", _start)
    response = Response()

    await mod.oauth_start("srv", "user-1", _request(), response, None)

    [cookie] = _set_cookies(response)
    assert cookie.startswith("mcp_oauth_cb_state-A=nonce-A")
    assert "path=/api/v1/mcp/oauth" in cookie.lower()
    assert "httponly" in cookie.lower()
    # The cookie must not outlive the state record it authenticates.
    from src.server.services.mcp_oauth.connect import STATE_TTL_SECONDS

    assert f"max-age={STATE_TTL_SECONDS}" in cookie.lower()


@pytest.mark.asyncio
async def test_two_concurrent_starts_do_not_share_a_cookie_name(monkeypatch):
    flows = iter(
        [
            StartedConnect(
                authorize_url="u",
                state="state-A",
                browser_nonce="nonce-A",
                redirect_uri="https://api.example.com/api/v1/mcp/oauth/callback",
            ),
            StartedConnect(
                authorize_url="u",
                state="state-B",
                browser_nonce="nonce-B",
                redirect_uri="https://api.example.com/api/v1/mcp/oauth/callback",
            ),
        ]
    )

    async def _start(
        user_id, name, *, return_to, web_origin, loopback_redirect, expected_url,
        **_rest,
    ):
        return next(flows)

    monkeypatch.setattr(mod, "start_connect", _start)

    r1, r2 = Response(), Response()
    await mod.oauth_start("srv", "user-1", _request(), r1, None)
    await mod.oauth_start("srv", "user-1", _request(), r2, None)

    [c1] = _set_cookies(r1)
    [c2] = _set_cookies(r2)
    # Distinct names → the second start cannot overwrite the first's nonce.
    assert c1.startswith("mcp_oauth_cb_state-A=nonce-A")
    assert c2.startswith("mcp_oauth_cb_state-B=nonce-B")


@pytest.mark.asyncio
async def test_loopback_start_sets_no_cookie(monkeypatch):
    async def _start(
        user_id, name, *, return_to, web_origin, loopback_redirect, expected_url,
        **_rest,
    ):
        # Loopback callback → no nonce minted (see redirects.callback_is_loopback).
        return StartedConnect(
            authorize_url="u",
            state="state-A",
            browser_nonce="",
            redirect_uri="http://127.0.0.1:8788/mcp/callback",
        )

    monkeypatch.setattr(mod, "start_connect", _start)
    response = Response()

    await mod.oauth_start("srv", "user-1", _request(), response, None)

    assert _set_cookies(response) == []


@pytest.mark.asyncio
async def test_callback_reads_the_cookie_named_for_its_state(monkeypatch):
    seen = {}

    async def _complete(*, state, code, iss, error, error_description, browser_nonce):
        seen["state"] = state
        seen["browser_nonce"] = browser_nonce
        return "/plugins?mcp_connected=srv"

    monkeypatch.setattr(mod, "complete_callback", _complete)
    # The browser carries BOTH flows' cookies; the callback must pick its own.
    request = _request(
        cookies={
            "mcp_oauth_cb_state-A": "nonce-A",
            "mcp_oauth_cb_state-B": "nonce-B",
        }
    )

    resp = await mod.oauth_callback(request, state="state-A", code="code-1")

    assert seen["browser_nonce"] == "nonce-A"  # not nonce-B
    # And the response clears exactly this flow's cookie.
    [cleared] = _set_cookies(resp)
    assert cleared.startswith("mcp_oauth_cb_state-A=")
    assert 'max-age=0' in cleared.lower() or 'expires=' in cleared.lower()


@pytest.mark.asyncio
async def test_callback_without_state_reads_no_cookie(monkeypatch):
    seen = {}

    async def _complete(*, state, code, iss, error, error_description, browser_nonce):
        seen["browser_nonce"] = browser_nonce
        return "/plugins?mcp_error=missing_state"

    monkeypatch.setattr(mod, "complete_callback", _complete)
    request = _request(cookies={"mcp_oauth_cb_state-A": "nonce-A"})

    await mod.oauth_callback(request, state=None, code="code-1")

    assert seen["browser_nonce"] is None


# ---------------------------------------------------------------------------
# The desktop half of phase 1: which body key carries the shell's loopback URI,
# and what the caller is told back about it.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_body_key_that_carries_the_loopback_uri(monkeypatch):
    """``redirect_uri`` in, ``loopback_redirect`` on: the SPA-to-service seam.

    Renaming or mistyping either half leaves both suites green while every
    desktop connect quietly degrades to the hosted callback -- which for the
    vendors this exists for produces no error at all, because their consent
    screen simply never redirects anywhere the shell can hear.

    ``expected_url`` rides the same dict and fails the same silent way: a page
    that names no address is let through by design, so a mistyped key turns the
    gate off everywhere at once with nothing on either side to say so.
    """
    seen = {}

    async def _start(
        user_id, name, *, return_to, web_origin, loopback_redirect, expected_url,
        **_rest,
    ):
        seen["loopback_redirect"] = loopback_redirect
        seen["expected_url"] = expected_url
        return StartedConnect(
            authorize_url="u",
            state="state-A",
            browser_nonce="",
            redirect_uri=loopback_redirect or "https://api.example.com/cb",
        )

    monkeypatch.setattr(mod, "start_connect", _start)

    loopback = "http://127.0.0.1:8788/mcp/callback"
    row_url = "https://mcp.demo.test/mcp"
    await mod.oauth_start(
        "srv",
        "user-1",
        _request(),
        Response(),
        {"redirect_uri": loopback, "expected_url": row_url},
    )
    assert seen["loopback_redirect"] == loopback
    assert seen["expected_url"] == row_url

    await mod.oauth_start("srv", "user-1", _request(), Response(), {})
    assert seen["loopback_redirect"] is None
    assert seen["expected_url"] is None


@pytest.mark.asyncio
async def test_the_start_says_which_callback_it_actually_bound(monkeypatch):
    """The shell armed a listener before this request and cannot otherwise tell.

    A value that fails the loopback check degrades to the hosted callback by
    design, and the request still answers 200. Without the echo the only symptom
    is a listener held for five minutes on a code that was never coming.
    """
    hosted = "https://api.example.com/api/v1/mcp/oauth/callback"

    async def _start(
        user_id, name, *, return_to, web_origin, loopback_redirect, expected_url,
        **_rest,
    ):
        return StartedConnect(
            authorize_url="u",
            state="state-A",
            browser_nonce="",
            redirect_uri=hosted,
        )

    monkeypatch.setattr(mod, "start_connect", _start)

    body = await mod.oauth_start(
        "srv",
        "user-1",
        _request(),
        Response(),
        {"redirect_uri": "http://127.0.0.1:8788/mcp/callback"},
    )

    assert body["redirect_uri"] == hosted, "the caller cannot see it was refused"
    assert body["state"] == "state-A"
