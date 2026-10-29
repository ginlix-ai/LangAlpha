"""What crosses the preview proxy, header by header.

The capability is the hostname, so every rule here is about not leaking it
(referrers, redirects, cookies, shared caches) and not letting the client,
the edge or another origin speak for the sandbox.
"""

from __future__ import annotations

from src.server.preview_proxy.headers import (
    request_headers,
    response_headers,
    rewrite_location,
)

CAP = "https://8080-abcdefghijklmnopqrstuvwxyz234567.preview.example.com"
UP = "https://8080-sbx-1.e2b.app"
INJECT = {"E2B-Traffic-Access-Token": "tok-1"}


def _raw(*pairs: tuple[str, str]) -> list[tuple[bytes, bytes]]:
    return [(k.encode(), v.encode()) for k, v in pairs]


def _request(*pairs: tuple[str, str], websocket: bool = False) -> dict[str, str]:
    out = request_headers(
        _raw(*pairs),
        capability_origin=CAP,
        upstream_origin=UP,
        inject=INJECT,
        websocket=websocket,
    )
    names = [name for name, _ in out]
    assert len(names) == len(set(names)), names
    return dict(out)


def _response(*pairs: tuple[str, str], frame_ancestors: str | None = None):
    out = response_headers(
        _raw(*pairs),
        upstream_origin=UP,
        capability_origin=CAP,
        frame_ancestors=frame_ancestors,
        withheld=INJECT.values(),
    )
    return [(k.decode(), v.decode()) for k, v in out]


class TestRequest:
    def test_the_sandbox_header_is_the_proxys_own(self):
        headers = _request(("e2b-traffic-access-token", "forged"), ("accept", "*/*"))

        assert headers == {"accept": "*/*", "e2b-traffic-access-token": "tok-1"}

    def test_any_injected_header_replaces_the_browsers_copy(self):
        out = request_headers(
            _raw(("X-Sandbox-Key", "forged"), ("Accept", "*/*")),
            capability_origin=CAP,
            upstream_origin=UP,
            inject={"X-Sandbox-Key": "k-1"},
        )

        assert out == [("accept", "*/*"), ("x-sandbox-key", "k-1")]

    def test_client_identity_and_edge_headers_never_reach_the_sandbox(self):
        headers = _request(
            ("Host", CAP.split("://")[1]),
            ("Forwarded", "for=1.2.3.4"),
            ("X-Forwarded-For", "1.2.3.4"),
            ("X-Forwarded-Host", "app.example.com"),
            ("X-Real-IP", "1.2.3.4"),
            ("CF-Connecting-IP", "1.2.3.4"),
            ("True-Client-IP", "1.2.3.4"),
            ("CDN-Loop", "cloudflare"),
        )

        assert headers == {"e2b-traffic-access-token": "tok-1"}

    def test_hop_by_hop_and_connection_listed_headers_are_dropped(self):
        headers = _request(
            ("Connection", "keep-alive, X-Private"),
            ("X-Private", "1"),
            ("Keep-Alive", "timeout=5"),
            ("TE", "trailers"),
            ("Transfer-Encoding", "chunked"),
            ("Upgrade", "h2c"),
            ("Proxy-Authorization", "Basic x"),
            ("X-Kept", "1"),
        )

        assert set(headers) == {"x-kept", "e2b-traffic-access-token"}

    def test_cookies_are_the_preview_apps_own(self):
        # The boot check keeps the preview base off the app's site, so the
        # browser sends a preview host only the cookies that host set.
        cookie = "sb-access-token=s1; theme=dark"

        assert _request(("Cookie", cookie))["cookie"] == cookie

    def test_the_previews_own_origin_is_moved_onto_the_sandbox(self):
        headers = _request(("Origin", CAP), ("Referer", f"{CAP}/page?x=1#top"))

        assert headers["origin"] == UP
        assert headers["referer"] == f"{UP}/page?x=1#top"

    def test_a_foreign_origin_passes_so_the_app_can_refuse_it(self):
        headers = _request(
            ("Origin", "https://evil.example"), ("Referer", f"{CAP}.evil.example/x")
        )

        assert headers["origin"] == "https://evil.example"
        assert headers["referer"] == f"{CAP}.evil.example/x"

    def test_authorization_is_the_apps_to_read(self):
        assert _request(("Authorization", "Bearer app"))["authorization"] == "Bearer app"

    def test_a_websocket_handshake_leaves_its_own_fields_to_the_client_library(self):
        headers = _request(
            ("Origin", CAP),
            ("User-Agent", "browser"),
            ("Sec-WebSocket-Key", "k"),
            ("Sec-WebSocket-Protocol", "chat"),
            ("Sec-WebSocket-Extensions", "permessage-deflate"),
            ("Cookie", "a=1"),
            websocket=True,
        )

        assert headers == {
            "origin": UP,
            "user-agent": "browser",
            "cookie": "a=1",
            "e2b-traffic-access-token": "tok-1",
        }


class TestLocation:
    def test_a_redirect_to_the_sandbox_comes_back_through_the_proxy(self):
        assert rewrite_location(f"{UP}/login?next=/", upstream_origin=UP, capability_origin=CAP) == f"{CAP}/login?next=/"
        assert rewrite_location(UP, upstream_origin=UP, capability_origin=CAP) == CAP
        assert rewrite_location("//8080-sbx-1.e2b.app/x", upstream_origin=UP, capability_origin=CAP) == f"{CAP}/x"

    def test_relative_and_foreign_redirects_are_left_alone(self):
        for value in ("/login", "login", "https://example.com/login", f"{UP}.evil.example/x"):
            assert rewrite_location(value, upstream_origin=UP, capability_origin=CAP) == value


class TestResponse:
    def test_every_reply_withholds_the_referrer_and_stays_out_of_indexes(self):
        headers = _response(("Referrer-Policy", "unsafe-url"), ("X-Robots-Tag", "all"))

        assert ("referrer-policy", "no-referrer") in headers
        assert ("x-robots-tag", "noindex") in headers
        assert [n for n, _ in headers].count("referrer-policy") == 1
        assert [n for n, _ in headers].count("x-robots-tag") == 1

    def test_no_shared_cache_keeps_a_reply_past_a_revocation(self):
        assert ("cache-control", "private") in _response()
        assert ("cache-control", "private, max-age=60") in _response(
            ("Cache-Control", "public, max-age=60, s-maxage=600")
        )
        assert ("cache-control", "no-store") in _response(("Cache-Control", "no-store"))
        assert ("cache-control", "private, max-age=5") in _response(
            ("Cache-Control", "private, max-age=5")
        )

    def test_a_cookie_stays_on_its_one_preview_host(self):
        headers = _response(
            ("Set-Cookie", "a=1; Domain=.e2b.app; Path=/; HttpOnly"),
            ("Set-Cookie", "b=2; domain=preview.example.com"),
        )

        assert [v for n, v in headers if n == "set-cookie"] == ["a=1; Path=/; HttpOnly", "b=2"]

    def test_redirects_are_rewritten(self):
        headers = dict(_response(("Location", f"{UP}/next"), ("Content-Location", f"{UP}/c")))

        assert headers["location"] == f"{CAP}/next"
        assert headers["content-location"] == f"{CAP}/c"

    def test_fields_the_server_or_the_sandbox_host_own_are_dropped(self):
        names = {
            n
            for n, _ in _response(
                ("Date", "x"),
                ("Server", "TornadoServer"),
                ("Alt-Svc", 'h3=":443"'),
                ("E2B-Debug", "x"),
                ("Connection", "close"),
                ("Transfer-Encoding", "chunked"),
                ("Content-Type", "text/html"),
            )
        }

        assert names == {
            "content-type",
            "cache-control",
            "referrer-policy",
            "x-robots-tag",
        }

    def test_a_field_carrying_the_injected_header_never_reaches_the_page(self):
        names = {
            n
            for n, _ in _response(
                ("X-Echo", "tok-1"),
                ("Set-Cookie", "seen=tok-1; Path=/"),
                ("Content-Type", "text/plain"),
            )
        }

        assert "x-echo" not in names
        assert "set-cookie" not in names
        assert "content-type" in names

    def test_framing_is_limited_only_when_the_deployment_says_so(self):
        assert not any(n == "content-security-policy" for n, _ in _response())
        assert ("content-security-policy", "frame-ancestors 'self' https://app.example.com") in _response(
            frame_ancestors="'self' https://app.example.com"
        )
