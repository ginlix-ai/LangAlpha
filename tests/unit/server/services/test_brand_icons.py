"""Vendor-mark resolution: ranking, sniffing, and the size floor.

Every fixture here is a shape taken from a real vendor, because each one of
them broke a plausible implementation before it was measured.
"""

from __future__ import annotations

import pytest
from unittest.mock import patch

from src.server.services.brokerages import BROKERAGES
from src.server.services.brand_icons import (
    MIN_PIXELS,
    _declared_icons,
    _pixels,
    _sniff,
    icon_response,
)

# robinhood.com: the unsized apple-touch-icon really is 60px and sits beside a
# declared 152px one, so any implementation that scores the convention's 180
# picks the smaller mark.
ROBINHOOD_HEAD = """
<link href="/us/en/rh_favicon_32.png?v=2024" rel="shortcut icon" type="image/png"/>
<link href="/us/en/rh_favicon_32.png?v=2024" rel="icon" type="image/png"/>
<link href="/us/en/rh_favicon_60.png?v=2024" rel="apple-touch-icon" type="image/png"/>
<link href="/us/en/rh_favicon_76.png?v=2024" rel="apple-touch-icon" sizes="76x76"/>
<link href="/us/en/rh_favicon_120.png?v=2024" rel="apple-touch-icon" sizes="120x120"/>
<link href="/us/en/rh_favicon_152.png?v=2024" rel="apple-touch-icon" sizes="152x152"/>
"""

# www.interactivebrokers.com: relative hrefs under a path nobody would guess.
IBKR_HEAD = """
<link rel="icon" sizes="192x192" href="/images/web/favicons/home-screen-icon-192x192.png" />
<link rel="icon" sizes="128x128" href="/images/web/favicons/home-screen-icon-128x128.png" />
<link rel="apple-touch-icon" sizes="57x57" href="/images/web/favicons/apple-touch-icon-57x57.png" />
<link rel="apple-touch-icon" sizes="144x144" href="/images/web/favicons/apple-touch-icon-144x144.png" />
"""


def _png(width: int, height: int) -> bytes:
    return (
        b"\x89PNG\r\n\x1a\n"
        + b"\x00\x00\x00\rIHDR"
        + width.to_bytes(4, "big")
        + height.to_bytes(4, "big")
    )


def _ico(*sides: int) -> bytes:
    header = b"\x00\x00\x01\x00" + len(sides).to_bytes(2, "little")
    return header + b"".join(
        bytes([side % 256, side % 256]) + b"\x00" * 14 for side in sides
    )


class TestRanking:
    def test_declared_size_beats_the_convention(self):
        """A declared 152 outranks an unsized apple-touch-icon."""
        best = _declared_icons(ROBINHOOD_HEAD, "https://robinhood.com/")[0]
        assert best == "https://robinhood.com/us/en/rh_favicon_152.png?v=2024"

    def test_largest_declared_size_wins(self):
        assert _declared_icons(IBKR_HEAD, "https://www.interactivebrokers.com/")[0] == (
            "https://www.interactivebrokers.com/images/web/favicons/home-screen-icon-192x192.png"
        )

    def test_an_unreadable_size_claims_nothing_rather_than_everything(self):
        """The HTML comes from a site we do not control.

        ``int()`` refuses a string past 4,300 digits, and that ValueError
        would escape a reducer whose worst case is meant to be the monogram.
        Matching six digits out of the middle of a long run is the other wrong
        answer: it would outrank a link that gave a real size.
        """
        head = (
            '<link rel="icon" sizes="' + "9" * 5000 + 'x1" href="/huge.png">'
            '<link rel="icon" sizes="64x64" href="/real.png">'
        )
        assert _declared_icons(head, "https://x.test/")[0] == "https://x.test/real.png"

    def test_svg_outranks_every_raster(self):
        head = (
            '<link rel="icon" sizes="192x192" href="/big.png">'
            '<link rel="icon" href="/mark.svg">'
        )
        assert _declared_icons(head, "https://x.test/")[0] == "https://x.test/mark.svg"

    def test_unsized_apple_touch_still_beats_a_bare_icon(self):
        head = (
            '<link rel="icon" href="/favicon.png">'
            '<link rel="apple-touch-icon" href="/touch.png">'
        )
        assert _declared_icons(head, "https://x.test/")[0] == "https://x.test/touch.png"

    def test_non_icon_links_are_ignored(self):
        head = (
            '<link rel="stylesheet" href="/app.css">'
            '<link rel="preload" as="image" href="/hero.png">'
        )
        assert _declared_icons(head, "https://x.test/") == []

    def test_duplicate_hrefs_collapse(self):
        """Robinhood declares its 32px twice; the list must not."""
        urls = _declared_icons(ROBINHOOD_HEAD, "https://robinhood.com/")
        assert len(urls) == len(set(urls))


class TestSniff:
    def test_html_served_as_an_image_is_refused(self):
        """robinhood.com answers /apple-touch-icon.png with 200 and an error
        page, so a vendor's content-type is not evidence."""
        assert _sniff(b"<!DOCTYPE html><html><body>Not found</body></html>") is None

    def test_json_is_refused(self):
        assert _sniff(b'{"error":"not found"}') is None

    @pytest.mark.parametrize(
        "data,expected",
        [
            (_png(64, 64), "image/png"),
            (_ico(48), "image/vnd.microsoft.icon"),
            (b"GIF89a\x00", "image/gif"),
            (b"\xff\xd8\xff\xe0", "image/jpeg"),
            (b"RIFF\x00\x00\x00\x00WEBPVP8 ", "image/webp"),
            (b'  <svg xmlns="http://www.w3.org/2000/svg"/>', "image/svg+xml"),
        ],
    )
    def test_real_images_are_typed_from_their_bytes(self, data, expected):
        assert _sniff(data) == expected

    def test_xml_that_is_not_svg_is_refused(self):
        assert _sniff(b'<?xml version="1.0"?><rss></rss>') is None


class TestPixels:
    def test_png_dimensions_come_from_the_header(self):
        assert _pixels(_png(192, 192), "image/png") == 192

    def test_the_shorter_side_decides(self):
        assert _pixels(_png(512, 24), "image/png") == 24

    def test_ico_reports_its_largest_entry(self):
        """api.ibkr.com's favicon is 16px and must fall below the floor, while
        robinhood.com's holds a 48 that must not."""
        assert _pixels(_ico(16), "image/vnd.microsoft.icon") < MIN_PIXELS
        assert _pixels(_ico(16, 32, 48), "image/vnd.microsoft.icon") == 48

    def test_a_zero_side_means_256(self):
        assert _pixels(_ico(256), "image/vnd.microsoft.icon") == 256

    def test_scalable_and_unjudged_formats_do_not_bind(self):
        assert _pixels(b"<svg/>", "image/svg+xml") is None
        assert _pixels(b"GIF89a", "image/gif") is None


class TestRegistrySites:
    """The brand site is named, because deriving it from the endpoint fails.

    Both of these would break a trim-the-subdomain rule: robinhood's endpoint
    host has no page, and no prefix of ``api.ibkr.com`` is
    ``interactivebrokers.com`` at all.
    """

    def test_every_shipped_brokerage_names_its_site(self):
        assert all(b.site and "/" not in b.site for b in BROKERAGES)

    def test_the_site_is_not_assumed_to_be_the_endpoint_host(self):
        sites = {b.name: b.site for b in BROKERAGES}
        assert sites["ibkr"] == "interactivebrokers.com"
        assert sites["robinhood"] == "robinhood.com"


class TestServedMarksCannotRunAsDocuments:
    """The art routes are same-origin, unauthenticated, and serve SVG.

    An MCP server declares its own mark, so for a user-added server those
    bytes belong to whoever runs it. SVG is a document: navigated to directly
    rather than drawn in an ``<img>``, an unsandboxed one executes script
    under this app's origin, which is a stored XSS with the handle as its
    only gate. These two headers are the whole defense and nothing else in
    the response depends on them, so they are easy to drop by accident.
    """

    @pytest.mark.asyncio
    async def test_a_served_mark_is_sandboxed_and_not_sniffable(self, monkeypatch):
        from src.server.services import brand_icons

        svg = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'

        async def _fake(source):
            return brand_icons.BrandIcon(content=svg, content_type="image/svg+xml")

        monkeypatch.setattr(brand_icons, "icon_for_source", _fake)
        response = await icon_response("example.test")

        assert response.status_code == 200
        assert "sandbox" in response.headers["content-security-policy"]
        assert response.headers["x-content-type-options"] == "nosniff"

    @pytest.mark.asyncio
    async def test_the_404_carries_them_too(self):
        # The miss is cacheable and same-origin like any other answer, so it
        # must not be the one response that arrives without the headers.
        response = await icon_response(None)

        assert response.status_code == 404
        assert "sandbox" in response.headers["content-security-policy"]
        assert response.headers["x-content-type-options"] == "nosniff"


class TestPublishingASourceIsBounded:
    """A handshake cannot park unbounded bytes in shared storage.

    The source is the server operator's own string and it is stored for a
    month, while the size check that would reject it runs only when a reader
    asks for the image. Something no reader can accept is not worth keeping.
    """

    @pytest.mark.asyncio
    async def test_an_oversized_source_is_not_published(self):
        from src.server.services import brand_icons

        stored: dict = {}

        class _Cache:
            async def set(self, key, value, ttl=None):
                stored[key] = value

        with patch.object(brand_icons, "get_cache_client", lambda: _Cache()):
            huge = "data:image/png;base64," + "A" * (brand_icons.MAX_ICON_BYTES * 2)
            assert await brand_icons.publish_icon_source(huge) is None
            assert stored == {}

    @pytest.mark.asyncio
    async def test_an_ordinary_source_still_publishes(self):
        from src.server.services import brand_icons

        stored: dict = {}

        class _Cache:
            async def set(self, key, value, ttl=None):
                stored[key] = value

        with patch.object(brand_icons, "get_cache_client", lambda: _Cache()):
            handle = await brand_icons.publish_icon_source("https://a.test/i.png")
            assert handle and len(stored) == 1


class TestAMalformedRedirectIsAMiss:
    """The resolver has one fallback, and reaching it must not need a 500.

    Every hop is re-pinned, so the ``Location`` an upstream writes is fed back
    through ``urljoin`` -- which raises ``ValueError`` on a URL it cannot parse
    (``http://[bad``). That is neither an ``HTTPError`` nor an ``OSError``, so
    it escaped the one handler in ``_get`` and turned a decorative route into
    a 500. The caller wants the cacheable miss and its own monogram.
    """

    @staticmethod
    def _redirecting_to(location: str):
        import contextlib

        import httpx

        class _Client:
            async def get(self, url):
                return httpx.Response(
                    302, headers={"location": location}, request=httpx.Request("GET", url)
                )

        @contextlib.asynccontextmanager
        async def _client(target, *, max_bytes):
            yield _Client()

        return _client

    @pytest.mark.asyncio
    async def test_an_unparseable_location_stops_the_walk(self, monkeypatch):
        from src.server.services import brand_icons

        async def _pin(url):
            return url

        monkeypatch.setattr(brand_icons, "pin_public_url", _pin)
        monkeypatch.setattr(
            brand_icons, "pinned_stream_client", self._redirecting_to("http://[bad")
        )

        assert await brand_icons._get("https://vendor.test/i.png", max_bytes=1000) is None

    @pytest.mark.asyncio
    async def test_the_route_answers_the_ordinary_404(self, monkeypatch):
        from src.server.services import brand_icons

        async def _pin(url):
            return url

        monkeypatch.setattr(brand_icons, "pin_public_url", _pin)
        monkeypatch.setattr(
            brand_icons, "pinned_stream_client", self._redirecting_to("http://[bad")
        )

        response = await icon_response("https://vendor.test/i.png")

        assert response.status_code == 404


class TestASourceThatCannotBeStored:
    """A handshake string that no UTF-8 encoder will take.

    JSON carries lone surrogates, so a server can put one in an icon source
    and Python will hold it happily until something encodes it. Here that is
    the digest, and the raise is neither an HTTPError nor an OSError, so it
    would leave the whole builtin listing 500ing over a decorative field.
    """

    @pytest.mark.asyncio
    async def test_a_lone_surrogate_names_no_mark(self):
        from src.server.services import brand_icons

        assert await brand_icons.publish_icon_source(
            "https://vendor.test/" + chr(0xD800)
        ) is None

    @pytest.mark.asyncio
    async def test_an_ordinary_source_still_publishes(self):
        from src.server.services import brand_icons

        assert await brand_icons.publish_icon_source(
            "https://vendor.test/logo.png"
        ) is not None


class TestAPortThatIsNotAPort:
    """``urlsplit`` parses the port lazily, so the raise lands on the read.

    Every caller here holds a third-party string, so an unusable one is an
    ordinary input. It has to arrive as the refusal the callers already
    handle, not as a bare ValueError that turns a cacheable miss into a 500.
    """

    @pytest.mark.parametrize(
        "url",
        [
            "https://vendor.test:99999/icon.png",
            "https://vendor.test:notaport/icon.png",
        ],
    )
    @pytest.mark.asyncio
    async def test_it_is_refused_as_egress(self, url):
        from src.server.utils.egress_guard import EgressBlockedError, pin_public_url

        with pytest.raises(EgressBlockedError):
            await pin_public_url(url)

    @pytest.mark.asyncio
    async def test_an_unparseable_url_is_refused_the_same_way(self):
        from src.server.utils.egress_guard import EgressBlockedError, pin_public_url

        with pytest.raises(EgressBlockedError):
            await pin_public_url("https://[bad/icon.png")

    @pytest.mark.asyncio
    async def test_the_route_answers_the_ordinary_404(self):
        response = await icon_response("https://vendor.test:99999/icon.png")

        assert response.status_code == 404
