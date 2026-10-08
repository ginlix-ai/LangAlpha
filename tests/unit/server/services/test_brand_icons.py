"""Vendor-mark resolution: ranking, sniffing, and the size floor.

Every fixture here is a shape taken from a real vendor, because each one of
them broke a plausible implementation before it was measured.
"""

from __future__ import annotations

import asyncio
import base64
import time

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


class TestAHostilePageIsReadInOnePass:
    """The parse runs on the event loop, where no timeout can stop it.

    These shapes took seconds at 40,000 characters and grew with the square of
    the page, so a 1 MB page held a worker for over an hour.
    """

    @pytest.mark.parametrize(
        "page",
        [
            '<link rel="icon" ' + "a" * 40_000 + ">",
            "<link " * 40_000,
        ],
    )
    def test_it_is_parsed_at_once(self, page):
        started = time.perf_counter()
        _declared_icons(page, "https://x.test/")
        assert time.perf_counter() - started < 1.0

    def test_names_still_read_beside_a_hyphen(self):
        head = '<link data-x=1 rel="icon" sizes="64x64" href="/m.png">'
        assert _declared_icons(head, "https://x.test/") == ["https://x.test/m.png"]


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
                    302,
                    headers={"location": location},
                    request=httpx.Request("GET", url),
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

        assert (
            await brand_icons._get("https://vendor.test/i.png", max_bytes=1000) is None
        )

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

        assert (
            await brand_icons.publish_icon_source("https://vendor.test/" + chr(0xD800))
            is None
        )

    @pytest.mark.asyncio
    async def test_an_ordinary_source_still_publishes(self):
        from src.server.services import brand_icons

        assert (
            await brand_icons.publish_icon_source("https://vendor.test/logo.png")
            is not None
        )


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


class _Cache:
    """Redis as a dict, with the lock a refresh takes."""

    def __init__(self, entries=None, *, locked=False):
        self.entries = dict(entries or {})
        self.ttls: dict[str, int | None] = {}
        self.locked = locked
        self.held: set[str] = set()

    async def get(self, key):
        return self.entries.get(key)

    async def set(self, key, value, ttl=None):
        self.entries[key] = value
        self.ttls[key] = ttl
        return True

    async def acquire_lock(self, key, token, ttl_ms):
        if self.locked or key in self.held:
            return False
        self.held.add(key)
        return True

    async def release_lock(self, key, token):
        self.held.discard(key)


OLD_MARK = _png(152, 152)
NEW_MARK = _png(180, 180)
SITE_KEY = "brand-icon:v1:vendor.test"


def _stored(content: bytes, *, fresh_for: float, found_ago: float = 0) -> dict:
    return {
        "content": base64.b64encode(content).decode("ascii"),
        "content_type": "image/png",
        "fresh_until": time.time() + fresh_for,
        "found_at": time.time() - found_ago,
    }


def _resolving(monkeypatch, name: str, found: bytes | None) -> list[str]:
    """Stand in for one way of finding a mark, recording who it was asked for."""
    from src.server.services import brand_icons

    asked: list[str] = []

    async def _find(source):
        asked.append(source)
        if found is None:
            return None
        return brand_icons.BrandIcon(content=found, content_type="image/png")

    monkeypatch.setattr(brand_icons, name, _find)
    return asked


async def _settle():
    from src.server.services import brand_icons

    await asyncio.gather(*brand_icons._REFRESHES)


class TestAStoredMarkIsServedWhileItRefreshes:
    """Finding a mark reads a vendor's homepage, which can take seconds.

    moomoo's is two redirects and a 470 KB page. Once any answer is stored, a
    viewer gets it at once and the refresh runs behind them, and a refresh that
    finds nothing keeps what was there: a vendor's site being down for a day
    is no reason to draw its logo as a letter.
    """

    @pytest.mark.asyncio
    async def test_a_stale_mark_is_served_and_replaced_behind_it(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", NEW_MARK)

        served = await brand_icons.icon_for_site("vendor.test")
        await _settle()

        assert served.content == OLD_MARK
        assert asked == ["vendor.test"]
        stored = brand_icons._read(cache.entries[SITE_KEY])
        assert stored.icon.content == NEW_MARK
        assert stored.fresh

    @pytest.mark.asyncio
    async def test_a_refresh_that_finds_nothing_keeps_the_mark(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        _resolving(monkeypatch, "_from_site", None)

        await brand_icons.icon_for_site("vendor.test")
        await _settle()

        entry = cache.entries[SITE_KEY]
        assert base64.b64decode(entry["content"]) == OLD_MARK
        # Kept, but asked about again as soon as a miss would be.
        assert entry["fresh_until"] - time.time() <= brand_icons._MISS_FRESH

    @pytest.mark.asyncio
    async def test_a_current_mark_asks_nobody(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=3600)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", NEW_MARK)

        served = await brand_icons.icon_for_site("vendor.test")
        await _settle()

        assert served.content == OLD_MARK
        assert asked == []

    @pytest.mark.asyncio
    async def test_a_refresh_already_running_elsewhere_is_not_repeated(
        self, monkeypatch
    ):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)}, locked=True)
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", NEW_MARK)

        served = await brand_icons.icon_for_site("vendor.test")
        await _settle()

        assert served.content == OLD_MARK
        assert asked == []

    @pytest.mark.asyncio
    async def test_an_entry_stored_before_freshness_was_kept_is_refreshed(
        self, monkeypatch
    ):
        from src.server.services import brand_icons

        old_format = {
            "content": base64.b64encode(OLD_MARK).decode("ascii"),
            "content_type": "image/png",
        }
        cache = _Cache({SITE_KEY: old_format})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", NEW_MARK)

        served = await brand_icons.icon_for_site("vendor.test")
        await _settle()

        assert served.content == OLD_MARK
        assert asked == ["vendor.test"]

    @pytest.mark.asyncio
    async def test_a_site_never_seen_is_resolved_for_its_first_viewer(
        self, monkeypatch
    ):
        from src.server.services import brand_icons

        cache = _Cache()
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        _resolving(monkeypatch, "_from_site", NEW_MARK)

        served = await brand_icons.icon_for_site("vendor.test")

        assert served.content == NEW_MARK
        assert brand_icons._read(cache.entries[SITE_KEY]).fresh

    @pytest.mark.asyncio
    async def test_viewers_arriving_during_a_refresh_start_nothing(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", NEW_MARK)
        started: list[str] = []
        refresh = brand_icons._refresh

        async def _counted(key, *rest):
            started.append(key)
            await refresh(key, *rest)

        monkeypatch.setattr(brand_icons, "_refresh", _counted)

        served = await asyncio.gather(
            *(brand_icons.icon_for_site("vendor.test") for _ in range(20))
        )
        await _settle()

        assert {icon.content for icon in served} == {OLD_MARK}
        assert started == [SITE_KEY]
        assert asked == ["vendor.test"]

    @pytest.mark.asyncio
    async def test_a_refresh_done_elsewhere_meanwhile_is_not_undone(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", None)

        await brand_icons.icon_for_site("vendor.test")
        # Another worker stores the new mark before this one's refresh runs.
        cache.entries[SITE_KEY] = _stored(NEW_MARK, fresh_for=3600)
        await _settle()

        assert asked == []
        assert base64.b64decode(cache.entries[SITE_KEY]["content"]) == NEW_MARK

    @pytest.mark.asyncio
    async def test_a_reread_that_fails_writes_nothing(self, monkeypatch):
        """The cache reads a failure as absence, and absence confirms nothing:
        another worker may have stored a newer mark since this one's read."""
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)})
        reads = 0
        read = cache.get

        async def _fails_after_the_first(key):
            nonlocal reads
            reads += 1
            return await read(key) if reads == 1 else None

        cache.get = _fails_after_the_first
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked = _resolving(monkeypatch, "_from_site", None)

        served = await brand_icons.icon_for_site("vendor.test")
        await _settle()

        assert served.content == OLD_MARK
        assert asked == []
        assert cache.ttls == {}
        assert cache.held == set()

    @pytest.mark.asyncio
    async def test_a_refresh_stopped_at_shutdown_releases_its_lock(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        asked: list[str] = []

        async def _slow_vendor(source):
            asked.append(source)
            await asyncio.sleep(60)

        monkeypatch.setattr(brand_icons, "_from_site", _slow_vendor)

        await brand_icons.icon_for_site("vendor.test")
        await asyncio.sleep(0)
        assert asked == ["vendor.test"] and cache.held

        await brand_icons.stop_refreshes()

        assert cache.held == set()
        assert not brand_icons._REFRESHES
        assert base64.b64decode(cache.entries[SITE_KEY]["content"]) == OLD_MARK

    @pytest.mark.asyncio
    async def test_a_mark_not_found_for_a_week_is_let_go(self, monkeypatch):
        from src.server.services import brand_icons

        week = brand_icons._KEEP_UNFOUND
        cache = _Cache({SITE_KEY: _stored(OLD_MARK, fresh_for=-1, found_ago=week + 1)})
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)
        _resolving(monkeypatch, "_from_site", None)

        await brand_icons.icon_for_site("vendor.test")
        await _settle()

        assert cache.entries[SITE_KEY]["content"] is None
        assert cache.ttls[SITE_KEY] == brand_icons._MISS_KEEP

    @pytest.mark.asyncio
    async def test_a_resolver_that_raises_is_stored_as_a_miss(self, monkeypatch):
        from src.server.services import brand_icons

        cache = _Cache()
        monkeypatch.setattr(brand_icons, "get_cache_client", lambda: cache)

        async def _find(host):
            raise ValueError("label too long")

        monkeypatch.setattr(brand_icons, "_from_site", _find)

        assert await brand_icons.icon_for_site("vendor.test") is None
        assert cache.entries[SITE_KEY]["content"] is None
        assert cache.ttls[SITE_KEY] == brand_icons._MISS_KEEP
