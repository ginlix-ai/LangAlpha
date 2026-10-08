"""The vendor's own mark for a connector row, resolved from the vendor's site.

A row that can place orders deserves the broker's logo rather than a letter,
and the only place that logo reliably exists is the vendor's own page. Guessing
paths does not work: ``/favicon.ico`` is missing on ``agent.robinhood.com``,
16x16 on ``api.ibkr.com``, and ``/apple-touch-icon.png`` answers 200 with an
HTML error page on ``robinhood.com``. The usable art is only ever named in
``<link rel="icon">`` — ``/us/en/rh_favicon_152.png``,
``/images/web/favicons/home-screen-icon-192x192.png`` — so finding it means
reading the page.

That is why this runs on the host. A browser cannot read another origin's
``<link>`` tags, one resolution here serves every user instead of every
settings-page render telling a broker who is looking, and same-origin bytes
sidestep whatever hotlink rules the vendor keeps. The caller names a vendor we
ship, never a host, so there is no URL for a page to point this at.

Which site to ask is the registry's answer, not a rule applied to the endpoint.
Trimming labels off a host looks like it would work and does not: it turns
``api.ibkr.com`` into ``ibkr.com``, which only reaches the brand because that
domain happens to redirect twice, and it turns ``example.co.uk`` into a public
suffix. A vendor's website is a fact known when the vendor is added, so it is
written down beside the endpoint rather than guessed at from it.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx2
from fastapi import Response

from src.server.services.mcp_oauth.http import OAuthHopBlocked, pinned_stream_client
from src.server.utils.egress_guard import EgressBlockedError, pin_public_url
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

# A mark is single-digit KB. Both caps are headroom against a hostile pump,
# and the icon cap is also what keeps a cached mark small enough to sit in
# Redis as base64.
MAX_ICON_BYTES = 256 * 1024
MAX_PAGE_BYTES = 1_048_576
DEADLINE_SECONDS = 12.0
_MAX_REDIRECTS = 3

# Below this a mark reads as a smudge at the 28px a row draws it, and worse
# at the 40px of a detail header — a clean monogram beats a blurred logo.
# api.ibkr.com's 16px favicon is the shape this rejects.
MIN_PIXELS = 32

# Art changes on a rebrand, so a hit is current for a week. A miss is usually
# a site with nothing to find, but it is also every transient failure, so it is
# asked again sooner. Past either, the stored answer is still served while a
# refresh runs behind it: resolving reads a vendor's homepage and can take
# seconds (moomoo's is two redirects and a 470 KB page), and no viewer should
# wait on that more than once. A refresh that finds nothing keeps the mark it
# had, so a bad day on a vendor's site does not turn its logo into a letter,
# but only for a week past the last time the mark was found: art that is gone
# for good gives way to the next source, or to the letter.
_HIT_FRESH = 7 * 24 * 3600
_MISS_FRESH = 6 * 3600
_KEEP_UNFOUND = _HIT_FRESH
# How long an answer outlives its last write. Each refresh rewrites it, so only
# a source nobody has asked about in this long resolves in front of a viewer.
# A miss holds nothing worth keeping that long, and an older build reads any
# entry as current for as long as it lives.
_KEEP = 90 * 24 * 3600
_MISS_KEEP = 2 * _MISS_FRESH
_CACHE_PREFIX = "brand-icon:v1:"
# One refresh per answer at a time across workers. The lease outlasts a whole
# refresh, whose reread and write can each wait out the cache's pool and socket
# timeouts around a resolve of DEADLINE_SECONDS, so no second refresh starts
# while the first can still write. Only a worker that dies mid-refresh holds it
# this long, and the stale mark is served meanwhile.
_REFRESH_LOCK_PREFIX = "brand-icon-refresh:v1:"
_REFRESH_LOCK_MS = 60_000

# What a browser is told, kept here because it has to agree with the windows
# above and cannot from another file. A client re-checks a mark daily but may
# draw a stale one for as long as the answer here stays current, so nobody
# waits on a re-resolve. A miss expires sooner in the browser than in the
# cache: resolving is the expensive half, and a site that gains a logo should
# reappear without waiting out both.
_HIT_CACHE = f"public, max-age=86400, stale-while-revalidate={_HIT_FRESH}"
_MISS_CACHE = "public, max-age=3600"

# Brand art is fetched from third parties and, for an MCP server, from bytes
# the server's own operator chose. SVG is a document format: navigated to
# directly it executes script under whatever origin served it, and these
# routes are same-origin with the app and unauthenticated. `sandbox` drops the
# response into an opaque origin so a mark can never reach the app's session,
# and `nosniff` keeps a mislabelled body from being re-read as HTML. Inline
# style stays allowed because logos legitimately carry it, and neither header
# affects the `<img>` render that is the only intended use.
_ART_SAFETY = {
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; sandbox",
    "X-Content-Type-Options": "nosniff",
}


@dataclass(frozen=True)
class BrandIcon:
    content: bytes
    content_type: str


# Sniffed, never echoed: robinhood.com answers /apple-touch-icon.png with 200
# and an HTML error page, so a vendor's own content-type is not evidence of
# anything. What we serve is what the bytes actually are.
_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\x00\x00\x01\x00", "image/vnd.microsoft.icon"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"\xff\xd8\xff", "image/jpeg"),
)

# Linear on any page, because the parse runs in one step on the event loop
# where no timeout can stop it. A tag ends at the next ``<`` and an attribute
# name starts only where a name can begin, so a page built of unclosed
# ``<link`` runs or of one long name with no ``=`` is read in one pass instead
# of once from every character.
_LINK_RE = re.compile(r"<link\b[^<>]*>", re.IGNORECASE)
_ATTR_RE = re.compile(
    r"""(?<![a-zA-Z0-9-])([a-zA-Z][a-zA-Z0-9-]*)\s*=\s*"""
    r"""(?:"([^"]*)"|'([^']*)'|([^\s"'>]+))"""
)
# Bounded and delimited on purpose. The HTML is fetched from a site we do not
# control, and an unbounded run of digits reaches ``int()``, which refuses a
# string past 4,300 digits and would raise out of a reducer whose worst case is
# meant to be the monogram. The lookarounds make an over-long run match nothing
# rather than matching six digits out of its middle: an unreadable size must
# not outrank a link that gave a real one. Six is the same ceiling
# ``mcp_identity._MAX_DIMENSION_DIGITS`` uses for the handshake's icons.
_SIZES_RE = re.compile(r"(?<!\d)(\d{1,6})\s*[x×]\s*(\d{1,6})(?!\d)")


def _sniff(data: bytes) -> str | None:
    """The image type these bytes actually are, or None if they are not one."""
    for prefix, content_type in _MAGIC:
        if data.startswith(prefix):
            return content_type
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    head = data[:1024].lstrip()
    if head[:5] == b"<?xml" or head[:4] == b"<svg":
        return "image/svg+xml" if b"<svg" in data[:4096] else None
    return None


def _pixels(data: bytes, content_type: str) -> int | None:
    """The mark's shorter side, or None for formats whose size does not bind.

    Only PNG and ICO are measured because they are the only formats a favicon
    is small in — SVG scales, and the rest are rare enough that guessing wrong
    costs a monogram rather than a bad render.
    """
    if content_type == "image/png" and len(data) >= 24:
        return min(
            int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
        )
    if content_type == "image/vnd.microsoft.icon":
        # An .ico is a directory of images; the largest is the one we would
        # draw, and a zero byte in the entry means 256.
        count = int.from_bytes(data[4:6], "little")
        best = 0
        for i in range(min(count, 32)):
            off = 6 + i * 16
            if off + 2 > len(data):
                break
            best = max(best, min(data[off] or 256, data[off + 1] or 256))
        return best or None
    return None


def _declared_icons(html: str, base_url: str) -> list[str]:
    """Absolute icon URLs the page declares, best first.

    A declared ``sizes`` is a fact and everything else is a guess, so the two
    never compete: sized icons rank above unsized ones regardless of what the
    convention for the unsized one would have been. Robinhood is why — its
    unsized apple-touch-icon is 60px and sits in the same head as a declared
    152px one, so scoring the convention's 180px against a real 152 picked the
    smaller mark. SVG outranks both because scalable has no size to lose to.
    """
    ranked: list[tuple[tuple[int, int], str]] = []
    for tag in _LINK_RE.findall(html):
        attrs = {
            m.group(1).lower(): (m.group(2) or m.group(3) or m.group(4) or "")
            for m in _ATTR_RE.finditer(tag)
        }
        rel = attrs.get("rel", "").lower()
        href = attrs.get("href", "").strip()
        if not href or "icon" not in rel:
            continue
        if href.lower().split("?")[0].endswith(".svg"):
            score = (3, 0)
        elif (size := _SIZES_RE.search(attrs.get("sizes", ""))) is not None:
            score = (2, min(int(size.group(1)), int(size.group(2))))
        elif "apple-touch-icon" in rel:
            score = (1, 0)
        else:
            score = (0, 0)
        try:
            ranked.append((score, urljoin(base_url, href)))
        except ValueError:
            continue
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    seen: set[str] = set()
    return [url for _score, url in ranked if not (url in seen or seen.add(url))]


async def _get(url: str, *, max_bytes: int) -> tuple[httpx2.Response, str] | None:
    """One GET with every redirect hop re-pinned; the response and final URL.

    None for every refusal there is — blocked host, redirect loop, non-200,
    transport error — because a vendor mark has exactly one fallback and no
    failure here is worth distinguishing from any other.
    """
    current = url
    try:
        for _hop in range(_MAX_REDIRECTS + 1):
            target = await pin_public_url(current)
            async with pinned_stream_client(target, max_bytes=max_bytes) as client:
                response = await client.get(current)
                if response.status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get("location")
                    if not location:
                        return None
                    try:
                        current = urljoin(current, location)
                    except ValueError:
                        # A Location the upstream wrote, so it can be
                        # unparseable (``http://[bad`` raises here). One more
                        # refusal, not a fault: ValueError is not an HTTPError
                        # and would otherwise leave the route 500ing where the
                        # whole contract is a miss and a monogram.
                        return None
                    continue
                if response.status_code != 200:
                    return None
                return response, current
        return None
    except (EgressBlockedError, OAuthHopBlocked, httpx2.HTTPError, OSError) as e:
        logger.debug("[brand_icons] %s: %s", url, e)
        return None


async def _fetch_icon(url: str) -> BrandIcon | None:
    """Bytes at ``url``, if they are an image big enough to be worth drawing."""
    got = await _get(url, max_bytes=MAX_ICON_BYTES)
    if got is None:
        return None
    data = got[0].content
    content_type = _sniff(data)
    if content_type is None:
        return None
    pixels = _pixels(data, content_type)
    if pixels is not None and pixels < MIN_PIXELS:
        return None
    return BrandIcon(content=data, content_type=content_type)


async def _from_site(host: str) -> BrandIcon | None:
    """The best mark ``host`` offers, reading its page before guessing paths."""
    candidates: list[str] = []
    if (page := await _get(f"https://{host}/", max_bytes=MAX_PAGE_BYTES)) is not None:
        # latin-1 never raises, and every byte a tag attribute can hold round
        # trips through it — the page's real encoding only matters for text we
        # are not reading.
        html = page[0].content.decode("latin-1", errors="ignore")
        candidates = _declared_icons(html, page[1])[:3]
    candidates.append(f"https://{host}/favicon.ico")
    for url in candidates:
        if (icon := await _fetch_icon(url)) is not None:
            return icon
    return None


async def icon_for_site(site: str) -> BrandIcon | None:
    """The mark ``site`` publishes, cached by host.

    Cached on the site rather than on whoever asked, so two vendors sharing a
    brand share both the answer and the one fetch that found it.
    """
    host = site.strip().lower()
    if not host:
        return None
    return await _cached(f"{_CACHE_PREFIX}{host}", lambda: _from_site(host))


_Resolver = Callable[[], Awaitable[BrandIcon | None]]

# Strong references to running refreshes: the loop holds tasks weakly, and a
# refresh nobody awaits could otherwise be collected before it lands.
_REFRESHES: set[asyncio.Task[None]] = set()


@dataclass(frozen=True)
class _Stored:
    icon: BrandIcon | None
    fresh: bool
    # When a resolver last returned ``icon``, which a refresh that finds
    # nothing keeps only for so long.
    found_at: float


def _read(raw: object) -> _Stored | None:
    """A stored answer, or None when there is none worth serving.

    An entry with no ``fresh_until`` predates it and reads as stale, so it is
    served once more while its refresh rewrites it. One with no ``found_at``
    counts as found now, so its clock starts with that rewrite.
    """
    if not isinstance(raw, dict):
        return None
    until = raw.get("fresh_until")
    fresh = isinstance(until, (int, float)) and time.time() < until
    found = raw.get("found_at")
    found_at = found if isinstance(found, (int, float)) else time.time()
    if not raw.get("content"):
        return _Stored(icon=None, fresh=fresh, found_at=found_at)
    try:
        icon = BrandIcon(
            content=base64.b64decode(raw["content"]),
            content_type=raw["content_type"],
        )
    except (ValueError, KeyError, TypeError):
        return None  # Unreadable: resolve again and overwrite it.
    return _Stored(icon=icon, fresh=fresh, found_at=found_at)


async def _resolve(
    key: str, resolver: _Resolver, kept: _Stored | None
) -> BrandIcon | None:
    """Ask the vendor and store the answer, keeping ``kept`` if it gives none.

    Any failure to resolve is a miss rather than an error, so it is stored and
    asked about again on the miss schedule, and the next source gets its turn.
    """
    try:
        async with asyncio.timeout(DEADLINE_SECONDS):
            icon = await resolver()
    except TimeoutError:
        icon = None
    except Exception:
        logger.warning("[brand_icons] resolving %.200s failed", key, exc_info=True)
        icon = None
    now = time.time()
    found = icon is not None
    found_at = now
    if not found and kept is not None and now - kept.found_at < _KEEP_UNFOUND:
        icon, found_at = kept.icon, kept.found_at
    value: dict[str, object] = {
        "content": None,
        "fresh_until": now + (_HIT_FRESH if found else _MISS_FRESH),
        "found_at": found_at,
    }
    if icon is not None:
        value["content"] = base64.b64encode(icon.content).decode("ascii")
        value["content_type"] = icon.content_type
    await get_cache_client().set(
        key, value, ttl=_KEEP if icon is not None else _MISS_KEEP
    )
    return icon


async def _refresh(key: str, resolver: _Resolver, lock: str, token: str) -> None:
    """Resolve again behind an answer already served, holding the refresh lock."""
    cache = get_cache_client()
    try:
        # Read again under the lock: another worker may have refreshed it since
        # this one's read. A read that fails confirms nothing, so it ends the
        # refresh rather than writing over what may be newer.
        current = _read(await cache.get(key))
        if current is not None and not current.fresh:
            await _resolve(key, resolver, current)
    except Exception:
        logger.warning("[brand_icons] refresh of %.200s failed", key, exc_info=True)
    finally:
        await cache.release_lock(lock, token)


async def _cached(key: str, resolver: _Resolver) -> BrandIcon | None:
    """The stored answer for ``key``, resolving it only when there is none."""
    cache = get_cache_client()
    stored = _read(await cache.get(key))
    if stored is None:
        return await _resolve(key, resolver, None)
    if not stored.fresh:
        # The lock is taken before the task rather than inside it, so viewers
        # arriving while a refresh runs start nothing at all.
        lock = f"{_REFRESH_LOCK_PREFIX}{key}"
        token = uuid.uuid4().hex
        if await cache.acquire_lock(lock, token, _REFRESH_LOCK_MS) is True:
            task = asyncio.create_task(_refresh(key, resolver, lock, token))
            _REFRESHES.add(task)
            task.add_done_callback(_REFRESHES.discard)
    return stored.icon


async def stop_refreshes() -> None:
    """Cancel the refreshes still running and wait for them.

    Called at shutdown before the cache closes, so each releases its lock
    while Redis is open; a lock left to expire holds off the refresh of that
    mark after a quick restart.
    """
    tasks = list(_REFRESHES)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _from_data_uri(source: str) -> BrandIcon | None:
    """The bytes a ``data:`` URI carries, if they are really an image.

    The declared media type is not trusted and not read: a server that inlines
    its mark is naming its own content type, and the sniff below is the same
    check every fetched icon passes. Nothing here leaves the host, which is the
    whole appeal of the shape.
    """
    head, _, payload = source.partition(",")
    if not payload or ";base64" not in head:
        # A percent-encoded data: URI is legal and vanishingly rare for binary
        # image data; refusing it costs a fallback, not a feature.
        return None
    if len(payload) > (MAX_ICON_BYTES * 4) // 3 + 4:
        return None
    try:
        data = base64.b64decode(payload, validate=True)
    except (ValueError, TypeError):
        return None
    content_type = _sniff(data)
    if content_type is None:
        return None
    pixels = _pixels(data, content_type)
    if pixels is not None and pixels < MIN_PIXELS:
        return None
    return BrandIcon(content=data, content_type=content_type)


async def _icon_at_url(url: str) -> BrandIcon | None:
    """One named file, cached on the URL that named it.

    Separate from ``icon_for_site`` because the work is different: there is no
    page to read and no candidate list, just the one address the author gave.
    The fetch is still pinned per hop and capped, since the address came from a
    server the user added rather than from us.
    """
    key = f"{_CACHE_PREFIX}url:{hashlib.sha256(url.encode()).hexdigest()}"
    return await _cached(key, lambda: _fetch_icon(url))


async def icon_for_source(source: str) -> BrandIcon | None:
    """The mark ``source`` names, whichever way it names one.

    Three spellings arrive here and each announces itself: a ``data:`` URI
    carries the bytes, an absolute URL names one file to fetch, and anything
    else is a bare host meaning "read this site and find its mark". A server
    describing itself in the handshake may use any of the three; a bundle or a
    brokerage only ever names a host.

    Reading the prefix beats carrying a kind field beside the value, because
    the value already had to be unambiguous to be resolvable at all.
    """
    source = source.strip()
    if not source:
        return None
    if source.startswith("data:"):
        return _from_data_uri(source)
    if source.startswith(("http://", "https://")):
        return await _icon_at_url(source)
    return await icon_for_site(source)


# A handle stays resolvable well past any page that minted it, and every list
# call re-stamps the ones it hands out, so the only handles that expire are for
# servers nobody has looked at in a month. Those come back on the next look.
_SOURCE_PREFIX = "brand-icon-src:v1:"
_SOURCE_TTL = 30 * 24 * 3600

# The longest source that could ever resolve: a base64 `data:` URI carrying a
# mark of the maximum size, plus room for its header. A server declares this
# string itself, so without a bound a handshake could park megabytes in shared
# storage for a month that no reader would ever accept, because the payload
# check that rejects it runs only when someone asks for the image.
MAX_SOURCE_CHARS = (MAX_ICON_BYTES * 4) // 3 + 256


async def publish_icon_source(source: str) -> str | None:
    """Register ``source`` as resolvable and return the handle that names it.

    The indirection is what keeps the icon route from being an open image
    proxy. A route taking a URL outright would fetch any address any caller
    named; this one resolves only sources an authenticated user's own server
    actually declared, because minting a handle is the only way one gets in.

    Content-addressed, so the handle is stable across calls (a browser can
    cache the image) and two users whose servers declare the same mark share
    one entry and one fetch. ``None`` for a source too long to ever resolve,
    which the caller reads as "this server has no mark".
    """
    if len(source) > MAX_SOURCE_CHARS:
        return None
    try:
        encoded = source.encode("utf-8")
    except UnicodeEncodeError:
        # A handshake field, so it is whatever the server put in its JSON, and
        # ``"\ud800"`` decodes to a lone surrogate that no UTF-8 encoder will
        # take. Length says nothing about it, and the raise is not an
        # HTTPError, so it would leave the whole builtin listing 500ing over
        # one decorative string. Unencodable is unresolvable: same answer as
        # too long.
        return None
    digest = hashlib.sha256(encoded).hexdigest()[:32]
    await get_cache_client().set(
        f"{_SOURCE_PREFIX}{digest}", {"src": source}, ttl=_SOURCE_TTL
    )
    return digest


async def icon_response_for_handle(handle: str) -> Response:
    """The mark behind a published handle, or the ordinary cacheable 404.

    An unknown handle and a source with no usable mark answer identically on
    purpose: both mean "draw your own stand-in", and distinguishing them would
    only tell a caller which guesses were closer.
    """
    entry = await get_cache_client().get(f"{_SOURCE_PREFIX}{handle}")
    source = entry.get("src") if isinstance(entry, dict) else None
    return await icon_response(source if isinstance(source, str) else None)


async def icon_response(source: str | None) -> Response:
    """``source``'s mark as an HTTP response, or a 404 that is safe to cache.

    Every brand-art route answers identically and differs only in how it finds
    the name, so the answer lives here rather than once per router. 404 is an
    ordinary outcome, not a fault: a vendor may publish no usable mark, and the
    caller draws its own stand-in.
    """
    icon = await icon_for_source(source) if source else None
    if icon is None:
        return Response(
            status_code=404, headers={"Cache-Control": _MISS_CACHE, **_ART_SAFETY}
        )
    return Response(
        content=icon.content,
        media_type=icon.content_type,
        headers={"Cache-Control": _HIT_CACHE, **_ART_SAFETY},
    )
