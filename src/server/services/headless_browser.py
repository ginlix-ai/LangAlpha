"""The headless Chromium the server renders agent-authored pages in.

One warm browser per worker process, a context per render. Playwright is
imported lazily so this module imports without the ``pdf`` extra; a missing
Playwright or Chromium raises ``BrowserUnavailable``.

SSRF containment: every request a render makes goes through a
``RequestGate``, which lets through the workspace's own serve prefix (when the
render has one) and any https host at a public address, and follows a
redirect only hop by hop through the same gate (``_fetch_gated``). The page
therefore loads what it loads in the viewer, and cannot reach services or
cloud metadata endpoints on the server's own network. A stylesheet or framed
document that redirected reaches the page as an import of, or a refresh to,
where it landed, so its relative URLs resolve there and that request meets
the gate too (``_stand_in``). WebSockets, which the HTTP gate never sees, are
closed, and WebRTC, which no route sees either, is taken out of every frame
and kept off UDP (``gated_context``, ``_LAUNCH_ARGS``).

The browser runs a page an agent or a widget wrote, so it is held to less than
the worker it shares a machine with: a capped JavaScript heap, first in line
for Linux's OOM killer (``_oom_first``), and closed once no render has used it
for a while (``use_browser``).
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import shlex
import sys
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx

from src.server.utils.egress_guard import EgressBlockedError, resolve_public_ips
from src.tools.web.inhouse.guard import GuardedAsyncTransport

logger = logging.getLogger(__name__)

__all__ = [
    "BrowserError",
    "BrowserUnavailable",
    "Budget",
    "RequestGate",
    "TooLarge",
    "close_browser",
    "gated_context",
    "get_browser",
    "settle_later",
    "use_browser",
    "within",
]

T = TypeVar("T")


class BrowserError(Exception):
    """The render failed for a reason other than a timeout or a missing browser."""


class BrowserUnavailable(BrowserError):
    """Playwright or its Chromium binary is not installed."""


# --- The gate ----------------------------------------------------------------


def _asks_for_pdf(url: str) -> bool:
    """True for a URL asking a serve route for ``format=pdf``: refused on every
    path a render's request can take, its own answers (``gated_context``) and
    each redirect hop (``_is_request_allowed``) alike, so a document cannot
    have the renderer render again and use up its slots."""
    # Parsed, not substring-matched, so percent-encoded forms like
    # ``format=%70df`` that a serve endpoint decodes back to ``pdf`` count too.
    query = urlparse(url).query or ""
    return any(
        key.lower() == "format" and value.lower() == "pdf"
        for key, value in parse_qsl(query, keep_blank_values=True)
    )


async def _is_request_allowed(url: str, workspace_serve_prefix: str | None) -> bool:
    """True if ``url`` may be fetched during a render.

    Allowed when the URL starts with this workspace's serve prefix, or is https
    to a public address. This browser sits on the server's network, where the
    viewer's does not, so a report may load what a reader's browser can and
    nothing only the server can. Https is what holds against a name repointed at
    a private address after the check: the service there has no certificate for
    that name. Any URL asking for ``format=pdf`` is refused, on any host, so a
    document cannot have the renderer render again through the deployment's
    public address and use up its slots.
    """
    if _asks_for_pdf(url):
        return False
    if workspace_serve_prefix and url.startswith(workspace_serve_prefix):
        return True
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    try:
        await resolve_public_ips(parsed.hostname)
    except EgressBlockedError:
        return False
    return True


# Hops a gated fetch follows before it gives up, as a browser caps a chain.
_MAX_REDIRECT_HOPS = 5

# What a render takes from the internet, per answer and in all. Each answer is
# held here until the browser has it, and a report may name any public
# address, which may answer without end.
_MAX_FETCH_BYTES = 16 * 1024 * 1024
_MAX_RENDER_FETCH_BYTES = 64 * 1024 * 1024
# One fetch, its redirects included, so a server sending a byte at a time
# cannot hold a render slot past the render's own budget.
_FETCH_TIMEOUT_S = 20.0

# Not passed on in either direction: they describe the connection or the
# encoding, and the body crosses whole and as sent.
_HOP_HEADERS = frozenset(
    {
        "accept-encoding",
        "connection",
        "content-encoding",
        "content-length",
        "host",
        "keep-alive",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


@dataclass(frozen=True)
class _Fetched:
    status: int
    headers: dict[str, str]
    body: bytes
    #: Where the answer came from, past any redirects.
    url: str


#: Dropped once a redirect leaves the origin they were sent to, as a browser
#: drops them: a token meant for one site never goes on to another.
_CREDENTIAL_HEADERS = frozenset({"authorization", "cookie", "proxy-authorization"})
_REDIRECTS = frozenset({301, 302, 303, 307, 308})
#: What describes a body, dropped with the body when a redirect turns the
#: request into a GET.
_BODY_HEADERS = frozenset({"content-language", "content-location", "content-type"})


def _redirect_drops_body(status: int, method: str) -> bool:
    """Fetch's rule: a 301 or 302 turns a POST into a GET and a 303 turns
    anything but GET or HEAD into one; every other method and status repeats
    the request as it was."""
    return (status in (301, 302) and method == "POST") or (
        status == 303 and method not in ("GET", "HEAD")
    )


def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = httpx.URL(url)
    return parsed.scheme, parsed.host, parsed.port or {"http": 80, "https": 443}.get(parsed.scheme)


def _asked(url: str) -> tuple[str, str, int | None, bytes]:
    """What a request for ``url`` puts on the wire, however the URL is
    spelled: Chromium and httpx write the same one differently."""
    return (*_origin(url), httpx.URL(url).raw_path)


class TooLarge(Exception):
    """An answer past its own cap or the render's budget."""


class Budget:
    """What one render may still hold of what it fetched, over all its
    fetches, and the answers it holds for a file the browser will ask for
    again (``_stand_in``), already counted against it.

    ``too_large`` is every URL the render aborted for size alone, as the
    browser asked for it: a cap of the render's, not a failure the reader
    would meet, so a report can tell the two apart."""

    def __init__(self, total: int = _MAX_RENDER_FETCH_BYTES) -> None:
        self.left = total
        self.held: dict[tuple[str, str, int | None, bytes], _Fetched] = {}
        self.too_large: list[str] = []

    def spend(self, size: int) -> bool:
        """Count ``size`` more bytes; False once the render is past its budget."""
        self.left -= size
        return self.left >= 0

    def cut_off(self, url: str) -> None:
        url = url.split("#", 1)[0]
        if url not in self.too_large:
            self.too_large.append(url)


def _fetch_transport() -> httpx.AsyncBaseTransport:
    # Dials only the addresses it checked, so a name repointed after the gate
    # reaches nothing private.
    return GuardedAsyncTransport()


async def _fetch_gated(
    request, url: str, workspace_serve_prefix: str | None, budget: Budget
) -> _Fetched | None:
    """The answer for ``url``, each redirect hop gated as the first request
    was, or None when a hop is refused or the chain runs too long. Raises
    ``TooLarge`` for an answer past its cap.

    Fetched here rather than by Chromium, which follows a redirect without
    asking the route handler again, so a public address answering 302 to a
    private one would be fetched ungated; and rather than by Playwright's
    ``route.fetch``, which holds the whole body before anything could cap it.
    """
    method, content = request.method, request.post_data_buffer
    headers = {
        name: value
        for name, value in request.headers.items()
        if name.lower() not in _HOP_HEADERS
    }
    headers["accept-encoding"] = "identity"
    async with (
        asyncio.timeout(_FETCH_TIMEOUT_S),
        httpx.AsyncClient(
            transport=_fetch_transport(), timeout=_FETCH_TIMEOUT_S
        ) as client,
    ):
        for _ in range(_MAX_REDIRECT_HOPS + 1):
            asked = client.build_request(method, url, headers=headers, content=content)
            response = await client.send(asked, stream=True)
            try:
                location = response.headers.get("location")
                # Fetch follows only these; a 300 or 304 reaches the page as is.
                if response.status_code not in _REDIRECTS or not location:
                    return await _read_capped(response, budget)
            finally:
                await response.aclose()
            hop = urljoin(url, location)
            if not await _is_request_allowed(hop, workspace_serve_prefix):
                return None
            if _origin(hop) != _origin(url):
                headers = {
                    name: value
                    for name, value in headers.items()
                    if name.lower() not in _CREDENTIAL_HEADERS
                }
            url = hop
            if _redirect_drops_body(response.status_code, method.upper()):
                method, content = "GET", None
                headers = {
                    name: value
                    for name, value in headers.items()
                    if name.lower() not in _BODY_HEADERS
                }
    return None


async def _read_capped(response: httpx.Response, budget: Budget) -> _Fetched | None:
    """The answer whole, or ``TooLarge`` once it passes its cap or the
    render's budget. Asked uncompressed and refused compressed anyway, as
    ``fetch_capped`` is, since a decoder expands a chunk with no bound before a
    cap could see it; and streamed, since a Content-Length is only a claim."""
    encoding = response.headers.get("content-encoding", "identity").strip().lower()
    if encoding not in ("", "identity"):
        return None
    declared = response.headers.get("content-length", "")
    # A HEAD, 204 or 304 has no body, and its length describes the resource.
    if response.request.method == "HEAD" or response.status_code in (204, 304):
        declared = ""
    if declared.isdigit() and int(declared) > min(_MAX_FETCH_BYTES, budget.left):
        raise TooLarge
    body = bytearray()
    async for chunk in response.aiter_raw():
        body.extend(chunk)
        if not budget.spend(len(chunk)) or len(body) > _MAX_FETCH_BYTES:
            raise TooLarge
    headers: dict[str, str] = {}
    for name, value in response.headers.multi_items():
        name = name.lower()
        if name in _HOP_HEADERS:
            continue
        # Cookies cannot be folded on commas, an Expires holds one: the browser
        # end splits a Set-Cookie on line breaks, one cookie each.
        joined = "\n" if name == "set-cookie" else ", "
        headers[name] = f"{headers[name]}{joined}{value}" if name in headers else value
    return _Fetched(response.status_code, headers, bytes(body), str(response.url))


_CSS_STRING_ESCAPES = re.compile(r'[\x00-\x1f\x7f"\\]')


def _css_string(value: str) -> str:
    """``value`` quoted as CSSOM serializes a string, so no URL ends it early."""

    def escape(match: re.Match[str]) -> str:
        char = match.group()
        return "\\" + char if char in '"\\' else f"\\{ord(char):x} "

    return '"' + _CSS_STRING_ESCAPES.sub(escape, value) + '"'


def _stand_in(request, url: str, fetched: _Fetched) -> tuple[str, bytes] | None:
    """What reaches the page in place of a redirected answer's bytes, so the
    file resolves its own relative URLs from where it landed, or None for the
    bytes as they are.

    Fulfilled under the URL the page asked for, they would resolve against
    that URL. A stylesheet becomes an ``@import`` of where it landed, a framed
    document a refresh to it: Chromium asks the route again for either, where
    for a fulfilled redirect's target it would not, so the gate holds. A sheet
    only with no Origin: one in cors mode checks the answer's CORS headers,
    and SRI its bytes, which a stand-in passes neither of. A script has none:
    a classic one cannot import, and at the route a module looks like a
    classic script with crossorigin.
    """
    if _asked(fetched.url) == _asked(url):
        return None
    if request.resource_type == "stylesheet" and all(
        name.lower() != "origin" for name in request.headers
    ):
        return "text/css; charset=utf-8", f"@import url({_css_string(fetched.url)});".encode()
    if request.resource_type == "document" and request.method == "GET":
        target = html.escape(fetched.url.replace("'", "%27"), quote=True)
        refresh = f"<!doctype html><meta http-equiv=\"refresh\" content=\"0; url='{target}'\">"
        return "text/html; charset=utf-8", refresh.encode()
    return None


class RequestGate:
    """Lets a render's requests through the gate or aborts them.

    ``workspace_prefix`` is the serve route the rendered document reads its
    workspace's files under, which the browser fetches itself, since those
    routes answer without redirecting; None for a render with no such route.
    """

    def __init__(self, workspace_prefix: str | None, budget: Budget | None = None) -> None:
        self.workspace_prefix = workspace_prefix
        self.budget = budget if budget is not None else Budget()

    async def answer(self, route, request) -> None:
        """What a ``_stand_in`` sends the browser on to meets the gate like
        any request, then takes the answer already fetched, once."""
        url = request.url
        if not await _is_request_allowed(url, self.workspace_prefix):
            await route.abort()
            return
        if self.workspace_prefix and url.startswith(self.workspace_prefix):
            await route.continue_()
            return
        budget = self.budget
        try:
            # Parked for the GET a stand-in sends on; any other method fetches
            # its own answer and leaves it there.
            held = budget.held.pop(_asked(url), None) if request.method == "GET" else None
            fetched = held or await _fetch_gated(request, url, self.workspace_prefix, budget)
        except TooLarge:
            # Noted before the abort, which the page's failure report follows.
            budget.cut_off(url)
            fetched = None
        except Exception:
            logger.debug("render fetch failed for %s", url, exc_info=True)
            fetched = None
        if fetched is None:
            await route.abort()
        elif stand_in := _stand_in(request, url, fetched):
            budget.held[_asked(fetched.url)] = fetched
            content_type, body = stand_in
            await route.fulfill(status=200, headers={"content-type": content_type}, body=body)
        else:
            await route.fulfill(
                status=fetched.status, headers=fetched.headers, body=fetched.body
            )


#: Answers a request itself and returns True, or returns False to leave it to the gate.
OwnHandler = Callable[[Any, Any], Awaitable[bool]]

_CONTEXT_CLOSE_TIMEOUT_S = 5.0

# WebRTC's ICE reaches STUN and TURN servers past every route and socket
# hook, loopback and the server's network included. An init script runs in
# every frame and window of the context, a sandboxed srcdoc frame and a fresh
# about:blank one included, before any of the page's own script.
_NO_WEBRTC_JS = """
for (const name of ["RTCPeerConnection", "webkitRTCPeerConnection", "RTCDataChannel"]) {
  try { delete globalThis[name]; } catch (e) {}
}
"""


async def _close_socket(ws_route) -> None:
    # A rendered page has no use for a live feed, so every WebSocket is closed,
    # which seals the one channel the HTTP gate misses (ws://169.254.169.254,
    # ws://127.0.0.1). There is deliberately no connect path.
    await ws_route.close()


@asynccontextmanager
async def gated_context(
    browser, gate: RequestGate, *, own: OwnHandler | None = None, **context_options: Any
) -> AsyncIterator[Any]:
    """A browser context every request of which passes ``gate``, after
    ``own`` where given, closed on the way out.

    Routed on the context, not a page: a window the document opens is a page
    of its own, which a page route never sees, so it would load whatever it
    names from the server's network.
    """

    async def handle(route, request) -> None:
        try:
            # Before ``own`` too: what the render answers itself may forward
            # to a serve route, and the refusal holds on every path.
            if _asks_for_pdf(request.url):
                await route.abort()
                return
            if own is None or not await own(route, request):
                await gate.answer(route, request)
        except Exception:
            # Most often the context closed under a request still in flight.
            # Answered all the same, so it never stays pending in the page.
            logger.debug("render request not answered", exc_info=True)
            try:
                await route.abort()
            except Exception:
                pass

    # A service worker's own requests pass no route, so none may register.
    context = await browser.new_context(**{**context_options, "service_workers": "block"})
    try:
        await context.add_init_script(_NO_WEBRTC_JS)
        await context.route("**/*", handle)
        await context.route_web_socket("**/*", _close_socket)
        yield context
    finally:
        try:
            await within(context.close(), _CONTEXT_CLOSE_TIMEOUT_S)
        except Exception:
            logger.debug("render context not closed", exc_info=True)


def _settle(task: asyncio.Future) -> None:
    if not task.cancelled():
        task.exception()


async def within(awaitable: Awaitable[T], timeout_s: float) -> T:
    """``awaitable``'s result, or TimeoutError past ``timeout_s``. Left to
    finish rather than cancelled on a timeout: a cancelled Playwright call
    leaves its protocol future to fail unread when the context closes, which
    asyncio logs as an error; the task reads it instead."""
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=max(timeout_s, 0.1))
    except asyncio.CancelledError:
        task.cancel()
        raise
    if not done:
        task.add_done_callback(_settle)
        raise TimeoutError
    return task.result()


def settle_later(task: asyncio.Future) -> None:
    """Read ``task``'s outcome once it ends, so a failure nobody awaits is
    not logged as never retrieved."""
    task.add_done_callback(_settle)


# --- The browser ----------------------------------------------------------------

_EXECUTABLE_MISSING_HINT = "executable doesn't exist"

#: A widget's heap past this is its renderer's crash, not the machine's memory.
_JS_HEAP_MB = 512
_LAUNCH_ARGS = (
    f"--js-flags=--max-old-space-size={_JS_HEAP_MB}",
    # Belt to the init script's braces: no UDP leaves outside a proxy, and
    # the render browser has none.
    "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
)
#: Closed after this long with no render holding it: a warm browser is a few
#: hundred MB a worker keeps for renders that may not come.
_IDLE_CLOSE_S = 300.0

_browser = None
_playwright = None
_browser_lock = asyncio.Lock()
# Process-local execution context: how many renders on this worker hold the
# browser, since when none has, and the task that closes it once idle.
_users = 0
_idle_since = 0.0
_idle_watch: asyncio.Task | None = None
#: The OOM wrapper, made once per process; False once it could not be.
_oom_wrapper: str | bool | None = None


async def _reset_browser() -> None:
    """Drop the cached browser + playwright driver, swallowing teardown errors.

    Called while holding ``_browser_lock`` after a dead handle is detected so
    the next launch starts clean. The driver stops even when the close is
    cancelled (``close_browser`` cancels the idle watch, which may be here):
    both handles are already cleared, so nothing would stop it later.
    """
    global _browser, _playwright
    browser, playwright = _browser, _playwright
    _browser = None
    _playwright = None
    try:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
    finally:
        if playwright is not None:
            await _stop_driver(playwright)


async def _stop_driver(playwright) -> None:
    # ``stop`` is the started Playwright's; its context manager has none.
    try:
        await playwright.stop()
    except Exception:
        logger.debug("playwright driver not stopped", exc_info=True)


def _browser_alive(browser) -> bool:
    """True if a cached browser handle is still connected to live Chromium."""
    try:
        return browser.is_connected()
    except Exception:
        return False


async def close_browser() -> None:
    """Close the cached headless Chromium and stop its Playwright driver.

    Called from the application lifespan on shutdown so an idle render browser
    doesn't outlive the server. No-op when nothing was ever launched.
    """
    global _idle_watch
    watch, _idle_watch = _idle_watch, None
    if watch is not None and watch is not asyncio.current_task():
        watch.cancel()
    async with _browser_lock:
        await _reset_browser()


async def _close_when_idle() -> None:
    """Close the browser once no render has held it for ``_IDLE_CLOSE_S``.

    Checked under the launch lock with the count of holders, which a render
    raises before it asks for the browser, so a render that has it, or is
    about to, never has it closed under it.
    """
    while True:
        idle_for = time.monotonic() - _idle_since
        await asyncio.sleep(_IDLE_CLOSE_S if _users else max(_IDLE_CLOSE_S - idle_for, 0.0))
        async with _browser_lock:
            if _browser is None:
                return
            if _users == 0 and time.monotonic() - _idle_since >= _IDLE_CLOSE_S:
                logger.info("closing the idle render browser")
                await _reset_browser()
                return


def _watch_idle() -> None:
    global _idle_watch
    if _idle_watch is None or _idle_watch.done():
        _idle_watch = asyncio.ensure_future(_close_when_idle())
        settle_later(_idle_watch)


@asynccontextmanager
async def use_browser(launch_timeout_s: float | None = None) -> AsyncIterator[Any]:
    """The warm browser, held for the block so the idle close leaves it be.

    Past ``launch_timeout_s`` it raises ``BrowserError`` and leaves the launch
    to finish, so the next render finds it warm. Raises as ``get_browser`` does.
    """
    global _users, _idle_since
    _users += 1
    try:
        if launch_timeout_s is None:
            browser = await get_browser()
        else:
            try:
                browser = await within(get_browser(), launch_timeout_s)
            except TimeoutError:
                raise BrowserError("Chromium did not start in time") from None
        yield browser
    finally:
        _users -= 1
        if _users == 0:
            _idle_since = time.monotonic()


async def _headless_shell() -> str | None:
    """The binary Playwright launches for ``headless=True``, or None when it
    cannot be told.

    Playwright's Python API names only the full Chromium; the headless shell
    the image installs (``--only-shell``) is where its own installer says,
    asked of the driver this Playwright ships, so its browser cache, revision
    and platform rules are its own.
    """
    from playwright._impl._driver import compute_driver_executable, get_driver_env

    node, cli = compute_driver_executable()
    process = await asyncio.create_subprocess_exec(
        node,
        cli,
        "install",
        "--dry-run",
        "--only-shell",
        "chromium",
        env=get_driver_env(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        out, _ = await asyncio.wait_for(process.communicate(), 15)
    except BaseException as exc:
        # Past its time or cancelled with the launch, the probe would
        # otherwise outlive it.
        if process.returncode is None:
            process.kill()
            await asyncio.shield(process.wait())
        if isinstance(exc, TimeoutError):
            return None
        raise
    lines = out.decode(errors="replace").splitlines()
    for i, line in enumerate(lines):
        if "chromium-headless-shell" not in line:
            continue
        for following in lines[i + 1 :]:
            name, _, value = following.strip().partition(":")
            if name == "Install location":
                folder = Path(value.strip())
                for binary in ("chrome-headless-shell", "headless_shell"):
                    for found in sorted(folder.glob(f"*/{binary}")):
                        if os.access(found, os.X_OK):
                            return str(found)
                return None
    return None


async def _oom_first() -> str | None:
    """A wrapper to launch Chromium through that makes it, and every process
    it starts, the first the Linux OOM killer takes, or None to launch as is.

    A widget that runs away with memory should cost its own render, not a
    worker's live turns. The score is set before the exec, so every process
    the browser starts inherits it, which a score set on the browser after
    launch would miss for those started later. Linux only, as the sandbox
    browser does (``browser_runtime/wrapper.py``).
    """
    global _oom_wrapper
    if _oom_wrapper is False:
        return None
    if isinstance(_oom_wrapper, str) and os.path.exists(_oom_wrapper):
        return _oom_wrapper
    if not sys.platform.startswith("linux") or not os.path.exists("/proc/self/oom_score_adj"):
        _oom_wrapper = False
        return None
    try:
        binary = await _headless_shell()
        if binary is None:
            raise FileNotFoundError("headless shell not found")
        # A folder of this process's own, so no other user can plant the file.
        wrapper = Path(tempfile.mkdtemp(prefix="langalpha-render-")) / "chromium"
        wrapper.write_text(
            "#!/bin/sh\n"
            "echo 1000 > /proc/self/oom_score_adj 2>/dev/null\n"
            f"exec {shlex.quote(binary)} \"$@\"\n"
        )
        wrapper.chmod(0o700)
    except Exception:
        logger.warning("render browser launches without its OOM score", exc_info=True)
        _oom_wrapper = False
        return None
    _oom_wrapper = str(wrapper)
    return _oom_wrapper


async def get_browser():
    """Return the singleton headless Chromium, launching it once.

    A cached browser that has crashed or disconnected is discarded and
    relaunched so a dead handle can't wedge every render until restart.
    Raises ``BrowserUnavailable`` if Playwright or the Chromium binary is
    missing: both are optional, needed only to render. A render holds it
    through ``use_browser``, which keeps the idle close away.
    """
    global _browser, _playwright, _idle_since
    if _browser is not None and _browser_alive(_browser):
        return _browser
    async with _browser_lock:
        if _browser is not None and _browser_alive(_browser):
            return _browser
        # Tear down a dead handle (and its playwright driver) before relaunch.
        if _browser is not None:
            await _reset_browser()
        try:
            from playwright.async_api import Error as PlaywrightError
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise BrowserUnavailable("playwright is not installed") from exc
        pw = None
        try:
            pw = await async_playwright().start()
            options: dict[str, Any] = {"headless": True, "args": list(_LAUNCH_ARGS)}
            if wrapper := await _oom_first():
                options["executable_path"] = wrapper
                # The zygote sets each renderer it forks to 300 and the GPU
                # process to 200, which needs no privilege; launched without
                # it, every process keeps the 1000 it inherits. Playwright
                # already runs Chromium without its sandbox, which this needs.
                options["args"].append("--no-zygote")
            browser = await pw.chromium.launch(**options)
        except BaseException as exc:
            # Any failure, cancellation too, would leave the driver running
            # with nothing holding it.
            if pw is not None:
                await _stop_driver(pw)
            if not isinstance(exc, PlaywrightError):
                raise
            if _EXECUTABLE_MISSING_HINT in str(exc).lower():
                raise BrowserUnavailable(
                    "Chromium is not installed (run: playwright install chromium)"
                ) from exc
            raise BrowserError(f"Failed to launch Chromium: {exc}") from exc
        _playwright = pw
        _browser = browser
        _idle_since = time.monotonic()
        _watch_idle()
        return _browser
