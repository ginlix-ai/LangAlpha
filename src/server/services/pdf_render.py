"""Server-side HTML→PDF rendering via headless Chromium (Playwright).

Renders a workspace HTML document — loaded from the server's own loopback
``/api/v1/wsfiles/...`` route — into PDF bytes. Playwright is imported lazily
inside functions so this module never fails to import without the ``pdf`` extra;
callers map ``PdfRenderUnavailable`` to a 501.

SSRF containment: every subresource request is gated by ``_is_request_allowed``,
which permits only the workspace's own serve prefix plus a fixed https CDN
allowlist. Agent-authored HTML therefore cannot reach internal services or
cloud metadata endpoints from inside the server.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from urllib.parse import parse_qsl, urlparse

logger = logging.getLogger(__name__)


# --- Error taxonomy --------------------------------------------------------


class PdfRenderError(Exception):
    """Rendering failed for a reason other than timeout/unavailability."""


class PdfRenderUnavailable(PdfRenderError):
    """Playwright or its Chromium binary is not installed."""


class PdfRenderTimeout(PdfRenderError):
    """Rendering exceeded the allotted time budget."""


# --- SSRF allowlist --------------------------------------------------------

# https-only CDN hosts agent HTML may pull libraries/fonts from. Everything
# else (internal hosts, metadata IPs, lookalike domains) is aborted.
_CDN_ALLOWLIST: frozenset[str] = frozenset(
    {
        "cdnjs.cloudflare.com",
        "cdn.jsdelivr.net",
        "unpkg.com",
        "esm.sh",
        "fonts.googleapis.com",
        "fonts.gstatic.com",
    }
)


def _is_request_allowed(url: str, workspace_serve_prefix: str) -> bool:
    """True if ``url`` may be fetched during a render.

    Allowed when the URL starts with this workspace's serve prefix, or when its
    host is an exact match in the https CDN allowlist. Exact host equality (not
    suffix) blocks lookalikes like ``unpkg.com.evil.io``. A workspace-prefix URL
    carrying ``format=pdf`` is rejected so a rendered document cannot embed a
    subresource that recursively re-invokes the renderer — the query is parsed
    (not substring-matched) so percent-encoded forms like ``format=%70df`` that
    the serve endpoint decodes back to ``pdf`` are caught too.
    """
    if url.startswith(workspace_serve_prefix):
        query = urlparse(url).query or ""
        for key, value in parse_qsl(query, keep_blank_values=True):
            if key.lower() == "format" and value.lower() == "pdf":
                return False
        return True
    parsed = urlparse(url)
    if parsed.scheme != "https":
        return False
    return parsed.hostname in _CDN_ALLOWLIST


# --- Browser lifecycle -----------------------------------------------------

# Cap concurrent renders: each holds a Chromium context + page.
_RENDER_SEMAPHORE = asyncio.Semaphore(2)

# Total per-render budget (navigation + pdf). Networkidle wait is bounded
# below this so a slow asset can fall back to "load" and still finish.
_RENDER_TIMEOUT_MS = 30_000
_GOTO_TIMEOUT_MS = 20_000
# The fallback goto only runs after the networkidle goto already burned most of
# the outer _RENDER_TIMEOUT_MS budget, so it gets a shorter cap that fits the
# remaining window rather than a full _GOTO_TIMEOUT_MS the outer wait_for would
# pre-empt anyway.
_GOTO_FALLBACK_TIMEOUT_MS = 10_000

# Letter paper at CSS 96dpi (8.5x11in). The viewport must match the print
# width so responsive layouts and chart canvases render at paper size from
# the start — otherwise Chart.js rasterizes at screen width and printToPDF
# snapshots stale, oversized bitmaps over the reflowed layout.
_PRINT_VIEWPORT = {"width": 816, "height": 1056}

# Returns the first `size` declaration found in a CSS @page rule (including
# ones nested under @media), or ''. Used to match the viewport to documents
# designed for non-default paper (landscape, A4, ...) — page.pdf with
# prefer_css_page_size already honors the declaration for the paper itself,
# but layout/charts would otherwise still happen at portrait Letter width.
_PAGE_SIZE_PROBE_JS = """
() => {
  const probe = (rules) => {
    for (const rule of rules) {
      if (rule.type === CSSRule.PAGE_RULE) {
        const s = rule.style.getPropertyValue('size');
        if (s) return s;
      }
      if (rule.cssRules && rule.cssRules.length) {
        const s = probe(rule.cssRules);
        if (s) return s;
      }
    }
    return '';
  };
  for (const sheet of document.styleSheets) {
    try {
      const s = probe(sheet.cssRules);
      if (s) return s;
    } catch (e) {}
  }
  return '';
}
"""

# Named CSS @page sizes in portrait CSS px at 96dpi (ledger is defined as
# landscape tabloid, matching Chromium).
_PAGE_SIZE_NAMES: dict[str, tuple[int, int]] = {
    "a3": (1123, 1587),
    "a4": (794, 1123),
    "a5": (559, 794),
    "b4": (944, 1334),
    "b5": (665, 944),
    "letter": (816, 1056),
    "legal": (816, 1344),
    "tabloid": (1056, 1632),
    "ledger": (1632, 1056),
}

_LENGTH_UNITS_PX: dict[str, float] = {
    "px": 1.0,
    "in": 96.0,
    "cm": 96.0 / 2.54,
    "mm": 96.0 / 25.4,
    "q": 96.0 / 101.6,
    "pt": 96.0 / 72.0,
    "pc": 16.0,
}

_LENGTH_RE = re.compile(r"(\d+(?:\.\d+)?)([a-z]+)")


def _viewport_from_page_size(size_decl: str) -> dict | None:
    """Translate a CSS ``@page size`` declaration into a viewport dict.

    Accepts named sizes, orientation keywords, and 1–2 explicit lengths per
    the CSS spec. Returns None for ``auto``, unparseable values, or absurd
    dimensions — callers then keep the default portrait-Letter viewport.
    """
    tokens = size_decl.strip().lower().split()
    if not tokens or "auto" in tokens:
        return None
    orientation: str | None = None
    name: str | None = None
    lengths: list[float] = []
    for tok in tokens:
        if tok in ("portrait", "landscape"):
            orientation = tok
        elif tok in _PAGE_SIZE_NAMES:
            name = tok
        else:
            m = _LENGTH_RE.fullmatch(tok)
            if not m or m.group(2) not in _LENGTH_UNITS_PX:
                return None
            lengths.append(float(m.group(1)) * _LENGTH_UNITS_PX[m.group(2)])
    if lengths:
        width = lengths[0]
        height = lengths[1] if len(lengths) > 1 else lengths[0]
        # The tokenizer accepts an orientation keyword alongside explicit
        # lengths (a lenient superset of the CSS grammar); honor it rather than
        # silently dropping it, so "8.5in 11in landscape" lands in landscape.
        if orientation == "landscape" and width < height:
            width, height = height, width
        elif orientation == "portrait" and width > height:
            width, height = height, width
    elif name:
        width, height = _PAGE_SIZE_NAMES[name]
        if orientation == "landscape" and width < height:
            width, height = height, width
    elif orientation:
        width, height = _PRINT_VIEWPORT["width"], _PRINT_VIEWPORT["height"]
        if orientation == "landscape":
            width, height = height, width
    else:
        return None
    width, height = round(width), round(height)
    if not (200 <= width <= 5000 and 200 <= height <= 5000):
        return None
    return {"width": width, "height": height}

# Settle script run after load, before snapshotting. A PDF freezes a single
# frame, so unlike a live page (where ResizeObserver redraws are invisible
# transients) any mid-reflow chart state becomes permanent. Wait for web fonts
# (the main late-reflow source), force chart libraries to resize to their
# current containers, and loop until the layout fingerprint is stable across
# frames — bounded, no timing guesses.
_SETTLE_JS = """
async () => {
  const raf = () => new Promise((r) => requestAnimationFrame(r));
  if (document.fonts && document.fonts.ready) {
    await Promise.race([document.fonts.ready, new Promise((r) => setTimeout(r, 3000))]);
  }
  const resizeCharts = () => {
    window.dispatchEvent(new Event('resize'));
    const C = window.Chart;
    if (C && typeof C.getChart === 'function') {
      document.querySelectorAll('canvas').forEach((c) => {
        try { const i = C.getChart(c); if (i) i.resize(); } catch (e) {}
      });
    }
    const E = window.echarts;
    if (E && typeof E.getInstanceByDom === 'function') {
      document.querySelectorAll('[_echarts_instance_]').forEach((el) => {
        try { const i = E.getInstanceByDom(el); if (i) i.resize(); } catch (e) {}
      });
    }
  };
  const fingerprint = () => {
    let s = document.documentElement.scrollHeight + ':';
    document.querySelectorAll('canvas').forEach((c) => {
      s += c.clientWidth + 'x' + c.clientHeight + ',';
    });
    return s;
  };
  let prev = '';
  for (let i = 0; i < 20; i++) {
    resizeCharts();
    await raf(); await raf();
    const cur = fingerprint();
    if (cur === prev) break;
    prev = cur;
  }
}
"""

# Backstop: even if a library ignores the forced resize, a canvas may never
# paint wider than its container in the snapshot.
_CANVAS_CLAMP_CSS = "canvas { max-width: 100% !important; }"

# Bounds for the caller-supplied render scale (Chromium itself accepts 0.1–2;
# below 0.5 output is unreadably small, so we don't offer it).
PDF_SCALE_MIN = 0.5
PDF_SCALE_MAX = 2.0

# Header/footer drawn by Chromium in the page margins when branding or page
# numbers are requested. The header must be explicitly blank or Chromium
# prints its default date/title line. Templates require inline font-size or
# render at 0.
_PDF_HEADER_TEMPLATE = "<span></span>"


def _footer_template(branding: bool, page_numbers: bool, date_str: str) -> str:
    """Footer HTML: branding ("LangAlpha · date") left, page count right."""
    left = f"<span>LangAlpha · {date_str}</span>" if branding else "<span></span>"
    right = (
        '<span><span class="pageNumber"></span> / <span class="totalPages"></span></span>'
        if page_numbers
        else "<span></span>"
    )
    return (
        '<div style="width:100%; font-size:9px; color:#777; padding:0 12mm; '
        'display:flex; justify-content:space-between;">'
        f"{left}{right}"
        "</div>"
    )

_EXECUTABLE_MISSING_HINT = "executable doesn't exist"

_browser = None
_playwright_cm = None
_browser_lock = asyncio.Lock()


async def _reset_browser() -> None:
    """Drop the cached browser + playwright driver, swallowing teardown errors.

    Called while holding ``_browser_lock`` after a dead handle is detected so
    the next launch starts clean.
    """
    global _browser, _playwright_cm
    browser, cm = _browser, _playwright_cm
    _browser = None
    _playwright_cm = None
    if browser is not None:
        try:
            await browser.close()
        except Exception:
            pass
    if cm is not None:
        try:
            await cm.stop()
        except Exception:
            pass


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
    async with _browser_lock:
        await _reset_browser()


async def _get_browser():
    """Return the singleton headless Chromium, launching it once.

    A cached browser that has crashed or disconnected is discarded and
    relaunched so a dead handle can't wedge every render until restart.
    Raises ``PdfRenderUnavailable`` if Playwright or the Chromium binary is
    missing — both are optional and only required to serve ``?format=pdf``.
    """
    global _browser, _playwright_cm
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
            raise PdfRenderUnavailable("playwright is not installed") from exc
        try:
            cm = async_playwright()
            pw = await cm.start()
            browser = await pw.chromium.launch(headless=True)
        except PlaywrightError as exc:
            if _EXECUTABLE_MISSING_HINT in str(exc).lower():
                raise PdfRenderUnavailable(
                    "Chromium is not installed (run: playwright install chromium)"
                ) from exc
            raise PdfRenderError(f"Failed to launch Chromium: {exc}") from exc
        _playwright_cm = cm
        _browser = browser
        return _browser


async def render_workspace_pdf(
    internal_url: str,
    *,
    workspace_serve_prefix: str,
    scale: float | None = None,
    page_numbers: bool = False,
    branding: bool = True,
) -> bytes:
    """Render a workspace HTML URL to PDF bytes in headless Chromium.

    ``internal_url`` is the server's own loopback wsfiles URL; subresource
    requests are SSRF-gated to ``workspace_serve_prefix`` plus the CDN
    allowlist. ``scale`` (clamped to 0.5–2.0) shrinks/enlarges the whole
    rendering; ``branding`` (default on) stamps "LangAlpha · <date>" in the
    footer and ``page_numbers`` adds ``N / total`` beside it. Raises
    ``PdfRenderUnavailable`` / ``PdfRenderTimeout`` / ``PdfRenderError`` for
    the corresponding failure classes.
    """
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    browser = await _get_browser()

    async with _RENDER_SEMAPHORE:
        context = None
        try:
            # new_context / new_page / route / emulate_media can all raise a
            # PlaywrightError (e.g. the browser died after _get_browser); keep
            # them inside the mapped try so none escapes as a raw 500.
            context = await browser.new_context(viewport=_PRINT_VIEWPORT)

            async def _route_handler(route, request):
                if _is_request_allowed(request.url, workspace_serve_prefix):
                    await route.continue_()
                else:
                    await route.abort()

            async def _ws_route_handler(ws_route):
                # WebSockets are not covered by page.route, so gate them
                # separately. No ws:// URL can pass _is_request_allowed (it
                # requires the https serve prefix or an https CDN host), so
                # every WS is closed unconditionally — sealing the one egress
                # channel the HTTP guard misses (e.g. ws://169.254.169.254,
                # ws://127.0.0.1). There is deliberately no connect path.
                await ws_route.close()

            page = await context.new_page()
            await page.route("**/*", _route_handler)
            await page.route_web_socket("**/*", _ws_route_handler)
            # Lay out with print CSS from the first paint so charts size for
            # paper instead of re-rendering during the print reflow.
            await page.emulate_media(media="print")

            async def _render() -> bytes:
                try:
                    await page.goto(
                        internal_url,
                        wait_until="networkidle",
                        timeout=_GOTO_TIMEOUT_MS,
                    )
                except PlaywrightTimeoutError:
                    # Networkidle never settled (a long-polling asset, etc.).
                    # Re-navigate with wait_until="load": the second goto often
                    # completes where the first stalled, now that CDN assets are
                    # warm in the browser cache.
                    await page.goto(internal_url, wait_until="load", timeout=_GOTO_FALLBACK_TIMEOUT_MS)
                declared_size = await page.evaluate(_PAGE_SIZE_PROBE_JS)
                viewport = _viewport_from_page_size(declared_size) if declared_size else None
                if viewport and viewport != _PRINT_VIEWPORT:
                    await page.set_viewport_size(viewport)
                await page.evaluate(_SETTLE_JS)
                await page.add_style_tag(content=_CANVAS_CLAMP_CSS)
                await page.evaluate(
                    "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
                )
                pdf_kwargs: dict = {"print_background": True, "prefer_css_page_size": True}
                if scale is not None:
                    pdf_kwargs["scale"] = min(PDF_SCALE_MAX, max(PDF_SCALE_MIN, scale))
                if branding or page_numbers:
                    pdf_kwargs["display_header_footer"] = True
                    pdf_kwargs["header_template"] = _PDF_HEADER_TEMPLATE
                    pdf_kwargs["footer_template"] = _footer_template(
                        branding, page_numbers, datetime.now().strftime("%Y-%m-%d")
                    )
                return await page.pdf(**pdf_kwargs)

            return await asyncio.wait_for(_render(), timeout=_RENDER_TIMEOUT_MS / 1000)
        except (asyncio.TimeoutError, PlaywrightTimeoutError) as exc:
            raise PdfRenderTimeout("PDF rendering timed out") from exc
        except PdfRenderError:
            raise
        except PlaywrightError as exc:
            raise PdfRenderError(f"PDF rendering failed: {exc}") from exc
        finally:
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass
