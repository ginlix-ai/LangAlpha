"""Server-side HTML→PDF rendering via headless Chromium (Playwright).

Renders a workspace HTML document, loaded from the server's own loopback
``/api/v1/wsfiles/...`` route, into PDF bytes, in the browser and behind the
request gate of ``headless_browser``: the document loads what it loads in the
viewer and nothing only the server's network can reach. Playwright is
imported lazily, so this module imports without the ``pdf`` extra; callers
map ``PdfRenderUnavailable`` to a 501.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime

from src.server.services.headless_browser import (
    BrowserError,
    BrowserUnavailable,
    RequestGate,
    close_browser,
    gated_context,
    use_browser,
)

__all__ = [
    "PDF_SCALE_MAX",
    "PDF_SCALE_MIN",
    "PdfRenderError",
    "PdfRenderTimeout",
    "PdfRenderUnavailable",
    "close_browser",
    "render_workspace_pdf",
]


# --- Error taxonomy --------------------------------------------------------

# The browser's own failures, under the names this module's callers catch.
PdfRenderError = BrowserError
PdfRenderUnavailable = BrowserUnavailable


class PdfRenderTimeout(PdfRenderError):
    """Rendering exceeded the allotted time budget."""


# --- The render ------------------------------------------------------------

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
    requests pass the render gate (``headless_browser.RequestGate``). ``scale`` (clamped to 0.5 to 2.0) shrinks/enlarges the whole
    rendering; ``branding`` (default on) stamps "LangAlpha · <date>" in the
    footer and ``page_numbers`` adds ``N / total`` beside it. Raises
    ``PdfRenderUnavailable`` / ``PdfRenderTimeout`` / ``PdfRenderError`` for
    the corresponding failure classes.
    """
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    async with use_browser() as browser, _RENDER_SEMAPHORE:
        try:
            # new_context / new_page / route / emulate_media can all raise a
            # PlaywrightError (e.g. the browser died after get_browser); keep
            # them inside the mapped try so none escapes as a raw 500.
            async with gated_context(
                browser, RequestGate(workspace_serve_prefix), viewport=_PRINT_VIEWPORT
            ) as context:
                page = await context.new_page()
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
