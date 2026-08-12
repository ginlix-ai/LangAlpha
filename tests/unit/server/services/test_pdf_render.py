"""Unit tests for the server-side PDF renderer's browser-free logic.

Exercises ``_is_request_allowed`` and ``_viewport_from_page_size`` directly
(pure functions) — the parts of ``pdf_render`` that run without Chromium. The
browser-singleton liveness recovery and the new_context error-taxonomy mapping
are exercised with fake browser/context objects (no real Chromium); the full
render path itself is covered by mocking in the route tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from playwright.async_api import Error as PlaywrightError

from src.server.services import pdf_render
from src.server.services.pdf_render import (
    PdfRenderError,
    _footer_template,
    _is_request_allowed,
    _viewport_from_page_size,
)

_PREFIX = "http://127.0.0.1:8000/api/v1/wsfiles/ws-abc-0001/"

_CDN_HOSTS = [
    "cdnjs.cloudflare.com",
    "cdn.jsdelivr.net",
    "unpkg.com",
    "esm.sh",
    "fonts.googleapis.com",
    "fonts.gstatic.com",
]


# --- Workspace serve prefix -----------------------------------------------


def test_workspace_prefix_allowed():
    assert _is_request_allowed(_PREFIX + "results/report.html", _PREFIX)
    assert _is_request_allowed(_PREFIX + "results/charts/x.png", _PREFIX)


def test_other_workspace_prefix_blocked():
    # A different workspace's serve prefix is not this render's prefix.
    other = "http://127.0.0.1:8000/api/v1/wsfiles/ws-other-9999/"
    assert not _is_request_allowed(other + "results/report.html", _PREFIX)


# --- CDN allowlist (https only) -------------------------------------------


def test_each_cdn_host_allowed_over_https():
    for host in _CDN_HOSTS:
        assert _is_request_allowed(f"https://{host}/lib/x.js", _PREFIX), host


def test_cdn_host_blocked_over_http():
    for host in _CDN_HOSTS:
        assert not _is_request_allowed(f"http://{host}/lib/x.js", _PREFIX), host


# --- SSRF targets blocked --------------------------------------------------


def test_cloud_metadata_endpoint_blocked():
    assert not _is_request_allowed("http://169.254.169.254/latest/meta-data/", _PREFIX)
    assert not _is_request_allowed("https://169.254.169.254/latest/meta-data/", _PREFIX)


def test_other_internal_hosts_blocked():
    assert not _is_request_allowed("http://127.0.0.1:8003/api/auth/keys", _PREFIX)
    assert not _is_request_allowed("http://localhost:6379/", _PREFIX)
    assert not _is_request_allowed("http://10.0.0.5/internal", _PREFIX)


def test_lookalike_domains_blocked():
    # Suffix/substring tricks must not satisfy the exact-host check.
    assert not _is_request_allowed(
        "https://evil-cdnjs.cloudflare.com.attacker.io/x.js", _PREFIX
    )
    assert not _is_request_allowed("https://unpkg.com.evil.io/x.js", _PREFIX)
    assert not _is_request_allowed("https://notunpkg.com/x.js", _PREFIX)
    assert not _is_request_allowed("https://cdnjs.cloudflare.com.evil.io/x.js", _PREFIX)


def test_recursive_format_pdf_subresource_blocked():
    # A rendered document must not re-invoke the renderer via a subresource.
    assert not _is_request_allowed(_PREFIX + "results/report.html?format=pdf", _PREFIX)
    assert not _is_request_allowed(_PREFIX + "results/report.html?x=1&format=pdf", _PREFIX)
    # Percent-encoded forms the serve endpoint decodes back to `pdf` are caught
    # too — a substring match on the raw query would have let these through.
    assert not _is_request_allowed(_PREFIX + "results/report.html?format=%70df", _PREFIX)
    assert not _is_request_allowed(_PREFIX + "results/report.html?format=PDF", _PREFIX)
    assert not _is_request_allowed(_PREFIX + "results/report.html?x=1&format=%70df", _PREFIX)
    # Other query strings on workspace assets remain allowed.
    assert _is_request_allowed(_PREFIX + "results/report.html?v=2", _PREFIX)
    # A value that merely contains "pdf" is not the pdf format and stays allowed.
    assert _is_request_allowed(_PREFIX + "results/report.html?format=pdfx", _PREFIX)


def test_websocket_targets_blocked():
    # WebSockets need a separate route guard; no ws/wss URL satisfies the
    # https-only allowlist, so the WS handler closes every connection.
    assert not _is_request_allowed("ws://127.0.0.1:6379/", _PREFIX)
    assert not _is_request_allowed("ws://169.254.169.254/latest/meta-data/", _PREFIX)
    assert not _is_request_allowed("wss://cdnjs.cloudflare.com/socket", _PREFIX)


# --- @page size → viewport ---------------------------------------------------


def test_landscape_keyword_flips_letter_default():
    assert _viewport_from_page_size("landscape") == {"width": 1056, "height": 816}


def test_portrait_keyword_keeps_letter_default():
    assert _viewport_from_page_size("portrait") == {"width": 816, "height": 1056}


def test_named_size_portrait_default():
    assert _viewport_from_page_size("a4") == {"width": 794, "height": 1123}
    assert _viewport_from_page_size("letter") == {"width": 816, "height": 1056}


def test_named_size_with_landscape():
    assert _viewport_from_page_size("letter landscape") == {"width": 1056, "height": 816}
    assert _viewport_from_page_size("a4 landscape") == {"width": 1123, "height": 794}
    # Keyword order is free per the CSS grammar.
    assert _viewport_from_page_size("landscape a4") == {"width": 1123, "height": 794}


def test_explicit_lengths():
    assert _viewport_from_page_size("297mm 210mm") == {"width": 1123, "height": 794}
    assert _viewport_from_page_size("11in 8.5in") == {"width": 1056, "height": 816}
    assert _viewport_from_page_size("1056px 816px") == {"width": 1056, "height": 816}


def test_explicit_lengths_honor_orientation_keyword():
    # The tokenizer accepts an orientation keyword alongside explicit lengths;
    # honor it instead of silently dropping it. 8.5in=816px, 11in=1056px.
    assert _viewport_from_page_size("8.5in 11in landscape") == {"width": 1056, "height": 816}
    assert _viewport_from_page_size("11in 8.5in portrait") == {"width": 816, "height": 1056}
    # A keyword that agrees with the given length order is a no-op.
    assert _viewport_from_page_size("11in 8.5in landscape") == {"width": 1056, "height": 816}


def test_single_length_is_square():
    assert _viewport_from_page_size("8.5in") == {"width": 816, "height": 816}


def test_auto_and_unparseable_return_none():
    assert _viewport_from_page_size("auto") is None
    assert _viewport_from_page_size("") is None
    assert _viewport_from_page_size("bogus") is None
    assert _viewport_from_page_size("100vw 100vh") is None


def test_absurd_dimensions_return_none():
    assert _viewport_from_page_size("10px 10px") is None
    assert _viewport_from_page_size("9000px 816px") is None


# --- Footer template ---------------------------------------------------------


def test_footer_branding_and_page_numbers():
    footer = _footer_template(True, True, "2026-06-12")
    assert "LangAlpha · 2026-06-12" in footer
    assert 'class="pageNumber"' in footer
    assert 'class="totalPages"' in footer


def test_footer_branding_only():
    footer = _footer_template(True, False, "2026-06-12")
    assert "LangAlpha · 2026-06-12" in footer
    assert "pageNumber" not in footer


def test_footer_page_numbers_only():
    footer = _footer_template(False, True, "2026-06-12")
    assert "LangAlpha" not in footer
    assert "2026-06-12" not in footer
    assert 'class="pageNumber"' in footer


# --- Browser singleton liveness recovery (Finding 1a) ----------------------


@pytest.fixture
def _isolated_browser_globals():
    """Save/restore the module's browser singleton globals around a test."""
    saved = (pdf_render._browser, pdf_render._playwright_cm)
    pdf_render._browser = None
    pdf_render._playwright_cm = None
    try:
        yield
    finally:
        pdf_render._browser, pdf_render._playwright_cm = saved


def _fake_launch_patch(launched):
    """Patch ``async_playwright`` so each launch yields the next fake browser."""
    cm = MagicMock()
    cm.start = AsyncMock()
    cm.stop = AsyncMock()
    pw = cm.start.return_value
    pw.chromium.launch = AsyncMock(side_effect=list(launched))
    return patch("playwright.async_api.async_playwright", return_value=cm), cm


@pytest.mark.asyncio
async def test_get_browser_relaunches_after_disconnect(_isolated_browser_globals):
    dead = MagicMock()
    dead.is_connected.return_value = True
    dead.close = AsyncMock()
    fresh = MagicMock()
    fresh.is_connected.return_value = True

    launch_patch, cm = _fake_launch_patch([dead, fresh])
    with launch_patch:
        first = await pdf_render._get_browser()
        assert first is dead
        # Cached handle is reused while connected.
        assert await pdf_render._get_browser() is dead

        # Chromium crashes: the cached handle now reports disconnected.
        dead.is_connected.return_value = False
        second = await pdf_render._get_browser()

    assert second is fresh
    dead.close.assert_awaited_once()  # dead handle torn down
    cm.stop.assert_awaited_once()  # its playwright driver stopped too
    assert pdf_render._browser is fresh


@pytest.mark.asyncio
async def test_get_browser_reuses_live_handle(_isolated_browser_globals):
    live = MagicMock()
    live.is_connected.return_value = True

    launch_patch, _ = _fake_launch_patch([live])
    with launch_patch:
        assert await pdf_render._get_browser() is live
        assert await pdf_render._get_browser() is live
        # A single launch backs every call while the handle stays connected.
        assert pdf_render._browser is live


# --- shutdown hook (Finding 3) ---------------------------------------------


@pytest.mark.asyncio
async def test_close_browser_tears_down_singleton(_isolated_browser_globals):
    # Lifespan shutdown must close the cached Chromium and stop its driver,
    # then null the globals so a later render relaunches cleanly.
    browser = MagicMock()
    browser.close = AsyncMock()
    cm = MagicMock()
    cm.stop = AsyncMock()
    pdf_render._browser = browser
    pdf_render._playwright_cm = cm

    await pdf_render.close_browser()

    browser.close.assert_awaited_once()
    cm.stop.assert_awaited_once()
    assert pdf_render._browser is None
    assert pdf_render._playwright_cm is None


@pytest.mark.asyncio
async def test_close_browser_noop_when_never_launched(_isolated_browser_globals):
    # No browser was ever launched (the pdf extra unused) — shutdown is a no-op.
    await pdf_render.close_browser()
    assert pdf_render._browser is None


# --- new_context error taxonomy (Finding 1b) -------------------------------


@pytest.mark.asyncio
async def test_new_context_playwright_error_maps_to_taxonomy(_isolated_browser_globals):
    # A live browser whose new_context fails (e.g. it died right after the
    # liveness check) must surface as PdfRenderError, not a raw PlaywrightError.
    browser = MagicMock()
    browser.is_connected.return_value = True
    browser.new_context = AsyncMock(side_effect=PlaywrightError("target closed"))
    pdf_render._browser = browser

    with pytest.raises(PdfRenderError):
        await pdf_render.render_workspace_pdf(
            "http://127.0.0.1:8000/api/v1/wsfiles/ws-abc-0001/results/report.html",
            workspace_serve_prefix=_PREFIX,
        )


@pytest.mark.asyncio
async def test_new_page_playwright_error_maps_to_taxonomy(_isolated_browser_globals):
    # PlaywrightError from page setup (after the context opens) must also map to
    # the taxonomy, and the opened context must still be closed.
    context = MagicMock()
    context.new_page = AsyncMock(side_effect=PlaywrightError("page crashed"))
    context.close = AsyncMock()
    browser = MagicMock()
    browser.is_connected.return_value = True
    browser.new_context = AsyncMock(return_value=context)
    pdf_render._browser = browser

    with pytest.raises(PdfRenderError):
        await pdf_render.render_workspace_pdf(
            "http://127.0.0.1:8000/api/v1/wsfiles/ws-abc-0001/results/report.html",
            workspace_serve_prefix=_PREFIX,
        )
    context.close.assert_awaited_once()
