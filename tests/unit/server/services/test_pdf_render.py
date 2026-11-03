"""Unit tests for the server-side PDF renderer's browser-free logic.

Exercises ``_is_request_allowed`` and ``_viewport_from_page_size`` directly
(pure functions) — the parts of ``pdf_render`` that run without Chromium. The
browser-singleton liveness recovery and the new_context error-taxonomy mapping
are exercised with fake browser/context objects (no real Chromium); the full
render path itself is covered by mocking in the route tests.
"""

from __future__ import annotations

import asyncio
import socket

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from playwright.async_api import Error as PlaywrightError

from src.server.services import pdf_render
from src.server.services.pdf_render import (
    PdfRenderError,
    _footer_template,
    _viewport_from_page_size,
)

_PREFIX = "http://127.0.0.1:8000/api/v1/wsfiles/ws-abc-0001/"

_PUBLIC_HOSTS = [
    "cdnjs.cloudflare.com",
    "fonts.gstatic.com",
    "cdn.plot.ly",
    "api.example.com",
]


def _allowed(url: str, prefix: str) -> bool:
    return asyncio.run(pdf_render._is_request_allowed(url, prefix))


@pytest.fixture(autouse=True)
def _names_resolve_to_a_public_address(monkeypatch):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_a, **_k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))],
    )


# --- Workspace serve prefix -----------------------------------------------


def test_workspace_prefix_allowed():
    assert _allowed(_PREFIX + "results/report.html", _PREFIX)
    assert _allowed(_PREFIX + "results/charts/x.png", _PREFIX)


def test_other_workspace_prefix_blocked():
    # A different workspace's serve prefix is not this render's prefix.
    other = "http://127.0.0.1:8000/api/v1/wsfiles/ws-other-9999/"
    assert not _allowed(other + "results/report.html", _PREFIX)


# --- Other hosts (https only) -------------------------------------------


def test_a_public_host_is_allowed_over_https():
    for host in _PUBLIC_HOSTS:
        assert _allowed(f"https://{host}/lib/x.js", _PREFIX), host


def test_a_public_host_is_blocked_over_http():
    for host in _PUBLIC_HOSTS:
        assert not _allowed(f"http://{host}/lib/x.js", _PREFIX), host


# --- SSRF targets blocked --------------------------------------------------


def test_cloud_metadata_endpoint_blocked():
    assert not _allowed("http://169.254.169.254/latest/meta-data/", _PREFIX)
    assert not _allowed("https://169.254.169.254/latest/meta-data/", _PREFIX)


def test_other_internal_hosts_blocked():
    assert not _allowed("http://127.0.0.1:8003/api/auth/keys", _PREFIX)
    assert not _allowed("http://localhost:6379/", _PREFIX)
    assert not _allowed("http://10.0.0.5/admin", _PREFIX)


@pytest.mark.parametrize("address", ["10.0.0.5", "169.254.169.254", "127.0.0.1", "::1"])
def test_a_name_at_a_private_address_is_blocked(monkeypatch, address):
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_a, **_k: [(family, socket.SOCK_STREAM, 6, "", (address, 443))],
    )
    assert not _allowed("https://metadata.example.com/x", _PREFIX)


def test_a_name_that_does_not_resolve_is_blocked(monkeypatch):
    def no_answer(*_a, **_k):
        raise socket.gaierror

    monkeypatch.setattr(socket, "getaddrinfo", no_answer)
    assert not _allowed("https://nothing.example.com/x", _PREFIX)


def test_recursive_format_pdf_subresource_blocked():
    # A rendered document must not re-invoke the renderer via a subresource.
    assert not _allowed(_PREFIX + "results/report.html?format=pdf", _PREFIX)
    assert not _allowed(_PREFIX + "results/report.html?x=1&format=pdf", _PREFIX)
    # Percent-encoded forms the serve endpoint decodes back to `pdf` are caught
    # too — a substring match on the raw query would have let these through.
    assert not _allowed(_PREFIX + "results/report.html?format=%70df", _PREFIX)
    assert not _allowed(_PREFIX + "results/report.html?format=PDF", _PREFIX)
    assert not _allowed(_PREFIX + "results/report.html?x=1&format=%70df", _PREFIX)
    # Other query strings on workspace assets remain allowed.
    assert _allowed(_PREFIX + "results/report.html?v=2", _PREFIX)
    # A value that merely contains "pdf" is not the pdf format and stays allowed.
    assert _allowed(_PREFIX + "results/report.html?format=pdfx", _PREFIX)


def test_format_pdf_is_refused_on_any_host():
    # The deployment's public address reaches the renderer as well as its
    # loopback prefix does.
    assert not _allowed("https://app.example.com/api/v1/wsfiles/g/x/r.html?format=pdf", _PREFIX)
    assert not _allowed("https://app.example.com/r.html?a=1&FORMAT=%70df", _PREFIX)
    assert _allowed("https://app.example.com/r.html?format=csv", _PREFIX)


# --- Fetches, hop by hop and capped ------------------------------------------


class _Route:
    """Records what the gate did with one request."""

    def __init__(self) -> None:
        self.outcome: tuple[str, object] | None = None
        self.headers: dict[str, str] | None = None

    async def fulfill(self, *, status: int, headers: dict[str, str], body: bytes) -> None:
        self.outcome = ("fulfilled", (status, body))
        self.headers = headers

    async def continue_(self) -> None:
        self.outcome = ("continued", None)

    async def abort(self) -> None:
        self.outcome = ("aborted", None)


@pytest.fixture
def upstream(monkeypatch):
    """The internet as a script of answers by URL, and what was asked of it."""
    answers: dict[str, httpx.Response | Exception] = {}
    asked: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        asked.append(request)
        answer = answers[str(request.url)]
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(pdf_render, "_fetch_transport", lambda: httpx.MockTransport(handler))
    return answers, asked


def _answer(
    url: str,
    budget: pdf_render._Budget | None = None,
    headers: dict[str, str] | None = None,
    method: str = "GET",
    body: bytes | None = None,
    resource_type: str = "other",
) -> _Route:
    route = _Route()
    request = MagicMock(
        url=url,
        method=method,
        headers=headers or {"accept": "*/*"},
        post_data_buffer=body,
        resource_type=resource_type,
    )
    asyncio.run(pdf_render._answer_request(route, request, _PREFIX, budget or pdf_render._Budget()))
    return route


class _Chunks(httpx.AsyncByteStream):
    """A body as a server streams it: built with ``content=``, an answer
    arrives already read, which no transport hands back."""

    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk


def _ok(*chunks: bytes, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(200, headers=headers, stream=_Chunks(*chunks))


def _redirect(location: str, status: int = 302) -> httpx.Response:
    return httpx.Response(status, headers={"location": location}, stream=_Chunks())


def test_a_redirect_through_public_hosts_is_followed_and_answered(upstream):
    answers, asked = upstream
    answers["https://a.example.com/img"] = _redirect("https://b.example.com/img.png")
    answers["https://b.example.com/img.png"] = _ok(b"png")
    route = _answer("https://a.example.com/img")
    assert [str(r.url) for r in asked] == ["https://a.example.com/img", "https://b.example.com/img.png"]
    assert route.outcome == ("fulfilled", (200, b"png"))
    # Asked uncompressed, so the cap counts what the browser gets.
    assert {r.headers["accept-encoding"] for r in asked} == {"identity"}


@pytest.mark.parametrize(
    "hop",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://127.0.0.1:8000/health",
        "https://private.example.com/x",
        "https://app.example.com/r.html?format=pdf",
    ],
)
def test_a_redirect_hop_that_fails_the_gate_is_never_fetched(monkeypatch, upstream, hop):
    def resolve(host, *_a, **_k):
        address = "10.0.0.5" if host == "private.example.com" else "8.8.8.8"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    answers, asked = upstream
    answers["https://a.example.com/img"] = _redirect(hop)
    route = _answer("https://a.example.com/img")
    assert [str(r.url) for r in asked] == ["https://a.example.com/img"]
    assert route.outcome == ("aborted", None)


def test_credentials_stay_with_the_origin_they_were_sent_to(upstream):
    answers, asked = upstream
    answers["https://api.example.com/v1/chart"] = _redirect("/v1/chart.png")
    answers["https://api.example.com/v1/chart.png"] = _redirect("https://cdn.example.net/c.png")
    answers["https://cdn.example.net/c.png"] = _redirect("https://api.example.com/back.png")
    answers["https://api.example.com/back.png"] = _ok(b"png")
    sent = {"accept": "*/*", "Authorization": "Bearer t", "cookie": "s=1", "x-trace": "k"}
    route = _answer("https://api.example.com/v1/chart", headers=sent)
    assert route.outcome == ("fulfilled", (200, b"png"))
    held = [(r.headers.get("authorization"), r.headers.get("cookie")) for r in asked]
    # Kept on the same origin, dropped once the chain leaves it, and not given
    # back when it returns.
    assert held == [("Bearer t", "s=1"), ("Bearer t", "s=1"), (None, None), (None, None)]
    assert {r.headers["x-trace"] for r in asked} == {"k"}


@pytest.mark.parametrize(
    "method, status, then",
    [
        ("POST", 302, "GET"),
        ("POST", 301, "GET"),
        ("PUT", 303, "GET"),
        ("PUT", 302, "PUT"),
        ("DELETE", 301, "DELETE"),
        ("HEAD", 303, "HEAD"),
        ("POST", 307, "POST"),
        ("PATCH", 308, "PATCH"),
    ],
)
def test_a_redirect_changes_the_method_only_where_fetch_does(upstream, method, status, then):
    answers, asked = upstream
    answers["https://api.example.com/a"] = _redirect("https://api.example.com/b", status)
    answers["https://api.example.com/b"] = _ok(b"ok")
    sent = {"accept": "*/*", "content-type": "application/json"}
    _answer("https://api.example.com/a", headers=sent, method=method, body=b'{"q":1}')
    hop = asked[1]
    assert hop.method == then
    # The body and what describes it go only when the request became a GET.
    kept = then == method
    assert (hop.content == b'{"q":1}') is kept
    assert ("content-type" in hop.headers) is kept


@pytest.mark.parametrize("status", [300, 304])
def test_a_3xx_fetch_does_not_follow_reaches_the_page_as_it_is(upstream, status):
    answers, asked = upstream
    answers["https://api.example.com/a"] = httpx.Response(
        status, headers={"location": "https://api.example.com/b"}, stream=_Chunks(b"as is")
    )
    route = _answer("https://api.example.com/a")
    assert [str(r.url) for r in asked] == ["https://api.example.com/a"]
    assert route.outcome == ("fulfilled", (status, b"as is"))


def test_a_chain_past_the_hop_cap_is_aborted(upstream):
    answers, asked = upstream
    for i in range(pdf_render._MAX_REDIRECT_HOPS + 2):
        answers[f"https://a.example.com/{i}"] = _redirect(f"https://a.example.com/{i + 1}")
    route = _answer("https://a.example.com/0")
    assert len(asked) == pdf_render._MAX_REDIRECT_HOPS + 1
    assert route.outcome == ("aborted", None)


def test_the_workspace_prefix_goes_on_as_the_browser_asked(upstream):
    _, asked = upstream
    route = _answer(_PREFIX + "results/chart.png")
    assert asked == []
    assert route.outcome == ("continued", None)


def test_a_fetch_that_fails_aborts_the_request(upstream):
    answers, _ = upstream
    answers["https://a.example.com/img"] = httpx.ConnectError("connection reset")
    assert _answer("https://a.example.com/img").outcome == ("aborted", None)


@pytest.mark.parametrize(
    "answer",
    [
        # Said too large up front: refused before a byte is read.
        lambda: _ok(headers={"content-length": "2048"}),
        # Says nothing, and passes the cap as it streams.
        lambda: _ok(*[b"x" * 512] * 8),
        # Compressed despite the ask: a decoder would expand it past any cap.
        lambda: _ok(b"\x1f\x8b", headers={"content-encoding": "gzip"}),
    ],
    ids=["declared", "streamed", "compressed"],
)
def test_an_answer_past_its_cap_is_aborted(monkeypatch, upstream, answer):
    monkeypatch.setattr(pdf_render, "_MAX_FETCH_BYTES", 1024)
    answers, _ = upstream
    answers["https://a.example.com/big"] = answer()
    assert _answer("https://a.example.com/big").outcome == ("aborted", None)


def test_an_answer_within_its_cap_streams_through_whole(monkeypatch, upstream):
    monkeypatch.setattr(pdf_render, "_MAX_FETCH_BYTES", 4096)
    answers, _ = upstream
    answers["https://a.example.com/big"] = _ok(*[b"x" * 512] * 8)
    assert _answer("https://a.example.com/big").outcome == ("fulfilled", (200, b"x" * 4096))


def test_a_render_stops_taking_answers_once_its_budget_is_spent(upstream):
    answers, _ = upstream
    answers["https://a.example.com/1"] = _ok(b"x" * 600)
    answers["https://a.example.com/2"] = _ok(b"x" * 600)
    budget = pdf_render._Budget(1000)
    assert _answer("https://a.example.com/1", budget).outcome[0] == "fulfilled"
    assert _answer("https://a.example.com/2", budget).outcome == ("aborted", None)


# --- A redirected stylesheet ------------------------------------------------

_SHEET = "https://fonts.example.com/css?family=Inter"
_LANDED = "https://cdn.example.net/v1/css/inter.css"
_NO_CORS = {"accept": "text/css,*/*;q=0.1"}


def _sheet_redirects(answers) -> None:
    answers[_SHEET] = _redirect(_LANDED)
    answers[_LANDED] = _ok(b"@font-face{src:url(../fonts/inter.woff2)}")


def test_a_redirected_stylesheet_imports_where_it_landed(upstream):
    answers, _ = upstream
    _sheet_redirects(answers)
    route = _answer(_SHEET, headers=_NO_CORS, resource_type="stylesheet")
    # Under the URL the page asked for, the sheet's relative url() would
    # resolve against the wrong folder.
    assert route.outcome == ("fulfilled", (200, f'@import url("{_LANDED}");'.encode()))
    assert route.headers == {"content-type": "text/css; charset=utf-8"}


def test_the_import_takes_what_the_redirect_fetched_once(upstream):
    answers, asked = upstream
    _sheet_redirects(answers)
    budget = pdf_render._Budget()
    _answer(_SHEET, budget, headers=_NO_CORS, resource_type="stylesheet")
    left = budget.left

    route = _answer(_LANDED, budget, headers=_NO_CORS, resource_type="stylesheet")

    assert route.outcome == ("fulfilled", (200, b"@font-face{src:url(../fonts/inter.woff2)}"))
    assert [str(r.url) for r in asked] == [_SHEET, _LANDED]
    assert budget.left == left
    assert budget.held == {}
    # Held once: asked again, the browser's next request goes out as any does.
    answers[_LANDED] = _ok(b"again")
    again = _answer(_LANDED, budget, headers=_NO_CORS, resource_type="stylesheet")
    assert again.outcome == ("fulfilled", (200, b"again"))
    assert len(asked) == 3


def test_the_import_still_meets_the_gate(monkeypatch, upstream):
    answers, asked = upstream
    _sheet_redirects(answers)
    budget = pdf_render._Budget()
    _answer(_SHEET, budget, headers=_NO_CORS, resource_type="stylesheet")
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_a, **_k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 443))],
    )

    route = _answer(_LANDED, budget, headers=_NO_CORS, resource_type="stylesheet")

    assert route.outcome == ("aborted", None)
    assert len(asked) == 2


@pytest.mark.parametrize(
    "resource_type, headers",
    [
        # Cors mode checks the answer's CORS headers, and SRI its bytes.
        ("stylesheet", {**_NO_CORS, "origin": "http://127.0.0.1:8000"}),
        ("script", {"accept": "*/*"}),
        ("script", {"accept": "*/*", "origin": "http://127.0.0.1:8000"}),
        ("image", {"accept": "image/*"}),
    ],
    ids=["cors-stylesheet", "classic-script", "cors-script", "image"],
)
def test_anything_else_redirected_takes_the_answer_as_fetched(upstream, resource_type, headers):
    answers, _ = upstream
    _sheet_redirects(answers)
    budget = pdf_render._Budget()
    route = _answer(_SHEET, budget, headers=headers, resource_type=resource_type)
    assert route.outcome == ("fulfilled", (200, b"@font-face{src:url(../fonts/inter.woff2)}"))
    assert budget.held == {}


def test_a_redirected_framed_document_is_refreshed_to_where_it_landed(upstream):
    answers, asked = upstream
    page = "https://b.example.com/reports/q3/index.html"
    answers["https://a.example.com/start"] = _redirect(page)
    answers[page] = _ok(b'<img src="chart.png">')
    budget = pdf_render._Budget()
    route = _answer("https://a.example.com/start", budget, headers={"accept": "text/html"}, resource_type="document")
    # Under the URL the page asked for, its chart.png would resolve on a.example.com.
    assert route.outcome == (
        "fulfilled",
        (200, f"<!doctype html><meta http-equiv=\"refresh\" content=\"0; url='{page}'\">".encode()),
    )
    assert route.headers == {"content-type": "text/html; charset=utf-8"}
    # The refresh meets the gate and takes the document already fetched.
    landed = _answer(page, budget, headers={"accept": "text/html"}, resource_type="document")
    assert landed.outcome == ("fulfilled", (200, b'<img src="chart.png">'))
    assert len(asked) == 2


def test_a_refresh_names_where_it_landed_as_an_attribute_holds_it():
    request = MagicMock(resource_type="document", method="GET", headers={})
    fetched = pdf_render._Fetched(200, {}, b"", url="https://b.example.com/a'b\"<c>.html")
    _, body = pdf_render._stand_in(request, "https://a.example.com/x", fetched)
    assert body == (
        b"<!doctype html><meta http-equiv=\"refresh\" "
        b"content=\"0; url='https://b.example.com/a%27b&quot;&lt;c&gt;.html'\">"
    )


def test_a_redirected_document_posted_to_takes_the_answer_as_fetched(upstream):
    answers, _ = upstream
    answers["https://a.example.com/form"] = _redirect("https://b.example.com/done", 307)
    answers["https://b.example.com/done"] = _ok(b"done")
    route = _answer("https://a.example.com/form", method="POST", body=b"q=1", resource_type="document")
    assert route.outcome == ("fulfilled", (200, b"done"))


def test_every_cookie_an_answer_sets_reaches_the_browser_apart(upstream):
    answers, _ = upstream
    expires = "a=1; Path=/; Expires=Wed, 21 Oct 2026 07:28:00 GMT"
    answers[_LANDED] = httpx.Response(
        200,
        headers=[("set-cookie", expires), ("set-cookie", "b=2; Path=/"), ("vary", "a"), ("vary", "b")],
        stream=_Chunks(b".x{}"),
    )
    route = _answer(_LANDED, headers=_NO_CORS, resource_type="stylesheet")
    # The browser end splits a Set-Cookie on line breaks; an Expires holds a comma.
    assert route.headers["set-cookie"] == f"{expires}\nb=2; Path=/"
    assert route.headers["vary"] == "a, b"


def test_a_stylesheet_that_did_not_redirect_is_answered_as_it_is(upstream):
    answers, _ = upstream
    answers[_LANDED] = _ok(b".x{}")
    budget = pdf_render._Budget()
    route = _answer(_LANDED, budget, headers=_NO_CORS, resource_type="stylesheet")
    assert route.outcome == ("fulfilled", (200, b".x{}"))
    assert budget.held == {}


def test_the_import_quotes_its_url_as_css_does():
    assert pdf_render._css_string('https://a.example.com/a"b\\c\n.css') == (
        '"https://a.example.com/a\\"b\\\\c\\a .css"'
    )


def test_websocket_targets_blocked():
    # WebSockets need a separate route guard, which closes every connection;
    # none would pass this gate either, since it admits only https.
    assert not _allowed("ws://127.0.0.1:6379/", _PREFIX)
    assert not _allowed("ws://169.254.169.254/latest/meta-data/", _PREFIX)
    assert not _allowed("wss://cdnjs.cloudflare.com/socket", _PREFIX)


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
