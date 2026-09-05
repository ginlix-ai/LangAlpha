"""Scrape MCP server: the bounds that keep a call inside the client's timeout.

The client kills the whole server process when a call passes ``_CALL_TIMEOUT``
(mcp_client_runtime), taking every result in the batch with it and orphaning a
~400MB browser in an image with no reaper. Three guards stand between the tool
and that kill, and all three are pinned here: an outer bound on the fetch, a
batch budget no documented argument combination can exceed, and URL validation
that matches the crawler's — the scrapling tiers bypass the transport guard, so
this server's own check is the only one on the path.
"""

from __future__ import annotations

import asyncio

import pytest

from plugins.alternative_data import scrape_mcp_server as srv
from mcp_servers._browser import url_block_reason


# ── URL validation (parity with src/tools/web/inhouse/extractors/base.py) ──

_BLOCKED = [
    "http://localhost/x",
    "http://localhost:8000/x",
    "http://user@localhost/x",
    "http://127.0.0.1/x",
    "https://127.0.0.1:8443/x",
    "http://10.0.0.5/x",
    "http://192.168.1.1/x",
    "http://172.16.0.1/x",
    "http://169.254.169.254/latest/meta-data/",  # cloud metadata
    "http://0.0.0.0/x",
    "http://[::1]/x",
    "http://[::ffff:127.0.0.1]/x",
    "ftp://example.com/x",
    "file:///etc/passwd",
    "javascript:alert(1)",
    "example.com",  # no scheme
    "http://[::1",  # unparseable — urlparse raises
    "",
]

_ALLOWED = [
    "http://example.com/x",
    "https://EXAMPLE.com/x",
    "https://example.com:8443/x",
    "https://8.8.8.8/x",
    "https://sub.domain.example.co.uk/a?b=c#d",
]


@pytest.mark.parametrize("url", _BLOCKED)
@pytest.mark.asyncio
async def test_blocked_urls_never_reach_the_fetch(url, monkeypatch):
    async def boom(*a, **kw):
        raise AssertionError(f"fetch attempted for blocked URL: {url}")

    monkeypatch.setattr(srv, "_fetch_html", boom)
    result = await srv._scrape_one(url, "fast", "markdown", 5.0, False)
    assert result["error"] == "invalid_url", result
    assert result["detail"]


@pytest.mark.parametrize("url", _ALLOWED)
@pytest.mark.asyncio
async def test_public_urls_pass_validation(url, monkeypatch):
    async def ok(*a, **kw):
        return "<title>t</title><p>body</p>", 200

    monkeypatch.setattr(srv, "_fetch_html", ok)
    result = await srv._scrape_one(url, "fast", "markdown", 5.0, False)
    assert "error" not in result, result
    assert result["status"] == 200


@pytest.mark.parametrize("url", _BLOCKED)
def test_url_block_reason_is_total(url):
    """Never raises — a malformed URL has to come back as a reason string."""
    assert isinstance(url_block_reason(url), str)


@pytest.mark.parametrize("url", _ALLOWED)
def test_url_block_reason_allows_public(url):
    assert url_block_reason(url) is None


@pytest.mark.asyncio
async def test_blocked_url_is_a_row_not_a_batch_failure(monkeypatch):
    async def ok(*a, **kw):
        return "<html></html>", 200

    monkeypatch.setattr(srv, "_fetch_html", ok)
    out = await srv.scrape_pages(
        ["http://127.0.0.1/x", "https://example.com/y"], mode="fast", timeout=5.0
    )
    assert out["count"] == 2
    assert out["results"][0]["error"] == "invalid_url"
    assert "error" not in out["results"][1]


# ── The outer bound on the fetch ──


@pytest.mark.asyncio
async def test_hung_fetch_is_bounded_and_returns_fetch_failed(monkeypatch):
    """A browser fetch has no bound of its own — start() takes no timeout and
    the Cloudflare solver loops — so this server must impose one."""
    started = asyncio.Event()

    async def hang(*a, **kw):
        started.set()
        await asyncio.sleep(3600)

    monkeypatch.setattr(srv, "_fetch_html", hang)
    monkeypatch.setattr(srv, "_BROWSER_GRACE_S", 0.0)

    # Outer wait_for so an unbounded fetch fails the test instead of hanging it.
    result = await asyncio.wait_for(
        srv._scrape_one("https://example.com", "browser", "markdown", 0.05, False),
        timeout=10,
    )
    assert started.is_set()
    assert result["error"] == "fetch_failed", result
    assert "timed out" in result["detail"]
    assert result["url"] == "https://example.com"


@pytest.mark.asyncio
async def test_bound_leaves_room_for_session_start(monkeypatch):
    """The bound is timeout + grace: a fetch using its full page budget plus
    ordinary browser startup must not be cut off."""

    async def slow(*a, **kw):
        await asyncio.sleep(0.15)  # overruns timeout_s, inside timeout_s + grace
        return "<html></html>", 200

    monkeypatch.setattr(srv, "_fetch_html", slow)
    monkeypatch.setattr(srv, "_BROWSER_GRACE_S", 0.5)

    assert srv._fetch_bound_s("browser", 0.05) == pytest.approx(0.55)
    result = await srv._scrape_one(
        "https://example.com", "browser", "html", 0.05, False
    )
    assert "error" not in result, result


def test_admission_and_runtime_read_the_same_bound():
    """A budget computed off a different formula than the one enforced at
    runtime would admit calls it cannot actually finish."""
    for mode in ("fast", "browser", "stealth"):
        assert srv._fetch_bound_s(mode, 30.0) == 30.0 + srv._grace_s(mode)


@pytest.mark.asyncio
async def test_semaphore_is_released_when_the_fetch_times_out(monkeypatch):
    """A bound that leaked its slot would wedge every later browser call."""

    async def hang(*a, **kw):
        await asyncio.sleep(3600)

    monkeypatch.setattr(srv, "_fetch_html", hang)
    monkeypatch.setattr(srv, "_BROWSER_GRACE_S", 0.0)
    for _ in range(srv._BROWSER_CONCURRENCY + 1):
        await asyncio.wait_for(
            srv._scrape_one("https://example.com", "browser", "html", 0.05, False),
            timeout=10,
        )
    assert not srv._BROWSER_SEM.locked()


# ── The batch budget ──


def _accepts(mode: str, timeout: float, n: int) -> bool:
    return (
        srv._validate_args(mode, "markdown", timeout) is None
        and srv._check_budget(mode, timeout, n) is None
    )


@pytest.mark.parametrize("mode", ["fast", "browser", "stealth"])
def test_no_accepted_call_can_outlive_the_budget(mode):
    """The invariant, swept over every documented argument combination.

    Worst case is one full bound per wave: waves x (timeout + grace).
    """
    grace = srv._grace_s(mode)
    checked = 0
    for n in range(1, srv._MAX_BULK_URLS + 1):
        for tenths in range(10, int(srv._MAX_TIMEOUT_S * 10) + 1):
            timeout = tenths / 10
            if not _accepts(mode, timeout, n):
                continue
            checked += 1
            worst = srv._waves(mode, n) * (timeout + grace)
            assert worst <= srv._CALL_BUDGET_S, (
                f"{mode} n={n} timeout={timeout} → worst {worst:.1f}s"
            )
    assert checked  # the sweep must actually accept something


def test_budget_sits_under_the_client_call_timeout():
    """The number this whole file defends. If the client's timeout drops, the
    budget has to drop with it — that coupling is invisible from this file."""
    from ptc_agent.core.sandbox.mcp_client_runtime import _CALL_TIMEOUT

    assert srv._CALL_BUDGET_S < _CALL_TIMEOUT
    assert _CALL_TIMEOUT - srv._CALL_BUDGET_S >= 10.0


def test_single_fetch_fits_the_budget_at_the_documented_maximum():
    """The documented per-call max must fit one fetch, or the cap is a lie."""
    for mode in ("fast", "browser", "stealth"):
        assert srv._MAX_TIMEOUT_S + srv._grace_s(mode) <= srv._CALL_BUDGET_S


@pytest.mark.asyncio
async def test_full_browser_batch_at_default_timeout_is_refused(monkeypatch):
    """The combination that used to worst-case at 150s against a 120s kill."""

    async def boom(*a, **kw):
        raise AssertionError("over-budget batch must be refused before fetching")

    monkeypatch.setattr(srv, "_fetch_html", boom)
    urls = [f"https://example.com/{i}" for i in range(srv._MAX_BULK_URLS)]
    out = await srv.scrape_pages(urls, mode="browser", timeout=srv._DEFAULT_TIMEOUT_S)
    assert out["error"] == "invalid_timeout", out
    assert "browser" in out["detail"]
    assert "4 URLs" in out["detail"]  # actionable: what would fit


@pytest.mark.asyncio
async def test_full_fast_batch_at_default_timeout_is_accepted(monkeypatch):
    async def ok(*a, **kw):
        return "<html></html>", 200

    monkeypatch.setattr(srv, "_fetch_html", ok)
    urls = [f"https://example.com/{i}" for i in range(srv._MAX_BULK_URLS)]
    out = await srv.scrape_pages(urls, mode="fast", timeout=srv._DEFAULT_TIMEOUT_S)
    assert out["count"] == srv._MAX_BULK_URLS
    assert all("error" not in r for r in out["results"])


@pytest.mark.asyncio
async def test_small_browser_batch_at_default_timeout_is_accepted(monkeypatch):
    async def ok(*a, **kw):
        return "<html></html>", 200

    monkeypatch.setattr(srv, "_fetch_html", ok)
    urls = ["https://example.com/a", "https://example.com/b"]
    out = await srv.scrape_pages(urls, mode="browser", timeout=srv._DEFAULT_TIMEOUT_S)
    assert out["count"] == 2
    assert all("error" not in r for r in out["results"])


@pytest.mark.asyncio
async def test_over_max_timeout_still_rejected_first():
    out = await srv.scrape_page("https://example.com", timeout=srv._MAX_TIMEOUT_S + 1)
    assert out["error"] == "invalid_timeout"
    assert f"1-{srv._MAX_TIMEOUT_S:.0f}s" in out["detail"]


# ── Docstrings state the real numbers ──


@pytest.mark.parametrize("tool", ["scrape_page", "scrape_pages"])
def test_docstring_states_the_real_timeout_cap(tool):
    doc = getattr(srv, tool).__doc__ or getattr(srv, tool).fn.__doc__
    assert f"1-{srv._MAX_TIMEOUT_S:.0f}." in doc


def test_docstring_states_the_real_browser_batch_cap():
    """The agent plans batches off this number; drift makes it plan calls that
    only come back as invalid_timeout."""
    cap = next(
        n
        for n in range(srv._MAX_BULK_URLS, 0, -1)
        if _accepts("browser", srv._DEFAULT_TIMEOUT_S, n)
    )
    doc = srv.scrape_pages.__doc__ or srv.scrape_pages.fn.__doc__
    assert f"{cap} URLs per call" in doc or f"({cap} in browser/stealth)" in doc


# ── Teardown reporting ──


@pytest.mark.asyncio
async def test_session_fetch_gets_the_close_error_hook(monkeypatch):
    seen: dict[str, object] = {}

    def fake_session(mode, **kw):
        return object()

    async def fake_fetch(session, url, **kwargs):
        seen.update(kwargs)
        return None, "<html></html>", 200

    monkeypatch.setattr(srv, "make_session", fake_session)
    monkeypatch.setattr(srv, "fetch_with_session", fake_fetch)
    await srv._fetch_html("https://example.com", "browser", 5.0, False)
    assert seen["on_close_error"] is srv._report_close_error


def test_close_error_hook_writes_one_stderr_line(capsys):
    srv._report_close_error(RuntimeError("driver gone"), False)
    srv._report_close_error(RuntimeError("driver gone"), True)
    lines = capsys.readouterr().err.strip().splitlines()
    assert len(lines) == 2
    assert "RuntimeError: driver gone" in lines[0]
    assert "during teardown" in lines[0]
    assert "after cancellation" in lines[1]


@pytest.mark.asyncio
async def test_hung_close_does_not_stall_the_caller(monkeypatch):
    """A wedged Playwright close previously held the teardown await open until
    the generated client's whole-server kill took every batch result with it;
    the deadline detaches the close and returns the fetch instead."""
    from mcp_servers import _browser

    monkeypatch.setattr(_browser, "_CLOSE_DEADLINE_S", 0.05)
    release = asyncio.Event()

    class _Page:
        body = b"<html></html>"
        encoding = "utf-8"
        status = 200

    class _Session:
        async def start(self):
            pass

        async def fetch(self, url, **kw):
            return _Page()

        async def close(self):
            await release.wait()

    _page, html_body, status = await asyncio.wait_for(
        _browser.fetch_with_session(_Session(), "http://x"), timeout=2.0
    )
    assert status == 200
    assert html_body == "<html></html>"
    release.set()  # let the detached close finish so the loop drains clean
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_close_wait_is_capped_by_the_callers_deadline():
    """With most of the call budget already spent, a hung close may only wait
    out the remainder — never the full close deadline on top of it, which
    chained across waves would cross the client's process kill."""
    import time as _time

    from mcp_servers import _browser

    release = asyncio.Event()

    class _Page:
        body = b"<html></html>"
        encoding = "utf-8"
        status = 200

    class _Session:
        async def start(self):
            pass

        async def fetch(self, url, **kw):
            return _Page()

        async def close(self):
            await release.wait()

    # _CLOSE_DEADLINE_S stays at its shipped 15s; the caller's deadline wins.
    await asyncio.wait_for(
        _browser.fetch_with_session(
            _Session(), "http://x", close_deadline=_time.monotonic() + 0.05
        ),
        timeout=2.0,
    )
    release.set()
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_tools_charge_teardown_to_the_call_budget(monkeypatch):
    """Both entry points hand the fetch an absolute deadline sized by
    _CALL_BUDGET_S, so teardown draws down the budget admission checked."""
    import time as _time

    seen: dict[str, object] = {}

    def fake_session(mode, **kw):
        return object()

    async def fake_fetch(session, url, **kwargs):
        seen.update(kwargs)
        return None, "<html></html>", 200

    monkeypatch.setattr(srv, "make_session", fake_session)
    monkeypatch.setattr(srv, "fetch_with_session", fake_fetch)

    for call in (
        lambda: srv.scrape_page("https://example.com", mode="browser"),
        lambda: srv.scrape_pages(["https://example.com"], mode="browser"),
    ):
        seen.clear()
        before = _time.monotonic()
        await call()
        after = _time.monotonic()
        deadline = seen["close_deadline"]
        assert before + srv._CALL_BUDGET_S <= deadline <= after + srv._CALL_BUDGET_S
