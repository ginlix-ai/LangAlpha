"""Tests for ScraplingCrawler's cancel-safe session lifecycle.

Verifies that cancelling a browser fetch mid-flight does NOT leak the
scrapling session's close() coroutine. The shielded-task pattern should
let close() complete in the background so Playwright's browser/context/
playwright.stop chain runs to completion.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.tools.crawler.scrapling_crawler import ScraplingCrawler


def _make_session(fetch_side_effect=None):
    """Build a MagicMock shaped like an AsyncDynamicSession / AsyncStealthySession."""
    session = MagicMock()
    session.start = AsyncMock()
    session.fetch = AsyncMock()
    if fetch_side_effect is not None:
        session.fetch.side_effect = fetch_side_effect
    session.close = AsyncMock()
    return session


class TestFetchWithSession:

    @pytest.mark.asyncio
    async def test_happy_path_calls_close(self):
        """Normal completion: start → fetch → close."""
        crawler = ScraplingCrawler()
        fake_page = MagicMock()
        fake_page.body = b"<html>ok</html>"
        fake_page.encoding = "utf-8"
        fake_page.status = 200
        session = _make_session()
        session.fetch.return_value = fake_page

        page, html_body, status = await crawler._fetch_with_session(session, "http://x")

        assert session.start.await_count == 1
        assert session.fetch.await_count == 1
        assert session.close.await_count == 1
        assert status == 200
        assert "ok" in html_body

    @pytest.mark.asyncio
    async def test_fetch_exception_still_closes(self):
        """Exception during fetch → close() still runs."""
        crawler = ScraplingCrawler()
        session = _make_session(fetch_side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError):
            await crawler._fetch_with_session(session, "http://x")

        assert session.close.await_count == 1

    @pytest.mark.asyncio
    async def test_close_exception_logged_but_not_raised(self):
        """close() failing must not mask the fetch result or crash the caller."""
        crawler = ScraplingCrawler()
        fake_page = MagicMock()
        fake_page.body = b"<html>ok</html>"
        fake_page.encoding = "utf-8"
        fake_page.status = 200
        session = _make_session()
        session.fetch.return_value = fake_page
        session.close.side_effect = RuntimeError("close fail")

        # Must not raise even though close errored
        page, html_body, status = await crawler._fetch_with_session(session, "http://x")
        assert status == 200

    @pytest.mark.asyncio
    async def test_fetch_kwargs_forwarded_to_session(self):
        """solve_cloudflare (Tier 3) and other fetch kwargs must reach session.fetch().

        Regression guard: the refactor from StealthyFetcher.async_fetch() to
        AsyncStealthySession.fetch() moved solve_cloudflare from the fetcher
        classmethod to the session's fetch() kwargs. If _fetch_with_session
        ever stopped threading **fetch_kwargs through, Cloudflare bypass
        would silently degrade.
        """
        crawler = ScraplingCrawler()
        fake_page = MagicMock()
        fake_page.body = b"<html>ok</html>"
        fake_page.encoding = "utf-8"
        fake_page.status = 200
        session = _make_session()
        session.fetch.return_value = fake_page

        await crawler._fetch_with_session(
            session, "http://x", solve_cloudflare=True, timeout=42
        )

        session.fetch.assert_awaited_once_with(
            "http://x", solve_cloudflare=True, timeout=42
        )

    @pytest.mark.asyncio
    async def test_cancellation_during_start_forces_close(self):
        """REGRESSION: cancel during session.start() must still trigger teardown.

        Scrapling's AsyncDynamicSession.start() uses `except Exception` which
        misses CancelledError. It sets `self.playwright` before the try but
        only sets `_is_alive=True` at the end. close() guards on _is_alive
        and early-returns. Without our force-_is_alive-true workaround, the
        playwright driver process would leak every cancelled fetch.
        """
        crawler = ScraplingCrawler()
        start_entered = asyncio.Event()

        # Shape the mock to mirror the real half-initialized state that
        # scrapling leaves behind after CancelledError during start().
        session = MagicMock()
        session.playwright = MagicMock()
        session._is_alive = False

        async def slow_start():
            start_entered.set()
            await asyncio.sleep(10)  # Will be cancelled

        session.start = AsyncMock(side_effect=slow_start)
        session.fetch = AsyncMock()
        session.close = AsyncMock()

        async def run():
            await crawler._fetch_with_session(session, "http://x")

        outer = asyncio.create_task(run())
        await start_entered.wait()
        outer.cancel()

        with pytest.raises(asyncio.CancelledError):
            await outer

        # close() must have been invoked even though _is_alive was False.
        # Our finally block forces _is_alive=True so scrapling's close() runs
        # its teardown branches instead of early-returning.
        assert session.close.await_count == 1
        assert session._is_alive is True

    @pytest.mark.asyncio
    async def test_post_cancel_close_exception_is_logged(self, caplog):
        """REGRESSION: close_task raising after outer cancel is observed, not dropped.

        Without the done-callback, asyncio emits 'Task exception was never
        retrieved' when close() raises post-cancel. The done-callback turns
        that into a structured WARNING log.
        """
        import logging

        crawler = ScraplingCrawler()

        fetch_started = asyncio.Event()
        close_raised = asyncio.Event()

        async def slow_close():
            await asyncio.sleep(0.05)
            close_raised.set()
            raise RuntimeError("post-cancel close boom")

        async def slow_fetch(*_, **__):
            fetch_started.set()
            await asyncio.sleep(10)

        session = _make_session()
        session.fetch.side_effect = slow_fetch
        session.close.side_effect = slow_close

        async def run():
            await crawler._fetch_with_session(session, "http://x")

        outer = asyncio.create_task(run())
        await fetch_started.wait()
        outer.cancel()

        with pytest.raises(asyncio.CancelledError):
            await outer

        with caplog.at_level(logging.WARNING, logger="src.tools.crawler.scrapling_crawler"):
            await asyncio.wait_for(close_raised.wait(), timeout=5.0)
            # Yield once so the done-callback fires before assertion.
            await asyncio.sleep(0)

        assert any(
            "post-cancel" in rec.message for rec in caplog.records
        ), f"expected post-cancel warning, got: {[r.message for r in caplog.records]}"

    @pytest.mark.asyncio
    async def test_outer_cancellation_keeps_close_alive(self):
        """CRITICAL: when outer task is cancelled mid-fetch, close() still runs.

        Shields the close task so Playwright teardown completes even when
        the safe_wrapper's asyncio.wait_for cancels us on timeout. Uses an
        Event to synchronize (no sleep races on loaded CI).
        """
        crawler = ScraplingCrawler()

        fetch_started = asyncio.Event()
        close_completed = asyncio.Event()

        async def slow_close():
            # Simulate Playwright's browser.close() taking real time
            await asyncio.sleep(0.05)
            close_completed.set()

        async def slow_fetch(*_, **__):
            fetch_started.set()
            await asyncio.sleep(10)  # Will be cancelled

        session = _make_session()
        session.fetch.side_effect = slow_fetch
        session.close.side_effect = slow_close

        async def run():
            await crawler._fetch_with_session(session, "http://x")

        outer = asyncio.create_task(run())
        await fetch_started.wait()  # Deterministic: fetch is definitely awaiting
        outer.cancel()

        with pytest.raises(asyncio.CancelledError):
            await outer

        # close_task survives because of asyncio.shield — give it headroom
        # so a slow CI runner doesn't flake.
        await asyncio.wait_for(close_completed.wait(), timeout=5.0)
        assert session.close.await_count == 1
