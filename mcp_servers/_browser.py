"""Scrapling fetch + browser-session lifecycle, shared by the scrape MCP server
and the in-process crawler.

Sessions are driven directly rather than through ``DynamicFetcher.async_fetch``
so ``close()`` can be shielded from cancellation: an outer ``asyncio.wait_for``
timeout otherwise cancels teardown mid-flight and orphans Chromium helpers.

Sandbox-runnable: scrapling imports inside the functions that use it, the same
rule the MCP server files follow. Logging is the caller's job — pass
``on_close_error`` to observe a teardown failure.

``url_block_reason`` lives here rather than beside the crawler's copy because
callers of these fetchers run in sandboxes, where only ``mcp_servers/`` and a
couple of ``src`` packages exist.
"""

from __future__ import annotations

import asyncio
import ipaddress
import time
from typing import Any, Callable
from urllib.parse import urlparse

#: ``(exception, post_cancel)`` — ``post_cancel`` marks the shielded close task
#: failing after the caller was already cancelled, where nothing can be raised.
CloseErrorHook = Callable[[BaseException, bool], None]


def _page_html(page: Any) -> str:
    return page.body.decode(page.encoding or "utf-8", errors="replace")


def url_block_reason(url: str) -> str | None:
    """Why this URL must not be fetched, or None if it may be.

    The scrapling tiers drive their own transports, so the httpx-level guard
    never sees them and this is the whole SSRF check on this path. Semantics
    match the crawler's ``_validate_url``: literal addresses only, since a
    hostname's resolution is not known until the fetch. Total by construction —
    a malformed URL is a reason string, never a raise.
    """
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
    except ValueError as e:
        return f"unparseable URL ({e})"

    if parsed.scheme not in ("http", "https"):
        return f"URL must be http(s), got scheme {parsed.scheme!r}"
    if hostname in ("", "localhost"):
        return "access to localhost is not allowed"

    try:
        addr = ipaddress.ip_address(hostname)
    except ValueError:
        return None  # a name, not a literal address
    if (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_reserved
        or addr.is_unspecified
    ):
        return f"access to private/reserved address {hostname} is not allowed"
    return None


async def fetch_fast(url: str, *, timeout_s: float) -> tuple[Any, str, int]:
    """HTTP-only fetch via curl_cffi — no browser, so no teardown to shield."""
    from scrapling.fetchers import AsyncFetcher

    page = await AsyncFetcher.get(
        url, stealthy_headers=True, follow_redirects=True, timeout=timeout_s
    )
    return page, _page_html(page), page.status


def make_session(
    mode: str,
    *,
    timeout_ms: float,
    disable_resources: bool = True,
    network_idle: bool = True,
) -> Any:
    """Unstarted browser session: ``"browser"`` drives Chromium, else Camoufox."""
    if mode == "browser":
        from scrapling.engines._browsers._controllers import AsyncDynamicSession

        return AsyncDynamicSession(
            headless=True,
            disable_resources=disable_resources,
            network_idle=network_idle,
            timeout=timeout_ms,
        )

    from scrapling.engines._browsers._stealth import AsyncStealthySession

    return AsyncStealthySession(
        headless=True, network_idle=network_idle, timeout=timeout_ms
    )


# A hung Playwright/Camoufox close would otherwise stall the caller (bulk
# fetches wait for a cancelled fetch's teardown) until the whole-server kill;
# past this deadline the close keeps running detached.
_CLOSE_DEADLINE_S = 15.0


async def fetch_with_session(
    session: Any,
    url: str,
    *,
    on_close_error: CloseErrorHook | None = None,
    close_deadline: float | None = None,
    **fetch_kwargs: Any,
) -> tuple[Any, str, int]:
    """Start a session, fetch one URL, then tear down without losing the browser.

    Two hazards the plain ``async with``/``close()`` shape gets wrong:
      - ``asyncio.shield(coro)`` only protects a Task, so close() is scheduled
        as an explicit task first; on outer cancellation it keeps running in
        the background and its exception is handed to ``on_close_error``
        instead of surfacing as "Task exception was never retrieved".
      - Scrapling's ``start()`` wraps the browser spawn in ``except Exception``,
        which misses CancelledError: cancel-during-start leaves ``playwright``
        set but ``_is_alive`` False, and close() early-returns on that guard,
        leaking the node driver. Forcing the flag runs the teardown branches,
        which are idempotent on None-valued context/browser.
    """
    try:
        await session.start()
        page = await session.fetch(url, **fetch_kwargs)
        return page, _page_html(page), page.status
    finally:
        if getattr(session, "playwright", None) is not None and not getattr(
            session, "_is_alive", True
        ):
            session._is_alive = True  # unblock close()'s guard clause
        # ``close_deadline`` (absolute time.monotonic()) lets the caller charge
        # teardown to its own call budget: a fetch cancelled at its bound would
        # otherwise ADD the full close wait on top, and chained across a batch's
        # waves that overshoot crosses the client's whole-process kill.
        wait_s = _CLOSE_DEADLINE_S
        if close_deadline is not None:
            wait_s = max(0.0, min(wait_s, close_deadline - time.monotonic()))
        close_task = asyncio.create_task(session.close())
        try:
            await asyncio.wait_for(asyncio.shield(close_task), wait_s)
        except (TimeoutError, asyncio.CancelledError):
            close_task.add_done_callback(
                lambda task: _report_post_cancel(task, on_close_error)
            )
        except Exception as e:  # noqa: BLE001 - teardown must never mask the fetch
            if on_close_error is not None:
                on_close_error(e, False)


def _report_post_cancel(task: asyncio.Task, hook: CloseErrorHook | None) -> None:
    # Reads the exception even without a hook — that read is what stops asyncio
    # logging "Task exception was never retrieved".
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None and hook is not None:
        hook(exc, True)
