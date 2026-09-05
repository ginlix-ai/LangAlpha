"""
Crawler backend using Scrapling library.

Implements a three-tier fetching strategy:
  Tier 1 (Fast):    AsyncFetcher.get() -- HTTP-only, TLS impersonation
  Tier 2 (Dynamic): AsyncDynamicSession -- Playwright/patchright Chromium
  Tier 3 (Stealth): AsyncStealthySession -- Camoufox anti-bot bypass

Automatic fallback: Tier 1 -> Tier 2 -> Tier 3 (terminal blocks short-circuit at Tier 1).

Stage-level concurrency: Tier 1 (HTTP) and Tier 2/3 (browser) acquire separate
semaphores. A burst of stuck browser fetches cannot starve fast Tier-1 calls.
The HTTP semaphore is released before any browser-tier wait.

Fetching, browser-session lifecycle and HTML→markdown extraction live in
`mcp_servers/_browser.py` and `mcp_servers/_extract.py`, shared with the scrape
MCP server so the same URL yields the same content whichever path fetched it.
This module owns the tier policy — when to escalate, when to give up — plus the
logging those shared helpers deliberately leave to their caller.
"""

import asyncio
import logging
from typing import Literal, Optional

from mcp_servers._browser import fetch_fast, fetch_with_session, make_session
from mcp_servers._extract import to_markdown

from .backend import CrawlOutput

logger = logging.getLogger(__name__)

# Signals that indicate Tier 1 content is blocked/empty and needs browser rendering
_BLOCKED_SIGNALS = [
    "cloudflare",
    "just a moment",
    "checking your browser",
    "enable javascript",
    "please enable js",
    "ray id",
    "access denied",
    "403 forbidden",
    "captcha",
]

# HTTP statuses where retrying through browser tiers won't help. 401 = auth required;
# 451 = legal block. 403 is excluded — some Cloudflare configs return 403 to curl_cffi
# but 200 to Camoufox's full browser fingerprint, so it remains a real recovery path.
_TERMINAL_BLOCK_STATUSES = (401, 451)

# Tier-1 timeout. curl_cffi calls that haven't returned in 15s are dead — release
# the slot rather than hold it for the full 30s. Covers slow international hosts
# and large EDGAR PDFs comfortably.
_TIER1_TIMEOUT_MS = 15000


def _log_close_failure(exc: BaseException, post_cancel: bool) -> None:
    """``fetch_with_session``'s teardown-failure sink.

    tini (init: true in compose) reaps leaked helpers in prod; dev/macOS has no
    init backstop so a failed close leaks until the Python process exits.
    """
    if post_cancel:
        logger.warning(f"Browser session close failed post-cancel: {exc!r}")
    else:
        logger.warning(f"Browser session close failed (init will reap if present): {exc}")


def _html_to_markdown(html: str) -> str:
    """Shared extraction with this module's logger wired to its decision trace."""
    return to_markdown(html, trace=logger.debug)


def _needs_browser(html_body: str, status: int) -> bool:
    """Detect if HTTP-only fetch returned blocked/empty content needing browser tiers.

    Returns True for 4xx/5xx (except terminal blocks handled separately by caller),
    near-empty bodies, or pages containing block-signal keywords.
    """
    if status >= 400:
        return True
    if not html_body or len(html_body.strip()) < 200:
        return True
    lower = html_body.lower()
    return any(signal in lower for signal in _BLOCKED_SIGNALS)


def _needs_stealth(
    html_body: str, status: int
) -> Optional[Literal["cloudflare", "blocked"]]:
    """Detect if dynamic fetch hit anti-bot protection.

    Returns "cloudflare" when CF challenge signals are present (Tier 3 should run
    the CF solver), "blocked" for plain 401/403 with no challenge page (Tier 3
    should run without the solver), or None when content is acceptable.
    """
    lower = (html_body or "").lower()
    # Cloudflare challenge — solver may help.
    if "cloudflare" in lower and ("ray id" in lower or "just a moment" in lower):
        return "cloudflare"
    # DataDome / generic JS challenge on a short page.
    if len(lower) < 2000 and ("enable js" in lower or "enable javascript" in lower):
        return "cloudflare"
    # Bare 401/403 with no challenge page — running the CF solver would just log
    # "No Cloudflare challenge found" without helping. Still worth a stealth
    # attempt with a different fingerprint.
    if status in (401, 403):
        return "blocked"
    return None


def _extract_title(page) -> str:
    """Extract page title from Scrapling response."""
    try:
        title_el = page.css("title::text")
        return title_el.get() or ""
    except Exception:
        return ""


class ScraplingCrawler:
    """Async crawler using Scrapling with tiered fetching and stage-level concurrency."""

    def __init__(
        self,
        timeout: int = 30000,
        disable_resources: bool = True,
        network_idle: bool = True,
        http_concurrency: int = 20,
        browser_concurrency: int = 6,
    ):
        self.timeout = timeout
        self.disable_resources = disable_resources
        self.network_idle = network_idle
        # Stage-level semaphores. Tier 1 (curl_cffi) is cheap so its cap is high;
        # Tier 2/3 (Chromium/Camoufox) bound RAM at ~browser_concurrency * 400MB.
        # Critically: Tier 1 releases its semaphore before any browser wait, so a
        # burst of stuck browser calls cannot block fast HTTP calls.
        self._http_sem = asyncio.Semaphore(http_concurrency)
        self._browser_sem = asyncio.Semaphore(browser_concurrency)

    async def crawl(self, url: str) -> str:
        """Crawl and return markdown."""
        output = await self.crawl_with_metadata(url)
        return output.markdown

    async def crawl_with_metadata(self, url: str) -> CrawlOutput:
        """Crawl with tiered fallback, return CrawlOutput with status + failure_kind."""
        from .extractors.base import _validate_url
        _validate_url(url)

        # --- Tier 1: Fast HTTP fetch (requires curl_cffi) ---
        try:
            page, html_body, status = await self._tier1_fetch(url)

            # Hard blocks — host permanently rejects scrapers. Skip Tier 2/3
            # entirely. Each blocked-host call burns one curl_cffi call instead
            # of spawning two browsers to confirm the same "no".
            if status in _TERMINAL_BLOCK_STATUSES:
                logger.debug(f"Tier 1 terminal block ({status}) for {url}")
                return CrawlOutput(
                    title="",
                    html="",
                    markdown="",
                    status=status,
                    failure_kind="blocked",
                )
            if status == 429:
                logger.debug(f"Tier 1 rate limited for {url}")
                return CrawlOutput(
                    title="",
                    html="",
                    markdown="",
                    status=status,
                    failure_kind="rate_limited",
                )
            if not _needs_browser(html_body, status):
                title = _extract_title(page)
                markdown = await asyncio.to_thread(_html_to_markdown, html_body)
                logger.debug(f"Tier 1 (fast) succeeded for {url}")
                return CrawlOutput(
                    title=title, html=html_body, markdown=markdown, status=status
                )
            logger.debug(f"Tier 1 insufficient for {url}, escalating to Tier 2")
        except ImportError:
            # curl_cffi not installed — skip Tier 1 (scrapling without [fetchers])
            logger.debug(f"Tier 1 unavailable (curl_cffi not installed), using Tier 2 for {url}")
        except Exception as e:
            # Hard reachability failures (DNS, conn refused) at Tier 1 mean the
            # host is down. Spawning Chromium and Camoufox to confirm the same
            # would just burn ~800MB and ~10s. Short-circuit to infra_error so
            # the wrapper routes the failure correctly without browser cost.
            err = str(e).lower()
            if (
                "could not resolve" in err
                or "couldn't resolve" in err
                or "name resolution" in err
                or "connection refused" in err
            ):
                logger.debug(f"Tier 1 unreachable for {url}: {e}, skipping browsers")
                return CrawlOutput(
                    title="", html="", markdown="", failure_kind="infra_error",
                )
            logger.debug(f"Tier 1 failed for {url}: {e}, escalating to Tier 2")

        # --- Tier 2: Dynamic browser fetch ---
        stealth_reason: Optional[Literal["cloudflare", "blocked"]] = None
        try:
            page, html_body, status = await self._tier2_fetch(url)

            if status == 429:
                logger.debug(f"Tier 2 rate limited for {url}")
                return CrawlOutput(
                    title="",
                    html="",
                    markdown="",
                    status=status,
                    failure_kind="rate_limited",
                )
            stealth_reason = _needs_stealth(html_body, status)
            if stealth_reason is None:
                title = _extract_title(page)
                markdown = await asyncio.to_thread(_html_to_markdown, html_body)
                logger.debug(f"Tier 2 (dynamic) succeeded for {url}")
                return CrawlOutput(
                    title=title, html=html_body, markdown=markdown, status=status
                )
            logger.debug(
                f"Tier 2 blocked ({stealth_reason}) for {url}, escalating to Tier 3"
            )
        except Exception as e:
            logger.debug(f"Tier 2 failed for {url}: {e}, escalating to Tier 3")

        # --- Tier 3: Stealth fetch ---
        # Run CF solver only when Tier 2 actually saw a Cloudflare challenge.
        # On bare 401/403 (stealth_reason == "blocked"), the solver would just
        # log "No Cloudflare challenge found" — wasteful and noisy.
        solve_cloudflare = stealth_reason == "cloudflare"
        try:
            page, html_body, status = await self._tier3_fetch(
                url, solve_cloudflare=solve_cloudflare
            )
            tier3_reason = _needs_stealth(html_body, status)
            if tier3_reason is None:
                title = _extract_title(page)
                markdown = await asyncio.to_thread(_html_to_markdown, html_body)
                logger.debug(f"Tier 3 (stealth) completed for {url} (status={status})")
                return CrawlOutput(
                    title=title, html=html_body, markdown=markdown, status=status
                )
            # Still blocked after stealth tier.
            logger.debug(f"Tier 3 still blocked for {url} (status={status})")
            failure_kind = "blocked" if tier3_reason == "blocked" else "stealth_failed"
            return CrawlOutput(
                title="",
                html="",
                markdown="",
                status=status,
                failure_kind=failure_kind,
            )
        except Exception:
            # Re-raise so SafeCrawlerWrapper._classify_exception can distinguish
            # host-specific errors (DNS, connection refused) from genuinely
            # cross-cutting infra failures (browser crash). Blanket-classifying
            # as "infra_error" here would trip the global breaker on a few bad
            # hostnames and starve all crawls — the exact bug this PR set out
            # to fix.
            logger.debug(f"Tier 3 failed for {url}", exc_info=True)
            raise

    async def _tier1_fetch(self, url: str):
        """HTTP-only fetch via curl_cffi. Bounded by _http_sem; releases on return."""
        async with self._http_sem:
            return await fetch_fast(url, timeout_s=_TIER1_TIMEOUT_MS / 1000)

    async def _tier2_fetch(self, url: str):
        async with self._browser_sem:
            session = make_session(
                "browser",
                timeout_ms=self.timeout,
                disable_resources=self.disable_resources,
                network_idle=self.network_idle,
            )
            return await self._fetch_with_session(session, url)

    async def _tier3_fetch(self, url: str, *, solve_cloudflare: bool):
        """Stealth fetch with optional CF solver. Caller decides based on Tier 2's
        observation — invoking the solver against a non-CF page is wasted work."""
        async with self._browser_sem:
            session = make_session(
                "stealth", timeout_ms=self.timeout, network_idle=self.network_idle
            )
            return await self._fetch_with_session(
                session, url, solve_cloudflare=solve_cloudflare
            )

    async def _fetch_with_session(self, session, url: str, **fetch_kwargs):
        """Shared cancel-safe session fetch with this module's logger attached."""
        return await fetch_with_session(
            session, url, on_close_error=_log_close_failure, **fetch_kwargs
        )

    async def shutdown(self) -> None:
        """No persistent resources to clean up (sessions are per-fetch)."""
        pass
