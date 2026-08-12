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

Browser lifecycle: Tier 2/3 use the session classes directly (rather than the
`DynamicFetcher.async_fetch()` classmethod wrapper) so we can shield
`session.close()` from cancellation. `asyncio.wait_for` in safe_wrapper.py
cancels the fetch coroutine on timeout; if close() is not shielded, it gets
cancelled mid-teardown and orphans Chromium helper processes.
"""

import asyncio
import logging
import re
from typing import Literal, Optional

import html_to_markdown
import trafilatura

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


def _log_close_task_exception(task: asyncio.Task) -> None:
    # Observes close_task after outer cancel so asyncio doesn't emit
    # "Task exception was never retrieved" when close() raises post-cancel.
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.warning(f"Browser session close failed post-cancel: {exc!r}")


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


# Tuning for the trafilatura-vs-full-page decision, calibrated on a live 10-page
# sample (financial news, IR releases, explainers, government statements). Well
# extracted pages retain 88-100% of figures and stay above ~10% of the full-page
# size; pages where trafilatura silently drops the article body retain <=28% of
# figures (CNBC card/liveblog layouts) or collapse to <2% of the page (index/
# listing stubs). Thresholds sit inside those gaps, biased toward preserving
# content — for a research agent a noisier full page beats silent data loss.
_STUB_SIZE_RATIO = 0.10       # extraction below this fraction of the full page...
_STUB_MIN_FULL_LEN = 5000     # ...on a non-trivial page => listing/index stub
_FIGURE_MIN_SAMPLE = 8        # only trust the figure ratio above this many figures
_FIGURE_KEEP_RATIO = 0.65     # retaining fewer than this fraction => body dropped

# Context-safety ceiling on the full-page fallback. The fallback returns the
# entire noisy page, which on liveblog/hub layouts runs to hundreds of KB; cap
# it to ~100K tokens (~4 chars/token) so a single crawl can't swamp the agent's
# context. The clean trafilatura extraction is small and never hits this.
_MAX_FULL_PAGE_CHARS = 400_000

_PCT_RE = re.compile(r"\d+(?:\.\d+)?\s?%")
_DOLLAR_RE = re.compile(
    r"\$\s?\d[\d,]*(?:\.\d+)?\s?(?:billion|million|trillion|bn|b|m)?", re.IGNORECASE
)
_BIGNUM_RE = re.compile(r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b")


def _financial_figures(text: str) -> set[str]:
    """Normalized set of $/% /comma-grouped numbers — the detail an analyst needs."""
    figs: set[str] = set()
    for rx in (_PCT_RE, _DOLLAR_RE, _BIGNUM_RE):
        for match in rx.findall(text):
            figs.add(re.sub(r"\s+", "", match.lower()))
    return figs


def _try_trafilatura(html: str) -> Optional[str]:
    """Extract the main article as markdown with a YAML metadata frontmatter.

    `with_metadata=True` keeps trafilatura's title/source/date/author/description
    block — without it a lone `<h1>` page extracts to body-only and loses the
    heading. The frontmatter is already clean (junk fields come back empty and are
    omitted); pass it through and let the agent decide which fields it cares about.
    """
    try:
        return trafilatura.extract(
            html,
            favor_recall=True,
            output_format="markdown",
            include_links=True,
            include_images=True,
            include_formatting=True,
            include_tables=True,
            with_metadata=True,
        )
    except Exception as e:
        logger.debug(f"trafilatura extraction failed: {e}")
        return None


def _try_full_page(html: str) -> Optional[str]:
    """Convert the entire page to markdown via html-to-markdown's Rust core.

    `extract_metadata=False` suppresses html-to-markdown's default <head> dump (a
    `meta-og:*` / gtm-dataLayer frontmatter block) that is pure noise on the hub,
    listing and error pages this full-page fallback handles.
    """
    try:
        return html_to_markdown.convert(
            html, html_to_markdown.ConversionOptions(extract_metadata=False)
        ).content
    except Exception as e:
        logger.debug(f"html-to-markdown conversion failed: {e}")
        return None


def _cap_full_page(text: str) -> str:
    """Truncate an oversized full-page fallback to the context-safety ceiling."""
    if len(text) <= _MAX_FULL_PAGE_CHARS:
        return text
    logger.debug(
        f"full-page fallback {len(text)} chars exceeds cap — truncating to "
        f"{_MAX_FULL_PAGE_CHARS}"
    )
    return text[:_MAX_FULL_PAGE_CHARS] + "\n\n[... truncated: page exceeded ~100K tokens ...]"


def _html_to_markdown(html: str) -> str:
    """Convert fetched HTML to markdown for the LLM.

    trafilatura extracts the main article and strips nav/ads/boilerplate (cleaner,
    3-7x cheaper input), but silently under-extracts on two page shapes. We compare
    it against a faithful full-page conversion (html-to-markdown's Rust core — the
    cheaper of the two and immune to recursion limits) and prefer the full page when
    trafilatura returns an index/listing stub or drops most of the page's financial
    figures. A stdlib text extractor is the last resort.
    """
    extracted = _try_trafilatura(html)
    full = _try_full_page(html)

    # trafilatura found no main content (e.g. legacy table-only filings).
    if not (extracted and extracted.strip()):
        return _cap_full_page(full) if (full and full.strip()) else _plain_text(html)

    # No full-page baseline to compare against — trust trafilatura.
    if not (full and full.strip()):
        return extracted

    # Index/listing stub: trafilatura kept an intro blurb and dropped the link
    # list (SEC/Fed newsrooms). The full page preserves the headlines.
    if len(extracted) < _STUB_SIZE_RATIO * len(full) and len(full) > _STUB_MIN_FULL_LEN:
        logger.debug(
            f"trafilatura output {len(extracted)} chars vs full {len(full)} — "
            "treating as listing stub, using full page"
        )
        return _cap_full_page(full)

    # Card/liveblog layout: trafilatura kept the lead card and dropped the body's
    # figures (CNBC). Compare $/% figure sets; prefer the full page on heavy loss.
    full_figs = _financial_figures(full)
    if len(full_figs) >= _FIGURE_MIN_SAMPLE:
        kept = len(_financial_figures(extracted) & full_figs) / len(full_figs)
        if kept < _FIGURE_KEEP_RATIO:
            logger.debug(
                f"trafilatura retained {kept:.0%} of {len(full_figs)} figures — "
                "treating as dropped body, using full page"
            )
            return _cap_full_page(full)

    return extracted


def _plain_text(html: str) -> str:
    """Last-resort plain-text extraction using the stdlib parser (never recurses)."""
    from html.parser import HTMLParser

    class _Extractor(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.parts: list[str] = []
            self._skip = 0

        def handle_starttag(self, tag, attrs):
            if tag in ("script", "style"):
                self._skip += 1

        def handle_endtag(self, tag):
            if tag in ("script", "style") and self._skip:
                self._skip -= 1

        def handle_data(self, data):
            if not self._skip and data.strip():
                self.parts.append(data.strip())

    try:
        p = _Extractor()
        p.feed(html)
        return " ".join(p.parts)
    except Exception:
        return html


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
            from scrapling.fetchers import AsyncFetcher

            page = await AsyncFetcher.get(
                url,
                stealthy_headers=True,
                follow_redirects=True,
                timeout=_TIER1_TIMEOUT_MS / 1000,  # ms → seconds
            )
            html_body = page.body.decode(page.encoding or "utf-8", errors="replace")
            return page, html_body, page.status

    async def _tier2_fetch(self, url: str):
        # Direct session use (not DynamicFetcher.async_fetch) so we own the
        # close() path and can shield it from outer cancellation.
        async with self._browser_sem:
            from scrapling.engines._browsers._controllers import AsyncDynamicSession

            session = AsyncDynamicSession(
                headless=True,
                disable_resources=self.disable_resources,
                network_idle=self.network_idle,
                timeout=self.timeout,
            )
            return await self._fetch_with_session(session, url)

    async def _tier3_fetch(self, url: str, *, solve_cloudflare: bool):
        """Stealth fetch with optional CF solver. Caller decides based on Tier 2's
        observation — invoking the solver against a non-CF page is wasted work."""
        async with self._browser_sem:
            from scrapling.engines._browsers._stealth import AsyncStealthySession

            session = AsyncStealthySession(
                headless=True,
                network_idle=self.network_idle,
                timeout=self.timeout,
            )
            return await self._fetch_with_session(
                session, url, solve_cloudflare=solve_cloudflare
            )

    async def _fetch_with_session(self, session, url: str, **fetch_kwargs):
        """Start a scrapling session, fetch one URL, shield close() from cancel.

        Prior learnings applied:
          - CancelledError inherits from BaseException and is NOT caught by
            `except Exception`. It must be handled explicitly if we want to
            run teardown before re-raising.
          - `asyncio.shield(coro)` only protects a Task; wrapping a bare
            coroutine is a no-op. We create the close task explicitly, then
            await shield() so the inner task keeps running even if the outer
            is cancelled.
          - Scrapling's `AsyncDynamicSession.start()` wraps browser spawn in
            `except Exception`, which misses CancelledError. On cancellation
            during start(), `self.playwright` stays set but `_is_alive=False`,
            and close() early-returns on the `_is_alive` guard. We force
            `_is_alive=True` before close() so the cleanup path actually runs
            and stops the playwright driver. Without this, cancel-during-start
            leaks the node driver process.
        """
        try:
            await session.start()
            page = await session.fetch(url, **fetch_kwargs)
            html_body = page.body.decode(page.encoding or "utf-8", errors="replace")
            return page, html_body, page.status
        finally:
            # If start() was cancelled mid-spawn, scrapling's own cleanup was
            # skipped (CancelledError bypassed its except Exception). Force
            # close() to run its teardown branches — they're idempotent on
            # None-valued context/browser, so this is safe even if only
            # playwright.stop() is needed.
            if (
                getattr(session, "playwright", None) is not None
                and not getattr(session, "_is_alive", True)
            ):
                session._is_alive = True  # unblock close()'s guard clause
            close_task = asyncio.create_task(session.close())
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError:
                # Outer task cancelled. close_task survives (shield) and will
                # complete in the background, freeing the browser. Attach a
                # done-callback so its exception (if any) is logged instead of
                # surfacing as asyncio's "Task exception was never retrieved".
                close_task.add_done_callback(_log_close_task_exception)
                pass
            except Exception as e:
                # tini (init: true in compose) reaps leaked helpers in prod;
                # dev/macOS has no init backstop so a failed close leaks until
                # the Python process exits.
                logger.warning(
                    f"Browser session close failed (init will reap if present): {e}"
                )

    async def shutdown(self) -> None:
        """No persistent resources to clean up (sessions are per-fetch)."""
        pass
