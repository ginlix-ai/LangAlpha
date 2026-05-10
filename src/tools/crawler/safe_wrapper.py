"""
Safe wrapper for crawler operations with isolation and fault tolerance.

Three-layer health model:
  - Blocked-host cache: hosts that returned 401/403/451. TTL 15 min, LRU 256.
    Hit → fast-fail with [blocked]. Never trips any breaker.
  - Per-host circuit breaker: transient host-level failures (timeout, 5xx,
    rate-limited, stealth_failed). LRU 256. Trips after N consecutive failures.
  - Global infra breaker: cross-cutting crawler-side failures (browser crash,
    DNS, conn refused). Trips → all hosts fail-fast until recovery.

A failing Reuters cannot poison Wikipedia: each host has its own breaker, and
permanent blocks bypass breakers entirely via the cache.

Stage-level concurrency lives inside ScraplingCrawler (HTTP vs browser
semaphores). The wrapper keeps an admission counter (_max_queue) but no outer
semaphore — that was the source of the Tier-1 starvation bug.
"""

import asyncio
import logging
import os
import signal
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Orphan reaper tunables. Tier 2 (patchright) spawns `chrome*`; Tier 3 (Camoufox)
# spawns `firefox`/`camoufox`. Both get reparented to PID 1 on cancellation, so
# the reaper matches both prefixes.
_INIT_PID = 1
_BROWSER_COMM_PREFIXES = ("chrome", "firefox", "camoufox")
_FORK_EXEC_GRACE_SECONDS = 5.0  # Skip procs briefly showing ppid=1 during fork/exec
_STAT_PPID_IDX = 1       # /proc/[pid]/stat field 4 (1-indexed), 0-indexed after comm
_STAT_STARTTIME_IDX = 19  # /proc/[pid]/stat field 22, 0-indexed after comm
# The reaper only runs when PID 1 is one of these, because ppid==1 only reliably
# means "orphan reaped to init" when init actually reaps. If PID 1 is our python
# worker (init: true silently failed), ppid==1 matches direct children including
# LIVE browsers of in-flight workflows — the reaper would then SIGKILL healthy crawls.
_SAFE_INIT_PROCESSES = ("tini", "docker-init", "catatonit", "dumb-init")

# Three-layer health model tunables.
_BLOCKED_TTL_SECONDS = 900.0       # 15-min blocked-host cache entry
_BLOCKED_LRU_CAP = 256             # bound the cache under host churn
_HOST_BREAKERS_LRU_CAP = 256       # bound per-host breaker state
_REAPER_INTERVAL_SECONDS = 300.0   # opportunistic reap cadence
# Number of consecutive `blocked` responses on the same host before caching it.
# A single 401 on /paywalled-article does not poison nytimes.com homepage; two
# consecutive blocks (no successful fetch in between) confirm the whole host is
# rejecting us. Resets on any successful fetch to that host.
_BLOCK_CACHE_THRESHOLD = 2
_BLOCK_ATTEMPTS_LRU_CAP = 256      # bound block-attempt counter under churn

# Exception substrings that classify as cross-cutting infra failures (trip the
# global infra breaker in addition to the per-host breaker). Other exceptions
# stay host-scoped.
_INFRA_ERROR_TYPES = ("browser_closed", "dns_error", "connection_refused")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CrawlResult:
    """Structured crawl result that never raises exceptions."""
    success: bool
    markdown: Optional[str] = None
    title: Optional[str] = None
    error: Optional[str] = None
    # Passthrough of CrawlOutput.failure_kind for failures classified by the
    # crawler, plus wrapper-level kinds (circuit_open, queue_full, cancelled,
    # crawl_error) for failures classified at this layer.
    error_type: Optional[str] = None


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class QueueFullError(Exception):
    """Raised when crawler queue is at capacity."""
    pass


class CrawlerCircuitBreaker:
    """Circuit breaker. Used for both per-host and global infra layers."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ):
        self.failure_threshold = failure_threshold
        self._base_recovery_timeout = recovery_timeout
        self._max_recovery_timeout = 900.0
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self._consecutive_opens = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def check_state(self) -> None:
        """Check and potentially transition state based on time elapsed."""
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if self.last_failure_time and \
                   time.time() - self.last_failure_time > self.recovery_timeout:
                    logger.info("Circuit breaker transitioning to half-open")
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0

    async def record_success(self) -> None:
        async with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    logger.info("Circuit breaker closing after recovery")
                    self.state = CircuitState.CLOSED
                    self._consecutive_opens = 0
                    self.recovery_timeout = self._base_recovery_timeout

    async def record_failure(self, trigger_reset: Optional[Callable] = None) -> None:
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            should_open = False

            if self.state == CircuitState.HALF_OPEN:
                self._consecutive_opens += 1
                self.recovery_timeout = min(
                    self._base_recovery_timeout * (2 ** self._consecutive_opens),
                    self._max_recovery_timeout,
                )
                logger.warning(
                    f"Circuit breaker re-opening after half-open failure "
                    f"(consecutive_opens={self._consecutive_opens}, "
                    f"next_recovery={self.recovery_timeout}s)"
                )
                self.state = CircuitState.OPEN
                should_open = True
            elif self.failure_count >= self.failure_threshold:
                logger.warning(f"Circuit breaker opening after {self.failure_count} failures")
                self.state = CircuitState.OPEN
                should_open = True

            if should_open and trigger_reset:
                logger.info("Triggering browser reset due to circuit open")
                asyncio.create_task(trigger_reset())

    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN


_VALID_BACKENDS = frozenset({"scrapling", "router"})


class SafeCrawlerWrapper:
    """Safe wrapper for web crawling with three-layer health isolation.

    Per-host breaker isolates one bad host from the rest. Blocked-host cache
    fast-fails permanent rejections in <1ms without touching any breaker.
    Global infra breaker only trips on cross-cutting failures (browser crash,
    DNS resolver down) — never on host-specific issues.
    """

    def __init__(
        self,
        max_queue_size: int = 100,
        default_timeout: float = 60.0,
        circuit_failure_threshold: int = 5,
        circuit_recovery_timeout: float = 60.0,
        circuit_success_threshold: int = 2,
        backend: str = "scrapling",
        http_concurrency: int = 20,
        browser_concurrency: int = 6,
    ):
        self._queue_count = 0
        self._max_queue = max_queue_size
        self._default_timeout = default_timeout
        # Per-host breaker config — shared across all hosts but each host
        # gets its own breaker instance.
        self._circuit_failure_threshold = circuit_failure_threshold
        self._circuit_recovery_timeout = circuit_recovery_timeout
        self._circuit_success_threshold = circuit_success_threshold
        # Global infra breaker. Trips only on cross-cutting failures
        # (browser_closed, dns_error, connection_refused). When open, all
        # crawls fail-fast until recovery — DNS being broken is genuinely a
        # cross-host problem.
        self._infra_breaker = CrawlerCircuitBreaker(
            failure_threshold=circuit_failure_threshold,
            recovery_timeout=circuit_recovery_timeout,
            success_threshold=circuit_success_threshold,
        )
        # Three-layer state. All access guarded by self._lock.
        self._blocked_hosts: OrderedDict[str, float] = OrderedDict()
        self._host_breakers: OrderedDict[str, CrawlerCircuitBreaker] = OrderedDict()
        # Consecutive-block counter — see `_BLOCK_CACHE_THRESHOLD`. Required to
        # avoid a single 401 on a paywalled URL poisoning the entire host.
        self._block_attempts: OrderedDict[str, int] = OrderedDict()
        self._lock = asyncio.Lock()
        # Reaper state.
        self._reaper_lock = asyncio.Lock()
        self._last_reap_time = 0.0
        # Crawler instance (lazy-init). Receives concurrency caps for stage-level
        # semaphores inside the crawler.
        self._crawler = None
        self._http_concurrency = http_concurrency
        self._browser_concurrency = browser_concurrency
        if backend not in _VALID_BACKENDS:
            raise ValueError(f"Unknown crawler backend: {backend!r}. Must be one of {_VALID_BACKENDS}")
        self._backend = backend

    async def _get_crawler(self):
        """Lazy-initialize crawler with stage-level concurrency caps."""
        if self._crawler is None:
            if self._backend == "scrapling":
                from .scrapling_crawler import ScraplingCrawler
                self._crawler = ScraplingCrawler(
                    http_concurrency=self._http_concurrency,
                    browser_concurrency=self._browser_concurrency,
                )
            elif self._backend == "router":
                from .router import ContentRouter
                self._crawler = ContentRouter(
                    http_concurrency=self._http_concurrency,
                    browser_concurrency=self._browser_concurrency,
                )
            else:
                raise ValueError(f"Unknown crawler backend: {self._backend}")
        return self._crawler

    # ------------------------------------------------------------------ #
    # Three-layer state helpers — all expect self._lock to be held.
    # ------------------------------------------------------------------ #

    def _is_blocked_locked(self, netloc: str) -> bool:
        """Check blocked-host cache. LRU-touches on hit. Removes expired entries."""
        expires = self._blocked_hosts.get(netloc)
        if expires is None:
            return False
        if time.time() >= expires:
            # Expired — drop and treat as miss. Next call will re-fetch and
            # re-classify.
            del self._blocked_hosts[netloc]
            return False
        self._blocked_hosts.move_to_end(netloc)
        return True

    def _set_blocked_locked(self, netloc: str) -> None:
        """Mark a host as blocked for _BLOCKED_TTL_SECONDS."""
        self._blocked_hosts[netloc] = time.time() + _BLOCKED_TTL_SECONDS
        self._blocked_hosts.move_to_end(netloc)
        while len(self._blocked_hosts) > _BLOCKED_LRU_CAP:
            self._blocked_hosts.popitem(last=False)

    def _record_block_attempt_locked(self, netloc: str) -> int:
        """Increment the block-attempt counter for netloc and return new value."""
        count = self._block_attempts.get(netloc, 0) + 1
        self._block_attempts[netloc] = count
        self._block_attempts.move_to_end(netloc)
        while len(self._block_attempts) > _BLOCK_ATTEMPTS_LRU_CAP:
            self._block_attempts.popitem(last=False)
        return count

    def _clear_block_attempts_locked(self, netloc: str) -> None:
        """Reset block-attempt counter on success — paywalled article doesn't poison the host."""
        self._block_attempts.pop(netloc, None)

    def _get_or_create_host_breaker_locked(self, netloc: str) -> CrawlerCircuitBreaker:
        """Return the per-host breaker, creating one (and LRU-evicting) if needed."""
        breaker = self._host_breakers.get(netloc)
        if breaker is None:
            breaker = CrawlerCircuitBreaker(
                failure_threshold=self._circuit_failure_threshold,
                recovery_timeout=self._circuit_recovery_timeout,
                success_threshold=self._circuit_success_threshold,
            )
            self._host_breakers[netloc] = breaker
        else:
            self._host_breakers.move_to_end(netloc)
        while len(self._host_breakers) > _HOST_BREAKERS_LRU_CAP:
            self._host_breakers.popitem(last=False)
        return breaker

    def _should_reap_locked(self) -> bool:
        """Atomic gate for opportunistic reaping. Returns True at most once per interval."""
        now = time.time()
        if now - self._last_reap_time > _REAPER_INTERVAL_SECONDS:
            self._last_reap_time = now
            return True
        return False

    # ------------------------------------------------------------------ #
    # Browser orphan reaper.
    # ------------------------------------------------------------------ #

    async def _trigger_browser_reset(self) -> None:
        """Reap orphaned browser processes. Runs the /proc walk in a thread.

        Belt-and-suspenders to tini: if a browser fetch was cancelled before
        scrapling's session.close() ran, its child Chromium/Camoufox/Firefox
        processes get reparented to PID 1. tini reaps eventually; this actively
        frees RAM + PID slots now.
        """
        if self._reaper_lock.locked():
            logger.debug("Browser reaper already running; skipping concurrent invocation")
            return
        async with self._reaper_lock:
            try:
                await asyncio.to_thread(self._reap_orphan_browsers_sync)
            except Exception as e:
                logger.warning(
                    f"Browser reset reaper failed (non-Linux? readonly cgroup?): {e}"
                )

    @staticmethod
    def _reap_orphan_browsers_sync() -> None:
        """Sync /proc walk. Runs in a thread via `_trigger_browser_reset`."""
        proc_root = Path("/proc")
        if not proc_root.exists():
            logger.debug("Browser reset skipped: /proc not available (not Linux)")
            return

        try:
            pid1_comm = (proc_root / "1" / "comm").read_text().strip()
        except (OSError, IndexError):
            logger.warning("Browser reset aborted: could not read /proc/1/comm")
            return
        if pid1_comm not in _SAFE_INIT_PROCESSES:
            logger.error(
                f"Browser reset aborted: PID 1 is {pid1_comm!r}, not an init "
                "process. Reaping would risk killing live browsers. Verify "
                "`init: true` in docker-compose and tini is installed."
            )
            return

        try:
            uptime = float((proc_root / "uptime").read_text().split()[0])
            clk_tck = os.sysconf("SC_CLK_TCK")
        except (OSError, ValueError) as e:
            logger.warning(f"Browser reset: could not read uptime/CLK_TCK: {e}")
            return

        killed = 0
        for pid_dir in proc_root.iterdir():
            if not pid_dir.name.isdigit():
                continue
            try:
                # /proc/[pid]/stat: "pid (comm) state ppid ... starttime ...".
                # comm is inside parens and can contain spaces, so rsplit(')', 1).
                stat_fields = (pid_dir / "stat").read_text().rsplit(")", 1)
                comm = stat_fields[0].split("(", 1)[1]
                rest = stat_fields[1].split()
                ppid = int(rest[_STAT_PPID_IDX])
                start_ticks = int(rest[_STAT_STARTTIME_IDX])
                if ppid != _INIT_PID or not comm.startswith(_BROWSER_COMM_PREFIXES):
                    continue
                age_s = uptime - (start_ticks / clk_tck)
                if age_s < _FORK_EXEC_GRACE_SECONDS:
                    continue
                os.kill(int(pid_dir.name), signal.SIGKILL)
                killed += 1
            except (ProcessLookupError, FileNotFoundError, PermissionError, IndexError, ValueError):
                continue

        if killed > 0:
            logger.warning(
                f"Circuit reset reaped {killed} orphaned browser processes"
            )

    # ------------------------------------------------------------------ #
    # Main crawl entry point.
    # ------------------------------------------------------------------ #

    async def crawl(
        self,
        url: str,
        timeout: Optional[float] = None,
    ) -> CrawlResult:
        """Crawl a URL with all protections. Never raises — always returns CrawlResult."""
        timeout = timeout or self._default_timeout
        netloc = (urlparse(url).netloc or "").lower()

        # Step 1: pre-call lock window. Cache check, breaker fetch, queue
        # admission, reap gate. All cheap, all under one lock acquisition.
        # Crucially, we do NOT call the actual crawler under this lock.
        async with self._lock:
            # Blocked-cache hit: instant fail-fast.
            if netloc and self._is_blocked_locked(netloc):
                return CrawlResult(
                    success=False,
                    error_type="blocked",
                    error=(
                        f"Site blocks automated access ({netloc}). "
                        "Retrying will not help — try an alternative source."
                    ),
                )
            # Get/create per-host breaker (also LRU-evicts).
            host_breaker = self._get_or_create_host_breaker_locked(netloc) if netloc else None
            # Opportunistic reap gate (atomic check-and-set).
            should_reap = self._should_reap_locked()
            # Queue admission.
            if self._queue_count >= self._max_queue:
                return CrawlResult(
                    success=False,
                    error="Crawler queue at capacity",
                    error_type="queue_full",
                )
            self._queue_count += 1

        if should_reap:
            asyncio.create_task(self._trigger_browser_reset())

        try:
            # Step 2: breaker state checks. Per-breaker locks; safe outside self._lock.
            if host_breaker is not None:
                await host_breaker.check_state()
                if host_breaker.is_open():
                    return CrawlResult(
                        success=False,
                        error=f"Crawler temporarily unavailable for {netloc} (host breaker open)",
                        error_type="circuit_open",
                    )
            await self._infra_breaker.check_state()
            if self._infra_breaker.is_open():
                return CrawlResult(
                    success=False,
                    error="Crawler temporarily unavailable (infrastructure breaker open)",
                    error_type="circuit_open",
                )

            # Step 3: run the crawl.
            try:
                crawler = await self._get_crawler()
                output = await asyncio.wait_for(
                    crawler.crawl_with_metadata(url),
                    timeout=timeout,
                )
                return await self._classify_output(output, host_breaker, netloc)

            except asyncio.TimeoutError:
                # Host-level timeout → host breaker only.
                if host_breaker is not None:
                    await host_breaker.record_failure(self._trigger_browser_reset)
                return CrawlResult(
                    success=False,
                    error=f"Crawl timed out after {timeout}s",
                    error_type="timeout",
                )
            except asyncio.CancelledError:
                # Don't count cancellation as failure.
                return CrawlResult(
                    success=False,
                    error="Crawl was cancelled",
                    error_type="cancelled",
                )
            except Exception as e:
                return await self._classify_exception(e, host_breaker)

        finally:
            async with self._lock:
                self._queue_count -= 1

    async def _classify_output(
        self,
        output,
        host_breaker: Optional[CrawlerCircuitBreaker],
        netloc: str,
    ) -> CrawlResult:
        """Map CrawlOutput.failure_kind to CrawlResult and update appropriate health layer."""
        kind = output.failure_kind

        if kind == "blocked":
            # Host rejection. Cache only after _BLOCK_CACHE_THRESHOLD consecutive
            # blocks to avoid one paywalled URL poisoning the entire host's
            # cache (NYT homepage works, /article 401s — don't block both).
            # Never trip any breaker on blocks.
            if netloc:
                async with self._lock:
                    count = self._record_block_attempt_locked(netloc)
                    if count >= _BLOCK_CACHE_THRESHOLD:
                        self._set_blocked_locked(netloc)
                        # Reset counter — cache is the durable record now.
                        self._clear_block_attempts_locked(netloc)
            status_str = f"HTTP {output.status}" if output.status else "blocked"
            return CrawlResult(
                success=False,
                error=(
                    f"Site blocks automated access ({status_str}). "
                    "Retrying will not help — try an alternative source."
                ),
                error_type="blocked",
                markdown=output.markdown or "",
                title=output.title or "",
            )

        if kind == "infra_error":
            # Cross-cutting failure → both breakers.
            if host_breaker is not None:
                await host_breaker.record_failure(self._trigger_browser_reset)
            await self._infra_breaker.record_failure(self._trigger_browser_reset)
            return CrawlResult(
                success=False,
                error="Crawler infrastructure error",
                error_type="infra_error",
            )

        if kind in ("rate_limited", "stealth_failed"):
            # Host-scoped failure → host breaker only.
            if host_breaker is not None:
                await host_breaker.record_failure(self._trigger_browser_reset)
            return CrawlResult(
                success=False,
                markdown=output.markdown or "",
                title=output.title or "",
                error=f"Site returned {kind}",
                error_type=kind,
            )

        # No failure_kind set: legacy/successful path. Still guard against the
        # silent-empty case (empty markdown without any classification — rare,
        # but catches old extractors that don't set failure_kind).
        if not output.markdown or len(output.markdown.strip()) < 10:
            if host_breaker is not None:
                await host_breaker.record_failure(self._trigger_browser_reset)
            return CrawlResult(
                success=False,
                markdown=output.markdown or "",
                title=output.title or "",
                error="Page returned empty content",
                error_type="empty_content",
            )

        # Success — reset host breaker, infra breaker, and any pending
        # block-attempt counter for this host. Without record_success on the
        # infra breaker, a HALF_OPEN breaker can never transition back to
        # CLOSED, and recovery_timeout ratchets upward on every transient blip
        # until it caps at 15 minutes.
        if host_breaker is not None:
            await host_breaker.record_success()
        await self._infra_breaker.record_success()
        if netloc and netloc in self._block_attempts:
            async with self._lock:
                self._clear_block_attempts_locked(netloc)
        return CrawlResult(
            success=True,
            markdown=output.markdown,
            title=output.title,
        )

    async def _classify_exception(
        self,
        e: Exception,
        host_breaker: Optional[CrawlerCircuitBreaker],
    ) -> CrawlResult:
        """Classify an exception, update appropriate breaker(s), return CrawlResult."""
        error_str = str(e)

        if "has been closed" in error_str or "Target page" in error_str:
            error_type = "browser_closed"
        elif "ERR_NAME_NOT_RESOLVED" in error_str:
            error_type = "dns_error"
        elif "ERR_CONNECTION_REFUSED" in error_str:
            error_type = "connection_refused"
        elif "ERR_CONNECTION_TIMED_OUT" in error_str:
            error_type = "connection_timeout"
        elif "net::" in error_str:
            error_type = "network_error"
        else:
            error_type = "crawl_error"

        if host_breaker is not None:
            await host_breaker.record_failure(self._trigger_browser_reset)
        if error_type in _INFRA_ERROR_TYPES:
            await self._infra_breaker.record_failure(self._trigger_browser_reset)

        return CrawlResult(
            success=False,
            error=error_str[:200],
            error_type=error_type,
        )

    def get_status(self) -> dict:
        """Wrapper status for monitoring."""
        return {
            "infra_circuit_state": self._infra_breaker.state.value,
            "infra_failure_count": self._infra_breaker.failure_count,
            "host_breaker_count": len(self._host_breakers),
            "blocked_host_count": len(self._blocked_hosts),
            "queue_count": self._queue_count,
            "max_queue": self._max_queue,
            "infra_last_failure_time": self._infra_breaker.last_failure_time,
        }

    def is_healthy(self) -> bool:
        """Healthy iff infra breaker is closed. Per-host breakers being open
        is normal and does not mean the crawler itself is unhealthy."""
        return not self._infra_breaker.is_open()


# Global singleton instance
_safe_wrapper: Optional[SafeCrawlerWrapper] = None
_wrapper_lock = asyncio.Lock()


def _build_configured_wrapper() -> SafeCrawlerWrapper:
    """Build a SafeCrawlerWrapper from agent_config settings."""
    try:
        from src.config.tool_settings import (
            get_crawler_browser_concurrency,
            get_crawler_circuit_failure_threshold,
            get_crawler_circuit_recovery_timeout,
            get_crawler_circuit_success_threshold,
            get_crawler_http_concurrency,
            get_crawler_page_timeout,
            get_crawler_queue_max_size,
            get_crawler_backend,
        )

        return SafeCrawlerWrapper(
            default_timeout=get_crawler_page_timeout() / 1000,
            max_queue_size=get_crawler_queue_max_size(),
            circuit_failure_threshold=get_crawler_circuit_failure_threshold(),
            circuit_recovery_timeout=get_crawler_circuit_recovery_timeout(),
            circuit_success_threshold=get_crawler_circuit_success_threshold(),
            backend=get_crawler_backend(),
            http_concurrency=get_crawler_http_concurrency(),
            browser_concurrency=get_crawler_browser_concurrency(),
        )
    except Exception as e:
        logger.warning(f"Failed to load crawler config, using defaults: {e}")
        return SafeCrawlerWrapper()


async def get_safe_crawler() -> SafeCrawlerWrapper:
    """Get or create safe crawler wrapper singleton."""
    global _safe_wrapper

    if _safe_wrapper is not None:
        return _safe_wrapper

    async with _wrapper_lock:
        if _safe_wrapper is not None:
            return _safe_wrapper

        _safe_wrapper = _build_configured_wrapper()
        return _safe_wrapper


def get_safe_crawler_sync() -> SafeCrawlerWrapper:
    """Synchronous version of get_safe_crawler for non-async contexts."""
    global _safe_wrapper

    if _safe_wrapper is None:
        _safe_wrapper = _build_configured_wrapper()

    return _safe_wrapper
