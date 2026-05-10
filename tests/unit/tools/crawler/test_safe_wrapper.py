"""Unit tests for SafeCrawlerWrapper — three-layer health model + classification."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.tools.crawler.backend import CrawlOutput
from src.tools.crawler.safe_wrapper import (
    CircuitState,
    CrawlerCircuitBreaker,
    SafeCrawlerWrapper,
    _build_configured_wrapper,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_wrapper(**kwargs) -> SafeCrawlerWrapper:
    """Create a SafeCrawlerWrapper with safe defaults for tests."""
    defaults = dict(
        max_queue_size=10,
        default_timeout=5.0,
        circuit_failure_threshold=3,
        circuit_recovery_timeout=60.0,
        circuit_success_threshold=2,
        http_concurrency=20,
        browser_concurrency=6,
    )
    defaults.update(kwargs)
    return SafeCrawlerWrapper(**defaults)


def _inject_mock_crawler(wrapper: SafeCrawlerWrapper) -> AsyncMock:
    """Inject a mock crawler so _get_crawler() returns it without real imports."""
    mock_crawler = AsyncMock()
    wrapper._crawler = mock_crawler
    return mock_crawler


# ---------------------------------------------------------------------------
# Wrapper-level fault tolerance (kept from previous suite, adapted to new API)
# ---------------------------------------------------------------------------


class TestSafeCrawlerCrawl:
    @pytest.mark.asyncio
    async def test_infra_breaker_open_returns_circuit_open(self):
        wrapper = _make_wrapper()
        wrapper._infra_breaker.state = CircuitState.OPEN
        wrapper._infra_breaker.last_failure_time = time.time()

        result = await wrapper.crawl("https://example.com")

        assert result.success is False
        assert result.error_type == "circuit_open"

    @pytest.mark.asyncio
    async def test_queue_full_returns_error(self):
        wrapper = _make_wrapper(max_queue_size=2)
        _inject_mock_crawler(wrapper)
        wrapper._queue_count = 2

        result = await wrapper.crawl("https://example.com")

        assert result.success is False
        assert result.error_type == "queue_full"

    @pytest.mark.asyncio
    async def test_crawl_timeout_returns_timeout_error(self):
        wrapper = _make_wrapper(default_timeout=0.05)
        mock_crawler = _inject_mock_crawler(wrapper)

        async def slow_crawl(url):
            await asyncio.sleep(10)

        mock_crawler.crawl_with_metadata = slow_crawl

        result = await wrapper.crawl("https://example.com")

        assert result.success is False
        assert result.error_type == "timeout"

    @pytest.mark.asyncio
    async def test_crawl_timeout_trips_host_breaker_only(self):
        wrapper = _make_wrapper(default_timeout=0.05, circuit_failure_threshold=1)
        mock_crawler = _inject_mock_crawler(wrapper)

        async def slow_crawl(url):
            await asyncio.sleep(10)

        mock_crawler.crawl_with_metadata = slow_crawl

        await wrapper.crawl("https://example.com")
        host_breaker = wrapper._host_breakers["example.com"]
        assert host_breaker.state == CircuitState.OPEN
        # Infra breaker should remain CLOSED — timeout is host-scoped.
        assert wrapper._infra_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_cancelled_returns_cancelled_no_failure(self):
        wrapper = _make_wrapper(circuit_failure_threshold=1)
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(side_effect=asyncio.CancelledError())

        result = await wrapper.crawl("https://example.com")

        assert result.error_type == "cancelled"
        # Host breaker may not even have been created since we returned early,
        # but infra breaker definitely shouldn't trip.
        assert wrapper._infra_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_dns_error_classifies_correctly(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            side_effect=Exception("net::ERR_NAME_NOT_RESOLVED at https://bogus.invalid")
        )

        result = await wrapper.crawl("https://bogus.invalid")

        assert result.error_type == "dns_error"
        # DNS errors trip BOTH per-host AND infra breakers.
        assert wrapper._host_breakers["bogus.invalid"].failure_count == 1
        assert wrapper._infra_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_browser_closed_classifies_correctly(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            side_effect=Exception("Browser has been closed unexpectedly")
        )

        result = await wrapper.crawl("https://example.com")

        assert result.error_type == "browser_closed"
        # browser_closed is infra → trips both.
        assert wrapper._infra_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_connection_refused_trips_infra(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            side_effect=Exception("net::ERR_CONNECTION_REFUSED")
        )

        result = await wrapper.crawl("https://example.com")

        assert result.error_type == "connection_refused"
        assert wrapper._infra_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_generic_crawl_error_host_only(self):
        """Unclassified exceptions are host-scoped, not infra."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            side_effect=RuntimeError("Something completely unexpected")
        )

        result = await wrapper.crawl("https://example.com")

        assert result.error_type == "crawl_error"
        # Host breaker tripped, infra unchanged.
        assert wrapper._host_breakers["example.com"].failure_count == 1
        assert wrapper._infra_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_long_error_message_truncated(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(side_effect=RuntimeError("X" * 500))

        result = await wrapper.crawl("https://example.com")

        assert len(result.error) == 200

    @pytest.mark.asyncio
    async def test_successful_crawl(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(
                title="Example", html="<p>X</p>", markdown="# Example\n\nContent",
                status=200,
            )
        )

        result = await wrapper.crawl("https://example.com")

        assert result.success is True
        assert result.title == "Example"
        assert result.error_type is None

    @pytest.mark.asyncio
    async def test_successful_crawl_resets_host_failures(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="real content here",
                                     status=200)
        )
        # First call to populate host_breaker.
        await wrapper.crawl("https://example.com")
        wrapper._host_breakers["example.com"].failure_count = 2
        # Second call should reset.
        await wrapper.crawl("https://example.com")
        assert wrapper._host_breakers["example.com"].failure_count == 0

    @pytest.mark.asyncio
    async def test_empty_legacy_output_trips_host_only(self):
        """Empty markdown without failure_kind (legacy path) trips host breaker."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="")
        )

        result = await wrapper.crawl("https://example.com")

        assert result.success is False
        assert result.error_type == "empty_content"
        assert wrapper._host_breakers["example.com"].failure_count == 1
        assert wrapper._infra_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_queue_count_decremented_on_success(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="ok content here",
                                     status=200)
        )
        await wrapper.crawl("https://example.com")
        assert wrapper._queue_count == 0

    @pytest.mark.asyncio
    async def test_queue_count_decremented_on_error(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(side_effect=RuntimeError("boom"))
        await wrapper.crawl("https://example.com")
        assert wrapper._queue_count == 0

    @pytest.mark.asyncio
    async def test_timeout_override(self):
        wrapper = _make_wrapper(default_timeout=30.0)
        mock_crawler = _inject_mock_crawler(wrapper)

        async def slow_crawl(url):
            await asyncio.sleep(10)

        mock_crawler.crawl_with_metadata = slow_crawl

        result = await wrapper.crawl("https://example.com", timeout=0.05)

        assert result.error_type == "timeout"
        assert "0.05" in result.error


# ---------------------------------------------------------------------------
# Three-layer health: blocked-cache + per-host breaker + infra breaker
# ---------------------------------------------------------------------------


class TestThreeLayerHealth:
    @pytest.mark.asyncio
    async def test_one_block_does_not_cache_host(self):
        """A single blocked response counts but doesn't cache — paywalled URL
        on a mixed-access host (NYT homepage works, /article 401s) shouldn't
        poison the whole host. _BLOCK_CACHE_THRESHOLD=2 consecutive blocks needed."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=401, failure_kind="blocked")
        )

        result = await wrapper.crawl("https://nytimes.com/paywalled-article")

        assert result.error_type == "blocked"
        # Cache NOT populated yet.
        assert "nytimes.com" not in wrapper._blocked_hosts
        # Counter incremented.
        assert wrapper._block_attempts.get("nytimes.com") == 1
        # No breaker mutation.
        assert wrapper._host_breakers["nytimes.com"].failure_count == 0
        assert wrapper._infra_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_two_consecutive_blocks_cache_host(self):
        """After _BLOCK_CACHE_THRESHOLD consecutive blocks, host is cached."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=401, failure_kind="blocked")
        )

        await wrapper.crawl("https://reuters.com/markets/p1")
        await wrapper.crawl("https://reuters.com/markets/p2")

        assert "reuters.com" in wrapper._blocked_hosts
        # Counter reset after caching.
        assert "reuters.com" not in wrapper._block_attempts
        # Still no breaker mutation.
        assert wrapper._host_breakers["reuters.com"].failure_count == 0
        assert wrapper._infra_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_block_attempts_reset_on_success(self):
        """Success on a host clears the block-attempt counter — paywalled URL
        followed by working homepage should not promote to host-cache."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)

        async def fake_crawl(url):
            if "paywalled" in url:
                return CrawlOutput(title="", html="", markdown="",
                                   status=401, failure_kind="blocked")
            return CrawlOutput(title="OK", html="", markdown="real content here",
                               status=200)

        mock_crawler.crawl_with_metadata = fake_crawl

        await wrapper.crawl("https://nytimes.com/paywalled")
        assert wrapper._block_attempts.get("nytimes.com") == 1
        await wrapper.crawl("https://nytimes.com/")  # homepage works
        assert "nytimes.com" not in wrapper._block_attempts
        # Another blocked URL counts from 1, not 2.
        await wrapper.crawl("https://nytimes.com/paywalled2")
        assert wrapper._block_attempts.get("nytimes.com") == 1
        assert "nytimes.com" not in wrapper._blocked_hosts

    @pytest.mark.asyncio
    async def test_blocked_cache_hit_skips_fetch(self):
        """Repeat calls to a cached blocked host don't invoke the crawler at all."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=401, failure_kind="blocked")
        )

        # Prime cache — needs two consecutive blocks now.
        await wrapper.crawl("https://reuters.com/markets")
        await wrapper.crawl("https://reuters.com/markets")
        assert mock_crawler.crawl_with_metadata.await_count == 2
        assert "reuters.com" in wrapper._blocked_hosts

        # Third call should short-circuit.
        result3 = await wrapper.crawl("https://reuters.com/some-other-page")

        assert result3.error_type == "blocked"
        # crawler not invoked again.
        assert mock_crawler.crawl_with_metadata.await_count == 2

    @pytest.mark.asyncio
    async def test_blocked_cache_expiry_refetches(self):
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="real content",
                                     status=200)
        )

        # Manually plant an expired entry.
        wrapper._blocked_hosts["reuters.com"] = time.time() - 1

        result = await wrapper.crawl("https://reuters.com/foo")

        assert result.success is True
        assert "reuters.com" not in wrapper._blocked_hosts
        assert mock_crawler.crawl_with_metadata.await_count == 1

    @pytest.mark.asyncio
    async def test_blocked_cache_lru_evicts_oldest(self):
        from src.tools.crawler.safe_wrapper import _BLOCKED_LRU_CAP

        wrapper = _make_wrapper()
        # Populate cap entries.
        for i in range(_BLOCKED_LRU_CAP):
            wrapper._blocked_hosts[f"host{i}.example"] = time.time() + 900
        # Add one more under the lock.
        async with wrapper._lock:
            wrapper._set_blocked_locked("newhost.example")

        assert "host0.example" not in wrapper._blocked_hosts
        assert "newhost.example" in wrapper._blocked_hosts
        assert len(wrapper._blocked_hosts) == _BLOCKED_LRU_CAP

    @pytest.mark.asyncio
    async def test_per_host_breaker_isolation(self):
        """Reuters host breaker open does not affect Wikipedia."""
        wrapper = _make_wrapper(circuit_failure_threshold=1)
        mock_crawler = _inject_mock_crawler(wrapper)

        async def fake_crawl(url):
            if "reuters" in url:
                raise RuntimeError("boom")
            return CrawlOutput(title="OK", html="", markdown="real content here", status=200)

        mock_crawler.crawl_with_metadata = fake_crawl

        # Reuters fails → host breaker opens.
        await wrapper.crawl("https://reuters.com/foo")
        assert wrapper._host_breakers["reuters.com"].state == CircuitState.OPEN
        # Wikipedia should still succeed.
        result = await wrapper.crawl("https://en.wikipedia.org/wiki/Reuters")
        assert result.success is True

    @pytest.mark.asyncio
    async def test_infra_breaker_only_trips_on_infra_kind(self):
        """failure_kind='infra_error' trips both per-host and infra breakers."""
        wrapper = _make_wrapper(circuit_failure_threshold=1)
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=None, failure_kind="infra_error")
        )

        await wrapper.crawl("https://example.com")
        assert wrapper._host_breakers["example.com"].state == CircuitState.OPEN
        assert wrapper._infra_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_infra_breaker_open_blocks_all_hosts(self):
        wrapper = _make_wrapper()
        wrapper._infra_breaker.state = CircuitState.OPEN
        wrapper._infra_breaker.last_failure_time = time.time()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="ok ok ok ok ok",
                                     status=200)
        )

        result = await wrapper.crawl("https://wikipedia.org/wiki/Foo")

        assert result.error_type == "circuit_open"
        # Crawler should not have been invoked.
        assert mock_crawler.crawl_with_metadata.await_count == 0

    @pytest.mark.asyncio
    async def test_per_host_breaker_recovery(self):
        wrapper = _make_wrapper(
            circuit_failure_threshold=1,
            circuit_recovery_timeout=0.05,
            circuit_success_threshold=1,
        )
        mock_crawler = _inject_mock_crawler(wrapper)

        # First call: fail to open the breaker.
        mock_crawler.crawl_with_metadata = AsyncMock(side_effect=RuntimeError("boom"))
        await wrapper.crawl("https://example.com")
        breaker = wrapper._host_breakers["example.com"]
        assert breaker.state == CircuitState.OPEN

        # Advance past recovery timeout.
        breaker.last_failure_time = time.time() - 1.0

        # Second call: succeed → breaker closes.
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="recovered content here",
                                     status=200)
        )
        result = await wrapper.crawl("https://example.com")
        assert result.success is True
        assert breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_rate_limited_kind_trips_host_only(self):
        wrapper = _make_wrapper(circuit_failure_threshold=1)
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=429, failure_kind="rate_limited")
        )

        result = await wrapper.crawl("https://example.com")

        assert result.error_type == "rate_limited"
        assert wrapper._host_breakers["example.com"].state == CircuitState.OPEN
        assert wrapper._infra_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_stealth_failed_kind_trips_host_only(self):
        wrapper = _make_wrapper(circuit_failure_threshold=1)
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=200, failure_kind="stealth_failed")
        )

        result = await wrapper.crawl("https://example.com")

        assert result.error_type == "stealth_failed"
        assert wrapper._host_breakers["example.com"].state == CircuitState.OPEN
        assert wrapper._infra_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_infra_breaker_closes_after_recovery_success(self):
        """HALF_OPEN infra breaker must close on success — otherwise recovery_timeout
        ratchets upward forever on every transient blip until the 15-min cap.
        """
        wrapper = _make_wrapper(
            circuit_failure_threshold=1,
            circuit_recovery_timeout=0.05,
            circuit_success_threshold=1,
        )
        mock_crawler = _inject_mock_crawler(wrapper)

        # Trip infra breaker via an infra_error failure.
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=None, failure_kind="infra_error")
        )
        await wrapper.crawl("https://example.com")
        infra = wrapper._infra_breaker
        assert infra.state == CircuitState.OPEN
        base_timeout = infra.recovery_timeout

        # Advance past recovery → HALF_OPEN on next check.
        infra.last_failure_time = time.time() - 1.0

        # Successful crawl from a different host must close infra and reset
        # the escalating recovery_timeout.
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="recovered ok ok ok",
                                     status=200)
        )
        result = await wrapper.crawl("https://other.example/page")
        assert result.success is True
        assert infra.state == CircuitState.CLOSED
        assert infra.failure_count == 0
        assert infra.recovery_timeout == base_timeout
        assert infra._consecutive_opens == 0

    @pytest.mark.asyncio
    async def test_concurrent_host_breaker_creation_no_race(self):
        """50 concurrent calls to same novel host create exactly one breaker."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="content here ok ok",
                                     status=200)
        )

        await asyncio.gather(*(
            wrapper.crawl(f"https://example.com/page{i}") for i in range(50)
        ))

        # Only one breaker for example.com.
        assert sum(1 for k in wrapper._host_breakers if k == "example.com") == 1
        assert wrapper._host_breakers["example.com"].failure_count == 0

    @pytest.mark.asyncio
    async def test_host_breakers_lru_evicts_oldest(self):
        """Parallel structure to blocked-cache LRU — host breaker dict is bounded."""
        from src.tools.crawler.safe_wrapper import (
            _HOST_BREAKERS_LRU_CAP,
            CrawlerCircuitBreaker,
        )

        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="ok ok ok ok ok",
                                     status=200)
        )
        # Prefill at cap.
        for i in range(_HOST_BREAKERS_LRU_CAP):
            wrapper._host_breakers[f"host{i}.example"] = CrawlerCircuitBreaker()

        # Crawl a novel host — should evict the oldest entry.
        await wrapper.crawl("https://newhost.example/page")

        assert "host0.example" not in wrapper._host_breakers
        assert "newhost.example" in wrapper._host_breakers
        assert len(wrapper._host_breakers) == _HOST_BREAKERS_LRU_CAP

    @pytest.mark.asyncio
    async def test_concurrent_blocked_does_not_poison_other_hosts(self):
        """Regression for the production incident: 5 concurrent Reuters (blocked) +
        1 concurrent Wikipedia (success). Reuters all return blocked, Wikipedia
        succeeds, infra breaker stays closed, no cross-host poisoning."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)

        async def fake_crawl(url):
            if "reuters" in url:
                return CrawlOutput(title="", html="", markdown="",
                                   status=401, failure_kind="blocked")
            return CrawlOutput(title="Wiki", html="",
                               markdown="real wikipedia content here ok",
                               status=200)

        mock_crawler.crawl_with_metadata = fake_crawl

        coros = [wrapper.crawl(f"https://reuters.com/p{i}") for i in range(5)]
        coros.append(wrapper.crawl("https://en.wikipedia.org/wiki/Reuters"))
        results = await asyncio.gather(*coros)

        reuters_results, wiki_result = results[:5], results[5]
        # All Reuters blocked.
        assert all(r.error_type == "blocked" for r in reuters_results)
        # Wikipedia unaffected.
        assert wiki_result.success is True
        assert "wikipedia" in wiki_result.markdown.lower()
        # Infra breaker uncontaminated.
        assert wrapper._infra_breaker.failure_count == 0
        assert wrapper._infra_breaker.state == CircuitState.CLOSED
        # Reuters host breaker also untouched (blocks don't trip breakers).
        assert wrapper._host_breakers["reuters.com"].failure_count == 0
        # After 2+ blocks, reuters.com is cached.
        assert "reuters.com" in wrapper._blocked_hosts


# ---------------------------------------------------------------------------
# Opportunistic reaper
# ---------------------------------------------------------------------------


class TestOpportunisticReaper:
    @pytest.mark.asyncio
    async def test_reap_throttled_to_one_per_interval(self):
        """Multiple crawls within reap interval schedule reap exactly once."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="ok " * 20, status=200)
        )

        reap_count = 0

        async def fake_reap():
            nonlocal reap_count
            reap_count += 1

        wrapper._trigger_browser_reset = fake_reap

        # Fire 5 calls in quick succession.
        await asyncio.gather(*(
            wrapper.crawl(f"https://example.com/p{i}") for i in range(5)
        ))
        # Allow scheduled tasks to run.
        await asyncio.sleep(0.02)

        assert reap_count == 1

    @pytest.mark.asyncio
    async def test_reap_runs_again_after_interval(self):
        from src.tools.crawler.safe_wrapper import _REAPER_INTERVAL_SECONDS

        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="OK", html="", markdown="content here ok",
                                     status=200)
        )

        reap_count = 0

        async def fake_reap():
            nonlocal reap_count
            reap_count += 1

        wrapper._trigger_browser_reset = fake_reap

        await wrapper.crawl("https://example.com/a")
        await asyncio.sleep(0.02)
        # Force interval to have elapsed.
        wrapper._last_reap_time = time.time() - _REAPER_INTERVAL_SECONDS - 1
        await wrapper.crawl("https://example.com/b")
        await asyncio.sleep(0.02)

        assert reap_count == 2


# ---------------------------------------------------------------------------
# Caller-facing error string formatting
# ---------------------------------------------------------------------------


class TestErrorStringPrefix:
    @pytest.mark.asyncio
    async def test_blocked_error_string_helpful(self):
        """Blocked CrawlResult.error tells the LLM not to retry."""
        wrapper = _make_wrapper()
        mock_crawler = _inject_mock_crawler(wrapper)
        mock_crawler.crawl_with_metadata = AsyncMock(
            return_value=CrawlOutput(title="", html="", markdown="",
                                     status=401, failure_kind="blocked")
        )

        result = await wrapper.crawl("https://reuters.com/markets")

        assert result.error_type == "blocked"
        assert "blocks automated access" in result.error.lower()
        assert "retrying will not help" in result.error.lower()


# ---------------------------------------------------------------------------
# Circuit breaker state transitions (unchanged class — kept tests verbatim)
# ---------------------------------------------------------------------------


class TestCircuitBreakerTransitions:
    @pytest.mark.asyncio
    async def test_closed_to_open_after_threshold_failures(self):
        cb = CrawlerCircuitBreaker(failure_threshold=3, recovery_timeout=60.0)
        for _ in range(3):
            await cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 3

    @pytest.mark.asyncio
    async def test_open_to_half_open_after_recovery_timeout(self):
        cb = CrawlerCircuitBreaker(failure_threshold=1, recovery_timeout=0.05)
        await cb.record_failure()
        cb.last_failure_time = time.time() - 1.0
        await cb.check_state()
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_to_closed_after_success_threshold(self):
        cb = CrawlerCircuitBreaker(
            failure_threshold=1, recovery_timeout=0.05, success_threshold=2,
        )
        await cb.record_failure()
        cb.last_failure_time = time.time() - 1.0
        await cb.check_state()
        await cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN
        await cb.record_success()
        assert cb.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_to_open_on_failure(self):
        cb = CrawlerCircuitBreaker(
            failure_threshold=1, recovery_timeout=10.0, success_threshold=2,
        )
        cb.state = CircuitState.OPEN
        cb.last_failure_time = time.time() - 20.0
        await cb.check_state()
        await cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb._consecutive_opens == 1

    @pytest.mark.asyncio
    async def test_exponential_backoff_capped(self):
        cb = CrawlerCircuitBreaker(
            failure_threshold=1, recovery_timeout=500.0, success_threshold=2,
        )
        cb._consecutive_opens = 10
        cb.state = CircuitState.HALF_OPEN
        await cb.record_failure()
        assert cb.recovery_timeout == 900.0

    @pytest.mark.asyncio
    async def test_record_failure_triggers_reset_callback(self):
        cb = CrawlerCircuitBreaker(failure_threshold=1)
        reset_called = asyncio.Event()

        async def mock_reset():
            reset_called.set()

        await cb.record_failure(trigger_reset=mock_reset)
        await asyncio.sleep(0.01)
        assert reset_called.is_set()


# ---------------------------------------------------------------------------
# _build_configured_wrapper
# ---------------------------------------------------------------------------


class TestBuildConfiguredWrapper:
    def test_happy_path(self):
        with patch(
            "src.config.tool_settings.get_crawler_http_concurrency",
            return_value=12,
        ), patch(
            "src.config.tool_settings.get_crawler_browser_concurrency",
            return_value=4,
        ), patch(
            "src.config.tool_settings.get_crawler_page_timeout",
            return_value=30000,
        ), patch(
            "src.config.tool_settings.get_crawler_queue_max_size",
            return_value=50,
        ), patch(
            "src.config.tool_settings.get_crawler_circuit_failure_threshold",
            return_value=4,
        ), patch(
            "src.config.tool_settings.get_crawler_circuit_recovery_timeout",
            return_value=120.0,
        ), patch(
            "src.config.tool_settings.get_crawler_circuit_success_threshold",
            return_value=3,
        ), patch(
            "src.config.tool_settings.get_crawler_backend",
            return_value="scrapling",
        ):
            wrapper = _build_configured_wrapper()

        assert wrapper._max_queue == 50
        assert wrapper._default_timeout == 30.0
        assert wrapper._http_concurrency == 12
        assert wrapper._browser_concurrency == 4
        assert wrapper._infra_breaker.failure_threshold == 4
        assert wrapper._backend == "scrapling"

    def test_fallback_on_import_error(self):
        with patch.dict("sys.modules", {"src.config.tool_settings": None}):
            wrapper = _build_configured_wrapper()
        assert wrapper._max_queue == 100
        assert wrapper._default_timeout == 60.0
        assert wrapper._http_concurrency == 20
        assert wrapper._browser_concurrency == 6


# ---------------------------------------------------------------------------
# get_status / is_healthy
# ---------------------------------------------------------------------------


class TestWrapperStatus:
    def test_get_status_initial(self):
        wrapper = _make_wrapper()
        status = wrapper.get_status()
        assert status["infra_circuit_state"] == "closed"
        assert status["infra_failure_count"] == 0
        assert status["host_breaker_count"] == 0
        assert status["blocked_host_count"] == 0
        assert status["queue_count"] == 0
        assert status["max_queue"] == 10

    def test_is_healthy_when_infra_closed(self):
        wrapper = _make_wrapper()
        assert wrapper.is_healthy() is True

    def test_is_healthy_when_infra_open(self):
        wrapper = _make_wrapper()
        wrapper._infra_breaker.state = CircuitState.OPEN
        assert wrapper.is_healthy() is False

    def test_is_healthy_indifferent_to_host_breakers(self):
        """Per-host breaker open is not 'unhealthy' — that's expected operation."""
        wrapper = _make_wrapper()
        wrapper._host_breakers["reuters.com"] = CrawlerCircuitBreaker()
        wrapper._host_breakers["reuters.com"].state = CircuitState.OPEN
        assert wrapper.is_healthy() is True
