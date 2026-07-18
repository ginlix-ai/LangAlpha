"""Circuit breaker shared by the fetch chain and the in-house crawler.

The fetch router keeps one breaker per provider; SafeCrawlerWrapper reuses the
same class for its per-host and global-infra layers.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker. Used per fetch provider and for the in-house
    crawler's per-host and global infra layers."""

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

    async def record_failure(self, on_open: Optional[Callable] = None) -> None:
        """Count a failure; fire ``on_open`` (async, detached) if this one
        opens the circuit."""
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

            if should_open and on_open:
                logger.info("Running circuit-open callback")
                asyncio.create_task(on_open())

    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN
