"""Circuit breaker shared by the fetch chain and the in-house crawler.

The fetch router keeps one breaker per provider; SafeCrawlerWrapper reuses the
same class for its per-host and global-infra layers.
"""

import asyncio
import logging
import time
from collections import Counter, deque
from enum import Enum
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_MAX_REASON_CHARS = 200


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker. Used per fetch provider and for the in-house
    crawler's per-host and global infra layers.

    ``name`` is carried into every log line: several layers share this class
    and each worker has its own instances, so an unlabelled transition cannot
    be attributed to a layer — let alone to one breaker — in a merged log.
    """

    def __init__(
        self,
        name: str = "unnamed",
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
    ):
        self.name = name
        # Why the current failure streak happened. Held rather than logged per
        # failure: routine failures are noise, but the transition they cause
        # is invisible without them.
        self._failure_reasons: deque[str] = deque(maxlen=failure_threshold)
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
        # Strong refs to detached on-open callbacks: asyncio keeps only weak
        # refs to tasks, so without this the fire-and-forget callback can be
        # collected before it finishes.
        self._open_callbacks: set[asyncio.Task] = set()

    async def check_state(self) -> None:
        """Check and potentially transition state based on time elapsed."""
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if self.last_failure_time and \
                   time.time() - self.last_failure_time > self.recovery_timeout:
                    logger.info(f"Circuit breaker [{self.name}] transitioning to half-open")
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0

    async def record_success(self) -> None:
        async with self._lock:
            self.failure_count = 0
            self._failure_reasons.clear()
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    logger.info(f"Circuit breaker [{self.name}] closing after recovery")
                    self.state = CircuitState.CLOSED
                    self._consecutive_opens = 0
                    self.recovery_timeout = self._base_recovery_timeout

    async def record_failure(
        self, on_open: Optional[Callable] = None, reason: Optional[str] = None
    ) -> None:
        """Count a failure; fire ``on_open`` (async, detached) if this one
        opens the circuit. ``reason`` surfaces in the open/re-open log line."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if reason:
                self._failure_reasons.append(reason)
            should_open = False

            if self.state == CircuitState.HALF_OPEN:
                self._consecutive_opens += 1
                self.recovery_timeout = min(
                    self._base_recovery_timeout * (2 ** self._consecutive_opens),
                    self._max_recovery_timeout,
                )
                logger.warning(
                    f"Circuit breaker [{self.name}] re-opening after half-open failure "
                    f"(consecutive_opens={self._consecutive_opens}, "
                    f"next_recovery={self.recovery_timeout}s){self._reasons_suffix()}"
                )
                self.state = CircuitState.OPEN
                should_open = True
            # Once per transition INTO open, never per failure. An already-open
            # breaker still collects failures from calls that were in flight
            # when it tripped; re-firing on_open for each would repeat the
            # recovery work (a browser reset, for one) and log the same
            # transition several times. A re-open out of HALF_OPEN above does
            # fire — the probe failed, so the remedy is owed another run.
            elif self.state is CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
                logger.warning(
                    f"Circuit breaker [{self.name}] opening after "
                    f"{self.failure_count} failures{self._reasons_suffix()}"
                )
                self.state = CircuitState.OPEN
                should_open = True

            if should_open and on_open:
                logger.info(f"Circuit breaker [{self.name}] running circuit-open callback")
                task = asyncio.create_task(on_open())
                self._open_callbacks.add(task)
                task.add_done_callback(self._open_callbacks.discard)

    def _reasons_suffix(self) -> str:
        """Streak causes, repeats collapsed — a breaker usually opens on five
        copies of one error, and printing it five times is noise, not detail."""
        if not self._failure_reasons:
            return ""
        counts = Counter(self._failure_reasons)
        parts = [
            (reason if n == 1 else f"{n}x {reason}")[:_MAX_REASON_CHARS]
            for reason, n in counts.items()
        ]
        return ": " + " | ".join(parts)

    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN
