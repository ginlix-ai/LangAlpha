"""Generic async retry utility with exponential backoff."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Any

import structlog

from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError

logger = structlog.get_logger(__name__)


class RetryPolicy(str, Enum):
    """Retry behaviour for sandbox operations."""

    SAFE = "safe"  # Idempotent operations — auto-retry with backoff
    UNSAFE = "unsafe"  # Mutating operations — raise on first transient error


async def async_retry_with_backoff(
    func: Callable[..., Any],
    *args: Any,
    retry_policy: RetryPolicy,
    is_transient: Callable[[Exception], bool],
    on_transient: Callable[[], Awaitable[None]] | None = None,
    rebind: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    retries: int = 5,
    initial_delay_s: float = 0.25,
    total_timeout: float = 120.0,
    **kwargs: Any,
) -> Any:
    """Retry *func* with exponential backoff on transient errors.

    Args:
        func: Async callable to invoke.
        *args: Positional arguments forwarded to *func*.
        retry_policy: SAFE retries automatically; UNSAFE raises immediately.
        is_transient: Predicate — return True if the exception is transient.
        on_transient: Optional async callback invoked once after the first
            transient error (e.g. to trigger a reconnect).
        rebind: Optional hook applied to *func* after ``on_transient`` runs. A
            reconnect can replace the object *func* was bound to, leaving the
            remaining attempts pointed at a dead receiver; the hook re-resolves it.
        retries: Maximum number of attempts.
        initial_delay_s: Backoff seed (doubles each attempt).
        total_timeout: Hard wall-clock deadline in seconds.
        **kwargs: Keyword arguments forwarded to *func*.

    Returns:
        The return value of *func* on success.

    Raises:
        SandboxGoneError: When the reconnect callback determines the sandbox no
            longer exists — propagated so the caller can recover rather than
            burning every remaining attempt against a sandbox that is not there.
        SandboxTransientError: When retries or timeout are exhausted, or when
            an UNSAFE policy encounters a transient error.
    """
    deadline = time.monotonic() + total_timeout
    delay_s = initial_delay_s
    on_transient_called = False

    for attempt in range(1, retries + 1):
        if time.monotonic() > deadline:
            raise SandboxTransientError(
                f"Operation timed out after {total_timeout}s"
            )

        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if not is_transient(e):
                raise

            # Fire the on_transient callback at most once.
            if on_transient is not None and not on_transient_called:
                try:
                    await on_transient()
                    on_transient_called = True
                except SandboxGoneError:
                    # The reconnect proved the sandbox is gone — a verdict, not a
                    # callback failure. Swallowing it costs four more doomed
                    # attempts and hands the caller a generic transient, so the
                    # recovery path that recreates the sandbox never runs.
                    raise
                except Exception as cb_err:
                    logger.debug(
                        "on_transient callback failed",
                        error=str(cb_err),
                    )
                else:
                    if rebind is not None:
                        func = rebind(func)

            if retry_policy == RetryPolicy.UNSAFE:
                message = (
                    "Sandbox disconnected during unsafe operation; not retrying automatically"
                )
                logger.warning(
                    message,
                    func=getattr(func, "__name__", str(func)),
                    attempt=attempt,
                    error=str(e),
                )
                raise SandboxTransientError(message) from e

            if attempt == retries:
                raise SandboxTransientError(
                    "Transient sandbox transport error; operation failed after retries"
                ) from e

            logger.debug(
                "Retrying after transient error",
                func=getattr(func, "__name__", str(func)),
                attempt=attempt,
                error=str(e),
            )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SandboxTransientError(
                    f"Operation timed out after {total_timeout}s"
                ) from e
            await asyncio.sleep(min(delay_s, remaining))
            delay_s *= 2

    # Should be unreachable, but guard defensively.
    raise SandboxTransientError("Transient sandbox transport error")
