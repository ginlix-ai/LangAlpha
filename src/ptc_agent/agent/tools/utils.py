"""Shared utilities for agent tools."""

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import structlog

from ptc_agent.core.sandbox._shared import SANDBOX_TIMEOUT
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxTransientError
from ptc_agent.agent.backends.composite import IN_PROCESS_CHANNEL
from src.observability import (
    mark_tool_failed,
    mark_tool_unavailable,
    stamp_tool_channel,
    stamp_tool_provider,
)

logger = structlog.get_logger(__name__)

T = TypeVar("T")

# 137 is any SIGKILL (128+9), not only the kernel OOM killer's, so the count
# it feeds is an upper bound on OOM kills.
OOM_KILL_EXIT_CODE = 137
_OOM_RECORD_TIMEOUT_S = 2


def resource_tier_of(session: Any, backend: Any) -> str | None:
    """The tier of the machine a call runs on, read at call time.

    The session is preferred because a long turn's session can be rebound after
    the tool is built; the sandbox's own tier covers a caller with no session.
    """
    tier = getattr(session, "resource_tier", None)
    if tier is None:
        tier = getattr(getattr(backend, "sandbox", None), "resource_tier", None)
    return tier


async def note_sandbox_exit(backend: Any, exit_code: int | None, failure: str | None) -> None:
    """Count an exit-137 run against the computer's open usage run, by the
    call that saw it end within its wait.

    Never raises or waits long: usage accounting must not break or slow the
    tool. A call that failed in the sandbox (a timeout kills the process too)
    is not the code being OOM-killed, so it is skipped.
    """
    if exit_code != OOM_KILL_EXIT_CODE or failure:
        return
    try:
        sandbox_id = backend.sandbox_id
        if sandbox_id is None:
            return
        # Lazy: the CLI has no server database.
        from src.server.database.computer_runs import record_oom_kill

        await asyncio.wait_for(
            record_oom_kill(backend.provider, sandbox_id), timeout=_OOM_RECORD_TIMEOUT_S
        )
    except Exception as e:  # a call never fails for its accounting
        logger.debug("oom kill record failed: %s", e)


def mark_sandbox_failed(failure: str) -> None:
    """Mark the enclosing ExecuteCode or Bash run failed for a sandbox fault.

    Only for the sandbox failing, never for the agent's code raising or
    exiting nonzero, which is the tool doing its job. A timeout is kept apart
    from the failures: code the agent wrote running long hits the limit far
    more often than a stuck sandbox does, and a dead one raises instead.
    """
    if failure == SANDBOX_TIMEOUT:
        mark_tool_unavailable(failure)
    else:
        mark_tool_failed(failure)


def mark_sandbox_raised(exc: BaseException) -> None:
    """Mark the enclosing run failed when what a tool caught is the sandbox failing.

    For a tool that catches everything to answer the agent in words, where
    most of what it catches is the agent's request refused (a read-only path,
    content the store will not hold). A sandbox gone or unreachable is the
    provider failing.
    """
    if isinstance(exc, (SandboxGoneError, SandboxTransientError)):
        mark_tool_failed(exc)


def stamp_sandbox_served(backend: Any, path: str) -> None:
    """Name the channel a file call takes, and the sandbox's provider on a
    call the sandbox serves.

    On every such call, not only the failures, so a provider's error rate and
    a channel's latency have the calls beneath them. A path under a store
    mount is served in process and names no provider.
    """
    channel_for = getattr(backend, "channel_for", None)
    if channel_for is None:
        return
    try:
        channel = channel_for(path)
    except Exception as e:  # a call never fails for its telemetry
        logger.debug("tool channel lookup failed: %s", e)
        return
    stamp_tool_channel(channel)
    if channel != IN_PROCESS_CHANNEL:
        stamp_tool_provider(backend.provider)


def tool_error_handler(operation_name: str) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[str]]]:
    """Decorator for consistent tool error handling.

    Args:
        operation_name: Human-readable name of the operation for logging

    Usage:
        @tool_error_handler("file read")
        async def my_tool(...):
            ...
    """
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[str]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> str:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"{operation_name} failed",
                    error=str(e),
                    exc_info=True
                )
                return f"ERROR: {operation_name} failed - {e!s}"
        return wrapper
    return decorator
