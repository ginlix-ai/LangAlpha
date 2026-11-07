"""A sandbox process that ends once the backend stops renewing its lease.

A provider keeps a stream's stdin open for as long as its own end lives, so a
backend worker that dies leaves the process with no end it can see: it runs
on in the sandbox. ``leased_command`` starts the command beside a watchdog
that ends it once its lease file is gone or older than ``lease_s``, and
``keep_leased`` renews the lease every ``renew_s`` until the process ends.
The lease follows the backend, not the stream: a backend that keeps a process
quiet for an hour still renews it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shlex
import uuid

from ptc_agent.core.sandbox.runtime import SandboxRuntime, StreamProcess

logger = logging.getLogger(__name__)

LEASE_S = 300
RENEW_S = 60
_RENEW_TIMEOUT_S = 30


def leased_command(command: str, lease: str, lease_s: int) -> str:
    """``command`` run in place of the shell, its pid kept (as the stream's
    signals need), beside a watchdog that ends it, and the processes it
    started in the stream's process group, once ``lease`` is gone or older
    than ``lease_s``. The watchdog holds none of the stream's descriptors,
    so the stream ends when the process does."""
    check = max(1, min(15, lease_s // 3))
    return (
        f'l={shlex.quote(lease)}; : > "$l" || exit 1; P=$$; '
        f'( trap "" TERM; while kill -0 "$P" 2>/dev/null; do sleep {check}; '
        f'[ -e "$l" ] && [ $(( $(date +%s) - $(stat -c %Y "$l" 2>/dev/null || echo 0) )) -le {lease_s} ] && continue; '
        'rm -f "$l"; kill -TERM -"$P" 2>/dev/null || kill -TERM "$P" 2>/dev/null; sleep 5; '
        'kill -KILL -"$P" 2>/dev/null || kill -KILL "$P" 2>/dev/null; break; done; rm -f "$l" ) '
        "</dev/null >/dev/null 2>&1 & "
        f"exec sh -c {shlex.quote(command)}"
    )


def new_lease() -> str:
    """A lease file no other process names."""
    return f"/tmp/.langalpha-lease-{uuid.uuid4().hex}"


async def keep_leased(
    runtime: SandboxRuntime, stream: StreamProcess, lease: str, *, lease_s: int = LEASE_S, renew_s: float = RENEW_S,
) -> None:
    """Renew ``lease`` every ``renew_s`` until ``stream`` ends, then remove it,
    which a watchdog killed with the stream's group could not. A renewal that
    fails is logged, so a process the watchdog ended says why."""
    ended = asyncio.ensure_future(stream.wait())
    failing_since: float | None = None
    loop = asyncio.get_running_loop()
    try:
        while not (await asyncio.wait({ended}, timeout=renew_s))[0]:
            try:
                result = await runtime.exec(f"touch -c -- {shlex.quote(lease)}", timeout=_RENEW_TIMEOUT_S)
                failure = None if result.exit_code == 0 else f"exit {result.exit_code}"
            except Exception as e:  # noqa: BLE001 - one failed renewal is retried; the lease outlives several
                failure = type(e).__name__
            if failure is None:
                failing_since = None
                continue
            failing_since = failing_since if failing_since is not None else loop.time()
            logger.warning(
                "[sandbox] could not renew a process's lease on %s (%s), failing for %.0fs of %ds",
                getattr(runtime, "id", "?"), failure, loop.time() - failing_since, lease_s,
            )
    finally:
        ended.cancel()
        with contextlib.suppress(Exception):
            await runtime.exec(f"rm -f -- {shlex.quote(lease)}", timeout=_RENEW_TIMEOUT_S)
