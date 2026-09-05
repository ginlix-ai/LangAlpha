"""Shared XREAD tuning knobs for the chat SSE consumers.

Three consumers run the same XREAD BLOCK geometry against the dedicated
stream-reader pool — the main run lane (``run_stream_reader``), the
per-task v2 reader (``task_run_sse_reader``) and the thread mux
(``thread_stream_mux_v2``). The numbers live here, public, so all three
move together and no module reaches into another's privates for them.
"""

from __future__ import annotations

from src.config.settings import get_redis_socket_timeout
from src.utils.cache.stream_pool import connect_path_budget_s

# Error backoff between XREAD retries. Pool exhaustion gets the longer one on
# purpose: a 0.5s retry from every reader is what turns a brief shortage into a
# self-sustaining storm — each failed acquire is itself contention.
XREAD_ERROR_BACKOFF_S = 0.5
XREAD_EXHAUSTION_BACKOFF_S = 2.0

# Cap entries per XREAD round for the single-stream readers. Keeps us
# responsive to terminal-check polling under sustained traffic without
# per-event round-trips. The mux sizes its own COUNT separately: there the
# argument applies per stream, so it doubles as the per-channel fairness bound.
XREAD_COUNT = 100

_XREAD_BLOCK_MARGIN_MS = 1_000
_XREAD_BLOCK_FLOOR_MS = 500


def xread_block_ms() -> int:
    """Compute XREAD's BLOCK arg given the pool's socket_timeout.

    redis-py applies the connection's ``socket_timeout`` to every command,
    blocking ones included. If BLOCK >= socket_timeout the socket read
    raises ``Timeout reading from redis`` before XREAD ever returns. We
    keep BLOCK strictly below socket_timeout by ``_XREAD_BLOCK_MARGIN_MS``
    (1 s by default — the cost is one extra XREAD round-trip per
    ``socket_timeout - 1`` s on idle streams, negligible vs LLM latency).

    When ``socket_timeout`` is configured very low (1-2 s) the natural
    ``timeout - margin`` would go to zero or negative; we floor at
    ``_XREAD_BLOCK_FLOOR_MS`` (500 ms) so the consumer still polls at a
    sane cadence. The accepted trade-off is that with ``socket_timeout=1
    s`` the safety margin shrinks from 1 s to 500 ms — still positive, but
    redis-py is more likely to win the race and surface a Timeout. Bump
    ``redis.socket_timeout`` (config.yaml) above 2 s in production.
    """
    socket_seconds = get_redis_socket_timeout() or 5
    socket_ms = max(1, socket_seconds) * 1_000
    return max(_XREAD_BLOCK_FLOOR_MS, socket_ms - _XREAD_BLOCK_MARGIN_MS)


# Headroom over BLOCK + the connect path, for scheduling jitter only.
_XREAD_WAIT_SLACK_S = 0.5


def xread_wait_timeout_s() -> float:
    """Outer deadline for one XREAD round, cold connect included.

    BLOCK bounds only the server-side wait. A round that has to build a
    connection first also pays the pool acquire, the TCP dial and the
    handshake, so the deadline is derived from
    ``stream_pool.connect_path_budget_s()`` rather than a fixed margin —
    a deadline shorter than the connect path cancels mid-handshake, and
    that cancel tears the socket down and redials, which is the storm the
    dedicated pool exists to prevent.
    """
    return (
        (xread_block_ms() / 1000.0) + connect_path_budget_s() + _XREAD_WAIT_SLACK_S
    )
