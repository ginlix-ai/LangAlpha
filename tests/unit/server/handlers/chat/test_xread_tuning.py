"""Locks for the shared XREAD tuning knobs.

The three chat SSE consumers must read one set of numbers: before this module
existed they imported module-private names out of ``run_stream_reader``, which
is exactly the drift hazard these identity checks close.
"""

from unittest.mock import patch

from src.server.handlers.chat import (
    run_stream_reader,
    task_run_sse_reader,
    thread_stream_mux_v2,
    xread_tuning,
)
from src.utils.cache import stream_pool

_TUNING = "src.server.handlers.chat.xread_tuning"


class TestSharedByAllConsumers:
    def test_every_consumer_uses_the_shared_block_geometry(self):
        assert run_stream_reader.xread_block_ms is xread_tuning.xread_block_ms
        assert task_run_sse_reader.xread_block_ms is xread_tuning.xread_block_ms
        assert thread_stream_mux_v2.xread_block_ms is xread_tuning.xread_block_ms

    def test_every_consumer_uses_the_shared_backoffs(self):
        for mod in (run_stream_reader, task_run_sse_reader, thread_stream_mux_v2):
            assert mod.XREAD_ERROR_BACKOFF_S == xread_tuning.XREAD_ERROR_BACKOFF_S
            assert (
                mod.XREAD_EXHAUSTION_BACKOFF_S
                == xread_tuning.XREAD_EXHAUSTION_BACKOFF_S
            )
        assert (
            xread_tuning.XREAD_EXHAUSTION_BACKOFF_S
            > xread_tuning.XREAD_ERROR_BACKOFF_S
        )

    def test_mux_keeps_its_own_per_channel_count(self):
        # COUNT is per stream, so the mux's is a fairness bound, not the
        # single-stream throughput cap — deliberately not shared.
        assert thread_stream_mux_v2._XREAD_COUNT < xread_tuning.XREAD_COUNT


class TestBlockGeometry:
    def test_block_stays_below_socket_timeout(self):
        with patch(f"{_TUNING}.get_redis_socket_timeout", return_value=5):
            assert xread_tuning.xread_block_ms() == 4_000

    def test_low_socket_timeout_floors_instead_of_going_negative(self):
        for socket_timeout in (0, 1, None):
            with patch(
                f"{_TUNING}.get_redis_socket_timeout", return_value=socket_timeout
            ):
                block_ms = xread_tuning.xread_block_ms()
            assert block_ms >= 500
            if socket_timeout:
                assert block_ms < socket_timeout * 1_000


class TestWaitDeadline:
    def test_deadline_leaves_room_for_a_cold_connect(self):
        # The bound has to clear BLOCK *plus* the whole connect path; a deadline
        # inside it cancels mid-handshake, and that cancel is what redials.
        for socket_timeout in (1, 5, 30):
            with patch(
                f"{_TUNING}.get_redis_socket_timeout", return_value=socket_timeout
            ):
                assert xread_tuning.xread_wait_timeout_s() > (
                    xread_tuning.xread_block_ms() / 1000.0
                ) + stream_pool.connect_path_budget_s()

    def test_every_consumer_uses_the_shared_deadline(self):
        for mod in (run_stream_reader, task_run_sse_reader, thread_stream_mux_v2):
            assert mod.xread_wait_timeout_s is xread_tuning.xread_wait_timeout_s
