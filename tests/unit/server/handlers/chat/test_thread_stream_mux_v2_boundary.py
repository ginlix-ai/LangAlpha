"""Locks the per-channel backlog boundary of the v2 mux.

A channel replays from 0 (or from a stale cursor) before it goes live, and the
client presents that backlog as already rendered. ``chan_caught_up`` is the
marker between the two: emitted at once for an empty stream, once the cursor
reaches the head recorded at open, or on the first empty read for the channel.
A failed head probe sends no marker, so the client's bounded fallback decides,
and a channel closed by its run_end never trails one behind the close.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.handlers.chat.thread_stream_mux_v2 import (
    _RunChan,
    _caught_up_frame,
    _probe_backlog_head,
)


def _chan(cursor: bytes = b"0") -> _RunChan:
    return _RunChan(run_id="r1", lane="task:t1", stream_key=b"k", cursor=cursor)


def _cache(tail=None, error=None):
    cache = MagicMock()
    cache.client.xrevrange = AsyncMock(return_value=tail)
    if error is not None:
        cache.client.xrevrange.side_effect = error
    return cache


def _payload(frame: str) -> dict:
    assert frame.startswith("event: chan_caught_up\n")
    return json.loads(frame.split("data: ", 1)[1])


@pytest.mark.asyncio
async def test_empty_stream_is_caught_up_at_once():
    chan = _chan()
    frame = await _probe_backlog_head(_cache(tail=[]), chan)
    assert _payload(frame) == {"chan": "run:r1"}
    assert chan.caught_up is True
    assert _caught_up_frame(chan, got_entries=False) is None


@pytest.mark.asyncio
async def test_marker_follows_the_cursor_to_the_recorded_head():
    chan = _chan()
    assert await _probe_backlog_head(_cache(tail=[(b"5-0", {})]), chan) is None
    assert chan.head_at_open == b"5-0"
    chan.cursor = b"3-0"
    assert _caught_up_frame(chan, got_entries=True) is None
    chan.cursor = b"5-0"
    assert _payload(_caught_up_frame(chan, got_entries=True)) == {"chan": "run:r1"}
    # Once only: later reads never repeat it.
    chan.cursor = b"9-0"
    assert _caught_up_frame(chan, got_entries=True) is None


@pytest.mark.asyncio
async def test_empty_read_marks_a_backlog_shorter_than_its_head():
    chan = _chan()
    await _probe_backlog_head(_cache(tail=[(b"5-0", {})]), chan)
    chan.cursor = b"2-0"
    assert _payload(_caught_up_frame(chan, got_entries=False)) == {"chan": "run:r1"}


@pytest.mark.asyncio
async def test_failed_probe_sends_no_marker_and_stops_probing():
    chan = _chan()
    assert await _probe_backlog_head(_cache(error=RuntimeError("down")), chan) is None
    assert chan.caught_up is True
    assert chan.head_at_open is None
    assert _caught_up_frame(chan, got_entries=False) is None


@pytest.mark.asyncio
async def test_closed_channel_never_trails_a_marker_after_its_close():
    # A run_end at the head closes the channel first; chan_close already ends
    # the client's replay, so a marker behind it would be noise.
    chan = _chan()
    await _probe_backlog_head(_cache(tail=[(b"5-0", {})]), chan)
    chan.cursor = b"5-0"
    chan.closed = True
    assert _caught_up_frame(chan, got_entries=True) is None


@pytest.mark.asyncio
async def test_marker_goes_ahead_of_the_first_entry_past_the_head():
    # A batch that straddles the boundary: entries after the recorded head
    # are live, so the marker is emitted before them, not after the batch.
    from src.server.handlers.chat.thread_stream_mux_v2 import _marker_before

    chan = _chan()
    await _probe_backlog_head(_cache(tail=[(b"5-0", {})]), chan)
    assert _marker_before(chan, b"5-0") is None
    assert _payload(_marker_before(chan, b"6-0")) == {"chan": "run:r1"}
    assert _marker_before(chan, b"7-0") is None
    assert _caught_up_frame(chan, got_entries=True) is None
