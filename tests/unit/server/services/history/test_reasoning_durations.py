"""Replay has to report the thinking time the row it draws actually stands for.

How long the model thought is measured on the live stream and is absent from
the checkpoint, so ``_carry_reasoning_durations`` copies it onto the projected
close. The two sides count differently: interleaved thinking streams one close
per block, while the projector joins those blocks into one reasoning row.
"""

from src.server.services.history.replay import legacy
from src.server.services.history.replay.stored_merge import derive_merge


def _chunk(message_id: str, content: str, content_type: str, **extra):
    data = {
        "thread_id": "t",
        "agent": "main",
        "id": message_id,
        "role": "assistant",
        "content": content,
        "content_type": content_type,
    }
    data.update(extra)
    return {"event": "message_chunk", "data": data}


def _signal(message_id: str, content: str, **extra):
    return _chunk(message_id, content, "reasoning_signal", **extra)


def _carry(turn_items, stored):
    # The merge writes onto the projected items in place.
    legacy.apply(turn_items, derive_merge(turn_items, stored))
    return turn_items


def test_single_reasoning_block_carries_its_own_duration():
    turn_items = [_signal("m1", "start"), _signal("m1", "complete")]
    stored = [_signal("m1", "start"), _signal("m1", "complete", elapsed_ms=4200)]

    assert _carry(turn_items, stored)[1]["data"]["elapsed_ms"] == 4200


def test_interleaved_blocks_sum_onto_the_one_row_they_merge_into():
    """Three thinking blocks, one projected row: the row owns all three.

    Taking the last close alone reported 8,000 for 23,000 of thinking.
    """
    turn_items = [_signal("m1", "start"), _signal("m1", "complete")]
    stored = [
        _signal("m1", "start"), _signal("m1", "complete", elapsed_ms=3000),
        _signal("m1", "start"), _signal("m1", "complete", elapsed_ms=12000),
        _signal("m1", "start"), _signal("m1", "complete", elapsed_ms=8000),
    ]

    assert _carry(turn_items, stored)[1]["data"]["elapsed_ms"] == 23000


def test_separate_messages_keep_separate_durations():
    turn_items = [
        _signal("m1", "start"), _signal("m1", "complete"),
        _signal("m2", "start"), _signal("m2", "complete"),
    ]
    stored = [
        _signal("m1", "start"), _signal("m1", "complete", elapsed_ms=1500),
        _signal("m2", "start"), _signal("m2", "complete", elapsed_ms=9000),
    ]

    carried = _carry(turn_items, stored)
    assert carried[1]["data"]["elapsed_ms"] == 1500
    assert carried[3]["data"]["elapsed_ms"] == 9000


def test_a_close_with_no_stored_duration_leaves_the_row_alone():
    turn_items = [_signal("m1", "start"), _signal("m1", "complete")]
    stored = [_signal("m1", "start"), _signal("m1", "complete")]

    assert "elapsed_ms" not in _carry(turn_items, stored)[1]["data"]


def _answered(message_id: str, **close):
    return [
        _signal(message_id, "start"),
        _chunk(message_id, "Weighing the filings.", "reasoning"),
        _signal(message_id, "complete", **close),
        _chunk(message_id, "Revenue rose 8%.", "text"),
    ]


def test_a_phantom_attempt_does_not_lend_the_row_its_duration():
    """A model call that failed mid-stream and was retried leaves its partial
    in the capture, one message ahead of the one the checkpoint holds. By
    position, the phantom's 900 ms of thinking landed on the committed row."""
    turn_items = _answered("ai-1")
    stored = [
        _signal("lc-phantom", "start"),
        _chunk("lc-phantom", "Weighing", "reasoning"),
        _signal("lc-phantom", "complete", elapsed_ms=900),
        *_answered("lc-final", elapsed_ms=12000),
    ]

    assert _carry(turn_items, stored)[2]["data"]["elapsed_ms"] == 12000


def test_a_phantom_that_never_closed_does_not_cost_the_row_its_duration():
    """The phantom died mid-thought, so it has no close. By position the
    committed close read as a second message the checkpoint never held."""
    turn_items = _answered("ai-1")
    stored = [
        _signal("lc-phantom", "start"),
        _chunk("lc-phantom", "Weighing", "reasoning"),
        *_answered("lc-final", elapsed_ms=12000),
    ]

    assert _carry(turn_items, stored)[2]["data"]["elapsed_ms"] == 12000
