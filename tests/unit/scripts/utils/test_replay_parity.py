"""A stopped turn is excused only for output streamed past its last
checkpoint, never for a change to what the checkpoints committed."""

import pytest

from scripts.utils.replay_parity import _streamed_past_commit

TEXT = ("main", "text")
CALL = ("search", '{"q":"x"}')


@pytest.mark.parametrize(
    ("key", "committed", "streamed"),
    [
        ("text", {TEXT: "The answer"}, {TEXT: "The answer is 4"}),
        ("text", {}, {TEXT: "Started"}),
        ("signals", {"main": 1}, {"main": 3}),
        ("elapsed_ms", {"main": [120]}, {"main": [120, 80]}),
        ("tool_calls", {CALL}, {CALL, ("fetch", "{}")}),
        (
            "results",
            {"call-1": ("text", "ok")},
            {"call-1": ("text", "ok"), "call-2": ("text", "")},
        ),
        ("results", None, {"call-1": ("text", "ok")}),
    ],
)
def test_output_streamed_after_the_last_checkpoint_is_excused(key, committed, streamed):
    assert _streamed_past_commit(key, committed, streamed)


@pytest.mark.parametrize(
    ("key", "committed", "streamed"),
    [
        ("text", {TEXT: "The answer is 4"}, {TEXT: "The answer"}),
        ("text", {TEXT: "The answer"}, {TEXT: "An answer is 4"}),
        ("text", {TEXT: "The answer"}, {}),
        ("signals", {"main": 2}, {"main": 1}),
        ("elapsed_ms", {"main": [120]}, {"main": [90, 80]}),
        ("tool_calls", {CALL}, set()),
        ("results", {"call-1": ("text", "ok")}, {"call-1": ("text", "changed")}),
        ("results", {"call-1": ("text", "ok")}, None),
        ("artifacts", [], ['{"id":"a-1"}']),
    ],
)
def test_a_change_to_committed_output_is_a_diff(key, committed, streamed):
    assert not _streamed_past_commit(key, committed, streamed)
