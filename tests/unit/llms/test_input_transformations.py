"""Counting what the API did to a request's earlier thinking, by reason."""

from langchain_core.messages import AIMessage

from src.llms.input_transformations import thinking_drops


def _answer(entries):
    return AIMessage("ok", response_metadata={"input_transformations": entries})


def test_drops_are_counted_by_reason():
    entries = [
        {"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"},
        {"type": "thinking_dropped", "path": "messages.3.content.0", "reason": "prefix_binding_mismatch"},
        {"type": "thinking_dropped", "path": "messages.5.content.0", "reason": "model_binding_mismatch"},
        {"type": "thinking_dropped", "path": "messages.7.content.0", "reason": "organization_binding_mismatch"},
    ]
    assert thinking_drops(_answer(entries)) == {
        "prefix_binding_mismatch": 2,
        "model_binding_mismatch": 1,
        "organization_binding_mismatch": 1,
    }


def test_a_block_let_through_counts_apart_from_a_dropped_one():
    entries = [{"type": "thinking_mismatch_allowed", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"}]
    assert thinking_drops(_answer(entries)) == {"thinking_mismatch_allowed": 1}


def test_an_entry_added_later_counts_as_other():
    entries = [
        {"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "a_later_reason"},
        {"type": "a_later_type", "path": "messages.3.content.0"},
    ]
    assert thinking_drops(_answer(entries)) == {"other": 2}


def test_nothing_listed_counts_nothing():
    assert thinking_drops(_answer([])) == {}
    assert thinking_drops(AIMessage("ok")) == {}
    assert thinking_drops(_answer("not a list")) == {}


def test_a_block_the_answer_before_listed_counts_as_repeated():
    """A block the request still sends is dropped on every request until a
    compaction summarizes it; only its first listing is a new loss."""
    first = [{"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"}]
    again = [*first, {"type": "thinking_dropped", "path": "messages.5.content.0", "reason": "prefix_binding_mismatch"}]

    assert thinking_drops(_answer(again), _answer(first)) == {"repeated": 1, "prefix_binding_mismatch": 1}
    assert thinking_drops(_answer(again), AIMessage("ok")) == {"prefix_binding_mismatch": 2}
