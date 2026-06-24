"""Parity between the vendored `messages_delta_reducer` and langgraph's `add_messages`.

The vendored reducer is the linchpin of DeltaChannel correctness: replaying
writes through it must reconstruct exactly what `add_messages` would have built.
`add_messages` is a single-write reducer `(left, right)`; `messages_delta_reducer`
is a batch reducer `(state, [writes...])`. We drive them equivalently — apply
writes one-by-one through `add_messages` to build the expected, the same batch
through the vendored reducer — so the structural comparison is fair.

IDs minted for id-less messages differ between the two functions (both call
`uuid4()`), so we compare structure/content/count, not the UUID values.
"""

import copy

import pytest
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    RemoveMessage,
    ToolMessage,
)
from langgraph.graph.message import REMOVE_ALL_MESSAGES, add_messages

from ptc_agent.agent.state import messages_delta_reducer


def _apply_add_messages(writes):
    """Sequentially fold writes through the single-write `add_messages` reducer."""
    state = []
    for w in writes:
        state = add_messages(state, w)
    return state


def _assert_structural_parity(expected, actual):
    """Compare two message lists by type + content + id-presence, not UUID value."""
    assert len(expected) == len(actual)
    for exp, act in zip(expected, actual, strict=True):
        assert type(exp) is type(act)
        assert exp.content == act.content
        # ids must both be present (assigned) or both absent
        assert (exp.id is None) == (act.id is None)


def test_append_new_messages():
    state = [HumanMessage(content="hi", id="h1"), AIMessage(content="hello", id="a1")]
    writes = [[HumanMessage(content="how are you", id="h2")]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert [m.content for m in actual] == ["hi", "hello", "how are you"]
    assert [m.id for m in actual] == ["h1", "a1", "h2"]


def test_dedup_by_id_replaces_in_place():
    state = [
        HumanMessage(content="hi", id="h1"),
        AIMessage(content="draft", id="a1"),
        HumanMessage(content="next", id="h2"),
    ]
    writes = [[AIMessage(content="final", id="a1")]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    # same id -> replaced, position preserved (not moved to end)
    assert [m.content for m in actual] == ["hi", "final", "next"]
    assert [m.id for m in actual] == ["h1", "a1", "h2"]


def test_remove_message_by_id():
    state = [
        HumanMessage(content="hi", id="h1"),
        AIMessage(content="bye", id="a1"),
        HumanMessage(content="again", id="h2"),
    ]
    writes = [[RemoveMessage(id="a1")]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert [m.id for m in actual] == ["h1", "h2"]
    assert [m.content for m in actual] == ["hi", "again"]


def test_remove_all_messages_mid_batch_resets_then_appends():
    state = [HumanMessage(content="old1", id="h1"), AIMessage(content="old2", id="a1")]
    # one write carrying the reset sentinel followed by a fresh message
    writes = [
        [
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            HumanMessage(content="fresh", id="h2"),
        ]
    ]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert [m.id for m in actual] == ["h2"]
    assert [m.content for m in actual] == ["fresh"]


def test_remove_all_across_separate_writes():
    """REMOVE_ALL in one write, appends in a later write within the same batch."""
    state = [HumanMessage(content="old", id="h1")]
    writes = [
        [RemoveMessage(id=REMOVE_ALL_MESSAGES)],
        [AIMessage(content="a", id="a1"), HumanMessage(content="b", id="h2")],
    ]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert [m.id for m in actual] == ["a1", "h2"]
    assert [m.content for m in actual] == ["a", "b"]


def test_idless_messages_get_ids_assigned():
    state = []
    writes = [[HumanMessage(content="hi")], [AIMessage(content="hello")]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    # ids minted (not None); values differ between the two functions, so only
    # assert presence + uniqueness, not equality.
    assert all(m.id is not None for m in actual)
    assert len({m.id for m in actual}) == len(actual)
    assert [m.content for m in actual] == ["hi", "hello"]


def test_raw_dict_input_is_coerced():
    state = []
    writes = [[{"role": "user", "content": "hi"}]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert len(actual) == 1
    assert isinstance(actual[0], HumanMessage)
    assert actual[0].content == "hi"
    assert actual[0].id is not None


def test_raw_str_and_tuple_input_coerced():
    """Raw string and ``(role, content)`` tuple writes coerce like add_messages."""
    for raw in ["hi there", ("user", "howdy")]:
        expected = _apply_add_messages([[], [raw]])
        actual = messages_delta_reducer([], [[raw]])

        _assert_structural_parity(expected, actual)
        assert len(actual) == 1
        assert isinstance(actual[0], HumanMessage)
        assert actual[0].id is not None


def test_remove_unknown_id_is_silently_ignored_diverging_from_add_messages():
    """Intentional divergence: an unknown-id ``RemoveMessage`` is a no-op here.

    ``add_messages`` raises ``ValueError`` for a RemoveMessage targeting an id
    not present; the batch reducer ``DeltaChannel`` requires silently ignores it
    (batching-invariance — a RemoveMessage may legitimately re-run after its
    target is already gone). This pins that contract so a future refactor doesn't
    accidentally "fix" it back to raising and break delta replay.
    """
    state = [HumanMessage(content="hi", id="h1")]

    # add_messages raises on the phantom remove ...
    with pytest.raises(ValueError):
        add_messages(state, [RemoveMessage(id="absent")])

    # ... the vendored reducer silently no-ops, leaving state intact.
    actual = messages_delta_reducer(list(state), [[RemoveMessage(id="absent")]])
    assert [m.id for m in actual] == ["h1"]
    assert [m.content for m in actual] == ["hi"]


def test_offload_remove_all_then_full_relist_parity():
    """The REAL `/offload` + `/compact` write shape: REMOVE_ALL then a full
    re-list of the (already id'd) prior state.

    ``compact.py`` emits ``[RemoveMessage(REMOVE_ALL_MESSAGES), *messages]`` in one
    ``aupdate_state``. The existing REMOVE_ALL tests reset then append a *single
    fresh* message; this pins the reset-then-rebuild that must reconstruct
    identically to ``add_messages`` (including preserving the original ids, since
    after the reset they are fresh appends to an empty list).
    """
    state = [
        HumanMessage(content="q", id="k1"),
        AIMessage(content="draft", id="k2"),
        ToolMessage(content="tool", tool_call_id="tc1", id="k3"),
    ]
    # offload truncates args but keeps the same message objects/ids; simulate by
    # re-listing the same ids with one content edited.
    relisted = [
        HumanMessage(content="q", id="k1"),
        AIMessage(content="draft", id="k2"),
        ToolMessage(content="[offloaded]", tool_call_id="tc1", id="k3"),
    ]
    writes = [[RemoveMessage(id=REMOVE_ALL_MESSAGES), *relisted]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert [m.id for m in actual] == ["k1", "k2", "k3"]
    assert [m.content for m in actual] == ["q", "draft", "[offloaded]"]


def test_last_remove_all_wins_among_multiple():
    """Two REMOVE_ALL sentinels in one batch → only writes after the LAST one
    survive ("last sentinel wins").

    Load-bearing for replay-invariance: a re-run/stacked compaction can put two
    resets in a single delta batch, and the reducer must discard everything
    (state + writes) before the final sentinel. Asserted directly (add_messages
    handles repeated REMOVE_ALL differently, so parity is not the contract here).
    """
    state = [HumanMessage(content="old", id="o1")]
    writes = [
        [
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            AIMessage(content="dropped", id="d1"),
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            AIMessage(content="kept", id="k1"),
        ]
    ]

    actual = messages_delta_reducer(state, writes)

    assert [m.id for m in actual] == ["k1"]
    assert [m.content for m in actual] == ["kept"]


def test_state_with_raw_dicts_is_coerced():
    """Slow-path: a non-empty ``state`` of raw dicts (deserialized-blob / initial
    dict state) is coerced via ``convert_to_messages``.

    The fast-path guard only skips coercion when ``state[0]`` is a BaseMessage;
    the raw-dict-state branch (the reason the guard exists) is otherwise
    unexercised, so a regression dropping coercion would pass silently.
    """
    state = [{"role": "user", "content": "hi", "id": "h1"}]
    writes = [[AIMessage(content="x", id="a1")]]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert isinstance(actual[0], HumanMessage)
    assert [m.id for m in actual] == ["h1", "a1"]
    assert [m.content for m in actual] == ["hi", "x"]


def test_mixed_batch_parity():
    """A realistic multi-write batch: append, tool result, dedup, remove."""
    state = [
        HumanMessage(content="question", id="h1"),
        AIMessage(content="thinking", id="a1"),
    ]
    writes = [
        [ToolMessage(content="tool out", tool_call_id="tc1", id="t1")],
        [AIMessage(content="answer", id="a1")],  # dedup replace in place
        [HumanMessage(content="followup", id="h2")],
        [RemoveMessage(id="t1")],  # remove the tool message
    ]

    expected = _apply_add_messages([state, *writes])
    actual = messages_delta_reducer(state, writes)

    _assert_structural_parity(expected, actual)
    assert [m.id for m in actual] == [m.id for m in expected]
    assert [m.content for m in actual] == ["question", "answer", "followup"]


# --- drift guard: the vendored reducer must match deepagents' upstream copy ----
#
# `messages_delta_reducer` is a near-verbatim copy of deepagents'
# `_messages_delta_reducer` (see src/ptc_agent/agent/state.py). Because it is
# reconstruction logic for persisted delta blobs, we vendor it — freezing its
# semantics to our release — rather than importing the private symbol at runtime
# (a deepagents bump could otherwise silently change how existing threads read
# back). This guard imports deepagents' reducer in the TEST ONLY and asserts
# behavioural parity across the reconstruction-defining cases, so drift surfaces
# as a red CI light we consciously reconcile, not a silent divergence.

# (state, writes, ids_deterministic) — ids_deterministic is True when every input
# message carries an explicit id, so the two reducers' output id sequences must
# match exactly; False when id-less inputs are minted (both call uuid4(), so only
# structure / content / id-presence is comparable).
_DEEPAGENTS_PARITY_CASES = [
    pytest.param([], [[HumanMessage("a", id="h1")]], True, id="append"),
    pytest.param(
        [AIMessage("x", id="a1")], [[AIMessage("y", id="a1")]], True, id="dedup-in-place"
    ),
    pytest.param(
        [AIMessage("x", id="a1")], [[RemoveMessage(id="a1")]], True, id="tombstone"
    ),
    pytest.param(
        [AIMessage("x", id="a1")],
        [[RemoveMessage(id=REMOVE_ALL_MESSAGES), HumanMessage("fresh", id="h2")]],
        True,
        id="remove-all-mid-batch",
    ),
    pytest.param(
        [HumanMessage("old", id="h1")],
        [[RemoveMessage(id=REMOVE_ALL_MESSAGES)], [AIMessage("a", id="a1")]],
        True,
        id="remove-all-across-writes",
    ),
    pytest.param(
        [HumanMessage("hi", id="h1")],
        [[RemoveMessage(id="absent")]],
        True,
        id="unknown-id-remove-noop",
    ),
    pytest.param(
        [HumanMessage("old", id="o1")],
        [
            [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                AIMessage("d", id="d1"),
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                AIMessage("k", id="k1"),
            ]
        ],
        True,
        id="last-remove-all-wins",
    ),
    pytest.param(
        [HumanMessage("q", id="k1"), AIMessage("draft", id="k2")],
        [
            [
                RemoveMessage(id=REMOVE_ALL_MESSAGES),
                HumanMessage("q", id="k1"),
                AIMessage("draft", id="k2"),
            ]
        ],
        True,
        id="offload-relist",
    ),
    pytest.param(
        [{"role": "user", "content": "hi", "id": "h1"}],
        [[AIMessage("x", id="a1")]],
        True,
        id="raw-dict-state",
    ),
    pytest.param(
        [HumanMessage("q", id="h1"), AIMessage("thinking", id="a1")],
        [
            [ToolMessage("tool out", tool_call_id="tc1", id="t1")],
            [AIMessage("answer", id="a1")],
            [HumanMessage("followup", id="h2")],
            [RemoveMessage(id="t1")],
        ],
        True,
        id="mixed-batch",
    ),
    pytest.param([], [[HumanMessage("hi")], [AIMessage("hello")]], False, id="idless-mint"),
    pytest.param([], [[{"role": "user", "content": "d"}]], False, id="dict-input"),
    pytest.param([], [["just a string"]], False, id="str-input"),
    pytest.param([], [[("user", "howdy")]], False, id="tuple-input"),
]


def _struct(msgs):
    """Reducer output as (type, content, id-present) — minted UUID values are
    random, so compared only for presence, not equality."""
    return [(type(m).__name__, m.content, m.id is not None) for m in msgs]


@pytest.mark.parametrize("state, writes, ids_deterministic", _DEEPAGENTS_PARITY_CASES)
def test_vendored_reducer_matches_deepagents(state, writes, ids_deterministic):
    """The vendored reducer must behave identically to deepagents' upstream copy.

    A red here means deepagents changed `_messages_delta_reducer`: reconcile
    `src/ptc_agent/agent/state.py` and confirm the change is safe for already-
    persisted delta blobs before following it.
    """
    try:
        from deepagents._messages_reducer import _messages_delta_reducer as upstream
    except ImportError as exc:  # private module/symbol — a rename is itself drift
        pytest.fail(
            "deepagents._messages_reducer._messages_delta_reducer is gone "
            f"({exc}); the vendored messages_delta_reducer can no longer be drift-"
            "checked against upstream — reconcile state.py with deepagents."
        )

    # deepcopy per call: both reducers mint ids in place, so shared inputs would
    # let the first call pollute the second.
    ours = messages_delta_reducer(copy.deepcopy(state), copy.deepcopy(writes))
    theirs = upstream(copy.deepcopy(state), copy.deepcopy(writes))

    assert _struct(ours) == _struct(theirs), (
        "vendored reducer diverged from deepagents' _messages_delta_reducer"
    )
    if ids_deterministic:
        assert [m.id for m in ours] == [m.id for m in theirs], (
            "explicit-id reconstruction order/ids diverged from deepagents'"
        )
