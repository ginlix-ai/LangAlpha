"""A backfilled turn replays from its legacy facts exactly as from its stored
events, and without reading them.

The backfill derives what a turn's stored events add to its projection once,
into ``replay_facts.legacy``. The facts are stored as jsonb, so they must
survive a JSON round trip, and must place every row where the stored-event
merge placed it.
"""

from __future__ import annotations

import copy
import json
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from scripts.utils.backfill_replay_facts import Derivation
from src.server.database.replay_facts import LEGACY_KEY
from src.server.services.history import replay
from src.server.services.history.replay import legacy, stored_merge
from tests.unit.server.services.history.replay_builders import (
    thread_rows,
    THREAD,
    TIP,
    ThreadHistory,
    _cache_probe,
    _mock_reader,
    _query,
    _response,
    _turn,
)
from tests.unit.server.services.history.test_replay_facts import (
    _RUN_FACTS,
    _returned,
    _signal,
    _task_reader,
    _text,
)

# A pointer as older turns checkpointed it, which is what derive has to
# recognize whatever the agent-facing template reads today.
_EVICTION_POINTER = (
    "Tool result too large, the result of this tool call tc-r was saved in the "
    "filesystem at this path: .agents/large_tool_results/tc-r.md"
)


def _stored_reads_refused(response):
    raise AssertionError("a backfilled turn read its stored events")


async def _both_ways(monkeypatch, responses, run_facts=None):
    """Each turn's lines from its stored events, the facts they derived, and
    the lines again from those facts (through JSON) with the events gone."""
    queries = [_query(ti) for ti in sorted(responses)]
    turns = sorted(responses)
    derivation = Derivation()
    before = await replay.project_turns(
        thread_rows(queries, responses, run_facts=run_facts),
        TIP,
        turn_indexes=turns,
        derive=derivation,
    )
    derived = derivation.turns
    assert all(d.verified for d in derived.values())
    backfilled = {}
    for ti, response in responses.items():
        row = {k: v for k, v in response.items() if k != "sse_events"}
        row["replay_facts"] = {
            **(response.get("replay_facts") or {}),
            LEGACY_KEY: json.loads(json.dumps(derived[ti].facts)),
        }
        backfilled[ti] = row
    monkeypatch.setattr(stored_merge, "_stored_events", _stored_reads_refused)
    again = Derivation()
    after = await replay.project_turns(
        thread_rows(queries, backfilled, run_facts=run_facts),
        TIP,
        turn_indexes=turns,
        derive=again,
    )
    assert again.turns == {}  # nothing derived a second time
    return before, {ti: d.facts for ti, d in derived.items()}, after


def _items(lines):
    return {ti: [line.item() for line in turn] for ti, turn in lines.items()}


# ------------------------------------------------------------ through replay


@pytest.mark.asyncio
async def test_widgets_results_and_signals_replay_from_facts(monkeypatch):
    messages = [
        AIMessage(
            content="looking",
            id="ai-1",
            tool_calls=[
                {"name": "ShowWidget", "args": {}, "id": "tc-s"},
                {"name": "get_sec_filing", "args": {}, "id": "tc-r"},
            ],
        ),
        ToolMessage(
            content="shown",
            tool_call_id="tc-s",
            name="ShowWidget",
            id="tm-1",
            artifact={"html": "<div/>", "title": "W"},
        ),
        ToolMessage(
            content=_EVICTION_POINTER, tool_call_id="tc-r", name="get_sec_filing", id="tm-2"
        ),
        AIMessage(content="done", id="ai-2"),
    ]

    def widget(artifact_id, data):
        payload = {"html": "<div/>", "title": "W", "data": data}
        return {
            "event": "artifact",
            "data": {
                "artifact_type": "html_widget",
                "artifact_id": artifact_id,
                "payload": payload,
            },
        }

    stored = [
        {"event": "message_chunk", "data": {"id": "lc-1", "content_type": "text", "content": "looking"}},
        widget("widget_1", {"a.csv": "1"}),
        # Beyond what the checkpoint projected: placed like a signal row.
        widget("widget_2", {"b.csv": "2"}),
        {"event": "tool_call_result", "data": {"tool_call_id": "tc-r", "content": "full", "content_type": "text"}},
        {"event": "context_window", "data": {"action": "offload", "kind": "tool_args"}},
        {"event": "provenance", "data": {"source_type": "web", "identifier": "https://a.example"}},
        {"event": "message_chunk", "data": {"id": "lc-2", "content_type": "text", "content": "done"}},
        {"event": "credit_usage", "data": {"total_credits": 1.0}},
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, messages)]))

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    merge = facts[0]["merge"]
    # Named by the projected widget it upgraded; the extra one by nothing.
    assert [artifact_id for artifact_id, _data in merge["widgets"]] == ["tc-s", None]
    assert merge["results"] == {"tc-r": {"content": "full", "content_type": "text"}}
    assert [s.get("event", "widget") for s in merge["signals"]] == [
        "widget", "widget", "context_window", "provenance", "credit_usage"
    ]
    assert after == before
    result = next(i for i in _items(after)[0] if i["event"] == "tool_call_result"
                  and i["data"]["tool_call_id"] == "tc-r")
    assert result["data"]["content"] == "full"


@pytest.mark.asyncio
async def test_reasoning_durations_replay_from_facts(monkeypatch):
    reasoning = AIMessage(
        content=[
            {"type": "thinking", "thinking": "weighing it"},
            {"type": "text", "text": "the answer"},
        ],
        id="msg_1",
    )
    stored = [
        _signal("lc-1", "start"),
        _signal("lc-1", "complete", elapsed_ms=1500),
        _signal("lc-1", "start"),
        _signal("lc-1", "complete", elapsed_ms=500),
        _text("lc-1", "the answer"),
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [reasoning])]))

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    assert [ms for _key, ms in facts[0]["merge"]["durations"]] == [2000]
    assert after == before


@pytest.mark.asyncio
async def test_interrupt_and_error_keep_their_places(monkeypatch):
    messages = [
        AIMessage(
            content="",
            id="ai-1",
            tool_calls=[{"name": "AskUserQuestion", "args": {}, "id": "tc-q"}],
        ),
        ToolMessage(
            content="answer: blue", tool_call_id="tc-q", name="AskUserQuestion", id="tm-1"
        ),
    ]
    stored = [
        {"event": "tool_calls", "data": {"id": "run-1", "tool_calls": [{"id": "tc-q", "name": "AskUserQuestion"}]}},
        {"event": "interrupt", "data": {"interrupt_id": "int-q", "action_requests": [{"type": "ask_user_question"}]}},
        {"event": "tool_call_result", "data": {"tool_call_id": "tc-q", "content": "answer: blue"}},
        {"event": "error", "data": {"error": "provider exploded"}},
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, messages)]))

    before, _facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    assert after == before
    events = [i["event"] for i in _items(after)[0]]
    assert events.index("tool_calls") < events.index("interrupt") < events.index("tool_call_result")


@pytest.mark.asyncio
async def test_unresolved_images_resolve_to_the_stored_url_with_their_basename(monkeypatch):
    """Capture rewrote the stored copy's paths to keys ending in the file's
    basename. The turn keeps its projection, so a stored message the
    checkpoint never committed stays out, and a path capture never reached
    stays a path."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[
                _turn(
                    0,
                    [AIMessage(content="![chart](work/chart.png) ![raw](work/raw.png)", id="ai-1")],
                )
            ],
        ),
    )
    stored = [
        _text("lc-1", "![chart](https://cdn/abc/chart.png) ![raw](work/raw.png)"),
        _text("lc-2", "a reply the checkpoint never committed"),
    ]

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    assert facts[0]["images"] == {"work/chart.png": "https://cdn/abc/chart.png"}
    assert "verbatim" not in facts[0]
    assert after == before
    assert [i["data"]["content"] for i in _items(after)[0] if i["event"] == "message_chunk"] == [
        "![chart](https://cdn/abc/chart.png) ![raw](work/raw.png)"
    ]


@pytest.mark.asyncio
async def test_a_run_lanes_unresolved_image_resolves_from_the_stored_copy(monkeypatch):
    """A run settled before its row kept facts has its image URL only in
    the stored copy, which a run lane resolves from like the main lane."""
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    stored = [_text("lc-s", "see ![c](https://cdn/abc/sub.png)", agent="task:tsk1")]

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    assert facts[0]["images"] == {"work/sub.png": "https://cdn/abc/sub.png"}
    assert after == before
    assert [
        i["data"]["content"]
        for i in _items(after)[0]
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ] == ["see ![c](https://cdn/abc/sub.png)"]


@pytest.mark.asyncio
async def test_a_run_lanes_ambiguous_image_keeps_the_main_lane_projected(monkeypatch):
    """Only the main lane can replay verbatim, so a run lane's path that
    several stored URLs could match stays a path and sends nothing there."""
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    stored = [
        _text("lc-s1", "see ![c](https://cdn/1/sub.png)", agent="task:tsk1"),
        _text("lc-s2", "or ![c](https://cdn/2/sub.png)", agent="task:tsk1"),
    ]

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    assert "verbatim" not in facts[0]
    assert "images" not in facts[0]
    assert after == before


@pytest.mark.asyncio
async def test_images_no_basename_settles_replay_their_main_lane_verbatim(monkeypatch):
    """Two stored URLs share the basename of both projected paths, so only
    the stored copy knows which image is which."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[
                _turn(
                    0,
                    [AIMessage(content="![a](work/a/chart.png) ![b](work/b/chart.png)", id="ai-1")],
                )
            ],
        ),
    )
    stored = [_text("lc-1", "![a](https://cdn/1/chart.png) ![b](https://cdn/2/chart.png)")]

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0, stored)})

    assert facts[0]["verbatim"] == stored
    assert "images" not in facts[0]
    assert after == before
    chunk = next(i for i in _items(after)[0] if i["event"] == "message_chunk")
    assert chunk["data"]["content"] == "![a](https://cdn/1/chart.png) ![b](https://cdn/2/chart.png)"


@pytest.mark.asyncio
async def test_a_run_lane_keeps_its_own_returns_once(monkeypatch):
    """A run with facts places its returns; the stored copy adds only the
    ones its facts do not hold."""
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    stored = [_returned("in-1"), _returned("in-2", content="and NVDA")]

    before, _facts, after = await _both_ways(
        monkeypatch, {0: _response(0, stored)}, run_facts=[_RUN_FACTS]
    )

    assert after == before
    returns = [i["data"]["input_id"] for i in _items(after)[0] if i["event"] == "steering_returned"]
    assert sorted(returns) == ["in-1", "in-2"]


@pytest.mark.asyncio
async def test_a_turn_without_stored_events_is_known_empty(monkeypatch):
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, [AIMessage(content="hi", id="ai-1")])]),
    )

    before, facts, after = await _both_ways(monkeypatch, {0: _response(0)})

    assert facts[0] == {"v": legacy.FACTS_VERSION}
    assert after == before


@pytest.mark.asyncio
async def test_facts_the_row_would_hold_differently_are_not_verified(monkeypatch):
    """A jsonb bind drops a NUL, so facts carrying one would replay without
    it: the backfill must not write them."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(thread_id=THREAD, turns=[_turn(0, [AIMessage(content="hi", id="ai-1")])]),
    )
    stored = [
        {"event": "message_chunk", "data": {"id": "lc-1", "content_type": "text", "content": "hi"}},
        {"event": "provenance", "data": {"source_type": "web", "identifier": "a\x00b"}},
    ]
    derivation = Derivation()
    await replay.project_turns(
        thread_rows([_query(0)], {0: _response(0, stored)}),
        TIP,
        turn_indexes=[0],
        derive=derivation,
    )

    assert derivation.turns[0].verified is False
    signal = derivation.turns[0].facts["merge"]["signals"][0]
    assert signal["data"]["identifier"] == "ab"  # as the row would hold it


# ------------------------------------------------------------ facts_of


@pytest.mark.parametrize(
    ("replay_facts", "expected"),
    [
        (None, None),
        ({"v": 1, "reasoning_ms": {}}, None),
        ({"legacy": "not a dict"}, None),
        ({"legacy": {"v": legacy.FACTS_VERSION + 1, "merge": {}}}, None),
        ({"legacy": {"v": legacy.FACTS_VERSION}}, {"v": legacy.FACTS_VERSION}),
    ],
    ids=["none", "run_facts_only", "malformed", "other_version", "known_empty"],
)
def test_only_this_versions_facts_stand_in_for_the_events(replay_facts, expected):
    assert legacy.facts_of({"replay_facts": replay_facts}) == expected


# ------------------------------------------------------------ apply


def _row(event, **data):
    return {"event": event, "data": data}


def _projection():
    return [
        _row("message_chunk", agent="main", id="m1", content_type="text", content="a"),
        _row("tool_calls", agent="main", tool_calls=[{"id": "tc-1"}, {"id": "tc-2"}]),
        _row("tool_call_result", agent="main", tool_call_id="tc-1", content="r1"),
        _row("tool_call_result", agent="main", tool_call_id="tc-2", content="r2"),
        _row("message_chunk", agent="main", id="m2", content_type="text", content="b"),
    ]


def _events(items):
    return [(i["event"], i["data"].get("tool_call_id") or i["data"].get("tag")) for i in items]


M1 = ["message_chunk", "main", "m1", "text"]
TC1 = ["tool_call_result", "tc-1"]
TC2 = ["tool_call_result", "tc-2"]
GONE = ["tool_call_result", "gone"]


def test_a_signal_without_at_follows_the_one_before_it():
    merge = {
        "anchors": [TC1],
        "signals": [
            {"event": "provenance", "data": {"tag": "p1"}},
            {"event": "provenance", "data": {"tag": "p2"}, "at": 0},
            {"event": "provenance", "data": {"tag": "p3"}},
        ],
    }
    assert _events(legacy.apply(_projection(), merge)) == [
        ("provenance", "p1"),
        ("message_chunk", None),
        ("tool_calls", None),
        ("tool_call_result", "tc-1"),
        ("provenance", "p2"),
        ("provenance", "p3"),
        ("tool_call_result", "tc-2"),
        ("message_chunk", None),
    ]


def test_a_lost_anchor_falls_back_to_the_newest_that_remains():
    merge = {"anchors": [M1, TC1, GONE], "signals": [{"event": "provenance", "data": {"tag": "p"}, "at": 2}]}
    out = _events(legacy.apply(_projection(), merge))
    assert out[2:4] == [("tool_call_result", "tc-1"), ("provenance", "p")]


def test_the_fallback_stops_at_the_previous_signal():
    """Anchors before the previous signal placed that one; a signal whose own
    anchors are all gone follows it."""
    merge = {
        "anchors": [M1, TC1, GONE],
        "signals": [
            {"event": "provenance", "data": {"tag": "p1"}, "at": 1},
            {"event": "provenance", "data": {"tag": "p2"}, "at": 2},
        ],
    }
    out = _events(legacy.apply(_projection(), merge))
    assert out[2:5] == [("tool_call_result", "tc-1"), ("provenance", "p1"), ("provenance", "p2")]


def test_a_signal_whose_anchors_are_all_gone_leads_the_turn():
    merge = {"anchors": [GONE], "signals": [{"event": "credit_usage", "data": {"tag": "c"}, "at": 0}]}
    assert _events(legacy.apply(_projection(), merge))[0] == ("credit_usage", "c")


def test_each_call_of_a_parallel_round_anchors_on_the_round():
    merge = {
        "anchors": [["tool_calls", "tc-2"]],
        "signals": [{"event": "provenance", "data": {"tag": "p"}, "at": 0}],
    }
    out = _events(legacy.apply(_projection(), merge))
    assert out[1:3] == [("tool_calls", None), ("provenance", "p")]


def test_an_error_follows_everything_its_lane_committed():
    merge = {"anchors": [TC1], "signals": [{"event": "error", "data": {"agent": "main", "tag": "e"}, "at": 0}]}
    assert _events(legacy.apply(_projection(), merge))[-1] == ("error", "e")


def test_a_result_is_restored_by_its_call_whatever_the_projection_shows():
    """The pointer text is agent-facing and may be reworded; the facts
    already decided which results to restore."""
    projected = _projection()
    projected[2]["data"]["content"] = "Saved to .agents/large_tool_results/tc-1.md"
    merge = {"results": {"tc-1": {"content": "full", "content_type": "text"}}}
    out = legacy.apply(projected, merge)
    assert (out[2]["data"]["content"], out[2]["data"]["content_type"]) == ("full", "text")
    assert out[3]["data"]["content"] == "r2"


def _widget(artifact_id, **payload):
    return _row("artifact", artifact_type="html_widget", artifact_id=artifact_id, payload=payload)


def _stored_widget(k):
    return {"artifact_type": "html_widget", "artifact_id": f"s{k}", "payload": {"k": k}}


def test_a_widget_takes_the_payload_that_names_it():
    projected = [_widget("tc-a"), _widget("tc-b")]
    merge = {"widgets": [["tc-b", _stored_widget(1)], ["tc-a", _stored_widget(0)]]}
    out = legacy.apply(projected, merge)
    assert [i["data"]["artifact_id"] for i in out] == ["s0", "s1"]


def test_an_unnamed_payload_pairs_in_order_or_is_placed():
    """One the projection did not emit then pairs with a widget no payload
    names; one whose widget the projection dropped is placed where it
    streamed, and a payload a widget took is not placed again."""
    projected = [
        _widget("tc-new"),
        _row("message_chunk", agent="main", id="m1", content_type="text", content="a"),
    ]
    merge = {
        "widgets": [["tc-gone", _stored_widget(0)], [None, _stored_widget(1)]],
        "anchors": [M1],
        "signals": [{"widget": 0, "at": 0}, {"widget": 1}, {"widget": 5}],
    }
    out = legacy.apply(projected, merge)
    assert [(i["event"], i["data"].get("artifact_id")) for i in out] == [
        ("artifact", "s1"),
        ("message_chunk", None),
        ("artifact", "s0"),
    ]
    # The placed copy is the turn's own: enriching it leaves the facts alone.
    out[-1]["data"]["turn_index"] = 0
    assert "turn_index" not in merge["widgets"][0][1]


def test_no_merge_leaves_the_projected_signals_standing():
    projected = [*_projection(), _row("context_window", action="offload")]
    assert legacy.apply(projected, None) is projected


def test_a_message_without_an_id_is_named_by_its_occurrence():
    """The projector gives each id-less message the same placeholder."""
    rows = [
        _row("message_chunk", agent="main", id="unknown", content_type="reasoning", content="x"),
        _row("message_chunk", agent="main", id="unknown", content_type="text", content="a"),
        _row("tool_calls", agent="main", tool_calls=[{"id": "tc-1"}]),
        _row("message_chunk", agent="main", id="unknown", content_type="text", content="b"),
    ]
    assert legacy.row_keys(rows) == [
        ("message_chunk", "main", "unknown", "reasoning"),
        ("message_chunk", "main", "unknown", "text"),
        ("tool_calls", "tc-1"),
        ("message_chunk", "main", "unknown", "text", 1),
    ]


# ------------------------------------------------------------ projection drift
#
# Facts resolved against one projection and applied to a later one, the way a
# backfilled row meets a projector change: what the facts carry stays with
# the rows it was resolved onto.


def _message(message_id, elapsed_ms=None):
    close = {"elapsed_ms": elapsed_ms} if elapsed_ms is not None else {}
    return [
        _row("message_chunk", agent="main", id=message_id, content_type="reasoning_signal", content="start"),
        _row("message_chunk", agent="main", id=message_id, content_type="reasoning", content="r"),
        _row("message_chunk", agent="main", id=message_id, content_type="reasoning_signal", content="complete", **close),
        _row("message_chunk", agent="main", id=message_id, content_type="text", content="t"),
    ]


def _call(call_id):
    return [
        _row("tool_calls", agent="main", tool_calls=[{"id": call_id}]),
        _row("tool_call_result", agent="main", tool_call_id=call_id, content="ok"),
    ]


def _resolved_then_applied(then, now, stored):
    facts = json.loads(json.dumps(stored_merge.derive_turn(then, [], stored)))
    return legacy.apply_turn(copy.deepcopy(now), [], facts)


def _shape(items):
    out = []
    for item in items:
        data = item["data"]
        if item["event"] == "message_chunk" and data["content_type"] == "reasoning_signal":
            if data["content"] == "complete":
                out.append(f"close:{data['id']}:{data.get('elapsed_ms')}")
        elif item["event"] == "message_chunk" and data["content_type"] == "text":
            out.append(f"text:{data['id']}")
        elif item["event"] == "artifact":
            out.append(f"widget:{data['artifact_id']}")
        elif item["event"] not in ("message_chunk", "tool_calls", "tool_call_result"):
            out.append(item["event"])
    return out


def test_a_message_the_projection_gained_takes_nothing_of_its_neighbours():
    stored = [
        *_message("lc0", 1000),
        *_call("A"),
        *_message("lc1", 2000),
        _row("context_window", agent="main", action="token_usage"),
    ]
    then = [*_message("p0"), *_call("A"), *_message("p1")]
    now = [*_message("p0"), *_call("A"), *_message("px"), *_message("p1")]

    assert _shape(_resolved_then_applied(then, now, stored)) == [
        "close:p0:1000", "text:p0",
        "close:px:None", "text:px",
        "close:p1:2000", "text:p1",
        "context_window",
    ]


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        ([*_message("p1")], ["close:p1:None", "text:p1", "credit_usage"]),
        ([*_message("p0")], ["close:p0:None", "text:p0", "credit_usage"]),
    ],
    ids=["first_dropped", "last_dropped"],
)
def test_a_signal_whose_anchor_the_projection_dropped_stays_in_place(now, expected):
    stored = [*_message("lc0"), *_message("lc1"), _row("credit_usage", agent="main")]
    then = [*_message("p0"), *_message("p1")]

    assert _shape(_resolved_then_applied(then, now, stored)) == expected


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (
            [_widget("tc-new"), _widget("tc-a"), _widget("tc-b")],
            ["widget:tc-new", "widget:s0", "widget:s1", "widget:s2", "provenance"],
        ),
        (
            [*_call("x"), _widget("tc-b")],
            ["widget:s0", "widget:s1", "widget:s2", "provenance"],
        ),
        (
            [_widget("tc-a"), _widget("tc-b"), _widget("tc-c")],
            ["widget:s0", "widget:s1", "widget:s2", "provenance"],
        ),
    ],
    ids=["widget_gained", "widget_dropped", "missed_widget_emitted"],
)
def test_a_widget_payload_stays_with_its_widget(now, expected):
    """The third payload streamed beyond what the projection then emitted:
    placed where it streamed, or taken by a widget that appears after its
    neighbours' widgets, never by one ahead of them."""
    stored = [
        _row("artifact", artifact_type="html_widget", artifact_id=f"s{k}", payload={"k": k})
        for k in range(3)
    ] + [_row("provenance", agent="main")]
    then = [_widget("tc-a"), _widget("tc-b")]

    assert _shape(_resolved_then_applied(then, now, stored)) == expected


# ------------------------------------------------------------ re-read


@pytest.mark.asyncio
@pytest.mark.parametrize(("verbatim", "asked"), [(False, legacy.FACTS_VERSION), (True, None)])
async def test_the_re_read_leaves_out_events_the_facts_stand_in_for(
    monkeypatch, verbatim, asked
):
    read = AsyncMock(return_value={"resp-0": {"conversation_response_id": "resp-0"}})
    monkeypatch.setattr("src.server.database.conversation.get_replay_responses", read)

    await replay.with_stored_events(
        {0: {"conversation_response_id": "resp-0"}}, [0], verbatim=verbatim
    )

    read.assert_awaited_once_with(["resp-0"], legacy_facts=asked)
