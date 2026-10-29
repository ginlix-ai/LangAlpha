"""A legacy row replays from its checkpoints plus its legacy facts.

A turn settled while its live stream was archived carries what only that
archive kept in ``replay_facts.legacy``, resolved once against the projection
of its day. The facts outlive that code, so they name projected rows by what
the checkpoint gives them, and today's projection has to keep giving its rows
those names for each fact to land where it was resolved to.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from src.server.database.conversation import replay_rows
from src.server.database.replay_facts import LEGACY_KEY
from src.server.services.history import replay
from src.server.services.history.replay import legacy
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
    _task_reader,
    _text,
)


def _legacy_row(turn_index, facts, **fields):
    row = _response(turn_index) | fields
    row["replay_facts"] = {LEGACY_KEY: {"v": legacy.FACTS_VERSION, **facts}}
    return row


async def _replayed(responses, **rows):
    turns = sorted(responses)
    lines = await replay.project_turns(
        thread_rows([_query(ti) for ti in turns], responses, **rows),
        TIP,
        turn_indexes=turns,
    )
    return {ti: [line.item() for line in turn] for ti, turn in lines.items()}


_ROW_NAME = {"message_chunk": "id", "artifact": "artifact_id", "tool_call_result": "tool_call_id"}


def _named(items):
    return [(i["event"], i["data"].get(_ROW_NAME.get(i["event"], ""))) for i in items]


def _tagged(items):
    """``_named``, with a signal row named by its tag: only an archived copy
    carries one."""
    return [
        (event, name or item["data"].get("tag"))
        for item, (event, name) in zip(items, _named(items))
    ]


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
            content=(
                "Tool result too large, the result of this tool call tc-r was saved "
                "in the filesystem at this path: .agents/large_tool_results/tc-r.md"
            ),
            tool_call_id="tc-r",
            name="get_sec_filing",
            id="tm-2",
        ),
        AIMessage(content="done", id="ai-2"),
    ]

    def widget(artifact_id, data):
        payload = {"html": "<div/>", "title": "W", "data": data}
        return {"artifact_type": "html_widget", "artifact_id": artifact_id, "payload": payload}

    facts = {
        "merge": {
            # Named by the projected widget it upgrades; the second streamed
            # beyond what the checkpoint projects, so it is placed.
            "widgets": [
                ["tc-s", widget("widget_1", {"a.csv": "1"})],
                [None, widget("widget_2", {"b.csv": "2"})],
            ],
            "results": {"tc-r": {"content": "full", "content_type": "text"}},
            "anchors": [
                ["message_chunk", "main", "ai-1", "text"],
                ["artifact", "tc-s"],
                ["widget", 1],
                ["tool_call_result", "tc-r"],
                ["message_chunk", "main", "ai-2", "text"],
            ],
            "signals": [
                {"widget": 0, "at": 0},
                {"widget": 1, "at": 1},
                {"event": "context_window", "data": {"action": "offload", "kind": "tool_args"}, "at": 3},
                {"event": "provenance", "data": {"source_type": "web", "identifier": "https://a.example"}},
                {"event": "credit_usage", "data": {"total_credits": 1.0}, "at": 4},
            ],
        }
    }
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, messages)]))

    items = (await _replayed({0: _legacy_row(0, facts)}))[0]

    assert _named(items) == [
        ("user_message", None),
        ("message_chunk", "ai-1"),
        ("tool_calls", None),
        ("artifact", "widget_1"),
        ("artifact", "widget_2"),
        ("tool_call_result", "tc-s"),
        ("tool_call_result", "tc-r"),
        ("context_window", None),
        ("provenance", None),
        ("message_chunk", "ai-2"),
        ("credit_usage", None),
    ]
    assert items[3]["data"]["payload"]["data"] == {"a.csv": "1"}
    assert items[6]["data"]["content"] == "full"


@pytest.mark.asyncio
async def test_reasoning_durations_replay_from_facts(monkeypatch):
    reasoning = AIMessage(
        content=[
            {"type": "thinking", "thinking": "weighing it"},
            {"type": "text", "text": "the answer"},
        ],
        id="msg_1",
    )
    facts = {"merge": {"durations": [[["message_chunk", "main", "msg_1", "reasoning_signal"], 2000]]}}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[_turn(0, [reasoning])]))

    items = (await _replayed({0: _legacy_row(0, facts)}))[0]

    assert [
        i["data"].get("elapsed_ms")
        for i in items
        if i["event"] == "message_chunk"
        and i["data"]["content_type"] == "reasoning_signal"
        and i["data"]["content"] == "complete"
    ] == [2000]


@pytest.mark.asyncio
async def test_images_resolve_to_the_urls_the_facts_hold(monkeypatch):
    """A path capture never reached stays a path."""
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
    facts = {"images": {"work/chart.png": "https://cdn/abc/chart.png"}, "merge": {}}

    items = (await _replayed({0: _legacy_row(0, facts)}))[0]

    assert [i["data"]["content"] for i in items if i["event"] == "message_chunk"] == [
        "![chart](https://cdn/abc/chart.png) ![raw](work/raw.png)"
    ]


@pytest.mark.asyncio
async def test_a_run_lanes_image_resolves_from_its_turns_facts(monkeypatch):
    """A run settled before its row kept facts has its image URL only in its
    turn's legacy facts, which reach its lane like the main lane."""
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    facts = {"images": {"work/sub.png": "https://cdn/abc/sub.png"}, "merge": {}}

    items = (await _replayed({0: _legacy_row(0, facts)}))[0]

    assert [
        i["data"]["content"]
        for i in items
        if i["event"] == "message_chunk" and i["data"].get("agent") == "task:tsk1"
    ] == ["see ![c](https://cdn/abc/sub.png)"]


@pytest.mark.asyncio
async def test_a_main_lane_held_verbatim_replays_as_held(monkeypatch):
    """Two captured URLs shared the basename of both projected paths, so the
    facts hold the main lane as it streamed."""
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
    facts = {"verbatim": [_text("lc-1", "![a](https://cdn/1/chart.png) ![b](https://cdn/2/chart.png)")]}

    items = (await _replayed({0: _legacy_row(0, facts)}))[0]

    assert [(i["data"]["id"], i["data"]["content"]) for i in items if i["event"] == "message_chunk"] == [
        ("lc-1", "![a](https://cdn/1/chart.png) ![b](https://cdn/2/chart.png)")
    ]


@pytest.mark.asyncio
async def test_a_run_lane_keeps_its_own_returns_once(monkeypatch):
    """A run with facts places its returns; the legacy facts add only the
    ones the run's facts do not hold."""
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    facts = {"merge": {"signals": [_returned("in-1"), _returned("in-2", content="and NVDA")]}}

    items = (await _replayed({0: _legacy_row(0, facts)}, run_facts=[_RUN_FACTS]))[0]

    returns = [i["data"]["input_id"] for i in items if i["event"] == "steering_returned"]
    assert sorted(returns) == ["in-1", "in-2"]


def _archived(event, at=None, **data):
    """A signal row as the archive kept it, tagged apart from the projected
    copy of its kind."""
    signal = {"event": event, "data": {"agent": "main", "tag": "archived", **data}}
    return signal if at is None else signal | {"at": at}


@pytest.mark.asyncio
async def test_archived_signal_rows_replace_the_projected_ones(monkeypatch):
    """Each kind the archive kept in full is projected too, from the
    checkpoint or the tables. The facts place the archived copies where they
    streamed, so the projected ones go, or every signal row would render
    twice."""
    usage = {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}
    delivered = {"count": 1, "messages": [{"content": "go deeper"}], "timestamp": 2.0}
    fallback = {
        "type": "ui",
        "id": "fb-1",
        "name": "model_fallback",
        "props": {"from_model": "p", "to_model": "f"},
        "metadata": {},
    }
    turn = _turn(
        0,
        [
            AIMessage(
                content="looking",
                id="ai-1",
                tool_calls=[{"name": "web_search", "args": {}, "id": "tc-1"}],
                usage_metadata=usage,
            ),
            ToolMessage(content="result", tool_call_id="tc-1", id="tm-1"),
            HumanMessage(
                content="[Steering from User]\ngo deeper",
                id="h-steer",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": delivered,
                },
            ),
            AIMessage(content="done", id="ai-2", usage_metadata=usage),
        ],
        new_ui_records=[fallback],
    )
    turn.anchor.ending_interrupts = [
        {"id": "int-1", "value": {"action_requests": [{"description": "pick one"}]}}
    ]
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    provenance = {
        "conversation_response_id": "resp-0",
        "provenance_record_id": "row-1",
        "tool_call_id": "tc-1",
        "source_type": "web_page",
        "identifier": "https://a.example",
        "agent": "main",
    }
    credits = {
        "conversation_response_id": "resp-0",
        "token_usage": {"by_model": {"m": usage}},
        "total_credits": 1.0,
        "created_at": "t0",
    }
    facts = {
        "merge": {
            "anchors": [
                ["tool_calls", "tc-1"],
                ["tool_call_result", "tc-1"],
                ["message_chunk", "main", "ai-2", "text"],
            ],
            "signals": [
                _archived("context_window", at=0, action="token_usage"),
                _archived("model_fallback", at=1, to_model="f"),
                _archived("provenance", identifier="https://a.example"),
                _archived("steering_delivered", count=1),
                _archived("context_window", at=2, action="token_usage"),
                _archived("interrupt", interrupt_id="int-1"),
                _archived("credit_usage", total_credits=1.0),
            ],
        }
    }

    items = (
        await _replayed(
            {0: _legacy_row(0, facts)}, provenance=[provenance], usages=[credits]
        )
    )[0]

    assert _tagged(items) == [
        ("user_message", None),
        ("message_chunk", "ai-1"),
        ("tool_calls", None),
        ("context_window", "archived"),
        ("tool_call_result", "tc-1"),
        ("model_fallback", "archived"),
        ("provenance", "archived"),
        ("steering_delivered", "archived"),
        ("message_chunk", "ai-2"),
        ("context_window", "archived"),
        ("interrupt", "archived"),
        ("credit_usage", "archived"),
    ]


@pytest.mark.asyncio
async def test_a_verbatim_main_lane_keeps_its_run_lanes_and_leaves_its_facts_alone(
    monkeypatch,
):
    """The archive held the root stream alone, so a turn whose main lane is
    held verbatim still takes its run lanes from the projection, and the task
    card the lane holds is stamped with the runs they contain. Replay stamps
    the rows it places, so those are copies: the row's facts stay as read for
    every later projection of the turn."""
    _cache_probe(monkeypatch)
    _task_reader(monkeypatch)
    card = {
        "agent": "main",
        "artifact_type": "task",
        "artifact_id": "task:tsk1",
        "payload": {"task_id": "tsk1", "action": "init", "task_run_id": "run-1"},
    }
    facts = {
        "verbatim": [
            _text("lc-1", "![a](https://cdn/1/chart.png)"),
            {"event": "artifact", "data": card},
            {
                "event": "context_window",
                "data": {"agent": "main", "action": "token_usage", "input_tokens": 9},
            },
        ]
    }
    row = _legacy_row(0, facts)
    held = copy.deepcopy(row["replay_facts"])

    items = (await _replayed({0: row}))[0]

    assert [
        (i["event"], i["data"].get("content"))
        for i in items
        if str(i["data"].get("agent", "")).startswith("task:")
    ] == [("user_message", "p"), ("message_chunk", "see ![c](work/sub.png)")]
    (stamped,) = [i["data"] for i in items if i["event"] == "artifact"]
    assert stamped["payload"]["projected_run_started_ms"] == (
        datetime(2026, 1, 3, tzinfo=timezone.utc).timestamp() * 1000
    )
    assert row["replay_facts"] == held


@pytest.mark.asyncio
async def test_a_compact_after_a_legacy_turn_follows_its_facts(monkeypatch):
    """The compact is a context_window row, a kind the facts replace, and
    only the checkpoint keeps it, so it is added after they are applied. It
    still precedes the error the row records, which ends the turn."""
    from ptc_agent.agent.middleware.compaction.utils import build_summary_message
    from src.server.services.history.projector import context_window_record

    summary = build_summary_message("condensed", None, original_message_count=9)
    compact = context_window_record(
        {"action": "summarize", "signal": "complete"},
        summary_message_id=summary.id,
    )
    turn = _turn(0, [AIMessage(content="answer", id="ai-0")], new_ui_records=[compact])
    turn.new_summarization_event = {"summary_message": summary, "cutoff_index": 4}
    _mock_reader(monkeypatch, ThreadHistory(thread_id=THREAD, turns=[turn]))
    facts = {
        "merge": {
            "anchors": [["message_chunk", "main", "ai-0", "text"]],
            "signals": [_archived("context_window", at=0, action="token_usage")],
        }
    }
    row = _legacy_row(0, facts, status="error", errors=["provider exploded"])

    items = (await _replayed({0: row}))[0]

    assert _tagged(items) == [
        ("user_message", None),
        ("message_chunk", "ai-0"),
        ("context_window", "archived"),
        ("context_window", None),
        ("error", None),
    ]
    assert items[3]["data"]["summary_text"] == "condensed"


def _stop_close(message_id):
    """The close a live stream wrote for the message open at a stop."""
    return {
        "event": "message_chunk",
        "data": {
            "agent": "main",
            "id": message_id,
            "role": "assistant",
            "finish_reason": "stopped",
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verbatim",
    [
        [_text("lc-1", "The answer is"), _stop_close("lc-1")],
        [_text("lc-1", "The answer is")],
    ],
    ids=["close_held", "close_owed"],
)
async def test_a_stopped_turn_held_verbatim_closes_its_last_message_once(
    monkeypatch, verbatim
):
    """A stop mid-answer streamed a close, which the verbatim lane holds; a
    stop between messages streamed none, and the row owes one. Either way
    the lane's last message gets one close, under the id the lane gave it."""
    _mock_reader(
        monkeypatch,
        ThreadHistory(
            thread_id=THREAD,
            turns=[_turn(0, [AIMessage(content="The answer is", id="ai-1")])],
        ),
    )
    row = _legacy_row(
        0,
        {"verbatim": verbatim},
        status="cancelled",
        metadata={"cancelled_by_user": True},
    )

    items = (await _replayed({0: row}))[0]

    closes = [i for i in items if i["data"].get("finish_reason") == "stopped"]
    assert [i["data"]["id"] for i in closes] == ["lc-1"]
    assert items[-1] is closes[0]


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
def test_only_this_versions_facts_are_read(replay_facts, expected):
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
# legacy row meets a projector change: what the facts carry stays with
# the rows it was resolved onto.


def _message(message_id):
    return [
        _row("message_chunk", agent="main", id=message_id, content_type="reasoning_signal", content="start"),
        _row("message_chunk", agent="main", id=message_id, content_type="reasoning", content="r"),
        _row("message_chunk", agent="main", id=message_id, content_type="reasoning_signal", content="complete"),
        _row("message_chunk", agent="main", id=message_id, content_type="text", content="t"),
    ]


def _message_keys(message_id):
    """The anchors ``_message``'s rows streamed as."""
    return [
        ["message_chunk", "main", message_id, content_type]
        for content_type in ("reasoning_signal", "reasoning", "reasoning_signal", "text")
    ]


def _call(call_id):
    return [
        _row("tool_calls", agent="main", tool_calls=[{"id": call_id}]),
        _row("tool_call_result", agent="main", tool_call_id=call_id, content="ok"),
    ]


def _applied(now, merge):
    return legacy.apply_turn(copy.deepcopy(now), [], {"v": legacy.FACTS_VERSION, "merge": merge})


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
    # Resolved against p0, A, p1.
    merge = {
        "durations": [
            [["message_chunk", "main", "p0", "reasoning_signal"], 1000],
            [["message_chunk", "main", "p1", "reasoning_signal"], 2000],
        ],
        "anchors": [
            *_message_keys("p0"),
            ["tool_calls", "A"],
            ["tool_call_result", "A"],
            *_message_keys("p1"),
        ],
        "signals": [
            {"event": "context_window", "data": {"agent": "main", "action": "token_usage"}, "at": 9}
        ],
    }
    now = [*_message("p0"), *_call("A"), *_message("px"), *_message("p1")]

    assert _shape(_applied(now, merge)) == [
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
    # Resolved against p0, p1.
    merge = {
        "anchors": [*_message_keys("p0"), *_message_keys("p1")],
        "signals": [{"event": "credit_usage", "data": {"agent": "main"}, "at": 7}],
    }

    assert _shape(_applied(now, merge)) == expected


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
    """Resolved against widgets tc-a and tc-b. The third payload streamed
    beyond what the projection then emitted: placed where it streamed, or
    taken by a widget that appears after its neighbours' widgets, never by
    one ahead of them."""
    merge = {
        "widgets": [["tc-a", _stored_widget(0)], ["tc-b", _stored_widget(1)], [None, _stored_widget(2)]],
        "anchors": [["artifact", "tc-a"], ["artifact", "tc-b"], ["widget", 2]],
        "signals": [
            {"widget": 0},
            {"widget": 1, "at": 0},
            {"widget": 2, "at": 1},
            {"event": "provenance", "data": {"agent": "main"}, "at": 2},
        ],
    }

    assert _shape(_applied(now, merge)) == expected


# ------------------------------------------------------------ re-read


@pytest.mark.asyncio
async def test_only_a_row_read_without_its_facts_is_read_again(monkeypatch):
    """Replay rows arrive without ``replay_facts``, so a turn projected
    afresh re-reads its row for them; a row that already holds the key is
    projected as it is."""
    whole = {
        "conversation_response_id": "resp-0",
        "replay_facts": {LEGACY_KEY: {"v": legacy.FACTS_VERSION}},
    }
    read = AsyncMock(return_value={"resp-0": whole})
    monkeypatch.setattr(replay_rows, "get_replay_responses", read)
    rows = {0: {"conversation_response_id": "resp-0"}, 1: _response(1)}

    out = await replay_rows.with_replay_facts(rows, [0, 1])

    read.assert_awaited_once_with(["resp-0"])
    assert out[0] is whole
    assert out[1] is rows[1]
