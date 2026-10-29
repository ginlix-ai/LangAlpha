"""An incremental render leaves the files a full render would.

A turn end renders only the segments whose shape changed since the stored
manifest and carries the rest over, so what it carries has to be what a full
render would have written, byte for byte: the mount keys its cache on the
digest. A rewritten turn (a regenerate, a truncation) renders again with
every turn after it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from ptc_agent.agent.middleware.large_result_eviction import TOO_LARGE_TOOL_MSG
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import TURN_ROW_KIND
from ptc_agent.agent.transcript import Window, build_directory, load_manifest
from ptc_agent.agent.transcript.render import message_turns


def _turn(n: int) -> list:
    call = f"call-{n}"
    return [
        HumanMessage(content=f"question {n}", id=f"h-{n}"),
        AIMessage(
            content="",
            id=f"a-{n}",
            tool_calls=[{"id": call, "name": "execute_code", "args": {"code": "1"}}],
        ),
        ToolMessage(content=f"result {n}", tool_call_id=call, id=f"t-{n}"),
        AIMessage(content="done", id=f"r-{n}"),
    ]


def _thread(turns: int) -> list:
    return [message for n in range(1, turns + 1) for message in _turn(n)]


def test_a_turn_end_renders_only_the_turn_that_grew():
    before = build_directory(_thread(3), window=Window())
    steering = HumanMessage(
        content="steer", id="h-3b", additional_kwargs={"lc_source": "steering"}
    )
    grown = [*_thread(3), steering]

    after = build_directory(grown, previous=load_manifest(before.manifest), window=Window())

    assert set(after.rendered) == {"turn-0003.jsonl"}
    full = build_directory(grown, window=Window())
    assert after.manifest == full.manifest
    assert after.files == full.files


def test_a_new_turn_renders_alone():
    before = build_directory(_thread(3), window=Window())
    after = build_directory(_thread(4), previous=load_manifest(before.manifest), window=Window())
    assert set(after.rendered) == {"turn-0004.jsonl"}
    assert after.files == build_directory(_thread(4), window=Window()).files


def test_a_rewritten_turn_renders_again_with_every_turn_after_it():
    before = build_directory(_thread(4), window=Window())
    rewritten = _thread(4)
    rewritten[7] = AIMessage(content="regenerated", id="r-2-new")

    after = build_directory(rewritten, previous=load_manifest(before.manifest), window=Window())

    assert set(after.rendered) == {"turn-0002.jsonl", "turn-0003.jsonl", "turn-0004.jsonl"}
    assert after.files == build_directory(rewritten, window=Window()).files


def test_a_message_replaced_under_its_id_with_same_length_text_renders_again():
    before = build_directory(_thread(3), window=Window())
    edited = _thread(3)
    # "question 2" -> "question 9": the reducer replaces by id, the length holds.
    edited[4] = HumanMessage(content="question 9", id="h-2")

    after = build_directory(edited, previous=load_manifest(before.manifest), window=Window())

    assert set(after.rendered) == {"turn-0002.jsonl", "turn-0003.jsonl"}
    assert b"question 9" in after.rendered["turn-0002.jsonl"]
    assert after.files == build_directory(edited, window=Window()).files


def test_a_tool_call_whose_args_changed_renders_again():
    before = build_directory(_thread(2), window=Window())
    edited = _thread(2)
    edited[1] = AIMessage(
        content="",
        id="a-1",
        tool_calls=[{"id": "call-1", "name": "execute_code", "args": {"code": "2"}}],
    )

    after = build_directory(edited, previous=load_manifest(before.manifest), window=Window())

    assert set(after.rendered) == {"turn-0001.jsonl", "turn-0002.jsonl"}
    assert b'"code": "2"' in after.rendered["turn-0001.jsonl"]
    assert after.files == build_directory(edited, window=Window()).files


def test_a_manifest_of_another_schema_renders_everything():
    before = load_manifest(build_directory(_thread(2), window=Window()).manifest)
    before["schema"] = 1
    after = build_directory(_thread(2), previous=before, window=Window())
    assert set(after.rendered) == {"turn-0001.jsonl", "turn-0002.jsonl"}


def test_a_message_ahead_of_the_first_user_message_stays_with_the_first_turn():
    """The chat API takes a client's ``system`` message ahead of the first
    user message. Given a turn of its own, it would leave an empty first file
    and number every real turn one too high, the compaction pointer's too."""
    thread = [SystemMessage(content="Answer tersely.", id="s-0"), *_thread(2)]

    directory = build_directory(thread, window=Window())

    assert set(directory.files) == {"turn-0001.jsonl", "turn-0002.jsonl"}
    first = json.loads(directory.rendered["turn-0001.jsonl"].splitlines()[0])
    assert (first["type"], first["text"]) == ("user", "question 1")
    turns = message_turns(thread, base=0)
    assert (turns["s-0"], turns["h-1"], turns["h-2"]) == (1, 1, 2)


def test_a_lone_surrogate_renders_as_the_checkpoint_stores_it():
    """Compaction renders the messages in hand, before the checkpoint
    serializer has replaced a lone surrogate a tool returned."""
    thread = _thread(2)
    thread[2] = ToolMessage(content="bad \ud800 byte", tool_call_id="call-1", id="t-1")

    before = build_directory(thread[:4], window=Window())
    after = build_directory(thread, previous=load_manifest(before.manifest), window=Window())

    assert b"bad ? byte" in before.rendered["turn-0001.jsonl"]
    assert set(after.rendered) == {"turn-0002.jsonl"}
    full = build_directory(thread, window=Window())
    assert after.manifest == full.manifest
    assert after.files == full.files


def test_the_turn_row_and_the_eviction_pointer_read_as_their_producers_write_them():
    opened = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)
    row = build_update_message(
        DurableUpdate(kind=TURN_ROW_KIND, schema_version=1, text="turn", created_at=opened)
    )
    row.id = "row-1"
    path = "/home/workspace/.agents/threads/11111111/large_tool_results/call-1.txt"
    pointer = TOO_LARGE_TOOL_MSG.format(
        tool_call_id="call-1", file_path=path, content_sample="1\tx"
    )
    messages = [
        HumanMessage(content="question", id="h-1"),
        row,
        AIMessage(
            content="",
            id="a-1",
            tool_calls=[{"id": "call-1", "name": "Bash", "args": {"command": "ls"}}],
        ),
        ToolMessage(content=pointer, tool_call_id="call-1", id="t-1"),
    ]

    data = build_directory(messages, window=Window()).rendered["turn-0001.jsonl"]
    events = [json.loads(line) for line in data.splitlines()]

    assert events[0]["type"] == "user"
    assert events[0]["at"] == opened.isoformat()
    [result] = [event for event in events if event["type"] == "tool_result"]
    assert result["evicted"] == path
