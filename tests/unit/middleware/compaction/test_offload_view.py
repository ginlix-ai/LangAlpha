"""Tier 1 offloads are a view over the checkpoint, applied from recorded ids.

The id sets are the only record, every model call re-applies them, and nothing
rewrites messages. New ids join only at the start of a turn after a long
pause. A cut argument keeps no second copy: it points at its call in the
transcript, so it is cut only where a transcript is saved.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer, offload_tool_args
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.offloading import (
    apply_recorded_offloads,
    record_offloads,
)
from ptc_agent.agent.middleware.compaction.types import OffloadSettings
from ptc_agent.agent.transcript import TranscriptTarget, Window
from ptc_agent.agent.transcript.pointer import TranscriptTurns
from ptc_agent.config.agent import CompactionConfig

THREAD = "abcd1234-0000-0000-0000-000000000000"
TURN_FILE = ".agents/transcripts/abcd1234/turn-0001.jsonl"


def _history(pairs: int) -> list:
    msgs = [HumanMessage("go", id="h0")]
    for i in range(pairs):
        msgs.append(
            AIMessage(
                "",
                id=f"a{i}",
                tool_calls=[
                    {"name": "Write", "id": f"w{i}", "args": {"content": "x" * 5000}},
                    {"name": "Read", "id": f"r{i}", "args": {"file_path": "same.py"}},
                ],
            )
        )
        msgs.append(ToolMessage("wrote", tool_call_id=f"w{i}", id=f"tw{i}"))
        msgs.append(ToolMessage("body", tool_call_id=f"r{i}", id=f"tr{i}"))
    return msgs


class _Mount:
    def __init__(self, saves: bool = True):
        self.saves = saves
        self.saved: list[int] = []

    async def save_transcript(self, target, messages, *, window):
        self.saved.append(len(messages))
        return self.saves


def _sandbox(saves: bool = True, linked: bool = True):
    mount = _Mount(saves)

    async def settled_livefs(workspace_id=None):
        return mount if linked else None

    return SimpleNamespace(livefs=mount, settled_livefs=settled_livefs)


def _writes(messages: list) -> dict[str, str]:
    return {
        tc["id"]: tc["args"]["content"]
        for m in messages
        if isinstance(m, AIMessage)
        for tc in m.tool_calls
        if tc["name"] == "Write"
    }


@pytest.fixture
def in_thread(monkeypatch):
    monkeypatch.setattr(
        mw_module,
        "get_config",
        lambda: {"configurable": {"thread_id": THREAD, "checkpoint_ns": ""}},
    )


def _middleware(
    backend=None, idle_minutes=90, offload=None, token_threshold=10_000_000
) -> CompactionMiddleware:
    if offload is None:
        offload = OffloadSettings(
            keep_messages=10,
            max_length=200,
            idle_seconds=None if idle_minutes is None else idle_minutes * 60,
        )
    return CompactionMiddleware(
        # The default counter downloads its encoding on first use.
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000, counter=len),
        token_threshold=token_threshold,
        keep_messages=5,
        offload=offload,
        backend=backend,
    )


def _state(msgs, idle_minutes: float | None, **extra) -> dict:
    state = {"messages": msgs, **extra}
    if idle_minutes is not None:
        state["_last_model_response_at"] = time.time() - idle_minutes * 60
    return state


async def _call(mw, msgs, state, seen):
    async def handler(req):
        seen.append(_writes(req.messages))
        return ModelResponse(result=[AIMessage("done")])

    req = ModelRequest(
        model=mw._summarizer.model,
        messages=msgs,
        system_message=None,
        tool_choice=None,
        tools=[],
        response_format=None,
        state=state,
        runtime=None,
    )
    return (await mw.awrap_model_call(req, handler)).command.update


@pytest.mark.asyncio
async def test_offload_records_ids_and_never_rewrites_messages():
    msgs = _history(12)
    before = [m.model_dump_json() for m in msgs]

    args, reads = await offload_tool_args(
        msgs, {}, OffloadSettings(), thread_id=THREAD, backend=_sandbox()
    )

    assert [m.model_dump_json() for m in msgs] == before
    assert args and reads
    assert args.isdisjoint(reads)

    with pytest.raises(ValueError, match="Nothing to offload"):
        await offload_tool_args(
            msgs,
            record_offloads({}, args, reads),
            OffloadSettings(),
            thread_id=THREAD,
            backend=_sandbox(),
        )


@pytest.mark.asyncio
async def test_tier1_runs_only_at_the_start_of_a_turn_after_a_long_pause(in_thread):
    # Mid-turn trimming threw away a warm prompt cache; after the pause the
    # cache is cold, so the turn start is the one place a trim is free.
    mw = _middleware(_sandbox())
    msgs = _history(14)

    assert await mw.abefore_agent(_state(msgs, 89), None) is None
    assert await mw.abefore_agent(_state(msgs, None), None) is None
    assert await _middleware(_sandbox(), None).abefore_agent(
        _state(msgs, 600), None
    ) is None

    update = await mw.abefore_agent(_state(msgs, 91), None)
    args, reads = update["_offloaded_tool_call_ids"], update["_offloaded_read_result_ids"]
    # The newest 10 messages are kept: a10 sits just before them, its Read
    # result just inside.
    assert args == {f"w{i}" for i in range(11)}
    assert reads == {f"r{i}" for i in range(10)}

    # Model calls only re-apply what was recorded, and stamp the answer time.
    seen: list[dict] = []
    before = time.time()
    after_call = await _call(mw, msgs + _history(16)[len(msgs):], update, seen)
    assert "_offloaded_tool_call_ids" not in after_call
    assert after_call["_last_model_response_at"] >= before
    assert {k for k, v in seen[0].items() if len(v) < 5000} == args


@pytest.mark.asyncio
async def test_new_cuts_drop_the_token_count_measured_before_them(in_thread):
    # The last turn's final call measured the view before the cuts; read as
    # the size of the cut view, it would compact a context they brought under.
    mw = _middleware(_sandbox(), token_threshold=1_000)
    msgs = _history(14)
    state = _state(msgs, 91, _cached_input_tokens=5_000, _cached_output_tokens=100)

    update = await mw.abefore_agent(state, None)
    assert update["_cached_input_tokens"] == update["_cached_output_tokens"] == 0

    after_call = await _call(mw, msgs, {**state, **update}, [])
    assert "_summarization_event" not in after_call


@pytest.mark.asyncio
async def test_cut_args_point_at_their_call_and_stay_cut(in_thread):
    sandbox = _sandbox()
    mw = _middleware(sandbox)
    seen: list[dict] = []

    update = await mw.abefore_agent(_state(_history(14), 120), None)
    await _call(mw, _history(14), update, seen)
    await _call(mw, _history(15), update, seen)

    assert sandbox.livefs.saved, "the transcript is saved before args are cut"
    cut = seen[0]["w0"]
    assert cut.startswith("x" * 20) and len(cut) < 400
    assert TURN_FILE in cut and '.call_id=="w0"' in cut
    assert seen[1]["w0"] == cut


@pytest.mark.asyncio
async def test_recorded_cuts_show_whole_while_no_mount_serves(in_thread):
    # Ids recorded before this thread's args were kept in a transcript, or
    # before its mount went away, would name a file no command can read.
    sandbox = _sandbox()
    sandbox.livefs = None
    state = _state(_history(14), None, _offloaded_tool_call_ids={"w0", "w1"})
    seen: list[dict] = []

    await _call(_middleware(sandbox), _history(14), state, seen)

    assert seen[0]["w0"] == seen[0]["w1"] == "x" * 5000


@pytest.mark.asyncio
async def test_recorded_cuts_use_the_configured_length_with_tier1_off(in_thread):
    # Automatic Tier 1 off still leaves the cuts a manual /offload recorded,
    # chosen at the configured length, which the view has to apply as chosen.
    offload = OffloadSettings.from_config(
        CompactionConfig(truncate_args_idle_minutes=None, truncate_args_max_length=500)
    )
    msgs = [
        HumanMessage("go", id="h0"),
        AIMessage(
            "", id="a0", tool_calls=[{"name": "Write", "id": "w0", "args": {"content": "x" * 1000}}]
        ),
        ToolMessage("wrote", tool_call_id="w0", id="tw0"),
    ]
    seen: list[dict] = []

    await _call(
        _middleware(_sandbox(), offload=offload), msgs, {"_offloaded_tool_call_ids": {"w0"}}, seen
    )

    assert offload.idle_seconds is None
    assert TURN_FILE in seen[0]["w0"]


@pytest.mark.asyncio
async def test_args_stay_whole_without_a_saved_transcript(in_thread):
    # No transcript the agent can read (Flash, an unmounted sandbox, a failed
    # save, a folder whose links are not in): a cut argument would have no
    # copy left to read, so none is cut. Stale Read results are still
    # cleared, since the file they read is still there.
    for backend in (None, _sandbox(saves=False), _sandbox(linked=False)):
        update = await _middleware(backend).abefore_agent(
            _state(_history(14), 120), None
        )
        assert "_offloaded_tool_call_ids" not in update
        assert update["_offloaded_read_result_ids"]

    args, reads = await offload_tool_args(
        _history(12), {}, OffloadSettings(), thread_id=THREAD, backend=None
    )
    assert not args and reads

    with pytest.raises(RuntimeError, match="transcript"):
        await offload_tool_args(
            _history(12),
            {"_offloaded_read_result_ids": {f"r{i}" for i in range(12)}},
            OffloadSettings(),
            thread_id=THREAD,
            backend=_sandbox(saves=False),
        )


def test_legacy_merged_id_sets_only_hit_their_own_tool():
    # Older /offload merged arg and read ids into both sets.
    ids = {"w0", "r0"}
    msgs = _history(1)
    turns = TranscriptTurns.of(TranscriptTarget(THREAD), msgs, window=Window())
    out = {m.id: m for m in apply_recorded_offloads(msgs, ids, ids, 200, turns)}

    assert out["tw0"].content == "wrote"
    assert out["tr0"].content.startswith("... [this tool call's read result")
    assert TURN_FILE in out["a0"].tool_calls[0]["args"]["content"]
    assert out["a0"].tool_calls[1]["args"] == {"file_path": "same.py"}
