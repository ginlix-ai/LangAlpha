"""offload_tool_args must write in place, not REMOVE_ALL (compaction Bug B).

The old offload returned ``[RemoveMessage(REMOVE_ALL_MESSAGES), *full_list]`` —
a blanket reset + full rebuild. If that ran concurrently with a live turn (e.g.
an offload during a Redis outage that bypassed the admission gate), it rebuilt
from a stale snapshot and silently wiped any message the live turn appended in
between. The fix returns only the messages truncation actually changed, keyed by
their existing ids, so the DeltaChannel reducer overwrites them in place and
concurrently-appended messages survive.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage

from ptc_agent.agent.middleware.compaction.compact import offload_tool_args
from ptc_agent.agent.state import messages_delta_reducer


def _big_write_ai(idx: int) -> AIMessage:
    """An AIMessage with a Write tool call whose `content` exceeds the 2000-char
    truncation threshold, so offload truncates it."""
    return AIMessage(
        content="",
        id=f"a{idx}",
        tool_calls=[
            {
                "name": "Write",
                "args": {"file_path": "/x", "content": "Z" * 3000},
                "id": f"tc{idx}",
                "type": "tool_call",
            }
        ],
    )


def _conversation() -> list:
    # 1 truncatable AIMessage up front + 25 fillers so cutoff (= len - 20) > 0
    # and the AIMessage sits before it.
    return [_big_write_ai(0)] + [
        HumanMessage(content=f"m{i}", id=f"h{i}") for i in range(25)
    ]


@pytest.mark.asyncio
async def test_offload_returns_only_changed_messages_no_remove_all():
    msgs = _conversation()
    result = await offload_tool_args(messages=msgs, backend=None)
    changed = result["messages"]

    # No blanket reset in the write.
    assert all(not isinstance(m, RemoveMessage) for m in changed)
    # Only the truncated AIMessage is returned, keyed by its existing id.
    assert [m.id for m in changed] == ["a0"]
    assert result["offloaded_args"] == 1


@pytest.mark.asyncio
async def test_offload_write_preserves_concurrent_append():
    msgs = _conversation()
    result = await offload_tool_args(messages=msgs, backend=None)
    changed = result["messages"]

    # `msgs` now carry stable ids (offload stamped them in place). Simulate the
    # live state: the committed messages PLUS one appended concurrently after the
    # offload took its snapshot.
    concurrent = HumanMessage(content="appended mid-offload", id="concurrent")
    live_state = [*msgs, concurrent]

    # Apply the offload write through the real reducer.
    new_state = messages_delta_reducer(live_state, [changed])

    ids = [m.id for m in new_state]
    # The concurrently-appended message survives — no REMOVE_ALL wipe.
    assert "concurrent" in ids
    # In-place overwrite: no duplication, no loss.
    assert len(new_state) == len(live_state)
    # The truncated message was overwritten in place.
    a0_new = next(m for m in new_state if m.id == "a0")
    assert "argument truncated" in a0_new.tool_calls[0]["args"]["content"]
