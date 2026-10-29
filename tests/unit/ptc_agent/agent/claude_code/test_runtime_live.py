"""The runtime on a real Claude login: this machine's own (``claude auth login``).

Integration tests, deselected by default. No sandbox and no command runs: the
one tool is a stub. Each turn spawns the bundled CLI on the machine's login, so
they need the ``claude-code`` extra and a logged-in machine, and skip without one:

    uv run --extra claude-code pytest -m integration -s tests/unit/ptc_agent/agent/claude_code/test_runtime_live.py

``CLAUDE_CODE_LIVE_MODEL`` picks the model (default ``claude-haiku-5-5``). With
``-s`` each test prints the usage it saw.
"""

from __future__ import annotations

import dataclasses
import json
import os
import uuid
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

from .harness import new_graph, run_turn, spec

pytestmark = [pytest.mark.integration, pytest.mark.enable_socket, pytest.mark.asyncio]

SPEC = spec(os.environ.get("CLAUDE_CODE_LIVE_MODEL", "claude-haiku-5-5"))


async def turn(graph, thread: str, messages: list, callbacks: list | None = None, runtime_spec=SPEC) -> dict:
    launcher = LocalLauncher(machine_login=True)
    status = await launcher.auth_status()
    if not status.get("loggedIn"):
        pytest.skip(f"the CLI a turn spawns is not logged in ({status.get('authMethod')})")
    return await run_turn(runtime_spec, launcher, graph, thread, messages, callbacks)


def usage(values: dict) -> list[dict]:
    seen, out = set(), []
    for e in values["claude_session"]["entries"]:
        msg = e.get("message") or {}
        if e.get("type") == "assistant" and msg.get("usage") and msg.get("id") not in seen:
            seen.add(msg["id"])
            u = msg["usage"]
            out.append({k: u.get(k) for k in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens")})
    return out


async def test_a_tool_call_reaches_the_tool_node_and_its_result_reaches_claude():
    values = await turn(new_graph(), "live-tools", [HumanMessage(
        "Call the Bash tool once with the command `echo live-42`, then reply with exactly what it returned.")])
    calls = [c for m in values["messages"] if isinstance(m, AIMessage) for c in m.tool_calls]
    results = [m for m in values["messages"] if isinstance(m, ToolMessage)]
    print(json.dumps({"tool_calls": [c["name"] for c in calls], "reply": values["messages"][-1].text[:300]}))
    assert calls and calls[0]["name"] == "Bash"
    assert results and results[0].tool_call_id == calls[0]["id"]
    assert "live-42" in values["messages"][-1].text


async def test_the_next_turn_reads_the_whole_of_the_last_turns_cache_and_leaves_nothing_in_the_users_claude_dir():
    """Cold: a fresh nonce opens the system prompt, so only turn 1 can have written what turn 2 reads.

    Turn 1 makes a tool call. A first turn that is a single plain reply without
    thinking costs turn 2 a full cache write on the API's side, which nothing
    the runtime sends changes, so it would prove nothing here.
    """
    nonce = uuid.uuid4().hex
    cold = dataclasses.replace(SPEC, system_prompt=f"Run {nonce}. {SPEC.system_prompt}")
    graph, thread = new_graph(), f"live-resume-{nonce[:8]}"
    first = await turn(graph, thread, [HumanMessage(
        "Call the Bash tool once with the command `echo heron`, then reply with OK and nothing else.")], runtime_spec=cold)
    second = await turn(graph, thread, [HumanMessage("What word did the command print? Reply with the word only.")], runtime_spec=cold)
    earlier, later = usage(first), usage(second)[len(usage(first)):]
    print(json.dumps({"turn1": earlier, "turn2": later, "reply": second["messages"][-1].text[:200]}))
    assert any(m.tool_calls for m in first["messages"] if isinstance(m, AIMessage))
    assert second["claude_session"]["session_id"] == first["claude_session"]["session_id"]
    assert "heron" in second["messages"][-1].text.lower()
    written = (earlier[-1]["cache_read_input_tokens"] or 0) + (earlier[-1]["cache_creation_input_tokens"] or 0)
    assert later and (later[0]["cache_read_input_tokens"] or 0) >= written > 0
    assert not list((Path.home() / ".claude" / "projects").glob("*langalpha-claude-live-resume*"))


async def test_messages_added_since_claude_spoke_go_as_one_message_of_blocks():
    row = "<context>The user's favourite colour is teal.</context>"
    values = await turn(new_graph(), "live-blocks", [HumanMessage(row), HumanMessage("What is my favourite colour? One word.")])
    sent = [e["message"]["content"] for e in values["claude_session"]["entries"]
            if e.get("type") == "user" and isinstance((e.get("message") or {}).get("content"), list)]
    assert any(len(blocks) >= 2 and blocks[0].get("text") == row for blocks in sent)
    assert "teal" in values["messages"][-1].text.lower()


async def test_usage_is_recorded_under_the_stamp_the_spec_carries():
    tracker = PerCallTokenTracker()
    await turn(new_graph(), "live-billing", [HumanMessage("Reply with OK and nothing else.")], callbacks=[tracker])
    records = tracker.get_per_call_records()
    print(json.dumps([{k: r.get(k) for k in ("billing_type", "model_name", "usage")} for r in records], default=str))
    assert records and all(r["billing_type"] == "oauth" and r["model_name"] == SPEC.billing["manifest_model"] for r in records)
