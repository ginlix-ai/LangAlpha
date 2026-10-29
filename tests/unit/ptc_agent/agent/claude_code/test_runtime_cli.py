"""The runtime end to end: the bundled claude CLI against a loopback Messages API.

No credential and no network: the launcher points the real CLI at
``fake_api.FakeMessagesAPI`` with a dummy key.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.runtime import TURN_KEY, ClaudeTurn, ClaudeTurnFailed
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

from .conftest import TEST_KEY
from .fake_api import FakeMessagesAPI
from .harness import new_graph, run_turn, spec

pytestmark = [pytest.mark.enable_socket, pytest.mark.asyncio]

SPEC = spec("claude-haiku-5-5")


async def turn(api: FakeMessagesAPI, graph, thread: str, messages: list, callbacks: list | None = None) -> dict:
    launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
    return await run_turn(SPEC, launcher, graph, thread, messages, callbacks)


async def test_a_tool_call_runs_through_the_tool_node_under_its_own_name():
    async with FakeMessagesAPI() as api:
        values = await turn(api, new_graph(), "t-tool", [HumanMessage("RUN_TOOL please")])
    ai_call, result, final = values["messages"][1:]
    assert isinstance(ai_call, AIMessage) and ai_call.tool_calls[0]["name"] == "Bash"
    assert isinstance(result, ToolMessage) and result.tool_call_id == ai_call.tool_calls[0]["id"]
    assert result.content == "ran: echo spike-$((6*7))"
    assert isinstance(final, AIMessage) and "ran: echo spike-$((6*7))" in final.text
    sent = api.model_requests[0]
    assert [t["name"] for t in sent["tools"]] == ["Bash"]
    assert not any("Attribution for git" in str(m.get("content")) for m in sent["messages"])
    assert values["claude_session"]["session_id"]


async def test_the_next_turn_resumes_claudes_session_from_the_checkpoint():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        first = await turn(api, graph, "t-resume", [HumanMessage("FACT: the code word is heron")])
        second = await turn(api, graph, "t-resume", [HumanMessage("RECALL it")])
    assert second["claude_session"]["session_id"] == first["claude_session"]["session_id"]
    assert "FACT: the code word is heron" in second["messages"][-1].text
    assert len(second["claude_session"]["entries"]) > len(first["claude_session"]["entries"])



async def test_a_run_recovered_after_its_tool_results_carries_on_from_them():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        await turn(api, graph, "t-recover", [HumanMessage("RUN_TOOL please")])
        history = [s async for s in graph.aget_state_history({"configurable": {"thread_id": "t-recover"}})]
        after_tools = next(s for s in history if s.next == ("model",) and isinstance(s.values["messages"][-1], ToolMessage))
        n = len(api.model_requests)
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        replay = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-recover")
        config = {"configurable": {"thread_id": "t-recover", "checkpoint_id": after_tools.config["configurable"]["checkpoint_id"],
                                   TURN_KEY: replay}}
        try:
            await asyncio.wait_for(graph.ainvoke(None, config), 60)
        finally:
            await replay.close()
        values = (await graph.aget_state({"configurable": {"thread_id": "t-recover"}})).values
        resumed = api.model_requests[n:]
    assert len(resumed) == 1
    assert any(b.get("type") == "tool_result" for b in resumed[0]["messages"][-1]["content"])
    assert isinstance(values["messages"][-1], AIMessage) and "ran: echo spike-$((6*7))" in values["messages"][-1].text


def _cacheable(request: dict) -> dict:
    """What the API matches a cached prefix on.

    ``cache_control`` markers move with the last breakpoint, a text block and the
    same string are one input, and the CLI's billing-header line in ``system`` is
    read by the API, not cached (it changes on every request, cache hits included),
    as are its request ``diagnostics``.
    """

    def blocks(content):
        content = [{"type": "text", "text": content}] if isinstance(content, str) else content
        return [{k: v for k, v in b.items() if k != "cache_control"} for b in content]

    system = [b for b in blocks(request["system"]) if not b.get("text", "").startswith("x-anthropic-billing-header:")]
    messages = [{**m, "content": blocks(m["content"])} for m in request["messages"]]
    return {**{k: v for k, v in request.items() if k != "diagnostics"}, "system": system, "messages": messages}


@pytest.mark.parametrize("first_party", [False, True], ids=["api-key", "plan-login"])
async def test_two_turns_of_a_thread_send_one_prefix_from_one_device(first_party):
    graph = new_graph()
    async with FakeMessagesAPI(first_party=first_party) as api:
        await turn(api, graph, "t-prefix", [HumanMessage("FACT: the code word is heron")])
        n = len(api.model_requests)
        await turn(api, graph, "t-prefix", [HumanMessage("RECALL it")])
        raw = api.model_requests[n]
        last, first = _cacheable(api.model_requests[n - 1]), _cacheable(raw)
    if first_party:
        assert "diagnostics" in raw and raw["system"][-1]["cache_control"]["ttl"] == "1h"
    devices = {json.loads(r["metadata"]["user_id"])["device_id"] for r in (last, first)}
    keyed = hmac.new(TEST_KEY.encode(), b"claude-code-device:test-user", hashlib.sha256).hexdigest()
    assert devices == {keyed}
    assert {k: v for k, v in first.items() if k != "messages"} == {k: v for k, v in last.items() if k != "messages"}
    assert first["messages"][: len(last["messages"])] == last["messages"]


async def test_messages_added_since_claude_spoke_go_as_one_message_of_blocks():
    async with FakeMessagesAPI() as api:
        await turn(api, new_graph(), "t-blocks", [HumanMessage("<row>a runtime row</row>"), HumanMessage("ECHO me")])
    user = [m for m in api.model_requests[0]["messages"] if m["role"] == "user" and isinstance(m["content"], list)]
    texts = [[b.get("text") for b in m["content"]] for m in user]
    assert ["<row>a runtime row</row>", "ECHO me"] in texts


async def test_a_refused_request_fails_the_turn_and_commits_no_reply():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        with pytest.raises(ClaudeTurnFailed) as failed:
            await turn(api, graph, "t-refused", [HumanMessage("FAIL_AUTH")])
    assert failed.value.kind == "auth_failed"
    values = (await graph.aget_state({"configurable": {"thread_id": "t-refused"}})).values
    assert not any(isinstance(m, AIMessage) for m in values.get("messages", []))


async def test_usage_is_recorded_under_the_stamp_the_spec_carries():
    tracker = PerCallTokenTracker()
    async with FakeMessagesAPI() as api:
        await turn(api, new_graph(), "t-billing", [HumanMessage("hello")], callbacks=[tracker])
    (record,) = tracker.get_per_call_records()
    assert record["billing_type"] == "oauth"
    assert record["model_name"] == "claude-haiku-5-5-code"
    assert record["pricing_model_id"] == "claude-haiku-5-5"
    assert record["pricing_provider"] == "anthropic"
    assert record["usage"]


async def test_a_reply_cut_at_the_output_limit_is_committed_with_its_continuation():
    async with FakeMessagesAPI() as api:
        values = await turn(api, new_graph(), "t-maxtok", [HumanMessage("MAX_TOKENS please")])
    replies = [m for m in values["messages"] if isinstance(m, AIMessage)]
    assert len(replies) == 2 and replies[0].text == "part one"
    ids = {m.response_metadata["claude_message_id"] for m in replies}
    recorded = {(e.get("message") or {}).get("id") for e in values["claude_session"]["entries"] if e.get("type") == "assistant"}
    assert recorded == ids


async def test_an_at_path_in_user_text_reaches_claude_as_written(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("do-not-read-7731")
    async with FakeMessagesAPI() as api:
        await turn(api, new_graph(), "t-atpath", [HumanMessage(f"ECHO @{secret}")])
    assert "do-not-read-7731" not in json.dumps(api.model_requests)

