"""The runtime end to end: the bundled claude CLI against a loopback Messages API.

No credential and no network: the launcher points the real CLI at
``fake_api.FakeMessagesAPI`` with a dummy key or setup token.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import hmac
import base64
import json
import os
import shlex
from unittest import mock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import ToolNode

from ptc_agent.agent.claude_code.compaction import CompactionRules
from ptc_agent.agent.claude_code.launcher import LocalLauncher
from ptc_agent.agent.claude_code.runtime import (
    TURN_KEY,
    TURN_QUERIES,
    ClaudeTurn,
    ClaudeTurnFailed,
    build_claude_code_graph,
    token_works,
)
from ptc_agent.agent.middleware.steering import ALL_KINDS, SteeredReport, SteeringMiddleware, steers_ahead_message
from ptc_agent.agent.middleware.runtime_context.durable import DurableUpdate, build_update_message, render_update_row
from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

from .conftest import TEST_KEY
from .fake_api import TOKEN, FakeMessagesAPI
from .harness import new_graph, run_turn, spec

pytestmark = [pytest.mark.enable_socket, pytest.mark.asyncio]

SPEC = spec("claude-haiku-5-5")
PNG = base64.b64encode(bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000d49444154789c6360000002000154a24f5d0000000049454e44ae426082"
)).decode()
PDF = base64.b64encode(b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF").decode()
#: Past the SDK's default line limit (1 MiB) once the CLI writes it base64.
BIG_PDF = base64.b64encode(b"%PDF-1.4\n%" + b"x" * 1_200_000 + b"\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF").decode()


async def turn(api: FakeMessagesAPI, graph, thread: str, messages: list, callbacks: list | None = None,
               baseline=None) -> dict:
    launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, token=TOKEN if api.first_party else None)
    return await run_turn(SPEC, launcher, graph, thread, messages, callbacks, baseline)


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


@pytest.mark.parametrize("size", [60_000, 150_000])
async def test_a_tool_result_short_of_our_own_eviction_reaches_claude_whole(size):
    @tool("Bash")
    async def large(command: str) -> str:
        """Run a shell command."""
        return "x" * size

    async with FakeMessagesAPI() as api:
        await turn(api, build_claude_code_graph(ToolNode([large]), checkpointer=InMemorySaver()), f"t-large-{size}",
                   [HumanMessage("RUN_TOOL please")])
    (result,) = [b for m in api.model_requests[1]["messages"] if isinstance(m["content"], list)
                 for b in m["content"] if b.get("type") == "tool_result"]
    content = result["content"]
    text = content if isinstance(content, str) else "".join(b.get("text", "") for b in content)
    assert text == "x" * size


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


async def test_a_turn_hands_claude_its_rows_and_only_a_baseline_it_does_not_hold():
    row = DurableUpdate(kind="turn", schema_version=1, text="It is noon.")

    async def opens(state):
        return {"messages": [build_update_message(row)]}

    graph = new_graph(entry=[("opens", opens)])
    epochs = [("e1", "BASELINE one"), ("e1", "BASELINE one"), ("e2", "BASELINE two")]
    sent = []
    async with FakeMessagesAPI() as api:
        for n, epoch in enumerate(epochs):
            values = await turn(api, graph, "t-context", [HumanMessage(f"ECHO {n}")], baseline=lambda _s, e=epoch: e)
            opening = [m for m in api.model_requests[-1]["messages"] if m["role"] == "user"][-1]
            sent.append([b["text"] for b in opening["content"]])
    rows = [frame_reminder(render_update_row(row))]
    assert sent[0] == [frame_reminder("BASELINE one"), "ECHO 0", *rows]
    assert sent[1] == ["ECHO 1", *rows]
    assert sent[2] == [frame_reminder("BASELINE two"), "ECHO 2", *rows]
    assert values["claude_session"]["baseline"] == "e2"


async def test_a_session_keeps_the_prompt_and_tools_it_started_on_across_a_deploy():
    """Turn 2 runs on a build whose prompt and tools changed (a deploy between
    turns): it still sends turn 1's system bytes and tool list, which Claude's
    thinking is bound to. A new thread takes the new build's."""
    import dataclasses

    from langchain_core.tools import tool

    @tool("Grep")
    async def grep(pattern: str) -> str:
        """Search files."""
        return ""

    deployed = dataclasses.replace(SPEC, system_prompt="You are the redeployed test agent. Reply tersely.", tools=[*SPEC.tools, grep])
    graph = new_graph()

    def sent(request):
        return json.dumps(request["system"]), [t["name"] for t in request["tools"]]

    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        await run_turn(SPEC, launcher, graph, "t-pin", [HumanMessage("FACT: the code word is heron")])
        first = sent(api.model_requests[-1])
        n = len(api.model_requests)
        values = await run_turn(deployed, launcher, graph, "t-pin", [HumanMessage("RECALL it")])
        second = sent(api.model_requests[n])
        await run_turn(deployed, launcher, new_graph(), "t-pin-new", [HumanMessage("ECHO hi")])
        fresh = sent(api.model_requests[-1])
    assert SPEC.system_prompt in first[0] and second == first
    assert values["claude_session"]["pinned"]["system_prompt"] == SPEC.system_prompt
    assert deployed.system_prompt in fresh[0] and fresh[1] == ["Bash", "Grep"]


#: The CLI's own compaction set to fire once a turn's context passes about
#: 4,800 tokens: 6% of the smallest window it accepts, less its output reserve.
COMPACTS_EARLY = {"CLAUDE_CODE_AUTO_COMPACT_WINDOW": "100000", "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE": "6"}


async def test_a_session_keeps_its_prompt_and_tools_past_a_compaction_the_cli_made_mid_turn():
    """The CLI compacts between a tool result and the next request, re-records
    the prompt and tools the turn launched with, and replays that record on
    every later launch. So the turn after a deploy still sends the bytes the
    compacted turn's thinking was made under."""
    @tool("Bash")
    async def long_output(command: str) -> str:
        """Run a shell command."""
        return "result line\n" * 1000

    @tool("Grep")
    async def grep(pattern: str) -> str:
        """Search files."""
        return ""

    started = dataclasses.replace(SPEC, tools=[long_output])
    deployed = dataclasses.replace(started, system_prompt="You are the redeployed test agent. Reply tersely.", tools=[long_output, grep])
    graph = build_claude_code_graph(ToolNode([long_output, grep]), checkpointer=InMemorySaver())

    def sent(request):
        return json.dumps(request["system"][2:]), [t["name"] for t in request["tools"]]

    async with FakeMessagesAPI(first_party=True) as api:
        env = {**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}
        await run_turn(started, LocalLauncher(env=env, token=TOKEN), graph, "t-mid", [HumanMessage("FACT: the code word is heron")])
        compacting = await run_turn(started, LocalLauncher(env={**env, **COMPACTS_EARLY}, token=TOKEN), graph, "t-mid",
                                    [HumanMessage("RUN_TOOL please")])
        last = api.model_requests[-1]
        n = len(api.model_requests)
        values = await run_turn(deployed, LocalLauncher(env=env, token=TOKEN), graph, "t-mid", [HumanMessage("RECALL it")])
        after = api.model_requests[n]
    assert any(e.get("subtype") == "compact_boundary" for e in compacting["claude_session"]["entries"])
    assert any(b.get("type") == "thinking" for m in last["messages"] if isinstance(m["content"], list) for b in m["content"])
    assert sent(after) == sent(last) and started.system_prompt in sent(after)[0]
    assert values["claude_session"]["pinned"]["system_prompt"] == started.system_prompt


@pytest.mark.parametrize(("threshold", "compacts"), [(67_000, True), (0, False)])
async def test_the_cli_compacts_before_a_request_only_at_the_threshold_it_was_given(threshold, compacts):
    """On its default window the CLI never compacts before a request, so a
    turn's context would grow until a request was refused as too long. The
    turn before gives the summary something to summarize."""
    @tool("Bash")
    async def large(command: str) -> str:
        """Run a shell command."""
        return "result line\n" * 30_000

    async with FakeMessagesAPI(first_party=True) as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, token=TOKEN)
        given = dataclasses.replace(SPEC, tools=[large])
        rules = CompactionRules(0, threshold=threshold)
        graph = build_claude_code_graph(ToolNode([large]), checkpointer=InMemorySaver())
        await run_turn(given, launcher, graph, f"t-window-{threshold}", [HumanMessage("FACT: the code word is heron")], compaction=rules)
        values = await run_turn(given, launcher, graph, f"t-window-{threshold}", [HumanMessage("RUN_TOOL please")], compaction=rules)
    boundaries = [e for e in values["claude_session"]["entries"] if e.get("subtype") == "compact_boundary"]
    assert bool(boundaries) is compacts
    assert isinstance(values["messages"][-1], AIMessage)


async def test_each_reply_carries_and_streams_the_threshold_the_cli_compacts_at():
    """The live ring reads the event and replay reads the stamp, so both show
    the thread's threshold rather than a default."""
    from ptc_agent.agent.middleware.compaction.utils import context_threshold

    graph = new_graph()
    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        turn_ = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-usage", compaction=CompactionRules(0, threshold=500_000))
        config = {"configurable": {"thread_id": "t-usage", TURN_KEY: turn_}}
        try:
            events = [e async for e in graph.astream({"messages": [HumanMessage("RUN_TOOL please")]}, config, stream_mode="custom")]
        finally:
            await turn_.close()
        replies = [m for m in (await graph.aget_state(config)).values["messages"] if isinstance(m, AIMessage)]
    usage = [e for e in events if isinstance(e, dict) and e.get("action") == "token_usage"]
    assert len(replies) == 2 and [context_threshold(m) for m in replies] == [500_000, 500_000]
    assert [(e["threshold"], e["total_tokens"]) for e in usage] == [
        (500_000, m.usage_metadata["input_tokens"] + m.usage_metadata["output_tokens"]) for m in replies
    ]


async def test_a_session_due_a_new_pin_compacts_under_its_old_one_and_runs_the_turn_on_the_new_one():
    """The summary request is the session's own prefix with its thinking; the
    turn's requests then carry this build's prompt and tools and no thinking
    from before the summary, and the thinking they make goes out again on the
    next turn under the prefix it was made on."""
    @tool("Grep")
    async def grep(pattern: str) -> str:
        """Search files."""
        return ""

    class Due(CompactionRules):
        def repin_plan(self, records, current, now):
            return (True, "due") if records["pinned"]["system_prompt"] != current["system_prompt"] else None

    deployed = dataclasses.replace(SPEC, system_prompt="You are the redeployed test agent. Reply tersely.", tools=[*SPEC.tools, grep])
    graph = build_claude_code_graph(ToolNode([*SPEC.tools, grep]), checkpointer=InMemorySaver())

    def thinking(request):
        return [b["signature"] for m in request["messages"] if isinstance(m["content"], list) for b in m["content"] if b.get("type") == "thinking"]

    def made(values):
        return {b["signature"] for m in values["messages"] if isinstance(m, AIMessage)
                for b in m.content if isinstance(b, dict) and b.get("type") == "thinking"}

    async def turn_on(spec_, text):
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, token=TOKEN)
        turn_ = ClaudeTurn(spec_, launcher, identity="test-user", thread_key="t-repin", compaction=Due(1),
                           baseline=lambda _s: ("epoch-1", "The thread's baseline."))
        config = {"configurable": {"thread_id": "t-repin", TURN_KEY: turn_}}
        try:
            await graph.ainvoke({"messages": [HumanMessage(text)]}, config)
        finally:
            await turn_.close()
        return (await graph.aget_state(config)).values

    async with FakeMessagesAPI(first_party=True) as api:
        await turn_on(SPEC, "FACT: the code word is heron")
        prior = made(await turn_on(SPEC, "RUN_TOOL please"))
        before = api.model_requests[-1]
        n = len(api.model_requests)
        moved = await turn_on(deployed, "RECALL it")
        summary, after = api.model_requests[n:]
        again = await turn_on(deployed, "say hi")
        later = api.model_requests[-1]
    assert SPEC.system_prompt in json.dumps(summary["system"]) and summary["tools"] == before["tools"]
    assert thinking(summary) and set(thinking(summary)) <= set(thinking(before)) | prior
    assert deployed.system_prompt in json.dumps(after["system"]) and [t["name"] for t in after["tools"]] == ["Bash", "Grep"]
    assert not set(thinking(after)) & prior
    assert moved["messages"][-1].text.startswith("I remember")
    assert moved["claude_session"]["pinned"]["system_prompt"] == deployed.system_prompt
    assert sum(e.get("subtype") == "compact_boundary" for e in moved["claude_session"]["entries"]) == 1
    assert moved["claude_session"]["baseline"] == "epoch-1"
    assert "The thread's baseline." not in json.dumps(later["messages"][-1])
    assert later["system"][2:] == after["system"][2:] and later["tools"] == after["tools"]
    assert made(moved) - prior and made(moved) - prior <= set(thinking(later))
    assert again["claude_session"]["pinned"] == moved["claude_session"]["pinned"]


async def test_a_compaction_whose_summary_is_refused_fails_in_its_own_words_and_reports_the_token():
    """The CLI ends such a launch as a success, saying so only in its status."""
    from ptc_agent.agent.claude_code.compaction import compact

    graph = new_graph()
    async with FakeMessagesAPI(first_party=True) as api:
        env = {**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}
        launcher = LocalLauncher(env=env, token=TOKEN)
        await run_turn(SPEC, launcher, graph, "t-compact-refused", [HumanMessage("FACT: the code word is heron")])
        session = (await run_turn(SPEC, launcher, graph, "t-compact-refused", [HumanMessage("RUN_TOOL please")]))["claude_session"]
        api.refused = frozenset({TOKEN})
        saved = _SavedToken(TOKEN)
        with pytest.raises(ClaudeTurnFailed) as failed:
            await compact(SPEC, session["pinned"], LocalLauncher(env=env, saved=saved), thread_key="t-compact-refused",
                          identity="test-user", records=session)
    assert failed.value.kind == "auth_failed" and "401" in str(failed.value)
    assert saved.refusals == [TOKEN]


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


def _machine_login(home) -> tuple[str, str]:
    """A Claude Code login of the machine's own, where the CLI keeps one on Linux; its token and the file's hash."""
    machine = "sk-ant-oat01-" + "9" * 80
    creds = home / ".claude" / ".credentials.json"
    creds.parent.mkdir(parents=True)
    creds.write_text(json.dumps({"claudeAiOauth": {
        "accessToken": machine, "refreshToken": "sk-ant-ort01-machine", "expiresAt": 4102444800000, "scopes": ["user:inference"]}}))
    return machine, hashlib.sha256(creds.read_bytes()).hexdigest()


async def test_a_turn_never_signs_in_with_the_machines_own_login(monkeypatch, tmp_path):
    machine, before = _machine_login(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    graph = new_graph()
    async with FakeMessagesAPI(first_party=True) as api:
        signed_out = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        with pytest.raises(ClaudeTurnFailed) as failed:
            await run_turn(SPEC, signed_out, graph, "t-machine", [HumanMessage("ECHO hi")])
        await turn(api, graph, "t-machine", [HumanMessage("ECHO hi")])
        assert await token_works(LocalLauncher(env=api.cli_env(), token=TOKEN), "claude-haiku-5-5")
    assert failed.value.kind == "auth_failed"
    assert api.credentials and set(api.credentials) == {TOKEN} and machine not in api.credentials
    creds = tmp_path / ".claude" / ".credentials.json"
    assert hashlib.sha256(creds.read_bytes()).hexdigest() == before
    assert [p.name for p in (tmp_path / ".claude").iterdir()] == [".credentials.json"]


class _SavedToken:
    """A user's saved token as a turn's launcher reads it, and the refusals it is told of."""

    def __init__(self, token: str) -> None:
        self.token, self.refusals = token, []

    async def read(self) -> str:
        return self.token

    async def refused(self, token: str) -> None:
        self.refusals.append(token)


async def test_a_turn_runs_on_the_saved_token_and_reports_one_anthropic_refuses():
    revoked = "sk-ant-oat01-revoked"
    good, bad = _SavedToken(TOKEN), _SavedToken(revoked)
    async with FakeMessagesAPI(first_party=True, refused=frozenset({revoked})) as api:
        env = {**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}
        await run_turn(SPEC, LocalLauncher(env=env, saved=good), new_graph(), "t-saved", [HumanMessage("ECHO hi")])
        with pytest.raises(ClaudeTurnFailed) as failed:
            await run_turn(SPEC, LocalLauncher(env=env, saved=bad), new_graph(), "t-revoked", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "auth_failed"
    assert (good.refusals, bad.refusals) == ([], [revoked])
    assert api.credentials[0] == TOKEN and set(api.credentials[1:]) == {revoked}


async def test_the_token_check_asks_anthropic_and_tells_a_refused_token_from_a_good_one():
    async with FakeMessagesAPI(first_party=True, refused=frozenset({"sk-ant-oat01-revoked"})) as api:
        good = await token_works(LocalLauncher(env=api.cli_env(), token=TOKEN), "claude-haiku-5-5")
        revoked = await token_works(LocalLauncher(env=api.cli_env(), token="sk-ant-oat01-revoked"), "claude-haiku-5-5")
    assert (good, revoked) == (True, False)
    assert api.credentials[0] == TOKEN and set(api.credentials[1:]) == {"sk-ant-oat01-revoked"}


def _children() -> list[str]:
    me, found = str(os.getpid()), []
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            with open(f"/proc/{pid}/stat") as stat:
                if stat.read().rsplit(")", 1)[1].split()[1] == me:
                    found.append(pid)
        except (OSError, IndexError):
            pass
    return found


@pytest.mark.skipif(not os.path.isdir("/proc"), reason="reads child processes from /proc")
async def test_the_token_check_has_stopped_its_cli_when_it_answers():
    before = _children()
    async with FakeMessagesAPI(first_party=True) as api:
        assert await token_works(LocalLauncher(env=api.cli_env(), token=TOKEN), "claude-haiku-5-5")
        assert _children() == before


@dataclasses.dataclass
class _Configured(LocalLauncher):
    """A launcher whose CLI also takes ``settings`` as flag settings, which the
    CLI applies as it does a policy's: they stand in for one no file on the
    machine shows, such as an organization's account's."""

    settings: dict = dataclasses.field(default_factory=dict)

    def transport(self, options):
        merged = {**json.loads(options.settings or "{}"), **self.settings}
        return super().transport(dataclasses.replace(options, settings=json.dumps(merged)))


def _tool_results_sent(api: FakeMessagesAPI) -> dict[str, tuple]:
    return {b["tool_use_id"]: (b["content"], b.get("is_error")) for m in api.model_requests[-1]["messages"]
            if isinstance(m["content"], list) for b in m["content"] if b.get("type") == "tool_result"}


async def test_a_call_the_cli_refuses_never_runs_and_keeps_the_answer_claude_was_given():
    ran = []

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        ran.append(command)
        return f"ran: {command}"

    async with FakeMessagesAPI() as api:
        values = await turn(api, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()), "t-refused-call",
                            [HumanMessage("CALLS Nope:never Bash:one")])
        sent = _tool_results_sent(api)
    call, refused, result = values["messages"][1:4]
    assert [c["name"] for c in call.tool_calls] == ["Nope", "Bash"] and ran == ["one"]
    assert refused.tool_call_id == call.tool_calls[0]["id"]
    assert (refused.content, refused.status) == (sent[refused.tool_call_id][0], "error") and sent[refused.tool_call_id][1]
    assert "No such tool available: Nope" in refused.content
    assert (result.content, result.status) == ("ran: one", "success")


async def test_calls_run_as_the_cli_asks_for_them_one_at_a_time_and_read_only_ones_together():
    events = []

    def recording(name: str):
        @tool(name)
        async def run(command: str) -> str:
            """Run a command."""
            events.append(("start", command))
            await asyncio.sleep(0.3)
            events.append(("end", command))
            return f"{name}: {command}"

        return run

    bash, grep = recording("Bash"), recording("Grep")
    spec_ = dataclasses.replace(SPEC, tools=[bash, grep], read_only=frozenset({"Grep"}))
    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        values = await run_turn(spec_, launcher, build_claude_code_graph(ToolNode([bash, grep]), checkpointer=InMemorySaver()),
                                "t-serial", [HumanMessage("CALLS Bash:one Bash:two Grep:a Grep:b")])
    assert events[:4] == [("start", "one"), ("end", "one"), ("start", "two"), ("end", "two")]
    assert set(events[4:6]) == {("start", "a"), ("start", "b")}
    results = [m for m in values["messages"] if isinstance(m, ToolMessage)]
    assert [m.content for m in results] == ["Bash: one", "Bash: two", "Grep: a", "Grep: b"]


async def test_a_model_the_machines_settings_do_not_allow_fails_the_turn_before_any_reply():
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        launcher = _Configured(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, settings={"availableModels": ["claude-sonnet-5-5"]})
        with pytest.raises(ClaudeTurnFailed) as failed:
            await run_turn(SPEC, launcher, graph, "t-not-allowed", [HumanMessage("ECHO hi")])
    assert failed.value.kind == "model_unavailable"
    assert str(failed.value) == "Your organization's Claude Code policy does not allow claude-haiku-5-5. Pick another model."
    values = (await graph.aget_state({"configurable": {"thread_id": "t-not-allowed"}})).values
    assert not any(isinstance(m, AIMessage) for m in values.get("messages", []))


async def test_a_cli_that_exits_at_startup_fails_the_turn_in_its_own_words(tmp_path, caplog):
    leaked = "sk-ant-oat01-" + "7" * 40
    cli = tmp_path / "claude"
    cli.write_text("#!/bin/sh\n"
                   f"echo 'starting with {leaked}' >&2\n"
                   "echo 'You cannot use --strict-mcp-config when an enterprise MCP config is present' >&2\n"
                   "exit 1\n")
    cli.chmod(0o755)
    launcher = LocalLauncher(cli_path=str(cli), token=TOKEN)
    with pytest.raises(ClaudeTurnFailed) as failed:
        await run_turn(SPEC, launcher, new_graph(), "t-no-start", [HumanMessage("ECHO hi")])
    with pytest.raises(ClaudeTurnFailed) as checked:
        await token_works(launcher, "claude-haiku-5-5")
    for e in (failed.value, checked.value):
        assert e.kind == "startup_failed"
        assert str(e).startswith("Claude Code could not start: ")
        assert str(e).endswith("You cannot use --strict-mcp-config when an enterprise MCP config is present")
        assert leaked not in str(e)
    assert leaked not in caplog.text
    assert any(r.name == "ptc_agent.agent.claude_code.launcher" and "enterprise MCP config is present" in r.getMessage()
               for r in caplog.records)


async def test_a_call_that_raises_fails_the_turn_at_once_while_the_cli_waits_for_its_result():
    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        raise RuntimeError(f"broke on {command}")

    async with FakeMessagesAPI() as api:
        with pytest.raises(RuntimeError, match="broke on one"):
            await asyncio.wait_for(turn(api, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()),
                                        "t-raises", [HumanMessage("CALLS Bash:one Bash:two")]), 60)



def _hook(event: str, output: dict) -> dict:
    """Flag settings with one hook on every tool that answers ``output``."""
    answer = json.dumps({"hookSpecificOutput": {"hookEventName": event, **output}})
    return {"hooks": {event: [{"matcher": ".*", "hooks": [{"type": "command", "command": f"cat >/dev/null; echo {shlex.quote(answer)}"}]}]}}


async def test_a_call_runs_with_the_arguments_a_hook_rewrote_it_to():
    ran = []

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        ran.append(command)
        return f"ran: {command}"

    rewrite = _hook("PreToolUse", {"permissionDecision": "allow", "updatedInput": {"command": "echo rewritten"}})
    async with FakeMessagesAPI() as api:
        launcher = _Configured(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, settings=rewrite)
        values = await run_turn(SPEC, launcher, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()),
                                "t-rewritten", [HumanMessage("RUN_TOOL please")])
        sent = _tool_results_sent(api)
    call, result = values["messages"][1:3]
    assert call.tool_calls[0]["args"] == {"command": "echo spike-$((6*7))"}
    assert ran == ["echo rewritten"] and result.content == "ran: echo rewritten"
    assert sent[result.tool_call_id][0] == [{"type": "text", "text": "ran: echo rewritten"}]


async def test_a_result_a_hook_replaced_is_kept_as_claude_was_given_it_with_the_tools_artifact():
    @tool("Bash", response_format="content_and_artifact")
    async def bash(command: str) -> tuple[str, dict]:
        """Run a shell command."""
        return f"ran: {command}", {"exit_code": 0}

    replace = _hook("PostToolUse", {"updatedMCPToolOutput": "replaced by the hook"})
    async with FakeMessagesAPI() as api:
        launcher = _Configured(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, settings=replace)
        values = await run_turn(SPEC, launcher, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()),
                                "t-replaced", [HumanMessage("RUN_TOOL please")])
        sent = _tool_results_sent(api)
    result = values["messages"][2]
    assert sent[result.tool_call_id][0] == "replaced by the hook"
    assert (result.content, result.status, result.artifact) == ("replaced by the hook", "success", {"exit_code": 0})


async def test_an_image_a_hook_replaced_with_text_is_kept_as_the_text_claude_was_given():
    @tool("Bash")
    async def bash(command: str):
        """Run a shell command."""
        return [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": PNG}}]

    redact = _hook("PostToolUse", {"updatedMCPToolOutput": "redacted by the hook"})
    async with FakeMessagesAPI() as api:
        launcher = _Configured(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, settings=redact)
        values = await run_turn(SPEC, launcher, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()),
                                "t-redacted", [HumanMessage("RUN_TOOL please")])
    assert values["messages"][2].content == "redacted by the hook"


@pytest.mark.parametrize("returned", [
    "",
    [{"type": "text", "text": "a chart"}, {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": PNG}}],
    [{"type": "text", "text": "a pdf"}, {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": PDF}}],
], ids=["empty", "image", "document"])
async def test_a_result_the_cli_words_its_own_way_is_kept_as_the_tool_returned_it(returned):
    @tool("Bash")
    async def bash(command: str):
        """Run a shell command."""
        return returned

    async with FakeMessagesAPI() as api:
        values = await turn(api, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()), "t-own-words",
                            [HumanMessage("RUN_TOOL please")])
    assert values["messages"][2].content == returned


async def test_a_cancelled_turn_ends_its_running_calls_before_the_turn_closes():
    started, order = asyncio.Event(), []

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(1)
            order.append("call ended")
        return "never"

    graph = build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver())
    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        claude = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-cancelled")
        config = {"configurable": {"thread_id": "t-cancelled", TURN_KEY: claude}}
        running = asyncio.ensure_future(graph.ainvoke({"messages": [HumanMessage("RUN_TOOL please")]}, config))
        try:
            await asyncio.wait_for(started.wait(), 60)
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
            order.append("turn closing")
        finally:
            await claude.close()
    assert order == ["call ended", "turn closing"]



class _Inbox:
    """The thread's steers as the server's report inbox hands them to a run."""

    def __init__(self, *, endless: bool = False) -> None:
        self.waiting: list[SteeredReport] = []
        self.confirmed: list[str] = []
        self.sent = 0
        self.endless = endless

    def send(self, text: str) -> None:
        self.waiting.append(SteeredReport(job_id=f"job-{self.sent}", content=text, kind="steer", timestamp=0.0))
        self.sent += 1

    async def take(self, thread_id, run_id, *, kinds=ALL_KINDS):
        if self.endless:
            self.send(f"steer {self.sent}")
        taken, self.waiting = self.waiting, []
        return taken

    async def checker(self, thread_id, own_run_id=None, *, alert_senders=frozenset()):
        """``build_message_checker``: who sent the steers waiting."""

        async def senders():
            return ["user"] if self.waiting or self.endless else []

        return senders

    def render_group(self, kind, contents):
        raise AssertionError("steers only")

    async def confirm(self, run_id, job_ids):
        self.confirmed.extend(job_ids)


async def _steered_turn(api, inbox: _Inbox, tool_node: ToolNode, thread: str, messages: list, graph=None):
    graph = graph or build_claude_code_graph(tool_node, checkpointer=InMemorySaver())
    launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
    steering = SteeringMiddleware(report_inbox=inbox, kinds=("steer",))
    claude = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key=thread, steering=steering)
    config = {"configurable": {"thread_id": thread, TURN_KEY: claude}, "metadata": {"run_id": "run-1"}}
    events = []
    try:
        with mock.patch("ptc_agent.agent.claude_code.mid_turn.build_message_checker", inbox.checker):
            async for event in graph.astream({"messages": messages}, config, stream_mode="custom"):
                events.append(event)
    finally:
        await claude.close()
    return (await graph.aget_state({"configurable": {"thread_id": thread}})).values, events


STEERED = "The user sent a new message while you were working:\nUse the other file.\n\nAddress the message above as you continue this turn."


async def test_a_steer_sent_while_a_call_runs_reaches_claude_right_after_its_result_and_is_committed_with_it():
    inbox = _Inbox()

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        inbox.send("Use the other file.")
        return f"ran: {command}"

    async with FakeMessagesAPI() as api:
        values, events = await _steered_turn(api, inbox, ToolNode([bash]), "t-steer", [HumanMessage("RUN_TOOL please")])
        after = api.model_requests[-1]["messages"]
    _, call, result, steering, reply = values["messages"]
    assert isinstance(result, ToolMessage) and isinstance(reply, AIMessage)
    assert steering.content == STEERED
    assert steering.additional_kwargs["held_jobs"] == {"run_id": "run-1", "job_ids": ["job-0"]}
    assert [e["type"] for e in events] == ["steering_delivered"]
    assert inbox.confirmed == ["job-0"]
    told = [m["content"] if isinstance(m["content"], str) else m["content"][0]["text"] for m in after if m["role"] == "system"]
    assert any(t.startswith("The user sent a new message while you were working:\nUse the other file.\n") for t in told)
    assert not any("hook additional context" in t for t in told)
    assert len(api.model_requests) == 2


async def test_steers_waiting_as_a_query_ends_are_the_turns_next_query_up_to_its_limit():
    async with FakeMessagesAPI() as api:
        inbox = _Inbox()
        inbox.send("Use the other file.")
        values, events = await _steered_turn(api, inbox, ToolNode([]), "t-again", [HumanMessage("hello")])
        again = api.model_requests[-1]["messages"][-1]
    _, first, trigger, steering, reply = values["messages"]
    assert isinstance(first, AIMessage) and isinstance(reply, AIMessage)
    assert trigger.content == "User sent additional instructions." and trigger.additional_kwargs["lc_source"] == "orchestrator"
    assert steering.content == STEERED and inbox.confirmed == ["job-0"]
    assert [b["text"] for b in again["content"] if b["type"] == "text"][-2:] == [trigger.content, STEERED]
    assert [e["type"] for e in events] == ["steering_delivered"]

    async with FakeMessagesAPI() as api:
        await _steered_turn(api, _Inbox(endless=True), ToolNode([]), "t-endless", [HumanMessage("hello")])
        assert len(api.model_requests) == TURN_QUERIES


async def test_the_steers_a_turn_opened_with_are_shown_and_closed_once_claude_has_them():
    inbox = _Inbox()
    inbox.send("Use the other file.")
    ahead = steers_ahead_message(await inbox.take("t", "run-1"), "run-1")
    graph = build_claude_code_graph(ToolNode([]), checkpointer=InMemorySaver())
    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        claude = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-ahead",
                            steering=SteeringMiddleware(report_inbox=inbox, kinds=("steer",)))
        user = HumanMessage("hello", id="input-1")
        config = {"configurable": {"thread_id": "t-ahead", TURN_KEY: claude, "steer_input_message_id": "input-1"},
                  "metadata": {"run_id": "run-1"}}
        try:
            events = [e async for e in graph.astream({"messages": [ahead, user]}, config, stream_mode="custom")]
        finally:
            await claude.close()
    assert [e["type"] for e in events] == ["steering_delivered"]
    assert inbox.confirmed == ["job-0"]


async def test_a_call_a_stop_cut_off_is_answered_as_claude_was_told_when_the_thread_goes_on():
    started = asyncio.Event()

    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        started.set()
        await asyncio.sleep(60)
        return "never"

    graph = build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver())
    async with FakeMessagesAPI() as api:
        launcher = LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"})
        claude = ClaudeTurn(SPEC, launcher, identity="test-user", thread_key="t-cut")
        config = {"configurable": {"thread_id": "t-cut", TURN_KEY: claude}}
        running = asyncio.ensure_future(graph.ainvoke({"messages": [HumanMessage("RUN_TOOL please")]}, config))
        try:
            await asyncio.wait_for(started.wait(), 60)
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
        finally:
            await claude.close()
        values = await run_turn(SPEC, LocalLauncher(env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}), graph,
                                "t-cut", [HumanMessage("hello again")])
        sent = _tool_results_sent(api)
    first, call, cut, again, reply = values["messages"]
    assert isinstance(cut, ToolMessage) and cut.tool_call_id == call.tool_calls[0]["id"] and cut.status == "error"
    assert cut.content.startswith("[Tool call interrupted")
    assert sent[cut.tool_call_id] == (cut.content, True)
    assert again.content == "hello again" and reply.text == "ECHO hello again"


@pytest.mark.parametrize("pdf", [PDF, BIG_PDF], ids=["small", "past-the-default-line-limit"])
async def test_a_pdf_a_call_returns_reaches_claude_as_a_document_and_is_kept_as_returned(pdf):
    returned = [{"type": "text", "text": "a pdf"},
                {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": pdf}}]

    @tool("Bash")
    async def bash(command: str):
        """Run a shell command."""
        return returned

    async with FakeMessagesAPI() as api:
        values = await turn(api, build_claude_code_graph(ToolNode([bash]), checkpointer=InMemorySaver()), "t-pdf",
                            [HumanMessage("RUN_TOOL please")])
        sent = _tool_results_sent(api)
    result = values["messages"][2]
    assert result.content == returned
    assert sent[result.tool_call_id][0] == returned
