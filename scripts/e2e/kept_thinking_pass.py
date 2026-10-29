#!/usr/bin/env python3
"""Kept thinking pass: does the thinking a compaction keeps pass the API's check?

The doc's own recipe (compaction-thinking-blocks, "Check that the kept thinking
held"): compact the older turns while keeping one that holds a thinking block,
then send the next request with ``thinking.block_binding.prefix_mismatch_behavior``
set to ``"error"``. A 200 whose ``input_transformations`` is empty means every
kept block held; a 400 saying a block is bound to a different conversation
names the first one that did not.

Four phases, all live against the Anthropic API:

  sdk    The recipe as the doc writes it, with the SDK alone. Proves the
         account and model run the check and that a well-formed keep-tail
         compaction passes it.
  stack  The same check through the agent's own request path: a real
         ``create_agent`` graph with the compaction middleware, the reasoning
         compatibility and resilience middleware, on the project's client for
         the model, every request sent with ``"error"``. Turns run until the
         middleware has sent a compaction request and two requests after it.
         Each request's body is recorded on the way out and checked with the
         unit harness's prefix rules too, so a failure names the part that
         changed.
  deploy A deploy between turns, through the tool surface and the system
         pin: turn 1 on render A, then a build with another static prompt and
         a tool whose schema gained a required argument. The thread must keep
         sending A's system and tools byte for byte, give the model the new
         schema in a message (and the model call the tool with it), and the
         next turn start, over the threshold, must summarize every turn on A
         and send the call after it on B.
  rollout A thread built without the pin and the tool surface (as before
         they shipped), then continued with them: on the same render nothing
         may be dropped; on a changed one the earlier thinking is lost, and
         the API drops it again on every request that still sends it until a
         compaction summarizes it. The count of distinct blocks is printed and
         does not fail the pass.

``--behavior`` picks what a failing block does. ``error`` (the recipe) refuses
the request; ``drop_block`` (what the manifest sends) drops it and lists it;
``unset`` sends the beta without ``block_binding``, which on an account the API
does not enforce by default lists each failing block as
``thinking_mismatch_allowed`` and changes nothing the model receives, the
doc's way to watch for edits without opting in.

Exit status 0 only when no request was refused and every answer listed no
dropped or flagged block.

The stack phase binds a Read tool, as the main agent does, so a request
setting that only a bound tool brings on goes out on the compaction request
too. Each answer's cache reads and writes are printed beside its list.

Live runs use Sonnet 5.5 and Haiku 5.5; Opus only where the behavior under
test differs on it. The deploy and rollout phases open with a reasoning turn at
the highest effort and fail as "not armed" when it still produces no thinking,
since a skipped precondition must never read as a pass.

Debugging the deploy phase: KTP_ASK_WATCH=1 adds a turn asking which watch
codes the model can see, KTP_DUMP=<path> writes every payload and HTTP body to
that file, KTP_TRACE=1 prints the compaction trigger's reading per request, and
KTP_NOTES=1 gives its compaction a notes folder holding one note, so each
summary sends the notes pointer after the turns it keeps.

Usage:
  ANTHROPIC_API_KEY=... uv run python scripts/e2e/kept_thinking_pass.py \
      [--model claude-haiku-5-5] [--phase sdk,stack,deploy,rollout] [--behavior error] \
      [--max-turns 14] [--threshold 6000]

The key is read from the environment and never printed. ``ANTHROPIC_BASE_URL``
and ``ANTHROPIC_AUTH_TOKEN`` are ignored, so the run always reaches the API itself.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

os.environ["LANGSMITH_TRACING"] = "false"
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ.setdefault("HOST_MODE", "oss")
for _name in ("ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN"):
    os.environ.pop(_name, None)

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(REPO_ROOT), str(REPO_ROOT / "src")]

BETAS = ["compact-2026-09-04", "thinking-binding-controls-2026-08-01"]


def _thinking(base: dict, behavior: str) -> dict:
    thinking = {k: v for k, v in base.items() if k != "block_binding"}
    if behavior != "unset":
        thinking["block_binding"] = {"prefix_mismatch_behavior": behavior}
    return thinking


def _key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise SystemExit("ANTHROPIC_API_KEY is not set")
    return key


def _model_id(name: str) -> str:
    from src.llms.llm import LLM

    entry = LLM.get_model_config().llm_config.get(name) or {}
    return entry.get("model_id") or name


def sdk_phase(model: str, behavior: str) -> bool:
    """The doc's recipe, step for step."""
    import anthropic

    client = anthropic.Anthropic(api_key=_key())
    system = "You help plan a recipe app's release. Keep answers short."
    thinking = _thinking({"type": "adaptive"}, behavior)
    history: list[dict] = [{"role": "user", "content": "What are the main entities in the app's data model?"}]

    def send(**extra):
        return client.beta.messages.create(
            model=model, max_tokens=8192, system=system, betas=BETAS, thinking=thinking, messages=history, **extra
        )

    first = send()
    history += [
        {"role": "assistant", "content": first.content},
        {
            "role": "user",
            "content": "Testing starts on Tuesday, March 3, 2026, takes 10 weekdays, and pauses on "
            "March 9 and March 16. On which date does it end?",
        },
    ]
    second = send()
    history.append({"role": "assistant", "content": second.content})
    kept = sum(block.type == "thinking" for block in second.content)
    print(f"[sdk] thinking blocks in the kept turn: {kept}")
    if not kept:
        print("[sdk] the kept turn holds no thinking, so nothing was checked")
        return False

    summarized, kept_turns = history[:2], history[2:]
    history = summarized
    # anthropic 0.120 has no ``compaction`` parameter, so it rides the body.
    summary = send(extra_body={"compaction": {"type": "summarize"}})
    if summary.stop_reason != "compaction":
        print(f"[sdk] no summary: stop_reason={summary.stop_reason}")
        return False
    history = [
        {"role": "assistant", "content": summary.content},
        *kept_turns,
        {"role": "user", "content": "Which day should the release go out?"},
    ]
    try:
        third = send()
    except anthropic.BadRequestError as e:
        print(f"[sdk] refused: {e.message}")
        return False
    listed = getattr(third, "input_transformations", None) or []
    print(f"[sdk] input_transformations after the swap: {json.dumps(listed)}")
    for name, answer in (("first", first), ("second", second), ("summary", summary), ("third", third)):
        print(f"[sdk] usage {name}: {answer.usage.model_dump_json(exclude_none=True)}")
    return not listed


async def stack_phase(model_name: str, behavior: str, max_turns: int, threshold: int) -> bool:
    """The recipe through the agent's own request path."""
    import anthropic
    from langchain.agents import create_agent
    from langchain_anthropic import ChatAnthropic
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
    from langchain_core.messages import AIMessage, HumanMessage
    from langchain_core.tools import tool
    from langgraph.checkpoint.memory import InMemorySaver

    from ptc_agent.agent.middleware.compaction.compact import Summarizer
    from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
    from ptc_agent.agent.middleware.model_resilience import ModelResilienceMiddleware
    from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
    from src.llms.llm import create_llm
    from tests.unit.ptc_agent.agent.prefix_stability import (
        Wire,
        is_compaction_request,
        kept_breaks,
        thinking_prefixes,
    )

    sent = Wire()
    acreate = ChatAnthropic._acreate

    async def recording(model, payload):
        sent.payloads.append(json.loads(json.dumps(payload, default=str)))
        return await acreate(model, payload)

    ChatAnthropic._acreate = recording

    # A refusal the resilience middleware answers by stripping the reasoning
    # never reaches the caller, so the strip itself is what fails the pass.
    strips: list[int] = []
    escalate = ModelResilienceMiddleware._escalate_reasoning

    def recording_strip(request):
        strips.append(len(sent.payloads))
        return escalate(request)

    ModelResilienceMiddleware._escalate_reasoning = staticmethod(recording_strip)

    @tool("Read")
    def read(file_path: str) -> str:
        """Read a file from the workspace."""
        return f"{file_path} is empty."

    system = " ".join("You help plan a recipe app's release. Keep each answer to about a hundred words." for _ in range(60))
    saver = InMemorySaver()
    cfg = {"configurable": {"thread_id": "kept-thinking-pass", "user_id": "kept-thinking-pass"}}
    questions = [
        "What are the main entities in the app's data model?",
        "Suggest field names for Recipe.",
        "And for Ingredient?",
        "How should units be stored?",
        "Which indexes does the recipe search need?",
        "What should the first release leave out?",
        "Testing starts on Tuesday, March 3, 2026, takes 10 weekdays, and pauses on March 9 and March 16. "
        "On which date does it end?",
        "Which day should the release go out?",
        "Write the release note's first sentence.",
        "What is the riskiest part of the plan?",
        "How would you test the unit conversion?",
        "Summarize the plan in three bullets.",
        "What would you cut if the date slipped a week?",
        "Anything else before the release?",
    ]
    refused = None
    for turn, question in enumerate(questions[:max_turns], 1):
        model = create_llm(model_name, api_key=_key())
        model.thinking = _thinking(model.thinking or {}, behavior)
        agent = create_agent(
            model=model,
            tools=[read],
            system_prompt=system,
            middleware=[
                CompactionMiddleware(
                    Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
                    token_threshold=threshold,
                    keep_tokens=threshold // 3,
                    claude_keep_tokens=threshold // 3,
                ),
                ModelResilienceMiddleware(primary_name=model_name, primary_client=model, max_retries=1),
                ReasoningCompatibilityMiddleware(),
            ],
            checkpointer=saver,
        )
        try:
            state = await agent.ainvoke({"messages": [HumanMessage(question)]}, cfg)
        except anthropic.BadRequestError as e:
            refused = e.message
            print(f"[stack] turn {turn} refused: {refused}")
            break
        answers = [m for m in state["messages"] if isinstance(m, AIMessage)]
        last = answers[-1]
        listed = (last.response_metadata or {}).get("input_transformations")
        kinds = [is_compaction_request(p) for p in sent.payloads]
        print(
            f"[stack] turn {turn}: {len(sent.payloads)} requests, "
            f"{sum(kinds)} compaction, input_tokens={(last.usage_metadata or {}).get('input_tokens')}, "
            f"input_transformations={json.dumps(listed)} "
            f"cache={json.dumps((last.usage_metadata or {}).get('input_token_details'))}"
        )
        if True in kinds and len(kinds) - kinds.index(True) > 2:
            break

    if not any(is_compaction_request(p) for p in sent.payloads):
        print("[stack] no compaction request was sent; raise --max-turns or lower the threshold")
        return False
    listed = [
        (m.response_metadata or {}).get("input_transformations")
        for m in state["messages"]
        if isinstance(m, AIMessage)
    ]
    flagged = [entries for entries in listed if entries]
    print(f"[stack] answers listing a dropped or flagged block: {len(flagged)}")
    # A block answered the last request before the one that first sent it
    # back that asked for an answer: a compaction request in between answers
    # with its block alone, and leaves out the turns it keeps.
    first_sent: dict[str, int] = {}
    for n, payload in enumerate(sent.payloads):
        for signature in thinking_prefixes(payload):
            first_sent.setdefault(signature, n)
    answered = [n for n, payload in enumerate(sent.payloads) if not is_compaction_request(payload)]
    sent.produced = [
        (max(a for a in answered if a < n), signature)
        for signature, n in first_sent.items()
        if any(a < n for a in answered)
    ]
    print(f"[stack] harness prefix check: {kept_breaks(sent) or 'no break'}")
    if strips:
        print(f"[stack] reasoning stripped after a 400 on request(s) {strips}; the log line above has the text")
    return refused is None and not flagged and not strips


def _recorder():
    """A Wire that records every request the project's clients send, and the
    HTTP body each went out with (what the SDK serialized)."""
    import httpx
    from langchain_anthropic import ChatAnthropic

    from tests.unit.ptc_agent.agent.prefix_stability import Wire

    sent = Wire()
    sent.answers = []
    acreate = ChatAnthropic._acreate
    send = httpx.AsyncClient.send

    async def recording(model, payload):
        sent.payloads.append(json.loads(json.dumps(payload, default=str)))
        return await acreate(model, payload)

    async def sending(client, request, **kwargs):
        if request.url.path.endswith("/v1/messages"):
            sent.bodies.append(json.loads(request.content))
        response = await send(client, request, **kwargs)
        if request.url.path.endswith("/v1/messages") and "json" in response.headers.get("content-type", ""):
            await response.aread()
            sent.answers.append((len(sent.bodies) - 1, response.status_code, response.json()))
        return response

    ChatAnthropic._acreate = recording
    httpx.AsyncClient.send = sending
    return sent


_STAMPS = iter(range(1, 1_000))


def _stamp_middleware():
    """A market-watch stamp on every call, as the watch writes one while a
    venue is open, so the deploy phase sends turn-scoped system messages."""
    from langchain.agents.middleware import AgentMiddleware

    from ptc_agent.agent.middleware.runtime_context.durable import DurableUpdate, build_update_message

    class Stamp(AgentMiddleware):
        async def abefore_model(self, state, runtime):
            n = next(_STAMPS)
            text = f"<market-watch>Watch code W{n}.</market-watch>"
            update = DurableUpdate(kind="market_watch", schema_version=2, text=text)
            return {"messages": [build_update_message(update)]}

    return Stamp()


def _attribute(sent) -> None:
    """Each block to the last request before the one that first sent it back
    that asked for an answer (see stack_phase)."""
    from tests.unit.ptc_agent.agent.prefix_stability import is_compaction_request, thinking_prefixes

    first_sent: dict[str, int] = {}
    for n, payload in enumerate(sent.payloads):
        for signature in thinking_prefixes(payload):
            first_sent.setdefault(signature, n)
    answered = [n for n, payload in enumerate(sent.payloads) if not is_compaction_request(payload)]
    sent.produced = [
        (max(a for a in answered if a < n), signature)
        for signature, n in first_sent.items()
        if any(a < n for a in answered)
    ]


def _lookups():
    from langchain_core.tools import tool

    @tool("lookup")
    def lookup_a(symbol: str) -> str:
        """Look up a stock's last price."""
        return f"{symbol}: 100.00"

    @tool("lookup")
    def lookup_b(symbol: str, currency: str) -> str:
        """Look up a stock's last price in the currency given, an ISO code such as USD."""
        return f"{symbol}: 100.00 {currency}"

    return lookup_a, lookup_b


_RENDER_A = " ".join("You help a user track stocks. Keep each answer to two sentences." for _ in range(60))
_RENDER_B = _RENDER_A + " Always name the currency of a price."


#: Turn 1 of the deploy and rollout phases: a question the model has to reason
#: about, asked at the highest effort, so the thread holds thinking before the
#: build changes. Adaptive thinking skips a short answer otherwise (Sonnet 5.5
#: did on every try), and with nothing bound there is nothing to check.
_REASONING_TURN = (
    "What is NVDA's last price? Use the lookup tool. Then work out privately which is larger, "
    "1937*23 or 211*211, and add one line naming it."
)


def _stamps(body):
    """(index, message) of each message a market-watch stamp rides in: a text
    block that opens with the tag, bare or inside a reminder. An answer or a
    summary that quotes the tag is not a stamp."""
    for i, m in enumerate(body["messages"]):
        content = m.get("content")
        blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content
        if m.get("role") != "assistant" and any(
            b.get("type") == "text" and b.get("text", "").removeprefix("<system-reminder>\n").startswith("<market-watch>")
            for b in blocks
        ):
            yield i, m


def _shape(body) -> list[str]:
    shape = []
    for m in body["messages"]:
        content = m.get("content")
        kinds = "str" if isinstance(content, str) else ",".join(b.get("type", "?") for b in content)
        shape.append(f"{m['role']}{'(clear_at)' if m.get('clear_at') else ''}:{kinds}")
    return shape


_NOTES_DIR = "/home/workspace/kept/.agents/scratchpad/kept0001/note"


class _NotesBackend:
    """A sandbox whose notes folder holds one note, so each summary carries
    the notes pointer (KTP_NOTES=1)."""

    livefs = None
    transcripts = None

    def normalize_path(self, path):
        return path

    async def aglob(self, pattern, path="/"):
        from types import SimpleNamespace

        return SimpleNamespace(matches=[{"path": f"{path}/nvda.md"}], error=None)


def _agent(model_name, behavior, saver, system, tools, *, pinned=True, stamped=False, threshold=1_000_000, effort=None):
    from langchain.agents import create_agent
    from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
    from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

    from ptc_agent.agent.main_state import MainAgentState
    from ptc_agent.agent.middleware.compaction.compact import Summarizer
    from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
    from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
    from ptc_agent.agent.middleware.runtime_context.frozen_prompt import SystemPinMiddleware
    from ptc_agent.agent.middleware.runtime_context.tail import RowCarrierMiddleware
    from ptc_agent.agent.middleware.tool_surface import ToolSurfaceMiddleware
    from src.llms.llm import create_llm

    model = create_llm(model_name, api_key=_key(), reasoning_effort=effort)
    model.thinking = _thinking(model.thinking or {}, behavior)
    compaction = CompactionMiddleware(
        Summarizer(GenericFakeChatModel(messages=iter([])), limit=100_000),
        token_threshold=threshold,
        keep_tokens=max(threshold // 3, 1),
        claude_keep_tokens=max(threshold // 3, 1),
        backend=_NotesBackend() if os.environ.get("KTP_NOTES") else None,
    )
    if os.environ.get("KTP_NOTES"):
        compaction = compaction.with_scratchpad_notes(_NOTES_DIR)
    middleware = [
        compaction,
        AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
        *([ToolSurfaceMiddleware()] if pinned else []),
        *([_stamp_middleware(), RowCarrierMiddleware()] if stamped else []),
        *([SystemPinMiddleware()] if pinned else []),
        ReasoningCompatibilityMiddleware(),
    ]
    return create_agent(
        model=model,
        tools=tools,
        system_prompt=system,
        middleware=middleware,
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def _unmarked(value):
    if isinstance(value, dict):
        return {k: _unmarked(v) for k, v in value.items() if k != "cache_control"}
    if isinstance(value, list):
        return [_unmarked(v) for v in value]
    return value


async def deploy_phase(model_name: str, behavior: str) -> bool:
    """A deploy between turns, then the summary at a turn start that brings it in."""
    import anthropic
    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver

    from ptc_agent.agent.middleware.thinking_binding import holds_bound_thinking
    from tests.unit.ptc_agent.agent.prefix_stability import is_compaction_request, kept_breaks, thinking_count

    sent = _recorder()
    if os.environ.get("KTP_TRACE"):
        from ptc_agent.agent.middleware.compaction import middleware as _mw

        _ct = _mw.context_tokens

        async def _traced(state, view, estimate, system=()):
            reading = await _ct(state, view, estimate, system)
            print(f"[deploy/trace] request {len(sent.payloads) + 1}: reading {reading.tokens}, cached_input {state.get('_cached_input_tokens')}, cached_output {state.get('_cached_output_tokens')}, added {reading.added}, view {len(view)} messages")
            return reading

        _mw.context_tokens = _traced
    lookup_a, lookup_b = _lookups()
    saver = InMemorySaver()
    cfg = {"configurable": {"thread_id": "kept-thinking-deploy", "user_id": "kept-thinking-pass"}}
    state = None
    try:
        state = await _agent(model_name, behavior, saver, _RENDER_A, [lookup_a], stamped=True, effort="max").ainvoke(
            {"messages": [HumanMessage(_REASONING_TURN)]}, cfg
        )
        if not holds_bound_thinking(state["messages"]):
            print("[deploy] not armed: turn 1 produced no thinking, so nothing binds render A and the deploy checks nothing")
            return False
        deployed_at = len(sent.payloads)
        state = await _agent(model_name, behavior, saver, _RENDER_B, [lookup_b], stamped=True).ainvoke(
            {"messages": [HumanMessage("And AMD's? Use the lookup tool.")]}, cfg
        )
        last = [m for m in state["messages"] if isinstance(m, AIMessage)][-1]
        threshold = (last.usage_metadata or {}).get("input_tokens", 0) + 1
        print(f"[deploy/debug] threshold set to {threshold} from the last answer's usage {json.dumps(last.usage_metadata)}")
        for question in ("Which of the two is higher?", "Say it again in one sentence."):
            state = await _agent(model_name, behavior, saver, _RENDER_B, [lookup_b], stamped=True, threshold=threshold).ainvoke(
                {"messages": [HumanMessage(question)]}, cfg
            )
        if os.environ.get("KTP_ASK_WATCH"):
            state = await _agent(model_name, behavior, saver, _RENDER_B, [lookup_b], stamped=True, threshold=threshold).ainvoke(
                {"messages": [HumanMessage("Which watch codes (W followed by a number) can you see anywhere in this conversation? List every one, exactly as written, and nothing else.")]}, cfg
            )
            seen = [m for m in state["messages"] if isinstance(m, AIMessage)][-1]
            newest = [m for body in sent.bodies[-1:] for _, m in _stamps(body)]
            print(f"[deploy/5.1] watch codes the model names: {seen.text!r}; stamps on the last body: {len(newest)}, newest: {json.dumps(newest[-1]['content'])[:120] if newest else None}")
    except anthropic.BadRequestError as e:
        print(f"[deploy] refused: {e.message}")
        for n, status, body in getattr(sent, "answers", [])[-2:]:
            print(f"[deploy] answer to body {n + 1}: HTTP {status} {json.dumps(body)[:600]}")
        return False

    for n, status, body in sent.answers:
        for block in body.get("content") or []:
            if block.get("type") == "compaction":
                print(f"[deploy/5.2] compaction answer to body {n + 1}: keys={sorted(block)} tool_changes={json.dumps(block.get('tool_changes'))[:900]}")
    for n, body in enumerate(sent.bodies):
        if n and "compaction" in sent.bodies[n - 1]:
            print(f"[deploy/5.3] body {n + 1}, first after the compaction request: {_shape(body)}")
            print(f"[deploy/5.3] tools on body {n + 1}: {[t.get('name') + ('(deferred)' if t.get('defer_loading') else '') for t in body.get('tools') or []]}")
            for m in body["messages"]:
                for block in m.get("content") if isinstance(m.get("content"), list) else []:
                    if block.get("type") == "compaction":
                        print(f"[deploy/5.2] compaction block sent on body {n + 1}: keys={sorted(block)} tool_changes={json.dumps(block.get('tool_changes'))[:900]}")
    stamps, unscoped, quoted = 0, [], set()
    for n, body in enumerate(sent.bodies):
        found = dict(_stamps(body))
        stamps += len(found)
        unscoped += [(n, i, m) for i, m in found.items() if not (m.get("role") == "system" and m.get("clear_at") == "next_user_message")]
        for i, m in enumerate(body["messages"]):
            if i not in found and "<market-watch>" in json.dumps(m) and m.get("role") not in quoted:
                quoted.add(m.get("role"))
                kinds = [b.get("type") for b in m["content"]] if isinstance(m.get("content"), list) else "str"
                print(f"[deploy/debug] body {n + 1} messages[{i}] ({m.get('role')}: {kinds}) quotes the stamp tag; not a stamp")
    for n, i, m in unscoped:
        print(f"[deploy/debug] unscoped stamp on body {n + 1} messages[{i}]: role={m.get('role')} keys={sorted(m)} {json.dumps(m)[:300]}")
        print(f"[deploy/debug] body {n + 1} shape: {_shape(sent.bodies[n])}")
    if os.environ.get("KTP_DUMP"):
        with open(os.environ["KTP_DUMP"], "w") as f:
            json.dump({"payloads": sent.payloads, "bodies": sent.bodies}, f)

    answers = [m for m in state["messages"] if isinstance(m, AIMessage)]
    flagged = [m.response_metadata.get("input_transformations") for m in answers if m.response_metadata.get("input_transformations")]
    kinds = [is_compaction_request(p) for p in sent.payloads]
    if True not in kinds:
        for m in answers:
            print(f"[deploy/debug] answer usage {json.dumps(m.usage_metadata)}")
        print("[deploy] no compaction request was sent")
        return False
    c = kinds.index(True)
    first = sent.payloads[0]
    held = all(
        _unmarked(p["system"]) == _unmarked(first["system"]) and _unmarked(p["tools"]) == _unmarked(first["tools"])
        for p in sent.payloads[: c + 1]
    )
    swap = any("tool_definition" in json.dumps(p["messages"]) and "currency" in json.dumps(p["messages"]) for p in sent.payloads[:c])
    calls = [call["args"] for m in answers for call in m.tool_calls if call["name"] == "lookup"]
    after = sent.payloads[c + 1] if c + 1 < len(sent.payloads) else None
    on_b = after is not None and "Always name the currency" in json.dumps(after["system"]) and "currency" in json.dumps(after["tools"])
    stable = all(_unmarked(p["system"]) == _unmarked(after["system"]) for p in sent.payloads[c + 1 :]) if after else False
    scoped = stamps > 0 and not unscoped
    _attribute(sent)
    breaks = kept_breaks(sent)
    print(f"[deploy] market-watch stamps on the HTTP bodies: {stamps}, every one turn-scoped: {scoped}")
    print(f"[deploy] {len(sent.payloads)} requests, render B built from request {deployed_at + 1}, compaction on request {c + 1}")
    print(f"[deploy] A's system and tools held through the compaction request: {held}")
    print(f"[deploy] new schema given in a message: {swap}; lookup calls: {json.dumps(calls)}")
    print(f"[deploy] compaction request sends {thinking_count(sent.payloads[c])} thinking blocks; after it on B: {on_b}, stable: {stable}")
    print(f"[deploy] answers listing a dropped or flagged block: {len(flagged)}")
    print(f"[deploy] harness prefix check: {breaks or 'no break'}")
    used_b = any("currency" in args for args in calls[1:])
    return held and swap and used_b and on_b and stable and scoped and not flagged and not breaks


async def rollout_phase(model_name: str) -> bool:
    """A thread from before the pin shipped, continued with it."""
    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver

    from ptc_agent.agent.middleware.thinking_binding import holds_bound_thinking
    from tests.unit.ptc_agent.agent.prefix_stability import thinking_count

    lookup_a, lookup_b = _lookups()
    ok = True
    for variant, system, tool in (("same render", _RENDER_A, lookup_a), ("changed render", _RENDER_B, lookup_b)):
        sent = _recorder()
        saver = InMemorySaver()
        cfg = {"configurable": {"thread_id": f"kept-thinking-rollout-{variant}", "user_id": "kept-thinking-pass"}}
        state = await _agent(model_name, "drop_block", saver, _RENDER_A, [lookup_a], pinned=False, effort="max").ainvoke(
            {"messages": [HumanMessage(_REASONING_TURN)]}, cfg
        )
        if not holds_bound_thinking(state["messages"]):
            print(f"[rollout] {variant}: not armed: turn 1 produced no thinking, so the pin has nothing to keep")
            ok = False
            continue
        pinned_from = len(state["messages"])
        state = await _agent(model_name, "drop_block", saver, system, [tool]).ainvoke(
            {"messages": [HumanMessage("And AMD's? Use the lookup tool.")]}, cfg
        )
        answers = [m for m in state["messages"][pinned_from:] if isinstance(m, AIMessage)]
        listed = [
            [entry.get("path") for entry in (m.response_metadata.get("input_transformations") or []) if entry.get("type") == "thinking_dropped"]
            for m in answers
        ]
        blocks = {path for paths in listed for path in paths}
        print(f"[rollout/debug] {variant}: thinking blocks sent per request: {[thinking_count(p) for p in sent.payloads]}, transformations: {[m.response_metadata.get('input_transformations') for m in answers]}")
        print(f"[rollout] {variant}: input tokens per call with the pin: {[(m.usage_metadata or {}).get('input_tokens') for m in answers]}")
        print(
            f"[rollout] {variant}: earlier thinking blocks dropped: {len(blocks)}, listed on {sum(1 for paths in listed if paths)} of {len(answers)} calls"
            " (a block the stack still sends is dropped again on every call until a compaction summarizes it)"
        )
        if variant == "same render":
            ok = ok and not blocks
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--model", default="claude-haiku-5-5", help="manifest model name")
    parser.add_argument("--phase", default="sdk,stack")
    parser.add_argument("--behavior", choices=("error", "drop_block", "unset"), default="error")
    parser.add_argument("--max-turns", type=int, default=14)
    parser.add_argument("--threshold", type=int, default=6_000, help="the stack's summary threshold, in tokens")
    args = parser.parse_args()
    phases = {p.strip() for p in args.phase.split(",") if p.strip()}
    if not phases or phases - {"sdk", "stack", "deploy", "rollout"}:
        parser.error(f"--phase takes one or more of sdk, stack, deploy, rollout, not {args.phase!r}")
    _key()
    ok = True
    if "sdk" in phases:
        ok = sdk_phase(_model_id(args.model), args.behavior) and ok
    if "stack" in phases:
        ok = asyncio.run(stack_phase(args.model, args.behavior, args.max_turns, args.threshold)) and ok
    if "deploy" in phases:
        ok = asyncio.run(deploy_phase(args.model, args.behavior)) and ok
    if "rollout" in phases:
        ok = asyncio.run(rollout_phase(args.model)) and ok
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
