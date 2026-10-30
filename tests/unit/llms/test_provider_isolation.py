"""Claude-only request fields never reach any other model.

The preserved-thinking work changes how requests to Claude are built: a fixed
tools array with tool changes as system messages, turn-scoped system messages
(clear_at), a pinned system message, the binding and compaction betas,
thinking.block_binding, Claude's own compaction block. Every one is gated on
the official Anthropic host and a capability, and the Anthropic-compatible
hosts (DeepSeek, Moonshot, MiniMax, Z.AI and Doubao coding plans) share the
SDK, so a gate keyed on the SDK or a model name alone would leak.

Each test drives one manifest entry that is not a native Claude target (plus
compatible-host routes the manifest lists no model for) through a real agent
graph with the main agent's middleware in its order: market watch on, a skill
that unlocks a tool mid-turn, a deploy between turns that rewords the static
prompt and a tool, a compaction crossing its threshold, and an image in a user
message and a tool result. Every request is recorded at the httpx seam, where
each SDK (anthropic, openai, google-genai) sends it, and answered in its own
wire format; nothing leaves the machine.

The same thread is also held to a recorded render (``golden/``): the requests a
Claude and a non-Claude target send are compared with the ones recorded when
the file was written. Preserved thinking binds every earlier byte, so a
dependency upgrade that re-renders stored history (a middleware that reorders
earlier messages, a serializer that drops a key) fails here before it ships.
Set ``LANGALPHA_RECORD_RENDER=1`` to record anew after a deliberate change.
"""

from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from langchain.agents import create_agent
from langchain_anthropic import ChatAnthropic
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from market_protocol import MarketPhase
from PIL import Image

import src.llms.llm as llm_module
from src.llms.claude_code import ClaudeCodeTarget
from ptc_agent.agent.main_state import MainAgentState
from ptc_agent.agent.middleware.compaction import middleware as compaction_module
from ptc_agent.agent.middleware.compaction import utils as compaction_utils
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.notes import NotesOffMiddleware
from ptc_agent.agent.middleware.file_operations.multimodal_strip import MultimodalStripMiddleware
from ptc_agent.agent.middleware.market_watch import MarketWatchMiddleware
from ptc_agent.agent.middleware.model_resilience import ModelResilienceMiddleware
from ptc_agent.agent.middleware.system_prompt_caching import SystemPromptCacheMiddleware
from ptc_agent.agent.middleware.reasoning_compat import ReasoningCompatibilityMiddleware
from ptc_agent.agent.middleware.runtime_context.baseline import BaselineContextMiddleware
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import (
    FrozenPromptMiddleware,
    SystemPinMiddleware,
)
from ptc_agent.agent.middleware.runtime_context.tail import RowCarrierMiddleware
from ptc_agent.agent.middleware.runtime_context.turn import TurnContextMiddleware
from ptc_agent.agent.middleware.skills import SkillsMiddleware
from ptc_agent.agent.middleware.skills.registry import SkillDefinition
from ptc_agent.agent.middleware.tool.argument_parsing import ToolArgumentParsingMiddleware
from ptc_agent.agent.middleware.tool.empty_call_retry import EmptyToolCallRetryMiddleware
from ptc_agent.agent.middleware.tool_surface import ToolSurfaceMiddleware
from ptc_agent.agent.prompts import resolve_prompt_guidance
from src.llms.endpoints import is_official_anthropic_endpoint
from src.llms.input_transformations import INPUT_TRANSFORMATIONS
from src.llms.llm import create_llm, create_llm_from_custom
from src.llms.operator_channel import INLINE_TOOLS_BETA, TURN_SCOPED_BETA
from src.llms.provider_compaction import COMPACTION_BETA

# -- what only Claude on the official host may be sent -----------------------

CLAUDE_ONLY_BETAS = frozenset(
    {"thinking-binding-controls-2026-08-01", TURN_SCOPED_BETA, INLINE_TOOLS_BETA, COMPACTION_BETA}
)
CLAUDE_ONLY_KEYS = frozenset({"clear_at", "defer_loading", "block_binding", "context_management", "tool_changes"})
CLAUDE_ONLY_TYPES = frozenset({"tool_addition", "tool_removal", "tool_definition", "tool_reference"})
CLAUDE_ONLY_STATE = frozenset({"tool_surface", "_unsent_thinking_ids"})

#: Anthropic-compatible routes the manifest offers no model on, and model ids
#: that only a name test would mistake for Claude.
CUSTOM_ROUTES = {
    "z-ai-coding/glm-5.3": {"model_id": "glm-5.3", "provider": "z-ai-coding"},
    "z-ai-cn-coding/glm-5.3": {"model_id": "glm-5.3", "provider": "z-ai-cn-coding"},
    "minimax-coding/MiniMax-M3": {"model_id": "MiniMax-M3", "provider": "minimax-coding"},
    "deepseek/claude-opus-5-5": {"model_id": "claude-opus-5-5", "provider": "deepseek"},
    "openrouter/anthropic/claude-opus-5-5": {"model_id": "anthropic/claude-opus-5-5", "provider": "openrouter"},
}

_KEY = "sk-placeholder-not-a-key"


def _build(target: str):
    if target in CUSTOM_ROUTES:
        return create_llm_from_custom(CUSTOM_ROUTES[target], api_key=_KEY)
    return create_llm(target, api_key=_KEY)


def _native_claude(model: Any) -> bool:
    return isinstance(model, ChatAnthropic) and is_official_anthropic_endpoint(model.anthropic_api_url)


def _targets() -> list[str]:
    """Every catalog row langalpha sends requests for, other than native
    Claude. A Claude Code row sends none: Claude Code builds its own."""
    manifest = json.loads((Path(llm_module.__file__).parent / "manifest" / "models.json").read_text())
    entries = [
        name for name in manifest
        if not _native_claude(model := _build(name)) and not isinstance(model, ClaudeCodeTarget)
    ]
    return [*entries, *CUSTOM_ROUTES]


# -- the network: every SDK's request recorded and answered in its format ----


@dataclass
class _Reply:
    n: int
    tool: tuple[str, dict] | None = None
    text: str = ""
    usage: int = 1000


@dataclass
class _Call:
    url: str
    headers: dict[str, str]
    body: Any
    kind: str
    summary: bool = False


_SUMMARY_MARK = "You're nearing your context limit."


class _Network:
    def __init__(self, script: list[_Reply]):
        self.script = list(script)
        self.calls: list[_Call] = []
        self.unexpected: list[str] = []

    def handle(self, request, mod):
        path = request.url.path
        body = json.loads(request.content) if request.content else None
        if path.endswith("/messages/count_tokens"):
            return mod.Response(200, json={"input_tokens": 100}, request=request)
        kind = (
            "anthropic" if path.endswith("/messages")
            else "responses" if path.endswith("/responses")
            else "chat" if path.endswith("/chat/completions")
            else "gemini" if ":generateContent" in path or ":streamGenerateContent" in path
            else None
        )
        if kind is None:
            self.unexpected.append(str(request.url))
            return mod.Response(404, json={}, request=request)
        call = _Call(str(request.url), {k.lower(): v for k, v in request.headers.items()}, body, kind)
        self.calls.append(call)
        text = json.dumps(body)
        if kind == "responses" and (body.get("input") or [{}])[-1].get("type") == "compaction_trigger":
            call.summary = True
            return _respond(kind, request, body, None, mod)
        if kind == "anthropic" and "compaction" in body:
            call.summary = True
            return _respond(kind, request, body, None, mod)
        if _SUMMARY_MARK in text:
            call.summary = True
            return _respond(kind, request, body, _Reply(0, text="<summary>\nThe user asked for data.\n</summary>"), mod)
        if not self.script:
            self.unexpected.append(f"unscripted call to {request.url}")
            return _respond(kind, request, body, _Reply(99, text="(end)"), mod)
        return _respond(kind, request, body, self.script.pop(0), mod)

    def install(self, monkeypatch) -> _Network:
        import socket

        modules = [httpx]
        with contextlib.suppress(ImportError):
            import httpx2  # the openai SDK's transport

            modules.append(httpx2)
        for mod in modules:

            async def asend(client, request, _mod=mod, **kwargs):
                return self.handle(request, _mod)

            def send(client, request, _mod=mod, **kwargs):
                return self.handle(request, _mod)

            monkeypatch.setattr(mod.AsyncClient, "send", asend)
            monkeypatch.setattr(mod.Client, "send", send)

        def refuse(*args, **kwargs):
            raise OSError("network disabled in this test")

        monkeypatch.setattr(socket.socket, "connect", refuse)
        monkeypatch.setattr(socket.socket, "connect_ex", refuse)
        with contextlib.suppress(ImportError):
            import google.genai._api_client as genai_client

            monkeypatch.setattr(genai_client, "has_aiohttp", False)
        return self


def _sse(events: list[dict], named: bool = True) -> bytes:
    return "".join((f"event: {e['type']}\n" if named else "") + f"data: {json.dumps(e)}\n\n" for e in events).encode()


def _respond(kind: str, request, body: dict, reply: _Reply | None, mod):
    stream = bool(body.get("stream"))
    model = body.get("model", "m")
    if kind == "anthropic":
        message = _anthropic(model, reply)
        if stream:
            return mod.Response(200, headers={"content-type": "text/event-stream"}, content=_anthropic_sse(message), request=request)
        return mod.Response(200, json=message, request=request)
    if kind == "responses":
        response = _responses(model, reply)
        if stream:
            return mod.Response(200, headers={"content-type": "text/event-stream"}, content=_responses_sse(response), request=request)
        return mod.Response(200, json=response, request=request)
    if kind == "chat":
        completion = _chat(model, reply)
        if stream:
            return mod.Response(200, headers={"content-type": "text/event-stream"}, content=_chat_sse(completion), request=request)
        return mod.Response(200, json=completion, request=request)
    answer = _gemini(model, reply)
    if ":streamGenerateContent" in request.url.path:
        return mod.Response(200, headers={"content-type": "text/event-stream"}, content=_sse([answer], named=False), request=request)
    return mod.Response(200, json=answer, request=request)


def _chat_sse(body: dict) -> bytes:
    choice = body["choices"][0]
    message = choice["message"]
    delta = {"role": "assistant", "content": message["content"], "reasoning_content": message["reasoning_content"]}
    if "tool_calls" in message:
        delta["tool_calls"] = [{**call, "index": i} for i, call in enumerate(message["tool_calls"])]
    head = {k: body[k] for k in ("id", "created", "model", "system_fingerprint")}
    chunks = [{**head, "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
              {**head, "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {}, "finish_reason": choice["finish_reason"]}],
               "usage": body["usage"]}]
    return (_sse(chunks, named=False).decode() + "data: [DONE]\n\n").encode()

def _anthropic(model: str, r: _Reply | None) -> dict:
    if r is None:
        return {"id": "msg_cmp", "type": "message", "role": "assistant", "model": model,
                "content": [{"type": "compaction", "content": "The user asked for data.", "signature": "cmp-1"}],
                "stop_reason": "compaction", "stop_sequence": None, "usage": {"input_tokens": 0, "output_tokens": 0}}
    content: list[dict] = [{"type": "thinking", "thinking": f"thought {r.n}", "signature": f"sig-{r.n}"}]
    if r.text:
        content.append({"type": "text", "text": r.text})
    if r.tool:
        content.append({"type": "tool_use", "id": f"toolu_{r.n:02d}", "name": r.tool[0], "input": r.tool[1]})
    return {"id": f"msg_{r.n:02d}", "type": "message", "role": "assistant", "model": model, "content": content,
            "stop_reason": "tool_use" if r.tool else "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": r.usage, "output_tokens": 50}}


def _anthropic_sse(body: dict) -> bytes:
    events: list[dict] = [{"type": "message_start", "message": {**body, "content": [], "stop_reason": None,
                           "usage": {"input_tokens": body["usage"]["input_tokens"], "output_tokens": 1}}}]
    for i, block in enumerate(body["content"]):
        if block["type"] == "thinking":
            events += [{"type": "content_block_start", "index": i, "content_block": {"type": "thinking", "thinking": "", "signature": ""}},
                       {"type": "content_block_delta", "index": i, "delta": {"type": "thinking_delta", "thinking": block["thinking"]}},
                       {"type": "content_block_delta", "index": i, "delta": {"type": "signature_delta", "signature": block["signature"]}}]
        elif block["type"] == "text":
            events += [{"type": "content_block_start", "index": i, "content_block": {"type": "text", "text": ""}},
                       {"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": block["text"]}}]
        elif block["type"] == "tool_use":
            events += [{"type": "content_block_start", "index": i, "content_block": {**block, "input": {}}},
                       {"type": "content_block_delta", "index": i, "delta": {"type": "input_json_delta", "partial_json": json.dumps(block["input"])}}]
        else:
            events.append({"type": "content_block_start", "index": i, "content_block": block})
        events.append({"type": "content_block_stop", "index": i})
    events += [{"type": "message_delta", "delta": {"stop_reason": body["stop_reason"], "stop_sequence": None}, "usage": body["usage"]},
               {"type": "message_stop"}]
    return _sse(events)


def _responses(model: str, r: _Reply | None) -> dict:
    if r is None:
        output, n, usage = [{"type": "compaction", "id": "cmp_1", "encrypted_content": "ENC"}], 0, 0
    else:
        n, usage = r.n, r.usage
        output = [{"type": "reasoning", "id": f"rs_{n:02d}", "summary": [], "encrypted_content": f"enc-{n}"}]
        if r.text:
            output.append({"type": "message", "id": f"msg_{n:02d}", "role": "assistant", "status": "completed",
                           "content": [{"type": "output_text", "text": r.text, "annotations": []}]})
        if r.tool:
            output.append({"type": "function_call", "id": f"fc_{n:02d}", "call_id": f"call_{n:02d}", "name": r.tool[0],
                           "arguments": json.dumps(r.tool[1]), "status": "completed"})
    return {"id": f"resp_{n:02d}", "object": "response", "created_at": 1700000000, "status": "completed", "model": model,
            "output": output, "parallel_tool_calls": True, "tool_choice": "auto", "tools": [], "error": None,
            "incomplete_details": None, "instructions": None, "metadata": {},
            "usage": {"input_tokens": usage, "output_tokens": 50, "total_tokens": usage + 50,
                      "input_tokens_details": {"cached_tokens": 0}, "output_tokens_details": {"reasoning_tokens": 0}}}


def _responses_sse(response: dict) -> bytes:
    events: list[dict] = [{"type": "response.created",
                           "response": {**response, "status": "in_progress", "output": [], "usage": None}}]
    for i, item in enumerate(response["output"]):
        base = {"output_index": i}
        if item["type"] == "message":
            text = item["content"][0]["text"]
            events += [{**base, "type": "response.output_item.added", "item": {**item, "status": "in_progress", "content": []}},
                       {**base, "type": "response.content_part.added", "item_id": item["id"], "content_index": 0,
                        "part": {"type": "output_text", "text": "", "annotations": []}},
                       {**base, "type": "response.output_text.delta", "item_id": item["id"], "content_index": 0, "delta": text, "logprobs": []},
                       {**base, "type": "response.output_text.done", "item_id": item["id"], "content_index": 0, "text": text, "logprobs": []},
                       {**base, "type": "response.content_part.done", "item_id": item["id"], "content_index": 0, "part": item["content"][0]}]
        elif item["type"] == "function_call":
            events += [{**base, "type": "response.output_item.added", "item": {**item, "arguments": "", "status": "in_progress"}},
                       {**base, "type": "response.function_call_arguments.delta", "item_id": item["id"], "delta": item["arguments"]},
                       {**base, "type": "response.function_call_arguments.done", "item_id": item["id"], "arguments": item["arguments"]}]
        else:
            events.append({**base, "type": "response.output_item.added", "item": item})
        events.append({**base, "type": "response.output_item.done", "item": item})
    events.append({"type": "response.completed", "response": response})
    for number, event in enumerate(events):
        event["sequence_number"] = number
    return _sse(events)


def _chat(model: str, r: _Reply) -> dict:
    message: dict[str, Any] = {"role": "assistant", "content": r.text or None, "reasoning_content": f"thought {r.n}"}
    if r.tool:
        message["tool_calls"] = [{"id": f"call_{r.n:02d}", "type": "function",
                                  "function": {"name": r.tool[0], "arguments": json.dumps(r.tool[1])}}]
    return {"id": f"chatcmpl-{r.n:02d}", "object": "chat.completion", "created": 1700000000, "model": model, "system_fingerprint": "fp_test",
            "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if r.tool else "stop"}],
            "usage": {"prompt_tokens": r.usage, "completion_tokens": 50, "total_tokens": r.usage + 50}}


def _gemini(model: str, r: _Reply) -> dict:
    parts: list[dict] = [{"text": r.text}] if r.text else []
    if r.tool:
        parts.append({"functionCall": {"name": r.tool[0], "args": r.tool[1]}})
    return {"candidates": [{"content": {"role": "model", "parts": parts or [{"text": ""}]}, "finishReason": "STOP", "index": 0}],
            "usageMetadata": {"promptTokenCount": r.usage, "candidatesTokenCount": 50, "totalTokenCount": r.usage + 50},
            "modelVersion": model, "responseId": f"r{r.n:02d}"}


# -- the thread ----------------------------------------------------------------

PROMPT_A = "You are a research agent. Rules: answer plainly."
PROMPT_B = "You are a research agent. Rules: answer plainly and cite sources."
DESCRIPTION_A, DESCRIPTION_B = "Look something up.", "Look something up, at most limit rows."
NOW = datetime(2026, 7, 1, 14, 30, tzinfo=UTC)
THRESHOLD = 10_000
CHARTS = SkillDefinition(name="charts", description="Draw charts.", tool_names=("draw_chart",), skill_md_path="charts/SKILL.md")


def _png(size: tuple[int, int]) -> str:
    buffer = io.BytesIO()
    Image.new("RGB", size, (40, 128, 215)).save(buffer, "PNG")
    return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode("ascii")


@tool("get_data", description=DESCRIPTION_A)
def get_data_a(query: str) -> str:
    return f"data for {query}"


@tool("get_data", description=DESCRIPTION_B)
def get_data_b(query: str, limit: int = 10) -> str:
    return f"data for {query}"


@tool("Read")
def read_file(file_path: str) -> str:
    """Read a file."""
    return "# Charts\nUse draw_chart."


@tool
def draw_chart(symbol: str) -> str:
    """Draw a chart."""
    return f"chart of {symbol}"


@tool
def chart_image(symbol: str) -> list:
    """Render a chart image."""
    return [{"type": "text", "text": f"chart of {symbol}"}, {"type": "image_url", "image_url": {"url": _png((2600, 900))}}]


SCRIPT = [
    _Reply(1, tool=("Read", {"file_path": "/home/user/.agents/skills/charts/SKILL.md"}), usage=1500),
    _Reply(2, tool=("draw_chart", {"symbol": "NVDA"}), usage=1600),
    _Reply(3, tool=("chart_image", {"symbol": "NVDA"}), usage=1700),
    _Reply(4, text="Charted.", usage=2100),
    _Reply(5, tool=("get_data", {"query": "b", "limit": 5}), usage=2200),
    _Reply(6, text="Here too.", usage=9995),
    _Reply(7, text="Done.", usage=1600),
]

#: Each turn: the build's prompt and data tool, then the user's message.
TURNS = [
    (PROMPT_A, get_data_a, [{"type": "text", "text": "Chart NVDA."}, {"type": "image_url", "image_url": {"url": _png((3000, 1500))}}]),
    (PROMPT_B, get_data_b, "Get b."),
    (PROMPT_B, get_data_b, "Get c."),
]


def _session() -> MagicMock:
    session = MagicMock()
    session.sandbox.normalize_path = MagicMock(side_effect=lambda p: f"/home/workspace/{p}")
    session.sandbox.aread_file_text = AsyncMock(return_value="# Notes\nalpha")
    session.take_agent_md_writer = MagicMock(return_value=None)
    return session


def _stack(target: str, model: Any, system: str) -> list:
    """The main agent's middleware in the order agent.py gives it, for the
    part buildable without a sandbox."""
    summarizer = Summarizer(GenericFakeChatModel(messages=iter([AIMessage("<summary>\nFallback.\n</summary>")] * 5)), limit=100_000)
    return [
        FrozenPromptMiddleware(files_mounted=True, render=lambda _mounted: system),
        ToolArgumentParsingMiddleware(),
        SkillsMiddleware(skill_registry={"charts": CHARTS}, inject_manifest=False),
        CompactionMiddleware(summarizer, token_threshold=THRESHOLD, keep_tokens=THRESHOLD // 3, claude_keep_tokens=THRESHOLD // 3),
        ModelResilienceMiddleware(primary_name=target, primary_client=model, fallbacks=[], max_retries=0, jitter=False),
        MultimodalStripMiddleware(model_name=None if target in CUSTOM_ROUTES else target),
        AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
        SystemPromptCacheMiddleware(),
        ToolSurfaceMiddleware(),
        EmptyToolCallRetryMiddleware(),
        PatchToolCallsMiddleware(),
        TurnContextMiddleware(now=NOW, timezone="UTC"),
        BaselineContextMiddleware(
            session=_session(),
            blocks={"mcp_servers": lambda _s: "- market-data: quotes", "skills": lambda _s: "## Skills\n\n- **charts**"},
            model_name=target,
        ),
        NotesOffMiddleware(),
        MarketWatchMiddleware(min_interval_seconds=0, guidance=resolve_prompt_guidance(target)),
        RowCarrierMiddleware(),
        SystemPinMiddleware(),
        ReasoningCompatibilityMiddleware(),
    ]


def _agent(target: str, model: Any, system: str, data_tool, saver: InMemorySaver):
    return create_agent(
        model=model,
        tools=[read_file, data_tool, draw_chart, chart_image],
        system_prompt=system,
        middleware=_stack(target, model, system),
        checkpointer=saver,
        state_schema=MainAgentState,
    )


def test_the_stack_keeps_the_main_agents_order():
    """The recorded render holds for the main agent only while ``_stack``
    keeps the order PTCAgent builds, scratchpad off and market watch on."""
    from ptc_agent.agent.agent import PTCAgent
    from ptc_agent.config.agent import AgentConfig, LLMConfig
    from ptc_agent.config.core import DaytonaConfig, FilesystemConfig, LoggingConfig, MCPConfig, SandboxConfig, SecurityConfig

    llm = GenericFakeChatModel(messages=iter([]))
    config = AgentConfig(
        llm=LLMConfig(name="test-model"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
        features={"market_watch": True, "scratchpad": False},
    )
    built: list[list] = []

    def capture(_model, **kwargs):
        built.append(kwargs["middleware"])
        return MagicMock()

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch("ptc_agent.agent.middleware.compaction.compact.create_llm", return_value=llm),
        patch("ptc_agent.agent.agent.create_agent", side_effect=capture),
    ):
        PTCAgent(config).create_agent(llm=llm, thread_id="t-1", sandbox=MagicMock(), mcp_registry=MagicMock())

    ours = [type(m) for m in _stack("claude-sonnet-5-5", llm, "prompt")]
    assert [type(m) for m in built[0] if type(m) in ours] == ours


@contextlib.contextmanager
def _market():
    module = "ptc_agent.agent.middleware.market_watch"
    prices = iter(range(200, 400))

    def snapshots(*_args, **_kwargs):
        price = float(next(prices))
        return [{"symbol": "NVDA", "price": price, "change_percent": 1.0, "volume": 1_000_000,
                 "last_trade_price": price, "market_status": "open"}]

    provider = AsyncMock()
    provider.get_snapshots = AsyncMock(side_effect=snapshots)
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch(f"{module}.get_watchlist", AsyncMock(return_value=["NVDA"])))
        stack.enter_context(patch(f"{module}.get_market_data_provider", return_value=provider))
        stack.enter_context(patch(f"{module}._any_venue_open", return_value=True))
        stack.enter_context(patch("src.tools.market_data.quote_format.get_market_session",
                                  side_effect=lambda *_a, **_k: ("REGULAR_HOURS", NOW)))
        stack.enter_context(patch("src.tools.market_data.quote_format.venue_phase",
                                  return_value=MarketPhase.REGULAR))
        stack.enter_context(patch("src.server.database.provenance_bodies.store_result_body", AsyncMock()))
        yield


# -- what each request may carry ---------------------------------------------------


def _walk(value: Any, path: str = ""):
    if isinstance(value, dict):
        for key, item in value.items():
            yield f"{path}.{key}", key, item
            yield from _walk(item, f"{path}.{key}")
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _walk(item, f"{path}[{i}]")


def _leaks(call: _Call) -> list[str]:
    found = []
    header = call.headers.get("anthropic-beta", "")
    found += [f"anthropic-beta header carries {b}" for b in CLAUDE_ONLY_BETAS if b in header]
    if "beta=true" in call.url:
        found.append("sent to the beta Messages endpoint")
    raw = json.dumps(call.body)
    found += [f"body names {b}" for b in CLAUDE_ONLY_BETAS if b in raw]
    if isinstance(call.body, dict) and call.kind == "anthropic":
        found += [f"body.{key}" for key in ("betas", "compaction", "context_management") if key in call.body]
    for path, key, value in _walk(call.body):
        if key in CLAUDE_ONLY_KEYS:
            found.append(f"{path} present")
        if key == "type" and value in CLAUDE_ONLY_TYPES:
            found.append(f"{path} = {value}")
        if key == "type" and value == "compaction" and call.kind == "anthropic":
            found.append(f"{path} = compaction (Claude's block)")
    return found


def _tool_names(call: _Call) -> list[str]:
    tools = (call.body or {}).get("tools") or []
    if call.kind == "chat":
        return [t["function"]["name"] for t in tools]
    if call.kind == "gemini":
        return [d["name"] for t in tools for d in (t.get("functionDeclarations") or t.get("function_declarations") or [])]
    return [t.get("name") for t in tools]


@pytest.fixture(autouse=True)
def _offline_token_counter(monkeypatch):
    """cl100k_base fetches its BPE file on first use, which the unit suite's
    blocked sockets refuse on a cold cache (CI runners). Routes whose answers
    report no usage reach the third turn's summary on the estimate alone, so
    a token is two characters: enough to cross it, as cl100k does."""
    fake = SimpleNamespace(encode=lambda text: [0] * (len(text) // 2 + 1))
    monkeypatch.setattr(compaction_utils, "_get_tiktoken_encoder", lambda: fake)

@pytest.mark.asyncio
@pytest.mark.parametrize("target", _targets())
async def test_no_claude_only_field_reaches_another_model(monkeypatch, target):
    network = _Network(SCRIPT).install(monkeypatch)
    monkeypatch.setattr(compaction_module, "get_stream_writer", lambda: (lambda _payload: None))
    config = {"configurable": {"thread_id": f"t-isolation-{target}", "user_id": "u-1"}}
    saver = InMemorySaver()
    turn_starts = []
    with _market():
        for system, data_tool, content in TURNS:
            turn_starts.append(len(network.calls))
            model = _build(target)
            agent = _agent(target, model, system, data_tool, saver)
            await agent.ainvoke({"messages": [HumanMessage(content=content)]}, config, durability="sync")
    state = (await agent.aget_state(config)).values

    # The thread went where the scenario says, so a pass is not vacuous.
    assert not network.unexpected
    assert not network.script, "the thread stopped before its scripted answers ran out"
    assert any("<market-watch>" in json.dumps(call.body) for call in network.calls), "no call carried a stamp"
    assert state.get("_summarization_event"), "the third turn did not summarize"
    leaks = [f"call {i + 1}: {leak}" for i, call in enumerate(network.calls) for leak in _leaks(call)]
    assert leaks == []

    # The system prompt and the tools follow the build on every call: turn 1
    # sends build A, every call after the deploy build B, whatever was sent before.
    for i, call in enumerate(network.calls):
        raw = json.dumps(call.body)
        after_deploy = i >= turn_starts[1]
        assert (PROMPT_B if after_deploy else PROMPT_A) in raw, f"call {i + 1} sent another build's system prompt"
        assert (PROMPT_A if after_deploy else PROMPT_B) not in raw, f"call {i + 1} sent another build's system prompt"
        assert (DESCRIPTION_B if after_deploy else DESCRIPTION_A) in raw, f"call {i + 1} sent another build's tool"
        assert (DESCRIPTION_A if after_deploy else DESCRIPTION_B) not in raw, f"call {i + 1} sent another build's tool"

    # The skill's tool is offered in the tools array itself once the skill is read.
    answered = [call for call in network.calls if not call.summary]
    assert "draw_chart" not in _tool_names(answered[0])
    assert all("draw_chart" in _tool_names(call) for call in answered[1:])

    # Nothing Claude-only lands in the thread's history or state.
    assert not CLAUDE_ONLY_STATE & {k for k, v in state.items() if v}
    for message in state["messages"]:
        assert INPUT_TRANSFORMATIONS not in (getattr(message, "response_metadata", None) or {})
        assert not (isinstance(message, SystemMessage) and (message.additional_kwargs or {}).get("lc_source") == "tool_change")
        assert "clear_at" not in (message.additional_kwargs or {})


# -- the recorded render -------------------------------------------------------

RENDER_TARGETS = ["claude-sonnet-5-5", "deepseek-flash"]
_GOLDEN = Path(__file__).parent / "golden" / "thread_render.json"
_IMAGE_DATA = re.compile(r"(?:data:image/[a-z]+;base64,)?iVBORw0KGgo[A-Za-z0-9+/=]+")


async def _run_thread(monkeypatch, target: str) -> tuple[_Network, dict]:
    network = _Network(SCRIPT).install(monkeypatch)
    monkeypatch.setattr(compaction_module, "get_stream_writer", lambda: (lambda _payload: None))
    config = {"configurable": {"thread_id": f"t-render-{target}", "user_id": "u-1"}}
    saver = InMemorySaver()
    with _market():
        for system, data_tool, content in TURNS:
            agent = _agent(target, _build(target), system, data_tool, saver)
            await agent.ainvoke({"messages": [HumanMessage(content=content)]}, config, durability="sync")
    return network, (await agent.aget_state(config)).values


def _rendered(network: _Network) -> list[dict]:
    """Each call's URL, betas and body, images replaced by their size: PNG
    bytes depend on the zlib build, which differs between machines."""

    def image(match: re.Match) -> str:
        data = match.group(0).split(",", 1)[-1]
        width, height = Image.open(io.BytesIO(base64.b64decode(data))).size
        return f"<png {width}x{height}>"

    return [
        {
            "url": call.url,
            "anthropic-beta": call.headers.get("anthropic-beta"),
            "body": json.loads(_IMAGE_DATA.sub(image, json.dumps(call.body, sort_keys=True))),
        }
        for call in network.calls
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", RENDER_TARGETS)
async def test_a_thread_renders_as_recorded(monkeypatch, target):
    network, _ = await _run_thread(monkeypatch, target)
    rendered = _rendered(network)

    recorded = json.loads(_GOLDEN.read_text()) if _GOLDEN.exists() else {}
    if os.environ.get("LANGALPHA_RECORD_RENDER"):
        _GOLDEN.parent.mkdir(exist_ok=True)
        _GOLDEN.write_text(json.dumps({**recorded, target: rendered}, indent=1, sort_keys=True) + "\n")
        return
    assert target in recorded, "no recorded render; run with LANGALPHA_RECORD_RENDER=1"
    for i, (now, then) in enumerate(zip(rendered, recorded[target], strict=False)):
        assert now == then, f"call {i + 1} renders differently from the recorded thread"
    assert len(rendered) == len(recorded[target])


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["claude-sonnet-5-5", "claude-haiku-5-5"])
async def test_every_stamp_claude_is_sent_stays_turn_scoped(monkeypatch, target):
    """No path the thread takes (a skill load, a deploy, the summary that
    brings it in, images) sends a stamp to Claude as a user turn: one there
    would be content a later request leaves out or sends as a system message."""
    network, _ = await _run_thread(monkeypatch, target)

    stamps = [
        (i + 1, message)
        for i, call in enumerate(network.calls)
        for message in call.body["messages"]
        if any(
            block.get("type") == "text"
            and block["text"].removeprefix("<system-reminder>\n").lstrip().startswith(("<market-watch>", "Runtime context"))
            and "<market-watch>" in block["text"]
            for block in (message["content"] if isinstance(message["content"], list) else [{"type": "text", "text": message["content"]}])
        )
    ]
    assert any(call.summary for call in network.calls), "the thread did not summarize"
    assert len(stamps) > len(network.calls)
    for call, message in stamps:
        assert message["role"] == "system" and message.get("clear_at") == "next_user_message", call
