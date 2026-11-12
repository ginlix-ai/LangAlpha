"""What a call sends ahead of its messages, by the part of the context each
piece is: counted by the provider where it counts a request itself, else
estimated, and never during a turn."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ModelResponse
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import StructuredTool

from ptc_agent.agent.middleware.compaction import middleware as middleware_module
from ptc_agent.agent.middleware.compaction import parts
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.usage_event import token_usage_fields
from ptc_agent.agent.middleware.model_resilience import FALLBACK_KEY
from src.llms.extension.anthropic_oauth import ChatAnthropicOAuth

SYSTEM = SystemMessage("s" * 200)

SECTIONS = {
    "workspace": "w" * 10,
    "mcp_servers": "m" * 20,
    "skills": "k" * 30,
    "scratchpad": "p" * 5,
    "agent_md": "a" * 40,
    "memory:user": "u" * 50,
    "memo": "o" * 60,
}


def _count(messages) -> int:
    return sum(len(m.text) for m in messages)


def _tools() -> list:
    return [StructuredTool.from_function(lambda query: query, name="lookup", description="d" * 300)]


def _claude(cls=ChatAnthropic, **kwargs):
    kwargs = {"model": "claude-haiku-5-5", "anthropic_api_url": "https://api.anthropic.com", **kwargs}
    return cls(api_key="k", **kwargs)


def _provider_counts(monkeypatch, count=None):
    """Stand in for the provider's count endpoint: a message's tokens are its
    text's length, and tools cost a flat thousand."""
    asked = []

    def counted(self, messages, tools=None):
        asked.append(([m.text for m in messages], bool(tools)))
        return sum(len(m.text) for m in messages) + (1000 if tools else 0)

    monkeypatch.setattr(ChatAnthropic, "get_num_tokens_from_messages", count or counted)
    return asked


async def _parts(model=None, system=SYSTEM, tools=None, sections=SECTIONS):
    return await parts.ahead_parts(
        system, _tools() if tools is None else tools, sections, model=model, counter=_count
    )


class TestAheadParts:
    @pytest.mark.asyncio
    async def test_each_baseline_section_counts_under_its_part(self):
        # A part's sections are one block, joined as they are sent.
        counted = await _parts()

        assert list(counted) == ["system", "tools", "mcp", "skills", "memory", "environment"]
        assert (counted["system"], counted["mcp"], counted["skills"]) == (200, 20, 30)
        assert (counted["memory"], counted["environment"]) == (154, 17)
        assert counted["tools"] > 300

    @pytest.mark.asyncio
    async def test_nothing_ahead_has_no_parts(self):
        assert await _parts(system=None, tools=[], sections={}) == {}

    @pytest.mark.asyncio
    async def test_plain_tool_specs_count_as_their_json(self):
        spec = {"name": "Bash", "description": "Run a command.", "input_schema": {"type": "object"}}

        counted = await _parts(tools=[spec], sections={})

        # As the pin hands them over: already the shape Claude is sent.
        assert counted == {"system": 200, "tools": len(json.dumps([spec]))}

    @pytest.mark.asyncio
    async def test_claude_on_its_own_api_is_counted_by_the_provider(self, monkeypatch):
        # Each part is one request beside a one-token message, whose own count
        # comes off; a part's sections go as one system text.
        asked = _provider_counts(monkeypatch)

        counted = await _parts(model=_claude())

        assert counted == {"system": 200, "tools": 1000, "mcp": 20, "skills": 30, "memory": 154, "environment": 17}
        assert (["."], False) in asked
        assert (["a" * 40 + "\n\n" + "u" * 50 + "\n\n" + "o" * 60, "."], False) in asked

    @pytest.mark.asyncio
    async def test_a_count_is_sent_once_and_gives_up_with_the_wait(self, monkeypatch):
        # A turn's client retries a request for minutes. The count's own does
        # not, even copied from a model whose client is already built.
        clients = []

        def counted(self, messages, tools=None):
            clients.append((self._client.max_retries, self._client.timeout))
            return 1

        _provider_counts(monkeypatch, counted)
        turn = _claude(max_retries=5, timeout=600.0)
        assert turn._client.max_retries == 5

        await _parts(model=turn)

        assert set(clients) == {(0, parts.PROVIDER_COUNT_TIMEOUT_S)}
        assert turn._client.max_retries == 5

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "model",
        [
            pytest.param(lambda new: new(cls=ChatAnthropicOAuth), id="a subscription's token"),
            pytest.param(lambda new: new(anthropic_api_url="https://api.deepseek.com/anthropic"), id="another host"),
            pytest.param(lambda new: new(model="deepseek-flash"), id="another model"),
            pytest.param(lambda new: new(metadata={"provider_route": "anthropic@proxy.example.com"}), id="a repointed key"),
        ],
    )
    async def test_only_claude_on_its_own_api_under_a_key_is_counted_there(self, monkeypatch, model):
        def counted(self, messages, tools=None):
            raise AssertionError("asked the provider")

        _provider_counts(monkeypatch, counted)

        counted = await _parts(model=model(_claude))

        assert (counted["system"], counted["memory"]) == (200, 154)

    @pytest.mark.asyncio
    async def test_the_estimate_stands_in_for_a_count_that_fails(self, monkeypatch):
        def counted(self, messages, tools=None):
            raise RuntimeError("429")

        _provider_counts(monkeypatch, counted)

        counted = await _parts(model=_claude())

        assert (counted["system"], counted["memory"]) == (200, 154)


@pytest.mark.asyncio
async def test_a_turns_call_counts_no_parts_and_stamps_its_models_window(monkeypatch):
    # The count converts every tool schema, so it waits to be asked for
    # (``between_turns.Parts``); a turn's answer carries only what is free.
    from langchain.agents.middleware.types import ModelRequest

    def ahead_parts(*_args, **_kwargs):
        raise AssertionError("counted during a turn")

    monkeypatch.setattr(middleware_module, "ahead_parts", ahead_parts)
    summarizer = SimpleNamespace(counter=_count, compact=None)
    mw = CompactionMiddleware(summarizer, token_threshold=10**6, keep_tokens=0, self_summary=False)
    mw = mw.with_baseline(lambda state: {"workspace": "b" * 300})
    usage = {
        "input_tokens": 2_000,
        "output_tokens": 10,
        "total_tokens": 2_010,
        "input_token_details": {"cache_read": 1_500},
    }
    answer = AIMessage("done", id="done", usage_metadata=usage)

    async def handler(request):
        return ModelResponse(result=[answer])

    messages = [HumanMessage("x" * 100, id="h0")]
    model = GenericFakeChatModel(messages=iter([]))
    model.profile = {"max_input_tokens": 64_000}
    request = ModelRequest(
        model=model,
        messages=messages,
        system_message=SYSTEM,
        tool_choice=None,
        tools=_tools(),
        response_format=None,
        state={"messages": messages},
        runtime=None,
    )
    await mw.awrap_model_call(request, handler)

    assert token_usage_fields(answer) == {
        "input_tokens": 2_000,
        "output_tokens": 10,
        "total_tokens": 2_010,
        "cached_tokens": 1_500,
        "threshold": 10**6,
        "context_limit": 64_000,
    }

    # A fallback answered from a model whose window this request does not hold.
    by_fallback = AIMessage(
        "done", id="fallback", usage_metadata=usage, response_metadata={FALLBACK_KEY: "fallback-a"}
    )

    async def falls_back(request):
        return ModelResponse(result=[by_fallback])

    await mw.awrap_model_call(request, falls_back)

    assert "context_limit" not in token_usage_fields(by_fallback)
