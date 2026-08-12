"""OpenAI prompt-cache breakpoint placement across the middleware chain.

Mirror of test_prompt_cache_breakpoint.py for OpenAIPromptCachingMiddleware:
verifies the breakpoint marker lands on the last static block (skills), that
dynamic blocks appended by inner middleware stay unmarked, that gating is
manifest-driven (prompt_cache_options on the model), and that the marker plus
prompt_cache_options survive all the way into the Responses API payload.
"""

from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic.chat_models import ChatAnthropic
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from ptc_agent.agent.middleware._utils import append_to_system_message
from ptc_agent.agent.middleware.openai_prompt_caching import OpenAIPromptCachingMiddleware
from ptc_agent.agent.middleware.runtime_context import RuntimeContextMiddleware
from ptc_agent.agent.middleware.workspace_context import WorkspaceContextMiddleware


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_openai_base_env(monkeypatch):
    """The endpoint gate reads these env fallbacks; isolate from the host env."""
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)


def _make_openai_model(prompt_cache_options=None, openai_api_base=None):
    model = MagicMock(spec=ChatOpenAI)
    model.prompt_cache_options = prompt_cache_options
    model.openai_api_base = openai_api_base
    return model


def _make_model_request(system_prompt: str, model) -> ModelRequest:
    return ModelRequest(
        model=model,
        messages=[],
        system_prompt=system_prompt,
    )


def _fake_skills_middleware():
    class FakeSkillsMiddleware:
        async def awrap_model_call(
            self,
            request: ModelRequest,
            handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        ) -> ModelResponse:
            new_sys = append_to_system_message(
                request.system_message,
                "<skills_manifest>\n- skill_a\n- skill_b\n</skills_manifest>",
            )
            return await handler(request.override(system_message=new_sys))

    return FakeSkillsMiddleware()


def _compose_middleware(middlewares, final_handler):
    """Compose middlewares: first in list = outermost = runs first."""

    async def chain(request: ModelRequest) -> ModelResponse:
        return await final_handler(request)

    for mw in reversed(middlewares):
        outer_handler = chain

        async def wrapper(req, *, _mw=mw, _h=outer_handler):
            return await _mw.awrap_model_call(req, _h)

        chain = wrapper

    return chain


def _build_chain(captured: dict):
    """The agent.py stack shape: skills → anthropic → openai → workspace → runtime."""
    session = MagicMock()
    session.get_agent_md = AsyncMock(return_value="# Workspace\nNotes")
    session.conversation_id = "ws-test"

    async def capture(req):
        captured["req"] = req
        return MagicMock()

    return _compose_middleware(
        [
            _fake_skills_middleware(),
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            OpenAIPromptCachingMiddleware(),
            WorkspaceContextMiddleware(session=session),
            RuntimeContextMiddleware(
                current_time="12:00 PM UTC, Monday, April 5, 2027",
                user_profile={"name": "Casey", "timezone": "UTC", "locale": "en-US"},
            ),
        ],
        capture,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOpenAIPromptCacheBreakpoint:
    @pytest.mark.asyncio
    async def test_block_ordering_and_breakpoint(self):
        """Marker on the skills block only; anthropic cache_control absent."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options={"mode": "implicit"})

        await chain(_make_model_request("Static system prompt.", model))

        content = captured["req"].system_message.content
        assert isinstance(content, list)
        assert len(content) == 4

        assert "prompt_cache_breakpoint" not in content[0]
        assert "skills_manifest" in content[1]["text"]
        assert content[1]["prompt_cache_breakpoint"] == {"mode": "explicit"}
        assert "prompt_cache_breakpoint" not in content[2]
        assert "prompt_cache_breakpoint" not in content[3]

        # AnthropicPromptCachingMiddleware must have no-opped for ChatOpenAI
        for block in content:
            assert "cache_control" not in block

    @pytest.mark.asyncio
    async def test_noop_without_prompt_cache_options(self):
        """A ChatOpenAI model that hasn't opted in via the manifest is untouched."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options=None)

        await chain(_make_model_request("Static system prompt.", model))

        for block in captured["req"].system_message.content:
            assert "prompt_cache_breakpoint" not in block

    @pytest.mark.asyncio
    async def test_noop_for_non_official_base_url(self):
        """Opted-in options but a non-official endpoint → marker withheld."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(
            prompt_cache_options={"mode": "implicit"},
            openai_api_base="https://proxy.example.com/v1",
        )

        await chain(_make_model_request("Static system prompt.", model))

        for block in captured["req"].system_message.content:
            assert "prompt_cache_breakpoint" not in block

    @pytest.mark.asyncio
    async def test_applies_for_explicit_official_base_url(self):
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(
            prompt_cache_options={"mode": "implicit"},
            openai_api_base="https://api.openai.com/v1",
        )

        await chain(_make_model_request("Static system prompt.", model))

        content = captured["req"].system_message.content
        assert content[1]["prompt_cache_breakpoint"] == {"mode": "explicit"}

    @pytest.mark.asyncio
    async def test_noop_for_env_base_url_override(self, monkeypatch):
        """OPENAI_BASE_URL env redirect counts as a non-official endpoint."""
        monkeypatch.setenv("OPENAI_BASE_URL", "http://localhost:9000/v1")
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options={"mode": "implicit"})

        await chain(_make_model_request("Static system prompt.", model))

        for block in captured["req"].system_message.content:
            assert "prompt_cache_breakpoint" not in block

    @pytest.mark.asyncio
    async def test_noop_for_anthropic_model(self):
        """Anthropic models get cache_control, never the OpenAI marker."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = MagicMock(spec=ChatAnthropic)

        await chain(_make_model_request("Static system prompt.", model))

        content = captured["req"].system_message.content
        assert any("cache_control" in b for b in content)
        for block in content:
            assert "prompt_cache_breakpoint" not in block

    def test_string_system_message_converted_to_tagged_block(self):
        """A plain-string system message becomes a single tagged text block."""
        mw = OpenAIPromptCachingMiddleware()
        tagged = mw._tag_system_message(SystemMessage(content="Static prompt."))
        assert tagged.content == [
            {
                "type": "text",
                "text": "Static prompt.",
                "prompt_cache_breakpoint": {"mode": "explicit"},
            }
        ]

    def test_wire_payload_carries_marker_and_options(self):
        """Marker + prompt_cache_options reach the Responses API payload."""
        model = ChatOpenAI(
            model="gpt-5.6-sol",
            api_key="test",
            reasoning={"effort": "medium", "summary": "auto"},
            prompt_cache_options={"mode": "implicit"},
        )
        mw = OpenAIPromptCachingMiddleware()
        sys_msg = mw._tag_system_message(SystemMessage(content="Static prompt."))
        sys_msg = append_to_system_message(sys_msg, "dynamic context")

        payload = model._get_request_payload([sys_msg, HumanMessage("hi")])

        assert payload["prompt_cache_options"] == {"mode": "implicit"}
        system_item = payload["input"][0]
        assert system_item["role"] == "system"
        parts = system_item["content"]
        assert parts[0]["prompt_cache_breakpoint"] == {"mode": "explicit"}
        assert "prompt_cache_breakpoint" not in parts[1]
