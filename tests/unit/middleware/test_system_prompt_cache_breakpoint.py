"""System-prompt cache breakpoint placement (OpenAI, Claude over OpenRouter) across the middleware chain.

Mirror of test_prompt_cache_breakpoint.py for SystemPromptCacheMiddleware:
verifies the breakpoint marker lands on the static prefix's own block, that
the per-thread baseline block appended by inner middleware takes a marker of
its own, that breakpoint 4 lands on the turn row in the message list rather
than on the system message, that gating is manifest-driven
(prompt_cache_options on the model), and that the marker plus
prompt_cache_options survive all the way into the Responses API payload.

Nothing appends a system block between the prefix and the baseline any more:
the skills manifest and the MCP roster are frozen into the baseline.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic.chat_models import ChatAnthropic
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from ptc_agent.agent.middleware._utils import append_to_system_message
from ptc_agent.agent.middleware.system_prompt_caching import SystemPromptCacheMiddleware
from ptc_agent.agent.middleware.runtime_context import (
    ENVELOPE_CLOSE,
    ENVELOPE_OPEN,
    BaselineContextMiddleware,
    RowCarrierMiddleware,
    TurnContextMiddleware,
)
from ptc_agent.agent.middleware.runtime_context.changes import sha256_text


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


_AGENT_MD = "# Workspace\nNotes"

SKILLS_MANIFEST = "## Available Skills\n\n- **pdf**: read and write PDFs"


def _baseline_state() -> dict:
    """A frozen baseline as ``before_agent`` would have written it."""
    return {
        "runtime_baseline": {
            "epoch": 1,
            "built_at": "2027-04-05T12:00:00+00:00",
            "workspace": {"name": "", "description": ""},
            "identity": {
                "name": "User",
                "timezone": "UTC",
                "locale": "en-US",
                "preferred_market": "US",
            },
            "agent_md": {
                "text": _AGENT_MD,
                "sha256": sha256_text(_AGENT_MD),
                "path": "/agent.md",
                "exists": True,
            },
            "memory": {},
            "blocks": {
                "skills": {
                    "text": SKILLS_MANIFEST,
                    "sha256": sha256_text(SKILLS_MANIFEST),
                    "path": "<skills>",
                    "exists": True,
                }
            },
            "memo": {},
            "memory_fill": {"percent": 0, "ceiling_tokens": 2048},
        }
    }


NOW = datetime(2027, 4, 5, 12, 0, tzinfo=UTC)


async def _turn_rows() -> list:
    """The turn's anchor row, exactly as the turn middleware writes it."""
    written = await TurnContextMiddleware(now=NOW, preferred_market="US").abefore_agent(
        {}
    )
    return list((written or {}).get("messages") or [])


def _make_model_request(
    system_prompt: str, model, rows: list | None = None
) -> ModelRequest:
    return ModelRequest(
        model=model,
        messages=[HumanMessage(content="What moved today?"), *(rows or [])],
        system_prompt=system_prompt,
        state=_baseline_state(),
    )


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
    """The agent.py stack shape: anthropic → openai → baseline → tail."""

    async def capture(req):
        captured["req"] = req
        return MagicMock()

    return _compose_middleware(
        [
            AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
            SystemPromptCacheMiddleware(),
            BaselineContextMiddleware(session=None, guidance="lean"),
            RowCarrierMiddleware(),
        ],
        capture,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOpenAIPromptCacheBreakpoint:
    @pytest.mark.asyncio
    async def test_block_ordering_and_breakpoint(self):
        """Marker on the prefix and the baseline; anthropic cache_control absent."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options={"mode": "implicit"})

        await chain(
            _make_model_request("Static system prompt.", model, await _turn_rows())
        )

        content = captured["req"].system_message.content
        assert isinstance(content, list)
        assert len(content) == 2

        # The static prefix is the last block the caching middleware sees, so
        # it takes breakpoint 2 itself.
        assert content[0]["text"] == "Static system prompt."
        assert content[0]["prompt_cache_breakpoint"] == {"mode": "explicit"}
        # The baseline block takes breakpoint 3; agent.md and the frozen
        # skills manifest both ride in it.
        assert "agentmd" in content[1]["text"]
        assert f"<skills>\n{SKILLS_MANIFEST}\n</skills>" in content[1]["text"]
        assert content[1]["prompt_cache_breakpoint"] == {"mode": "explicit"}

        # The turn row rides as the last block of the last user message and
        # takes breakpoint 4: it is history, so the next call carries the
        # boundary it writes.
        blocks = captured["req"].messages[-1].content
        assert len(blocks) == 2
        assert blocks[0]["text"] == "What moved today?"
        assert "prompt_cache_breakpoint" not in blocks[0]
        assert blocks[-1]["text"].startswith(ENVELOPE_OPEN)
        assert blocks[-1]["text"].endswith(ENVELOPE_CLOSE)
        assert "12:00 PM UTC, Monday, April 5, 2027" in blocks[-1]["text"]
        assert blocks[-1]["prompt_cache_breakpoint"] == {"mode": "explicit"}

        # AnthropicPromptCachingMiddleware must have no-opped for ChatOpenAI
        for block in content:
            assert "cache_control" not in block

    @pytest.mark.asyncio
    async def test_a_call_with_nothing_new_is_handed_over_untouched(self):
        """No rows and no call updates: the tail leaves the messages alone."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options={"mode": "implicit"})

        await chain(_make_model_request("Static system prompt.", model))

        messages = captured["req"].messages
        assert len(messages) == 1
        assert messages[0].content == "What moved today?"

    @pytest.mark.asyncio
    async def test_noop_without_prompt_cache_options(self):
        """A ChatOpenAI model that hasn't opted in via the manifest is untouched."""
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options=None)

        await chain(
            _make_model_request("Static system prompt.", model, await _turn_rows())
        )

        for block in captured["req"].system_message.content:
            assert "prompt_cache_breakpoint" not in block
        # Untagged model: nothing in the merged user message is marked either.
        for block in captured["req"].messages[-1].content:
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

        await chain(
            _make_model_request("Static system prompt.", model, await _turn_rows())
        )

        for block in captured["req"].system_message.content:
            assert "prompt_cache_breakpoint" not in block
        # Untagged model: nothing in the merged user message is marked either.
        for block in captured["req"].messages[-1].content:
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
        assert content[0]["prompt_cache_breakpoint"] == {"mode": "explicit"}
        assert content[1]["prompt_cache_breakpoint"] == {"mode": "explicit"}

    @pytest.mark.asyncio
    async def test_noop_for_env_base_url_override(self, monkeypatch):
        """OPENAI_BASE_URL env redirect counts as a non-official endpoint."""
        monkeypatch.setenv("OPENAI_BASE_URL", "http://localhost:9000/v1")
        captured: dict = {}
        chain = _build_chain(captured)
        model = _make_openai_model(prompt_cache_options={"mode": "implicit"})

        await chain(
            _make_model_request("Static system prompt.", model, await _turn_rows())
        )

        for block in captured["req"].system_message.content:
            assert "prompt_cache_breakpoint" not in block
        # Untagged model: nothing in the merged user message is marked either.
        for block in captured["req"].messages[-1].content:
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
        mw = SystemPromptCacheMiddleware()
        tagged = mw._tag_system_message(
            SystemMessage(content="Static prompt."), "prompt_cache_breakpoint", {"mode": "explicit"}
        )
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
        mw = SystemPromptCacheMiddleware()
        sys_msg = mw._tag_system_message(
            SystemMessage(content="Static prompt."), "prompt_cache_breakpoint", {"mode": "explicit"}
        )
        sys_msg = append_to_system_message(sys_msg, "dynamic context")

        payload = model._get_request_payload([sys_msg, HumanMessage("hi")])

        assert payload["prompt_cache_options"] == {"mode": "implicit"}
        system_item = payload["input"][0]
        assert system_item["role"] == "system"
        parts = system_item["content"]
        assert parts[0]["prompt_cache_breakpoint"] == {"mode": "explicit"}
        assert "prompt_cache_breakpoint" not in parts[1]


class TestOpenRouterClaudeBreakpoints:
    """OpenRouter forwards ``cache_control`` to Anthropic, which caches nothing
    without it; every other model there caches on its own and is left alone."""

    @pytest.mark.asyncio
    async def test_claude_takes_cache_control_on_all_three_boundaries(self):
        from langchain_openrouter import ChatOpenRouter

        captured: dict = {}
        model = ChatOpenRouter(model="anthropic/claude-haiku-5.5", api_key="test")

        await _build_chain(captured)(
            _make_model_request("Static system prompt.", model, await _turn_rows())
        )

        marker = {"type": "ephemeral"}
        content = captured["req"].system_message.content
        assert [block.get("cache_control") for block in content] == [marker, marker]
        assert captured["req"].messages[-1].content[-1]["cache_control"] == marker

    @pytest.mark.asyncio
    async def test_other_vendors_get_no_marker(self):
        from langchain_openrouter import ChatOpenRouter

        captured: dict = {}
        model = ChatOpenRouter(model="openai/gpt-oss-120b", api_key="test")

        await _build_chain(captured)(
            _make_model_request("Static system prompt.", model, await _turn_rows())
        )

        content = captured["req"].system_message.content
        blocks = content if isinstance(content, list) else []
        assert not any("cache_control" in b or "prompt_cache_breakpoint" in b for b in blocks)
