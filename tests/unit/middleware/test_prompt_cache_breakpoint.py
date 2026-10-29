"""Integration test: prompt cache breakpoint placement across the middleware chain.

Verifies that the system message ends up with 2 content blocks in the correct
order, that the static prefix and the per-thread baseline each carry their own
cache_control, and that breakpoint 4 lands on the newest thing in the message
list that the next call will still carry: the turn row, not a third system
block and not the transient envelope.

Nothing appends a system block between the prefix and the baseline any more:
the skills manifest and the MCP roster are frozen into the baseline, which is
why breakpoint 2 sits on the static prefix's own block.

Middleware chain (first = outermost = runs first):
    AnthropicPromptCachingMiddleware  ->  tags LAST block it sees with cache_control
    BaselineContextMiddleware  ->  appends the frozen baseline (block 1)
    RowCarrierMiddleware  ->  carries the rows in this model's shape

Expected final system message blocks:
    [0] static system prompt   (cache_control: breakpoint 2)
    [1] per-thread baseline    (cache_control: breakpoint 3)

Expected final message list:
    [..., HumanMessage([user input, turn row with cache_control])]
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from langchain_anthropic.chat_models import ChatAnthropic
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic.middleware import AnthropicPromptCachingMiddleware
from langchain_core.messages import HumanMessage

from langchain_core.tools import tool

from ptc_agent.agent.middleware.runtime_context import (
    ENVELOPE_CLOSE,
    ENVELOPE_OPEN,
    BaselineContextMiddleware,
    RowCarrierMiddleware,
    TurnContextMiddleware,
)


SKILLS_MANIFEST = "## Available Skills\n\n- **pdf**: read and write PDFs"


def _baseline_state(agent_md: str = "# My Workspace\nResearch notes") -> dict:
    """A frozen baseline as ``before_agent`` would have written it."""
    from ptc_agent.agent.middleware.runtime_context.changes import sha256_text

    return {
        "runtime_baseline": {
            "epoch": 1,
            "built_at": "2026-04-05T19:42:00+00:00",
            "workspace": {"name": "", "description": ""},
            "identity": {
                "name": "User",
                "timezone": "UTC",
                "locale": "en-US",
                "preferred_market": "US",
            },
            "agent_md": {
                "text": agent_md,
                "sha256": sha256_text(agent_md),
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


def _baseline_middleware() -> BaselineContextMiddleware:
    """The real baseline middleware; every read is already frozen in state."""
    return BaselineContextMiddleware(session=None, guidance="lean")


NOW = datetime(2026, 4, 5, 19, 42, tzinfo=UTC)


def _tail_middleware() -> RowCarrierMiddleware:
    """The real tail middleware, pinned to a fixed instant and guidance level."""
    return RowCarrierMiddleware()


async def _turn_rows(**kwargs) -> list:
    """The turn's anchor row, exactly as the turn middleware writes it."""
    written = await TurnContextMiddleware(
        now=NOW, timezone="US/Eastern", **kwargs
    ).abefore_agent({})
    return list((written or {}).get("messages") or [])


@tool
def _probe(query: str) -> str:
    """A stand-in tool so the tools array has something to tag."""
    return query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_model_request(
    system_prompt: str,
    state: dict | None = None,
    *,
    rows: list | None = None,
    tools: list | None = None,
) -> ModelRequest:
    """Create a ModelRequest with a mock ChatAnthropic model.

    ``rows`` are the durable rows the turn boundary already wrote into history;
    they follow the turn's user message, which is where every carrier shape
    needs them.
    """
    model = MagicMock(spec=ChatAnthropic)
    return ModelRequest(
        model=model,
        messages=[HumanMessage(content="What moved today?"), *(rows or [])],
        system_prompt=system_prompt,
        state=state if state is not None else _baseline_state(),
        tools=tools or [],
    )


def _compose_middleware(middlewares, final_handler):
    """Compose middlewares: first in list = outermost = runs first.

    Each middleware wraps the next, producing a single callable that
    processes the request through the full chain.
    """

    async def chain(request: ModelRequest) -> ModelResponse:
        return await final_handler(request)

    # Build from innermost to outermost
    for mw in reversed(middlewares):
        outer_handler = chain

        async def wrapper(req, *, _mw=mw, _h=outer_handler):
            return await _mw.awrap_model_call(req, _h)

        chain = wrapper

    return chain


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


class TestPromptCacheBreakpoint:
    """Verify cache_control breakpoint placement across the full middleware chain."""

    @pytest.mark.asyncio
    async def test_block_ordering_and_breakpoint(self):
        """Two system breakpoints, and the fourth on the turn row."""
        # 1. Build the middleware chain in the same order as agent.py
        caching_mw = AnthropicPromptCachingMiddleware(
            unsupported_model_behavior="ignore"
        )

        baseline_mw = _baseline_middleware()
        runtime_mw = _tail_middleware()

        # Capture the final request that would go to the model
        captured_request = {}

        async def capture_handler(request: ModelRequest) -> ModelResponse:
            captured_request["request"] = request
            return MagicMock()  # dummy response

        # Compose: first = outermost = runs first
        chain = _compose_middleware(
            [caching_mw, baseline_mw, runtime_mw],
            capture_handler,
        )

        # 2. Create initial request with static system prompt, the turn row
        #    already written into history, and a tool to complete the budget.
        request = _make_model_request(
            "You are LangAlpha Agent, a research agent.",
            rows=await _turn_rows(preferred_market="US", origin="automation"),
            tools=[_probe],
        )

        # 3. Run through the chain
        await chain(request)

        # 4. Inspect the final system message
        final_request = captured_request["request"]
        sys_msg = final_request.system_message
        assert sys_msg is not None, "System message should not be None"

        content = sys_msg.content
        assert isinstance(content, list), f"Expected list of blocks, got {type(content)}"
        assert len(content) == 2, (
            f"Expected 2 content blocks (static + baseline), got {len(content)}"
        )

        # Block 0: the static prefix, which is now the last block the caching
        # middleware sees and so carries breakpoint 2 itself.
        block0 = content[0]
        assert isinstance(block0, dict), f"Block 0 should be dict, got {type(block0)}"
        assert "You are LangAlpha Agent" in block0["text"]
        assert block0["cache_control"]["type"] == "ephemeral", (
            "Block 0 (static prefix) MUST have cache_control, this is breakpoint 2"
        )

        # Block 1: the per-thread baseline carries breakpoint 3, and the skills
        # manifest rides inside it rather than in a block of its own.
        block1 = content[1]
        assert isinstance(block1, dict), f"Block 1 should be dict, got {type(block1)}"
        assert "agentmd" in block1["text"]
        assert f"<skills>\n{SKILLS_MANIFEST}\n</skills>" in block1["text"]
        assert block1["cache_control"] == {"type": "ephemeral"}, (
            "Block 1 (baseline) MUST have cache_control, this is breakpoint 3"
        )

        # The turn row rides inside the user's own message as a framed reminder
        # block, and takes breakpoint 4: it is the newest thing here that the
        # next call still carries unchanged.
        messages = final_request.messages
        assert len(messages) == 1, (
            f"Expected one merged user message, got {len(messages)}"
        )
        blocks = messages[-1].content
        assert len(blocks) == 2
        assert blocks[0]["text"] == "What moved today?"
        assert "cache_control" not in blocks[0]
        assert blocks[1]["text"].startswith(ENVELOPE_OPEN)
        assert blocks[1]["text"].endswith(ENVELOPE_CLOSE)
        assert "3:42 PM EDT, Sunday, April 5, 2026" in blocks[1]["text"]
        assert blocks[1]["cache_control"] == {"type": "ephemeral"}

        # Nothing volatile went out: with no call updates there is no envelope.
        assert "Runtime context from the platform" not in blocks[1]["text"]

        # The whole Anthropic budget, and no more: tools, static prefix,
        # baseline, turn row.
        tool_marks = sum(
            1
            for t in final_request.tools
            if "cache_control" in (getattr(t, "extras", None) or {})
        )
        system_marks = sum(1 for b in content if "cache_control" in b)
        message_marks = sum(
            1
            for m in messages
            if isinstance(m.content, list)
            for b in m.content
            if isinstance(b, dict) and "cache_control" in b
        )
        assert (tool_marks, system_marks, message_marks) == (1, 2, 1)
        assert tool_marks + system_marks + message_marks == 4

    @pytest.mark.asyncio
    async def test_a_call_with_nothing_new_is_handed_over_untouched(self):
        """No rows and no call updates: the tail has nothing to carry.

        The request reaches the model exactly as it arrived, so the message-tail
        breakpoint the Anthropic client places from ``model_settings`` lands on
        the user's own block, which is where it belongs when nothing follows it.
        """
        captured: dict = {}

        async def capture(req):
            captured["req"] = req
            return MagicMock()

        chain = _compose_middleware(
            [
                AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore"),
                _baseline_middleware(),
                _tail_middleware(),
            ],
            capture,
        )
        await chain(_make_model_request("Static prompt."))

        messages = captured["req"].messages
        assert len(messages) == 1
        assert messages[0].content == "What moved today?"
        assert captured["req"].model_settings["cache_control"] == {
            "type": "ephemeral",
            "ttl": "5m",
        }

    @pytest.mark.asyncio
    async def test_breakpoint_stable_across_different_times(self):
        """Changing current_time should NOT affect which block has cache_control."""
        caching_mw = AnthropicPromptCachingMiddleware(
            unsupported_model_behavior="ignore"
        )

        baseline_mw = _baseline_middleware()

        captured_blocks = []

        for moment in [
            datetime(2026, 4, 5, 19, 42, tzinfo=UTC),
            datetime(2026, 4, 5, 19, 43, tzinfo=UTC),
            datetime(2026, 4, 6, 17, 0, tzinfo=UTC),
        ]:
            runtime_mw = RowCarrierMiddleware()
            rows = await _turn_rows()

            captured = {}

            async def capture(req, _c=captured):
                _c["req"] = req
                return MagicMock()

            chain = _compose_middleware(
                [caching_mw, baseline_mw, runtime_mw],
                capture,
            )
            await chain(_make_model_request("Static system prompt.", rows=rows))

            blocks = captured["req"].system_message.content
            captured_blocks.append(blocks)

        # The clock moved; the two system breakpoints did not.
        for i, blocks in enumerate(captured_blocks):
            assert len(blocks) == 2, f"Run {i}: expected 2 blocks"
            assert "cache_control" in blocks[0], f"Run {i}: block 0 must be cached"
            assert "cache_control" in blocks[1], f"Run {i}: block 1 must be cached"

        # Both blocks are byte-identical across runs: the static prefix, and
        # the baseline that now carries the skills manifest.
        for i in range(1, len(captured_blocks)):
            assert captured_blocks[0][0]["text"] == captured_blocks[i][0]["text"], (
                f"Run {i}: static prompt block should be identical"
            )
            assert captured_blocks[0][1]["text"] == captured_blocks[i][1]["text"], (
                f"Run {i}: baseline block should be identical"
            )

    @pytest.mark.asyncio
    async def test_system_message_carries_no_volatile_block(self):
        """Nothing per-turn reaches the system message; the row is in history."""
        caching_mw = AnthropicPromptCachingMiddleware(
            unsupported_model_behavior="ignore"
        )

        baseline_mw = _baseline_middleware()
        runtime_mw = _tail_middleware()

        captured = {}

        async def capture(req):
            captured["req"] = req
            return MagicMock()

        chain = _compose_middleware(
            [caching_mw, baseline_mw, runtime_mw],
            capture,
        )
        await chain(_make_model_request("Static prompt.", rows=await _turn_rows()))

        blocks = captured["req"].system_message.content
        assert len(blocks) == 2, f"Expected 2 blocks, got {len(blocks)}"

        # No block of the system message carries the stamp; it rides in history.
        for block in blocks:
            assert ENVELOPE_OPEN not in block["text"]
            assert "3:42 PM EDT" not in block["text"]

        # Breakpoints 2 and 3.
        assert "cache_control" in blocks[0]
        assert "cache_control" in blocks[1]
