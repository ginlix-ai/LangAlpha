"""Tests for ReasoningCompatibilityMiddleware.

Locks two settled contracts:

- **Gate A (origin)** — reasoning blocks never cross a provider lineage. The
  production failure is `messages.<i>.content.<j>.thinking.signature: Field
  required`, raised when a shim's thinking block is replayed to Anthropic after
  a mid-thread model switch or a fallback.
- **Gate B (structure)** — the Anthropic API also rejects blocks an interrupted
  stream leaves behind: signature-only orphans (repaired by injecting
  `thinking: ""`) and signature-less blocks (dropped).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain.agents.middleware.types import ExtendedModelResponse, ModelResponse
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.reasoning_compat import (
    ORIGIN_KEY,
    ReasoningCompatibilityMiddleware,
    _sanitize,
)

REAL_SIGNATURE = "CAISrAUKkwEIDxgCKkAw" * 40
SHIM_SIGNATURE = "60156c35-166d-41eb-9899-1a2b3c4d5e6f"

TEXT = {"type": "text", "text": "answer"}
TOOL_USE = {"type": "tool_use", "id": "tu_1", "name": "lookup", "input": {}}
TOOL_CALL = {"name": "lookup", "args": {}, "id": "tu_1", "type": "tool_call"}


def signed(signature: str = REAL_SIGNATURE) -> dict:
    return {"type": "thinking", "thinking": "reasoned", "signature": signature}


def ai(content, *, origin=None, model_name=None, tool_calls=None, **kwargs) -> AIMessage:
    metadata = {}
    if origin:
        metadata[ORIGIN_KEY] = origin
    if model_name:
        metadata["model_name"] = model_name
    return AIMessage(
        content=content,
        response_metadata=metadata,
        tool_calls=tool_calls or [],
        **kwargs,
    )


def types_of(msg: AIMessage) -> list[str]:
    return [b["type"] for b in msg.content if isinstance(b, dict)]


class _FakeRequest:
    """Duck-typed stand-in for langchain's ModelRequest."""

    def __init__(self, route, messages):
        self.model = MagicMock()
        self.model.metadata = {"provider_route": route} if route else {}
        self.messages = messages
        self.overridden_with = None

    def override(self, *, messages):
        self.overridden_with = messages
        return _FakeRequest(self.model.metadata.get("provider_route"), messages)


class TestOriginGate:
    def test_same_lineage_preserves_reasoning(self):
        msgs = [HumanMessage("q"), ai([signed(), TEXT], origin="anthropic")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs

    def test_oauth_variant_shares_the_anthropic_lineage(self):
        """claude-oauth and anthropic both reach api.anthropic.com."""
        msgs = [HumanMessage("q"), ai([signed(), TEXT], origin="claude-oauth")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs

    def test_foreign_lineage_reasoning_is_dropped(self):
        msgs = [HumanMessage("q"), ai([signed(SHIM_SIGNATURE), TEXT], origin="deepseek")]
        out, stats = _sanitize(msgs, "anthropic")
        assert types_of(out[1]) == ["text"]
        assert stats.msgs_stripped == 1

    def test_shim_keeps_its_own_unsigned_blocks(self):
        """A shim's own history is valid to replay to that same shim."""
        block = {"type": "thinking", "thinking": "reasoned"}
        msgs = [HumanMessage("q"), ai([block, TEXT], origin="deepseek")]
        out, _ = _sanitize(msgs, "deepseek")
        assert out is msgs

    def test_tool_loop_keeps_tool_use_and_pairing(self):
        msgs = [
            HumanMessage("q"),
            ai([signed(SHIM_SIGNATURE), TOOL_USE], origin="deepseek", tool_calls=[TOOL_CALL]),
            ToolMessage(content="42", tool_call_id="tu_1"),
        ]
        out, _ = _sanitize(msgs, "anthropic")
        assert types_of(out[1]) == ["tool_use"]
        assert len(out[1].tool_calls) == 1
        assert isinstance(out[2], ToolMessage)

    def test_reasoning_only_message_is_removed(self):
        """Stripping would leave empty content, which providers reject."""
        msgs = [HumanMessage("q"), ai([signed(SHIM_SIGNATURE)], origin="deepseek")]
        out, stats = _sanitize(msgs, "anthropic")
        assert len(out) == 1
        assert stats.msgs_removed == 1

    def test_additional_kwargs_reasoning_is_cleared(self):
        """ChatZai re-injects these into the payload, so content alone is not enough."""
        msg = ai([TEXT], origin="z-ai", additional_kwargs={"reasoning_content": "x", "keep": 1})
        out, _ = _sanitize([msg], "anthropic")
        assert out[0].additional_kwargs == {"keep": 1}

    def test_openai_reasoning_item_dropped_crossing_to_anthropic(self):
        item = {"type": "reasoning", "id": "rs_1", "encrypted_content": "opaque"}
        msgs = [ai([item, TEXT], origin="openai")]
        out, _ = _sanitize(msgs, "anthropic")
        assert types_of(out[0]) == ["text"]

    def test_unstamped_message_resolves_via_model_id(self):
        msgs = [ai([signed(), TEXT], model_name="claude-opus-5")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs

    def test_unattributable_message_fails_closed(self):
        msgs = [ai([signed(), TEXT], model_name="some-unknown-model")]
        out, _ = _sanitize(msgs, "anthropic")
        assert types_of(out[0]) == ["text"]

    def test_unknown_target_skips_the_origin_gate(self):
        """No provenance on the target is no basis to strip the whole history."""
        msgs = [ai([signed(SHIM_SIGNATURE), TEXT], origin="deepseek")]
        out, _ = _sanitize(msgs, "unknown")
        assert out is msgs


class TestStructureGate:
    def test_orphan_signature_block_is_repaired(self):
        """An interrupted stream checkpoints signature-only blocks."""
        orphan = {"type": "thinking", "signature": REAL_SIGNATURE, "index": 0}
        out, stats = _sanitize([ai([orphan, TEXT], origin="anthropic")], "anthropic")
        assert out[0].content[0]["thinking"] == ""
        assert out[0].content[0]["index"] == 0
        assert stats.blocks_repaired == 1

    def test_repair_runs_even_for_non_anthropic_targets(self):
        orphan = {"type": "thinking", "signature": "s"}
        out, stats = _sanitize([ai([orphan, TEXT], origin="deepseek")], "deepseek")
        assert out[0].content[0]["thinking"] == ""
        assert stats.blocks_repaired == 1

    def test_signatureless_block_dropped_for_anthropic(self):
        block = {"type": "thinking", "thinking": "reasoned"}
        out, stats = _sanitize([ai([block, TEXT], origin="anthropic")], "anthropic")
        assert types_of(out[0]) == ["text"]
        assert stats.blocks_dropped == 1

    def test_redacted_thinking_survives_without_a_signature(self):
        """It carries `data`, never a `signature` — dropping it would be a bug."""
        block = {"type": "redacted_thinking", "data": "opaque"}
        msgs = [ai([block, TEXT], origin="anthropic")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs

    def test_v1_reasoning_block_without_signature_dropped(self):
        block = {"type": "reasoning", "reasoning": "r", "extras": {}}
        out, _ = _sanitize([ai([block, TEXT], origin="anthropic")], "anthropic")
        assert types_of(out[0]) == ["text"]

    def test_v1_reasoning_block_with_signature_kept(self):
        block = {"type": "reasoning", "reasoning": "r", "extras": {"signature": REAL_SIGNATURE}}
        msgs = [ai([block, TEXT], origin="anthropic")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs

    def test_message_emptied_by_the_structure_gate_is_removed(self):
        msgs = [HumanMessage("q"), ai([{"type": "thinking", "thinking": "r"}], origin="anthropic")]
        out, stats = _sanitize(msgs, "anthropic")
        assert len(out) == 1
        assert stats.msgs_removed == 1

    @pytest.mark.parametrize(
        "block",
        [
            {"type": "text", "text": "hello"},
            TOOL_USE,
            {"type": "tool_result", "tool_use_id": "tu_1", "content": "ok"},
        ],
    )
    def test_non_reasoning_blocks_untouched(self, block):
        msgs = [ai([block], origin="anthropic")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs


class TestListIdentity:
    def test_empty_history(self):
        out, stats = _sanitize([], "anthropic")
        assert out == []
        assert not stats

    def test_clean_history_returns_same_list(self):
        msgs = [AIMessage(content="hi"), HumanMessage(content="hey")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out is msgs

    def test_untouched_messages_keep_identity(self):
        clean = ai([TEXT], origin="anthropic")
        msgs = [HumanMessage("q"), clean, ai([signed(SHIM_SIGNATURE), TEXT], origin="deepseek")]
        out, _ = _sanitize(msgs, "anthropic")
        assert out[0] is msgs[0]
        assert out[1] is clean


class TestRequestPath:
    """The middleware's own wiring: provider_route → lineage → sanitized request."""

    @pytest.mark.asyncio
    async def test_client_provider_route_drives_the_target_lineage(self):
        request = _FakeRequest(
            "claude-oauth", [ai([signed(SHIM_SIGNATURE), TEXT], origin="deepseek")]
        )
        handler = AsyncMock(return_value=AIMessage(content="out"))

        await ReasoningCompatibilityMiddleware().awrap_model_call(request, handler)

        # claude-oauth resolves to the anthropic lineage, so deepseek's block goes.
        assert types_of(request.overridden_with[0]) == ["text"]

    @pytest.mark.asyncio
    async def test_unstamped_client_skips_sanitization(self):
        """No provider_route means no target lineage, so there is nothing to compare."""
        request = _FakeRequest(None, [ai([signed(SHIM_SIGNATURE), TEXT], origin="deepseek")])
        handler = AsyncMock(return_value=AIMessage(content="out"))

        await ReasoningCompatibilityMiddleware().awrap_model_call(request, handler)

        assert request.overridden_with is None

    def test_clean_request_skips_override(self):
        request = _FakeRequest("anthropic", [ai([TEXT], origin="anthropic")])
        handler = MagicMock(return_value=AIMessage(content="out"))
        ReasoningCompatibilityMiddleware().wrap_model_call(request, handler)
        assert request.overridden_with is None


class TestOriginStamping:
    def _request(self, provider: str | None):
        return _FakeRequest(provider, [AIMessage(content="hi")])

    @pytest.mark.asyncio
    async def test_model_response_messages_are_stamped(self):
        response = ModelResponse(
            result=[AIMessage(content="out"), ToolMessage(content="t", tool_call_id="x")],
            structured_response={"kept": True},
        )
        handler = AsyncMock(return_value=response)

        result = await ReasoningCompatibilityMiddleware().awrap_model_call(
            self._request("claude-oauth"), handler
        )

        assert result.result[0].response_metadata[ORIGIN_KEY] == "claude-oauth"
        assert isinstance(result.result[1], ToolMessage)
        assert result.structured_response == {"kept": True}
        # Rebuilt, not mutated.
        assert response.result[0].response_metadata == {}

    @pytest.mark.asyncio
    async def test_extended_response_keeps_its_command(self):
        inner = ModelResponse(result=[AIMessage(content="out")])
        handler = AsyncMock(return_value=ExtendedModelResponse(model_response=inner, command=None))

        result = await ReasoningCompatibilityMiddleware().awrap_model_call(
            self._request("anthropic"), handler
        )

        assert isinstance(result, ExtendedModelResponse)
        assert result.model_response.result[0].response_metadata[ORIGIN_KEY] == "anthropic"

    @pytest.mark.asyncio
    async def test_unstamped_client_leaves_response_untouched(self):
        response = ModelResponse(result=[AIMessage(content="out")])
        handler = AsyncMock(return_value=response)

        result = await ReasoningCompatibilityMiddleware().awrap_model_call(
            self._request(None), handler
        )

        assert ORIGIN_KEY not in result.result[0].response_metadata

    def test_sync_path_stamps_a_bare_ai_message(self):
        handler = MagicMock(return_value=AIMessage(content="out"))
        result = ReasoningCompatibilityMiddleware().wrap_model_call(
            self._request("anthropic"), handler
        )
        assert result.response_metadata[ORIGIN_KEY] == "anthropic"

    def test_unrecognized_result_passes_through(self):
        handler = MagicMock(return_value="not a model response")
        result = ReasoningCompatibilityMiddleware().wrap_model_call(
            self._request("anthropic"), handler
        )
        assert result == "not a model response"
