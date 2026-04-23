"""
Tests for the message shape sent to the compaction LLM.

Regression coverage for the Codex OAuth 400 "Instructions are required" bug:
the Codex Responses-API adapter only populates the top-level ``instructions``
field from ``SystemMessage`` content in the input, so the summarization
prompt MUST be framed as a ``SystemMessage`` rather than a bare user string.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import HumanMessage, SystemMessage

from ptc_agent.agent.middleware.compaction.middleware import (
    _COMPACTION_USER_NUDGE,
    _build_summary_request,
)
from src.llms import maybe_disable_streaming


class TestBuildSummaryRequest:
    def test_system_message_contains_instructions_only(self):
        """Instructions go in the system channel untouched — no placeholder
        substitution, no message history leaking in."""
        prompt = "You are a summarizer. Follow these rules."
        trimmed = [HumanMessage(content="hello", id="h1")]

        result = _build_summary_request(prompt, trimmed)

        assert len(result) == 2
        assert isinstance(result[0], SystemMessage)
        assert result[0].content == prompt
        # History is NOT in the system message.
        assert "hello" not in result[0].content

    def test_human_message_starts_with_nudge_and_contains_history(self):
        """History goes in the human message, prefixed by the nudge."""
        trimmed = [
            HumanMessage(content="what is AAPL?", id="h1"),
            # Second message ensures we see multiple turns rendered.
        ]

        result = _build_summary_request("irrelevant system prompt", trimmed)

        assert isinstance(result[1], HumanMessage)
        assert result[1].content.startswith(_COMPACTION_USER_NUDGE)
        assert "Human: what is AAPL?" in result[1].content
        # Wrapped for the model so the boundary is explicit.
        assert "<messages>" in result[1].content
        assert "</messages>" in result[1].content

    def test_empty_history_still_produces_non_empty_human_message(self):
        """Codex proxy rejects calls with empty input arrays. Even with zero
        messages, the nudge alone keeps the human turn non-empty."""
        result = _build_summary_request("some instructions", [])

        assert isinstance(result[1], HumanMessage)
        assert result[1].content.strip() != ""
        assert _COMPACTION_USER_NUDGE in result[1].content

    def test_messages_rendered_compactly_not_as_repr(self):
        """Regression: before the get_buffer_string swap, history was rendered
        via Python's str(list) which embedded class reprs, message IDs, and
        additional_kwargs — roughly 2x token inflation vs the trim budget."""
        from langchain_core.messages import AIMessage

        trimmed = [
            HumanMessage(
                content="hello",
                id="h-with-a-very-long-uuid-that-would-show-up-in-repr",
                additional_kwargs={"large_metadata_field": "x" * 500},
            ),
            AIMessage(content="hi", id="ai-id"),
        ]

        result = _build_summary_request("sys", trimmed)
        rendered = result[1].content

        assert "Human: hello" in rendered
        assert "AI: hi" in rendered
        assert "HumanMessage(" not in rendered
        assert "additional_kwargs" not in rendered
        assert "x" * 500 not in rendered


@pytest.mark.asyncio
async def test_compact_messages_calls_llm_with_system_message(monkeypatch):
    """Manual /compact path (compact_messages) must frame the prompt the same
    way so Codex OAuth works end-to-end."""
    from ptc_agent.agent.middleware.compaction import compact as compact_module

    fake_llm = MagicMock()
    fake_llm.ainvoke = AsyncMock(
        return_value=MagicMock(content="summary", additional_kwargs={})
    )

    monkeypatch.setattr(
        compact_module, "get_llm_by_type", lambda model_name: fake_llm
    )

    # Short-circuit offloading so the test doesn't need a sandbox.
    async def _passthrough_offload(backend, messages):
        return messages

    monkeypatch.setattr(
        compact_module, "aoffload_base64_content", _passthrough_offload
    )

    async def _noop_offload_to_backend(backend, messages):
        return None

    monkeypatch.setattr(
        compact_module, "aoffload_to_backend", _noop_offload_to_backend
    )

    async def _noop_offload_args(*args, **kwargs):
        return None

    monkeypatch.setattr(
        compact_module, "aoffload_truncated_args", _noop_offload_args
    )

    messages = [
        HumanMessage(content="hello", id="h1"),
        HumanMessage(content="world", id="h2"),
        HumanMessage(content="later", id="h3"),
    ]

    await compact_module.compact_messages(
        messages=messages,
        keep_messages=1,
        model_name="gpt-4o",
        backend=None,
    )

    assert fake_llm.ainvoke.await_count == 1
    sent = fake_llm.ainvoke.await_args.args[0]
    assert isinstance(sent, list)
    assert isinstance(sent[0], SystemMessage), (
        "Compaction must send the prompt as SystemMessage so Codex OAuth "
        "populates its ``instructions`` field."
    )
    assert isinstance(sent[-1], HumanMessage)


class TestCompactMessagesErrorPath:
    """Manual /compact must fail loudly, not fabricate fake summary text. A
    silent fallback here corrupted thread state with a bogus "compacted"
    cutoff while telling the client HTTP 200, making partial outages
    invisible."""

    def _patch_offload_stubs(self, monkeypatch, compact_module):
        async def _passthrough(backend, messages):
            return messages

        async def _noop(*args, **kwargs):
            return None

        monkeypatch.setattr(compact_module, "aoffload_base64_content", _passthrough)
        monkeypatch.setattr(compact_module, "aoffload_to_backend", _noop)
        monkeypatch.setattr(compact_module, "aoffload_truncated_args", _noop)

    @pytest.mark.asyncio
    async def test_raises_on_llm_failure(self, monkeypatch):
        from ptc_agent.agent.middleware.compaction import compact as compact_module

        fake_llm = MagicMock()
        fake_llm.ainvoke = AsyncMock(side_effect=RuntimeError("boom"))

        monkeypatch.setattr(
            compact_module, "get_llm_by_type", lambda model_name: fake_llm
        )
        self._patch_offload_stubs(monkeypatch, compact_module)

        messages = [
            HumanMessage(content="a", id="1"),
            HumanMessage(content="b", id="2"),
            HumanMessage(content="c", id="3"),
        ]

        with pytest.raises(RuntimeError, match="boom"):
            await compact_module.compact_messages(
                messages=messages,
                keep_messages=1,
                model_name="gpt-4o",
                backend=None,
            )

    @pytest.mark.asyncio
    async def test_raises_on_empty_summary(self, monkeypatch):
        from ptc_agent.agent.middleware.compaction import compact as compact_module

        fake_llm = MagicMock()
        fake_llm.ainvoke = AsyncMock(
            return_value=MagicMock(content="", additional_kwargs={})
        )
        monkeypatch.setattr(
            compact_module, "get_llm_by_type", lambda model_name: fake_llm
        )
        self._patch_offload_stubs(monkeypatch, compact_module)

        messages = [
            HumanMessage(content="a", id="1"),
            HumanMessage(content="b", id="2"),
            HumanMessage(content="c", id="3"),
        ]

        with pytest.raises(RuntimeError, match="empty summary"):
            await compact_module.compact_messages(
                messages=messages,
                keep_messages=1,
                model_name="gpt-4o",
                backend=None,
            )


class TestAcreateSummaryWindowClose:
    """If CancelledError propagated past _acreate_summary without emitting a
    terminal signal, a cancelled stream would persist a naked "summarize
    start" event. On replay and for the in-flight stream handler, that keeps
    the compaction window open indefinitely."""

    def _make_middleware(self, ainvoke_side_effect=None):
        from ptc_agent.agent.middleware.compaction.middleware import (
            CompactionMiddleware,
        )

        # Build a stand-in BaseChatModel-ish object: the middleware only
        # calls ``self.model.ainvoke(...)``, so a plain MagicMock is enough.
        fake_model = MagicMock()
        fake_model.ainvoke = AsyncMock(side_effect=ainvoke_side_effect)

        mw = CompactionMiddleware.__new__(CompactionMiddleware)
        mw.model = fake_model
        mw.summary_prompt = "sys"
        mw.token_counter = lambda msgs: sum(len(str(m.content)) for m in msgs)
        mw.trim_tokens_to_summarize = None
        mw._backend = None
        return mw

    @pytest.mark.asyncio
    async def test_cancelled_error_still_emits_error_signal(self, monkeypatch):
        import asyncio as _asyncio

        mw = self._make_middleware(
            ainvoke_side_effect=_asyncio.CancelledError()
        )

        calls: list[tuple[str, str]] = []

        def _record(action, signal, **kwargs):
            calls.append((action, signal))

        monkeypatch.setattr(mw, "_emit_context_signal", _record)

        # aoffload_base64_content is awaited inside _acreate_summary — stub it
        from ptc_agent.agent.middleware.compaction import middleware as mw_mod

        async def _passthrough(backend, messages):
            return messages

        monkeypatch.setattr(mw_mod, "aoffload_base64_content", _passthrough)

        with pytest.raises(_asyncio.CancelledError):
            await mw._acreate_summary(
                [HumanMessage(content="hi", id="h")], original_count=1
            )

        assert ("summarize", "start") in calls
        assert ("summarize", "error") in calls, (
            "CancelledError must still close the compaction window via an "
            "error signal before re-raising."
        )

    @pytest.mark.asyncio
    async def test_normal_exception_emits_error_and_returns_fallback(
        self, monkeypatch
    ):
        mw = self._make_middleware(
            ainvoke_side_effect=RuntimeError("upstream down")
        )

        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            mw,
            "_emit_context_signal",
            lambda a, s, **k: calls.append((a, s)),
        )

        from ptc_agent.agent.middleware.compaction import middleware as mw_mod

        async def _passthrough(backend, messages):
            return messages

        monkeypatch.setattr(mw_mod, "aoffload_base64_content", _passthrough)

        result = await mw._acreate_summary(
            [HumanMessage(content="hi", id="h")], original_count=1
        )

        assert "upstream down" in result
        assert ("summarize", "start") in calls
        assert ("summarize", "error") in calls


class TestMaybeDisableStreaming:
    """Codex OAuth proxy rejects stream=false with '400 Stream must be set to true'.
    Non-Codex clients should still get streaming=False to suppress token leaks."""

    def test_disables_streaming_on_generic_client(self):
        class _FakeClient:
            streaming = True

        client = _FakeClient()
        maybe_disable_streaming(client)
        assert client.streaming is False

    def test_preserves_streaming_on_codex_client(self):
        from src.llms.extension.codex import ChatCodexOpenAI

        # ChatCodexOpenAI requires creds to construct; patch the isinstance
        # target to a lightweight stand-in so we don't hit auth.
        client = MagicMock(spec=ChatCodexOpenAI)
        client.streaming = True

        maybe_disable_streaming(client)

        # MagicMock.streaming would be settable — confirm it wasn't touched.
        assert client.streaming is True

    def test_no_streaming_attribute_is_a_noop(self):
        class _NoStreamingAttr:
            pass

        obj = _NoStreamingAttr()
        maybe_disable_streaming(obj)
        assert not hasattr(obj, "streaming")
