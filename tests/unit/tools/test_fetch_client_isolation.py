"""Regression tests for web_fetch's extraction LLM handling.

Two bugs collapse into one fix at ``src/tools/fetch.py::_extract_with_llm``:

1. The shared ``subsidiary_llm_clients["fetch"]`` override was mutated in
   place when streaming was flipped to False — poisoning every subsequent
   caller that reused the instance.
2. Codex-OAuth models (``ChatCodexOpenAI``) require ``stream=true`` at the
   proxy layer; the old unconditional flip returned HTTP 400
   ``Stream must be set to true``.

These tests pin both behaviors.
"""

import pytest

from src.llms.extension.codex import ChatCodexOpenAI
from src.tools import fetch as fetch_module
from src.tools.fetch import _extract_with_llm, fetch_llm_client_override


class _FakeLLM:
    """Minimal fake with ``.model_copy`` semantics that mirror pydantic."""

    def __init__(self, streaming: bool = True) -> None:
        self.streaming = streaming

    def model_copy(self) -> "_FakeLLM":
        return _FakeLLM(streaming=self.streaming)


@pytest.mark.asyncio
async def test_fetch_clones_override_before_mutation(monkeypatch):
    """The shared override instance must not have its ``streaming`` flag
    flipped — only the per-call clone does."""
    override = _FakeLLM(streaming=True)
    token = fetch_llm_client_override.set(override)

    captured = {}

    async def fake_api_call(*, llm, system_prompt, user_prompt, disable_tracing):
        captured["llm"] = llm
        return "ok"

    monkeypatch.setattr(fetch_module, "make_api_call", fake_api_call)

    try:
        result = await _extract_with_llm(
            markdown="content",
            prompt="extract",
            model="unused",
        )
    finally:
        fetch_llm_client_override.reset(token)

    assert result == "ok"
    # Shared instance untouched — next caller keeps streaming=True.
    assert override.streaming is True
    # Per-call clone had streaming flipped off.
    assert captured["llm"] is not override
    assert captured["llm"].streaming is False


@pytest.mark.asyncio
async def test_fetch_keeps_streaming_for_codex_override(monkeypatch):
    """Codex models ship with ``streaming=True`` for a reason — the proxy
    rejects ``stream=false``. The clone must keep streaming on."""
    override = ChatCodexOpenAI(
        model="gpt-5.4",
        api_key="fake",
        output_version="responses/v1",
        store=False,
        streaming=True,  # matches what LLM._get_codex_llm constructs with
    )
    assert override.streaming is True
    token = fetch_llm_client_override.set(override)

    captured = {}

    async def fake_api_call(*, llm, system_prompt, user_prompt, disable_tracing):
        captured["llm"] = llm
        return "ok"

    monkeypatch.setattr(fetch_module, "make_api_call", fake_api_call)

    try:
        await _extract_with_llm(
            markdown="content",
            prompt="extract",
            model="unused",
        )
    finally:
        fetch_llm_client_override.reset(token)

    # Shared Codex instance untouched.
    assert override.streaming is True
    # Clone is a distinct object but still a Codex client with streaming on.
    assert captured["llm"] is not override
    assert isinstance(captured["llm"], ChatCodexOpenAI)
    assert captured["llm"].streaming is True
