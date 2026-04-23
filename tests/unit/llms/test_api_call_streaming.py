"""Tests for ``maybe_disable_streaming`` — the Codex-aware streaming toggle
shared by the chat compaction middleware and the web_fetch tool.
"""

from src.llms.api_call import maybe_disable_streaming
from src.llms.extension.codex import ChatCodexOpenAI


class _PlainLLM:
    """Minimal stand-in with a ``streaming`` attribute."""

    def __init__(self, streaming: bool = True) -> None:
        self.streaming = streaming


class _NoStreamingLLM:
    """Stand-in without a ``streaming`` attribute."""


def _make_codex() -> ChatCodexOpenAI:
    # The real factory (``LLM._get_codex_llm``) passes ``streaming=True``
    # explicitly; mirror that here so the test pins the factory-built
    # instance behavior, not the class default.
    return ChatCodexOpenAI(
        model="gpt-5.4",
        api_key="fake",
        output_version="responses/v1",
        store=False,
        streaming=True,
    )


def test_codex_streaming_preserved():
    llm = _make_codex()
    assert llm.streaming is True

    maybe_disable_streaming(llm)

    # Codex proxy rejects stream=false with HTTP 400 — must stay True.
    assert llm.streaming is True


def test_plain_llm_streaming_disabled():
    llm = _PlainLLM(streaming=True)

    maybe_disable_streaming(llm)

    assert llm.streaming is False


def test_object_without_streaming_attr_is_noop():
    llm = _NoStreamingLLM()

    # Must not raise AttributeError.
    maybe_disable_streaming(llm)

    assert not hasattr(llm, "streaming")
