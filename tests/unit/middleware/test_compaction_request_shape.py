"""
Tests for the message shape sent to the compaction LLM.

Regression coverage for the Codex OAuth 400 "Instructions are required" bug:
the Codex Responses-API adapter only populates the top-level ``instructions``
field from ``SystemMessage`` content in the input, so the summarization
prompt MUST be framed as a ``SystemMessage`` rather than a bare user string.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessageChunk, HumanMessage, SystemMessage

from ptc_agent.agent.middleware.compaction.summary_request import (
    _COMPACTION_USER_NUDGE,
    build_summary_request as _build_summary_request,
)
from ptc_agent.agent.middleware.compaction.utils import count_tokens_tiktoken
from ptc_agent.core.sandbox.livefs_mount import TranscriptReach
from src.llms import maybe_disable_streaming



def _summary_model(reply: str = "summary", *, error: BaseException | None = None) -> MagicMock:
    """A summary model that streams ``reply``, keeping each request in ``sent``."""
    model = MagicMock()
    model.sent = []

    async def astream(request, config=None):
        model.sent.append(request)
        if error is not None:
            raise error
        yield AIMessageChunk(content=reply)

    model.astream = astream
    return model


def _hanging(cancelled: list[bool]):
    """An ``astream`` that never sends a chunk, noting when it is cut off."""
    import asyncio

    async def astream(request, config=None):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        yield AIMessageChunk(content="too late")

    return astream

def _config(main):
    """A config whose summary model is resolved by name. ``main`` is the
    user's main model, the fallback, which is not tried when it is the
    summary model itself."""
    from ptc_agent.config import AgentConfig, LLMConfig
    from ptc_agent.config.core import (
        DaytonaConfig,
        FilesystemConfig,
        LoggingConfig,
        MCPConfig,
        SandboxConfig,
        SecurityConfig,
    )

    cfg = AgentConfig(
        llm=LLMConfig(name="main-model", compaction="gpt-4o"),
        security=SecurityConfig(),
        logging=LoggingConfig(),
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        mcp=MCPConfig(),
        filesystem=FilesystemConfig(),
    )
    cfg.llm_client = main
    # Only the newest message is kept, so a few short ones leave some to compact.
    cfg.compaction.keep_tokens = 0
    return cfg


@pytest.fixture(autouse=True)
def _offline_token_counter(monkeypatch):
    """cl100k_base fetches its BPE file on first use; the unit suite blocks
    sockets, so a cold cache (CI runners) turns counting into a network
    error. These tests assert request shape, not token math — stub the
    encoder loader at its definition site so every caller stays offline."""
    from ptc_agent.agent.middleware.compaction import utils as compaction_utils

    fake = SimpleNamespace(encode=lambda text: [0] * (len(text) // 4 + 1))
    monkeypatch.setattr(compaction_utils, "_get_tiktoken_encoder", lambda: fake)


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

    def test_runtime_rows_are_not_rendered_as_the_user(self):
        """A turn anchor is dropped; a change row is relabelled as System."""
        from ptc_agent.agent.middleware.runtime_context.durable import (
            DurableUpdate,
            build_update_message,
        )
        from ptc_agent.agent.middleware.runtime_context.turn import TURN_ROW_KIND

        anchor = build_update_message(
            DurableUpdate(kind=TURN_ROW_KIND, schema_version=1, text="9:18 PM EDT, Saturday")
        )
        change = build_update_message(
            DurableUpdate(kind="workspace_changed", schema_version=1, text="Name: New (the frozen block says Old)")
        )
        trimmed = [HumanMessage(content="what is AAPL?", id="h1"), anchor, change]

        result = _build_summary_request("irrelevant system prompt", trimmed)

        history = result[1].content
        assert "Human: what is AAPL?" in history
        assert "9:18 PM EDT" not in history
        assert "System: " in history
        assert "Name: New (the frozen block says Old)" in history
        assert history.count("Human: ") == 1

    def test_orchestrator_rows_are_not_rendered_as_the_user(self):
        """The steering trigger is dropped, so the summary never reports an
        instruction still to come; a task notice is relabelled as System."""
        from ptc_agent.agent.transcript.classify import ORCHESTRATOR_SOURCE, STEERING_TRIGGER

        def orchestrator(content: str) -> HumanMessage:
            return HumanMessage(
                content=content,
                name="orchestrator",
                additional_kwargs={"lc_source": ORCHESTRATOR_SOURCE},
            )

        trimmed = [
            HumanMessage(content="what is AAPL?", id="h1"),
            orchestrator(STEERING_TRIGGER),
            HumanMessage(content="[Steering from User]\nAlso chart it.", id="s1"),
            orchestrator("Background task 1 completed."),
        ]

        history = _build_summary_request("irrelevant system prompt", trimmed)[1].content

        assert STEERING_TRIGGER not in history
        assert "Also chart it." in history
        assert "System: Background task 1 completed." in history
        assert "Human: Background task" not in history

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


class TestTranscriptMarkers:
    """Each turn in the summarizer's input is headed by the transcript file
    that holds it, numbered as the renderer numbers them: over the whole
    checkpoint list, not the trimmed stretch the summarizer is sent."""

    @staticmethod
    def _raw(turns: int) -> list:
        from langchain_core.messages import AIMessage

        return [
            m
            for i in range(1, turns + 1)
            for m in (HumanMessage(f"q{i}", id=f"h{i}"), AIMessage(f"a{i}", id=f"a{i}"))
        ]

    def test_turns_are_numbered_over_the_full_checkpoint(self):
        from ptc_agent.agent.middleware.compaction.utils import build_summary_message
        from ptc_agent.agent.transcript import TranscriptTarget, Window
        from ptc_agent.agent.transcript.pointer import SummarySpan, TranscriptTurns

        raw = self._raw(5)
        target = TranscriptTarget("abcd1234-0000")
        prior = build_summary_message("turns 1-2", target, span=SummarySpan(1, 2))
        sent = [prior, *raw[4:8]]
        turns = TranscriptTurns.of(target, raw, window=Window())

        system, human = _build_summary_request("sys", sent, turns)

        index, history = human.content.split("<messages>")
        assert "- `turn-0001.jsonl`: q1" in index and "turn-0005.jsonl" not in index
        # The earlier summary under its own heading, its pointer note left out.
        assert "[earlier summary of turn-0001.jsonl to turn-0002.jsonl]\nturns 1-2" in history
        assert "Human:" not in history.split("[transcript:")[0]
        assert target.directory not in history
        assert history.index("turns 1-2") < history.index("[transcript: turn-0003.jsonl]")
        assert history.index("[transcript: turn-0003.jsonl]\nHuman: q3") > 0
        assert history.index("[transcript: turn-0004.jsonl]\nHuman: q4") > 0
        assert "[transcript: turn-0001.jsonl]" not in history
        assert "turn-0005.jsonl" not in history
        assert "(turn-0007.jsonl)" in system.content

    def test_a_subagent_cites_its_own_runs(self):
        from ptc_agent.agent.transcript import TranscriptTarget, Window
        from ptc_agent.agent.transcript.pointer import TranscriptTurns

        raw = self._raw(2)
        target = TranscriptTarget.for_agent("abcd1234-0000", "task:t1")
        turns = TranscriptTurns.of(target, raw, window=Window())
        system, human = _build_summary_request("sys", raw, turns)

        assert "[transcript: run-0002.jsonl]\nHuman: q2" in human.content
        assert "(run-0007.jsonl)" in system.content

    def test_no_transcript_no_markers_and_no_citation_ask(self):
        raw = self._raw(2)
        system, human = _build_summary_request("sys", raw)

        assert system.content == "sys"
        assert "[transcript:" not in human.content


@pytest.mark.asyncio
async def test_compact_messages_calls_llm_with_system_message(monkeypatch):
    """Manual /compact path (compact_messages) must frame the prompt the same
    way so Codex OAuth works end-to-end."""
    from ptc_agent.agent.middleware.compaction import compact as compact_module

    fake_llm = _summary_model()

    monkeypatch.setattr(compact_module, "create_llm", lambda model_name, **_: fake_llm)

    # Short-circuit offloading so the test doesn't need a sandbox.
    async def _passthrough_offload(backend, messages, **kwargs):
        return messages

    monkeypatch.setattr(
        compact_module, "aoffload_base64_content", _passthrough_offload
    )
    messages = [
        HumanMessage(content="hello", id="h1"),
        HumanMessage(content="world", id="h2"),
        HumanMessage(content="later", id="h3"),
    ]

    await compact_module.compact_messages(
        messages, {}, _config(fake_llm), thread_id="thread-1"
    )

    assert len(fake_llm.sent) == 1
    sent = fake_llm.sent[0]
    assert isinstance(sent, list)
    assert isinstance(sent[0], SystemMessage), (
        "Compaction must send the prompt as SystemMessage so Codex OAuth "
        "populates its ``instructions`` field."
    )
    assert isinstance(sent[-1], HumanMessage)


@pytest.mark.asyncio
async def test_chained_compaction_anchors_and_reconstructs_safely(monkeypatch):
    """Chained compact_messages must stamp an ``anchor_message_id`` on the new
    event, ground ``cutoff_index`` at that anchor in the raw list, and never
    reconstruct a summary immediately followed by an orphaned tool_result."""
    from langchain_core.messages import AIMessage, ToolMessage

    from ptc_agent.agent.middleware.compaction import compact as compact_module
    from ptc_agent.agent.middleware.compaction.utils import get_effective_messages

    fake_llm = _summary_model()
    monkeypatch.setattr(compact_module, "create_llm", lambda model_name, **_: fake_llm)

    async def _passthrough_offload(backend, messages, **kwargs):
        return messages

    monkeypatch.setattr(compact_module, "aoffload_base64_content", _passthrough_offload)
    def _turn(i):
        # Human -> AI -> Tool, so the safe cutoff often lands on a ToolMessage.
        return [
            HumanMessage(content=f"q{i}", id=f"h{i}"),
            AIMessage(content=f"a{i}", id=f"a{i}"),
            ToolMessage(content="r", id=f"t{i}", tool_call_id=f"tc{i}"),
        ]

    messages = [m for i in range(6) for m in _turn(i)]

    config = _config(fake_llm)
    # Keep the last four messages, the newest turn and the result before it.
    config.compaction.keep_tokens = sum(count_tokens_tiktoken([m]) for m in messages[-4:])
    first = await compact_module.compact_messages(
        messages, {}, config, thread_id="thread-1"
    )
    event1 = first.event
    assert event1.get("anchor_message_id") is not None
    eff1 = get_effective_messages(messages, event1)
    assert not isinstance(eff1[1], ToolMessage)

    # Chain: append a fresh turn and compact again from the state the first
    # one left.
    messages2 = messages + _turn(99)
    second = await compact_module.compact_messages(
        messages2, first.update({}), config, thread_id="thread-1"
    )
    event2 = second.event
    anchor = event2.get("anchor_message_id")
    assert anchor is not None
    # cutoff_index is grounded at the anchor message in the raw list.
    assert messages2[event2["cutoff_index"]].id == anchor
    eff2 = get_effective_messages(messages2, event2)
    assert not isinstance(eff2[1], ToolMessage)


@pytest.mark.asyncio
async def test_compact_messages_summarizes_the_offload_view(monkeypatch):
    """Manual /compact summarizes what the model is sent, as automatic
    compaction does: an argument the thread recorded as cut reaches the
    summary model cut, pointing at its transcript file."""
    from langchain_core.messages import AIMessage, ToolMessage

    from ptc_agent.agent.middleware.compaction import compact as compact_module

    fake_llm = _summary_model()
    monkeypatch.setattr(compact_module, "create_llm", lambda model_name, **_: fake_llm)
    sent: list = []

    async def _recording_offload(backend, messages, **kwargs):
        sent.extend(messages)
        return messages

    monkeypatch.setattr(compact_module, "aoffload_base64_content", _recording_offload)
    mount = SimpleNamespace(save_transcript=AsyncMock(return_value=True))
    backend = SimpleNamespace(
        livefs=mount,
        transcripts=TranscriptReach(mount, shell=True),
        settled_livefs=AsyncMock(return_value=mount),
    )
    messages = [
        HumanMessage(content="go", id="h1"),
        AIMessage(
            content="",
            id="a1",
            tool_calls=[{"name": "Write", "id": "w1", "args": {"content": "x" * 5000}}],
        ),
        ToolMessage(content="wrote", tool_call_id="w1", id="t1"),
        HumanMessage(content="later", id="h2"),
    ]

    await compact_module.compact_messages(
        messages,
        {"_offloaded_tool_call_ids": {"w1"}},
        _config(fake_llm),
        thread_id="abcd1234-0000",
        backend=backend,
    )

    write = next(m for m in sent if m.id == "a1").tool_calls[0]["args"]["content"]
    assert len(write) < 5000 and "turn-0001.jsonl" in write
    assert messages[1].tool_calls[0]["args"]["content"] == "x" * 5000


class TestCompactMessagesNeverStoresAnError:
    """A summary replaces the history the agent sees, so a failed model call
    must never become the summary text. Manual /compact ends in a summary
    the server builds when no model answers, and says so in its source."""

    def _patch(self, monkeypatch, compact_module, fake_llm):
        async def _passthrough(backend, messages, **kwargs):
            return messages

        monkeypatch.setattr(compact_module, "aoffload_base64_content", _passthrough)
        monkeypatch.setattr(compact_module, "create_llm", lambda model_name, **_: fake_llm)
        self.fake_llm = fake_llm

    async def _compact(self, compact_module):
        messages = [
            HumanMessage(content="a", id="1"),
            HumanMessage(content="b", id="2"),
            HumanMessage(content="c", id="3"),
        ]
        return await compact_module.compact_messages(
            messages, {}, _config(self.fake_llm), thread_id="thread-1"
        )

    def _assert_server_summary(self, result):
        assert result.summary.source == "server"
        text = result.summary.text
        assert text.startswith("The summary model could not be reached")
        assert "Error generating summary" not in text
        assert "boom" not in text
        stamp = result.event["summary_message"].additional_kwargs["summarize_complete"]
        assert stamp["source"] == "server"

    @pytest.mark.asyncio
    async def test_llm_failure_ends_in_a_server_summary(self, monkeypatch):
        from ptc_agent.agent.middleware.compaction import compact as compact_module

        fake_llm = _summary_model(error=RuntimeError("boom"))
        self._patch(monkeypatch, compact_module, fake_llm)

        self._assert_server_summary(await self._compact(compact_module))

    @pytest.mark.asyncio
    async def test_empty_summary_ends_in_a_server_summary(self, monkeypatch):
        from ptc_agent.agent.middleware.compaction import compact as compact_module

        fake_llm = _summary_model("")
        self._patch(monkeypatch, compact_module, fake_llm)

        self._assert_server_summary(await self._compact(compact_module))

    @pytest.mark.asyncio
    async def test_a_hung_call_ends_in_a_server_summary_within_the_budget(self, monkeypatch):

        from ptc_agent.agent.middleware.compaction import compact as compact_module

        cancelled: list[bool] = []
        fake_llm = MagicMock()
        fake_llm.astream = _hanging(cancelled)
        self._patch(monkeypatch, compact_module, fake_llm)
        monkeypatch.setattr(compact_module, "get_compaction_timeout", lambda: 0.01)

        self._assert_server_summary(await self._compact(compact_module))
        # Not the hang ending on its own: the budget cut it short.
        assert cancelled


    @pytest.mark.asyncio
    async def test_the_transcript_save_counts_against_the_budget(self, monkeypatch):
        # Admission holds the next turn for about the compaction timeout, so
        # a slow save leaves the summary less time, not the turn more wait.
        import asyncio

        from ptc_agent.agent.middleware.compaction import compact as compact_module
        from ptc_agent.agent.middleware.compaction.summarize import Summary

        async def _slow_save(*args, **kwargs):
            await asyncio.sleep(0.05)
            return None

        budgets: list[float] = []

        async def _write(**kwargs):
            budgets.append(kwargs["budget"])
            return Summary("done", "model", [])

        self._patch(monkeypatch, compact_module, MagicMock())
        monkeypatch.setattr(compact_module, "aexport_transcript", _slow_save)
        monkeypatch.setattr(compact_module, "awrite_summary", _write)
        monkeypatch.setattr(compact_module, "get_compaction_timeout", lambda: 10.0)

        await self._compact(compact_module)

        assert budgets and budgets[0] <= 10.0 - 0.05


class TestCompactWindowClose:
    """Every summarize start is closed: by complete, carrying the summary's
    source, or by error when a cancellation propagates. Otherwise a stream
    persists a naked start, which keeps the compaction window open."""

    def _make_middleware(self, monkeypatch, error=None):
        from ptc_agent.agent.middleware.compaction import compact as compact_mod
        from ptc_agent.agent.middleware.compaction.compact import Summarizer
        from ptc_agent.agent.middleware.compaction.middleware import (
            CompactionMiddleware,
        )

        fake_model = _summary_model(error=error)

        mw = CompactionMiddleware(
            Summarizer(
                fake_model,
                limit=100_000,
                counter=lambda msgs: sum(len(str(m.content)) for m in msgs),
            ),
            token_threshold=100_000,
            keep_tokens=5,
        )

        async def _passthrough(backend, messages, **kwargs):
            return messages

        monkeypatch.setattr(compact_mod, "aoffload_base64_content", _passthrough)
        signals: list[tuple[str, str, dict]] = []
        monkeypatch.setattr(
            mw, "_emit_context_signal", lambda a, s, **k: signals.append((a, s, k))
        )
        return mw, signals

    async def _summarize(self, mw):
        messages = [HumanMessage(content="hi", id="h")]
        request = SimpleNamespace(messages=messages, model=None, state={})
        return (await mw._compact(request, messages, 1, None)).summary

    @pytest.mark.asyncio
    async def test_cancelled_error_still_emits_error_signal(self, monkeypatch):
        import asyncio as _asyncio

        mw, signals = self._make_middleware(
            monkeypatch, error=_asyncio.CancelledError()
        )

        with pytest.raises(_asyncio.CancelledError):
            await self._summarize(mw)

        assert [(a, s) for a, s, _ in signals] == [
            ("summarize", "start"),
            ("summarize", "error"),
        ], "CancelledError must close the compaction window before re-raising."

    @pytest.mark.asyncio
    async def test_a_failed_call_completes_with_a_server_summary(self, monkeypatch):
        mw, signals = self._make_middleware(
            monkeypatch, error=RuntimeError("upstream down")
        )

        summary = await self._summarize(mw)

        assert summary.source == "server"
        assert "upstream down" not in summary.text
        assert [(a, s) for a, s, _ in signals] == [
            ("summarize", "start"),
            ("summarize", "complete"),
        ]
        assert signals[-1][2]["source"] == "server"

    @pytest.mark.asyncio
    async def test_timeout_completes_with_a_server_summary(self, monkeypatch):
        """A hung summarize self-terminates on the compaction budget, so the
        window closes and the admission guard is released."""

        from ptc_agent.agent.middleware.compaction import compact as compact_mod

        mw, signals = self._make_middleware(monkeypatch)
        cancelled: list[bool] = []
        mw._summarizer.model.astream = _hanging(cancelled)
        monkeypatch.setattr(compact_mod, "get_compaction_timeout", lambda: 0.01)

        summary = await self._summarize(mw)

        assert cancelled
        assert summary.source == "server"
        assert [(a, s) for a, s, _ in signals][-1] == ("summarize", "complete")


class TestSummaryFallback:
    """When the summary model fails, the agent's own model is tried if it is
    another one, as the summarizer's fallback when that is the same model at
    the summary level. If it is the summary model, the summarizer's fallback
    is tried, since the same model would fail again."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("agent", "expected"),
        [
            ("summary", "from the summarizer's fallback"),
            ("other", "from the agent's model"),
            ("turn", "from the summarizer's fallback"),
        ],
    )
    async def test_the_next_model_tried(self, monkeypatch, agent, expected):
        from ptc_agent.agent.middleware.compaction import compact as compact_mod
        from ptc_agent.agent.middleware.compaction.compact import Summarizer
        from ptc_agent.agent.middleware.compaction.middleware import (
            CompactionMiddleware,
        )

        async def _passthrough(backend, messages, **kwargs):
            return messages

        monkeypatch.setattr(compact_mod, "aoffload_base64_content", _passthrough)
        def labelled(model, name):
            model.model_name = name
            return model

        summary_model = labelled(_summary_model(error=RuntimeError("upstream down")), "picked")
        mw = CompactionMiddleware(
            Summarizer(
                summary_model,
                limit=100_000,
                counter=lambda msgs: sum(len(str(m.content)) for m in msgs),
                fallback=labelled(_summary_model("from the summarizer's fallback"), "turn-model"),
            ),
            token_threshold=100_000,
            keep_tokens=5,
        )
        monkeypatch.setattr(mw, "_emit_context_signal", lambda *a, **k: None)
        messages = [HumanMessage(content="hi", id="h")]
        agent_model = {
            "summary": summary_model,
            "other": labelled(_summary_model("from the agent's model"), "other-model"),
            # The same model at the turn's own level: the summary-level copy runs.
            "turn": labelled(_summary_model("from the agent's model"), "turn-model"),
        }[agent]
        request = SimpleNamespace(messages=messages, model=agent_model, state={})

        summary = (await mw._compact(request, messages, 1, None)).summary

        assert summary.source == "fallback"
        assert expected in summary.text


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


class TestServerSummaryAfterAnEarlierSummary:
    """The view after a compaction can open partway through a turn whose
    request the earlier summary holds. That turn's last reply belongs to it,
    not to the next request."""

    def test_a_turn_cut_by_the_earlier_summary_keeps_its_own_reply(self):
        from langchain_core.messages import AIMessage

        from ptc_agent.agent.middleware.compaction.summarize import server_summary
        from ptc_agent.agent.middleware.compaction.utils import build_summary_message
        from ptc_agent.agent.transcript import Window

        raw = [
            HumanMessage(content="first request", id="h1"),
            AIMessage(content="first reply", id="a1"),
            HumanMessage(content="second request", id="h2"),
            AIMessage(content="second reply", id="a2"),
            HumanMessage(content="third request", id="h3"),
            AIMessage(content="third reply", id="a3"),
        ]
        earlier = build_summary_message("First and second requests.", None)
        to_summarize = [earlier, raw[3], raw[4]]

        text = server_summary(
            to_summarize, [raw[5]], raw_messages=raw, turns=None, window=Window()
        ).text

        second, third = text.split("## Turn 2\n", 1)[1].split("## Turn 3\n", 1)
        assert second.strip() == "Last reply: second reply"
        assert third.strip() == "Request: third request\nLast reply: (none)"

    def test_a_report_taken_mid_turn_stays_in_the_turn(self):
        """A task's report rides the steering message ahead of any steer; the
        turn block keeps it as well as the sender's own words."""
        from langchain_core.messages import AIMessage

        from ptc_agent.agent.middleware.compaction.summarize import server_summary
        from ptc_agent.agent.transcript import Window
        from ptc_agent.agent.transcript.steer_text import STEERS_AT, steer_text

        report = "Task-1 found revenue up 12%."
        sent = steer_text([("user", ["Also chart it."])])
        mixed = report + "\n\n" + sent
        raw = [
            HumanMessage(content="analyse AAPL", id="h1"),
            HumanMessage(
                content=report,
                id="s1",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": {"messages": [{"content": "", "report_source": "task"}]},
                    STEERS_AT: len(report),
                },
            ),
            HumanMessage(
                content=mixed,
                id="s2",
                additional_kwargs={
                    "lc_source": "steering",
                    "steering_delivered": {
                        "messages": [
                            {"content": "", "report_source": "task"},
                            {"content": "Also chart it.", "sender": "user"},
                        ]
                    },
                    STEERS_AT: len(mixed) - len(sent),
                },
            ),
            AIMessage(content="done", id="a1"),
        ]

        text = server_summary(raw, [], raw_messages=raw, turns=None, window=Window()).text

        assert text.count(f"Then: {report}") == 2
        assert "Then: Also chart it." in text
        assert "sent a new message" not in text


class TestServerSummaryLead:
    """The digest's lead says why no model wrote it; beside a provider's own
    compaction it says the real summary is one only that model can read."""

    @staticmethod
    def _raw():
        from langchain_core.messages import AIMessage

        return [
            HumanMessage(content="first request", id="h1"),
            AIMessage(content="first reply", id="a1"),
            HumanMessage(content="second request", id="h2"),
            AIMessage(content="second reply", id="a2"),
        ]

    def test_the_given_lead_opens_the_digest(self):
        from ptc_agent.agent.middleware.compaction.summarize import PROVIDER_LEAD, server_summary
        from ptc_agent.agent.transcript import Window

        raw = self._raw()
        default = server_summary(raw, [], raw_messages=raw, turns=None, window=Window()).text
        text = server_summary(
            raw, [], raw_messages=raw, turns=None, window=Window(), lead=PROVIDER_LEAD
        ).text

        assert text.startswith(PROVIDER_LEAD + "\n\n")
        assert text.partition("\n\n")[2] == default.partition("\n\n")[2]
        assert not default.startswith(PROVIDER_LEAD)

    def test_an_opaque_earlier_summary_is_read_like_a_server_one(self):
        """Its text is a digest too, so its lead is not repeated."""
        from ptc_agent.agent.middleware.compaction.summarize import PROVIDER_LEAD, server_summary
        from ptc_agent.agent.middleware.compaction.utils import build_summary_message
        from ptc_agent.agent.transcript import Window

        raw = self._raw()
        earlier = build_summary_message(
            f"{PROVIDER_LEAD}\n\n## Turn 1\nRequest: first request\nLast reply: first reply",
            None,
            source="provider",
            native={"items": [{"type": "compaction", "encrypted_content": "opaque"}]},
            origin="openai",
        )

        text = server_summary(
            [earlier, raw[2], raw[3]],
            [],
            raw_messages=raw,
            turns=None,
            window=Window(),
            lead=PROVIDER_LEAD,
        ).text

        assert text.count(PROVIDER_LEAD) == 1
        assert "## Earlier summary\n## Turn 1\nRequest: first request" in text

    def test_a_readable_provider_summary_keeps_its_opening(self):
        """A Claude block's summary is the model's own, read from its start."""
        from ptc_agent.agent.middleware.compaction.summarize import server_summary
        from ptc_agent.agent.middleware.compaction.utils import build_summary_message
        from ptc_agent.agent.transcript import Window

        raw = self._raw()
        earlier = build_summary_message(
            "Goal: chart AAPL.\n\nProgress: data pulled.",
            None,
            source="provider",
            native={"block": {"type": "compaction", "content": "x", "signature": "s"}},
            origin="anthropic",
        )

        text = server_summary(
            [earlier, raw[2], raw[3]], [], raw_messages=raw, turns=None, window=Window()
        ).text

        assert "## Earlier summary\nGoal: chart AAPL.\n\nProgress: data pulled." in text

    def test_an_opaque_summary_its_model_wrote_out_keeps_its_opening(self):
        """Handed off at a model switch, its text is the model's summary, no
        longer a digest with a lead to drop."""
        from ptc_agent.agent.middleware.compaction.summarize import PROVIDER_LEAD, server_summary
        from ptc_agent.agent.middleware.compaction.utils import (
            build_summary_message,
            with_summary_text,
        )
        from ptc_agent.agent.transcript import Window

        raw = self._raw()
        opaque = build_summary_message(
            f"{PROVIDER_LEAD}\n\n## Turn 1\nRequest: first request",
            None,
            source="provider",
            native={"items": [{"type": "compaction", "encrypted_content": "opaque"}]},
            origin="openai",
        )
        earlier = with_summary_text(opaque, "Goal: chart AAPL.\n\nProgress: data pulled.")

        text = server_summary(
            [earlier, raw[2], raw[3]],
            [],
            raw_messages=raw,
            turns=None,
            window=Window(),
            lead=PROVIDER_LEAD,
        ).text

        assert "## Earlier summary\nGoal: chart AAPL.\n\nProgress: data pulled." in text


class TestTrimForSummary:
    """The summarizer gets the newest messages that fit, and never loses the
    request they serve: once the summary replaces the history it is the
    request's one record, while the steps between are in the transcript."""

    @staticmethod
    def _count(messages):
        return sum(len(str(m.content)) for m in messages)

    def _turn(self, steps: int):
        from langchain_core.messages import AIMessage, ToolMessage

        msgs = [HumanMessage(content="build the model", id="req")]
        for i in range(steps):
            msgs.append(AIMessage(content="", id=f"a{i}", tool_calls=[
                {"name": "Bash", "id": f"c{i}", "args": {"command": "x" * 40}}
            ]))
            msgs.append(ToolMessage(content="y" * 100, tool_call_id=f"c{i}", id=f"t{i}"))
        return msgs

    def test_one_long_turn_keeps_its_request(self):
        from ptc_agent.agent.middleware.compaction.summary_request import trim_for_summary

        msgs = self._turn(20)
        trimmed = trim_for_summary(msgs, 500, self._count)

        assert trimmed[0].id == "req"
        assert trimmed[-1].id == "t19"
        assert self._count(trimmed) <= 500

    def test_the_count_includes_tool_call_arguments(self):
        # The request renders every call's arguments; counted without them, a
        # code-heavy history would reach the summarizer whole past its limit.
        from langchain_core.messages import AIMessage

        from ptc_agent.agent.middleware.compaction.summary_request import trim_for_summary
        from ptc_agent.agent.middleware.compaction.utils import count_tokens_tiktoken

        msgs = [HumanMessage(content="build the model", id="req")]
        for i in range(10):
            msgs.append(AIMessage(content="", id=f"a{i}", tool_calls=[
                {"name": "Write", "id": f"c{i}", "args": {"content": "x" * 4_000}}
            ]))

        assert count_tokens_tiktoken(msgs) > 10_000
        assert len(trim_for_summary(msgs, 5_000, count_tokens_tiktoken)) < len(msgs)

    def test_reasoning_the_request_leaves_out_does_not_count(self):
        # The rendered history never carries reasoning; counted in, a thinking
        # model's turns were trimmed away while they still fit.
        from langchain_core.messages import AIMessage

        from ptc_agent.agent.middleware.compaction.summary_request import trim_for_summary
        from ptc_agent.agent.middleware.compaction.utils import count_tokens_tiktoken

        msgs = [HumanMessage(content="build the model", id="req")]
        for i in range(10):
            msgs.append(AIMessage(
                content=f"step {i}", id=f"a{i}",
                additional_kwargs={"reasoning_content": "weighing the options " * 300},
            ))

        assert count_tokens_tiktoken(msgs) > 10_000
        assert trim_for_summary(msgs, 5_000, count_tokens_tiktoken) == msgs

    def test_a_call_kept_as_a_content_block_too_counts_once(self):
        # Anthropic keeps each call as a tool_use block beside tool_calls.
        from langchain_core.messages import AIMessage

        from ptc_agent.agent.middleware.compaction.utils import count_tokens_tiktoken

        args = {"content": "x" * 4_000}
        call = {"name": "Write", "id": "c1", "args": args}
        block = {"type": "tool_use", "id": "c1", "name": "Write", "input": args}
        both = AIMessage(content=[block], tool_calls=[call])

        assert count_tokens_tiktoken([both]) == count_tokens_tiktoken(
            [AIMessage(content="", tool_calls=[call])]
        )
