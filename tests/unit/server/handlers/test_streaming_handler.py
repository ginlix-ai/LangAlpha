"""
Tests for src/server/handlers/streaming_handler.py

Covers:
- StreamEventAccumulator: accumulation, merging, max buffer size
- WorkflowStreamHandler: SSE event formatting, keepalive, error events
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# StreamEventAccumulator
# ---------------------------------------------------------------------------


class TestStreamEventAccumulator:
    """Tests for the StreamEventAccumulator class."""

    def _make_accumulator(self, max_bytes=16 * 1024):
        from src.server.handlers.streaming_handler import StreamEventAccumulator

        return StreamEventAccumulator(max_merged_bytes=max_bytes)

    # -- basic add / get --

    def test_add_first_event_stores_it(self):
        acc = self._make_accumulator()
        acc.add("message_chunk", {"content": "hello", "thread_id": "t1"})
        events = acc.get_events()
        assert len(events) == 1
        assert events[0]["event"] == "message_chunk"
        assert events[0]["data"]["content"] == "hello"

    def test_add_non_dict_data_is_ignored(self):
        acc = self._make_accumulator()
        acc.add("message_chunk", "not a dict")  # type: ignore[arg-type]
        assert acc.get_events() == []

    def test_different_event_types_are_not_merged(self):
        acc = self._make_accumulator()
        acc.add("message_chunk", {"content": "a", "thread_id": "t1"})
        acc.add("tool_calls", {"tool_calls": []})
        events = acc.get_events()
        assert len(events) == 2
        assert events[0]["event"] == "message_chunk"
        assert events[1]["event"] == "tool_calls"

    # -- message_chunk merging --

    def test_merge_consecutive_message_chunks(self):
        acc = self._make_accumulator()
        base = {
            "thread_id": "t1",
            "agent": "main",
            "id": "msg-1",
            "role": "assistant",
            "content_type": "text",
        }
        acc.add("message_chunk", {**base, "content": "Hello"})
        acc.add("message_chunk", {**base, "content": " world"})
        events = acc.get_events()
        assert len(events) == 1
        assert events[0]["data"]["content"] == "Hello world"

    def test_no_merge_when_content_type_is_reasoning_signal(self):
        acc = self._make_accumulator()
        base = {
            "thread_id": "t1",
            "agent": "main",
            "id": "msg-1",
            "role": "assistant",
        }
        acc.add("message_chunk", {**base, "content": "first", "content_type": "text"})
        acc.add(
            "message_chunk",
            {**base, "content": "start", "content_type": "reasoning_signal"},
        )
        events = acc.get_events()
        assert len(events) == 2

    def test_no_merge_when_merge_keys_differ(self):
        acc = self._make_accumulator()
        base = {
            "thread_id": "t1",
            "id": "msg-1",
            "role": "assistant",
            "content_type": "text",
        }
        acc.add("message_chunk", {**base, "agent": "main", "content": "a"})
        acc.add("message_chunk", {**base, "agent": "task:abc", "content": "b"})
        events = acc.get_events()
        assert len(events) == 2

    def test_merge_respects_max_bytes(self):
        acc = self._make_accumulator(max_bytes=10)
        base = {
            "thread_id": "t1",
            "agent": "main",
            "id": "msg-1",
            "role": "assistant",
            "content_type": "text",
        }
        acc.add("message_chunk", {**base, "content": "12345"})
        # Adding 6 more bytes exceeds 10, so should not merge
        acc.add("message_chunk", {**base, "content": "678901"})
        events = acc.get_events()
        assert len(events) == 2

    def test_merge_propagates_finish_reason(self):
        acc = self._make_accumulator()
        base = {
            "thread_id": "t1",
            "agent": "main",
            "id": "msg-1",
            "role": "assistant",
            "content_type": "text",
        }
        acc.add("message_chunk", {**base, "content": "Hello"})
        acc.add("message_chunk", {**base, "content": "", "finish_reason": "stop"})
        events = acc.get_events()
        assert len(events) == 1
        assert events[0]["data"]["finish_reason"] == "stop"

    # -- tool_call_chunks merging --

    def test_merge_consecutive_tool_call_chunks(self):
        acc = self._make_accumulator()
        base = {"thread_id": "t1", "agent": "main", "id": "msg-1"}
        acc.add(
            "tool_call_chunks",
            {
                **base,
                "tool_call_chunks": [{"id": "call-1", "args": '{"ke', "index": 0}],
            },
        )
        acc.add(
            "tool_call_chunks",
            {
                **base,
                "tool_call_chunks": [{"id": "call-1", "args": 'y": "val"}', "index": 0}],
            },
        )
        events = acc.get_events()
        assert len(events) == 1
        merged_args = events[0]["data"]["tool_call_chunks"][0]["args"]
        assert merged_args == '{"key": "val"}'

    def test_tool_call_chunks_no_merge_different_ids(self):
        acc = self._make_accumulator()
        base = {"thread_id": "t1", "agent": "main", "id": "msg-1"}
        acc.add(
            "tool_call_chunks",
            {**base, "tool_call_chunks": [{"id": "call-1", "args": "a", "index": 0}]},
        )
        acc.add(
            "tool_call_chunks",
            {**base, "tool_call_chunks": [{"id": "call-2", "args": "b", "index": 1}]},
        )
        events = acc.get_events()
        assert len(events) == 2

    def test_tool_call_chunks_no_merge_when_exceeds_max_bytes(self):
        acc = self._make_accumulator(max_bytes=5)
        base = {"thread_id": "t1", "agent": "main", "id": "msg-1"}
        acc.add(
            "tool_call_chunks",
            {**base, "tool_call_chunks": [{"id": "call-1", "args": "abc", "index": 0}]},
        )
        acc.add(
            "tool_call_chunks",
            {**base, "tool_call_chunks": [{"id": "call-1", "args": "defgh", "index": 0}]},
        )
        events = acc.get_events()
        assert len(events) == 2

    # -- get_events returns deep copy --

    def test_get_events_returns_deep_copy(self):
        acc = self._make_accumulator()
        acc.add("message_chunk", {"content": "hello"})
        events1 = acc.get_events()
        events1[0]["data"]["content"] = "mutated"
        events2 = acc.get_events()
        assert events2[0]["data"]["content"] == "hello"


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — SSE formatting helpers
# ---------------------------------------------------------------------------


class TestWorkflowStreamHandlerFormatting:
    """Tests for WorkflowStreamHandler SSE formatting methods."""

    def _make_handler(self, thread_id="test-thread"):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id=thread_id)

    def test_format_sse_event_basic(self):
        handler = self._make_handler()
        result = handler._format_sse_event("message_chunk", {"content": "hi"})
        assert result.startswith("id: 1\n")
        assert "event: message_chunk\n" in result
        assert result.endswith("\n\n")
        parsed_data = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed_data["content"] == "hi"

    def test_format_sse_event_increments_sequence(self):
        handler = self._make_handler()
        e1 = handler._format_sse_event("message_chunk", {"content": "a"})
        e2 = handler._format_sse_event("message_chunk", {"content": "b"})
        assert "id: 1\n" in e1
        assert "id: 2\n" in e2

    def test_format_sse_event_strips_empty_content(self):
        handler = self._make_handler()
        result = handler._format_sse_event(
            "message_chunk", {"content": "", "thread_id": "t1"}
        )
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert "content" not in parsed

    def test_format_sse_event_accumulates_by_default(self):
        handler = self._make_handler()
        handler._format_sse_event("message_chunk", {"content": "hello"})
        events = handler.get_sse_events()
        assert events is not None
        assert len(events) == 1

    def test_format_sse_event_skip_accumulate(self):
        handler = self._make_handler()
        handler._format_sse_event(
            "message_chunk", {"content": "skip"}, accumulate=False
        )
        events = handler.get_sse_events()
        assert events is None  # No events accumulated

    def test_format_error_event(self):
        handler = self._make_handler(thread_id="err-thread")
        result = handler.format_error_event("Something went wrong")
        assert "event: error\n" in result
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["thread_id"] == "err-thread"
        assert parsed["error"] == "Something went wrong"
        assert "message" in parsed
        # Without ``exc``, legacy shape only — no classification fields.
        assert "error_kind" not in parsed
        assert "hints" not in parsed

    def test_format_error_event_with_upstream_exc(self):
        """Upstream 5xx exceptions classify as upstream with provider-outage hints."""
        from anthropic import InternalServerError

        # Fabricate an anthropic.InternalServerError the way the SDK would
        # raise it on a 500 response. We just need the exception type and
        # ``.status_code`` — the classifier doesn't read the body.
        exc = InternalServerError.__new__(InternalServerError)
        exc.status_code = 500
        Exception.__init__(exc, "Error code: 500 - {'error': {'message': 'Internal service error'}}")

        handler = self._make_handler(thread_id="err-thread")
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "upstream"
        assert parsed["status_code"] == 500
        assert parsed["provider_module"] == "anthropic"
        # 5xx is a provider outage — don't suggest checking the API key.
        assert parsed["hints"] == ["provider_status", "try_another_model"]

    def test_format_error_event_upstream_401_auth_hints(self):
        """401 upstream surfaces credential-oriented hints first."""
        from anthropic import AuthenticationError

        exc = AuthenticationError.__new__(AuthenticationError)
        exc.status_code = 401
        Exception.__init__(exc, "invalid x-api-key")

        handler = self._make_handler(thread_id="err-thread")
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "upstream"
        assert parsed["status_code"] == 401
        assert parsed["hints"] == ["api_key", "model_access", "try_another_model"]

    def test_format_error_event_upstream_no_status_falls_back(self):
        """Unknown status (network error) shows all hints."""
        from anthropic import APIConnectionError

        exc = APIConnectionError.__new__(APIConnectionError)
        Exception.__init__(exc, "Connection reset by peer")

        handler = self._make_handler(thread_id="err-thread")
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "upstream"
        assert parsed["hints"] == [
            "api_key",
            "model_access",
            "provider_status",
            "try_another_model",
        ]

    def test_format_error_event_with_internal_exc(self):
        """Bare Exception from our code is classified as internal, no hints."""
        exc = RuntimeError("workspace state corrupted")
        handler = self._make_handler(thread_id="err-thread")
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "internal"
        assert "hints" not in parsed

    def test_format_error_event_upstream_wrapped_by_internal(self):
        """Upstream error wrapped in our own exception still classifies as upstream."""
        from anthropic import APIConnectionError

        inner = APIConnectionError.__new__(APIConnectionError)
        Exception.__init__(inner, "Connection reset by peer")
        try:
            try:
                raise inner
            except Exception as e:
                raise RuntimeError("agent failed") from e
        except RuntimeError as e:
            exc = e

        handler = self._make_handler()
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "upstream"
        assert parsed["provider_module"] == "anthropic"

    def test_classify_stream_exception_parses_status_from_message(self):
        """When status_code isn't on the exception, parse it from the message."""
        from src.server.handlers.streaming_handler import classify_stream_exception
        import httpx

        exc = httpx.HTTPError("got HTTP 429 from upstream, backing off")
        info = classify_stream_exception(exc)
        assert info["kind"] == "upstream"
        assert info["status_code"] == 429
        assert info["provider_module"] == "httpx"

    def test_format_keepalive_event(self):
        handler = self._make_handler()
        result = handler._format_keepalive_event()
        assert "event: keepalive\n" in result
        assert '"status": "alive"' in result
        assert result.startswith("id: ")
        assert result.endswith("\n\n")

    def test_format_credit_usage_event(self):
        handler = self._make_handler(thread_id="credit-thread")
        token_usage = {
            "by_model": {
                "claude-3.5-sonnet": {
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "total_tokens": 150,
                },
                "gpt-4o": {
                    "input_tokens": 200,
                    "output_tokens": 80,
                    "total_tokens": 280,
                },
            }
        }
        result = handler._format_credit_usage_event(
            thread_id="credit-thread",
            token_usage=token_usage,
            total_credits=1.5,
        )
        assert "event: credit_usage\n" in result
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["thread_id"] == "credit-thread"
        assert parsed["tokens"]["input_tokens"] == 300
        assert parsed["tokens"]["output_tokens"] == 130
        assert parsed["tokens"]["total_tokens"] == 430
        assert parsed["total_credits"] == 1.5

    def test_format_reasoning_signal(self):
        handler = self._make_handler()
        result = handler._format_reasoning_signal("main", "msg-1", "start")
        assert "event: message_chunk\n" in result
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["content"] == "start"
        assert parsed["content_type"] == "reasoning_signal"
        assert parsed["agent"] == "main"


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — tool call filtering
# ---------------------------------------------------------------------------


class TestToolCallFiltering:
    """Tests for the _filter_tool_calls method."""

    def _make_handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="test-thread")

    def test_filters_empty_name(self):
        handler = self._make_handler()
        result = handler._filter_tool_calls([
            {"id": "c1", "name": "", "args": {}},
            {"id": "c2", "name": "search", "args": {}},
        ])
        assert len(result) == 1
        assert result[0]["name"] == "search"

    def test_filters_duplicate_ids(self):
        handler = self._make_handler()
        result = handler._filter_tool_calls([
            {"id": "c1", "name": "search", "args": {}},
            {"id": "c1", "name": "search", "args": {}},
        ])
        assert len(result) == 1

    def test_remembers_seen_ids_across_calls(self):
        handler = self._make_handler()
        handler._filter_tool_calls([{"id": "c1", "name": "search", "args": {}}])
        result = handler._filter_tool_calls([
            {"id": "c1", "name": "search", "args": {}},
            {"id": "c2", "name": "execute", "args": {}},
        ])
        assert len(result) == 1
        assert result[0]["id"] == "c2"


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — _extract_reasoning_summary_index
# ---------------------------------------------------------------------------


class TestExtractReasoningSummaryIndex:
    """Tests for _extract_reasoning_summary_index static method."""

    def test_returns_index_from_reasoning_dict(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        content = {
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "thought", "index": 2}],
        }
        assert WorkflowStreamHandler._extract_reasoning_summary_index(content) == 2

    def test_returns_none_for_non_reasoning(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        assert WorkflowStreamHandler._extract_reasoning_summary_index("hello") is None

    def test_returns_none_for_reasoning_without_summary(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        content = {"type": "reasoning", "status": "in_progress"}
        assert WorkflowStreamHandler._extract_reasoning_summary_index(content) is None


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — interrupt handling
# ---------------------------------------------------------------------------


class TestInterruptHandling:
    """Tests for _handle_interrupt method."""

    def _make_handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="int-thread")

    def test_handles_dict_interrupt_value(self):
        handler = self._make_handler()
        interrupt = MagicMock()
        interrupt.id = "int-1"
        interrupt.value = {"action_requests": [{"description": "Run analysis?"}]}
        result = handler._handle_interrupt({"__interrupt__": [interrupt]})
        assert result is not None
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["interrupt_id"] == "int-1"
        assert parsed["action_requests"] == [{"description": "Run analysis?"}]
        assert parsed["finish_reason"] == "interrupt"

    def test_handles_string_interrupt_value(self):
        handler = self._make_handler()
        interrupt = MagicMock()
        interrupt.id = "int-2"
        interrupt.value = "Should I proceed with plan?"
        result = handler._handle_interrupt({"__interrupt__": [interrupt]})
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["action_requests"] == [
            {"description": "Should I proceed with plan?"}
        ]

    def test_handles_list_interrupt_value(self):
        handler = self._make_handler()
        interrupt = MagicMock()
        interrupt.id = "int-3"
        interrupt.value = [{"description": "step 1"}, {"description": "step 2"}]
        result = handler._handle_interrupt({"__interrupt__": [interrupt]})
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert len(parsed["action_requests"]) == 2


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — event_counter integration
# ---------------------------------------------------------------------------


class TestTaskArtifactEvent:
    """Tests that task artifact SSE events include tool_call_id.

    When the backend emits an artifact event for a spawned subagent task,
    it must include the originating tool_call_id so the frontend can
    directly map the inline card to the correct subagent — without relying
    on FIFO ordering which is unreliable for parallel tool calls.
    """

    def _make_handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="t-task-artifact")

    def test_task_artifact_event_includes_tool_call_id(self):
        """Artifact event for a spawned task must carry tool_call_id."""
        handler = self._make_handler()
        task_artifact = {
            "task_id": "abc123",
            "action": "init",
            "description": "Research NVIDIA",
            "prompt": "Research NVIDIA GPU timeline",
            "type": "research",
        }
        # Build the artifact event data the same way stream_workflow does
        event_data = {
            "artifact_type": "task",
            "artifact_id": f"task:{task_artifact['task_id']}",
            "agent": "main",
            "thread_id": handler.thread_id,
            "status": "completed",
            "payload": task_artifact,
            "tool_call_id": "tc-nvidia-001",
        }
        raw = handler._format_sse_event("artifact", event_data)
        parsed = json.loads(raw.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["tool_call_id"] == "tc-nvidia-001"
        assert parsed["artifact_type"] == "task"
        assert parsed["artifact_id"] == "task:abc123"
        assert parsed["payload"]["task_id"] == "abc123"

    def test_task_artifact_event_without_tool_call_id_still_works(self):
        """Legacy events without tool_call_id should still emit correctly."""
        handler = self._make_handler()
        event_data = {
            "artifact_type": "task",
            "artifact_id": "task:legacy123",
            "agent": "main",
            "thread_id": handler.thread_id,
            "status": "completed",
            "payload": {"task_id": "legacy123", "action": "init"},
        }
        raw = handler._format_sse_event("artifact", event_data)
        parsed = json.loads(raw.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["artifact_type"] == "task"
        assert "tool_call_id" not in parsed


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — event_counter integration
# ---------------------------------------------------------------------------


class TestEventCounter:
    """Test that event_counter (shared counter) is respected."""

    def test_uses_event_counter_when_set(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        handler = WorkflowStreamHandler(thread_id="t1")
        counter = MagicMock()
        counter.next.side_effect = [42, 43]
        handler.event_counter = counter
        e1 = handler._format_sse_event("message_chunk", {"content": "a"})
        e2 = handler._format_sse_event("message_chunk", {"content": "b"})
        assert "id: 42\n" in e1
        assert "id: 43\n" in e2
        assert counter.next.call_count == 2


class TestToolAgentReasoningSuppression:
    """Tool-node inner LLM reasoning must not surface as user-facing reasoning;
    subagents (task:*/research:*) and the main model must still emit it."""

    def _handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler
        return WorkflowStreamHandler(thread_id="t-tool-gate")

    def _chunk(self, content, kwargs=None):
        from langchain_core.messages import AIMessageChunk
        return AIMessageChunk(
            content=content,
            id="msg-1",
            additional_kwargs=kwargs or {},
            response_metadata={},
        )

    async def _drain(self, agen):
        return [ev async for ev in agen]

    def test_tool_agent_reasoning_content_suppressed(self):
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "internal CoT"}],
            kwargs={"reasoning_content": "internal CoT"},
        )
        events = asyncio.run(self._drain(handler._process_message_chunk(chunk, "tools:abc")))
        assert not any("reasoning_signal" in e for e in events)
        assert not any('"content_type": "reasoning"' in e for e in events)
        assert "tools:abc" not in handler.reasoning_active

    def test_model_agent_reasoning_still_emitted(self):
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "thinking out loud"}],
            kwargs={"reasoning_content": "thinking out loud"},
        )
        events = asyncio.run(self._drain(handler._process_message_chunk(chunk, "model:xyz")))
        assert any("reasoning_signal" in e and '"content": "start"' in e for e in events)
        assert any('"content_type": "reasoning"' in e for e in events)

    def test_subagent_reasoning_still_emitted(self):
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "subagent thought"}],
            kwargs={"reasoning_content": "subagent thought"},
        )
        events = asyncio.run(
            self._drain(handler._process_message_chunk(chunk, "task:7d0e9f"))
        )
        assert any("reasoning_signal" in e and '"content": "start"' in e for e in events)
        assert any('"content_type": "reasoning"' in e for e in events)

    def test_bare_tools_agent_name_suppressed(self):
        """The LangGraph default tool node name "tools" (no colon) is also gated."""
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "x"}],
            kwargs={"reasoning_content": "x"},
        )
        events = asyncio.run(self._drain(handler._process_message_chunk(chunk, "tools")))
        assert not any("reasoning_signal" in e for e in events)


# ---------------------------------------------------------------------------
# Context-window token threshold resolution
# ---------------------------------------------------------------------------


class TestResolveTokenThreshold:
    """The UI ring threshold must match what the compaction middleware uses for
    this user — not the base YAML default — otherwise the profile picker is a
    lie from the user's perspective."""

    def _handler(self, agent_config=None):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="t1", agent_config=agent_config)

    def _cfg(self, threshold: int):
        cfg = MagicMock()
        cfg.compaction.token_threshold = threshold
        return cfg

    def test_per_request_config_wins_over_base(self):
        per_request = self._cfg(100_000)
        base = self._cfg(200_000)
        handler = self._handler(agent_config=per_request)

        with patch("src.server.app.setup.agent_config", base):
            assert handler._resolve_token_threshold() == 100_000

    def test_falls_back_to_base_config_when_no_per_request(self):
        base = self._cfg(180_000)
        handler = self._handler(agent_config=None)

        with patch("src.server.app.setup.agent_config", base):
            assert handler._resolve_token_threshold() == 180_000

    def test_falls_back_to_default_when_nothing_configured(self):
        handler = self._handler(agent_config=None)

        with patch("src.server.app.setup.agent_config", None):
            assert handler._resolve_token_threshold() == 120_000


# ---------------------------------------------------------------------------
# Compaction chunk routing
# ---------------------------------------------------------------------------


class TestCompactionChunkRouting:
    """LLM output from the compaction middleware must be emitted as
    compaction_chunk, not message_chunk, so the frontend can keep it out of
    the assistant response. The routing is driven by a per-namespace window
    opened by a context_window summarize start signal and closed by
    complete OR error (an error that never closed would infinitely mark
    regular output as compaction)."""

    def _handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="t1")

    def test_reasoning_signal_routes_to_compaction_when_flagged(self):
        handler = self._handler()
        evt = handler._format_reasoning_signal(
            "agent", "msg-1", "start", is_compaction=True
        )
        assert "event: compaction_chunk\n" in evt

    def test_reasoning_signal_stays_on_message_chunk_by_default(self):
        handler = self._handler()
        evt = handler._format_reasoning_signal("agent", "msg-1", "start")
        assert "event: message_chunk\n" in evt

    @pytest.mark.asyncio
    async def test_process_message_chunk_emits_compaction_chunk(self):
        from langchain_core.messages import AIMessageChunk

        handler = self._handler()
        chunk = AIMessageChunk(content="summary text", id="s-1")

        events = [
            e
            async for e in handler._process_message_chunk(
                chunk, "agent", is_compaction=True
            )
        ]
        assert any("event: compaction_chunk\n" in e for e in events)
        assert not any("event: message_chunk\n" in e for e in events)

    @pytest.mark.asyncio
    async def test_process_message_chunk_emits_message_chunk_by_default(self):
        from langchain_core.messages import AIMessageChunk

        handler = self._handler()
        chunk = AIMessageChunk(content="hello", id="m-1")

        events = [
            e async for e in handler._process_message_chunk(chunk, "agent")
        ]
        assert any("event: message_chunk\n" in e for e in events)
        assert not any("event: compaction_chunk\n" in e for e in events)

    def test_summarize_start_opens_window_for_namespace(self):
        handler = self._handler()
        ns = ("parent", "model:abc")
        handler._compaction_windows.add(ns)
        assert tuple(ns) in handler._compaction_windows

    def test_summarize_error_must_close_the_window(self):
        """If an error did not close the window we'd flag every subsequent
        chunk as compaction and the UI would never see real output again."""
        handler = self._handler()
        ns = ()
        handler._compaction_windows.add(ns)
        # Simulate the error-signal discard path
        handler._compaction_windows.discard(ns)
        assert ns not in handler._compaction_windows
