"""
Tests for src/server/handlers/streaming_handler.py

Covers:
- StreamEventAccumulator: accumulation, merging, max buffer size
- WorkflowStreamHandler: SSE event formatting, keepalive, error events
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

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

        return WorkflowStreamHandler(thread_id=thread_id, run_id="r-test")

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

        return WorkflowStreamHandler(thread_id="test-thread", run_id="r-test")

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

        return WorkflowStreamHandler(thread_id="int-thread", run_id="r-test")

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

        return WorkflowStreamHandler(thread_id="t-task-artifact", run_id="r-test")

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
# WorkflowStreamHandler — provenance custom-event dispatch
# ---------------------------------------------------------------------------


class TestProvenanceEvent:
    """Tests the provenance branch of the custom-event dispatch.

    The middleware emits a flat ``{"type": "provenance", ...}`` custom event
    with ``agent=None``; the handler strips ``type``, resolves ``agent`` from
    the LangGraph namespace, and re-emits a flat ``provenance`` SSE event whose
    fields land at the top level (matching the frontend's ProvenanceEvent).
    """

    def _make_handler(self, background_registry=None):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(
            thread_id="t-provenance",
            run_id="r-test",
            background_registry=background_registry,
        )

    def _dispatch(self, handler, event_data, agent_from_stream):
        """Drive the REAL provenance transform used by the dispatch branch."""
        prov_data = handler._resolve_provenance_event(event_data, agent_from_stream)
        raw = handler._format_sse_event("provenance", prov_data)
        event_line = raw.split("\n")[1]
        parsed = json.loads(raw.split("data: ", 1)[1].rstrip("\n"))
        return event_line, parsed

    def test_provenance_event_is_flat_with_resolved_main_agent(self):
        """Fields land at the top level; agent resolves to main with no namespace."""
        handler = self._make_handler()
        event_data = {
            "type": "provenance",
            "record_id": "rec-001",
            "source_type": "web_search",
            "identifier": "https://example.com/article",
            "title": "Example Article",
            "provider": "tavily",
            "tool_call_id": "tc-001",
            "args_fingerprint": {"query": "example"},
            "result_sha256": "abc123",
            "result_size": 4096,
            "result_snippet": "snippet text",
            "timestamp": "2024-01-01T00:00:00Z",
            "agent": None,
        }
        event_line, parsed = self._dispatch(handler, event_data, agent_from_stream=())

        assert event_line == "event: provenance"
        # type becomes the event name, not a payload field
        assert "type" not in parsed
        # main agent (empty namespace) is pinned to "main", honoring the
        # "main" | "task:{id}" contract (not the _extract_agent_name fallback).
        assert parsed["agent"] == "main"
        # all middleware fields pass through flat
        assert parsed["record_id"] == "rec-001"
        assert parsed["source_type"] == "web_search"
        assert parsed["identifier"] == "https://example.com/article"
        assert parsed["title"] == "Example Article"
        assert parsed["provider"] == "tavily"
        assert parsed["tool_call_id"] == "tc-001"
        assert parsed["args_fingerprint"] == {"query": "example"}
        assert parsed["result_sha256"] == "abc123"
        assert parsed["result_size"] == 4096
        assert parsed["result_snippet"] == "snippet text"
        assert parsed["timestamp"] == "2024-01-01T00:00:00Z"
        # no nested data envelope
        assert "data" not in parsed

    def test_provenance_event_resolves_subagent_attribution(self):
        """A registered subagent namespace yields task:{id} attribution."""
        from src.ptc_agent.agent.middleware.background_subagent.registry import (
            BackgroundTaskRegistry,
        )

        registry = BackgroundTaskRegistry(thread_id="t-provenance")
        task = MagicMock()
        task.task_id = "sub42"
        registry._tasks["tc-sub"] = task
        registry._ns_uuid_to_tool_call_id["ns-uuid-1"] = "tc-sub"

        handler = self._make_handler(background_registry=registry)
        event_data = {
            "type": "provenance",
            "record_id": "rec-002",
            "source_type": "file_read",
            "identifier": "work/notes.md",
            "timestamp": "2024-01-01T00:00:00Z",
            "agent": None,
        }
        # namespace last element matches the registered uuid
        event_line, parsed = self._dispatch(
            handler, event_data, agent_from_stream=("tools:ns-uuid-1",)
        )

        assert event_line == "event: provenance"
        assert parsed["agent"] == "task:sub42"
        assert parsed["source_type"] == "file_read"
        assert parsed["identifier"] == "work/notes.md"


# ---------------------------------------------------------------------------
# WorkflowStreamHandler — event_counter integration
# ---------------------------------------------------------------------------


class TestEventCounter:
    """Test that event_counter (shared counter) is respected."""

    def test_uses_event_counter_when_set(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        handler = WorkflowStreamHandler(thread_id="t1", run_id="r-test")
        counter = MagicMock()
        counter.next.side_effect = [42, 43]
        handler.event_counter = counter
        e1 = handler._format_sse_event("message_chunk", {"content": "a"})
        e2 = handler._format_sse_event("message_chunk", {"content": "b"})
        assert "id: 42\n" in e1
        assert "id: 43\n" in e2
        assert counter.next.call_count == 2


class TestToolNodeInnerLLMSuppression:
    """Inner-LLM chunks emitted from inside a tool node (e.g. WebFetch's
    extraction model) must not surface as user-facing message_chunk content —
    the tool's user-facing output arrives via tool_call_result. The gate keys
    on ``message_metadata.langgraph_node == "tools"`` rather than agent_name,
    so it correctly catches tool-internal LLM calls even when the surrounding
    namespace resolves to a subagent identity (task:*/research:*)."""

    def _handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler
        return WorkflowStreamHandler(thread_id="t-tool-gate", run_id="r-test")

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

    def test_tool_node_reasoning_suppressed(self):
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "internal CoT"}],
            kwargs={"reasoning_content": "internal CoT"},
        )
        events = asyncio.run(self._drain(
            handler._process_message_chunk(chunk, "tools", {"langgraph_node": "tools"})
        ))
        assert not any("reasoning_signal" in e for e in events)
        assert not any('"content_type": "reasoning"' in e for e in events)
        assert "tools" not in handler.reasoning_active

    def test_tool_node_text_also_suppressed(self):
        """Text content from the inner LLM is the tool's return value — it
        must not also leak as inline message_chunk content."""
        handler = self._handler()
        chunk = self._chunk("Based on the webpage, here is the extracted information...")
        events = asyncio.run(self._drain(
            handler._process_message_chunk(chunk, "tools", {"langgraph_node": "tools"})
        ))
        assert not any('"content_type": "text"' in e for e in events)

    def test_tool_node_inside_subagent_suppressed(self):
        """Regression: when web_fetch runs inside a `research` subagent, the
        agent_name resolves to task:<id> via the namespace tuple, but the
        underlying chunk still has langgraph_node="tools". Must be suppressed."""
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "subagent's tool extracting"}],
            kwargs={"reasoning_content": "We need to answer the user's prompt..."},
        )
        events = asyncio.run(self._drain(
            handler._process_message_chunk(
                chunk,
                "task:7d0e9f",
                {"langgraph_node": "tools"},
            )
        ))
        assert not any("reasoning_signal" in e for e in events)
        assert not any('"content_type": "reasoning"' in e for e in events)
        assert not any('"content_type": "text"' in e for e in events)

    def test_model_node_reasoning_still_emitted(self):
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "thinking out loud"}],
            kwargs={"reasoning_content": "thinking out loud"},
        )
        events = asyncio.run(self._drain(
            handler._process_message_chunk(chunk, "model:xyz", {"langgraph_node": "model_request"})
        ))
        assert any("reasoning_signal" in e and '"content": "start"' in e for e in events)
        assert any('"content_type": "reasoning"' in e for e in events)

    def test_subagent_primary_reasoning_still_emitted(self):
        """A subagent's own primary LLM call has langgraph_node != "tools" and
        must still surface its reasoning normally."""
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "subagent thought"}],
            kwargs={"reasoning_content": "subagent thought"},
        )
        events = asyncio.run(self._drain(
            handler._process_message_chunk(
                chunk,
                "task:7d0e9f",
                {"langgraph_node": "model_request"},
            )
        ))
        assert any("reasoning_signal" in e and '"content": "start"' in e for e in events)
        assert any('"content_type": "reasoning"' in e for e in events)

    def test_tool_message_content_preserved_in_tools_node(self):
        """ToolMessage carries the tool's user-facing return — its content
        must NOT be suppressed even though it's emitted from langgraph_node='tools'.
        The suppression gate keys on AIMessageChunk specifically; ToolMessage
        flows through to ``tool_call_result`` with its content intact.

        Pre-fix regression: a broader ``not is_tool_node`` gate stripped
        ``content`` from every tool's ``tool_call_result`` event, breaking the
        UI for web_search, web_fetch, sec, market_data, execute_code, every
        MCP tool, etc."""
        from langchain_core.messages import ToolMessage
        handler = self._handler()
        tm = ToolMessage(content="SEARCH RESULTS", tool_call_id="c1")
        events = asyncio.run(self._drain(
            handler._process_message_chunk(
                tm,
                "tools",
                {"langgraph_node": "tools"},
            )
        ))
        assert any(
            "tool_call_result" in e
            and '"tool_call_id": "c1"' in e
            and '"content": "SEARCH RESULTS"' in e
            for e in events
        ), f"tool_call_result event missing content; events={events!r}"

    def test_tool_message_content_preserved_inside_subagent(self):
        """ToolMessage inside a subagent's tool node also keeps its content —
        e.g. when a subagent calls web_search, the ToolMessage that returns
        the search results must surface to the per-task channel, not be
        dropped by the tools-node gate."""
        from langchain_core.messages import ToolMessage
        handler = self._handler()
        tm = ToolMessage(content="subagent tool output", tool_call_id="c2")
        events = asyncio.run(self._drain(
            handler._process_message_chunk(
                tm,
                "task:7d0e9f",
                {"langgraph_node": "tools"},
            )
        ))
        assert any(
            "tool_call_result" in e
            and '"tool_call_id": "c2"' in e
            and '"content": "subagent tool output"' in e
            for e in events
        ), f"tool_call_result event missing content; events={events!r}"

    def test_missing_metadata_defaults_to_emit(self):
        """If metadata is None or omits langgraph_node, the chunk is treated
        as user-facing (not a tool-internal call)."""
        handler = self._handler()
        chunk = self._chunk(
            [{"type": "text", "text": "x"}],
            kwargs={"reasoning_content": "x"},
        )
        events = asyncio.run(self._drain(
            handler._process_message_chunk(chunk, "model:xyz", None)
        ))
        assert any("reasoning_signal" in e and '"content": "start"' in e for e in events)


# ---------------------------------------------------------------------------
# Context-window token threshold resolution
# ---------------------------------------------------------------------------


class TestResolveTokenThreshold:
    """The UI ring threshold must match what the compaction middleware uses for
    this user — not the base YAML default — otherwise the profile picker is a
    lie from the user's perspective."""

    def _handler(self, agent_config=None):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="t1", run_id="r-test", agent_config=agent_config)

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

        return WorkflowStreamHandler(thread_id="t1", run_id="r-test")

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


# ---------------------------------------------------------------------------
# Stop-point reconciliation (decision 1b / T3-A): finalize_stopped_events
# ---------------------------------------------------------------------------


class TestFinalizeStoppedEvents:
    """A user stop must close every open streaming structure so replay shows
    partial fragments marked 'stopped' instead of live-looking zombies."""

    def _handler(self, thread_id="t-stop"):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id=thread_id, run_id="r-stop")

    def _events(self, handler):
        return handler.get_sse_events() or []

    def test_mid_reasoning_gets_reasoning_complete(self):
        handler = self._handler()
        # Simulate an open reasoning block on the main agent.
        handler.reasoning_active.add("agent")
        handler._open_message_ids["agent"] = "msg-1"

        out = handler.finalize_stopped_events()

        # A reasoning_signal:"complete" was appended for the open block.
        assert any(
            e["event"] in ("message_chunk", "compaction_chunk")
            and e["data"].get("content_type") == "reasoning_signal"
            and e["data"].get("content") == "complete"
            for e in out
        )
        # Reasoning state cleared.
        assert not handler.reasoning_active

    def test_mid_tool_args_gets_terminal_close(self):
        handler = self._handler()
        # Simulate an in-flight Anthropic tool-call stream.
        handler.anthropic_tool_call_state[("agent", 0)] = {
            "name": "execute_code",
            "id": "call-1",
            "args_accumulated": '{"code": "import pa',
        }
        handler._open_message_ids["agent"] = "msg-tool"

        out = handler.finalize_stopped_events()

        assert any(
            e["event"] == "tool_call_chunks"
            and e["data"].get("finish_reason") == "stopped"
            for e in out
        )
        assert not handler.anthropic_tool_call_state

    def test_same_agent_in_both_tool_states_closes_once(self):
        """A mid-turn provider fallback can leave the same agent in both the
        Response-API and Anthropic tool-call dicts; the stop must emit exactly
        one tool-call close for that agent, not two."""
        handler = self._handler()
        handler.function_call_state[("agent", 0)] = {"name": "execute_code", "id": "c1"}
        handler.anthropic_tool_call_state[("agent", 1)] = {"name": "execute_code", "id": "c2"}
        handler._open_message_ids["agent"] = "msg-tool"

        out = handler.finalize_stopped_events()

        tool_closes = [
            e
            for e in out
            if e["event"] == "tool_call_chunks"
            and e["data"].get("agent") == "agent"
            and e["data"].get("finish_reason") == "stopped"
        ]
        assert len(tool_closes) == 1
        assert not handler.function_call_state
        assert not handler.anthropic_tool_call_state

    def test_mid_artifact_gets_stopped_status(self):
        handler = self._handler()
        # Simulate an in-progress artifact.
        handler._open_artifacts["art-1"] = {
            "artifact_type": "chart",
            "artifact_id": "art-1",
            "agent": "agent",
            "status": "in_progress",
            "payload": {},
        }

        out = handler.finalize_stopped_events()

        assert any(
            e["event"] == "artifact"
            and e["data"].get("artifact_id") == "art-1"
            and e["data"].get("status") == "stopped"
            for e in out
        )
        assert not handler._open_artifacts

    def test_open_message_gets_finish_reason_stopped(self):
        handler = self._handler()
        handler._open_message_ids["agent"] = "msg-open"

        out = handler.finalize_stopped_events()

        assert any(
            e["event"] == "message_chunk"
            and e["data"].get("id") == "msg-open"
            and e["data"].get("finish_reason") == "stopped"
            for e in out
        )
        assert not handler._open_message_ids

    def test_clean_boundary_appends_no_synthetic_events(self):
        """Negative case: nothing open ⇒ no synthetic close events."""
        handler = self._handler()
        # A clean, already-closed message with a terminal finish_reason.
        handler._format_sse_event(
            "message_chunk",
            {
                "thread_id": "t-stop",
                "agent": "agent",
                "id": "msg-done",
                "role": "assistant",
                "content": "done.",
                "finish_reason": "stop",
            },
        )
        before = self._events(handler)

        out = handler.finalize_stopped_events()

        # No new events were appended.
        assert len(out) == len(before)
        assert not any(
            e["data"].get("finish_reason") == "stopped" for e in out
        )

    def test_idempotent_double_stop_no_duplicate_closes(self):
        """The stop-finalized marker prevents duplicate synthetic closes on a
        second stop / handler re-entry."""
        handler = self._handler()
        handler.reasoning_active.add("agent")
        handler._open_message_ids["agent"] = "msg-1"

        first = handler.finalize_stopped_events()
        first_len = len(first)

        second = handler.finalize_stopped_events()

        assert len(second) == first_len
        assert handler._stop_finalized is True


# ---------------------------------------------------------------------------
# Compaction admission-guard wiring
# ---------------------------------------------------------------------------


class TestCompactionWindowGuard:
    """_open_compaction_window / _close_compaction_window drive the
    BackgroundTaskManager admission guard off compaction-window cardinality:
    the guard opens on the FIRST window and closes only when the LAST one
    (main + any overlapping subgraph window) is gone."""

    BTM = (
        "src.server.services.background_task_manager."
        "BackgroundTaskManager.get_instance"
    )

    def _make_handler(self, thread_id="test-thread"):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id=thread_id, run_id="r-test")

    def test_first_window_opens_guard(self):
        handler = self._make_handler()
        manager = MagicMock()
        manager.begin_compaction.return_value = True
        with patch(self.BTM, return_value=manager):
            handler._open_compaction_window(())
        manager.begin_compaction.assert_called_once_with("test-thread")
        assert handler._compaction_active is True
        assert () in handler._compaction_windows

    def test_overlapping_windows_open_guard_once(self):
        handler = self._make_handler()
        manager = MagicMock()
        with patch(self.BTM, return_value=manager):
            handler._open_compaction_window(())
            handler._open_compaction_window(("sub",))
        manager.begin_compaction.assert_called_once_with("test-thread")
        assert handler._compaction_windows == {(), ("sub",)}

    def test_guard_released_only_when_last_window_closes(self):
        handler = self._make_handler()
        manager = MagicMock()
        manager.begin_compaction.return_value = True
        with patch(self.BTM, return_value=manager):
            handler._open_compaction_window(())
            handler._open_compaction_window(("sub",))
            handler._close_compaction_window(("sub",))
            # One window remains → the guard must stay open.
            manager.end_compaction.assert_not_called()
            assert handler._compaction_active is True
            handler._close_compaction_window(())
        manager.end_compaction.assert_called_once_with("test-thread")
        assert handler._compaction_active is False
        assert handler._compaction_windows == set()

    def test_close_without_open_is_noop(self):
        handler = self._make_handler()
        manager = MagicMock()
        with patch(self.BTM, return_value=manager):
            handler._close_compaction_window(())
        manager.end_compaction.assert_not_called()
        assert handler._compaction_active is False

    def test_open_window_skips_release_when_guard_not_owned(self):
        """begin_compaction returning False means another path already holds
        the thread's guard. This handler must NOT take ownership: _compaction_active
        stays False so closing the window (or the outer-finally net) never
        end_compaction()s a guard it does not own."""
        handler = self._make_handler()
        manager = MagicMock()
        manager.begin_compaction.return_value = False
        with patch(self.BTM, return_value=manager):
            handler._open_compaction_window(())
            assert handler._compaction_active is False
            handler._close_compaction_window(())
        manager.end_compaction.assert_not_called()

    @pytest.mark.asyncio
    async def test_outer_finally_releases_guard_on_stream_error(self):
        """The outer finally is the sole safety net: if the stream aborts
        (error / cancel / timeout) with a compaction window still open, the
        guard is released exactly once so a queued POST is never stranded."""
        handler = self._make_handler(thread_id="t-finally")
        manager = MagicMock()
        manager.begin_compaction.return_value = True
        graph = MagicMock()
        graph.astream.side_effect = RuntimeError("graph blew up")
        with patch(self.BTM, return_value=manager):
            handler._open_compaction_window(())  # _compaction_active=True
            with pytest.raises(RuntimeError):
                async for _ in handler.stream_workflow(
                    graph, {}, {"configurable": {}}
                ):
                    pass
        manager.end_compaction.assert_called_once_with("t-finally")
        assert handler._compaction_active is False

    @pytest.mark.asyncio
    async def test_outer_finally_clears_windows_so_guard_is_reacquirable(self):
        """The safety net must also clear _compaction_windows, not just
        _compaction_active. A stale window left behind would make a later
        _open_compaction_window see was_empty=False and skip begin_compaction,
        silently failing to re-arm the guard if the handler is ever reused."""
        handler = self._make_handler(thread_id="t-reopen")
        manager = MagicMock()
        manager.begin_compaction.return_value = True
        graph = MagicMock()
        graph.astream.side_effect = RuntimeError("graph blew up")
        with patch(self.BTM, return_value=manager):
            handler._open_compaction_window(())
            with pytest.raises(RuntimeError):
                async for _ in handler.stream_workflow(
                    graph, {}, {"configurable": {}}
                ):
                    pass
            # The safety net left no stale windows behind...
            assert handler._compaction_windows == set()
            manager.begin_compaction.reset_mock()
            # ...so a subsequent open re-arms the guard from scratch.
            handler._open_compaction_window(())
        manager.begin_compaction.assert_called_once_with("t-reopen")
        assert handler._compaction_active is True


# ---------------------------------------------------------------------------
# Model resilience events (model_retry / model_fallback + error trace)
# ---------------------------------------------------------------------------


class _CustomEventGraph:
    """LangGraph stand-in whose astream yields custom-mode events."""

    def __init__(self, events, namespace=()):
        self._events = events
        self._namespace = namespace

    def astream(self, _input_state, config=None, stream_mode=None, subgraphs=None):
        async def _gen():
            for event in self._events:
                yield (self._namespace, "custom", event)

        return _gen()


class TestModelResilienceEvents:
    def _make_handler(self, thread_id="res-thread"):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id=thread_id, run_id="r-test")

    async def _collect(self, handler, events, namespace=()):
        return [
            chunk
            async for chunk in handler.stream_workflow(
                _CustomEventGraph(events, namespace), input_state={}, config={}
            )
        ]

    @staticmethod
    def _find_event(chunks, event_type):
        for chunk in chunks:
            if f"event: {event_type}\n" in chunk:
                return json.loads(chunk.split("data: ", 1)[1].rstrip("\n"))
        return None

    @pytest.mark.asyncio
    async def test_model_retry_emitted_but_not_persisted(self):
        handler = self._make_handler()
        chunks = await self._collect(
            handler,
            [
                {
                    "type": "model_retry",
                    "model": "primary-model",
                    "attempt": 1,
                    "max_retries": 3,
                    "error": "upstream 500 at https://user:secret@api.example.com/v1",
                    "status_code": 500,
                    "delay_seconds": 1.0,
                }
            ],
        )
        data = self._find_event(chunks, "model_retry")
        assert data is not None
        assert data["thread_id"] == "res-thread"
        assert data["model"] == "primary-model"
        assert data["attempt"] == 1
        assert data["max_retries"] == 3
        assert data["status_code"] == 500
        # Credentials scrubbed from the middleware-provided error text
        assert "secret" not in data["error"]
        # Main-stream events pin "main" (empty namespace + no langgraph_node
        # in the middleware payload would otherwise hit the "agent" fallback).
        assert data["agent"] == "main"
        # Transient: never persisted to the SSE event log
        persisted = handler.get_sse_events() or []
        assert "model_retry" not in [e["event"] for e in persisted]

    @pytest.mark.asyncio
    async def test_model_fallback_emitted_and_persisted(self):
        handler = self._make_handler()
        chunks = await self._collect(
            handler,
            [
                {
                    "type": "model_fallback",
                    "from_model": "primary-model",
                    "to_model": "fallback-a",
                    "from_is_primary": True,
                    "error": "Error code: 404",
                    "status_code": 404,
                    "attempts_on_from": 1,
                }
            ],
        )
        data = self._find_event(chunks, "model_fallback")
        assert data is not None
        assert data["from_model"] == "primary-model"
        assert data["to_model"] == "fallback-a"
        assert data["from_is_primary"] is True
        assert data["attempts_on_from"] == 1
        assert data["agent"] == "main"
        # Persisted so the transcript note survives history replay
        persisted = handler.get_sse_events() or []
        assert "model_fallback" in [e["event"] for e in persisted]

    @pytest.mark.asyncio
    async def test_subagent_namespace_attributed(self):
        handler = self._make_handler()
        chunks = await self._collect(
            handler,
            [
                {
                    "type": "model_fallback",
                    "from_model": "m-a",
                    "to_model": "m-b",
                    "from_is_primary": True,
                    "error": "x",
                    "status_code": 400,
                    "attempts_on_from": 1,
                }
            ],
            namespace=("task-ns-uuid",),
        )
        data = self._find_event(chunks, "model_fallback")
        assert data is not None
        # Without a background registry the raw namespace passes through —
        # the point is main-agent events don't get subagent attribution and
        # vice versa.
        assert data["agent"] == "task-ns-uuid"


class TestFormatErrorEventResilienceTrace:
    def _make_handler(self):
        from src.server.handlers.streaming_handler import WorkflowStreamHandler

        return WorkflowStreamHandler(thread_id="err-thread", run_id="r-test")

    @staticmethod
    def _trace():
        return {
            "model": "primary-model",
            "attempted_models": [
                {
                    "model": "primary-model",
                    "error": "Error code: 404 at https://user:secret@h/v1",
                    "status_code": 404,
                    "attempts": 1,
                },
                {
                    "model": "fallback-a",
                    "error": "param mismatch",
                    "status_code": 400,
                    "attempts": 1,
                },
            ],
        }

    def test_trace_enriches_error_event(self):
        exc = RuntimeError("Error calling model 'primary-model'")
        exc.__model_resilience__ = self._trace()
        handler = self._make_handler()
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["model"] == "primary-model"
        attempted = parsed["attempted_models"]
        assert [a["model"] for a in attempted] == ["primary-model", "fallback-a"]
        # Per-entry error text is sanitized before hitting the wire
        assert "secret" not in attempted[0]["error"]
        assert attempted[1]["status_code"] == 400

    def test_trace_found_via_cause_chain(self):
        inner = RuntimeError("primary failed")
        inner.__model_resilience__ = self._trace()
        try:
            try:
                raise inner
            except Exception as e:
                raise RuntimeError("wrapped") from e
        except RuntimeError as outer:
            exc = outer
        handler = self._make_handler()
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["model"] == "primary-model"

    def test_no_trace_no_new_fields(self):
        exc = RuntimeError("plain failure")
        handler = self._make_handler()
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert "model" not in parsed
        assert "attempted_models" not in parsed
        assert parsed["error_kind"] == "internal"

    def test_generic_exception_with_trace_classifies_upstream(self):
        # A generic (non-SDK) exception carrying the trace is still a
        # model-call failure — it must not fall into the internal-error
        # banner path, which drops the model/attempted-models context.
        exc = RuntimeError("Error calling model 'primary-model'")
        exc.__model_resilience__ = self._trace()
        handler = self._make_handler()
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "upstream"
        # Status comes from the primary attempt record; 404 drives the hints.
        assert parsed["status_code"] == 404
        assert parsed["hints"] == ["model_access", "try_another_model"]

    def test_generic_exception_with_trace_but_no_status(self):
        trace = self._trace()
        trace["attempted_models"][0]["status_code"] = None
        exc = RuntimeError("model call blew up without a status")
        exc.__model_resilience__ = trace
        handler = self._make_handler()
        result = handler.format_error_event(str(exc), exc=exc)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["error_kind"] == "upstream"
        assert "status_code" not in parsed
        assert parsed["hints"] == [
            "api_key",
            "model_access",
            "provider_status",
            "try_another_model",
        ]


class TestSanitizeErrorText:
    """Credential shapes are masked before error text reaches the wire."""

    def test_url_userinfo_stripped(self):
        from src.server.handlers.streaming_handler import _sanitize_error_text

        out = _sanitize_error_text("GET https://user:hunter2@api.example.com/v1 failed")
        assert "hunter2" not in out
        assert "https://api.example.com/v1" in out

    def test_bearer_token_masked(self):
        from src.server.handlers.streaming_handler import _sanitize_error_text

        out = _sanitize_error_text("401 with header Authorization: Bearer abc123def456xyz")
        assert "abc123def456xyz" not in out
        assert "[REDACTED]" in out

    def test_key_params_masked(self):
        from src.server.handlers.streaming_handler import _sanitize_error_text

        out = _sanitize_error_text('request had api_key="tok_0123456789abcdef" set')
        assert "tok_0123456789abcdef" not in out
        out = _sanitize_error_text("x-api-key: somelongsecretvalue1")
        assert "somelongsecretvalue1" not in out

    def test_sk_style_token_masked(self):
        from src.server.handlers.streaming_handler import _sanitize_error_text

        out = _sanitize_error_text("Invalid API key provided: sk-abcdef0123456789")
        assert "sk-abcdef0123456789" not in out

    def test_url_query_key_param_masked(self):
        from src.server.handlers.streaming_handler import _sanitize_error_text

        out = _sanitize_error_text(
            "400 for url https://example.googleapis.com/v1/models/m:generateContent"
            "?key=AIzaSyExampleExampleExample123&alt=json"
        )
        assert "AIza" not in out
        assert "?key=[REDACTED]" in out
        assert "&alt=json" in out

    def test_plain_error_text_unchanged(self):
        from src.server.handlers.streaming_handler import _sanitize_error_text

        text = "Error code: 404 - model 'some-model' not found"
        assert _sanitize_error_text(text) == text
