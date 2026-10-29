"""
Tests for the run SSE producer (src/server/services/runs/sse_producer.py) and
its error classification (stream_errors.py).
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# RunSSEProducer — SSE formatting helpers
# ---------------------------------------------------------------------------


class TestRunSSEProducerFormatting:
    """Tests for RunSSEProducer SSE formatting methods."""

    def _make_handler(self, thread_id="test-thread", credential_source=None):
        from types import SimpleNamespace

        from src.server.services.runs.sse_producer import RunSSEProducer

        config = (
            SimpleNamespace(credential_source=credential_source)
            if credential_source is not None
            else None
        )
        return RunSSEProducer(thread_id=thread_id, run_id="r-test", agent_config=config)

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

    def test_format_sse_event_nan_becomes_null(self):
        """NaN/Inf in event data must never reach the wire as bare `NaN` tokens.

        Bare NaN is invalid JSON — browser JSON.parse throws and the whole SSE
        frame (e.g. a tool_call_result) is silently dropped.
        """
        handler = self._make_handler()
        result = handler._format_sse_event(
            "tool_call_result",
            {
                "content": "ok",
                "artifact": {"close": float("nan"), "series": [1.0, float("inf")]},
            },
        )
        payload = result.split("data: ", 1)[1].rstrip("\n")
        assert "NaN" not in payload and "Infinity" not in payload
        parsed = json.loads(payload)
        assert parsed["artifact"]["close"] is None
        assert parsed["artifact"]["series"] == [1.0, None]

    def test_build_error_event(self):
        handler = self._make_handler(thread_id="err-thread")
        frame = handler._build_error_event("Something went wrong")
        assert frame.event == "error"
        parsed = frame.data
        assert parsed["thread_id"] == "err-thread"
        assert parsed["error"] == "Something went wrong"
        assert "message" in parsed
        # Without ``exc``, legacy shape only — no classification fields.
        assert "error_kind" not in parsed
        assert "hints" not in parsed

    def test_build_error_event_with_upstream_exc(self):
        """Upstream 5xx exceptions classify as upstream with provider-outage hints."""
        from anthropic import InternalServerError

        # Fabricate an anthropic.InternalServerError the way the SDK would
        # raise it on a 500 response. We just need the exception type and
        # ``.status_code`` — the classifier doesn't read the body.
        exc = InternalServerError.__new__(InternalServerError)
        exc.status_code = 500
        Exception.__init__(exc, "Error code: 500 - {'error': {'message': 'Internal service error'}}")

        handler = self._make_handler(thread_id="err-thread")
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "upstream"
        assert parsed["status_code"] == 500
        assert parsed["provider_module"] == "anthropic"
        # 5xx is a provider outage — don't suggest checking the API key.
        assert parsed["hints"] == ["provider_status", "try_another_model"]

    def test_build_error_event_upstream_401_auth_hints(self, monkeypatch):
        """401 upstream surfaces credential-oriented hints first."""
        from anthropic import AuthenticationError

        # Pinned: these hints only survive when the reader owns the key, and
        # letting .env decide that silently is how a test passes for the wrong
        # reason.
        monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")

        exc = AuthenticationError.__new__(AuthenticationError)
        exc.status_code = 401
        Exception.__init__(exc, "invalid x-api-key")

        handler = self._make_handler(thread_id="err-thread")
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "upstream"
        assert parsed["status_code"] == 401
        assert parsed["hints"] == ["api_key", "model_access", "try_another_model"]

    def test_build_error_event_upstream_no_status_falls_back(self, monkeypatch):
        """Unknown status (network error) shows all hints."""
        from anthropic import APIConnectionError

        monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")

        exc = APIConnectionError.__new__(APIConnectionError)
        Exception.__init__(exc, "Connection reset by peer")

        handler = self._make_handler(thread_id="err-thread")
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "upstream"
        assert parsed["hints"] == [
            "api_key",
            "model_access",
            "provider_status",
            "try_another_model",
        ]

    @staticmethod
    def _upstream(status):
        """An anthropic SDK error carrying ``status``, the way the SDK raises it."""
        from anthropic import APIStatusError

        exc = APIStatusError.__new__(APIStatusError)
        exc.status_code = status
        Exception.__init__(exc, f"Error code: {status}")
        return exc

    def _hints(self, exc, credential_source=None):
        handler = self._make_handler(
            thread_id="err-thread", credential_source=credential_source
        )
        return handler._build_error_event(str(exc), exc=exc).data["hints"]

    @pytest.mark.parametrize("status", [400, 405, 413, 422])
    def test_request_refused_offers_only_a_model_switch(self, monkeypatch, status):
        """A refused request is neither a bad credential nor a provider outage.

        These used to fall through to the no-status branch and claim all four
        hints, so an image the model could not accept was reported as a possible
        API-key problem and a possible provider incident at the same time.
        """
        monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
        assert self._hints(self._upstream(status)) == ["try_another_model"]

    def test_platform_billed_turn_drops_credential_hints(self, monkeypatch):
        """The user holds no key on a platform-billed turn, so do not name one."""
        monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
        assert self._hints(self._upstream(401), credential_source="platform") == [
            "try_another_model"
        ]
        # 5xx never named a credential, so it is unchanged.
        assert self._hints(self._upstream(503), credential_source="platform") == [
            "provider_status",
            "try_another_model",
        ]

    def test_user_owned_credential_keeps_its_hints(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
        assert self._hints(self._upstream(401), credential_source="byok") == [
            "api_key",
            "model_access",
            "try_another_model",
        ]

    @pytest.mark.parametrize("host_mode", ["platform", "oss"])
    def test_subscription_401_asks_for_a_reconnect_not_a_key(self, monkeypatch, host_mode):
        """A subscription has no key; its 401 is the sign-in, renewed by reconnecting."""
        monkeypatch.setattr("src.config.settings.HOST_MODE", host_mode)
        assert self._hints(self._upstream(401), credential_source="oauth") == [
            "reconnect_account",
            "model_access",
            "try_another_model",
        ]
        assert self._hints(self._upstream(403), credential_source="oauth") == [
            "model_access",
            "try_another_model",
        ]

    def test_oss_platform_source_is_still_the_operators_own_key(self, monkeypatch):
        """``platform`` means opposite things in the two host modes.

        In OSS it is the key in the operator's own .env and the operator is the
        user, so the credential hints are the actionable ones. Reading
        ``credential_source`` without the host mode gets this backwards.
        """
        monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
        assert self._hints(self._upstream(401), credential_source="platform") == [
            "api_key",
            "model_access",
            "try_another_model",
        ]

    def test_unknown_credential_fails_closed_on_the_hosted_service(self, monkeypatch):
        """No agent_config: a wrong credential hint is worse than a missing one."""
        monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")
        assert self._hints(self._upstream(401)) == ["try_another_model"]

    def test_build_error_event_with_internal_exc(self):
        """Bare Exception from our code is classified as internal, no hints."""
        exc = RuntimeError("workspace state corrupted")
        handler = self._make_handler(thread_id="err-thread")
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "internal"
        assert "hints" not in parsed

    def test_build_error_event_upstream_wrapped_by_internal(self):
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
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "upstream"
        assert parsed["provider_module"] == "anthropic"

    def test_classify_stream_exception_parses_status_from_message(self):
        """When status_code isn't on the exception, parse it from the message."""
        from src.server.services.runs.stream_errors import classify_stream_exception
        import httpx

        exc = httpx.HTTPError("got HTTP 429 from upstream, backing off")
        info = classify_stream_exception(exc)
        assert info["kind"] == "upstream"
        assert info["status_code"] == 429
        assert info["provider_module"] == "httpx"

    def test_classify_stream_exception_reads_no_status_from_a_timeout(self):
        """A stall's chunk count is not an HTTP status."""
        from langchain_openai.chat_models._client_utils import StreamChunkTimeoutError

        from src.server.services.runs.stream_errors import classify_stream_exception

        info = classify_stream_exception(StreamChunkTimeoutError(120.0, chunks_received=413))
        assert info["kind"] == "upstream"
        assert info["status_code"] is None

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
        result = handler._number_sse_event(
            handler._build_credit_usage_event(
                thread_id="credit-thread",
                token_usage=token_usage,
                total_credits=1.5,
            )
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
        result = handler._number_sse_event(
            handler._build_reasoning_signal("main", "msg-1", "start")
        )
        assert "event: message_chunk\n" in result
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["content"] == "start"
        assert parsed["content_type"] == "reasoning_signal"
        assert parsed["agent"] == "main"


# ---------------------------------------------------------------------------
# RunSSEProducer — tool call filtering
# ---------------------------------------------------------------------------


class TestToolCallFiltering:
    """Tests for the _filter_tool_calls method."""

    def _make_handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="test-thread", run_id="r-test")

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
# RunSSEProducer — _extract_reasoning_summary_index
# ---------------------------------------------------------------------------


class TestExtractReasoningSummaryIndex:
    """Tests for _extract_reasoning_summary_index static method."""

    def test_returns_index_from_reasoning_dict(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        content = {
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "thought", "index": 2}],
        }
        assert RunSSEProducer._extract_reasoning_summary_index(content) == 2

    def test_returns_none_for_non_reasoning(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        assert RunSSEProducer._extract_reasoning_summary_index("hello") is None

    def test_returns_none_for_reasoning_without_summary(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        content = {"type": "reasoning", "status": "in_progress"}
        assert RunSSEProducer._extract_reasoning_summary_index(content) is None


# ---------------------------------------------------------------------------
# RunSSEProducer — interrupt handling
# ---------------------------------------------------------------------------


class TestInterruptHandling:
    """Tests for _handle_interrupt method."""

    def _make_handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="int-thread", run_id="r-test")

    def test_handles_dict_interrupt_value(self):
        handler = self._make_handler()
        interrupt = MagicMock()
        interrupt.id = "int-1"
        interrupt.value = {"action_requests": [{"description": "Run analysis?"}]}
        frame = handler._handle_interrupt({"__interrupt__": [interrupt]})
        assert frame is not None
        result = handler._number_sse_event(frame)
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["interrupt_id"] == "int-1"
        assert parsed["action_requests"] == [{"description": "Run analysis?"}]
        assert parsed["finish_reason"] == "interrupt"

    def test_handles_string_interrupt_value(self):
        handler = self._make_handler()
        interrupt = MagicMock()
        interrupt.id = "int-2"
        interrupt.value = "Should I proceed with plan?"
        result = handler._number_sse_event(
            handler._handle_interrupt({"__interrupt__": [interrupt]})
        )
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert parsed["action_requests"] == [
            {"description": "Should I proceed with plan?"}
        ]

    def test_handles_list_interrupt_value(self):
        handler = self._make_handler()
        interrupt = MagicMock()
        interrupt.id = "int-3"
        interrupt.value = [{"description": "step 1"}, {"description": "step 2"}]
        result = handler._number_sse_event(
            handler._handle_interrupt({"__interrupt__": [interrupt]})
        )
        parsed = json.loads(result.split("data: ", 1)[1].rstrip("\n"))
        assert len(parsed["action_requests"]) == 2


# ---------------------------------------------------------------------------
# RunSSEProducer — event_counter integration
# ---------------------------------------------------------------------------


class TestTaskArtifactEvent:
    """Tests that task artifact SSE events include tool_call_id.

    When the backend emits an artifact event for a spawned subagent task,
    it must include the originating tool_call_id so the frontend can
    directly map the inline card to the correct subagent — without relying
    on FIFO ordering which is unreliable for parallel tool calls.
    """

    def _make_handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="t-task-artifact", run_id="r-test")

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
# RunSSEProducer — provenance custom-event dispatch
# ---------------------------------------------------------------------------


class TestProvenanceEvent:
    """Tests the provenance branch of the custom-event dispatch.

    The middleware emits a flat ``{"type": "provenance", ...}`` custom event
    with ``agent=None``; the handler strips ``type``, resolves ``agent`` from
    the LangGraph namespace, and re-emits a flat ``provenance`` SSE event whose
    fields land at the top level (matching the frontend's ProvenanceEvent).
    """

    def _make_handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(
            thread_id="t-provenance",
            run_id="r-test",
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


# ---------------------------------------------------------------------------
# RunSSEProducer — event_counter integration
# ---------------------------------------------------------------------------


class TestEventCounter:
    """Test that event_counter (shared counter) is respected."""

    def test_uses_event_counter_when_set(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        handler = RunSSEProducer(thread_id="t1", run_id="r-test")
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
        from src.server.services.runs.sse_producer import RunSSEProducer
        return RunSSEProducer(thread_id="t-tool-gate", run_id="r-test")

    def _chunk(self, content, kwargs=None):
        from langchain_core.messages import AIMessageChunk
        return AIMessageChunk(
            content=content,
            id="msg-1",
            additional_kwargs=kwargs or {},
            response_metadata={},
        )

    async def _drain(self, agen):
        # Frames come out unnumbered; the stream numbers them after coalescing.
        return [self._number(ev) async for ev in agen]

    @staticmethod
    def _number(frame):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="t-n", run_id="r-n")._number_sse_event(frame)

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

    def test_tool_message_status_rides_the_live_result(self):
        """The live path carries the ToolMessage status whatever it holds.

        A direct MCP tool whose successful output opens with "Refused:" reads
        as a refusal to every prose heuristic, so a client that saw no status
        would render a success as a failure.
        """
        from langchain_core.messages import ToolMessage
        handler = self._handler()
        ok = ToolMessage(content="Refused: 7 applications", tool_call_id="c3")
        events = asyncio.run(self._drain(
            handler._process_message_chunk(ok, "tools", {"langgraph_node": "tools"})
        ))
        assert any(
            "tool_call_result" in e
            and '"tool_call_id": "c3"' in e
            and '"status": "success"' in e
            for e in events
        ), f"tool_call_result event missing success status; events={events!r}"

        failed = ToolMessage(content="Refused: not permitted", tool_call_id="c4", status="error")
        events = asyncio.run(self._drain(
            handler._process_message_chunk(failed, "tools", {"langgraph_node": "tools"})
        ))
        assert any(
            "tool_call_result" in e
            and '"tool_call_id": "c4"' in e
            and '"status": "error"' in e
            for e in events
        ), f"tool_call_result event missing error status; events={events!r}"

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
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="t1", run_id="r-test", agent_config=agent_config)

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
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="t1", run_id="r-test")

    def test_reasoning_signal_routes_to_compaction_when_flagged(self):
        handler = self._handler()
        evt = handler._number_sse_event(
            handler._build_reasoning_signal(
                "agent", "msg-1", "start", is_compaction=True
            )
        )
        assert "event: compaction_chunk\n" in evt

    def test_reasoning_signal_stays_on_message_chunk_by_default(self):
        handler = self._handler()
        evt = handler._number_sse_event(
            handler._build_reasoning_signal("agent", "msg-1", "start")
        )
        assert "event: message_chunk\n" in evt

    @pytest.mark.asyncio
    async def test_process_message_chunk_emits_compaction_chunk(self):
        from langchain_core.messages import AIMessageChunk

        handler = self._handler()
        chunk = AIMessageChunk(content="summary text", id="s-1")

        events = [
            handler._number_sse_event(e)
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
            handler._number_sse_event(e)
            async for e in handler._process_message_chunk(chunk, "agent")
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
# OpenAI Responses `phase` passthrough
# ---------------------------------------------------------------------------


class TestTextPhasePassthrough:
    """`phase` rides the text event when known and is omitted when it isn't."""

    def _handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer
        return RunSSEProducer(thread_id="t-phase", run_id="r-phase")

    def _chunk(self, content, metadata=None):
        from langchain_core.messages import AIMessageChunk
        chunk = AIMessageChunk(content=content, id="msg-1")
        chunk.response_metadata = metadata or {}
        return chunk

    def _text_payloads(self, events):
        return [
            json.loads(e.split("data: ", 1)[1])
            for e in events
            if "event: message_chunk\n" in e and '"content_type": "text"' in e
        ]

    def _drain(self, handler, chunk):
        return asyncio.run(
            self._collect(handler, handler._process_message_chunk(chunk, "agent"))
        )

    async def _collect(self, handler, agen):
        return [handler._number_sse_event(e) async for e in agen]

    def test_streaming_deltas_inherit_the_announced_phase(self):
        # output_item.added announces the phase in an empty text block, then
        # every delta carries only its index.
        handler = self._handler()
        assert self._drain(handler, self._chunk(
            [{"type": "text", "text": "", "phase": "commentary", "index": 0}]
        )) == []

        payloads = self._text_payloads(self._drain(handler, self._chunk(
            [{"type": "text", "text": "hold on", "index": 0}]
        )))
        assert len(payloads) == 1
        assert payloads[0]["content"] == "hold on"
        assert payloads[0]["phase"] == "commentary"

    def test_unknown_index_omits_the_key(self):
        handler = self._handler()
        payloads = self._text_payloads(self._drain(handler, self._chunk(
            [{"type": "text", "text": "plain", "index": 3}]
        )))
        assert len(payloads) == 1
        assert "phase" not in payloads[0]

    def test_non_stream_block_carries_phase_directly(self):
        handler = self._handler()
        payloads = self._text_payloads(self._drain(handler, self._chunk(
            [{"type": "text", "text": "the answer", "phase": "final_answer"}]
        )))
        assert len(payloads) == 1
        assert payloads[0]["phase"] == "final_answer"

    def test_disagreeing_blocks_omit_the_key(self):
        handler = self._handler()
        payloads = self._text_payloads(self._drain(handler, self._chunk([
            {"type": "text", "text": "a", "phase": "commentary"},
            {"type": "text", "text": "b", "phase": "final_answer"},
        ])))
        assert len(payloads) == 1
        assert "phase" not in payloads[0]

    def test_a_phaseless_block_beside_a_phased_one_omits_the_key(self):
        # Live and replay share one rule: a block with no phase is its own
        # answer, so the chunk has no single phase to carry.
        handler = self._handler()
        payloads = self._text_payloads(self._drain(handler, self._chunk([
            {"type": "text", "text": "a", "phase": "commentary"},
            {"type": "text", "text": "b"},
        ])))
        assert len(payloads) == 1
        assert "phase" not in payloads[0]

    def test_plain_string_content_omits_the_key(self):
        handler = self._handler()
        payloads = self._text_payloads(self._drain(handler, self._chunk("hello")))
        assert len(payloads) == 1
        assert "phase" not in payloads[0]

    def test_finish_clears_the_index_table(self):
        # Indices restart with the next message, so index 0 must not keep
        # resolving to the finished message's phase.
        handler = self._handler()
        self._drain(handler, self._chunk(
            [{"type": "text", "text": "", "phase": "commentary", "index": 0}]
        ))
        self._drain(handler, self._chunk("", metadata={"finish_reason": "stop"}))

        payloads = self._text_payloads(self._drain(handler, self._chunk(
            [{"type": "text", "text": "next turn", "index": 0}]
        )))
        assert len(payloads) == 1
        assert "phase" not in payloads[0]


# ---------------------------------------------------------------------------
# Compaction admission-guard wiring
# ---------------------------------------------------------------------------


class TestCompactionWindowGuard:
    """_open_compaction_window / _close_compaction_window drive the
    ThreadMutationRunner admission window off compaction-window cardinality:
    the window opens on the FIRST window and closes only when the LAST one
    (main + any overlapping subgraph window) is gone."""

    RUNNER = (
        "src.server.services.thread_mutation."
        "ThreadMutationRunner.get_instance"
    )

    def _make_handler(self, thread_id="test-thread"):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id=thread_id, run_id="r-test")

    def test_first_window_opens_guard(self):
        handler = self._make_handler()
        runner = MagicMock()
        runner.open_auto_compaction_window.return_value = True
        with patch(self.RUNNER, return_value=runner):
            handler._open_compaction_window(())
        runner.open_auto_compaction_window.assert_called_once_with("test-thread")
        assert handler._compaction_active is True
        assert () in handler._compaction_windows

    def test_overlapping_windows_open_guard_once(self):
        handler = self._make_handler()
        runner = MagicMock()
        with patch(self.RUNNER, return_value=runner):
            handler._open_compaction_window(())
            handler._open_compaction_window(("sub",))
        runner.open_auto_compaction_window.assert_called_once_with("test-thread")
        assert handler._compaction_windows == {(), ("sub",)}

    def test_guard_released_only_when_last_window_closes(self):
        handler = self._make_handler()
        runner = MagicMock()
        runner.open_auto_compaction_window.return_value = True
        with patch(self.RUNNER, return_value=runner):
            handler._open_compaction_window(())
            handler._open_compaction_window(("sub",))
            handler._close_compaction_window(("sub",))
            # One window remains → the admission window must stay open.
            runner.close_auto_compaction_window.assert_not_called()
            assert handler._compaction_active is True
            handler._close_compaction_window(())
        runner.close_auto_compaction_window.assert_called_once_with("test-thread")
        assert handler._compaction_active is False
        assert handler._compaction_windows == set()

    def test_close_without_open_is_noop(self):
        handler = self._make_handler()
        runner = MagicMock()
        with patch(self.RUNNER, return_value=runner):
            handler._close_compaction_window(())
        runner.close_auto_compaction_window.assert_not_called()
        assert handler._compaction_active is False

    def test_open_auto_compaction_window_skips_release_when_guard_not_owned(self):
        """open_auto_compaction_window returning False means another op already holds the
        thread's mutation slot. This handler must NOT take ownership:
        _compaction_active stays False so closing the window (or the
        outer-finally net) never close_auto_compaction_window()s a slot it does not own."""
        handler = self._make_handler()
        runner = MagicMock()
        runner.open_auto_compaction_window.return_value = False
        with patch(self.RUNNER, return_value=runner):
            handler._open_compaction_window(())
            assert handler._compaction_active is False
            handler._close_compaction_window(())
        runner.close_auto_compaction_window.assert_not_called()

    @pytest.mark.asyncio
    async def test_outer_finally_releases_guard_on_stream_error(self):
        """The outer finally is the sole safety net: if the stream aborts
        (error / cancel / timeout) with a compaction window still open, the
        admission window is closed exactly once so a queued POST is never
        stranded."""
        handler = self._make_handler(thread_id="t-finally")
        runner = MagicMock()
        runner.open_auto_compaction_window.return_value = True
        graph = MagicMock()
        graph.astream.side_effect = RuntimeError("graph blew up")
        with patch(self.RUNNER, return_value=runner):
            handler._open_compaction_window(())  # _compaction_active=True
            with pytest.raises(RuntimeError):
                async for _ in handler.stream_workflow(
                    graph, {}, {"configurable": {}}
                ):
                    pass
        runner.close_auto_compaction_window.assert_called_once_with("t-finally")
        assert handler._compaction_active is False

    @pytest.mark.asyncio
    async def test_outer_finally_clears_windows_so_guard_is_reacquirable(self):
        """The safety net must also clear _compaction_windows, not just
        _compaction_active. A stale window left behind would make a later
        _open_compaction_window see was_empty=False and skip open_auto_compaction_window,
        silently failing to re-arm the admission window if the handler is
        ever reused."""
        handler = self._make_handler(thread_id="t-reopen")
        runner = MagicMock()
        runner.open_auto_compaction_window.return_value = True
        graph = MagicMock()
        graph.astream.side_effect = RuntimeError("graph blew up")
        with patch(self.RUNNER, return_value=runner):
            handler._open_compaction_window(())
            with pytest.raises(RuntimeError):
                async for _ in handler.stream_workflow(
                    graph, {}, {"configurable": {}}
                ):
                    pass
            # The safety net left no stale windows behind...
            assert handler._compaction_windows == set()
            runner.open_auto_compaction_window.reset_mock()
            # ...so a subsequent open re-arms the window from scratch.
            handler._open_compaction_window(())
        runner.open_auto_compaction_window.assert_called_once_with("t-reopen")
        assert handler._compaction_active is True


class _InFlightGraph:
    """LangGraph stand-in with a model call in flight while it streams.

    Its stream cancels and awaits the call on close, as LangGraph's does, and
    the call finishing uncancelled is what would land a usage record after
    the run's finalize had already read the tracker. With ``release`` set, the
    call's cancellation waits on it under a shield, as a tool's cleanup does.
    """

    def __init__(self, release: asyncio.Event | None = None):
        self.call: asyncio.Task | None = None
        self.closed = False
        self.release = release

    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        async def _call():
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                if self.release is not None:
                    await asyncio.shield(self.release.wait())
                raise

        async def _gen():
            self.call = asyncio.ensure_future(_call())
            try:
                while True:
                    yield ((), "custom", {"type": "context_window", "action": "offload"})
                    await asyncio.sleep(0.01)
            finally:
                self.call.cancel()
                await asyncio.gather(self.call, return_exceptions=True)
                self.closed = True

        return _gen()


def _orchestrated(graph):
    """The PTC shape: the real orchestrator around the graph."""
    from ptc_agent.agent.middleware.background_subagent.orchestrator import (
        BackgroundSubagentOrchestrator,
    )

    return BackgroundSubagentOrchestrator(graph, MagicMock())


_SHAPES = pytest.mark.parametrize(
    "shape", [lambda g: g, _orchestrated], ids=["bare graph", "orchestrated"]
)


class TestGraphStreamClosedOnExit:
    """Every way out of the producer cancels the graph's work in flight
    before its caller, the executor about to finalize, regains control, with
    the orchestrator between them as in a PTC turn."""

    def _make_handler(self, **kwargs):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="t-close", run_id="r-close", **kwargs)

    @_SHAPES
    @pytest.mark.asyncio
    async def test_time_limit_cancels_the_call_in_flight(self, shape):
        from src.server.services.runs.sse_producer import TurnTimeLimitError

        graph = _InFlightGraph()
        handler = self._make_handler(workflow_timeout=0.05)
        with pytest.raises(TurnTimeLimitError):
            async for _ in handler.stream_workflow(shape(graph), {}, {"configurable": {}}):
                pass
        assert graph.closed
        assert graph.call.cancelled()

    @_SHAPES
    @pytest.mark.asyncio
    async def test_consumer_close_cancels_the_call_in_flight(self, shape):
        graph = _InFlightGraph()
        frames = self._make_handler().stream_workflow(
            shape(graph), {}, {"configurable": {}}
        )
        await anext(frames)  # metadata
        await anext(frames)
        # The executor's response to a lost transport.
        await frames.aclose()
        assert graph.closed
        assert graph.call.cancelled()

    @pytest.mark.asyncio
    async def test_a_close_that_hangs_is_left_running_past_its_bound(
        self, monkeypatch
    ):
        from src.server.services.runs import sse_producer
        from src.server.services.runs.sse_producer import TurnTimeLimitError

        monkeypatch.setattr(sse_producer, "GRAPH_CLOSE_TIMEOUT_S", 0.05)
        release = asyncio.Event()
        graph = _InFlightGraph(release)
        handler = self._make_handler(workflow_timeout=0.05)
        with pytest.raises(TurnTimeLimitError):
            async for _ in handler.stream_workflow(
                _orchestrated(graph), {}, {"configurable": {}}
            ):
                pass
        # The run's ending reached its caller with the close still going.
        assert not graph.closed
        release.set()
        for _ in range(100):
            if graph.closed:
                break
            await asyncio.sleep(0.01)
        assert graph.closed and graph.call.cancelled()


# ---------------------------------------------------------------------------
# Model resilience events (model_retry / model_fallback + error trace)
# ---------------------------------------------------------------------------


class _CustomEventGraph:
    """LangGraph stand-in whose astream yields custom-mode events."""

    def __init__(self, events, namespace=()):
        self._events = events
        self._namespace = namespace

    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        async def _gen():
            for event in self._events:
                yield (self._namespace, "custom", event)

        return _gen()


class TestModelResilienceEvents:
    def _make_handler(self, thread_id="res-thread"):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id=thread_id, run_id="r-test")

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
    async def test_model_retry_emitted(self):
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

    @pytest.mark.asyncio
    async def test_model_fallback_emitted(self):
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

    @pytest.mark.asyncio
    async def test_model_fallback_ui_record_unwraps_to_legacy_shape(self):
        # push_ui_message wraps the payload in a langgraph ui record; the
        # handler must unwrap it to the same wire shape as the legacy event.
        handler = self._make_handler()
        chunks = await self._collect(
            handler,
            [
                {
                    "type": "ui",
                    "id": "ui-abc123",
                    "name": "model_fallback",
                    "props": {
                        "from_model": "primary-model",
                        "to_model": "fallback-a",
                        "from_is_primary": True,
                        "error": "Error code: 404 https://user:secret@host/v1",
                        "status_code": 404,
                        "attempts_on_from": 1,
                    },
                    "metadata": {},
                }
            ],
        )
        data = self._find_event(chunks, "model_fallback")
        assert data is not None
        assert data["from_model"] == "primary-model"
        assert data["to_model"] == "fallback-a"
        assert data["agent"] == "main"
        assert "secret" not in data["error"]
        # No artifact event leaks from the generic ui passthrough
        assert self._find_event(chunks, "artifact") is None

    @pytest.mark.asyncio
    async def test_a_main_lane_fallback_names_its_ui_record(self):
        # Recovery rebuilds the record from the frame when the worker dies
        # before the step that pushed it saves.
        record = {
            "type": "ui",
            "id": "ui-abc123",
            "name": "model_fallback",
            "props": {"from_model": "primary-model", "to_model": "fallback-a"},
            "metadata": {"merge": False, "tags": ["t"], "after_message_id": "m-1"},
        }

        main = self._find_event(
            await self._collect(self._make_handler(), [record]), "model_fallback"
        )
        task = self._find_event(
            await self._collect(
                self._make_handler(), [record], namespace=("task-ns-uuid",)
            ),
            "model_fallback",
        )

        assert (main["fallback_id"], main["after_message_id"]) == ("ui-abc123", "m-1")
        assert "tags" not in main
        assert not {"fallback_id", "after_message_id"} & task.keys()

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


class TestErrorEventResilienceTrace:
    def _make_handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="err-thread", run_id="r-test")

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
        parsed = handler._build_error_event(str(exc), exc=exc).data
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
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["model"] == "primary-model"

    def test_no_trace_no_new_fields(self):
        exc = RuntimeError("plain failure")
        handler = self._make_handler()
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert "model" not in parsed
        assert "attempted_models" not in parsed
        assert parsed["error_kind"] == "internal"

    def test_generic_exception_with_trace_classifies_upstream(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
        # A generic (non-SDK) exception carrying the trace is still a
        # model-call failure — it must not fall into the internal-error
        # banner path, which drops the model/attempted-models context.
        exc = RuntimeError("Error calling model 'primary-model'")
        exc.__model_resilience__ = self._trace()
        handler = self._make_handler()
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "upstream"
        # Status comes from the primary attempt record; 404 drives the hints.
        assert parsed["status_code"] == 404
        assert parsed["hints"] == ["model_access", "try_another_model"]

    def test_generic_exception_with_trace_but_no_status(self, monkeypatch):
        monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
        trace = self._trace()
        trace["attempted_models"][0]["status_code"] = None
        exc = RuntimeError("model call blew up without a status")
        exc.__model_resilience__ = trace
        handler = self._make_handler()
        parsed = handler._build_error_event(str(exc), exc=exc).data
        assert parsed["error_kind"] == "upstream"
        assert "status_code" not in parsed
        assert parsed["hints"] == [
            "api_key",
            "model_access",
            "provider_status",
            "try_another_model",
        ]


class _SlowGraph:
    """A graph whose stream outlasts a tiny wall clock."""

    def astream(self, _input_state, config=None, **_kwargs):
        async def _gen():
            while True:
                yield ((), "custom", {"type": "context_window", "action": "offload"})
                await asyncio.sleep(0.01)

        return _gen()


class TestTimeLimitStop:
    @pytest.mark.asyncio
    async def test_stop_sends_its_own_frame_and_logs_no_traceback(self, caplog):
        """The wall clock is a planned stop: one ``timeout_error`` frame, and a
        warning rather than a traceback that reads as a crash."""
        from src.server.services.runs.sse_producer import (
            RunSSEProducer,
            TurnTimeLimitError,
        )

        handler = RunSSEProducer(
            thread_id="t-limit", run_id="r-limit", workflow_timeout=0.05
        )
        frames = []
        with caplog.at_level("WARNING", logger="src.server.services.runs.sse_producer"):
            with pytest.raises(TurnTimeLimitError):
                async for frame in handler.stream_workflow(
                    _SlowGraph(), {}, {"configurable": {}}
                ):
                    frames.append(frame)
        errors = [f for f in frames if "event: error" in f]
        assert len(errors) == 1 and '"timeout_error"' in errors[0]
        producer_records = [
            r for r in caplog.records
            if r.name == "src.server.services.runs.sse_producer"
        ]
        assert any("time limit" in r.getMessage() for r in producer_records)
        assert not any(r.exc_info for r in producer_records)


class TestSanitizeErrorText:
    """Credential shapes are masked before error text reaches the wire."""

    def test_url_userinfo_stripped(self):
        from src.server.services.runs.sse_producer import _sanitize_error_text

        out = _sanitize_error_text("GET https://user:hunter2@api.example.com/v1 failed")
        assert "hunter2" not in out
        assert "https://api.example.com/v1" in out

    def test_bearer_token_masked(self):
        from src.server.services.runs.sse_producer import _sanitize_error_text

        out = _sanitize_error_text("401 with header Authorization: Bearer abc123def456xyz")
        assert "abc123def456xyz" not in out
        assert "[REDACTED]" in out

    def test_key_params_masked(self):
        from src.server.services.runs.sse_producer import _sanitize_error_text

        out = _sanitize_error_text('request had api_key="tok_0123456789abcdef" set')
        assert "tok_0123456789abcdef" not in out
        out = _sanitize_error_text("x-api-key: somelongsecretvalue1")
        assert "somelongsecretvalue1" not in out

    def test_sk_style_token_masked(self):
        from src.server.services.runs.sse_producer import _sanitize_error_text

        out = _sanitize_error_text("Invalid API key provided: sk-abcdef0123456789")
        assert "sk-abcdef0123456789" not in out

    def test_url_query_key_param_masked(self):
        from src.server.services.runs.sse_producer import _sanitize_error_text

        out = _sanitize_error_text(
            "400 for url https://example.googleapis.com/v1/models/m:generateContent"
            "?key=AIzaSyExampleExampleExample123&alt=json"
        )
        assert "AIza" not in out
        assert "?key=[REDACTED]" in out
        assert "&alt=json" in out

    def test_plain_error_text_unchanged(self):
        from src.server.services.runs.sse_producer import _sanitize_error_text

        text = "Error code: 404 - model 'some-model' not found"
        assert _sanitize_error_text(text) == text


class _MultiModeGraph:
    """LangGraph stand-in whose astream yields pre-built (ns, mode, data) tuples."""

    def __init__(self, events):
        self._events = events

    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        async def _gen():
            for event in self._events:
                yield event

        return _gen()


class TestInterruptBuffering:
    """A main-stream interrupt is buffered for the durability barrier, never
    emitted inline."""

    def _make_handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer

        return RunSSEProducer(thread_id="int-thread", run_id="r-test")

    async def _collect(self, handler, events):
        return [
            chunk
            async for chunk in handler.stream_workflow(
                _MultiModeGraph(events), input_state={}, config={}
            )
        ]

    @pytest.mark.asyncio
    async def test_main_interrupt_buffers(self):
        handler = self._make_handler()

        async def _no_verify(_graph):
            return False

        handler._verify_interrupt_durable = _no_verify
        events = [((), "updates", {"__interrupt__": [object()]})]
        await self._collect(handler, events)
        assert handler.saw_interrupt is True
        assert len(handler._pending_interrupts) == 1


def _asking_graph(asks_on: set[int]):
    """A checkpointed graph whose one node asks the user a question on the
    passes numbered in ``asks_on`` and answers on the others."""
    from langchain_core.messages import AIMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import START, MessagesState, StateGraph
    from langgraph.types import interrupt

    passes = 0

    def agent(_state):
        nonlocal passes
        passes += 1
        if passes in asks_on:
            interrupt(
                {
                    "action_requests": [
                        {
                            "type": "ask_user_question",
                            "question": f"Which filing, take {passes}?",
                            "options": ["10-K", "10-Q"],
                        }
                    ]
                }
            )
        return {"messages": [AIMessage(content="Done.")]}

    builder = StateGraph(MessagesState)
    builder.add_node("agent", agent)
    builder.add_edge(START, "agent")
    return builder.compile(checkpointer=InMemorySaver())


def _payloads(frames, event_type):
    return [
        json.loads(frame.split("data: ", 1)[1])
        for frame in frames
        if f"event: {event_type}\n" in frame
    ]


class TestSupersededInterrupt:
    """A steer waiting when the agent stops on a question re-invokes it, and
    the re-invocation supersedes the question: the checkpoint no longer holds
    it, so neither replay nor a resume can see it. The live stream carries the
    pause the checkpoint holds and nothing else."""

    async def _steered_past_a_question(self, asks_on):
        from ptc_agent.agent.middleware.background_subagent.orchestrator import (
            BackgroundSubagentOrchestrator,
        )
        from src.server.services.runs.sse_producer import RunSSEProducer

        graph = _asking_graph(asks_on)
        middleware = MagicMock()
        middleware.registry.has_pending_tasks.return_value = False
        middleware.registry._tasks = {}
        steers = iter([["user"]])

        async def _checker():
            return next(steers, [])

        handler = RunSSEProducer(thread_id="t-steer", run_id="r-steer")
        config = {"configurable": {"thread_id": "t-steer"}}
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=_checker),
        ):
            frames = [
                frame
                async for frame in handler.stream_workflow(
                    BackgroundSubagentOrchestrator(graph, middleware),
                    {"messages": [("user", "Pull the filing.")]},
                    config,
                )
            ]
        pending = [intr.id for intr in (await graph.aget_state(config)).interrupts]
        return handler, frames, pending

    @pytest.mark.asyncio
    async def test_only_the_question_asked_again_is_streamed(self):
        _handler, frames, pending = await self._steered_past_a_question({1, 2})

        assert len(pending) == 1
        streamed = [p["interrupt_id"] for p in _payloads(frames, "interrupt")]
        assert streamed == pending

    @pytest.mark.asyncio
    async def test_a_question_not_asked_again_ends_the_turn_completed(self):
        from src.server.services.runs.finalization import classify_outcome

        handler, frames, pending = await self._steered_past_a_question({1})

        assert pending == []
        assert _payloads(frames, "interrupt") == []
        assert _payloads(frames, "error") == []
        status, *_ = await classify_outcome(
            "stream_end", handler=handler, graph=None, thread_id="t-steer", error=None
        )
        assert status == "completed"


class TestReasoningWhitespaceDeltas:
    """DeepSeek (Anthropic-compatible route) streams the space before a number
    as its own thinking token: ``" "`` then ``"9"``. The SSE text must carry
    those spaces so the stream matches the checkpoint, while whitespace alone
    still never opens a reasoning section the client would render empty."""

    def _handler(self):
        from src.server.services.runs.sse_producer import RunSSEProducer
        return RunSSEProducer(thread_id="t-ws", run_id="r-test")

    def _thinking(self, text, msg_id="msg-1"):
        from langchain_core.messages import AIMessageChunk
        return AIMessageChunk(
            content=[{"type": "thinking", "thinking": text, "index": 0}],
            id=msg_id,
            response_metadata={},
        )

    def _text(self, text):
        from langchain_core.messages import AIMessageChunk
        return AIMessageChunk(
            content=[{"type": "text", "text": text, "index": 1}],
            id="msg-1",
            response_metadata={},
        )

    def _timeline(self, handler, chunks):
        async def run():
            out = []
            for chunk in chunks:
                async for frame in handler._process_message_chunk(
                    chunk, "model:main", {"langgraph_node": "model"}
                ):
                    data = frame.data
                    out.append((data.get("content_type"), data.get("content")))
            return out

        return asyncio.run(run())

    def test_leading_space_digit_deltas_round_trip(self):
        deltas = ["Today", " is", " October", " ", "9", ",", " ", "202", "6", "."]
        handler = self._handler()
        timeline = self._timeline(
            handler, [self._thinking(d) for d in deltas] + [self._text("Done")]
        )
        reasoning = "".join(c for t, c in timeline if t == "reasoning")
        assert reasoning == "Today is October 9, 2026."
        assert timeline[0] == ("reasoning_signal", "start")

    def test_whitespace_alone_does_not_open_reasoning(self):
        handler = self._handler()
        timeline = self._timeline(
            handler, [self._thinking(" "), self._thinking("\n"), self._text("Hi")]
        )
        assert timeline == [("text", "Hi")]

    def test_held_whitespace_prefixes_first_visible_reasoning(self):
        handler = self._handler()
        timeline = self._timeline(
            handler, [self._thinking(" "), self._thinking("9 apples")]
        )
        assert timeline == [
            ("reasoning_signal", "start"),
            ("reasoning", " 9 apples"),
        ]

    def test_held_whitespace_stays_with_its_message(self):
        # A stream that fails mid-way sends no finish frame, and its retry is a
        # new message that must not inherit the held whitespace.
        handler = self._handler()
        timeline = self._timeline(
            handler, [self._thinking(" "), self._thinking("Let me check", "msg-2")]
        )
        assert timeline == [
            ("reasoning_signal", "start"),
            ("reasoning", "Let me check"),
        ]
