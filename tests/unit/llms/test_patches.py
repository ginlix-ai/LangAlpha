"""Tests for upstream library monkey-patches."""

import pytest
from pydantic import BaseModel

# Apply the patch before importing _create_usage_metadata
from src.llms.patches import (
    _patch_langchain_anthropic_context_management,
    _patch_langchain_anthropic_usage_metadata,
)
_patch_langchain_anthropic_usage_metadata()
_patch_langchain_anthropic_context_management()


class MockCacheCreation(BaseModel):
    ephemeral_5m_input_tokens: int = 0
    ephemeral_1h_input_tokens: int = 0


class MockUsage(BaseModel):
    input_tokens: int = 100
    output_tokens: int = 50
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_creation: MockCacheCreation = None

    model_config = {"arbitrary_types_allowed": True}


class TestPatchLangchainAnthropicUsageMetadata:
    """Test the _create_usage_metadata monkey-patch."""

    def test_none_cache_creation_fields_dont_crash(self):
        """Reproduces the original TypeError: int += None."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        # Simulate what third-party providers return: cache_creation with None values
        usage = MockUsage(
            cache_creation=MockCacheCreation(ephemeral_5m_input_tokens=0, ephemeral_1h_input_tokens=0)
        )
        # Force None values like the Anthropic SDK's lenient parsing does
        object.__setattr__(usage.cache_creation, "ephemeral_5m_input_tokens", None)
        object.__setattr__(usage.cache_creation, "ephemeral_1h_input_tokens", None)

        # This would raise TypeError without the patch
        result = _create_usage_metadata(usage)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50

    def test_normal_cache_creation_still_works(self):
        """Normal cache_creation values should pass through correctly."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(
            input_tokens=100,
            output_tokens=50,
            cache_creation=MockCacheCreation(ephemeral_5m_input_tokens=10, ephemeral_1h_input_tokens=20),
        )

        result = _create_usage_metadata(usage)
        assert result["output_tokens"] == 50
        input_details = result.get("input_token_details", {})
        assert input_details.get("ephemeral_5m_input_tokens") == 10
        assert input_details.get("ephemeral_1h_input_tokens") == 20

    def test_dict_cache_creation_with_none_fields(self):
        """cache_creation as a plain dict with None values should not crash."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(input_tokens=100, output_tokens=50)
        # Simulate cache_creation arriving as a plain dict (some providers do this)
        object.__setattr__(
            usage,
            "cache_creation",
            {"ephemeral_5m_input_tokens": None, "ephemeral_1h_input_tokens": None},
        )

        result = _create_usage_metadata(usage)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50

    def test_dict_cache_creation_with_valid_values(self):
        """cache_creation as a plain dict with valid int values should work."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(input_tokens=100, output_tokens=50)
        object.__setattr__(
            usage,
            "cache_creation",
            {"ephemeral_5m_input_tokens": 10, "ephemeral_1h_input_tokens": 20},
        )

        result = _create_usage_metadata(usage)
        assert result["output_tokens"] == 50
        input_details = result.get("input_token_details", {})
        assert input_details.get("ephemeral_5m_input_tokens") == 10
        assert input_details.get("ephemeral_1h_input_tokens") == 20

    def test_no_cache_creation_still_works(self):
        """Usage without cache_creation should work normally."""
        from langchain_anthropic.chat_models import _create_usage_metadata

        usage = MockUsage(input_tokens=100, output_tokens=50)

        result = _create_usage_metadata(usage)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50

    def test_patch_is_applied(self):
        """Verify the patch was applied to langchain_anthropic."""
        import langchain_anthropic.chat_models as chat_models

        # The patched function should be our wrapper, not the original
        assert "patched" in chat_models._create_usage_metadata.__name__


def _message_delta(**extra):
    """Built the way the SDK parses a stream line, so unknown keys land in extras."""
    from anthropic._models import construct_type
    from anthropic.types import RawMessageStreamEvent

    return construct_type(
        type_=RawMessageStreamEvent,
        value={
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": None},
            "usage": {"output_tokens": 5, "input_tokens": 10},
            **extra,
        },
    )


def _chunk_from(event):
    from langchain_anthropic import ChatAnthropic

    llm = ChatAnthropic(model="claude-sonnet-4-5", api_key="test-key")
    chunk, _ = llm._make_message_chunk_from_anthropic_event(
        event, stream_usage=True, coerce_content_to_string=False
    )
    return chunk


class TestPatchLangchainAnthropicContextManagement:
    """A proxy's ``context_management`` arrives in extras as a dict, not a model."""

    def test_dict_context_management_does_not_crash(self):
        event = _message_delta(context_management={"applied_edits": []})
        assert isinstance(event.context_management, dict)

        chunk = _chunk_from(event)

        assert chunk.response_metadata["stop_reason"] == "end_turn"
        assert chunk.response_metadata["context_management"] == {"applied_edits": []}
        assert chunk.usage_metadata["output_tokens"] == 5

    def test_event_is_left_untouched(self):
        event = _message_delta(context_management={"applied_edits": []})

        _chunk_from(event)

        assert event.context_management == {"applied_edits": []}

    def test_non_dict_value_is_dropped(self):
        chunk = _chunk_from(_message_delta(context_management="unexpected"))

        assert chunk.response_metadata["stop_reason"] == "end_turn"
        assert "context_management" not in chunk.response_metadata

    def test_absent_or_null_field_is_unchanged(self):
        for extra in ({}, {"context_management": None}):
            chunk = _chunk_from(_message_delta(**extra))
            assert "context_management" not in chunk.response_metadata

    def test_model_valued_field_takes_the_upstream_path(self):
        """Once the SDK models the field, the shim must stand aside."""

        class Managed(BaseModel):
            applied_edits: list = []

        event = _message_delta()
        event.__pydantic_extra__["context_management"] = Managed(applied_edits=["x"])

        chunk = _chunk_from(event)

        assert chunk.response_metadata["context_management"] == {"applied_edits": ["x"]}

    def test_patch_is_idempotent(self):
        from langchain_anthropic import ChatAnthropic

        installed = ChatAnthropic._make_message_chunk_from_anthropic_event
        _patch_langchain_anthropic_context_management()

        assert ChatAnthropic._make_message_chunk_from_anthropic_event is installed


def _iterated_usage():
    """A compaction response's usage: zero at the top, billed per iteration."""
    from anthropic.types.beta import BetaUsage

    return BetaUsage.model_validate(
        {
            "input_tokens": 0,
            "output_tokens": 0,
            "iterations": [
                {
                    "type": "compaction",
                    "input_tokens": 100,
                    "output_tokens": 20,
                    "cache_read_input_tokens": 30,
                    "cache_creation_input_tokens": 10,
                    "cache_creation": {
                        "ephemeral_5m_input_tokens": 4,
                        "ephemeral_1h_input_tokens": 6,
                    },
                },
                {
                    "type": "message",
                    "model": "claude-test",
                    "input_tokens": 5,
                    "output_tokens": 7,
                    "cache_read_input_tokens": 1,
                    "cache_creation_input_tokens": 2,
                    "cache_creation": {
                        "ephemeral_5m_input_tokens": 2,
                        "ephemeral_1h_input_tokens": 0,
                    },
                },
            ],
        }
    )


class TestBilledUsage:
    """A response that ran more than one iteration is billed for their sum."""

    def test_sums_thinking_tokens(self):
        from types import SimpleNamespace

        from src.llms.patches import _billed_usage

        usage = SimpleNamespace(
            iterations=[
                {"input_tokens": 10, "output_tokens": 5, "output_tokens_details": {"thinking_tokens": 3}},
                {"input_tokens": 1, "output_tokens": 2, "output_tokens_details": {"thinking_tokens": 1}},
            ]
        )
        assert _billed_usage(usage).output_tokens_details.thinking_tokens == 4

    def test_no_thinking_details_stay_absent(self):
        from types import SimpleNamespace

        from src.llms.patches import _billed_usage

        usage = SimpleNamespace(iterations=[{"input_tokens": 1, "output_tokens": 1}])
        assert _billed_usage(usage).output_tokens_details is None

    def test_sums_sdk_iterations(self):
        from src.llms.patches import _billed_usage

        billed = _billed_usage(_iterated_usage())

        assert billed.input_tokens == 105
        assert billed.output_tokens == 27
        assert billed.cache_read_input_tokens == 31
        assert billed.cache_creation_input_tokens == 12
        assert billed.cache_creation == {
            "ephemeral_5m_input_tokens": 6,
            "ephemeral_1h_input_tokens": 6,
        }

    def test_sums_dict_iterations_with_missing_fields(self):
        from types import SimpleNamespace

        from src.llms.patches import _billed_usage

        usage = SimpleNamespace(
            input_tokens=0,
            output_tokens=0,
            iterations=[
                {"input_tokens": 3, "output_tokens": 4, "cache_creation": None},
                {
                    "input_tokens": 5,
                    "output_tokens": None,
                    "cache_read_input_tokens": 9,
                    "cache_creation": {"ephemeral_1h_input_tokens": 8},
                },
            ],
        )

        billed = _billed_usage(usage)

        assert (billed.input_tokens, billed.output_tokens) == (8, 4)
        assert billed.cache_read_input_tokens == 9
        assert billed.cache_creation_input_tokens == 0
        assert billed.cache_creation == {
            "ephemeral_5m_input_tokens": 0,
            "ephemeral_1h_input_tokens": 8,
        }

    @pytest.mark.parametrize("iterations", [None, []])
    def test_without_iterations_the_usage_is_returned_as_is(self, iterations):
        from src.llms.patches import _billed_usage

        usage = MockUsage()
        object.__setattr__(usage, "iterations", iterations)
        assert _billed_usage(usage) is usage
        assert _billed_usage(MockUsage()).__class__ is MockUsage

    def test_usage_metadata_reports_the_billed_sum(self):
        from langchain_anthropic.chat_models import _create_usage_metadata

        result = _create_usage_metadata(_iterated_usage())

        # input_tokens + cache reads + the split cache writes, over both iterations
        assert result["input_tokens"] == 105 + 31 + 12
        assert result["output_tokens"] == 27
        details = result["input_token_details"]
        assert details["cache_read"] == 31
        assert details["ephemeral_5m_input_tokens"] == 6
        assert details["ephemeral_1h_input_tokens"] == 6


class TestCompactionBlockStream:
    """A streamed compaction block arrives whole on its start event, with no
    deltas after it; the chunk keeps it, with the index the stream merges by."""

    @staticmethod
    def _client():
        from langchain_anthropic import ChatAnthropic

        from src.llms.patches import _patch_langchain_anthropic_compaction_block

        _patch_langchain_anthropic_compaction_block()
        return ChatAnthropic(model="claude-test", api_key="x")

    def test_a_compaction_block_start_becomes_a_chunk(self):
        from anthropic.types.beta import BetaRawContentBlockStartEvent
        from langchain_core.messages import AIMessageChunk

        event = BetaRawContentBlockStartEvent.model_validate(
            {
                "type": "content_block_start",
                "index": 2,
                "content_block": {"type": "compaction", "content": "what happened", "signature": "sig"},
            }
        )

        chunk, start = self._client()._make_message_chunk_from_anthropic_event(
            event, coerce_content_to_string=False
        )

        assert isinstance(chunk, AIMessageChunk)
        assert chunk.content == [
            {"type": "compaction", "content": "what happened", "signature": "sig", "index": 2}
        ]
        assert start is event

    def test_other_events_go_through_upstream(self):
        from anthropic.types import RawContentBlockStartEvent

        event = RawContentBlockStartEvent.model_validate(
            {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": "hi"}}
        )

        chunk, _ = self._client()._make_message_chunk_from_anthropic_event(
            event, coerce_content_to_string=False
        )

        assert chunk.content[0]["type"] == "text"

    def test_the_patch_is_applied_once(self):
        from langchain_anthropic import ChatAnthropic

        from src.llms.patches import _patch_langchain_anthropic_compaction_block

        self._client()
        patched = ChatAnthropic._make_message_chunk_from_anthropic_event
        _patch_langchain_anthropic_compaction_block()
        assert ChatAnthropic._make_message_chunk_from_anthropic_event is patched
        assert getattr(patched, "_keeps_compaction_blocks", False)

    def test_the_context_management_patch_finds_itself_beneath_it(self):
        """Installed again over this one, it must see its own marker and stand
        aside rather than wrap the method a second time."""
        from langchain_anthropic import ChatAnthropic

        self._client()
        installed = ChatAnthropic._make_message_chunk_from_anthropic_event
        _patch_langchain_anthropic_context_management()
        assert ChatAnthropic._make_message_chunk_from_anthropic_event is installed

    def test_the_context_management_patch_still_runs_beneath_it(self):
        """Both wrap the same method; each must install whatever the other did."""
        chunk, _ = self._client()._make_message_chunk_from_anthropic_event(
            _message_delta(context_management={"applied_edits": []}),
            stream_usage=True,
            coerce_content_to_string=False,
        )

        assert chunk.response_metadata["context_management"] == {"applied_edits": []}


class TestHeaderBetas:
    """A request that names betas keeps the client's own: they replace its
    header, which on the OAuth route authenticates the call."""

    HEADER = "oauth-2025-04-20,thinking-binding-controls-2026-08-01"

    def _client(self):
        from langchain_anthropic import ChatAnthropic

        from src.llms.patches import _patch_langchain_anthropic_header_betas

        _patch_langchain_anthropic_header_betas()
        return ChatAnthropic(
            model="claude-opus-5-5", api_key="sk-ant-test", default_headers={"anthropic-beta": self.HEADER}
        )

    def test_a_call_time_list_keeps_the_header(self):
        from langchain_core.messages import HumanMessage

        payload = self._client()._get_request_payload([HumanMessage("hi")], betas=["inline-tools-2026-09-15"])
        assert payload["betas"] == [*self.HEADER.split(","), "inline-tools-2026-09-15"]

    def test_a_beta_langchain_adds_keeps_the_header(self):
        from langchain_core.messages import HumanMessage

        server = {"type": "url", "url": "https://mcp.example.com/sse", "name": "example"}
        payload = self._client()._get_request_payload([HumanMessage("hi")], mcp_servers=[server])
        assert payload["betas"] == [*self.HEADER.split(","), "mcp-client-2025-11-20"]

    def test_a_request_naming_no_betas_goes_out_on_the_header(self):
        from langchain_core.messages import HumanMessage

        payload = self._client()._get_request_payload([HumanMessage("hi")])
        assert not payload.get("betas")


class TestOpenAICompactionTrigger:
    """The compaction marker becomes the trigger item OpenAI compacts on, in
    the request the model builds anyway, Codex's included."""

    _MARKER = {"compaction": {"type": "summarize", "instructions": "p"}}

    @pytest.fixture(autouse=True)
    def _patched(self, monkeypatch):
        for var in ("OPENAI_BASE_URL", "OPENAI_API_BASE"):
            monkeypatch.delenv(var, raising=False)
        from src.llms.patches import _patch_langchain_openai_compaction_trigger

        _patch_langchain_openai_compaction_trigger()

    def _payload(self, model, **kwargs):
        from langchain_core.messages import HumanMessage, SystemMessage

        return model._get_request_payload([SystemMessage("sys"), HumanMessage("q")], **kwargs)

    def _gpt(self, **kwargs):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model="gpt-test", api_key="x", output_version="responses/v1", **kwargs)

    def test_the_marker_becomes_the_last_input_item(self):
        payload = self._payload(
            self._gpt(), extra_body={**self._MARKER, "reasoning_extra": 1}
        )

        assert payload["input"][-1] == {"type": "compaction_trigger"}
        assert payload["extra_body"] == {"reasoning_extra": 1}

    def test_a_marker_alone_leaves_no_extra_body(self):
        payload = self._payload(self._gpt(), extra_body=self._MARKER)

        assert "extra_body" not in payload
        assert payload["input"][-1] == {"type": "compaction_trigger"}

    def test_codex_keeps_its_instructions_and_gets_the_trigger(self):
        from src.llms.extension.codex import ChatCodexOpenAI

        codex = ChatCodexOpenAI(
            model="gpt-test",
            api_key="x",
            output_version="responses/v1",
            base_url="https://chatgpt.com/backend-api/codex",
        )
        payload = self._payload(codex, extra_body=self._MARKER)

        assert payload["instructions"] == "sys"
        assert payload["input"][-1] == {"type": "compaction_trigger"}

    @pytest.mark.parametrize("extra_body", [None, {"reasoning_extra": 1}])
    def test_any_other_request_is_untouched(self, extra_body):
        payload = self._payload(self._gpt(), **({"extra_body": extra_body} if extra_body else {}))

        assert all(item.get("type") != "compaction_trigger" for item in payload["input"])

    def test_chat_completions_is_untouched(self):
        from langchain_openai import ChatOpenAI

        model = ChatOpenAI(model="gpt-test", api_key="x")
        payload = self._payload(model, extra_body=self._MARKER)

        assert "input" not in payload
        assert payload["extra_body"] == self._MARKER


class TestOpenAIReasoningStatus:
    """A streamed reasoning item is stored ``in_progress``; Kimi Code refuses
    one sent back that way, so it goes out ``completed``."""

    @pytest.fixture(autouse=True)
    def _patched(self, monkeypatch):
        for var in ("OPENAI_BASE_URL", "OPENAI_API_BASE"):
            monkeypatch.delenv(var, raising=False)
        from src.llms.patches import _patch_langchain_openai_reasoning_status

        _patch_langchain_openai_reasoning_status()

    def _sent(self, reasoning):
        from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
        from langchain_openai import ChatOpenAI

        model = ChatOpenAI(model="k3.1-pro", api_key="x", output_version="responses/v1")
        call = {"type": "function_call", "call_id": "c1", "name": "f", "arguments": "{}"}
        history = [
            HumanMessage("q"),
            AIMessage(
                content=[reasoning, call],
                tool_calls=[{"id": "c1", "name": "f", "args": {}}],
            ),
            ToolMessage("1", tool_call_id="c1"),
        ]
        items = model._get_request_payload(history)["input"]
        return [item for item in items if item.get("type") == "reasoning"]

    def test_a_streamed_item_goes_out_completed(self):
        streamed = {"type": "reasoning", "id": "rs_1", "status": "in_progress", "summary": []}

        assert [item["status"] for item in self._sent(streamed)] == ["completed"]

    def test_an_item_without_a_status_gets_none(self):
        unstreamed = {"type": "reasoning", "id": "rs_1", "summary": []}

        assert ["status" in item for item in self._sent(unstreamed)] == [False]


class TestInputTransformations:
    """The API's list of the earlier thinking it dropped reaches a streamed
    answer. A non-streamed one keeps it through ``llm_output``, which the
    Wire's scenarios cover end to end."""

    ENTRIES = [
        {"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"},
        {"type": "thinking_mismatch_allowed", "path": "messages.3.content.0", "reason": "a_later_reason"},
    ]

    @pytest.fixture(autouse=True)
    def _patched(self):
        from src.llms.patches import _patch_langchain_anthropic_input_transformations

        _patch_langchain_anthropic_input_transformations()

    @staticmethod
    def _model():
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model="claude-opus-5-5", api_key="sk-ant-test-placeholder")

    @staticmethod
    def _message(**extra):
        from anthropic._models import construct_type
        from anthropic.types import Message

        body = {
            "id": "msg_01",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-5-5",
            "content": [
                {"type": "thinking", "thinking": "", "signature": "sig-1"},
                {"type": "text", "text": "ok"},
            ],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 10, "output_tokens": 2},
            **extra,
        }
        return construct_type(type_=Message, value=body)

    def _start(self, **extra):
        from anthropic._models import construct_type
        from anthropic.types import RawMessageStreamEvent

        message = self._message(**extra).model_dump()
        message.update(content=[], stop_reason=None)
        return construct_type(
            type_=RawMessageStreamEvent, value={"type": "message_start", "message": message}
        )

    @pytest.mark.parametrize("entries", [ENTRIES, []])
    def test_a_stream_keeps_the_list_from_message_start(self, entries):
        chunk, _ = self._model()._make_message_chunk_from_anthropic_event(
            self._start(input_transformations=entries), coerce_content_to_string=False
        )
        assert chunk.response_metadata["input_transformations"] == entries
        assert chunk.response_metadata["model_name"] == "claude-opus-5-5"

    def test_a_stream_without_the_list_is_unchanged(self):
        chunk, _ = self._model()._make_message_chunk_from_anthropic_event(
            self._start(), coerce_content_to_string=False
        )
        assert "input_transformations" not in chunk.response_metadata

    def test_the_patch_is_applied_once_and_keeps_the_others_beneath_it(self):
        from langchain_anthropic.chat_models import ChatAnthropic

        from src.llms.patches import (
            _patch_langchain_anthropic_compaction_block,
            _patch_langchain_anthropic_context_management,
            _patch_langchain_anthropic_input_transformations,
        )

        on_event = ChatAnthropic._make_message_chunk_from_anthropic_event
        _patch_langchain_anthropic_input_transformations()
        _patch_langchain_anthropic_context_management()
        _patch_langchain_anthropic_compaction_block()
        assert ChatAnthropic._make_message_chunk_from_anthropic_event is on_event
