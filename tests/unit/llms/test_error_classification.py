"""Tests for LLM error status extraction and retryability classification."""

import httpx
import pytest

from langchain_openai.chat_models._client_utils import StreamChunkTimeoutError

from src.llms.error_classification import (
    NON_RETRYABLE_STATUSES,
    extract_status_code,
    is_context_overflow,
    is_programming_error,
    is_retryable_error,
    is_stalled_stream,
)


class _FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class TestExtractStatusCode:
    def test_direct_status_code_attribute(self):
        exc = Exception("boom")
        exc.status_code = 429
        assert extract_status_code(exc) == 429

    def test_response_status_code_attribute(self):
        exc = Exception("boom")
        exc.response = _FakeResponse(404)
        assert extract_status_code(exc) == 404

    def test_status_found_via_cause_chain(self):
        inner = Exception("provider said no")
        inner.status_code = 401
        try:
            try:
                raise inner
            except Exception as e:
                raise RuntimeError("model call failed") from e
        except RuntimeError as outer:
            assert extract_status_code(outer) == 401

    def test_regex_fallback_on_message(self):
        exc = Exception("Error code: 404 - {'error': {'message': 'model not found'}}")
        assert extract_status_code(exc) == 404

    def test_regex_fallback_on_inner_message_via_chain(self):
        try:
            try:
                raise Exception("upstream returned HTTP 503, backing off")
            except Exception as e:
                raise RuntimeError("wrapped") from e
        except RuntimeError as outer:
            assert extract_status_code(outer) == 503

    def test_attribute_wins_over_message(self):
        exc = Exception("mentions 500 in text")
        exc.status_code = 429
        assert extract_status_code(exc) == 429

    def test_no_status_returns_none(self):
        assert extract_status_code(Exception("connection reset by peer")) is None

    def test_non_int_status_attribute_ignored(self):
        exc = Exception("boom")
        exc.status_code = "not-a-code"
        assert extract_status_code(exc) is None

    def test_self_referential_context_does_not_loop(self):
        exc = Exception("cyclic")
        exc.__context__ = exc
        assert extract_status_code(exc) is None


class TestIsRetryableError:
    @pytest.mark.parametrize("status", sorted(NON_RETRYABLE_STATUSES))
    def test_non_retryable_statuses(self, status):
        exc = Exception("boom")
        exc.status_code = status
        assert is_retryable_error(exc) is False

    @pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 529])
    def test_transient_statuses_retryable(self, status):
        exc = Exception("boom")
        exc.status_code = status
        assert is_retryable_error(exc) is True

    def test_no_status_is_retryable(self):
        assert is_retryable_error(Exception("connection reset")) is True

    def test_precomputed_status_short_circuits_extraction(self):
        # When the caller already extracted the status, the exception content
        # must not matter.
        exc = Exception("Error code: 500")
        assert is_retryable_error(exc, status_code=400) is False


def _chained(outer, inner):
    try:
        try:
            raise inner
        except Exception as e:
            raise outer from e
    except Exception as wrapped:
        return wrapped


class TestProgrammingErrors:
    """A crash in parsing is the same on every attempt, so it is not retried."""

    @pytest.mark.parametrize(
        "exc",
        [
            AttributeError("'dict' object has no attribute 'model_dump'"),
            TypeError("'NoneType' object is not iterable"),
            KeyError("choices"),
        ],
    )
    def test_not_retryable(self, exc):
        assert is_programming_error(exc)
        assert is_retryable_error(exc) is False

    def test_found_anywhere_in_the_chain(self):
        # The SDKs wrap whatever their transport raised in a connection error.
        import anthropic
        import httpx

        request = httpx.Request("POST", "https://example.test/v1/messages")
        wrapped = _chained(
            anthropic.APIConnectionError(request=request),
            AttributeError("'dict' object has no attribute 'model_dump'"),
        )
        assert is_retryable_error(wrapped) is False
        assert is_retryable_error(_chained(RuntimeError("model call failed"), KeyError("x"))) is False

    def test_as_the_implicit_context_too(self):
        try:
            try:
                raise TypeError("bad")
            except TypeError:
                raise RuntimeError("while handling")
        except RuntimeError as outer:
            assert is_retryable_error(outer) is False

    def test_a_reported_status_still_decides(self):
        exc = _chained(Exception("overloaded"), KeyError("error"))
        exc.status_code = 529
        assert is_retryable_error(exc) is True

        crash = TypeError("bad")
        crash.status_code = 400
        assert is_retryable_error(crash) is False

    def test_a_cycle_does_not_loop(self):
        exc = KeyError("x")
        exc.__context__ = exc
        assert is_retryable_error(exc) is False

    def test_network_failures_stay_retryable(self):
        import httpx

        request = httpx.Request("POST", "https://example.test/v1/messages")
        for exc in (
            httpx.ConnectError("refused", request=request),
            httpx.ReadTimeout("slow", request=request),
            httpx.RemoteProtocolError("closed mid-stream", request=request),
            ConnectionResetError("reset by peer"),
            TimeoutError("timed out"),
            StreamChunkTimeoutError(120.0, chunks_received=3),
        ):
            assert not is_programming_error(exc), type(exc)
            assert is_retryable_error(exc) is True, type(exc)

    def test_sdk_connection_errors_stay_retryable(self):
        # Built the way each SDK builds them around a transport failure.
        import anthropic
        import httpx
        import openai

        request = httpx.Request("POST", "https://example.test/v1/messages")
        transport = httpx.ConnectError("refused", request=request)
        for sdk in (anthropic, openai):
            for outer in (sdk.APIConnectionError(request=request), sdk.APITimeoutError(request=request)):
                exc = _chained(outer, transport)
                assert is_retryable_error(exc) is True, type(outer)


class TestDashScopeFailureStatus:
    """The adapter's verdict, and the message-regex it is there to pre-empt.

    Every payload below is verbatim from the live provider. Two fields are
    needed to read them: the code says ``server_error`` for a permanent content
    block, and the message carries no status at all for an unsupported model.
    """

    LIVE_TERMINAL = [
        (
            "server_error",
            "<400> InternalError.Algo.DataInspectionFailed: Input text data "
            "may contain inappropriate content.",
        ),
        ("InvalidParameter", "Unsupported model: 'qwen-does-not-exist'"),
        (
            "InvalidParameter",
            "<400> InternalError.Algo.InvalidParameter: The provided URL does "
            "not appear to be valid.",
        ),
    ]

    @staticmethod
    def _error(code, message):
        from src.llms.extension.dashscope import ResponsesStreamFailedError, _status_for_failure_code

        return ResponsesStreamFailedError(
            f"Responses stream failed ({code}): {message}",
            code=code,
            status_code=_status_for_failure_code(code, message),
        )

    @pytest.mark.parametrize("code,message", LIVE_TERMINAL)
    def test_live_terminal_failures_are_not_retried(self, code, message):
        assert is_retryable_error(self._error(code, message)) is False

    def test_unsupported_model_needs_the_code_not_the_message(self):
        # The message has no status token, so the regex alone reads this as
        # retryable and spends the full ladder on a model that cannot exist.
        code, message = "InvalidParameter", "Unsupported model: 'qwen-does-not-exist'"
        assert extract_status_code(Exception(message)) is None
        assert extract_status_code(self._error(code, message)) == 400

    def test_rate_limit_is_not_decided_by_a_number_in_its_own_message(self):
        exc = self._error("rate_limit_exceeded", "Request quota is 400 tokens per minute")
        assert extract_status_code(exc) == 429
        assert is_retryable_error(exc) is True

    @pytest.mark.parametrize(
        "code,message",
        [
            ("server_error", "upstream temporarily unavailable"),
            ("SomethingUnrecognised", "no digits here"),
        ],
    )
    def test_unknown_verdict_falls_back_to_the_message(self, code, message):
        # None means "I do not know", which must leave the older guess intact
        # rather than assert a wrong status.
        assert self._error(code, message).status_code is None
        assert is_retryable_error(self._error(code, message)) is True


class TestStreamTimeouts:
    def test_chunk_count_in_a_timeout_message_is_not_a_status(self):
        exc = StreamChunkTimeoutError(120.0, model_name="m", chunks_received=413)
        assert extract_status_code(exc) is None
        assert is_retryable_error(exc)

    def test_a_timeout_wrapped_by_another_error_is_still_skipped(self):
        try:
            try:
                raise StreamChunkTimeoutError(120.0, chunks_received=422)
            except TimeoutError as e:
                raise RuntimeError("model call failed") from e
        except RuntimeError as outer:
            assert extract_status_code(outer) is None

    def test_stalled_whether_or_not_the_stream_had_started(self):
        # The bound also covers the wait for the first chunk, so a stream that
        # never started costs each attempt the whole bound as well.
        assert is_stalled_stream(StreamChunkTimeoutError(120.0, chunks_received=1))
        assert is_stalled_stream(StreamChunkTimeoutError(120.0, chunks_received=0))
        assert not is_stalled_stream(TimeoutError("read timed out"))
        assert not is_stalled_stream(Exception("chunks_received=5"))


class TestContextOverflow:
    """Each message is one a provider sent for a request over its window."""

    @pytest.mark.parametrize(
        "message",
        [
            # DeepSeek, on its Anthropic route too
            "Error code: 400 - {'error': {'message': \"This model's maximum context length "
            "is 1048576 tokens. However, you requested 2514832 tokens\", 'type': "
            "'invalid_request_error', 'code': 'invalid_request_error'}}",
            # Moonshot
            "Invalid request: Your request exceeded model token limit: 262144 (requested: 428479)",
            # DashScope chat
            "<400> InternalError.Algo.InvalidParameter: Range of input length should be [1, 229376]",
            # Volcengine Ark
            "Total tokens of image and text exceed max message tokens. Request id: x",
            # MiniMax
            "invalid params, context window exceeds limit (2013)",
            # Z.ai's own API, in both wordings
            "Error code: 400 - {'error': {'code': '1261', 'message': 'Prompt exceeds max length'}}",
            "Prompt 超长",
            # Anthropic and its compatible endpoints
            "prompt is too long: 250000 tokens > 200000 maximum",
            # Anthropic, when the input fits but leaves no room for max_tokens
            "Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', "
            "'message': 'input length and `max_tokens` exceed context limit: 188240 + 21333 "
            "> 200000, decrease input length or `max_tokens` and try again'}}",
            # Gemini
            "The input token count exceeds the maximum number of tokens allowed (65536).",
            # OpenAI Responses, reported inside the stream
            "Your input exceeds the context window of this model.",
        ],
    )
    def test_each_provider_wording_is_an_overflow(self, message):
        assert is_context_overflow(Exception(message))

    def test_langchains_own_classification_is_trusted(self):
        from langchain_core.exceptions import ContextOverflowError

        assert is_context_overflow(ContextOverflowError("anything"))

    def test_the_code_in_an_sdk_body_is_enough(self):
        exc = Exception("Bad request")
        exc.body = {"error": {"code": "context_length_exceeded", "message": "..."}}
        assert is_context_overflow(exc)

    def test_a_payload_the_provider_refused_as_too_large(self):
        exc = Exception("Request Entity Too Large")
        exc.status_code = 413
        assert is_context_overflow(exc)

    def test_a_413_set_only_on_the_response(self):
        # httpx.HTTPStatusError carries its status on the response alone.
        request = httpx.Request("POST", "https://example.test/v1/messages")
        exc = httpx.HTTPStatusError(
            "Request Entity Too Large",
            request=request,
            response=httpx.Response(413, request=request),
        )
        assert is_context_overflow(exc)

    def test_found_through_the_cause_chain(self):
        try:
            try:
                raise Exception("prompt is too long")
            except Exception as inner:
                raise RuntimeError("model call failed") from inner
        except RuntimeError as outer:
            assert is_context_overflow(outer)

    @pytest.mark.parametrize(
        "exc",
        [
            Exception("tools.0.input_schema: unsupported keyword"),
            Exception("Rate limit reached for requests"),
            Exception("413 mentioned in prose is not the status"),
            StreamChunkTimeoutError(120.0, chunks_received=413),
        ],
    )
    def test_other_failures_are_not(self, exc):
        assert not is_context_overflow(exc)
