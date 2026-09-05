"""Tests for LLM error status extraction and retryability classification."""

import pytest

from src.llms.error_classification import (
    NON_RETRYABLE_STATUSES,
    extract_status_code,
    is_retryable_error,
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
