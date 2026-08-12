"""Tests for the shared envelope helpers (mcp_servers/_envelope.py).

The status→code mapping and the sanitized-exception rule are the security
seam: raw upstream exception text (URLs carrying API keys) must never reach
an agent-visible ``detail``.
"""

import pytest

from mcp_servers._envelope import (
    error_from_exception,
    error_from_upstream,
    make_error,
    make_response,
)
from src.data_client.fmp import FMPRequestError


class TestErrorFromUpstream:
    @pytest.mark.parametrize(
        ("status", "code"),
        [
            (404, "not_found"),
            (401, "auth_failed"),
            (403, "auth_failed"),
            (429, "rate_limited"),
            (422, "invalid_argument"),
            (500, "upstream_error"),
            (None, "upstream_error"),
        ],
    )
    def test_explicit_status_maps_to_code(self, status, code):
        env = error_from_upstream("upstream failed", status=status, symbol="AAPL")
        assert env["error"] == code
        assert env["symbol"] == "AAPL"

    def test_status_parsed_from_embedded_detail(self):
        assert error_from_upstream("ginlix-data error (404): nope")["error"] == "not_found"
        assert error_from_upstream("ginlix-data error (429): slow")["error"] == "rate_limited"

    def test_explicit_status_wins_over_embedded(self):
        env = error_from_upstream("weird (404) text", status=429)
        assert env["error"] == "rate_limited"

    def test_not_configured_maps_to_client_unavailable(self):
        env = error_from_upstream("Options data requires ginlix-data (not configured).")
        assert env["error"] == "client_unavailable"

    def test_explicit_status_wins_over_not_configured(self):
        # Precedence: an explicit status beats the "not configured" substring.
        env = error_from_upstream("X not configured", status=429)
        assert env["error"] == "rate_limited"

    def test_last_embedded_status_wins(self):
        # Multiple "(NNN)" in the detail: the final match is the real failure.
        env = error_from_upstream("requested (500) rows failed: not found (404)")
        assert env["error"] == "not_found"


class TestErrorFromException:
    def test_typed_exception_contributes_message_and_status(self):
        exc = FMPRequestError("FMP API request failed (403)", status_code=403)
        env = error_from_exception(exc, "fallback", symbol="AAPL")
        assert env == {
            "error": "auth_failed",
            "detail": "FMP API request failed (403)",
            "symbol": "AAPL",
        }

    def test_generic_exception_never_contributes_text(self):
        # The security rule: a raw exception (which may stringify a URL with an
        # apikey query param) yields only the static fallback detail.
        exc = Exception("403 for url 'https://x.example/?apikey=SECRET'")
        env = error_from_exception(exc, "FMP fetch failed.", symbol="AAPL")
        assert env == {
            "error": "upstream_error",
            "detail": "FMP fetch failed.",
            "symbol": "AAPL",
        }

    def test_typed_exception_without_status_still_sanitized_upstream(self):
        exc = FMPRequestError("FMP API request timed out")
        env = error_from_exception(exc, "fallback")
        assert env["error"] == "upstream_error"
        assert env["detail"] == "FMP API request timed out"


class TestAutoCount:
    def test_list_and_dict_of_lists(self):
        assert make_response([1, 2, 3], source="s")["count"] == 3
        assert make_response({"a": [1], "b": [2, 3]}, source="s")["count"] == 3

    def test_ambiguous_shapes_count_as_one(self):
        # Documented sharp edge: non-list-valued dicts are 1 — callers with
        # multi-record payloads in those shapes must pass count= explicitly.
        assert make_response({"a": {"x": 1}}, source="s")["count"] == 1
        assert make_response({"a": {"x": 1}}, source="s", count=5)["count"] == 5

    def test_empty_dict_and_dict_of_empty_lists_count_zero(self):
        # Regression: an empty dict (and a dict of only-empty lists) used to count 1.
        assert make_response({}, source="s")["count"] == 0
        assert make_response({"a": [], "b": []}, source="s")["count"] == 0


class TestMakeError:
    def test_off_contract_code_coerces_to_upstream_error(self):
        assert make_error("not_a_real_code", "d")["error"] == "upstream_error"

    def test_contract_code_passes_through(self):
        env = make_error("not_found", "d")
        assert env["error"] == "not_found"
        assert env["detail"] == "d"
