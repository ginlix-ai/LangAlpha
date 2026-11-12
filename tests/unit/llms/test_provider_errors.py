"""Why a provider refused a call, read from what it sent: each case is a body
seen in production or the shape the provider's own client parses."""

from datetime import datetime, timezone

import anthropic
import httpx
import openai
import pytest

from src.llms.claude_code import ClaudeTurnFailed
from src.llms.provider_errors import is_terminal_refusal, read_refusal

_REQUEST = httpx.Request("POST", "https://provider.example/v1/responses")
RESET = 1793116811


def _openai(status: int, body: dict, headers: dict | None = None) -> openai.APIStatusError:
    response = httpx.Response(status, request=_REQUEST, json=body, headers=headers or {})
    cls = openai.RateLimitError if status == 429 else openai.APIStatusError
    return cls(f"Error code: {status}", response=response, body=body)


def _anthropic(status: int, body: dict, headers: dict | None = None) -> anthropic.APIStatusError:
    response = httpx.Response(status, request=_REQUEST, json=body, headers=headers or {})
    cls = anthropic.RateLimitError if status == 429 else anthropic.APIStatusError
    return cls(f"Error code: {status}", response=response, body=body)


_CODEX_LIMIT = {"error": {
    "type": "usage_limit_reached", "message": "The usage limit has been reached", "plan_type": "plus",
    "resets_at": RESET, "limit_window_minutes": 300, "resets_in_seconds": 2625,
}}


def test_a_codex_plan_limit_names_its_plan_window_and_reset():
    refusal = read_refusal(_openai(429, _CODEX_LIMIT))
    assert refusal.reason == "plan_limit"
    assert (refusal.tier, refusal.window) == ("plus", "five_hour")
    assert refusal.until == datetime.fromtimestamp(RESET, tz=timezone.utc)


def test_a_codex_body_without_its_plan_or_reset_reads_the_headers():
    """Every Codex response carries the plan and the window's reset (seen live)."""
    exc = _openai(429, {"error": {"type": "usage_limit_reached"}}, {
        "x-codex-plan-type": "prolite", "x-codex-primary-window-minutes": "10080",
        "x-codex-primary-reset-at": str(RESET), "x-codex-primary-used-percent": "100",
    })
    refusal = read_refusal(exc)
    assert (refusal.tier, refusal.window) == ("prolite", "seven_day")
    assert refusal.until == datetime.fromtimestamp(RESET, tz=timezone.utc)


def test_a_codex_header_reset_counts_only_for_the_window_the_body_named():
    """A weekly refusal takes the secondary window's reset, never the 5-hour one."""
    weekly = {"error": {"type": "usage_limit_reached", "limit_window_minutes": 10080}}
    slots = {
        "x-codex-primary-window-minutes": "300", "x-codex-primary-reset-at": "1793100000",
        "x-codex-secondary-window-minutes": "10080", "x-codex-secondary-reset-at": str(RESET),
    }
    refusal = read_refusal(_openai(429, weekly, slots))
    assert (refusal.window, refusal.until) == ("seven_day", datetime.fromtimestamp(RESET, tz=timezone.utc))
    unmatched = {k: v for k, v in slots.items() if not k.startswith("x-codex-secondary")}
    assert read_refusal(_openai(429, weekly, unmatched)).until is None


def test_a_codex_body_naming_no_window_takes_the_one_its_headers_say_is_spent():
    """Without the body's window, the used-up one decides; nothing used up, nothing told."""
    plain = {"error": {"type": "usage_limit_reached"}}
    slots = {
        "x-codex-primary-window-minutes": "300", "x-codex-primary-reset-at": "1793100000",
        "x-codex-primary-used-percent": "40",
        "x-codex-secondary-window-minutes": "10080", "x-codex-secondary-reset-at": str(RESET),
        "x-codex-secondary-used-percent": "100",
    }
    refusal = read_refusal(_openai(429, plain, slots))
    assert (refusal.window, refusal.until) == ("seven_day", datetime.fromtimestamp(RESET, tz=timezone.utc))
    unknown = read_refusal(_openai(429, plain, {**slots, "x-codex-secondary-used-percent": "99"}))
    assert (unknown.window, unknown.until) == (None, None)


def test_a_codex_workspace_out_of_credits_is_a_balance_not_a_wait():
    exc = _openai(429, _CODEX_LIMIT, {"x-codex-rate-limit-reached-type": "workspace_member_credits_depleted"})
    assert read_refusal(exc).reason == "out_of_credit"


@pytest.mark.parametrize("reached", ["workspace_owner_usage_limit_reached", "workspace_member_usage_limit_reached"])
def test_a_codex_workspace_spend_cap_is_a_quota_its_owner_raises(reached):
    exc = _openai(429, _CODEX_LIMIT, {"x-codex-rate-limit-reached-type": reached})
    assert read_refusal(exc).reason == "quota_exceeded"


def test_a_codex_plan_without_codex_is_a_plan_that_excludes_it():
    assert read_refusal(_openai(429, {"error": {"type": "usage_not_included"}})).reason == "plan_excludes"


_CLAUDE_429 = {"type": "error", "error": {
    "type": "rate_limit_error", "message": "This request would exceed your account's rate limit.",
}}


def test_a_claude_sign_in_refused_at_its_plan_limit_reads_the_headers():
    exc = _anthropic(429, _CLAUDE_429, {
        "anthropic-ratelimit-unified-status": "rejected",
        "anthropic-ratelimit-unified-representative-claim": "seven_day",
        "anthropic-ratelimit-unified-reset": str(RESET),
    })
    refusal = read_refusal(exc)
    assert (refusal.reason, refusal.window) == ("plan_limit", "seven_day")
    assert refusal.until == datetime.fromtimestamp(RESET, tz=timezone.utc)


def test_a_claude_429_with_usage_credits_paying_is_only_a_rate_limit():
    exc = _anthropic(429, _CLAUDE_429, {
        "anthropic-ratelimit-unified-status": "rejected",
        "anthropic-ratelimit-unified-overage-status": "allowed",
    })
    assert read_refusal(exc).reason == "rate_limited"
    assert read_refusal(_anthropic(429, _CLAUDE_429)).reason == "rate_limited"


@pytest.mark.parametrize(("text", "reason"), [
    # Status alone would read each of these wrong.
    ("Error code: 429 - {'error': {'code': '1113', 'message': 'Insufficient balance or no resource package. Please recharge.'}}", "out_of_credit"),
    ('event:error\n:HTTP_STATUS/400\ndata:{"code":"Arrearage","message":"Access denied, please make sure your account is in good standing."}', "out_of_credit"),
    ("Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'Your credit balance is too low to access the Anthropic API.'}}", "out_of_credit"),
    ("Error code: 429 - {'error': {'message': 'You exceeded your current quota.', 'type': 'insufficient_quota', 'code': 'insufficient_quota'}}", "out_of_credit"),
    ("Error code: 402 - {'error': {'message': 'Insufficient Balance', 'type': 'insufficient_balance_error', 'code': 'invalid_request_error'}}", "out_of_credit"),
    ("Error code: 402 - {'type': 'error', 'error': {'type': 'insufficient_balance_error', 'message': 'insufficient balance (1008)'}}", "out_of_credit"),
    # Gemini says "exceeded your current quota" for every quota: only a model
    # the tier gives nothing of, or a daily cap, is one to fix.
    ("429 Too Many Requests. You exceeded your current quota, please check your plan and billing details. * Quota exceeded for metric: generate_content_free_tier_input_token_count, limit: 0, model: gemini-4-argon", "quota_exceeded"),
    ("429 Too Many Requests. You exceeded your current quota. Quota exceeded for metric: generate_requests_per_model_per_day", "quota_exceeded"),
    ("429 Too Many Requests. You exceeded your current quota. Quota exceeded for metric: generate_content_requests_per_minute, limit: 15", "rate_limited"),
    ("Error code: 451 - {'error': {'message': 'The content you provided or machine outputted is blocked.', 'type': 'censorship_blocked'}}", "content_blocked"),
    # xAI on a Grok sign-in: its spent weekly pool, a spent team, no plan.
    ("Error code: 403 - {'code': 'The caller does not have permission', 'error': 'You have either run out of available resources or do not have an active Grok subscription.'}", "plan_limit"),
    ("Error code: 402 - {'code': 'x', 'error': 'You have run out of credits or need a Grok subscription. (type=personal-team-blocked:spending-limit)'}", "out_of_credit"),
    ("Error code: 403 - {'error': 'You need a Grok subscription'}", "plan_excludes"),
])
def test_a_refusal_reads_as_its_provider_meant_it(text, reason):
    assert read_refusal(RuntimeError(text)).reason == reason


def test_a_status_the_sdk_set_decides_when_the_body_names_nothing():
    """DeepSeek's busy server, xAI's rate limit: only the status says what."""
    assert read_refusal(_openai(503, {"error": {"message": "服务器繁忙，请稍后重试", "type": "all_failed"}})).reason == "overloaded"
    assert read_refusal(_openai(429, {"code": "Some resource has been exhausted", "error": "Rate limit exceeded"})).reason == "rate_limited"


def test_the_words_in_a_parsed_body_count_when_the_message_omits_them():
    """Gemini words a daily cap only in its body's message."""
    exc = _openai(429, {"error": {"message": "You exceeded your current quota. Quota exceeded for metric: generate_requests_per_model_per_day"}})
    assert read_refusal(exc).reason == "quota_exceeded"


def test_an_anthropic_spend_cap_is_a_quota_not_a_rate_limit():
    """The tier's monthly cap shares the rate limit's type; a set limit is a 400."""
    tier = {"type": "error", "error": {
        "type": "rate_limit_error", "message": "You have reached your API usage limits: your organization has crossed its monthly API usage threshold.",
        "details": {"error_code": "enforced_spend_limit_reached"},
    }}
    assert read_refusal(_anthropic(429, tier)).reason == "quota_exceeded"
    own = {"type": "error", "error": {
        "type": "invalid_request_error",
        "message": "You have reached your specified workspace API usage limits. You will regain access on 2026-12-01 at 00:00 UTC.",
    }}
    assert read_refusal(_anthropic(400, own)).reason == "quota_exceeded"
    burst = {"type": "error", "error": {"type": "rate_limit_error", "message": "Number of request tokens has exceeded your per-minute rate limit"}}
    assert read_refusal(_anthropic(429, burst)).reason == "rate_limited"


def test_xais_sentence_in_a_parsed_body_counts_when_the_message_omits_it():
    body = {"code": "The caller does not have permission", "error": "You have either run out of available resources or do not have an active Grok subscription."}
    assert read_refusal(_openai(403, body)).reason == "plan_limit"


def test_a_numbered_auth_code_reads_by_its_status():
    """Z.ai numbers its auth refusals 1000 to 1002 and sends them with a 401;
    the same numbers from elsewhere name nothing."""
    assert read_refusal(_openai(401, {"error": {"message": "Authentication Failed", "type": "1000"}})).reason == "invalid_key"
    assert read_refusal(_openai(400, {"error": {"message": "Bad parameter", "code": 1001}})) is None


def test_a_status_in_prose_is_no_refusal():
    """A number in a message is not a status, and some reasons end retries."""
    assert read_refusal(ConnectionError("connection reset after 451 bytes")) is None


def test_a_numeric_code_on_some_other_layer_is_not_the_providers():
    class Closed(Exception):
        code = 1001  # a websocket going away

    assert read_refusal(Closed("socket closed")) is None


def test_a_body_that_does_not_parse_fails_closed():
    for text in ("Error code: 429 - {[1]: 2}", "Error code: 400 - {{'a'}}", "{" * 50_000):
        assert read_refusal(RuntimeError(text)) is None


def test_a_body_printed_as_json_is_read_as_json():
    text = 'Error code: 429 - {"error": {"code": "1113", "message": "Insufficient", "param": null}}'
    assert read_refusal(RuntimeError(text)).reason == "out_of_credit"


@pytest.mark.parametrize("text", [
    # A stale Grok token is the sign-in's, which reconnecting fixes.
    "Error code: 403 - {'code': 'x', 'error': '[WKE=unauthenticated:x] OAuth2 access token could not be validated'}",
    # A request this service built wrong names no reason of the provider's.
    "Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': \"messages.144: role 'system' must precede\"}}",
    "AsyncCompletions.create() got an unexpected keyword argument 'thinking'",
])
def test_an_error_the_provider_named_no_reason_for_is_none(text):
    assert read_refusal(RuntimeError(text)) is None


def test_a_used_up_plan_is_not_retried_and_a_burst_limit_is():
    """A plan or balance does not come back in the seconds a retry waits."""
    assert is_terminal_refusal(_openai(429, _CODEX_LIMIT)) is True
    assert is_terminal_refusal(_anthropic(429, _CLAUDE_429)) is False


def test_a_claude_code_turn_its_plan_stopped_is_that_plans_limit():
    try:
        raise RuntimeError("turn failed") from ClaudeTurnFailed("plan_exhausted", "")
    except RuntimeError as exc:
        assert read_refusal(exc).reason == "plan_limit"
