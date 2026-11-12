"""Why a model provider refused a call, read from what it sent back.

The status alone misleads: GLM answers an empty balance with 429, Qwen with
400, and a subscription's used-up plan arrives as the same 429 as a burst
limit. So the body's codes, a subscription's limit headers and, where an
integration keeps only text, the wording decide. Each match is one a provider
was seen to send, or the shape its own client parses; an unmatched error is
``None``, which leaves the caller's status-based handling as it was.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping

from src.llms.claude_code import PLAN_LIMITS, ClaudeTurnFailed, PlanWindow
from src.llms.error_classification import is_context_overflow, iter_exception_chain, sdk_status_code

Reason = Literal[
    "out_of_credit",
    "quota_exceeded",
    "plan_limit",
    "plan_excludes",
    "invalid_key",
    "rate_limited",
    "overloaded",
    "content_blocked",
    "context_too_long",
]

#: Reasons the same call meets again on every retry: only the user's account
#: changing, or another model, gets past them.
TERMINAL_REASONS: frozenset[Reason] = frozenset({
    "out_of_credit", "quota_exceeded", "plan_limit", "plan_excludes", "invalid_key", "content_blocked",
})

#: Codex reports a window in minutes, mapped onto the names Claude gives its
#: plan's windows (``PlanWindow``), which the plan card words.
_WINDOW_MINUTES: dict[int, PlanWindow] = {300: "five_hour", 10080: "seven_day"}


@dataclass(frozen=True)
class ProviderRefusal:
    """``tier`` is the plan tier a subscription reported (Codex's ``plus``),
    ``window`` and ``until`` the used-up limit and its reset, when known."""

    reason: Reason
    tier: str | None = None
    window: PlanWindow | None = None
    until: datetime | None = None


# Codes and types in an error body, matched exactly, each from the provider named.
_OUT_OF_CREDIT_CODES = frozenset({
    "insufficient_balance_error",  # DeepSeek, MiniMax
    "insufficient_quota",  # OpenAI: billing exhausted, sent as 429
    "1113",  # Z.ai / GLM: "Insufficient balance or no resource package", sent as 429
    "arrearage",  # Alibaba Model Studio (DashScope): account overdue, sent as 400
    "exceeded_current_quota_error",  # Moonshot: account balance, sent as 429
})
_OUT_OF_CREDIT_PHRASES = (
    "insufficient balance",
    "credit balance is too low",  # Anthropic, sent as 400
    "insufficient credits",  # OpenRouter, 402
    "run out of credits",  # xAI: team credits or its spending limit, 402 or 429
    "spending-limit",
    "余额不足",
)
# xAI on a Grok sign-in whose weekly pool is spent, as a 403 or a stream error
# frame. The same words cover an account with no subscription, which xAI does
# not tell apart, and give no reset.
_GROK_PLAN_PHRASES = ("run out of available resources",)
# Z.ai's auth codes (1000 to 1002) are bare numbers another provider may use
# for anything; they come with a 401, which reads as a bad key on its own.
_INVALID_KEY_CODES = frozenset({"invalid_api_key", "authentication_error"})
_CONTENT_BLOCKED_CODES = frozenset({
    "censorship_blocked",  # StepFun, 451
    "content_policy_violation",  # OpenAI
    "content_filter",  # Moonshot
    "data_inspection_failed",  # DashScope
})
_OVERLOADED_CODES = frozenset({"overloaded_error", "engine_overloaded_error", "server_overloaded"})
_RATE_LIMIT_CODES = frozenset({"rate_limit_error", "rate_limit_exceeded", "rate_limit_reached_error"})
# Gemini words every quota refusal so, a per-minute limit that clears in
# seconds as much as a model the plan's tier gives none of ("limit: 0") or a
# daily cap; only the last two are a quota to fix. OpenAI's billing refusal
# says the same words but is caught by its code first.
_QUOTA_PHRASE = "exceeded your current quota"
# Anthropic's spend caps hold until the month turns or the limit is raised:
# the tier's cap is a 429 ``rate_limit_error`` told apart only by this code,
# and a limit the account set is a 400 worded so.
_SPEND_CAP_CODE = "enforced_spend_limit_reached"
_SPEND_CAP_PHRASE = "you have reached your specified"
_QUOTA_LASTING = ("limit: 0", "perday", "per day", "per_day")

# Codex's ``x-codex-rate-limit-reached-type``: a workspace out of credits is
# a balance to top up, and its spend cap a quota its owner raises, which no
# wait clears.
_CODEX_CREDITS_DEPLETED = ("workspace_owner_credits_depleted", "workspace_member_credits_depleted")
_CODEX_SPEND_CAP = ("workspace_owner_usage_limit_reached", "workspace_member_usage_limit_reached")


def is_terminal_refusal(exc: BaseException) -> bool:
    """Whether the provider refused for a reason no retry of the same call gets
    past (an empty balance, a used-up plan, a bad key, blocked content)."""
    refusal = read_refusal(exc)
    return refusal is not None and refusal.reason in TERMINAL_REASONS


def read_refusal(exc: BaseException) -> ProviderRefusal | None:
    """The reason the provider gave for refusing the call ``exc`` reports."""
    if is_context_overflow(exc):
        return ProviderRefusal("context_too_long")
    for current in iter_exception_chain(exc):
        # A Claude Code turn its plan stopped, with no window known.
        if isinstance(current, ClaudeTurnFailed) and current.kind == "plan_exhausted":
            return ProviderRefusal("plan_limit")
        error = _error_body(current)
        headers = _headers(current)
        if (refusal := _subscription_limit(error, headers)) is not None:
            return refusal
        codes = _codes(current, error)
        # An SDK's message need not repeat the body, so its words count too:
        # its ``message``, or xAI's ``error``, which is the sentence itself.
        words = [_text((error or {}).get(key)) for key in ("message", "error")]
        text = " ".join(part for part in (str(current), *words) if part).lower()
        if any(p in text for p in _GROK_PLAN_PHRASES):
            return ProviderRefusal("plan_limit")
        if "need a grok subscription" in text and "run out of" not in text:
            return ProviderRefusal("plan_excludes")
        if codes & _OUT_OF_CREDIT_CODES or any(p in text for p in _OUT_OF_CREDIT_PHRASES):
            return ProviderRefusal("out_of_credit")
        if codes & _CONTENT_BLOCKED_CODES:
            return ProviderRefusal("content_blocked")
        if codes & _INVALID_KEY_CODES:
            return ProviderRefusal("invalid_key")
        if codes & _OVERLOADED_CODES:
            return ProviderRefusal("overloaded")
        details = (error or {}).get("details")
        if (isinstance(details, Mapping) and details.get("error_code") == _SPEND_CAP_CODE) or _SPEND_CAP_PHRASE in text:
            return ProviderRefusal("quota_exceeded")
        if _QUOTA_PHRASE in text:
            lasting = any(p in text for p in _QUOTA_LASTING)
            return ProviderRefusal("quota_exceeded" if lasting else "rate_limited")
        if codes & _RATE_LIMIT_CODES:
            return ProviderRefusal("rate_limited")
    # Only a status the SDK set: a message saying "451 bytes" is no refusal,
    # and some reasons end the call's retries.
    reason = _STATUS_REASONS.get(sdk_status_code(exc))
    return ProviderRefusal(reason) if reason else None


#: What a status says on its own, when the body named nothing.
_STATUS_REASONS: dict[int | None, Reason] = {
    402: "out_of_credit",
    401: "invalid_key",
    451: "content_blocked",
    503: "overloaded",
    529: "overloaded",
    429: "rate_limited",
}


def _subscription_limit(error: Mapping[str, Any] | None, headers: Mapping[str, str] | None) -> ProviderRefusal | None:
    """A subscription's usage limit: Codex's body, or Anthropic's headers on a
    Claude sign-in, which name the window and its reset."""
    kind = str((error or {}).get("type") or "").lower()
    if kind == "usage_limit_reached":
        reached = (_header(headers, "x-codex-rate-limit-reached-type") or "").strip().lower()
        if reached in _CODEX_CREDITS_DEPLETED:
            return ProviderRefusal("out_of_credit")
        if reached in _CODEX_SPEND_CAP:
            return ProviderRefusal("quota_exceeded")
        # The body names the plan and its reset; the headers every Codex
        # response carries say the same where it does not.
        window, until = _codex_window(error, headers)
        return ProviderRefusal(
            "plan_limit",
            tier=_text(error.get("plan_type")) or _header(headers, "x-codex-plan-type"),
            window=window,
            until=until,
        )
    if kind == "usage_not_included":
        return ProviderRefusal("plan_excludes")
    if (_header(headers, "anthropic-ratelimit-unified-status") or "").lower() == "rejected":
        # Usage credits on the Claude account would have paid; only a refusal
        # with them off or spent is the plan's limit.
        overage = (_header(headers, "anthropic-ratelimit-unified-overage-status") or "").lower()
        if overage in ("", "rejected"):
            claim = _header(headers, "anthropic-ratelimit-unified-representative-claim")
            return ProviderRefusal(
                "plan_limit",
                window=claim if claim in PLAN_LIMITS else None,  # type: ignore[arg-type]
                until=_epoch(_header(headers, "anthropic-ratelimit-unified-reset")),
            )
    return None


def _codex_window(
    error: Mapping[str, Any], headers: Mapping[str, str] | None
) -> tuple[PlanWindow | None, datetime | None]:
    """The refused window and when it resets. The headers carry a primary and
    a secondary window, each with its own reset, so a header's reset counts
    only for the window refused: the one the body named, else the one its
    header says is used up (the later to reset when both are). A window
    nothing identifies leaves both unknown rather than a wrong reset."""
    def reset_of(slot: str) -> datetime | None:
        return _epoch(_header(headers, f"x-codex-{slot}-reset-at"))

    minutes = _int(error.get("limit_window_minutes"))
    if minutes is not None:
        slot = next((s for s in _CODEX_SLOTS if _int(_header(headers, f"x-codex-{s}-window-minutes")) == minutes), None)
    else:
        spent = [s for s in _CODEX_SLOTS if (_float(_header(headers, f"x-codex-{s}-used-percent")) or 0) >= 100]
        slot = max(spent, key=lambda s: reset_of(s) or datetime.min.replace(tzinfo=timezone.utc), default=None)
        minutes = _int(_header(headers, f"x-codex-{slot}-window-minutes")) if slot else None
    until = _codex_reset(error) or (reset_of(slot) if slot else None)
    return _WINDOW_MINUTES.get(minutes) if minutes is not None else None, until


_CODEX_SLOTS = ("primary", "secondary")


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _codex_reset(error: Mapping[str, Any]) -> datetime | None:
    if (at := _epoch(error.get("resets_at"))) is not None:
        return at
    try:
        return datetime.now(timezone.utc) + timedelta(seconds=int(error["resets_in_seconds"]))
    except (KeyError, TypeError, ValueError):
        return None


def _epoch(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _error_body(exc: BaseException) -> Mapping[str, Any] | None:
    """The ``error`` object the provider sent: the SDK's parsed body, or the
    dict an integration printed into the message (``Error code: 429 - {...}``)."""
    body = getattr(exc, "body", None)
    if not isinstance(body, dict):
        body = _printed_body(str(exc))
    if not isinstance(body, dict):
        return None
    error = body.get("error", body)
    return error if isinstance(error, dict) else body


_PRINTED = re.compile(r"\{.*\}", re.DOTALL)
_PRINTED_MAX = 16_384


def _printed_body(text: str) -> dict | None:
    """JSON as the provider sent it, or the Python repr the SDK printed. The
    message is the provider's to word, so anything it holds fails closed."""
    match = _PRINTED.search(text[:_PRINTED_MAX])
    if not match:
        return None
    for parse in (json.loads, ast.literal_eval):
        try:
            value = parse(match.group(0))
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return None


def _codes(exc: BaseException, error: Mapping[str, Any] | None) -> set[str]:
    """The provider's codes: its body's, and a named code an SDK copied onto
    the error. A number on an exception's own ``code`` is some other layer's
    (a socket's close code), so numeric codes count only from the body."""
    codes = {
        value.lower() for value in (getattr(exc, "code", None), getattr(exc, "type", None))
        if isinstance(value, str) and value and not value.isdigit()
    }
    if error:
        codes |= {
            str(value).lower() for value in (error.get("code"), error.get("type"))
            if isinstance(value, (str, int)) and not isinstance(value, bool) and str(value)
        }
    return codes


def _headers(exc: BaseException) -> Mapping[str, str] | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    return headers if headers is not None and hasattr(headers, "get") else None


def _header(headers: Mapping[str, str] | None, name: str) -> str | None:
    if headers is None:
        return None
    value = headers.get(name)
    return value if isinstance(value, str) and value else None
