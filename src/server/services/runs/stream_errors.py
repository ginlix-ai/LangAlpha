"""Chat-stream failure classification, shared by the producer's error frame
and the ledger metadata a failed model call leaves on its run."""

import re
from typing import Any, Dict, Optional, Tuple

from ptc_agent.config.agent import CredentialPayer, Payer
from src.llms.claude_code import ClaudeTurnFailed
from src.llms.provider_errors import ProviderRefusal, Reason, read_refusal
from src.server.services.llm.claude_code import claude_code_refusal
from src.server.services.llm.payers import plan_display_name, source_payer
from src.server.services.oauth_health import subscription_token_refusal
from src.server.utils.error_sanitization import sanitize_error_text

# Chat-stream failures fall into two buckets and the user-facing remedy is
# different for each:
#
#   - ``upstream``  — the LLM provider we called returned an error (their
#                     server 500'd, the key is rejected, rate-limited, etc).
#                     User should check their key / plan / provider status.
#   - ``internal``  — our own pipeline failed (middleware bug, our DB, a
#                     schema mismatch in the payload we built). User can't
#                     do anything; we should log loudly and show a generic
#                     retry message.
#
# Classification is a module-prefix check with exception-chain walking — any
# exception in the chain sourced from a known provider SDK flips the whole
# failure to ``upstream``.

# Provider SDK and LangChain-wrapper module prefixes. Any exception in the
# cause chain whose ``__module__`` matches one of these flips the whole
# failure to ``upstream``. Keep in sync with the SDKs wired up in
# ``src/llms/llm.py`` — missing a prefix means the user sees "our service
# failed" for what's really a provider error.
#
# ``httpx`` is in this list as a last-resort catch: a bare httpx exception
# that reaches the stream error handler has almost always come from the
# LangChain call path (SDKs raise via httpx). If our own service calls
# (credit checks, workspace manager) ever start raising bare httpx errors to
# the stream path we should wrap them in a distinct exception type before
# they bubble; classification is a UI hint, not a diagnostic source of truth.
_UPSTREAM_MODULE_PREFIXES: tuple[str, ...] = (
    # Raw provider SDKs
    "anthropic",
    "openai",
    "google.api_core",
    "google.genai",
    "google.generativeai",
    "cohere",
    "openrouter",
    "httpx",
    # Our own wrappers around a provider stream. They quote the provider and
    # are raised fresh rather than chained, so nothing below would match them.
    "src.llms.extension",
    # LangChain wrappers — their exceptions may not chain through the raw SDK
    # when the wrapper normalizes errors, so match them directly.
    "langchain_openai",
    "langchain_anthropic",
    "langchain_deepseek",
    "langchain_openrouter",
    "langchain_qwq",
    "langchain_google_genai",
    "langchain_google_vertexai",
    "langchain_mistralai",
    "langchain_together",
    "langchain_groq",
    "groq",
)

_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")


def _parse_status_from_message(text: str) -> Optional[int]:
    match = _STATUS_CODE_RE.search(text)
    return int(match.group(1)) if match else None


# Statuses meaning the provider refused the request we sent: bad shape,
# unsupported content, oversized payload. Never the caller's credential and
# never the provider's health, so the only honest hint is to move to a model
# that accepts the request. 401/403/404 are absent because they have their own
# branches; they are about access, not about the request body.
_REQUEST_REFUSED_STATUSES: frozenset[int] = frozenset({400, 405, 413, 422})


def failure_payer(config: Any) -> CredentialPayer:
    """Who the provider billed for a failed call on the turn ``config``
    resolved (``resolve_clients`` records it); a config that never resolved a
    credential answers by its source alone."""
    return getattr(config, "credential_payer", None) or source_payer(
        getattr(config, "credential_source", None)
    )


def upstream_failure(exc: BaseException, config: Any) -> Dict[str, Any]:
    """What an upstream failure carries beyond its status: ``payer``,
    ``account`` when the user pays, the ``reason`` the provider gave, and
    ``plan_limit`` for a subscription's used-up limit. The frame and the run
    row carry the same fields, so a reload reads as the live card did."""
    named = failure_payer(config)
    refusal = read_refusal(exc)
    fields = {
        "payer": named.payer,
        "account": named.account,
        "reason": refusal.reason if refusal else None,
        "plan_limit": plan_limit_detail(refusal, named),
    }
    return {key: value for key, value in fields.items() if value is not None}


def plan_limit_detail(
    refusal: Optional[ProviderRefusal], named: CredentialPayer
) -> Optional[Dict[str, Any]]:
    """A subscription's used-up limit: the plan as its maker sells it
    (``ChatGPT Plus``), the window and its reset when the provider said. None
    for anything else, and for a plan the turn could not name, which the card
    would have to word as no one's."""
    if refusal is None or refusal.reason != "plan_limit":
        return None
    if named.payer != "subscription" or not named.account:
        return None
    return {
        "plan": plan_display_name(named.account, refusal.tier),
        "window": refusal.window,
        "until": refusal.until.isoformat() if refusal.until else None,
    }


def find_resilience_trace(exc: BaseException) -> Optional[Dict[str, Any]]:
    """Find the attempt trace attached by ``ModelResilienceMiddleware``.

    The middleware sets ``__model_resilience__`` (see ``RESILIENCE_TRACE_ATTR``
    in ``src/ptc_agent/agent/middleware/model_resilience.py``) on the primary
    model's exception before re-raising it. Walks the cause chain defensively
    in case a wrapper exception ends up on top.
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        trace = getattr(current, "__model_resilience__", None)
        if isinstance(trace, dict):
            return trace
        current = current.__cause__ or current.__context__
    return None


def model_call_failure(exc: BaseException, config: Any) -> Dict[str, Any]:
    """Ledger metadata for a run that failed on its model call; ``{}`` when
    the failure was anything else.

    The resilience trace is the signal, not the provider SDK prefix
    ``classify_stream_exception`` matches: the middleware attaches it only to
    a model call's exception, while an httpx 401 from a tool or data API
    would pass for a rejected model key. The primary's status is the one its
    credential earned, and that credential is the run's own.
    """
    trace = find_resilience_trace(exc)
    attempted = trace.get("attempted_models") if trace else None
    if not (isinstance(attempted, list) and attempted and isinstance(attempted[0], dict)):
        return {}
    status = attempted[0].get("status_code")
    if not isinstance(status, int):
        return {}
    return {
        "error_status_code": status,
        "error_credential_owned": failure_payer(config).payer != "credits",
    }


#: The Claude Code failures that were the answer the turn got, from Anthropic
#: or the machine's Claude Code, by ``ClaudeTurnFailed.kind``: the status
#: the frame carries and the hints it offers. Any other kind is either refused
#: as admission refuses (``auth_failed``, ``policy_blocked``) or this
#: service's own failure.
_CLAUDE_CODE_ANSWERS: Dict[str, Tuple[Optional[int], Tuple[str, ...]]] = {
    # The user's own plan is spent, not the provider down.
    "plan_exhausted": (429, ("try_another_model",)),
    # No status: could be anything.
    "api_error": (None, ("api_key", "model_access", "provider_status", "try_another_model")),
    "model_unavailable": (None, ("try_another_model",)),
    "startup_failed": (None, ("try_another_model",)),
}


def classify_stream_exception(exc: BaseException) -> Dict[str, Any]:
    """Classify a chat-stream exception as ``upstream`` or ``internal``.

    Walks ``__cause__`` / ``__context__`` so a wrapped provider error (e.g.
    a LangChain exception caused by ``anthropic.InternalServerError``) is
    still recognized as upstream. Returns a dict with ``kind``,
    ``status_code`` (when carried on the exception or parseable from its
    message), and ``provider_module`` (the matched SDK prefix, or None);
    a Claude Code failure the turn was answered with adds its ``hints``.
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ClaudeTurnFailed) and current.kind in _CLAUDE_CODE_ANSWERS:
            status, hints = _CLAUDE_CODE_ANSWERS[current.kind]
            return {"kind": "upstream", "status_code": status, "provider_module": None, "hints": list(hints)}
        module = getattr(type(current), "__module__", "") or ""
        for prefix in _UPSTREAM_MODULE_PREFIXES:
            if module == prefix or module.startswith(prefix + "."):
                status = getattr(current, "status_code", None)
                # A timeout's message carries counts, never a status: a
                # stall's ``chunks_received=413`` is not a refused payload.
                if not isinstance(status, int) and not isinstance(current, TimeoutError):
                    status = _parse_status_from_message(str(current))
                return {
                    "kind": "upstream",
                    "status_code": status if isinstance(status, int) else None,
                    "provider_module": prefix,
                }
        current = current.__cause__ or current.__context__

    return {"kind": "internal", "status_code": None, "provider_module": None}


def failure_kind(exc: BaseException) -> Dict[str, Any]:
    """``classify_stream_exception``, read with the resilience trace.

    The trace proves the failure happened inside a model call, so a generic
    wrapper exception (no recognizable SDK module in the chain) must not
    demote it to ``internal``: the frontend routes internal errors to a
    generic banner that drops the model / attempted-models context.
    """
    info = classify_stream_exception(exc)
    if info["kind"] != "internal":
        return info
    trace = find_resilience_trace(exc)
    if trace is None:
        return info
    primary_status: Any = None
    attempted_raw = trace.get("attempted_models")
    if isinstance(attempted_raw, list) and attempted_raw and isinstance(attempted_raw[0], dict):
        primary_status = attempted_raw[0].get("status_code")
    return {
        "kind": "upstream",
        "status_code": primary_status if isinstance(primary_status, int) else None,
        "provider_module": None,
    }


def failure_kind_metadata(exc: BaseException) -> Dict[str, Any]:
    """Ledger metadata naming whose fault a failed run was, as its ``error``
    frame names it. A reload replays the frame the run sent with this kind
    stamped on it, so the failure reads as it did live. A run whose stream
    could carry no frame (its quota spent) sent none, and the row's own
    error item is then the only place the failure is worded.

    ``{}`` for the turn's time limit and a Claude Code token refused mid-turn:
    their frames are worded for the user and carry no kind, and the reload
    replays them unchanged.
    """
    # Lazy: sse_producer imports this module.
    from src.server.services.runs.sse_producer import TurnTimeLimitError

    if isinstance(exc, TurnTimeLimitError) or claude_code_refusal(exc, None) is not None:
        return {}
    return {"error_kind": failure_kind(exc)["kind"]}


def plan_stop_metadata(exc: BaseException) -> Dict[str, Any]:
    """Ledger metadata for a run the Claude plan stopped (``plan_stopped``):
    its window, reset and whether usage credits paid first, which settling
    its automation words on its owner's clock. ``{}`` for any other failure."""
    refusal = claude_code_refusal(exc, None)
    if refusal is None or refusal.get("reason") != "plan_stopped":
        return {}
    keep = ("type", "reason", "window", "until", "credits_billed")
    return {"claude_plan": {k: refusal[k] for k in keep if k in refusal}}


#: Guidance for a refusal whose reason the provider named, most relevant
#: first, for a user who holds the account. A reason not here falls back to
#: the status.
_REASON_HINTS: Dict[Reason, Tuple[str, ...]] = {
    # The key is valid, so asking the user to check it misdirects them.
    "out_of_credit": ("provider_balance", "try_another_model"),
    "quota_exceeded": ("provider_quota", "try_another_model"),
    "plan_limit": ("try_another_model",),
    "plan_excludes": ("model_access", "try_another_model"),
    "invalid_key": ("api_key", "try_another_model"),
    "rate_limited": ("rate_limited", "try_another_model"),
    "overloaded": ("provider_overloaded", "try_another_model"),
    "content_blocked": ("content_blocked", "try_another_model"),
    "context_too_long": ("new_thread", "try_another_model"),
}

#: Reasons that are about the request, not the account, so they are the
#: user's to act on whoever pays.
_REQUEST_REASONS: frozenset[Reason] = frozenset({"content_blocked", "context_too_long"})

#: The guidance for a status the user can act on by their account or model.
_STATUS_HINTS: Dict[int, Tuple[str, ...]] = {
    401: ("api_key", "model_access", "try_another_model"),
    403: ("api_key", "model_access", "try_another_model"),
    # A 402 read from the message alone, which names no reason: the account
    # behind the key has no credit, and the key itself is valid.
    402: ("provider_balance", "try_another_model"),
    404: ("model_access", "try_another_model"),
}


def _status_hints(status: Optional[int]) -> list[str]:
    """5xx and 429 are the provider's health, not the user's credential, so a
    key check never leads there."""
    if status in _STATUS_HINTS:
        return list(_STATUS_HINTS[status])
    if status in _REQUEST_REFUSED_STATUSES:
        return ["try_another_model"]
    if status == 429 or (isinstance(status, int) and status >= 500):
        return ["provider_status", "try_another_model"]
    # No status (network error): could be anything, so show all.
    return ["api_key", "model_access", "provider_status", "try_another_model"]


def _upstream_hints(
    info: Dict[str, Any],
    payer: Payer,
    trace: Optional[Dict[str, Any]],
    signed_in: bool,
    reason: Optional[Reason],
) -> list[str]:
    """The guidance under an upstream failure, most relevant first: the
    reason the provider gave, or else its status, says what failed, and the
    payer who can act on it."""
    status = info.get("status_code")
    if reason in _REQUEST_REASONS:
        # The request itself: what the user sent, or how long the thread got.
        return list(_REASON_HINTS[reason])
    if payer == "local":
        # A server on the user's machine: no key, plan or status page, and a
        # failure there is most often the server not answering.
        return ["local_server", "try_another_model"]
    if payer == "credits":
        # The account is the service's: nothing about it is the user's to
        # check, and they do not know which vendor serves the model, so its
        # status page is no lead either. What is left is what they can do
        # here. A refused request is usually the thread itself (too long, a
        # file the model cannot read), which a new thread leaves behind.
        if status in _REQUEST_REFUSED_STATUSES:
            return ["try_another_model", "new_thread"]
        return ["try_another_model", "retry_later", "new_thread"]
    hints = list(info.get("hints") or _REASON_HINTS.get(reason) or _status_hints(status))
    if signed_in:
        # A subscription has no key to check. When its token was refused (any
        # 401, or xAI's stale-token 403, whose other 403s are the plan's)
        # reconnecting renews the sign-in, and a stale token says nothing
        # about the model's access.
        stale = status == 403 and subscription_token_refusal(trace) is not None
        hints = [h for h in hints if h != "api_key" and not (stale and h == "model_access")]
        if status == 401 or stale:
            hints.insert(0, "reconnect_account")
    return hints


def error_event_data(
    thread_id: str,
    error_message: str,
    *,
    exc: Optional[BaseException] = None,
    model: Optional[str] = None,
    config: Any = None,
) -> Dict[str, Any]:
    """The ``error`` frame's data.

    With ``exc`` the frame carries ``error_kind`` (``upstream`` or
    ``internal``), ``status_code`` when available, and ``hints`` for the
    frontend to render user-actionable guidance; the legacy ``error`` and
    ``message`` fields stay so older clients keep working.

    An upstream failure also carries ``payer``, who the provider bills for the
    call, because that decides what the user can do about it. A user who pays
    (their own key or subscription) is shown the provider's own words and told
    the account is theirs to fix, with ``account`` naming it as the turn
    resolved it (``config``, see ``upstream_failure``). On credits the
    account is the service's, so the client words the failure generically and
    the hints offer only what the user can do here: another model, a new
    thread, a later try.

    ``reason`` is why the provider refused, when it said (``read_refusal``),
    and a subscription whose usage limit is used up adds ``plan_limit`` (the
    plan, window and reset) for the client to word as a plan card.

    A Claude Code turn whose token Anthropic refused gets the admission
    refusal's shape instead, ``model`` naming the turn's row.
    """
    if exc is not None and (refusal := claude_code_refusal(exc, model)):
        return {"thread_id": thread_id, "error": refusal["message"], **refusal}
    data: Dict[str, Any] = {
        "thread_id": thread_id,
        "error": sanitize_error_text(error_message),
        "message": "An error occurred during processing",
    }
    if exc is not None:
        info = failure_kind(exc)
        trace = find_resilience_trace(exc)
        data["error_kind"] = info["kind"]
        if info["status_code"] is not None:
            data["status_code"] = info["status_code"]
        if info["provider_module"]:
            data["provider_module"] = info["provider_module"]
        if info["kind"] == "upstream":
            data.update(upstream_failure(exc, config))
            signed_in = str(getattr(config, "credential_source", None)) == "oauth"
            data["hints"] = _upstream_hints(info, data["payer"], trace, signed_in, data.get("reason"))
        if trace is not None:
            primary_model = trace.get("model")
            if isinstance(primary_model, str) and primary_model:
                data["model"] = primary_model
            attempted = trace.get("attempted_models")
            if isinstance(attempted, list):
                data["attempted_models"] = [
                    {
                        "model": entry.get("model"),
                        "error": sanitize_error_text(
                            str(entry.get("error") or "")
                        ),
                        "status_code": entry.get("status_code"),
                        "attempts": entry.get("attempts"),
                    }
                    for entry in attempted
                    if isinstance(entry, dict)
                ]
    return data
