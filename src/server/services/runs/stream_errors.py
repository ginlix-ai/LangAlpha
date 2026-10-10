"""Chat-stream failure classification, shared by the producer's error frame
and the ledger metadata a failed model call leaves on its run."""

import re
from typing import Any, Dict, Optional

from src.config import settings as app_settings
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
    "httpx",
    # Our own wrappers around a provider stream. They quote the provider and
    # are raised fresh rather than chained, so nothing below would match them.
    "src.llms.extension",
    # LangChain wrappers — their exceptions may not chain through the raw SDK
    # when the wrapper normalizes errors, so match them directly.
    "langchain_openai",
    "langchain_anthropic",
    "langchain_deepseek",
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

# Hints that ask the user to go fix a credential. Only truthful when the user
# is the one holding it.
_CREDENTIAL_HINTS: frozenset[str] = frozenset({"api_key", "model_access"})


def user_owns_credential(credential_source: Any) -> bool:
    """Whether the reader of the error holds the credential that ran the call.

    Both halves are load-bearing, because ``platform`` means opposite things in
    the two host modes: in OSS the key is the operator's own ``.env`` entry and
    the operator is the user, while on the hosted service the user has no key at
    all and "check your API key" sends them to a page that is not the cause.
    An unknown source fails closed, since a wrong credential hint is worse than
    a missing one.
    """
    if app_settings.HOST_MODE == "oss":
        return True
    return str(credential_source) in ("oauth", "byok")


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


def model_call_failure(
    exc: BaseException, credential_source: Any
) -> Dict[str, Any]:
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
        "error_credential_owned": user_owns_credential(credential_source),
    }


def classify_stream_exception(exc: BaseException) -> Dict[str, Any]:
    """Classify a chat-stream exception as ``upstream`` or ``internal``.

    Walks ``__cause__`` / ``__context__`` so a wrapped provider error (e.g.
    a LangChain exception caused by ``anthropic.InternalServerError``) is
    still recognized as upstream. Returns a dict with ``kind``,
    ``status_code`` (when carried on the exception or parseable from its
    message), and ``provider_module`` (the matched SDK prefix, or None).
    """
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        module = getattr(type(current), "__module__", "") or ""
        for prefix in _UPSTREAM_MODULE_PREFIXES:
            if module == prefix or module.startswith(prefix + "."):
                status = getattr(current, "status_code", None)
                if not isinstance(status, int):
                    status = _parse_status_from_message(str(current))
                return {
                    "kind": "upstream",
                    "status_code": status if isinstance(status, int) else None,
                    "provider_module": prefix,
                }
        current = current.__cause__ or current.__context__

    return {"kind": "internal", "status_code": None, "provider_module": None}


def error_event_data(
    thread_id: str,
    error_message: str,
    *,
    exc: Optional[BaseException] = None,
    credential_source: Any = None,
) -> Dict[str, Any]:
    """The ``error`` frame's data.

    With ``exc`` the frame carries ``error_kind`` (``upstream`` or
    ``internal``), ``status_code`` when available, and ``hints`` for the
    frontend to render user-actionable guidance; the legacy ``error`` and
    ``message`` fields stay so older clients keep working. Hints are chosen by
    status and then filtered by who holds the credential (see
    ``user_owns_credential``), so a platform-billed turn never asks the user
    to check a key they do not have.
    """
    data: Dict[str, Any] = {
        "thread_id": thread_id,
        "error": sanitize_error_text(error_message),
        "message": "An error occurred during processing",
    }
    if exc is not None:
        info = classify_stream_exception(exc)
        trace = find_resilience_trace(exc)
        if trace is not None and info["kind"] == "internal":
            # The trace proves the failure happened inside a model call.
            # A generic wrapper exception (no recognizable SDK module in
            # the chain) must not demote it to "internal" — the frontend
            # routes internal errors to a generic banner that drops the
            # model / attempted-models context.
            primary_status: Any = None
            attempted_raw = trace.get("attempted_models")
            if (
                isinstance(attempted_raw, list)
                and attempted_raw
                and isinstance(attempted_raw[0], dict)
            ):
                primary_status = attempted_raw[0].get("status_code")
            info = {
                "kind": "upstream",
                "status_code": primary_status
                if isinstance(primary_status, int)
                else None,
                "provider_module": None,
            }
        data["error_kind"] = info["kind"]
        if info["status_code"] is not None:
            data["status_code"] = info["status_code"]
        if info["provider_module"]:
            data["provider_module"] = info["provider_module"]
        if info["kind"] == "upstream":
            # Order matters — frontend renders the hints as a list, so the
            # most relevant hint for this status goes first. 5xx/429 are
            # provider outages, not the user's credentials; showing
            # "check your API key" first on a 503 is misleading.
            status = info.get("status_code")
            if status in (401, 403):
                hints = ["api_key", "model_access", "try_another_model"]
            elif status == 404:
                hints = ["model_access", "try_another_model"]
            elif status in _REQUEST_REFUSED_STATUSES:
                hints = ["try_another_model"]
            elif status == 429 or (isinstance(status, int) and status >= 500):
                hints = ["provider_status", "try_another_model"]
            else:
                # No status (network error) — could be anything; show all.
                hints = [
                    "api_key",
                    "model_access",
                    "provider_status",
                    "try_another_model",
                ]
            # The status says what failed; the credential says who can act
            # on it. Drop the hints that would send a user to fix a key
            # they do not hold. Every branch above keeps
            # "try_another_model", so the list never empties.
            if not user_owns_credential(credential_source):
                hints = [h for h in hints if h not in _CREDENTIAL_HINTS]
            data["hints"] = hints
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
