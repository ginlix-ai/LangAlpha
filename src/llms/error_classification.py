"""Status-code extraction and retryability classification for LLM call errors.

Used by the agent's model-resilience middleware to decide whether a failed
model call is worth retrying on the same model or should move straight to the
next fallback. Mirrors the exception-chain walking in
``src/server/services/runs/stream_errors.py::classify_stream_exception`` — kept
separate because ptc_agent code must not import from src/server.
"""

from __future__ import annotations

import re

# Client-side errors that fail identically on every retry of the same model:
# bad request shape, auth, unknown model, oversized payload, unsupported
# params. 408 and 429 are absent on purpose — those are transient.
NON_RETRYABLE_STATUSES: frozenset[int] = frozenset({400, 401, 403, 404, 405, 413, 422})

# Raised by our own or an SDK's code on input it cannot handle: a field the
# provider never sent, a value of the wrong type, a method a proxy's payload
# lacks. The same response parses the same way, so a retry raises it again.
# The transports never raise these for a transient failure: httpx and httpcore
# map connection and timeout failures to their own classes, and the Anthropic
# and OpenAI SDKs re-raise those as their connection and timeout errors.
_PROGRAMMING_ERRORS = (AttributeError, TypeError, KeyError)

_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")

# What providers send for a request over the model's input limit, each one
# seen from the provider named. They answer 400, which alone says nothing.
# Codes are matched exactly; phrases lowercased, against each message in the
# chain.
_OVERFLOW_CODES = frozenset({
    "context_length_exceeded",
    "request_too_large",
    "1261",  # Z.ai's own API, under either wording below
})
_OVERFLOW_PHRASES = (
    "prompt is too long",  # Anthropic, and the endpoints compatible with it
    "exceed context limit",  # Anthropic: the input leaves no room for max_tokens
    "context_length_exceeded",
    "maximum context length",  # OpenAI-compatible servers, DeepSeek
    "exceeds the context window",  # OpenAI Responses, as a stream failure
    "input tokens exceed the configured limit",
    "exceeds the maximum number of tokens allowed",  # Gemini
    "exceeded model token limit",  # Moonshot
    "range of input length should be",  # DashScope
    "exceed max message tokens",  # Volcengine Ark
    "context window exceeds limit",  # MiniMax
    "prompt exceeds max length",  # Z.ai's own API
    "prompt 超长",
)


def _iter_exception_chain(exc: BaseException):
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def extract_status_code(exc: BaseException) -> int | None:
    """Best-effort HTTP status extraction from an exception chain.

    Checks ``status_code`` / ``response.status_code`` attributes on every
    exception in the chain, then falls back to parsing a 4xx/5xx code out of
    the exception messages.
    """
    for current in _iter_exception_chain(exc):
        status = getattr(current, "status_code", None)
        if isinstance(status, int):
            return status
        response = getattr(current, "response", None)
        status = getattr(response, "status_code", None) if response is not None else None
        if isinstance(status, int):
            return status

    for current in _iter_exception_chain(exc):
        # A timeout never carries a provider status, and its message can carry
        # numbers that look like one: ``chunks_received=413`` read as a 413
        # would make a stalled stream look like a payload the provider refused.
        if isinstance(current, TimeoutError):
            continue
        match = _STATUS_CODE_RE.search(str(current))
        if match:
            return int(match.group(1))

    return None


def is_stalled_stream(exc: BaseException) -> bool:
    """Whether a stream went silent past its chunk bound, before or after its
    first chunk.

    Duck-typed on the ``chunks_received`` a chunk-timeout error carries, for the
    same reason as ``is_mid_stream_error``. A stall that has repeated on a fresh
    connection at the same point is the model's output, not the network, and
    another identical request stalls there again. One that never sent a chunk
    counts too: the bound is long enough for a reasoning model's first chunk,
    so each attempt costs the whole of it.
    """
    return any(
        isinstance(current, TimeoutError)
        and isinstance(getattr(current, "chunks_received", None), int)
        for current in _iter_exception_chain(exc)
    )


def is_mid_stream_error(exc: BaseException) -> bool:
    """Whether the failure was reported inside an already-established stream.

    A provider that answers HTTP 200 and then reports failure as a stream event
    sets ``mid_stream`` on its exception. Duck-typed rather than keyed to a
    provider's exception class, so this module stays provider-agnostic and the
    alternative, matching error prose, never has to be written.
    """
    return any(
        getattr(current, "mid_stream", False) is True
        for current in _iter_exception_chain(exc)
    )


def is_programming_error(exc: BaseException) -> bool:
    """Whether a crash in the call path, not the provider or the network, caused it.

    Looked for along the whole chain because the SDKs wrap whatever their
    transport raises in a connection error, which hides the crash one level down.
    """
    return any(
        isinstance(current, _PROGRAMMING_ERRORS)
        for current in _iter_exception_chain(exc)
    )


def is_context_overflow(exc: BaseException) -> bool:
    """Whether the provider refused the request as larger than the model takes.

    No retry answers this, and no fallback whose window is no larger: each is
    sent the same history, so only a smaller history does, which is
    compaction's job.

    LangChain's integrations raise ``ContextOverflowError`` for the wordings
    they know, which this trusts first. The rest is matched on the code or the
    wording each provider was seen to send, since there is no common status: a
    model that is not ours to fix still has to be told apart from a bad request.
    An unmatched overflow keeps the old handling, never a worse one.
    """
    from langchain_core.exceptions import ContextOverflowError

    for current in _iter_exception_chain(exc):
        if isinstance(current, ContextOverflowError):
            return True
        # The status as the SDK set it, on the error or its response, never
        # parsed from prose: a message can quote "413" for any reason.
        response = getattr(current, "response", None)
        if 413 in (getattr(current, "status_code", None), getattr(response, "status_code", None)):
            return True
        if _OVERFLOW_CODES.intersection(_error_codes(current)):
            return True
        text = str(current).lower()
        if any(phrase in text for phrase in _OVERFLOW_PHRASES):
            return True
    return False


def _error_codes(exc: BaseException) -> set[str]:
    """The codes an SDK error carries: on the exception, or in its JSON body."""
    codes = {getattr(exc, "code", None), getattr(exc, "type", None)}
    body = getattr(exc, "body", None)
    error = body.get("error", body) if isinstance(body, dict) else None
    if isinstance(error, dict):
        codes.update({error.get("code"), error.get("type")})
    return {code for code in codes if isinstance(code, str)}


def is_retryable_error(exc: BaseException, status_code: int | None = None) -> bool:
    """Whether retrying the same model could plausibly succeed.

    Errors with no extractable status (connection resets, timeouts) stay
    retryable, unless a programming error sits in the chain: it fails the same
    way on every attempt. A status the provider reported decides first, so the
    check never overrides a provider's own verdict.
    """
    if status_code is None:
        status_code = extract_status_code(exc)
    if status_code is None:
        return not is_programming_error(exc)
    return status_code not in NON_RETRYABLE_STATUSES
