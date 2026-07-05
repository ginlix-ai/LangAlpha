"""Status-code extraction and retryability classification for LLM call errors.

Used by the agent's model-resilience middleware to decide whether a failed
model call is worth retrying on the same model or should move straight to the
next fallback. Mirrors the exception-chain walking in
``src/server/handlers/streaming_handler.py::classify_stream_exception`` — kept
separate because ptc_agent code must not import from src/server.
"""

from __future__ import annotations

import re

# Client-side errors that fail identically on every retry of the same model:
# bad request shape, auth, unknown model, oversized payload, unsupported
# params. 408 and 429 are absent on purpose — those are transient.
NON_RETRYABLE_STATUSES: frozenset[int] = frozenset({400, 401, 403, 404, 405, 413, 422})

_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")


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
        match = _STATUS_CODE_RE.search(str(current))
        if match:
            return int(match.group(1))

    return None


def is_retryable_error(exc: BaseException, status_code: int | None = None) -> bool:
    """Whether retrying the same model could plausibly succeed.

    Errors with no extractable status (connection resets, timeouts) stay
    retryable.
    """
    if status_code is None:
        status_code = extract_status_code(exc)
    return status_code not in NON_RETRYABLE_STATUSES
