"""Error tracking with Sentry: exceptions only, no tracing, logs or replay.

Off unless ``SENTRY_DSN`` is set. Initialized per worker in the lifespan, like
the OTel runtime, so the SDK's transport thread belongs to the worker.

What leaves the process is kept to what triage needs. Stack frames go without
their local variables, which hold prompts, messages and tool output. Request
bodies, cookies and auth headers stay behind (``send_default_pii`` off), and
exception text is scrubbed for credentials the way error frames are.

A failed turn never raises out of the request: the run executor turns it into
an ``error`` frame and a terminal ledger write. So turns are captured
explicitly at finalize (``capture_turn_failure``), and only the ones that are
this service's fault. A provider's 5xx, 429 or refused key is the provider's
outage and reaches the user with hints, so it is not an issue here.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

_enabled = False


def init_sentry() -> None:
    global _enabled
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn or _enabled:
        return
    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
    except ImportError:
        logger.warning("SENTRY_DSN is set but sentry-sdk is not installed")
        return

    from src.observability.langsmith_run import build_sha

    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.getenv("SENTRY_ENVIRONMENT", "").strip() or None,
            release=build_sha(),
            send_default_pii=False,
            include_local_variables=False,
            max_request_body_size="never",
            traces_sample_rate=0.0,
            # No log line reaches Sentry, not even as a breadcrumb: a log line
            # can quote the user's research (their todos, a tool's output),
            # which no scrub tells apart from ordinary text. Promoting error
            # logs to events would also spend the quota on the ~400K lines a
            # day this service writes, most of them handled and expected.
            integrations=[LoggingIntegration(level=None, event_level=None)],
            before_send=_before_send,
        )
    except Exception:
        # A malformed DSN is a missing error tracker, not a reason to keep
        # the service from starting.
        logger.error("Sentry not enabled: SENTRY_DSN rejected", exc_info=True)
        return
    _enabled = True
    logger.info("Sentry error tracking enabled")


_CRUMB_DATA_KEPT = frozenset({"method", "url", "status_code", "reason"})


def _before_send(event: dict[str, Any], hint: dict[str, Any]) -> Optional[dict[str, Any]]:
    from src.server.utils.secret_redactor import scrub_outbound_text as _scrub

    exc_info = hint.get("exc_info")
    if exc_info and _is_expected(exc_info[1]):
        return None
    # The method and path are enough to find the route. Headers can carry the
    # service token, whose name the SDK's scrubber does not know, and a query
    # string a stream's token.
    request = event.get("request")
    if isinstance(request, dict):
        event["request"] = {k: request[k] for k in ("method", "url") if k in request}
    for exc in (event.get("exception") or {}).get("values") or ():
        if isinstance(exc.get("value"), str):
            exc["value"] = _scrub(exc["value"])
    for crumb in (event.get("breadcrumbs") or {}).get("values") or ():
        if isinstance(crumb.get("message"), str):
            crumb["message"] = _scrub(crumb["message"])
        # An outgoing call's breadcrumb carries its query string unsanitized
        # (``http.query``), where a vendor's API key often rides. The call's
        # method, address and status are enough to follow it.
        data = crumb.get("data")
        if isinstance(data, dict):
            crumb["data"] = {
                k: _scrub(v) if isinstance(v, str) else v
                for k, v in data.items()
                if k in _CRUMB_DATA_KEPT
            }
    logentry = event.get("logentry")
    if isinstance(logentry, dict) and isinstance(logentry.get("message"), str):
        logentry["message"] = _scrub(logentry["message"])
    return event


def _is_expected(exc: BaseException) -> bool:
    """Failures that are not this service's bug: the client went away, the
    request was refused on purpose, or a model provider failed."""
    import asyncio

    from fastapi import HTTPException

    if isinstance(exc, asyncio.CancelledError):
        return True
    if isinstance(exc, HTTPException):
        # A refusal is on purpose; a 5xx is a failure a route caught and
        # rewrote.
        return exc.status_code < 500
    from src.server.services.runs.stream_errors import failure_kind

    return failure_kind(exc)["kind"] != "internal"


def capture_turn_failure(
    exc: BaseException,
    *,
    thread_id: str,
    run_id: Optional[str],
    user_id: Optional[str],
    workspace_id: Optional[str],
) -> Optional[str]:
    """Send a failed turn's exception, tagged with the ids a report carries,
    so a report's ``run_id`` finds its issue. Returns the event id."""
    if not _enabled:
        return None
    try:
        import sentry_sdk

        with sentry_sdk.new_scope() as scope:
            if user_id:
                scope.set_user({"id": user_id})
            scope.set_tag("thread_id", thread_id)
            if run_id:
                scope.set_tag("run_id", run_id)
            if workspace_id:
                scope.set_tag("workspace_id", workspace_id)
            return scope.capture_exception(exc)
    except Exception:
        logger.warning("Sentry capture failed", exc_info=True)
        return None


def sentry_issues_url(run_id: Optional[str]) -> Optional[str]:
    """The Sentry search for a run's issues, for a report to link to.

    A search rather than one event's page: the event id is known only to the
    worker that captured it, and the run id is on every event of the run.
    """
    org = os.getenv("SENTRY_ORG", "").strip()
    if not (_enabled and org and run_id):
        return None
    return f"https://{org}.sentry.io/issues/?query={quote(f'run_id:{run_id}')}"
