"""Error tracking with Sentry: exceptions only, no tracing, logs or replay.

Off unless ``SENTRY_DSN`` is set. Initialized per worker in the lifespan, like
the OTel runtime, so the SDK's transport thread belongs to the worker.

What leaves the process is kept to what triage needs. Stack frames go without
their local variables, which hold prompts, messages and tool output. Request
bodies, cookies and auth headers stay behind (``send_default_pii`` off), a
request's path goes only as its route's template, and no log line goes, even
as a breadcrumb. Exception text is scrubbed for credential shapes the way
error frames are, and on a failed turn also for the values of that user's
own secrets, which no pattern knows; any other event (a route's 5xx) has no
user to read them for. ``extra`` goes empty, and ``contexts`` keeps only the
runtime, OS and trace, so text an integration adds later is not sent on.

A failed turn never raises out of the request: the run executor turns it into
an ``error`` frame and a terminal ledger write. So turns are captured
explicitly at finalize (``capture_turn_failure``), and only the ones that are
this service's fault. A provider's 5xx, 429 or refused key is the provider's
outage and reaches the user with hints, so it is not an issue here.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from contextvars import ContextVar
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
_CONTEXTS_KEPT = frozenset({"runtime", "os", "trace"})

# The secrets of the user a failed turn's event is about, set around its
# capture (``before_send`` runs inside it, in the same context). ``_WITHHELD``
# when their vault could not be read: the event keeps its type and stack and
# loses its text. Unset for any other event, which has no user to read for.
_WITHHELD: dict[str, str] = {}
# For an event whose text can quote the user's data, sent with none of it.
_NO_TEXT: dict[str, str] = {}
_event_vault: ContextVar[Optional[dict[str, str]]] = ContextVar("sentry_event_vault", default=None)


def _before_send(event: dict[str, Any], hint: dict[str, Any]) -> Optional[dict[str, Any]]:
    from src.server.utils.secret_redactor import redact_capability_paths, scrub_outbound_text

    exc_info = hint.get("exc_info")
    if exc_info and _is_expected(exc_info[1]):
        return None
    vault = _event_vault.get()
    if vault is _WITHHELD or vault is _NO_TEXT:
        reason = "the user's secrets could not be read" if vault is _WITHHELD else "it can quote the user's data"
        def _scrub(_text: str) -> str:
            return f"[withheld: {reason}]"
    else:
        def _scrub(text: str) -> str:
            # An error the service raised can quote the address it failed on,
            # which may be a capability (a file grant, a share token).
            return scrub_outbound_text(redact_capability_paths(text), vault)
    # The method and the matched route are enough to find it. Headers can
    # carry the service token, whose name the SDK's scrubber does not know, a
    # query string a stream's token, and the path itself a credential (a file
    # grant, a share code), so the path is sent only as its route's template.
    routed = (event.get("transaction_info") or {}).get("source") == "route"
    request = event.get("request")
    if isinstance(request, dict):
        kept = {"method": request["method"]} if "method" in request else {}
        if routed:
            kept["url"] = event.get("transaction")
        event["request"] = kept
    # A transaction named before any route matched is the raw path, which
    # can be a credential itself.
    if not routed:
        event.pop("transaction", None)
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
    # A captured message's text, whoever captured it; a log record's
    # arguments are dropped, since only their formatted text is scrubbed.
    if isinstance(event.get("message"), str):
        event["message"] = _scrub(event["message"])
    logentry = event.get("logentry")
    if isinstance(logentry, dict):
        for key in ("message", "formatted"):
            if isinstance(logentry.get(key), str):
                logentry[key] = _scrub(logentry[key])
        logentry.pop("params", None)
    event.pop("extra", None)
    contexts = event.get("contexts")
    if isinstance(contexts, dict):
        event["contexts"] = {k: v for k, v in contexts.items() if k in _CONTEXTS_KEPT}
    return event


def _is_expected(exc: BaseException) -> bool:
    """Failures that are not this service's bug: the client went away, the
    request was refused on purpose, or a model provider failed."""
    from fastapi import HTTPException

    if isinstance(exc, asyncio.CancelledError):
        return True
    if isinstance(exc, HTTPException):
        # A refusal is on purpose; a 5xx is a failure a route caught and
        # rewrote.
        return exc.status_code < 500
    from src.server.services.runs.stream_errors import failure_kind

    return failure_kind(exc)["kind"] != "internal"


async def capture_turn_failure(
    exc: BaseException,
    *,
    thread_id: str,
    run_id: Optional[str],
    user_id: Optional[str],
    workspace_id: Optional[str],
) -> Optional[str]:
    """Send a failed turn's exception, tagged with the ids a report carries,
    so a report's ``run_id`` finds its issue, and scrubbed of the user's
    secrets, which a tool's error can quote. Returns the event id."""
    if not _enabled:
        return None
    vault: dict[str, str] = {}
    if user_id:
        from src.server.utils.secret_redactor import get_vault_secrets_for_redaction

        try:
            # Bounded: the caller is finalizing the run, which must not wait
            # on a best-effort report.
            vault = await asyncio.wait_for(get_vault_secrets_for_redaction(user_id), timeout=5)
        except Exception:
            logger.warning("Vault unreadable for user %s; event text withheld", user_id)
            vault = _WITHHELD
    return _capture(exc, vault, user_id=user_id, thread_id=thread_id, run_id=run_id, workspace_id=workspace_id)


def capture_finalize_failure(exc: BaseException, *, thread_id: str, run_id: str) -> Optional[str]:
    """Send a failure to write a run's terminal state, which otherwise only
    logs. A database error quotes the row it refused, which can be the user's
    words, so the event keeps its type and stack and loses its text."""
    if not _enabled:
        return None
    return _capture(exc, _NO_TEXT, user_id=None, thread_id=thread_id, run_id=run_id, workspace_id=None)


def _capture(
    exc: BaseException,
    vault: dict[str, str],
    *,
    user_id: Optional[str],
    thread_id: str,
    run_id: Optional[str],
    workspace_id: Optional[str],
) -> Optional[str]:
    token = _event_vault.set(vault)
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
    finally:
        _event_vault.reset(token)


_ORG_SLUG = re.compile(r"[a-z0-9][a-z0-9_-]*")


def sentry_issues_url(run_id: Optional[str]) -> Optional[str]:
    """The Sentry search for a run's issues, for a report to link to.

    A search rather than one event's page: the event id is known only to the
    worker that captured it, and the run id is on every event of the run.
    """
    org = os.getenv("SENTRY_ORG", "").strip()
    # A slug, or no link: the platform refuses a report whose link is not a
    # clean URL, so a mistyped org would refuse every failed turn's report.
    if not (_enabled and run_id and _ORG_SLUG.fullmatch(org)):
        return None
    return f"https://{org}.sentry.io/issues/?query={quote(f'run_id:{run_id}')}"
