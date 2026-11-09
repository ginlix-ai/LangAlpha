"""Reports to the platform: bug reports, feedback and failed-turn reports, and
the reports a thumbs-down sends.

LangAlpha keeps nothing. It names what the report is about by id, adds the
links triage opens (the LangSmith trace, the Sentry issues), scrubs the
user's text for credentials, and forwards it to the platform's
``POST /api/auth/reports`` with the service token, the pair billing leases
already use. The platform adds the plan, applies the per-user limits and
posts it to Slack. A deployment without ``AUTH_SERVICE_URL`` has nowhere to
send reports, so the feature is off there.

A report never carries the conversation: ids and links only, plus the words
the user wrote to us.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Optional
from uuid import UUID, uuid5

from pydantic import BaseModel, Field

from src.config.settings import AUTH_SERVICE_URL, HOST_MODE
from src.observability.langsmith_feedback import trace_url
from src.server.utils.secret_redactor import redact_capability_paths
from src.server.models.report import (
    PlatformStatus,
    ReportClient,
    ReportCreate,
    ReportError,
)

logger = logging.getLogger(__name__)

# Past the platform's own budget for posting to Slack, so a slow post that
# lands is answered as accepted rather than given up on.
_FORWARD_TIMEOUT_S = 45.0
# The platform's limits on a note and on an error's text; scrubbing can
# lengthen text already at the limit.
_NOTE_MAX = 4000
_ERROR_MAX = 2000


def reports_enabled() -> bool:
    return HOST_MODE != "oss" and bool(AUTH_SERVICE_URL)


@dataclass(frozen=True)
class PlatformResult:
    """What the platform made of a call. A retry after ``rate_limited`` or
    ``unavailable`` keeps its ids, so nothing is posted twice."""

    status: PlatformStatus
    retry_after: Optional[int] = None

    @property
    def ok(self) -> bool:
        return self.status in ("accepted", "duplicate")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PlatformReport(BaseModel):
    """The body of the platform's ``POST /api/auth/reports``."""

    report_id: str
    event: Literal["user_report", "error_report", "refund_review"]
    source: Literal["button", "error_banner", "thumbs"]
    user_id: str
    client: ReportClient
    workspace_id: Optional[str] = None
    thread_id: Optional[str] = None
    run_id: Optional[str] = None
    turn_index: Optional[int] = None
    category: Optional[str] = None
    issue_categories: list[str] = Field(default_factory=list)
    user_note: Optional[str] = None
    description: Optional[str] = None
    error: Optional[ReportError] = None
    trace_url: Optional[str] = None
    sentry_url: Optional[str] = None
    created_at: str = Field(default_factory=_now)


class ScrubUnavailable(Exception):
    """The user's vault could not be read, so their text cannot be cleared to
    leave: the report is refused as unavailable, never sent unscrubbed."""


async def user_vault(user_id: str) -> dict[str, str]:
    """The values of the user's own secrets, to scrub from what they send."""
    from src.server.utils.secret_redactor import get_vault_secrets_for_redaction

    try:
        return await get_vault_secrets_for_redaction(user_id)
    except Exception as e:
        logger.warning("Vault unreadable for user %s, report held: %s", user_id, e)
        raise ScrubUnavailable from e


def _scrub(text: Optional[str], vault: dict[str, str], limit: int = _NOTE_MAX) -> Optional[str]:
    from src.server.utils.secret_redactor import scrub_outbound_text

    return scrub_outbound_text(text, vault)[:limit] if text else text


def _scrub_client(client: ReportClient, vault: dict[str, str]) -> ReportClient:
    """The app as a client named it: its own words, scrubbed as a note is."""
    return client.model_copy(
        update={"app_version": _scrub(client.app_version, vault, 64), "locale": _scrub(client.locale, vault, 35)}
    )


async def _workspace_id(thread_id: str) -> Optional[str]:
    from src.server.database.conversation.threads_read import get_thread_by_id

    thread = await get_thread_by_id(thread_id)
    return str(thread["workspace_id"]) if thread and thread.get("workspace_id") else None


async def _failed_run(thread_id: str, run_id: Optional[UUID]) -> Optional[dict[str, Any]]:
    """The run a failed-turn report is about: the one its banner named, when
    it is this thread's, else, from a client that names none, the thread's
    latest attempt when that attempt failed."""
    from src.server.database.runs.lifecycle import get_latest_attempt, get_run

    if run_id:
        run = await get_run(str(run_id))
        return run if run and str(run["conversation_thread_id"]) == thread_id else None
    run = await get_latest_attempt(thread_id)
    return run if run and run.get("status") == "error" else None


async def build_report(report: ReportCreate, *, user_id: str) -> PlatformReport:
    """A report from the report dialog, as the platform takes it. The caller
    has checked that the user owns ``report.thread_id``. A failed turn's
    report links its run's trace and Sentry issues. Raises
    ``ScrubUnavailable`` when the user's vault cannot be read."""
    from src.observability.sentry import sentry_issues_url

    vault = await user_vault(user_id)

    thread_id = str(report.thread_id) if report.thread_id else None
    workspace_id = await _workspace_id(thread_id) if thread_id else None
    run = (
        await _failed_run(thread_id, report.run_id)
        if thread_id and report.event == "error_report"
        else None
    )
    run_id = str(run["conversation_response_id"]) if run else None
    error = report.error
    return PlatformReport(
        report_id=str(report.report_id),
        event=report.event,
        source=report.source,
        user_id=user_id,
        client=_scrub_client(report.client, vault),
        workspace_id=workspace_id,
        thread_id=thread_id,
        run_id=run_id,
        turn_index=run.get("turn_index") if run else None,
        category=report.category,
        user_note=_scrub(report.user_note, vault),
        error=error.model_copy(
            update={
                "type": _scrub(redact_capability_paths(error.type), vault, 200),
                # A provider's error can quote an address it failed to fetch,
                # such as a file grant the agent handed it.
                "message": _scrub(redact_capability_paths(error.message), vault, _ERROR_MAX),
            }
        )
        if error
        else None,
        trace_url=await trace_url(run_id),
        sentry_url=sentry_issues_url(run_id),
    )


# ==================== Thumbs-down ====================

# Every report a thumbs-down sends is named from its rating's report id, so
# each is posted once however often it is sent, and withdrawing finds them.
_REFUND_NAMESPACE = UUID("6f1d3c52-8b0e-4f4a-9a57-2f6c1e0b7d41")
_REPLY_NAMESPACE = UUID("2b8f0e7a-51c4-4d0b-9f3e-7a6d2c9e4b18")


@dataclass(frozen=True)
class ThumbsDown:
    """A thumbs-down rating, as the reports it sends need it."""

    # conversation_feedback.report_id: new each time a rating becomes a
    # thumbs-down, so a new thumbs-down is a new report.
    report_id: str
    # The attempt the user rated.
    run_id: str
    user_id: str
    thread_id: str
    turn_index: int
    issue_categories: list[str]
    comment: Optional[str]

    @classmethod
    def from_row(cls, row: Mapping[str, Any], thread_id: str, turn_index: int) -> ThumbsDown:
        """From a ``conversation_feedback`` row."""
        return cls(
            report_id=str(row["report_id"]),
            run_id=str(row["conversation_response_id"]),
            user_id=str(row["user_id"]),
            thread_id=thread_id,
            turn_index=turn_index,
            issue_categories=list(row.get("issue_categories") or [])[:20],
            comment=row.get("comment"),
        )

    @property
    def refund_id(self) -> str:
        """The refund request a human review asked for after the rating."""
        return str(uuid5(_REFUND_NAMESPACE, f"{self.report_id}:refund_review"))


async def send_thumbs_down(
    rating: ThumbsDown, *, human_review: bool, client: Optional[ReportClient]
) -> PlatformResult:
    """The report a thumbs-down's rating stands for, as feedback on that turn.

    With "Request human review" it is a refund request, to the refunds
    channel, and carries the trace link: ticking it is the user agreeing that
    the team reads the conversation. Without it the report names the turn by
    id and leaves the link out.
    """
    return await _post_thumbs(rating, rating.report_id, human_review, client)


async def request_thumbs_review(
    rating: ThumbsDown, *, client: Optional[ReportClient]
) -> PlatformResult:
    """A human review asked for after the rating was sent: its own refund
    request, beside the report the rating sent."""
    return await _post_thumbs(rating, rating.refund_id, True, client)


async def _post_thumbs(
    rating: ThumbsDown,
    report_id: str,
    human_review: bool,
    client: Optional[ReportClient],
) -> PlatformResult:
    try:
        vault = await user_vault(rating.user_id)
    except ScrubUnavailable:
        return PlatformResult("unavailable")
    return await post_report(
        PlatformReport(
            report_id=report_id,
            event="refund_review" if human_review else "user_report",
            source="thumbs",
            user_id=rating.user_id,
            client=_scrub_client(client or ReportClient(surface="web"), vault),
            workspace_id=await _workspace_id(rating.thread_id),
            thread_id=rating.thread_id,
            run_id=rating.run_id,
            turn_index=rating.turn_index,
            category=None if human_review else "feedback",
            # Free text to the API, as the comment is.
            issue_categories=[c for c in (_scrub(c, vault, 64) for c in rating.issue_categories) if c],
            user_note=_scrub(rating.comment, vault),
            trace_url=await trace_url(rating.run_id) if human_review else None,
        )
    )


async def reply_to_thumbs_down(rating: ThumbsDown, note: str, *, note_id: str) -> PlatformResult:
    """Post a note under the rating's report. ``note_id`` is made by the
    client for the note and kept across its retries, so the note is posted
    once however often it is sent."""
    try:
        vault = await user_vault(rating.user_id)
    except ScrubUnavailable:
        return PlatformResult("unavailable")
    return await _post(
        f"/api/auth/reports/{rating.report_id}/replies",
        {
            "user_id": rating.user_id,
            "note": _scrub(note, vault),
            "reply_id": str(uuid5(_REPLY_NAMESPACE, f"{rating.report_id}:{note_id}")),
            "created_at": _now(),
        },
        rating.report_id,
    )


async def withdraw_thumbs_down(rating: ThumbsDown) -> PlatformResult:
    """Withdraw the rating's reports: the platform takes the user's words out
    of Slack, marks them withdrawn and cancels a pending review."""
    results = await asyncio.gather(
        *(_withdraw(report_id, rating.user_id) for report_id in (rating.report_id, rating.refund_id))
    )
    return next((r for r in results if not r.ok), PlatformResult("accepted"))


async def _withdraw(report_id: str, user_id: str) -> PlatformResult:
    result = await _post(f"/api/auth/reports/{report_id}/withdraw", {"user_id": user_id}, report_id)
    # A report the platform never got, such as a review never asked for, is
    # already gone.
    return PlatformResult("accepted") if result.status == "not_found" else result


# ==================== Transport ====================


async def post_report(report: PlatformReport) -> PlatformResult:
    return await _post("/api/auth/reports", report.model_dump(mode="json"), report.report_id)


async def _post(path: str, body: dict[str, Any], report_id: str) -> PlatformResult:
    """The one call to the platform, and what each of its answers means."""
    from src.server.dependencies.usage_limits import service_headers
    from src.server.utils.http_client import get_http_client

    try:
        # httpx's timeout bounds each phase (connect, write, every read), not
        # the call, so the whole call is bounded too: the client gives up at
        # 60s, and an answer after that would land on a rolled-back dialog.
        async with asyncio.timeout(_FORWARD_TIMEOUT_S):
            resp = await (await get_http_client()).post(
                f"{AUTH_SERVICE_URL.rstrip('/')}{path}",
                json=body,
                headers=service_headers(),
                timeout=_FORWARD_TIMEOUT_S,
            )
    except Exception as e:
        logger.warning("Platform call %s failed: %s", path, e)
        return PlatformResult("unavailable")

    code = resp.status_code
    if code in (200, 202):
        duplicate = _body(resp).get("status") == "duplicate"
        return PlatformResult("duplicate" if duplicate else "accepted")
    if code == 404:
        detail = _body(resp).get("detail")
        if isinstance(detail, dict) and detail.get("error") == "report_not_found":
            return PlatformResult("not_found")
        # Any other 404 is a platform without the route, and a withdrawal must
        # not read it as a report already gone.
        logger.error("Platform at %s has no route for %s", AUTH_SERVICE_URL, path)
        return PlatformResult("unavailable")
    if code == 409:
        detail = _body(resp).get("detail")
        error = detail.get("error") if isinstance(detail, dict) else None
        return PlatformResult("note_limit" if error == "reply_limit_reached" else "closed")
    retry_after = _retry_after(resp.headers.get("Retry-After"))
    if code == 429:
        return PlatformResult("rate_limited", retry_after)
    if code < 500:
        # A refused payload or token is this deployment's fault, not the
        # user's, and retrying sends the same thing.
        logger.error(
            "Platform refused %s for report %s: HTTP %d %s",
            path, report_id, code, resp.text[:300],
        )
    return PlatformResult("unavailable", retry_after)


def _body(resp) -> dict[str, Any]:
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _retry_after(value: Optional[str]) -> Optional[int]:
    try:
        return max(1, int(value)) if value else None
    except ValueError:
        return None
