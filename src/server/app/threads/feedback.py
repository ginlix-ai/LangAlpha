"""Per-turn user feedback: submit, list, remove, and the report a thumbs-down
sends to the team, which lives as long as the thumbs-down (add to it while it
stands; removing the rating, or changing it to a thumbs-up, withdraws it).
Every rating is also kept as feedback on its run in LangSmith, after the
response (``observability.langsmith_feedback``)."""

import asyncio
from typing import Any, Awaitable, Optional

from fastapi import BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.observability.langsmith_feedback import mirror_rating, remove_rating, tracing_enabled
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
)
from src.server.models.conversation import (
    FeedbackFollowUpRequest,
    FeedbackRequest,
    FeedbackResponse,
)
from src.server.models.report import ReportClient
from src.server.app.reports import report_refusal
from src.server.services.reports import (
    PlatformResult,
    ScrubUnavailable,
    ThumbsDown,
    reply_to_thumbs_down,
    reports_enabled,
    request_thumbs_review,
    send_thumbs_down,
    user_vault,
    withdraw_thumbs_down,
)
from src.server.database.conversation import (
    upsert_feedback,
    get_feedback_for_thread,
)
from src.server.database.conversation.feedback import (
    add_feedback_follow_up,
    delete_feedback_by_id,
    get_feedback_by_id,
    get_feedback_for_turn,
    mark_feedback_reported,
    request_feedback_review,
)

from ._deps import logger, router


def _response(
    row: dict[str, Any], turn_index: int, report_status: Optional[str] = None
) -> FeedbackResponse:
    return FeedbackResponse(
        conversation_feedback_id=str(row["conversation_feedback_id"]),
        turn_index=turn_index,
        rating=row["rating"],
        issue_categories=row.get("issue_categories"),
        comment=row.get("comment"),
        consent_human_review=row.get("consent_human_review", False),
        review_status=row.get("review_status"),
        created_at=str(row["created_at"]),
        report_id=str(row["report_id"]) if row.get("report_id") else None,
        reported_at=row["reported_at"].isoformat() if row.get("reported_at") else None,
        report_follow_ups=row.get("report_follow_ups") or [],
        report_status=report_status,
    )


def _has_report(row: Optional[dict[str, Any]]) -> bool:
    return bool(
        row and row["rating"] == "thumbs_down" and row.get("report_id") and reports_enabled()
    )


async def _send_report(
    row: dict[str, Any], rating: ThumbsDown, client: Optional[ReportClient]
) -> tuple[PlatformResult, dict[str, Any]]:
    """Send the report a thumbs-down's rating stands for, and note when the
    team has it. Returns what the platform said and the row as it now stands."""
    sent = await send_thumbs_down(
        rating, human_review=bool(row["consent_human_review"]), client=client
    )
    if not sent.ok:
        return sent, row
    marked = await mark_feedback_reported(str(row["conversation_feedback_id"]), rating.report_id)
    if marked is None:
        # The rating was removed, or changed to a thumbs-up, while this was
        # on its way, and that withdrawal found nothing yet to take back. The
        # team no longer has it, so the caller must not say it was sent.
        await withdraw_thumbs_down(rating)
        return PlatformResult("closed"), row
    return sent, marked


async def _sync_langsmith(conversation_feedback_id: str) -> None:
    """Write the rating as it now stands to LangSmith, or remove it there.
    Read again after the response, so a request that lost a race with another
    does not write its stale copy over the newer one."""
    if not tracing_enabled():
        return
    row = await get_feedback_by_id(conversation_feedback_id)
    if row is None:
        await asyncio.to_thread(remove_rating, conversation_feedback_id)
        return
    try:
        vault = await user_vault(str(row["user_id"]))
    except ScrubUnavailable:
        return
    await asyncio.to_thread(mirror_rating, row, vault)


async def _kept(update: Awaitable[Optional[dict[str, Any]]], rating: ThumbsDown) -> dict[str, Any]:
    """Record what the platform just took on the rating. When the rating no
    longer stands for that report, take the report back and answer 409."""
    row = await update
    if row is None:
        await withdraw_thumbs_down(rating)
        raise HTTPException(status_code=409, detail={"error": "report_closed"})
    return row


async def _withdraw_report(
    row: Optional[dict[str, Any]], thread_id: str, turn_index: int
) -> Optional[JSONResponse]:
    """Withdraw the report of a thumbs-down that is about to stop being one.
    The refusal to answer when the platform could not; the rating then stays,
    so a retry can finish."""
    if not (row and row["rating"] == "thumbs_down" and row.get("report_id")):
        return None
    if not reports_enabled():
        # A report the team got before reports were turned off here cannot be
        # withdrawn now, and the rating is its only record.
        return report_refusal(PlatformResult("unavailable")) if row.get("reported_at") else None
    done = await withdraw_thumbs_down(ThumbsDown.from_row(row, thread_id, turn_index))
    return None if done.ok else report_refusal(done)


# ==================== Feedback ====================


@router.post("/{thread_id}/feedback", response_model=FeedbackResponse)
async def submit_feedback(
    thread_id: str,
    request: FeedbackRequest,
    x_user_id: CurrentUserId,
    background: BackgroundTasks,
):
    """Submit or update feedback (thumbs up/down) for a response. A
    thumbs-down is also sent to the team as a report; a thumbs-up over one
    withdraws it."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        prior = await get_feedback_for_turn(thread_id, request.turn_index, x_user_id)
        if request.rating != "thumbs_down":
            if refused := await _withdraw_report(prior, thread_id, request.turn_index):
                return refused
        # Sent again over a thumbs-down the team already has, the rating keeps
        # its report, so a review asked for only now goes as its own refund
        # request, as one asked for from the sent dialog does, and is recorded
        # once the platform has it: recorded first, a refused one would read as
        # requested and never be offered again.
        review_later = bool(
            request.rating == "thumbs_down"
            and request.consent_human_review
            and _has_report(prior)
            and prior["reported_at"]
            and not prior["consent_human_review"]
        )
        result = await upsert_feedback(
            conversation_thread_id=thread_id,
            turn_index=request.turn_index,
            user_id=x_user_id,
            rating=request.rating,
            issue_categories=request.issue_categories,
            comment=request.comment,
            consent_human_review=request.consent_human_review and not review_later,
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No response found at turn_index={request.turn_index}",
            )
        report_status = None
        if _has_report(result):
            rating = ThumbsDown.from_row(result, thread_id, request.turn_index)
            sent, result = await _send_report(result, rating, request.client)
            report_status = sent.status
            if sent.ok and review_later:
                review = await request_thumbs_review(rating, client=request.client)
                report_status = review.status
                if review.ok:
                    result = await _kept(
                        request_feedback_review(str(result["conversation_feedback_id"]), rating.report_id),
                        rating,
                    )
        background.add_task(_sync_langsmith, str(result["conversation_feedback_id"]))
        return _response(result, request.turn_index, report_status)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error submitting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit feedback")


@router.post("/{thread_id}/feedback/follow-up", response_model=FeedbackResponse)
async def follow_up_feedback(
    thread_id: str,
    request: FeedbackFollowUpRequest,
    x_user_id: CurrentUserId,
    background: BackgroundTasks,
):
    """Add to the report a thumbs-down sent: a human review asked for, a note
    posted under it, or with neither, the report sent again if the team never
    got it. Each step is recorded once the platform has it and is posted once
    however often it is sent, so a retry after a 429 or 503 finishes the rest.

    A review or note that lands after another request removed the rating is
    withdrawn again, since nothing would be left to withdraw it through."""
    note = (request.note or "").strip()
    if note and not request.note_id:
        raise HTTPException(status_code=422, detail="A note needs its note_id")

    await auth_api.require_thread_owner(thread_id, x_user_id)
    row = await get_feedback_for_turn(thread_id, request.turn_index, x_user_id)
    if not _has_report(row):
        raise HTTPException(status_code=404, detail="No thumbs-down report on this turn")
    rating = ThumbsDown.from_row(row, thread_id, request.turn_index)
    feedback_id = str(row["conversation_feedback_id"])

    if not row["reported_at"]:
        sent, row = await _send_report(row, rating, request.client)
        if not sent.ok:
            return report_refusal(sent)

    if request.human_review and not row["consent_human_review"]:
        review = await request_thumbs_review(rating, client=request.client)
        if not review.ok:
            return report_refusal(review)
        row = await _kept(request_feedback_review(feedback_id, rating.report_id), rating)

    if note:
        note_id = str(request.note_id)
        posted = await reply_to_thumbs_down(rating, note, note_id=note_id)
        if not posted.ok:
            return report_refusal(posted)
        row = await _kept(
            add_feedback_follow_up(feedback_id, rating.report_id, note_id, note), rating
        )

    background.add_task(_sync_langsmith, feedback_id)
    return _response(row, request.turn_index, "accepted")


@router.get("/{thread_id}/feedback", response_model=list[FeedbackResponse])
async def get_feedback(thread_id: str, x_user_id: CurrentUserId):
    """Get all feedback for a thread by the current user."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        rows = await get_feedback_for_thread(thread_id, x_user_id)
        return [_response(row, row["turn_index"]) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get feedback")


@router.delete("/{thread_id}/feedback")
async def remove_feedback(
    thread_id: str,
    turn_index: int,
    x_user_id: CurrentUserId,
    background: BackgroundTasks,
):
    """Remove feedback for a specific response. Query param: ?turn_index=N

    A thumbs-down's report is withdrawn first: the team's post loses the
    user's words and a pending human review is cancelled. When the platform
    cannot do that now, the 429 or 503 leaves the rating in place."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        row = await get_feedback_for_turn(thread_id, turn_index, x_user_id)
        if not row:
            raise HTTPException(status_code=404, detail="Feedback not found")
        if refused := await _withdraw_report(row, thread_id, turn_index):
            return refused
        await delete_feedback_by_id(str(row["conversation_feedback_id"]))
        background.add_task(_sync_langsmith, str(row["conversation_feedback_id"]))
        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete feedback")
