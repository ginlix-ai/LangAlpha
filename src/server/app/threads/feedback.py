"""Per-turn user feedback routes: submit, follow up, list and remove a rating.
The rating and the report a thumbs-down sends live in
``services.feedback``; these routes check ownership and say what happened in
HTTP terms."""

from typing import Any, Optional

from fastapi import BackgroundTasks, HTTPException

# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
)
from src.server.models.conversation import (
    FeedbackFollowUpRequest,
    FeedbackRequest,
    FeedbackResponse,
)
from src.server.app.reports import report_refusal
from src.server.services import feedback as ratings
from src.server.services.feedback import NotFound, ReportClosed, ReportRefused

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


def _report_closed() -> HTTPException:
    return HTTPException(status_code=409, detail={"error": "report_closed"})


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
        row, report_status = await ratings.submit(
            thread_id,
            request.turn_index,
            x_user_id,
            rating=request.rating,
            issue_categories=request.issue_categories,
            comment=request.comment,
            consent_human_review=request.consent_human_review,
            client=request.client,
        )
    except ReportRefused as refused:
        return report_refusal(refused.result)
    except ReportClosed:
        raise _report_closed()
    except NotFound:
        raise HTTPException(status_code=404, detail=f"No response found at turn_index={request.turn_index}")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error submitting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit feedback")
    background.add_task(ratings.sync_langsmith, str(row["conversation_feedback_id"]))
    return _response(row, request.turn_index, report_status)


@router.post("/{thread_id}/feedback/follow-up", response_model=FeedbackResponse)
async def follow_up_feedback(
    thread_id: str,
    request: FeedbackFollowUpRequest,
    x_user_id: CurrentUserId,
    background: BackgroundTasks,
):
    """Add to the report a thumbs-down sent: a human review asked for, a note
    posted under it, or with neither, the report sent again if the team never
    got it. A review or note that lands after another request removed the
    rating is withdrawn again, and answers 409."""
    note = (request.note or "").strip()
    if note and not request.note_id:
        raise HTTPException(status_code=422, detail="A note needs its note_id")

    await auth_api.require_thread_owner(thread_id, x_user_id)
    try:
        row = await ratings.follow_up(
            thread_id,
            request.turn_index,
            x_user_id,
            note=note,
            note_id=str(request.note_id) if request.note_id else None,
            human_review=request.human_review,
            client=request.client,
        )
    except ReportRefused as refused:
        if refused.changed:
            background.add_task(ratings.sync_langsmith, refused.changed)
        return report_refusal(refused.result)
    except ReportClosed:
        raise _report_closed()
    except NotFound:
        raise HTTPException(status_code=404, detail="No thumbs-down report on this turn")
    except Exception as e:
        logger.exception(f"Error following up feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to follow up feedback")
    background.add_task(ratings.sync_langsmith, str(row["conversation_feedback_id"]))
    return _response(row, request.turn_index, "accepted")


@router.get("/{thread_id}/feedback", response_model=list[FeedbackResponse])
async def get_feedback(thread_id: str, x_user_id: CurrentUserId):
    """Get all feedback for a thread by the current user."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        rows = await ratings.for_thread(thread_id, x_user_id)
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

    A thumbs-down's report is withdrawn first; when the platform cannot do
    that now, the 429 or 503 leaves the rating in place."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        feedback_id = await ratings.remove(thread_id, turn_index, x_user_id)
    except ReportRefused as refused:
        return report_refusal(refused.result)
    except ReportClosed:
        raise _report_closed()
    except NotFound:
        raise HTTPException(status_code=404, detail="Feedback not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete feedback")
    background.add_task(ratings.sync_langsmith, feedback_id)
    return {"status": "deleted"}
