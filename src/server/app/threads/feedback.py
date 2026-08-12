"""Per-turn user feedback: submit, list, remove."""

from fastapi import HTTPException

# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
)
from src.server.models.conversation import (
    FeedbackRequest,
    FeedbackResponse,
)
from src.server.database.conversation import (
    upsert_feedback,
    get_feedback_for_thread,
    delete_feedback,
)



from ._deps import logger, router


# ==================== Feedback ====================


@router.post("/{thread_id}/feedback", response_model=FeedbackResponse)
async def submit_feedback(
    thread_id: str,
    request: FeedbackRequest,
    x_user_id: CurrentUserId,
):
    """Submit or update feedback (thumbs up/down) for a response."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        result = await upsert_feedback(
            conversation_thread_id=thread_id,
            turn_index=request.turn_index,
            user_id=x_user_id,
            rating=request.rating,
            issue_categories=request.issue_categories,
            comment=request.comment,
            consent_human_review=request.consent_human_review,
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No response found at turn_index={request.turn_index}",
            )
        return FeedbackResponse(
            conversation_feedback_id=str(result["conversation_feedback_id"]),
            turn_index=result["turn_index"],
            rating=result["rating"],
            issue_categories=result.get("issue_categories"),
            comment=result.get("comment"),
            consent_human_review=result.get("consent_human_review", False),
            review_status=result.get("review_status"),
            created_at=str(result["created_at"]),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error submitting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit feedback")


@router.get("/{thread_id}/feedback", response_model=list[FeedbackResponse])
async def get_feedback(thread_id: str, x_user_id: CurrentUserId):
    """Get all feedback for a thread by the current user."""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        rows = await get_feedback_for_thread(thread_id, x_user_id)
        return [
            FeedbackResponse(
                conversation_feedback_id=str(row["conversation_feedback_id"]),
                turn_index=row["turn_index"],
                rating=row["rating"],
                issue_categories=row.get("issue_categories"),
                comment=row.get("comment"),
                consent_human_review=row.get("consent_human_review", False),
                review_status=row.get("review_status"),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]
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
):
    """Remove feedback for a specific response. Query param: ?turn_index=N"""
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        deleted = await delete_feedback(thread_id, turn_index, x_user_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Feedback not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting feedback for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete feedback")


