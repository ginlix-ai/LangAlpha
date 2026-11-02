"""The owner's routes on a file's comments, and a thread's queue of them."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response

from src.server.app.artifacts.models import (
    AddCommentMessageRequest,
    CommentListResponse,
    CommentResponse,
    CreateCommentRequest,
    MarkCommentReadRequest,
    QueuedCommentListResponse,
    QueuedCommentResponse,
    UpdateCommentRequest,
)
from src.server.app.artifacts.records import db_errors, not_found, owned, path_id
from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import Artifact
from src.server.database.conversation import get_thread_owner_id
from src.server.services.artifacts.events import publish_comment_change
from src.server.utils.api import CurrentUserId

router = APIRouter()


async def _comments_changed(
    user_id: str,
    artifact: Artifact,
    comment_ids: list[str],
    *threads: str | None,
) -> None:
    """One event per thread whose queue the change touched, or one with none."""
    for thread_id in dict.fromkeys(threads) or [None]:
        await publish_comment_change(
            user_id,
            artifact_id=artifact.artifact_id,
            workspace_id=artifact.workspace_id,
            path=artifact.path,
            thread_id=thread_id,
            comment_ids=comment_ids,
        )


@router.get(
    "/threads/{thread_id}/queued-comments", response_model=QueuedCommentListResponse
)
async def list_thread_queued_comments(
    thread_id: str, x_user_id: CurrentUserId
) -> QueuedCommentListResponse:
    """The comments waiting for the thread's next send, on any artifact: what
    its composer shows it will carry."""
    thread_id = path_id(thread_id, "Thread")
    if await get_thread_owner_id(thread_id) != x_user_id:
        raise not_found("Thread")
    comments = await artifacts_db.list_queued_comments(
        user_id=x_user_id, thread_id=thread_id
    )
    return QueuedCommentListResponse(
        comments=[QueuedCommentResponse.model_validate(c) for c in comments]
    )


@router.get("/{artifact_id}/comments", response_model=CommentListResponse)
async def list_comments(
    artifact_id: str, x_user_id: CurrentUserId
) -> CommentListResponse:
    """Every comment on the file, by pin number, with its messages."""
    artifact = await owned(artifact_id, x_user_id)
    comments = await artifacts_db.list_comments(artifact.artifact_id)
    return CommentListResponse(
        comments=[CommentResponse.model_validate(c) for c in comments]
    )


@router.post("/{artifact_id}/comments", response_model=CommentResponse, status_code=201)
async def create_comment(
    artifact_id: str, body: CreateCommentRequest, x_user_id: CurrentUserId
) -> CommentResponse:
    artifact = await owned(artifact_id, x_user_id)
    with db_errors():
        comment = await artifacts_db.create_comment(
            user_id=x_user_id,
            artifact_id=artifact.artifact_id,
            anchor=body.anchor.model_dump(exclude_none=True),
            body=body.body,
            delivery=body.delivery,
            thread_id=body.thread_id,
            file_sha256=body.file_sha256,
            title=body.title,
        )
    await _comments_changed(
        x_user_id, artifact, [comment.comment_id], comment.thread_id
    )
    return CommentResponse.model_validate(comment)


@router.post(
    "/{artifact_id}/comments/{comment_id}/messages",
    response_model=CommentResponse,
    status_code=201,
)
async def add_comment_message(
    artifact_id: str,
    comment_id: str,
    body: AddCommentMessageRequest,
    x_user_id: CurrentUserId,
) -> CommentResponse:
    """The owner writes again on a comment, which reopens it if resolved."""
    artifact = await owned(artifact_id, x_user_id)
    comment_id = path_id(comment_id, "Comment")
    with db_errors():
        comment, before = await artifacts_db.add_owner_message(
            user_id=x_user_id,
            artifact_id=artifact.artifact_id,
            comment_id=comment_id,
            body=body.body,
            delivery=body.delivery,
            thread_id=body.thread_id,
        )
    await _comments_changed(
        x_user_id, artifact, [comment_id], comment.thread_id, before
    )
    return CommentResponse.model_validate(comment)


@router.patch("/{artifact_id}/comments/{comment_id}", response_model=CommentResponse)
async def update_comment(
    artifact_id: str,
    comment_id: str,
    body: UpdateCommentRequest,
    x_user_id: CurrentUserId,
) -> CommentResponse:
    """Queue a comment for a thread, drop it back to a note, move it to another
    thread's queue, resolve or reopen it."""
    artifact = await owned(artifact_id, x_user_id)
    comment_id = path_id(comment_id, "Comment")
    with db_errors():
        comment, before = await artifacts_db.update_comment(
            user_id=x_user_id,
            artifact_id=artifact.artifact_id,
            comment_id=comment_id,
            delivery=body.delivery,
            thread_id=body.thread_id,
            status=body.status,
        )
    await _comments_changed(
        x_user_id, artifact, [comment_id], comment.thread_id, before
    )
    return CommentResponse.model_validate(comment)


@router.post(
    "/{artifact_id}/comments/{comment_id}/read", response_model=CommentResponse
)
async def mark_comment_read(
    artifact_id: str,
    comment_id: str,
    body: MarkCommentReadRequest,
    x_user_id: CurrentUserId,
) -> CommentResponse:
    """The owner has read the comment up to message ``through``. Only a move
    tells the owner's other tabs; the comment itself is not changed."""
    artifact = await owned(artifact_id, x_user_id)
    comment_id = path_id(comment_id, "Comment")
    with db_errors():
        comment, moved = await artifacts_db.mark_comment_read(
            user_id=x_user_id,
            artifact_id=artifact.artifact_id,
            comment_id=comment_id,
            through=body.through,
        )
    if moved:
        await _comments_changed(x_user_id, artifact, [comment_id], comment.thread_id)
    return CommentResponse.model_validate(comment)


@router.post("/{artifact_id}/comments/read", response_model=CommentListResponse)
async def mark_comments_read(
    artifact_id: str, x_user_id: CurrentUserId
) -> CommentListResponse:
    """The owner has read every comment on the file, each up to its latest
    message. Answers with the file's comments, as the list does."""
    artifact = await owned(artifact_id, x_user_id)
    comments, moved = await artifacts_db.mark_artifact_read(
        user_id=x_user_id, artifact_id=artifact.artifact_id
    )
    if moved:
        await _comments_changed(x_user_id, artifact, moved)
    return CommentListResponse(
        comments=[CommentResponse.model_validate(c) for c in comments]
    )


@router.delete("/{artifact_id}/comments/{comment_id}", status_code=204)
async def delete_comment(
    artifact_id: str, comment_id: str, x_user_id: CurrentUserId
) -> Response:
    artifact = await owned(artifact_id, x_user_id)
    comment_id = path_id(comment_id, "Comment")
    deleted, thread_id = await artifacts_db.delete_comment(
        user_id=x_user_id, artifact_id=artifact.artifact_id, comment_id=comment_id
    )
    if not deleted:
        raise not_found("Comment")
    await _comments_changed(x_user_id, artifact, [comment_id], thread_id)
    return Response(status_code=204)
