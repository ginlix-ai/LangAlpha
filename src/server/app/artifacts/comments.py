"""The owner's routes on a file's comments, and a thread's queue of them.

A shared workspace's comments are its holder's, like its records: every
member writes, queues, resolves and reads them as the workspace's owner does,
and reading one marks it read for all of them (one cursor per comment). Each
message a member writes there names them (``written_by``), so the others see
whose words they are."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

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
from src.server.app.artifacts.records import db_errors, not_found, path_id, reached
from src.server.database import artifacts as artifacts_db
from src.server.database.artifacts import Artifact
from src.server.database.artifacts.comments import Comment
from src.server.database.conversation import thread_reach
from src.server.services.account import AccountSuspended
from src.server.services.shared_workspaces.access import Access, NoAccess, thread_access
from src.server.services.artifacts.events import publish_comment_change
from src.server.utils.api import CurrentUserId

router = APIRouter()


async def _shown(
    access: Access, caller: str, comments: Sequence[Comment]
) -> list[CommentResponse]:
    """The comments as ``caller`` may see them: in a shared workspace a
    thread they do not reach, another member's private one, is named by no
    comment or message."""
    hidden: set[str] = set()
    if access.is_shared:
        named = {c.thread_id for c in comments} | {
            m.thread_id for c in comments for m in c.messages
        }
        hidden = await thread_reach.hidden({t for t in named if t}, caller)
    return [_as_seen(c, hidden) for c in comments]


def _as_seen(comment: Comment, hidden: set[str]) -> CommentResponse:
    if not hidden:
        return CommentResponse.model_validate(comment)
    comment = replace(comment, messages=tuple(
        replace(m, thread_id=None) if m.thread_id in hidden else m for m in comment.messages
    ))
    if comment.thread_id not in hidden:
        return CommentResponse.model_validate(comment)
    unnamed = replace(comment, thread_id=None, thread_title=None, thread_archived=False)
    return CommentResponse.model_validate(unnamed).model_copy(update={"thread_hidden": True})


async def _one(access: Access, caller: str, comment: Comment) -> CommentResponse:
    return (await _shown(access, caller, [comment]))[0]


async def _require_reached(access: Access, caller: str, thread_id: str | None) -> None:
    """Refuse a member's named destination they may not see, as one that does
    not exist is refused, whatever the comment does with it: the write checks
    the thread again under its lock only where the comment waits for it."""
    if access.is_shared and thread_id and await thread_reach.hidden([thread_id], caller):
        raise artifacts_db.DestinationUnavailable(thread_id)


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
    try:
        access = await thread_access(thread_id, x_user_id)
    except (NoAccess, AccountSuspended):
        raise not_found("Thread") from None
    comments = await artifacts_db.list_queued_comments(
        user_id=access.owner_id, thread_id=thread_id
    )
    return QueuedCommentListResponse(
        comments=[QueuedCommentResponse.model_validate(c) for c in comments]
    )


@router.get("/{artifact_id}/comments", response_model=CommentListResponse)
async def list_comments(
    artifact_id: str, x_user_id: CurrentUserId
) -> CommentListResponse:
    """Every comment on the file, by pin number, with its messages."""
    artifact, access = await reached(artifact_id, x_user_id)
    comments = await artifacts_db.list_comments(artifact.artifact_id)
    return CommentListResponse(
        comments=await _shown(access, x_user_id, comments)
    )


@router.post("/{artifact_id}/comments", response_model=CommentResponse, status_code=201)
async def create_comment(
    artifact_id: str, body: CreateCommentRequest, x_user_id: CurrentUserId
) -> CommentResponse:
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    with db_errors():
        await _require_reached(access, x_user_id, body.thread_id)
        comment = await artifacts_db.create_comment(
            user_id=owner_id,
            artifact_id=artifact.artifact_id,
            anchor=body.anchor.model_dump(exclude_none=True),
            body=body.body,
            delivery=body.delivery,
            thread_id=body.thread_id,
            file_sha256=body.file_sha256,
            title=body.title,
            caller=x_user_id,
        )
    await _comments_changed(
        owner_id, artifact, [comment.comment_id], comment.thread_id
    )
    return await _one(access, x_user_id, comment)


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
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    comment_id = path_id(comment_id, "Comment")
    with db_errors():
        await _require_reached(access, x_user_id, body.thread_id)
        comment, before = await artifacts_db.add_owner_message(
            user_id=owner_id,
            artifact_id=artifact.artifact_id,
            comment_id=comment_id,
            body=body.body,
            delivery=body.delivery,
            thread_id=body.thread_id,
            caller=x_user_id,
        )
    await _comments_changed(
        owner_id, artifact, [comment_id], comment.thread_id, before
    )
    return await _one(access, x_user_id, comment)


@router.patch("/{artifact_id}/comments/{comment_id}", response_model=CommentResponse)
async def update_comment(
    artifact_id: str,
    comment_id: str,
    body: UpdateCommentRequest,
    x_user_id: CurrentUserId,
) -> CommentResponse:
    """Queue a comment for a thread, drop it back to a note, move it to another
    thread's queue, resolve or reopen it."""
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    comment_id = path_id(comment_id, "Comment")
    with db_errors():
        await _require_reached(access, x_user_id, body.thread_id)
        comment, before = await artifacts_db.update_comment(
            user_id=owner_id,
            artifact_id=artifact.artifact_id,
            comment_id=comment_id,
            delivery=body.delivery,
            thread_id=body.thread_id,
            status=body.status,
            caller=x_user_id,
        )
    await _comments_changed(
        owner_id, artifact, [comment_id], comment.thread_id, before
    )
    return await _one(access, x_user_id, comment)


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
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    comment_id = path_id(comment_id, "Comment")
    with db_errors():
        comment, moved = await artifacts_db.mark_comment_read(
            user_id=owner_id,
            artifact_id=artifact.artifact_id,
            comment_id=comment_id,
            through=body.through,
        )
    if moved:
        await _comments_changed(owner_id, artifact, [comment_id], comment.thread_id)
    return await _one(access, x_user_id, comment)


@router.post("/{artifact_id}/comments/read", response_model=CommentListResponse)
async def mark_comments_read(
    artifact_id: str, x_user_id: CurrentUserId
) -> CommentListResponse:
    """The owner has read every comment on the file, each up to its latest
    message. Answers with the file's comments, as the list does."""
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    comments, moved = await artifacts_db.mark_artifact_read(
        user_id=owner_id, artifact_id=artifact.artifact_id
    )
    if moved:
        await _comments_changed(owner_id, artifact, moved)
    return CommentListResponse(
        comments=await _shown(access, x_user_id, comments)
    )


@router.delete("/{artifact_id}/comments/{comment_id}", status_code=204)
async def delete_comment(
    artifact_id: str, comment_id: str, x_user_id: CurrentUserId
) -> Response:
    artifact, access = await reached(artifact_id, x_user_id)
    owner_id = access.owner_id
    comment_id = path_id(comment_id, "Comment")
    deleted, thread_id = await artifacts_db.delete_comment(
        user_id=owner_id, artifact_id=artifact.artifact_id, comment_id=comment_id
    )
    if not deleted:
        raise not_found("Comment")
    await _comments_changed(owner_id, artifact, [comment_id], thread_id)
    return Response(status_code=204)
