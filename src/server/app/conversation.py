"""
Workspace thread and message management endpoints.

This module handles:
- Listing threads in a workspace
- Retrieving workspace messages
- Getting individual message details
"""

import json
import logging
import time
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query

from src.server.utils.api import CurrentUserId
from fastapi.responses import Response, StreamingResponse

from src.server.models.conversation import (
    WorkspaceThreadListItem,
    WorkspaceThreadsListResponse,
    ResponseFullDetail,
    MessageQuery,
    MessageResponse,
    ConversationMessage,
    WorkspaceMessagesResponse,
    ThreadUpdateRequest,
    ThreadDeleteResponse,
)
from src.server.database.conversation import (
    get_response_by_id,
    get_workspace_threads,
    get_threads_for_user,
    get_thread_messages,
    get_thread_with_summary,
    get_queries_for_thread,
    get_responses_for_thread,
    delete_thread,
    update_thread_title,
    get_thread_by_id,
)
from src.server.utils.message_deduplicator import deduplicate_agent_messages

logger = logging.getLogger(__name__)
INTERNAL_SERVER_ERROR_DETAIL = "Internal Server Error"

# Create separate routers for different prefixes
workspaces_threads_router = APIRouter(prefix="/api/v1/workspaces", tags=["Workspace Threads"])
conversations_router = APIRouter(prefix="/api/v1/conversations", tags=["Conversations"])
threads_router = APIRouter(prefix="/api/v1/threads", tags=["Threads"])
messages_router = APIRouter(prefix="/api/v1/messages", tags=["Messages"])


# ==================== Workspace Thread Management Endpoints ====================
# - Threads are created automatically when first query is sent
# - Users see continuous chat messages per workspace
# - Cost/token usage hidden from user-facing endpoints

@workspaces_threads_router.get("/{workspace_id}/threads", response_model=WorkspaceThreadsListResponse)
async def list_workspace_threads_endpoint(
    workspace_id: str,
    limit: int = Query(20, ge=1, le=100, description="Max threads per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort_by: str = Query("updated_at", description="Sort field (created_at, updated_at)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)")
):
    """
    List threads for a workspace with pagination.

    Args:
        workspace_id: Workspace ID (path parameter)
        limit: Maximum threads to return (1-100)
        offset: Pagination offset
        sort_by: Field to sort by
        sort_order: Sort direction

    Returns:
        WorkspaceThreadsListResponse with threads and pagination info

    Raises:
        500: Database error during retrieval
    """
    try:
        logger.info(f"Listing threads for workspace_id={workspace_id}, limit={limit}, offset={offset}")

        threads, total = await get_workspace_threads(
            workspace_id=workspace_id,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order
        )

        # Build response
        thread_items = [
            WorkspaceThreadListItem(
                thread_id=str(thread["thread_id"]),
                workspace_id=str(thread["workspace_id"]),
                thread_index=thread["thread_index"],
                current_status=thread["current_status"],
                msg_type=thread.get("msg_type"),
                title=thread.get("title"),
                created_at=thread["created_at"],
                updated_at=thread["updated_at"]
            )
            for thread in threads
        ]

        response = WorkspaceThreadsListResponse(
            threads=thread_items,
            total=total,
            limit=limit,
            offset=offset
        )

        logger.info(f"Found {len(threads)} threads for workspace_id={workspace_id}")
        return response

    except Exception as e:
        logger.exception(f"Error listing threads: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list threads: {str(e)}"
        )


@conversations_router.get("", response_model=WorkspaceThreadsListResponse)
async def list_user_threads_endpoint(
    x_user_id: CurrentUserId,
    limit: int = Query(20, ge=1, le=100, description="Max threads per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort_by: str = Query("updated_at", description="Sort field (created_at, updated_at)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
):
    """List all threads across all workspaces for a user."""
    try:
        threads, total = await get_threads_for_user(
            user_id=x_user_id,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order,
        )

        thread_items = [
            WorkspaceThreadListItem(
                thread_id=str(thread["thread_id"]),
                workspace_id=str(thread["workspace_id"]),
                thread_index=thread["thread_index"],
                current_status=thread["current_status"],
                msg_type=thread.get("msg_type"),
                title=thread.get("title"),
                first_query_content=thread.get("first_query_content"),
                created_at=thread["created_at"],
                updated_at=thread["updated_at"],
            )
            for thread in threads
        ]

        return WorkspaceThreadsListResponse(
            threads=thread_items,
            total=total,
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        logger.exception(f"Error listing threads for user {x_user_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list user threads: {str(e)}",
        )


@threads_router.delete("/{thread_id}", response_model=ThreadDeleteResponse)
async def delete_thread_endpoint(thread_id: str):
    """
    Delete a thread and all its queries/responses.

    This permanently deletes the thread and all associated data (queries, responses, usage records)
    due to CASCADE constraints.

    Args:
        thread_id: Thread ID to delete

    Returns:
        ThreadDeleteResponse with deletion status

    Raises:
        404: Thread not found
        500: Database error during deletion
    """
    try:
        logger.info(f"Deleting thread thread_id={thread_id}")

        # Check if thread exists first
        thread = await get_thread_by_id(thread_id)
        if not thread:
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}"
            )

        # Delete the thread (CASCADE handles queries, responses, usage)
        await delete_thread(thread_id)

        logger.info(f"Successfully deleted thread thread_id={thread_id}")
        return ThreadDeleteResponse(
            success=True,
            thread_id=thread_id,
            message="Thread deleted successfully"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error deleting thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete thread: {str(e)}"
        )


@threads_router.patch("/{thread_id}", response_model=WorkspaceThreadListItem)
async def update_thread_endpoint(thread_id: str, request: ThreadUpdateRequest):
    """
    Update thread properties (currently only title).

    Args:
        thread_id: Thread ID to update
        request: ThreadUpdateRequest with fields to update

    Returns:
        Updated WorkspaceThreadListItem

    Raises:
        404: Thread not found
        500: Database error during update
    """
    try:
        logger.info(f"Updating thread thread_id={thread_id} with title={request.title}")

        # Update the thread title
        updated_thread = await update_thread_title(thread_id, request.title)

        if not updated_thread:
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}"
            )

        logger.info(f"Successfully updated thread thread_id={thread_id}")
        return WorkspaceThreadListItem(
            thread_id=str(updated_thread["thread_id"]),
            workspace_id=str(updated_thread["workspace_id"]),
            thread_index=updated_thread["thread_index"],
            current_status=updated_thread["current_status"],
            msg_type=updated_thread.get("msg_type"),
            title=updated_thread.get("title"),
            created_at=updated_thread["created_at"],
            updated_at=updated_thread["updated_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error updating thread {thread_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update thread: {str(e)}"
        )


@conversations_router.get("/{thread_id}/messages", response_model=WorkspaceMessagesResponse)
async def get_thread_messages_endpoint(
    thread_id: str,
    limit: int = Query(50, ge=1, le=200, description="Max messages per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Get conversation history for a single thread_id."""
    try:
        workspace, thread, messages, total = await get_thread_messages(
            thread_id=thread_id,
            limit=limit,
            offset=offset,
        )

        if not thread or not workspace:
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}",
            )

        workspace_id = str(workspace["workspace_id"])

        message_objects = []
        for msg in messages:
            query = MessageQuery(
                query_id=str(msg["query_id"]),
                content=msg["query_content"],
                type=msg["query_type"],
                feedback_action=msg.get("feedback_action"),
                metadata=msg.get("query_metadata", {}),
                timestamp=msg["query_timestamp"],
            )

            response = None
            if msg.get("response_id"):
                from src.server.database.conversation import (
                    get_file_snapshot_before_thread,
                    get_operations_for_thread,
                )
                from src.server.models.conversation import FileSnapshot, FileOperationEvent

                file_snapshot = None
                file_operations = None

                try:
                    snapshot_dict = await get_file_snapshot_before_thread(workspace_id, msg["thread_index"])
                    if snapshot_dict:
                        file_snapshot = {
                            path: FileSnapshot(
                                file_id=info["file_id"],
                                content=info["content"],
                                line_count=info["line_count"],
                                updated_in_thread_id=info["updated_in_thread_id"],
                                updated_in_pair_index=info["updated_in_pair_index"],
                            )
                            for path, info in snapshot_dict.items()
                        }

                    operations_list = await get_operations_for_thread(msg["thread_id"], msg["pair_index"])
                    if operations_list:
                        file_operations = [
                            FileOperationEvent(
                                operation=op["operation"],
                                file_path=op["file_path"],
                                content=op["content"] or "",
                                line_count=op.get("line_count", 0),
                                agent=op.get("agent", "unknown"),
                                thread_id=op["thread_id"],
                                pair_index=op["pair_index"],
                                timestamp=op["timestamp"],
                                operation_index=op["operation_index"],
                                old_string=op.get("old_string"),
                                new_string=op.get("new_string"),
                                tool_call_id=op.get("tool_call_id"),
                                file_id=op["file_id"],
                            )
                            for op in operations_list
                        ]

                except Exception as e:
                    logger.warning(f"Failed to get file data for thread {msg['thread_id']}: {e}")

                response = MessageResponse(
                    response_id=str(msg["response_id"]),
                    status=msg["status"],
                    interrupt_reason=msg.get("interrupt_reason"),
                    agent_messages=msg.get("agent_messages"),
                    execution_time=msg.get("execution_time", 0.0),
                    warnings=msg.get("warnings", []),
                    errors=msg.get("errors", []),
                    timestamp=msg["response_timestamp"],
                    file_snapshot=file_snapshot,
                    file_operations=file_operations,
                )

            message_objects.append(
                ConversationMessage(
                    pair_index=msg["pair_index"],
                    thread_id=str(msg["thread_id"]),
                    thread_index=msg["thread_index"],
                    query=query,
                    response=response,
                )
            )

        dedup_start = time.time()
        message_objects, duplicates_removed = deduplicate_agent_messages(message_objects)
        dedup_time = (time.time() - dedup_start) * 1000

        logger.info(
            f"Deduplication for thread_id={thread_id}: "
            f"removed {duplicates_removed} duplicates in {dedup_time:.2f}ms"
        )

        return WorkspaceMessagesResponse(
            workspace_id=workspace_id,
            user_id=workspace["user_id"],
            name=workspace.get("name"),
            messages=message_objects,
            total_messages=total,
            has_more=(offset + len(message_objects)) < total,
            created_at=workspace["created_at"],
            updated_at=workspace["updated_at"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting thread messages: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get thread messages: {str(e)}",
        )


@threads_router.get("/{thread_id}/replay")
async def replay_thread_endpoint(thread_id: str):
    """Replay a thread as SSE using persisted streaming_chunks.

    Stream includes:
    - user_message: emitted once per pair_index (query content)
    - message_chunk/tool_* events: emitted from stored streaming_chunks
    - replay_done: terminal sentinel
    """
    try:
        thread = await get_thread_with_summary(thread_id)
        if not thread:
            raise HTTPException(status_code=404, detail=f"Thread not found: {thread_id}")

        queries, _ = await get_queries_for_thread(thread_id)
        responses, _ = await get_responses_for_thread(thread_id)
        responses_by_pair = {r.get("pair_index"): r for r in responses if isinstance(r, dict)}

        async def event_generator():
            seq = 0

            for q in queries:
                if not isinstance(q, dict):
                    continue

                pair_index = q.get("pair_index")
                seq += 1
                payload = {
                    "thread_id": thread_id,
                    "pair_index": pair_index,
                    "content": q.get("content"),
                    "timestamp": q.get("timestamp"),
                }
                yield (
                    f"id: {seq}\n"
                    f"event: user_message\n"
                    f"data: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"
                )

                response = responses_by_pair.get(pair_index)
                if not response:
                    continue

                streaming_chunks = response.get("streaming_chunks")
                if not (isinstance(streaming_chunks, list) and streaming_chunks):
                    continue

                for item in streaming_chunks:
                    if not isinstance(item, dict):
                        continue
                    event_type = item.get("event")
                    data = item.get("data")
                    if not event_type or not isinstance(data, dict):
                        continue

                    seq += 1
                    replay_data = dict(data)
                    replay_data.setdefault("thread_id", thread_id)
                    replay_data["pair_index"] = pair_index
                    replay_data["response_id"] = str(response.get("response_id"))

                    yield (
                        f"id: {seq}\n"
                        f"event: {event_type}\n"
                        f"data: {json.dumps(replay_data, ensure_ascii=False, default=str)}\n\n"
                    )

            seq += 1
            yield f"id: {seq}\nevent: replay_done\ndata: {json.dumps({'thread_id': thread_id}, default=str)}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error replaying thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to replay thread: {str(e)}")


@messages_router.get("/{response_id}", response_model=ResponseFullDetail)
async def get_message_detail_endpoint(response_id: str):
    """
    Get complete message/response details by response_id (admin/debug endpoint).

    Includes all fields: state_snapshot, agent_messages, etc.
    This endpoint is for debugging and admin purposes only.

    Args:
        response_id: Response ID

    Returns:
        ResponseFullDetail with state_snapshot and agent_messages

    Raises:
        404: Response not found
        500: Database error during retrieval
    """
    try:
        logger.info(f"Getting full response detail for response_id={response_id}")

        # Get full response detail by response_id
        response_data = await get_response_by_id(response_id)

        if not response_data:
            raise HTTPException(
                status_code=404,
                detail=f"Response not found: {response_id}"
            )

        # Build response model
        # Convert UUID objects to strings for Pydantic validation
        response = ResponseFullDetail(
            response_id=str(response_data["response_id"]),
            thread_id=str(response_data["thread_id"]),
            pair_index=response_data["pair_index"],
            status=response_data["status"],
            interrupt_reason=response_data.get("interrupt_reason"),
            state_snapshot=response_data.get("state_snapshot"),
            agent_messages=response_data.get("agent_messages"),
            metadata=response_data.get("metadata", {}),
            warnings=response_data.get("warnings", []),
            errors=response_data.get("errors", []),
            execution_time=response_data.get("execution_time", 0.0),
            timestamp=response_data["timestamp"]
        )

        logger.info(f"Retrieved full response detail for response_id={response_id}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting full response detail: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get full response detail: {str(e)}"
        )