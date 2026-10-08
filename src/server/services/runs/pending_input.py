"""A turn attempt that never reached the graph.

An attempt that fails before the executor takes it leaves nothing in any
checkpoint, and replaying the thread's latest one would run something else.
A send or an edit has written the user's message only to the turn's query
row, so the replay would answer the turn before it and drop the message. A
regenerate pinned its fork base, but the failed finalize re-pins the thread to
the old branch tip, so the replay would answer from the wrong branch. The
failed run row therefore records the input still pending, in the same
finalize write as the error, and ``/retry`` runs that input again.

The latest attempt alone decides, and each attempt is stamped from its own
request, so a retry never inherits a stale stamp. The query row keeps one text,
so input beyond it (history, other roles, image parts) is stamped for the
client to send again, and context sent beside it (attachments, widgets, skills)
reruns only with a retry that brings it again. Either way the retry never
answers something the turn did not ask.
"""

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from fastapi import HTTPException

from src.server.models.chat import ChatMessage, ChatRequest
from src.server.models.workflow import RetryRequest

PENDING_INPUT_KEY = "pending_input"


@dataclass(frozen=True)
class PendingInput:
    # What the input ran against: None for a send, which takes the thread's
    # latest checkpoint, or the fork base of an edit or a regenerate. The
    # failed run's finalize re-pins the thread to the old branch tip, so the
    # base is kept here.
    checkpoint_id: Optional[str]
    # A regenerate: the checkpoint is the whole input, no message runs again.
    replay: bool = False
    # Input the query row does not hold whole; only the client can send it.
    resend: bool = False
    # Context sent beside the text, which no row keeps.
    context: bool = False


def _held_by_query_row(messages: list[ChatMessage]) -> bool:
    if len(messages) != 1 or messages[0].role not in ("user", "human"):
        return False
    content = messages[0].content
    return isinstance(content, str) or (
        all(part.type == "text" for part in content)
        and sum(1 for part in content if part.text) <= 1
    )


def pending_input_stamp(request: ChatRequest) -> dict:
    """Run-row metadata for an attempt whose input never reached the graph.

    A HITL resume's answers are kept by no row, so only the client can send
    them again; a checkpoint replay would raise the same pause unanswered.
    """
    if request.hitl_response:
        return {PENDING_INPUT_KEY: {"resend": True}}
    if request.messages:
        if not _held_by_query_row(request.messages):
            return {PENDING_INPUT_KEY: {"resend": True}}
        stamp: dict[str, Any] = {"checkpoint_id": request.checkpoint_id}
        if request.additional_context:
            stamp["context"] = True
        return {PENDING_INPUT_KEY: stamp}
    if request.checkpoint_id:
        return {
            PENDING_INPUT_KEY: {"checkpoint_id": request.checkpoint_id, "replay": True}
        }
    return {}


def read_pending_input(run: Optional[Mapping[str, Any]]) -> Optional[PendingInput]:
    pending = ((run or {}).get("metadata") or {}).get(PENDING_INPUT_KEY)
    if not isinstance(pending, dict):
        return None
    return PendingInput(
        checkpoint_id=pending.get("checkpoint_id"),
        replay=pending.get("replay") is True,
        resend=pending.get("resend") is True,
        context=pending.get("context") is True,
    )


def needs_resend(stamp: Mapping[str, Any]) -> bool:
    return (stamp.get(PENDING_INPUT_KEY) or {}).get("resend") is True


async def input_rerun_request(
    thread_id: str,
    failed_run: Mapping[str, Any],
    pending: PendingInput,
    *,
    workspace_id: str,
    body: Optional[RetryRequest],
) -> ChatRequest:
    """The retry of a pending input, as the send it stood in for.

    The text comes from the turn's query row, never the client, so a retry
    cannot rewrite what the turn asked. Context the turn carried (attachments,
    widgets) comes from the client, which holds it until a reload, since only
    the text is stored; a retry without it is refused rather than run as a
    different question.
    """
    from src.server.database.conversation.queries import get_query_at_turn

    if pending.resend or (pending.context and not (body and body.additional_context)):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "resend_required",
                "message": "The failed turn's input was more than its stored "
                "message; send it again.",
            },
        )
    query = await get_query_at_turn(thread_id, failed_run["turn_index"])
    if query is None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "not_retryable",
                "message": "The failed turn's message is no longer on the thread.",
            },
        )
    run_meta = failed_run.get("metadata") or {}
    return ChatRequest(
        workspace_id=workspace_id,
        messages=[{"role": "user", "content": query["content"]}],
        checkpoint_id=pending.checkpoint_id,
        additional_context=body.additional_context if body else None,
        locale=run_meta.get("locale"),
        timezone=run_meta.get("timezone"),
        subagents_enabled=run_meta.get("subagents_enabled"),
        request_key=body.request_key if body else None,
        llm_model=body.llm_model if body else None,
        reasoning_effort=body.reasoning_effort if body else None,
        fast_mode=body.fast_mode if body else None,
    )
