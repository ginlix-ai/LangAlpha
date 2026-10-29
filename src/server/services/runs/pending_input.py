"""A turn attempt that never reached the graph.

An attempt that fails before the executor takes it leaves nothing in any
checkpoint, and replaying the thread's latest one would run something else.
A send or an edit has written the user's message only to the turn's query
row, so the replay would answer the turn before it and drop the message. A
regenerate pinned its fork base, but the thread's newest checkpoint is still
the old branch's tip, which a replay that names no checkpoint takes, so it
would answer from the wrong branch. The failed run row therefore records the
input still pending, in the same finalize write as the error, and ``/retry``
runs that input again.

The latest attempt alone decides, and each attempt is stamped from its own
request, so a retry never inherits a stale stamp. The query row keeps one text,
so input beyond it (history, other roles, image parts) is stamped for the
client to send again, and context sent beside it (attachments, widgets, skills)
reruns only with a retry that brings it again. Either way the retry never
answers something the turn did not ask.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Optional

from fastapi import HTTPException
from pydantic import TypeAdapter, ValidationError

from src.server.contracts.turn_kinds import WAKE_QUERY_TYPE
from src.server.models.chat import ChatMessage, ChatRequest, ReportSource
from src.server.models.workflow import RetryRequest

if TYPE_CHECKING:
    from src.server.database.runs.lifecycle import NewestCheckpoint

PENDING_INPUT_KEY = "pending_input"


@dataclass(frozen=True)
class PendingInput:
    # What the input ran against: None for a send, which takes the thread's
    # latest checkpoint, or the fork base of an edit or a regenerate. The
    # thread's newest checkpoint is still the old branch's tip, so the base is
    # kept here.
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


def executor_input_stamp(request: ChatRequest) -> dict:
    """The stamp for a run that fails in its executor before the graph takes
    its input (``RunHandle.pending_input``).

    A HITL resume is left out: the error path asks its client to send the
    answers again while it still holds them, but the executor's failure has
    the client retry, and replaying the checkpoint raises the pause again for
    the user to answer.
    """
    return {} if request.hitl_response else pending_input_stamp(request)


# START keeps a run's ``executor_input_stamp`` on its row under this key, which
# ``/retry`` does not read: the recovery scanner settles a dead worker's run
# from the row alone, with no request to stamp from.
UNTAKEN_INPUT_KEY = "untaken_input"


def untaken_input_metadata(stamp: Mapping[str, Any]) -> dict:
    pending = stamp.get(PENDING_INPUT_KEY)
    return {UNTAKEN_INPUT_KEY: pending} if pending else {}


def read_untaken_input(run: Optional[Mapping[str, Any]]) -> dict:
    pending = ((run or {}).get("metadata") or {}).get(UNTAKEN_INPUT_KEY)
    return {PENDING_INPUT_KEY: pending} if isinstance(pending, dict) else {}


def untaken_input_stamp(
    stamp: Mapping[str, Any], run_id: str, newest: Optional["NewestCheckpoint"]
) -> dict:
    """``stamp`` for a failed or stopped attempt when no step of the thread is
    its run's, ``newest`` being the thread's newest step as a read that
    succeeded found it.

    The graph stamps every checkpoint it writes with its run, the input's
    first, so none means the run ended before the graph took the input, in
    its executor as much as before handoff or with its worker. ``/retry`` then
    sends that input again: a replay of the thread's checkpoint would answer
    the turn before it, or, for an edit, run nothing at the fork's base. A
    regenerate of the turn sends it again the same way.
    """
    if newest is not None and newest.run_id == run_id:
        return {}
    return dict(stamp)


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


def _resend_required() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": "resend_required",
            "message": "The failed turn's input was more than its stored "
            "message; send it again.",
        },
    )


def _message_gone() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "code": "not_retryable",
            "message": "The failed turn's message is no longer on the thread.",
        },
    )


_REPORT_SOURCE = TypeAdapter(ReportSource)


def _stored_turn_fields(query: Mapping[str, Any]) -> dict[str, Any]:
    """The server-set fields a turn's query row keeps, for a request that runs
    the turn again: its type, so a system turn (a report-back) runs as one and
    a wake turn may still end quietly, and the report or notices it announces,
    which label it in history and the live stream."""
    metadata = query.get("metadata") or {}
    stored = metadata.get("report_source") or None
    try:
        report_source = _REPORT_SOURCE.validate_python(stored) if stored else None
    except ValidationError:
        report_source = None
    market_target = metadata.get("market_target")
    wakeup = metadata.get("wakeup")
    return {
        "query_type": query.get("type"),
        "report_source": report_source,
        "market_target": market_target if isinstance(market_target, dict) else None,
        "wakeup": wakeup if isinstance(wakeup, dict) else None,
    }


async def retried_wake_fields(thread_id: str, turn_index: int) -> dict[str, Any]:
    """A wake turn's type and notices, for a retry that replays its checkpoint.

    The checkpoint holds the wake block with its quiet hint, so the retry must
    still be a wake turn, or quiet would be refused against the very message
    it replays and the live row would lose what fired. Other turns replay as
    they always have.
    """
    from src.server.database.conversation.queries import get_query_at_turn

    query = await get_query_at_turn(thread_id, turn_index)
    if query is None or query.get("type") != WAKE_QUERY_TYPE:
        return {}
    fields = _stored_turn_fields(query)
    return {"query_type": fields["query_type"], "wakeup": fields["wakeup"]}


async def unstarted_regenerate_request(
    thread_id: str,
    request: ChatRequest,
    turn_index: int,
    latest_run: Optional[Mapping[str, Any]],
) -> ChatRequest:
    """A regenerate of a turn whose run never reached the graph, as the edit
    that sends its message again.

    The branch holds no input for that turn, so a replay at the tip would
    answer the turn before it. The message comes from the turn's query row, as
    a retry's does, and only when the thread's latest attempt, ``latest_run``,
    is that turn's and was stamped as held by the row: an attempt that left no
    stamp says nothing about what else its input carried. The row's type and
    report come too, since the edit replaces the row.
    """
    from src.server.database.conversation.queries import get_query_at_turn

    pending = (
        read_pending_input(latest_run)
        if latest_run is not None and latest_run["turn_index"] == turn_index
        else None
    )
    if (
        pending is None
        or pending.resend
        or pending.replay
        or (pending.context and not request.additional_context)
    ):
        raise _resend_required()
    query = await get_query_at_turn(thread_id, turn_index)
    if query is None:
        raise _message_gone()
    return request.model_copy(
        update={
            "messages": [ChatMessage(role="user", content=query["content"])],
            **_stored_turn_fields(query),
        }
    )


async def input_rerun_request(
    thread_id: str,
    failed_run: Mapping[str, Any],
    pending: PendingInput,
    *,
    workspace_id: str,
    body: Optional[RetryRequest],
) -> ChatRequest:
    """The retry of a pending input, as the send it stood in for.

    The text, its type and its report come from the turn's query row, never
    the client, so a retry cannot rewrite what the turn asked, and a system
    turn reruns as one. Context the turn carried (attachments, widgets) comes from the
    client, which holds it until a reload, since only the text is stored; a
    retry without it is refused rather than run as a different question.
    """
    from src.server.database.conversation.queries import get_query_at_turn

    if pending.resend or (pending.context and not (body and body.additional_context)):
        raise _resend_required()
    query = await get_query_at_turn(thread_id, failed_run["turn_index"])
    if query is None:
        raise _message_gone()
    run_meta = failed_run.get("metadata") or {}
    return ChatRequest(
        workspace_id=workspace_id,
        messages=[{"role": "user", "content": query["content"]}],
        **_stored_turn_fields(query),
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
