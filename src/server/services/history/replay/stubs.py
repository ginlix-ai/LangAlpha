"""What a turn replays as when its checkpoints cannot project it.

Every stub comes through ``stub_lines``: a turn with no committed boundary
yet (``build_replay_page``), a turn that fails on its own (``cold``) and a
thread whose turns cannot be paired at all (``read_replay_page``), so what
such a turn shows is decided in one place. Its lines are never stored, so
nothing here is digested into the lines epoch.
"""

from __future__ import annotations

from typing import Any

from src.server.database.conversation.replay_rows import with_replay_facts
from src.server.database.replay_facts import archived_stream
from src.server.services.history.replay import items
from src.server.services.history.replay.lines import Line, line_of


async def stub_lines(
    thread_id: str,
    turn_indexes: list[Any],
    queries_by_turn: dict[Any, list[dict[str, Any]]],
    responses_by_turn: dict[Any, dict[str, Any]],
    open_runs: dict[Any, str] | None = None,
) -> tuple[dict[Any, list[Line]], list[Any]]:
    """Each turn as its user message, the stream its row archived when it
    kept one, and the terminal error or stop close the row records; and the
    turns among them that answered with nothing here to show for it, for the
    caller's log. ``open_runs`` names the run of a turn still going
    (``ThreadRows.open_runs``), which the client attaches to live.

    Without an archive, output a turn with no committed boundary streamed
    before it stopped or failed is not on the branch, so it is not in the
    model's context either, and its stub leaves it out. The rows are re-read
    with their replay facts, which hold the archive and the error frame a
    failed turn's stream sent.
    """
    open_runs = open_runs or {}
    responses_by_turn = await with_replay_facts(responses_by_turn, turn_indexes)
    lines = {
        ti: [
            line_of(item)
            for item in _stub_items(
                thread_id,
                ti,
                queries_by_turn.get(ti, []),
                responses_by_turn.get(ti),
                open_runs.get(ti),
            )
        ]
        for ti in turn_indexes
    }
    lost = [
        ti for ti in turn_indexes if _lost(responses_by_turn.get(ti), open_runs.get(ti))
    ]
    return lines, lost


def unavailable_item(thread_id: str) -> dict[str, Any]:
    """The close of a turn replay has nothing for: no projection, no
    archive. Without it the turn reads as an answer of nothing. An ``error``
    frame is what a client already shows on the turn's reply; ``internal``
    words it as this service's fault, which it is, whatever the run did. Its
    ``type`` tells the client the run did not fail, so the reply offers a
    Regenerate rather than a Retry, which reruns only a failed run."""
    return {
        "event": "error",
        "data": {
            "thread_id": thread_id,
            "error": "This response could not be loaded.",
            "type": "replay_unavailable",
            "error_kind": "internal",
        },
    }


def _stub_items(
    thread_id: str,
    turn_index: Any,
    queries: list[dict[str, Any]],
    response: dict[str, Any] | None,
    open_run: str | None,
) -> list[dict[str, Any]]:
    response_id = str(response.get("conversation_response_id")) if response else None
    # A turn still going shows what its run streams; an archive on its row is
    # an earlier attempt's.
    archived = [] if open_run else archived_stream(response) or []
    closing = items.terminal_items(thread_id, response, archived)
    if not closing and _lost(response, open_run):
        closing.append(unavailable_item(thread_id))
    turn_items = archived + closing
    for item in turn_items:
        items._enrich(item, thread_id, turn_index, response_id)
    return [
        items._user_message_item(thread_id, q, response, open_run) for q in queries
    ] + turn_items


# A turn is interrupted only by a pause the checkpoint holds, raised by a
# tool call the model made or by the credit gate past spend the turn already
# made, so it always streamed something.
_ANSWERED = ("completed", "interrupted")


def _lost(response: dict[str, Any] | None, open_run: str | None) -> bool:
    """A turn that answered, or paused on an interrupt after answering,
    whose row archived no stream: the answer is not here. A turn that failed
    or was stopped may have produced nothing (a stop before its first
    checkpoint), and its row closes it on its error or stop, so it reads as
    ended. A turn still going replays its stub and attaches live, so it has
    nothing to lose yet."""
    return (
        response is not None
        and open_run is None
        and response.get("status") in _ANSWERED
        and archived_stream(response) is None
    )
