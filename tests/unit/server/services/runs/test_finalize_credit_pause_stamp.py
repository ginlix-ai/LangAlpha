"""What the finalize records at the source so its readers never reopen the
turn's event archive.

A credit pause's denial is stamped into the run row's metadata, which is what
settling the run's automation relays; rows a release before the stamp
finalized still answer from the pause's stored interrupt. The producer's record
(its reasoning durations and provenance) rides the outcome, and a record that
cannot be read costs the row its facts, never its finalize.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from langgraph.types import Interrupt

from src.server.contracts.status import (
    INTERRUPT_REASON_CREDIT_PAUSE,
    classify_interrupts,
)
from src.server.database.runs.lifecycle import ProducerRecord
from src.server.services.automation_settlement import run_failure_message
from src.server.services.runs.finalization import (
    assemble_finalize_artifacts,
    classify_outcome,
)
from src.server.services.runs.recovery import RecoveryScanner
from src.server.services.runs.sse_producer import RunSSEProducer

_DENIAL = "You have used this month's credits."
_KEY = ("t-pause", "r-pause")


def _credit_pause(message=_DENIAL):
    request = {"type": INTERRUPT_REASON_CREDIT_PAUSE}
    if message is not None:
        request["message"] = message
    return request


def _question(message="Which filing?"):
    return {"type": "ask_user_question", "message": message}


def _payload(*requests):
    """One interrupt payload, as the graph raises it."""
    return Interrupt(value={"action_requests": list(requests)}, id="intr-1")


class _ScriptedGraph:
    def __init__(self, script):
        self._script = script

    def astream(
        self,
        _input_state,
        config=None,
        stream_mode=None,
        subgraphs=None,
        durability=None,
    ):
        async def _gen():
            for step in self._script:
                yield step

        return _gen()


async def _streamed(*script, durable=True):
    """A producer that ran the real stream loop over ``script``; a pause in
    it passes the durability barrier when ``durable``."""
    handler = RunSSEProducer(thread_id=_KEY[0], run_id=_KEY[1])

    async def _verify(_graph):
        return durable

    handler._verify_interrupt_durable = _verify
    async for _ in handler.stream_workflow(
        _ScriptedGraph(script), input_state={}, config={}
    ):
        pass
    return handler


def _paused_on(*requests):
    return ((), "updates", {"__interrupt__": (_payload(*requests),)})


async def _outcome(handler, kind="stream_end", *, graph=None, stop_events=None):
    """The outcome the executor's finalize would write for ``handler``'s run
    ending as ``kind``."""
    status, phase, interrupt_reason, pause_message, error = await classify_outcome(
        kind, handler=handler, graph=graph, thread_id=_KEY[0], error=None
    )
    return await assemble_finalize_artifacts(
        _KEY,
        metadata={"handler": handler},
        status=status,
        phase=phase,
        interrupt_reason=interrupt_reason,
        pause_message=pause_message,
        error=error,
        handler=handler,
        cancelled_by_user=False,
        workspace_id=None,
        user_id=None,
        stop_events=stop_events,
    )


async def _persist_metadata(handler, kind="stream_end", *, graph=None):
    return (await _outcome(handler, kind, graph=graph)).metadata


# ─── The finalize stamps the pause's words ────────────────────────────


@pytest.mark.asyncio
async def test_a_credit_pause_stamps_its_denial():
    handler = await _streamed(_paused_on(_credit_pause()))
    assert handler.interrupt_reason == INTERRUPT_REASON_CREDIT_PAUSE

    persist_metadata = await _persist_metadata(handler)

    assert persist_metadata["credit_pause_message"] == _DENIAL


@pytest.mark.asyncio
async def test_a_credit_pause_without_words_is_stamped_null():
    # Settling then knows there is nothing to relay without the archive.
    handler = await _streamed(_paused_on(_credit_pause(message=None)))

    persist_metadata = await _persist_metadata(handler)

    assert persist_metadata["credit_pause_message"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requests", "kind", "durable"),
    [
        ((_question(),), "stream_end", True),
        ((_credit_pause(),), "cancelled", True),
        ((_credit_pause(),), "stream_end", False),
    ],
    ids=["paused_on_a_question", "settled_cancelled", "pause_not_durable"],
)
async def test_nothing_is_stamped_unless_a_credit_pause_settles(
    requests, kind, durable
):
    handler = await _streamed(_paused_on(*requests), durable=durable)

    persist_metadata = await _persist_metadata(handler, kind)

    assert "credit_pause_message" not in persist_metadata


class _PausedGraph:
    """A graph whose checkpoint is paused on ``requests``, for the
    handler-less finalize's state probe."""

    def __init__(self, *requests):
        task = SimpleNamespace(interrupts=(_payload(*requests),))
        self._snapshot = SimpleNamespace(next=("tools",), tasks=(task,))

    async def aget_state(self, _config):
        return self._snapshot


@pytest.mark.asyncio
async def test_a_handler_less_credit_pause_stamps_its_denial():
    # The state probe classifies the pause from the same interrupts the
    # producer would have buffered, so it stamps the same words.
    metadata = await _persist_metadata(None, graph=_PausedGraph(_credit_pause()))
    assert metadata["credit_pause_message"] == _DENIAL

    metadata = await _persist_metadata(None, graph=_PausedGraph(_question()))
    assert "credit_pause_message" not in metadata
    assert "credit_pause_message" not in await _persist_metadata(None)


# ─── Recovery stamps a pause it settles ───────────────────────────────


async def _recovered_outcome(*requests, salvaged=None):
    """The outcome the scanner finalizes a dead run with, its tip checkpoint
    pending ``requests`` as one interrupt and its stream salvaged as
    ``salvaged``."""
    tip = SimpleNamespace(
        metadata={"run_id": _KEY[1]},
        config={"configurable": {"checkpoint_id": "cp-tip"}},
        pending_writes=[("task-1", "__interrupt__", [_payload(*requests)])],
    )
    saver = SimpleNamespace(aget_tuple=AsyncMock(return_value=tip))
    finalize = AsyncMock(return_value=SimpleNamespace(applied=True, run={}))
    coordinator = SimpleNamespace(finalize_detached_run=finalize)
    scanner = RecoveryScanner()
    scanner._salvage_stream = AsyncMock(return_value=(salvaged, "empty_stream"))
    with (
        patch("src.server.app.setup.checkpointer", saver),
        patch(
            "src.server.services.runs.coordinator.RunCoordinator.get_instance",
            return_value=coordinator,
        ),
    ):
        await scanner._recover_run({}, _KEY[1], _KEY[0])
    outcome = finalize.await_args.args[2]
    assert outcome.status == "interrupted"
    return outcome


async def _recovered_metadata(*requests):
    return (await _recovered_outcome(*requests)).metadata


@pytest.mark.asyncio
async def test_a_recovered_credit_pause_stamps_its_denial():
    # The scanner's salvaged archive is not where settling reads it.
    metadata = await _recovered_metadata(_credit_pause())
    assert metadata["credit_pause_message"] == _DENIAL
    assert metadata["recovery"] == "scanner"


@pytest.mark.asyncio
async def test_a_recovered_pause_is_stamped_only_when_it_is_a_credit_pause():
    assert (await _recovered_metadata(_credit_pause(message=None)))[
        "credit_pause_message"
    ] is None
    assert "credit_pause_message" not in await _recovered_metadata(_question())


@pytest.mark.asyncio
async def test_a_recovered_run_records_its_salvaged_provenance_but_no_durations():
    # Nothing timed the dead run's thinking, so its row keeps no reasoning
    # facts and replay reads the durations off the salvaged events instead.
    found = {
        "event": "provenance",
        "data": {"agent": "main", "source_type": "web", "identifier": "https://a.test"},
    }
    chunk = {"event": "message_chunk", "data": {"agent": "main", "content": "x"}}

    outcome = await _recovered_outcome(_question(), salvaged=[chunk, found])

    assert outcome.record == ProducerRecord(provenance=[found], reasoning_ms=None)
    assert (await _recovered_outcome(_question())).record == ProducerRecord()


# ─── classify_interrupts: the pause's words ──────────────────────────


def _words(interrupts):
    return classify_interrupts(interrupts)[1]


def test_the_last_credit_pause_with_words_wins():
    assert (
        _words(
            [_payload(_credit_pause("First denial.")), _payload(_credit_pause(_DENIAL))]
        )
        == _DENIAL
    )
    # A later pause without words does not erase the earlier one's, as the
    # stored-event reader never did either.
    assert (
        _words(
            [_payload(_credit_pause(_DENIAL)), _payload(_credit_pause(message=None))]
        )
        == _DENIAL
    )


def test_another_questions_words_are_never_relayed():
    assert classify_interrupts(
        [_payload(_credit_pause(_DENIAL)), _payload(_question("Proceed anyway?"))]
    ) == (INTERRUPT_REASON_CREDIT_PAUSE, _DENIAL)
    assert classify_interrupts([_payload(_question("Proceed anyway?"))]) == (
        "user_question",
        None,
    )


@pytest.mark.parametrize(
    "interrupts",
    [
        [],
        [Interrupt(value="not a dict")],
        [
            _payload(
                {"type": INTERRUPT_REASON_CREDIT_PAUSE, "message": {"text": _DENIAL}}
            )
        ],
    ],
    ids=["none", "not_a_payload", "words_not_a_string"],
)
def test_no_credit_pause_words_is_none(interrupts):
    assert _words(interrupts) is None


# ─── run_failure_message reads the stamp first ────────────────────────


def _paused_row(metadata, *events):
    return {
        "status": "interrupted",
        "interrupt_reason": INTERRUPT_REASON_CREDIT_PAUSE,
        "conversation_response_id": _KEY[1],
        "errors": None,
        "metadata": metadata,
        "sse_events": list(events) if events else None,
    }


def _pause_event(message):
    return {"event": "interrupt", "data": {"action_requests": [_credit_pause(message)]}}


def test_the_stamp_is_preferred_over_the_stored_events():
    row = _paused_row({"credit_pause_message": _DENIAL}, _pause_event("Older words."))
    assert run_failure_message(row) == _DENIAL


def test_the_stamp_needs_no_stored_events():
    # The settlement read leaves the archive out once the stamp is there.
    assert (
        run_failure_message(_paused_row({"credit_pause_message": _DENIAL})) == _DENIAL
    )


@pytest.mark.parametrize(
    "metadata", [{}, None, {"deepthinking": False}], ids=["empty", "none", "unstamped"]
)
def test_an_older_row_still_answers_from_its_stored_pause(metadata):
    assert run_failure_message(_paused_row(metadata, _pause_event(_DENIAL))) == _DENIAL


def test_the_stamp_is_read_only_for_a_credit_pause():
    row = {
        "status": "error",
        "interrupt_reason": None,
        "conversation_response_id": _KEY[1],
        "errors": ["upstream refused"],
        "metadata": {"credit_pause_message": _DENIAL},
    }
    assert run_failure_message(row) == "upstream refused"


# ─── The producer's record ────────────────────────────────────────────


def _provenance(agent, identifier):
    return {
        "event": "provenance",
        "data": {"source_type": "web_search", "identifier": identifier, "agent": agent},
    }


@pytest.mark.asyncio
async def test_the_outcome_carries_the_record_with_the_stop_drains_provenance():
    handler = await _streamed(
        (
            (),
            "custom",
            {
                "type": "provenance",
                "source_type": "web_search",
                "identifier": "https://example.test/a",
                "agent": None,
            },
        )
    )
    main = handler.provenance_events
    assert [e["data"]["agent"] for e in main] == ["main"]
    drained = _provenance("task:k7", "https://example.test/b")

    outcome = await _outcome(
        handler,
        "cancelled",
        stop_events=[{"event": "message_chunk", "data": {}}, drained, "not an event"],
    )

    assert outcome.record == ProducerRecord(
        provenance=[*main, drained], reasoning_ms={}
    )


@pytest.mark.asyncio
async def test_a_main_lane_with_no_provenance_records_an_empty_list():
    """A producer always timed the turn, even when nothing was measured, so
    its facts are written and replay never reads them off the events."""
    handler = await _streamed()
    drained = _provenance("task:k7", "https://example.test/b")

    assert handler.record() == ProducerRecord(provenance=[], reasoning_ms={})
    assert handler.record([drained]).provenance == [drained]


@pytest.mark.asyncio
async def test_a_record_that_cannot_be_read_never_blocks_the_finalize():
    # The finalize must still reach its CAS: a raise here would leave the
    # row in_progress for recovery.
    handler = await _streamed()

    def _raises(_stop_events=None):
        raise RuntimeError("record unavailable")

    handler.record = _raises
    outcome = await _outcome(handler)

    assert outcome.status == "completed"
    assert outcome.record is None


@pytest.mark.asyncio
async def test_a_run_without_a_producer_has_no_record():
    assert (await _outcome(None)).record is None
