"""A failed run is stamped with whose fault it was, as its live ``error``
frame named it, and a reload replays the failure by that kind.

A fault of this service's own replays with no text: the stored error is a
diagnostic naming the run and its stream, which a reload used to show the
user under a title guessed from its words. The turn's time limit is never
stamped, since its own frame is worded for the user and the reload keeps
those words.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.runs import lifecycle as tl_db
from src.server.database.runs.lifecycle import FinalizeResult, ProducerRecord, RunOutcome
from src.server.services.history.replay import items as replay_items
from src.server.services.runs.coordinator import RunCoordinator
from src.server.services.runs.finalization import (
    _producer_record,
    assemble_finalize_artifacts,
    classify_outcome,
)
from src.server.services.runs.sse_producer import RunSSEProducer, TurnTimeLimitError
from src.server.services.runs.stream_writer import StreamQuotaExceededError

_KEY = ("t-failed", "r-failed")
_QUOTA = (
    "transport_lost: stream quota exceeded for workflow:stream:t-failed:r-failed "
    "(5/4 events); finalizing instead of silently trimming the replay head"
)


class _ProviderError(Exception):
    """An exception a provider SDK raised, by its module."""


_ProviderError.__module__ = "openai"


async def _failed_metadata(exc: BaseException) -> dict:
    status, phase, interrupt_reason, pause_message, error = await classify_outcome(
        "failed", handler=None, graph=None, thread_id=_KEY[0], error=str(exc)
    )
    outcome = await assemble_finalize_artifacts(
        _KEY,
        metadata={},
        status=status,
        phase=phase,
        interrupt_reason=interrupt_reason,
        pause_message=pause_message,
        error=error,
        handler=None,
        cancelled_by_user=False,
        workspace_id=None,
        user_id=None,
        exc=exc,
    )
    return outcome.metadata


@pytest.mark.asyncio
async def test_a_run_whose_stream_ran_out_is_stamped_as_our_fault():
    assert (await _failed_metadata(StreamQuotaExceededError(_QUOTA)))["error_kind"] == "internal"


@pytest.mark.asyncio
async def test_a_provider_failure_is_stamped_as_the_providers():
    assert (await _failed_metadata(_ProviderError("Error code: 503")))["error_kind"] == "upstream"


@pytest.mark.asyncio
async def test_the_turn_time_limit_is_not_stamped():
    metadata = await _failed_metadata(TurnTimeLimitError("Workflow exceeded timeout of 60 seconds"))
    assert "error_kind" not in metadata


def _replayed_error(errors: list[str], metadata: dict) -> dict:
    row = {"status": "error", "errors": errors, "metadata": metadata}
    (item,) = replay_items.terminal_items(_KEY[0], row, [])
    assert item["event"] == "error"
    return item["data"]


def test_our_own_fault_replays_by_its_kind_without_the_diagnostic():
    data = _replayed_error([_QUOTA], {"error_kind": "internal"})
    assert data["error_kind"] == "internal"
    assert data["error"] == ""
    assert "status_code" not in data


def test_a_provider_failure_replays_its_words_and_status():
    data = _replayed_error(
        ["Error code: 401 - invalid key"],
        {"error_kind": "upstream", "error_status_code": 401},
    )
    assert data == {
        "thread_id": _KEY[0],
        "error": "Error code: 401 - invalid key",
        "type": "workflow_error",
        "error_kind": "upstream",
        "status_code": 401,
    }


def test_a_row_stamped_before_kinds_keeps_its_text():
    data = _replayed_error(["Turn timed out after 60 seconds"], {})
    assert data["error"] == "Turn timed out after 60 seconds"
    assert "error_kind" not in data


# ─── The frame a reload replays ───────────────────────────────────────


class _FailingGraph:
    """A graph whose stream raises before its first step."""

    def __init__(self, exc):
        self._exc = exc

    def astream(
        self, _input_state, config=None, stream_mode=None, subgraphs=None,
        durability=None,
    ):
        exc = self._exc

        async def _gen():
            raise exc
            yield  # pragma: no cover

        return _gen()


def _provider_outage() -> Exception:
    exc = _ProviderError("Error code: 503 - overloaded")
    exc.status_code = 503
    exc.__model_resilience__ = {
        "model": "primary",
        "attempted_models": [
            {
                "model": "primary",
                "error": "Error code: 503 - overloaded",
                "status_code": 503,
                "attempts": 2,
            }
        ],
    }
    return exc


async def _kept_frame(exc: Exception) -> dict:
    handler = RunSSEProducer(thread_id=_KEY[0], run_id=_KEY[1])
    with pytest.raises(type(exc)):
        async for _ in handler.stream_workflow(
            _FailingGraph(exc), input_state={}, config={}
        ):
            pass
    return handler.record().error_frame


def _failed_row(errors: list[str], metadata: dict, frame: dict) -> dict:
    return {
        "status": "error",
        "errors": errors,
        "metadata": metadata,
        "replay_facts": {"error_frame": frame},
    }


@pytest.mark.asyncio
async def test_a_reload_shows_a_provider_failure_as_the_live_frame_did():
    """The row alone has no model, hints or tried models, and a new turn has
    no stored events, so the frame the user saw is what the row keeps."""
    frame = await _kept_frame(_provider_outage())
    assert "thread_id" not in frame
    assert frame["model"] == "primary"
    assert frame["hints"]

    row = _failed_row(
        ["Error code: 503 - overloaded"],
        {"error_kind": "upstream", "error_status_code": 503},
        frame,
    )
    (item,) = replay_items.terminal_items("t-reloaded", row, [])

    assert item == {"event": "error", "data": {**frame, "thread_id": "t-reloaded"}}


def test_a_reload_keeps_the_time_limits_live_words():
    frame = {
        "error": "Turn timed out after 120 seconds",
        "type": "timeout_error",
        "elapsed_seconds": 120,
        "timeout_seconds": 120,
    }
    row = _failed_row(["Workflow exceeded timeout of 120 seconds"], {}, frame)

    (item,) = replay_items.terminal_items(_KEY[0], row, [])

    assert item["data"]["error"] == "Turn timed out after 120 seconds"
    assert "error_kind" not in item["data"]


def test_a_kept_frame_of_our_own_fault_drops_its_diagnostic():
    frame = {"error": _QUOTA, "error_kind": "internal"}
    row = _failed_row([_QUOTA], {"error_kind": "internal"}, frame)

    (item,) = replay_items.terminal_items(_KEY[0], row, [])

    assert item["data"]["error_kind"] == "internal"
    assert item["data"]["error"] == ""


def test_an_older_turns_stored_frame_is_stamped_and_no_twin_is_added():
    stored = {"event": "error", "data": {"thread_id": _KEY[0], "error": "live"}}
    row = _failed_row(["live"], {"error_kind": "upstream"}, {"error": "kept"})

    assert replay_items.terminal_items(_KEY[0], row, [stored]) == []
    assert stored["data"] == {
        "thread_id": _KEY[0],
        "error": "live",
        "error_kind": "upstream",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kept"), [("error", True), ("cancelled", False), ("completed", False)]
)
async def test_the_finalize_keeps_the_frame_only_beside_a_failure(status, kept):
    frame = {"error": "boom"}
    handler = SimpleNamespace(
        record=lambda stop_events: ProducerRecord(error_frame=frame)
    )

    record = await _producer_record(_KEY, handler, None, status)

    assert record.error_frame == (frame if kept else None)


# ─── A failed run with no owner left ──────────────────────────────────

_WORKER_LOST = "worker_lost: no live executor holds this run's writer fence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "kind"),
    [
        pytest.param(None, "internal", id="no_stream"),
        pytest.param(ProducerRecord(), "internal", id="no_frame"),
        pytest.param(
            ProducerRecord(
                error_frame={"error": "Error code: 500", "error_kind": "upstream"}
            ),
            "upstream",
            id="its_own_provider_frame",
        ),
        pytest.param(
            ProducerRecord(error_frame={"error": "Turn timed out after 120 seconds"}),
            None,
            id="its_own_time_limit",
        ),
    ],
)
async def test_a_recovered_failure_is_ours_unless_the_run_framed_its_own(record, kind):
    """Its worker died, a fault of this service's own, so a reload shows no
    diagnostic. A run that framed its failure before it died is replayed by
    that frame, so the row carries that frame's kind."""
    written = []

    async def finalize_run(**kwargs):
        written.append(kwargs["outcome"].metadata)
        return FinalizeResult(applied=False)

    with patch.object(tl_db, "finalize_run", side_effect=finalize_run):
        await RunCoordinator().finalize_detached_run(
            *_KEY,
            RunOutcome(
                status="error",
                errors=[_WORKER_LOST],
                metadata={"recovery": "scanner"},
                record=record,
            ),
        )

    (metadata,) = written
    assert metadata.get("error_kind") == kind
    if kind == "internal":
        assert _replayed_error([_WORKER_LOST], metadata)["error"] == ""


@pytest.mark.asyncio
async def test_a_dead_dispatch_reads_as_our_fault_live_and_on_a_reload():
    written = []

    async def finalize_run(**kwargs):
        written.append(kwargs["outcome"].metadata)
        return FinalizeResult(applied=True, run={"status": "error"})

    executor = SimpleNamespace(append_run_end_event=AsyncMock())
    coordinator = RunCoordinator()
    with (
        patch.object(tl_db, "finalize_run", side_effect=finalize_run),
        patch.object(coordinator, "post_finalize_tail"),
        patch.object(
            coordinator, "_detached_input_stamp", AsyncMock(return_value=(None, {}))
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=executor,
        ),
    ):
        await coordinator.reconcile_orphaned_dispatch(
            *_KEY, error_text="consumer died"
        )

    assert written[0]["error_kind"] == "internal"
    frame = executor.append_run_end_event.await_args.kwargs["error_frame"]
    assert frame["error_kind"] == "internal"
