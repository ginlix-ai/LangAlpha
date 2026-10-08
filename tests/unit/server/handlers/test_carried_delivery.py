"""A turn that resumes or retries one sending for an automation run sends for it too.

A turn sending for a run the messaging service holds (the run's own, or the
report-back of a hand-off from it) has the run in its config, and its tools
name the run on every send. The request that answers its interrupt, or
retries it after a failure, is a public one and names no run, so the turn
takes the run from the attempt it continues (``carry.carried_delivery``),
and stamps it on its own row so the turn after it does the same.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat.request_prep import PriorThread
from src.server.models.chat import ChatRequest
from src.server.services import automation_delivery
from src.server.services.automation_delivery import DELIVERY_KEY, Delivery, Target
from src.server.services.report_back.flash.carry import carried_delivery

PTC = "src.server.handlers.chat.ptc_run"
_LATEST = "src.server.database.runs.lifecycle.get_latest_attempt"
_GET_RUN = "src.server.database.runs.lifecycle.get_run"

_HELD = Delivery(
    id="exec-1",
    targets=[Target(entry="slack", address="slack:T1/C1", name="#research", ok=True)],
)
# A start whose entries all failed leaves the run to the webhook.
_LEFT = Delivery(
    id="exec-2",
    targets=[Target(entry="slack", address=None, name=None, ok=False, message="Not linked.")],
)


def _attempt(status: str, delivery: Delivery | None = None) -> dict:
    meta = automation_delivery.run_metadata(delivery) if delivery else {}
    return {"status": status, "metadata": meta}


def _resume() -> ChatRequest:
    return ChatRequest(
        workspace_id="ws-1",
        messages=[],
        hitl_response={"int-1": {"decisions": [{"type": "approve"}]}},
    )


def _retry() -> ChatRequest:
    return ChatRequest(
        workspace_id="ws-1", messages=[], checkpoint_id="cp-1", retry_of_run_id="run-0"
    )


def _message() -> ChatRequest:
    return ChatRequest(workspace_id="ws-1", messages=[{"role": "user", "content": "hi"}])


@pytest.mark.asyncio
async def test_a_resume_sends_for_the_run_its_interrupted_turn_sent_for():
    latest = AsyncMock(return_value=_attempt("interrupted", _HELD))
    with patch(_LATEST, latest):
        assert await carried_delivery(_resume(), "t-1") == (
            automation_delivery.turn_configurable(_HELD)
        )
    latest.assert_awaited_once_with("t-1")


@pytest.mark.asyncio
async def test_a_retry_sends_for_the_run_its_failed_attempt_sent_for():
    get_run = AsyncMock(return_value=_attempt("error", _HELD))
    latest = AsyncMock()
    with patch(_GET_RUN, get_run), patch(_LATEST, latest):
        assert await carried_delivery(_retry(), "t-1") == (
            automation_delivery.turn_configurable(_HELD)
        )
    get_run.assert_awaited_once_with("run-0")
    latest.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_message_past_the_interrupt_sends_for_nothing_and_reads_nothing():
    latest, get_run = AsyncMock(), AsyncMock()
    with patch(_LATEST, latest), patch(_GET_RUN, get_run):
        assert await carried_delivery(_message(), "t-1") is None
    latest.assert_not_awaited()
    get_run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "latest",
    [
        None,
        _attempt("completed", _HELD),
        _attempt("interrupted"),
        _attempt("interrupted", _LEFT),
    ],
    ids=["first_turn", "settled_turn", "no_run", "run_left_to_the_webhook"],
)
async def test_a_resume_carries_only_a_held_run_from_an_interrupted_turn(latest):
    with patch(_LATEST, AsyncMock(return_value=latest)):
        assert await carried_delivery(_resume(), "t-1") is None


@pytest.mark.asyncio
async def test_a_retry_carries_nothing_from_a_run_that_did_not_fail():
    with patch(_GET_RUN, AsyncMock(return_value=_attempt("completed", _HELD))):
        assert await carried_delivery(_retry(), "t-1") is None


@pytest.mark.asyncio
async def test_a_failed_read_fails_the_turn_start():
    with patch(_LATEST, AsyncMock(side_effect=ConnectionError("db down"))):
        with pytest.raises(ConnectionError):
            await carried_delivery(_resume(), "t-1")


# -- through the turn start ---------------------------------------------------


class _Admitted(Exception):
    """Raised from begin_run once it has been called, to end the run there."""


async def _stamped(
    request: ChatRequest,
    *,
    latest: dict | None = None,
    extra_configurable: dict | None = None,
    run_metadata: dict | None = None,
) -> dict:
    """Run the turn up to its START write; what it stamped there."""
    from src.server.handlers.chat.ptc_run import astream_ptc_workflow

    config = MagicMock()
    config.llm.name = "claude-test"
    config.subagents.enabled = False
    with (
        patch(f"{PTC}.setup"),
        patch(f"{PTC}.WorkspaceManager") as workspaces,
        patch(f"{PTC}.ensure_thread", new=AsyncMock(return_value=PriorThread())),
        patch(f"{PTC}.wait_or_steer", new=AsyncMock(return_value=(True, None))),
        patch(f"{PTC}.read_disk_notice", new=AsyncMock(return_value=(None, False))),
        patch(f"{PTC}.get_user_profile_for_prompt", new=AsyncMock(return_value=None)),
        patch(f"{PTC}._resolve_fork", return_value=("Q", None)),
        patch(f"{PTC}._resolve_origin_meta", new=AsyncMock(return_value={})),
        patch(f"{PTC}.carry.carried_pair", new=AsyncMock(return_value={})),
        patch(f"{PTC}.init_tracking", return_value=(MagicMock(), MagicMock())),
        patch(f"{PTC}.begin_run", new=AsyncMock(side_effect=_Admitted())) as begin,
        patch(_LATEST, new=AsyncMock(return_value=latest)),
    ):
        workspaces.get_instance.return_value.has_ready_session.return_value = True
        gen = astream_ptc_workflow(
            request=request,
            thread_id="t-1",
            run_id="r-1",
            user_input="",
            user_id="u-1",
            workspace_id="ws-1",
            is_byok=False,
            config=config,
            run_metadata=run_metadata,
            extra_configurable=extra_configurable,
        )
        try:
            async for _ in gen:
                pass
        except Exception:
            pass
        finally:
            await gen.aclose()
    begin.assert_awaited_once()
    return begin.await_args.kwargs["extra_run_metadata"]


@pytest.mark.asyncio
async def test_a_report_back_turn_records_the_run_it_sends_for():
    """What the turn after it reads to send for the run too. A report-back is
    not the run's own turn, so nothing here settles the run again."""
    stamped = await _stamped(
        _message(), extra_configurable=automation_delivery.turn_configurable(_HELD)
    )

    assert stamped[DELIVERY_KEY] == automation_delivery.stamp(_HELD)
    assert "automation_execution_id" not in stamped


@pytest.mark.asyncio
async def test_a_resumed_report_back_turn_sends_for_the_run():
    stamped = await _stamped(_resume(), latest=_attempt("interrupted", _HELD))

    # Recorded from the config the turn's tools read, so its sends name the
    # run, and a resume of this turn carries it on.
    assert stamped[DELIVERY_KEY] == automation_delivery.stamp(_HELD)
    assert "automation_execution_id" not in stamped


@pytest.mark.asyncio
async def test_the_runs_own_turn_keeps_the_stamp_its_start_wrote():
    """The run's own turn is stamped by its start, held or left, and that
    stamp is the one its settle reads."""
    own = automation_delivery.run_metadata(_LEFT)

    stamped = await _stamped(_message(), run_metadata=own)

    assert stamped[DELIVERY_KEY] == own[DELIVERY_KEY]


@pytest.mark.asyncio
async def test_a_turn_naming_its_run_takes_no_other():
    named = automation_delivery.turn_configurable(_HELD)
    other = Delivery(id="exec-9", targets=_HELD.targets)

    stamped = await _stamped(
        _resume(), latest=_attempt("interrupted", other), extra_configurable=named
    )

    assert stamped[DELIVERY_KEY]["id"] == "exec-1"
