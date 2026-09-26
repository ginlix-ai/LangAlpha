"""Feed-local wire vocabulary contract.

The lifecycle feed is a SEPARATE SSE surface from the chat wire: no replay, no
checkpoint home, outside the event ledger's scan roots (like the workspace
bring-up stream). Classifying its events in the chat ledger would be a category
error — but the wire shape still needs a guard, because the client switches on
these exact strings. This is that guard.
"""

import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.server.app import user_events
from src.server.services import thread_lifecycle_feed

_FEED_SRC = Path(thread_lifecycle_feed.__file__)
_ENDPOINT_SRC = Path(user_events.__file__)

# SSE `event:` names the endpoint emits.
SSE_EVENTS = {"snapshot", "thread_lifecycle", "timeout"}

# Inner `type` of every payload carried by a `thread_lifecycle` frame.
LIFECYCLE_TYPES = {
    "run_started",
    "run_settled",
    "thread_title",
    "thread_pinned",
    "thread_deleted",
    "thread_archived",
    "thread_unarchived",
    "automation_waiting",
    "automation_waiting_ended",
}


def test_endpoint_emits_exactly_the_pinned_sse_events():
    text = _ENDPOINT_SRC.read_text()
    emitted = set(re.findall(r'"?event: ([a-z_]+)', text))
    emitted |= set(
        re.findall(r'^LIFECYCLE_EVENT = "([a-z_]+)"', _FEED_SRC.read_text(), re.M)
    )
    # LIFECYCLE_EVENT is referenced by name in the f-string, not spelled out.
    assert emitted == SSE_EVENTS, emitted


def test_lifecycle_event_name_is_the_one_the_client_switches_on():
    assert thread_lifecycle_feed.LIFECYCLE_EVENT == "thread_lifecycle"


def test_publishers_emit_exactly_the_pinned_inner_types():
    text = _FEED_SRC.read_text()
    emitted = set(re.findall(r'type="([a-z_]+)"', text))
    assert emitted == LIFECYCLE_TYPES, emitted


@pytest.mark.parametrize("type_name", sorted(LIFECYCLE_TYPES))
def test_every_pinned_type_builds_the_one_wire_shape(type_name):
    event = thread_lifecycle_feed.build_lifecycle_event(
        type=type_name, thread_id="t-1", workspace_id="ws-1"
    )
    assert set(event) == {
        "v",
        "type",
        "thread_id",
        "workspace_id",
        "run_id",
        "run_seq",
        "status",
        "interrupt_reason",
    }
    assert event["v"] == thread_lifecycle_feed.EVENT_VERSION


@pytest.mark.asyncio
@pytest.mark.parametrize("waiting", [True, False])
async def test_automation_wait_events_name_their_firing(waiting):
    with patch.object(thread_lifecycle_feed, "publish_user_event", AsyncMock()) as sent:
        await thread_lifecycle_feed.publish_automation_wait(
            user_id="user-1",
            thread_id="t-1",
            automation_execution_id="exec-1",
            waiting=waiting,
        )
    user_id, event = sent.await_args.args
    assert user_id == "user-1"
    assert event["type"] == (
        "automation_waiting" if waiting else "automation_waiting_ended"
    )
    assert event["automation_execution_id"] == "exec-1"
    assert set(event) == {
        "v",
        "type",
        "thread_id",
        "workspace_id",
        "run_id",
        "run_seq",
        "status",
        "interrupt_reason",
        "automation_execution_id",
    }


def test_interrupt_reason_is_nulled_off_the_interrupted_status():
    settled = thread_lifecycle_feed.build_lifecycle_event(
        type="run_settled",
        thread_id="t-1",
        status="completed",
        interrupt_reason="user_question",
    )
    assert settled["interrupt_reason"] is None

    interrupted = thread_lifecycle_feed.build_lifecycle_event(
        type="run_settled",
        thread_id="t-1",
        status="interrupted",
        interrupt_reason="user_question",
    )
    assert interrupted["interrupt_reason"] == "user_question"


def test_the_feed_stays_out_of_the_chat_event_ledger():
    """If the feed ever lands inside a ledger scan root, its no-replay events
    would demand a replay home they can never have."""
    from tests.unit.server.services.history import test_event_ledger

    for root in test_event_ledger._SCAN_ROOTS:
        assert root not in _FEED_SRC.parents
        assert root not in _ENDPOINT_SRC.parents
