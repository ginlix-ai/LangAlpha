"""Rooms: who speaks, in which order, and what a member is handed.

The round decisions are pure functions here; the driver's DB and turn posting
are exercised by the app-level tests and the sweeper's own contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.rooms.context import is_pass, render_member_context
from src.server.services.rooms.coordinator import (
    _initial_slots,
    parse_mentions,
    start_round,
)

HOST = {"bot_id": "b-host", "name": "Chief", "room_role": "host", "status": "active", "workspace_id": "ws"}
A = {"bot_id": "b-a", "name": "数据官", "room_role": "member", "status": "active", "workspace_id": "ws"}
B = {"bot_id": "b-b", "name": "数据官2", "room_role": "member", "status": "active", "workspace_id": "ws"}
PAUSED = {"bot_id": "b-p", "name": "Sleepy", "room_role": "member", "status": "paused", "workspace_id": "ws"}
ROOM = {"room_id": "r-1", "name": "Research", "responder": "everyone", "bulletin": "One short message each."}


def test_everyone_speaks_host_first_and_paused_members_are_left_out():
    slots = _initial_slots(ROOM, [A, PAUSED, HOST, B], "hello")
    assert [s["bot_id"] for s in slots] == ["b-host", "b-a", "b-b"]


def test_mentions_responder_keeps_only_the_named_member():
    room = {**ROOM, "responder": "mentions"}
    slots = _initial_slots(room, [A, HOST], "@数据官 take this")
    assert [s["bot_id"] for s in slots] == ["b-a"]


def test_mentions_with_nobody_named_falls_back_to_everyone():
    room = {**ROOM, "responder": "mentions"}
    slots = _initial_slots(room, [A, HOST], "no mention here")
    assert [s["bot_id"] for s in slots] == ["b-host", "b-a"]


def test_a_named_responder_bot_speaks_alone():
    room = {**ROOM, "responder": "member:b-a"}
    slots = _initial_slots(room, [A, HOST], "hi")
    assert [s["bot_id"] for s in slots] == ["b-a"]


def test_mentions_use_the_longest_name_and_need_a_boundary():
    members = [A, B]
    # 数据官2 is a different bot from 数据官; the longer name wins.
    assert parse_mentions("@数据官2 please", members) == ["b-b"]
    # A bare prefix of the longer name is not a mention of either.
    assert parse_mentions("@数据官3 please", members) == []
    assert parse_mentions("@数据官，请", members) == ["b-a"]
    assert parse_mentions("@数据官 and @数据官2", members) == ["b-a", "b-b"]


def test_pass_is_recognised_in_the_shapes_a_model_uses():
    assert is_pass("[PASS]")
    assert is_pass(" [PASS] ")
    assert is_pass("[pass]")
    assert not is_pass("pass")
    assert not is_pass("I'll pass on that")


def test_the_member_context_carries_the_room_its_increments_and_its_turn():
    text = render_member_context(
        ROOM,
        [HOST, A],
        A,
        earlier=[{"seq": 1, "sender_type": "user", "sender_id": "u", "content": "old"}],
        increments=[
            {"seq": 2, "sender_type": "user", "sender_id": "u", "content": "new question"},
            {"seq": 3, "sender_type": "bot", "sender_id": "b-host", "content": "on it"},
        ],
        order_index=1,
        order_total=2,
    )
    assert '[Group chat "Research"]' in text
    assert "数据官" in text
    assert "Announcement:" in text
    assert "[2] the user: new question" in text
    assert "You speak 2 of 2" in text
    assert "[PASS]" in text


@pytest.mark.asyncio
async def test_a_user_message_opens_a_round_when_none_is_live():
    room = {"room_id": "r-1", "user_id": "u-1", "name": "R", "responder": "everyone"}
    created = {"round_id": "round-1", "status": "queued", "slots": []}
    with patch(
        "src.server.services.rooms.coordinator.room_db.append_log",
        AsyncMock(return_value={"seq": 7}),
    ), patch(
        "src.server.services.rooms.coordinator.room_db.active_round",
        AsyncMock(return_value=None),
    ), patch(
        "src.server.services.rooms.coordinator.room_db.list_members_with_bots",
        AsyncMock(return_value=[HOST, A]),
    ), patch(
        "src.server.services.rooms.coordinator.room_db.create_round",
        AsyncMock(return_value=created),
    ) as make, patch(
        "src.server.services.rooms.coordinator.schedule_start"
    ) as schedule:
        result = await start_round(room, "hello", user_id="u-1")
    assert result["round"]["round_id"] == "round-1"
    assert result["message"]["seq"] == 7
    make.assert_awaited_once()
    schedule.assert_called_once()


@pytest.mark.asyncio
async def test_a_message_during_a_live_round_joins_it_instead_of_opening_another():
    room = {"room_id": "r-1", "user_id": "u-1", "name": "R", "responder": "everyone"}
    live = {"round_id": "round-live", "status": "running"}
    with patch(
        "src.server.services.rooms.coordinator.room_db.append_log",
        AsyncMock(return_value={"seq": 8}),
    ), patch(
        "src.server.services.rooms.coordinator.room_db.active_round",
        AsyncMock(return_value=live),
    ), patch(
        "src.server.services.rooms.coordinator.room_db.create_round",
        AsyncMock(side_effect=AssertionError("must not open a second round")),
    ), patch(
        "src.server.services.rooms.coordinator.schedule_start"
    ):
        result = await start_round(room, "another point", user_id="u-1")
    assert result["round"]["round_id"] == "round-live"


@pytest.mark.asyncio
async def test_a_round_walks_its_slots_logs_answers_and_finishes():
    from src.server.services.rooms import coordinator

    slots0 = [
        {"bot_id": "b-a", "name": "数据官", "state": "pending"},
        {"bot_id": "b-host", "name": "Chief", "state": "pending"},
    ]
    slots1 = [slots0[0], {**slots0[1], "state": "pending"}]
    rounds = [
        {"round_id": "r-1", "room_id": "r-1", "status": "running", "slots": slots0, "slot_cursor": 0, "start_seq": 1},
        {"round_id": "r-1", "room_id": "r-1", "status": "running", "slots": [{**slots0[0], "state": "done"}, slots0[1]], "slot_cursor": 1, "start_seq": 1},
        {"round_id": "r-1", "room_id": "r-1", "status": "running", "slots": [{**slots0[0], "state": "done"}, {**slots0[1], "state": "done"}], "slot_cursor": 2, "start_seq": 1},
    ]
    room = {"room_id": "r-1", "user_id": "u-1", "name": "R", "responder": "everyone"}
    with patch.object(coordinator.room_db, "claim_round", AsyncMock(return_value=True)), \
         patch.object(coordinator.room_db, "get_round", AsyncMock(side_effect=rounds)), \
         patch.object(coordinator.room_db, "get_room", AsyncMock(return_value=room)), \
         patch.object(coordinator.room_db, "list_members_with_bots", AsyncMock(return_value=[HOST, A])), \
         patch.object(coordinator, "ensure_room_thread", AsyncMock(return_value="th-1")), \
         patch.object(coordinator.room_db, "seen_seq", AsyncMock(return_value=0)), \
         patch.object(coordinator.room_db, "read_log", AsyncMock(return_value=[])), \
         patch.object(coordinator.room_db, "read_log_before", AsyncMock(return_value=[])), \
         patch.object(coordinator.room_db, "save_round_progress", AsyncMock()) as save, \
         patch.object(coordinator.room_db, "set_seen", AsyncMock()) as seen, \
         patch.object(coordinator.room_db, "append_log", AsyncMock(side_effect=lambda *a, **k: {"seq": 10 + random_seq()})) as log, \
         patch.object(coordinator.room_db, "finish_round", AsyncMock()) as finish, \
         patch.object(coordinator, "_wait_terminal", AsyncMock(return_value="completed")), \
         patch.object(coordinator, "latest_run_reply", AsyncMock(side_effect=[{"text": "answer one"}, {"text": "[PASS]"}])), \
         patch.object(coordinator.room_db, "latest_seq", AsyncMock(return_value=0)), \
         patch.object(coordinator.room_db, "get_slot_reply", AsyncMock(return_value=None)), \
         patch.object(coordinator.room_db, "latest_user_entry", AsyncMock(return_value=None)), \
         patch.object(coordinator.room_db, "round_for_start", AsyncMock(return_value=None)), \
         patch("src.server.services.report_back.notify_turn.post_notification_turn", AsyncMock(return_value=("dispatched", "run-1"))):
        await coordinator.drive_round("r-1")

    assert any(
        call.kwargs.get("content") == "answer one"
        for call in log.await_args_list
    ), "the answer must reach the shared log"
    finish.assert_awaited_once()
    assert save.await_count >= 2
    assert seen.await_count >= 2  # a reply and a pass each advance the cursor


_SEQ = {"n": 10}


def random_seq() -> int:
    _SEQ["n"] += 1
    return _SEQ["n"]


@pytest.mark.asyncio
async def test_a_late_message_opens_a_follow_up_round():
    """A user message that arrived after the round opened was never covered by
    any slot; finishing the round must open one that answers it."""
    from src.server.services.rooms import coordinator

    room = {"room_id": "r-1", "user_id": "u-1", "name": "R", "responder": "everyone"}
    round_row = {"round_id": "r-1", "start_seq": 3}
    with patch.object(
        coordinator.room_db,
        "latest_user_entry",
        AsyncMock(return_value={"seq": 7, "content": "and this?", "sender_type": "user"}),
    ), patch.object(
        coordinator.room_db, "list_members_with_bots", AsyncMock(return_value=[HOST, A])
    ), patch.object(
        coordinator.room_db,
        "create_round",
        AsyncMock(return_value={"round_id": "r-2"}),
    ) as create:
        follow = await coordinator._open_follow_up_round(room, round_row)
    assert follow == "r-2"
    assert create.await_args.kwargs["start_seq"] == 7


@pytest.mark.asyncio
async def test_the_message_that_opened_the_round_is_not_replayed():
    from src.server.services.rooms import coordinator

    round_row = {"round_id": "r-1", "start_seq": 3}
    with patch.object(
        coordinator.room_db,
        "latest_user_entry",
        AsyncMock(return_value={"seq": 3, "content": "the question"}),
    ), patch.object(coordinator.room_db, "create_round", AsyncMock()) as create:
        follow = await coordinator._open_follow_up_round(
            {"room_id": "r-1", "user_id": "u-1"}, round_row
        )
    assert follow is None
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_retried_room_send_returns_the_message_it_already_opened():
    from src.server.services.rooms import coordinator

    room = {"room_id": "r-1", "user_id": "u-1", "responder": "everyone"}
    existing = {"seq": 5, "content": "hello"}
    with patch.object(
        coordinator.room_db,
        "get_log_by_request_key",
        AsyncMock(return_value=existing),
    ), patch.object(
        coordinator.room_db,
        "round_for_start",
        AsyncMock(return_value={"round_id": "r-9"}),
    ), patch.object(
        coordinator.room_db, "append_log", AsyncMock()
    ) as append:
        result = await start_round(room, "hello", user_id="u-1", request_key="k-1")
    assert result["message"] == existing
    assert result["round"] == {"round_id": "r-9"}
    append.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_resumed_slot_adopts_the_reply_it_already_logged():
    from src.server.services.rooms import coordinator

    round_row = {"round_id": "r-1", "start_seq": 1}
    bot = {
        "bot_id": "b-a",
        "name": "数据官",
        "status": "active",
        "workspace_id": "ws",
        "user_id": "u-1",
    }
    with patch.object(coordinator.room_db, "seen_seq", AsyncMock(return_value=0)), \
         patch.object(coordinator, "ensure_room_thread", AsyncMock(return_value="th-1")), \
         patch.object(coordinator.room_db, "latest_seq", AsyncMock(return_value=4)), \
         patch.object(coordinator.room_db, "read_log", AsyncMock(return_value=[])), \
         patch.object(coordinator.room_db, "read_log_before", AsyncMock(return_value=[])), \
         patch.object(coordinator, "render_member_context", lambda *a, **k: "context"), \
         patch("src.server.services.report_back.notify_turn.post_notification_turn", AsyncMock(return_value=("dispatched", "run-1"))) as posted, \
         patch.object(coordinator, "_wait_terminal", AsyncMock(return_value="completed")), \
         patch.object(coordinator, "latest_run_reply", AsyncMock(return_value={"text": "same answer"})), \
         patch.object(coordinator.room_db, "append_log", AsyncMock(return_value=None)), \
         patch.object(coordinator.room_db, "get_slot_reply", AsyncMock(return_value={"seq": 11})):
        result, logged = await coordinator._speak(
            {"room_id": "r-1", "user_id": "u-1"}, round_row, [bot], 0, bot
        )
    assert result["state"] == "done"
    assert result["seq"] == 11
    assert logged["seq"] == 11
    body = posted.await_args.kwargs["body"]
    assert body["suppress_push"] is True
    assert body["messages"][0]["content"].startswith("<system-reminder>")
    # The wire's request_key is a UUID column; a "room:..." string is a 422
    # and the slot is dropped without a run.
    import uuid as _uuid

    from src.server.models.chat import ChatRequest

    _uuid.UUID(body["request_key"])
    ChatRequest(
        messages=[{"role": "user", "content": "x"}],
        request_key=body["request_key"],
    )


def test_a_log_row_stringifies_its_uuids_for_json():
    import uuid as _uuid
    from datetime import datetime, timezone

    from src.server.database.rooms import _log_row

    row = _log_row(
        {
            "seq": 4,
            "room_id": _uuid.uuid4(),
            "round_id": _uuid.uuid4(),
            "sender_type": "user",
            "sender_id": "u-1",
            "kind": "message",
            "content": "hi",
            "created_at": datetime(2026, 11, 9, tzinfo=timezone.utc),
        }
    )
    assert isinstance(row["room_id"], str)
    assert isinstance(row["round_id"], str)
    assert isinstance(row["created_at"], str)


@pytest.mark.asyncio
async def test_a_room_thread_is_named_for_its_room_and_bot():
    from src.server.services.rooms import threads as room_threads

    room = {"room_id": "r-1", "name": "量化团队"}
    bot = {
        "bot_id": "b-1",
        "name": "风控官",
        "workspace_id": "ws-1",
        "user_id": "u-1",
    }
    with patch.object(
        room_threads.room_db, "get_room_thread", AsyncMock(return_value=None)
    ), patch.object(
        room_threads.room_db, "bind_room_thread", AsyncMock(return_value="t-1")
    ), patch.object(
        room_threads.bot_db, "bind_thread_bot", AsyncMock()
    ), patch(
        "src.server.database.conversation.threads_write.ensure_thread_exists",
        AsyncMock(),
    ) as ensure:
        thread_id = await room_threads.ensure_room_thread(room, bot)
    assert thread_id == "t-1"
    assert ensure.await_args.kwargs["initial_query"] == "量化团队 · 风控官"


@pytest.mark.asyncio
async def test_a_race_losing_room_thread_is_archived():
    from src.server.services.rooms import threads as room_threads

    room = {"room_id": "r-1", "name": "量化团队"}
    bot = {
        "bot_id": "b-1",
        "name": "风控官",
        "workspace_id": "ws-1",
        "user_id": "u-1",
    }
    with patch.object(
        room_threads.room_db, "get_room_thread", AsyncMock(return_value=None)
    ), patch.object(
        room_threads.room_db, "bind_room_thread", AsyncMock(return_value="t-winner")
    ), patch(
        "src.server.database.conversation.threads_write.ensure_thread_exists",
        AsyncMock(),
    ), patch(
        "src.server.database.conversation.threads_write.update_thread_fields",
        AsyncMock(),
    ) as archive:
        thread_id = await room_threads.ensure_room_thread(room, bot)
    assert thread_id == "t-winner"
    archived_id = archive.await_args.args[0]
    assert archived_id != "t-winner"
    assert archive.await_args.kwargs["archived"] is True
