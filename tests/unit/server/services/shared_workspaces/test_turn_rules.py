"""Which requests a member may make of a shared workspace's turns, each rule
pinned where it is decided."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from src.server.services.shared_workspaces.turns import refuse_crossing_workspaces

HOLDER = "shared-5f193e9b-ead2-45b8-8b78-d5b8c2668cc6"
PERSON = "b7000000-0000-4000-8000-000000000003"
SHARED_WS = "11111111-1111-4111-8111-111111111111"
OWN_WS = "22222222-2222-4222-8222-222222222222"
OTHER_OWN_WS = "33333333-3333-4333-8333-333333333333"


class TestAThreadRunsInItsOwnWorkspace:
    def test_a_shared_thread_sent_with_a_personal_workspace_is_refused(self):
        meta = {"user_id": HOLDER, "workspace_id": SHARED_WS}

        with pytest.raises(HTTPException) as e:
            refuse_crossing_workspaces(meta, OWN_WS, PERSON)

        assert e.value.status_code == 400
        assert e.value.detail["type"] == "thread_in_another_workspace"

    def test_a_personal_thread_sent_with_the_shared_workspace_is_refused(self):
        meta = {"user_id": PERSON, "workspace_id": OWN_WS}

        with pytest.raises(HTTPException) as e:
            refuse_crossing_workspaces(meta, SHARED_WS, HOLDER)

        assert e.value.status_code == 400

    def test_its_own_workspace_and_a_new_thread_pass(self):
        refuse_crossing_workspaces(
            {"user_id": HOLDER, "workspace_id": SHARED_WS}, SHARED_WS, HOLDER
        )
        refuse_crossing_workspaces(None, SHARED_WS, HOLDER)

    def test_a_personal_pair_keeps_what_it_did(self):
        # A Home thread on a former Home row runs in Home's current id.
        refuse_crossing_workspaces(
            {"user_id": PERSON, "workspace_id": OWN_WS}, OTHER_OWN_WS, PERSON
        )


class TestARunFromAnEarlierCheckpoint:
    """A send or retry naming a checkpoint replaces the turns after it, so it
    is held to the rule an edit or regenerate is: only your own turns."""

    @pytest.fixture
    def thread(self, monkeypatch):
        from types import SimpleNamespace

        from src.server.database.conversation import senders

        state = SimpleNamespace(others_from=None, paused=None)

        async def _others(thread_id, turn_index, user_id):
            return state.others_from is not None and turn_index <= state.others_from

        async def _paused(thread_id):
            return state.paused

        monkeypatch.setattr(senders, "others_turns_from", _others)
        monkeypatch.setattr(senders, "paused_turn_sender", _paused)
        return state

    @staticmethod
    def _request(**fields):
        from types import SimpleNamespace

        return SimpleNamespace(
            **{"fork_from_turn": None, "checkpoint_id": None, "hitl_response": None, **fields}
        )

    @pytest.mark.asyncio
    async def test_one_that_drops_another_members_turn_is_refused(self, thread):
        from src.server.services.shared_workspaces.turns import refuse_others_turns

        thread.others_from = 2

        with pytest.raises(HTTPException) as e:
            await refuse_others_turns(
                "t", self._request(checkpoint_id="cp-1"), PERSON, replaced=1
            )

        assert e.value.status_code == 403
        assert e.value.detail["type"] == "another_members_turn"

    @pytest.mark.asyncio
    async def test_a_send_while_another_members_turn_waits_on_them_is_refused(
        self, thread, monkeypatch
    ):
        from src.server.database import user
        from src.server.services.shared_workspaces.turns import refuse_others_turns

        async def _names(ids):
            return {"lee": "Lee Chen"}

        monkeypatch.setattr(user, "names_of", _names)
        thread.paused = "lee"

        with pytest.raises(HTTPException) as e:
            await refuse_others_turns("t", self._request(), PERSON, replaced=None)

        assert e.value.status_code == 409
        assert e.value.detail["code"] == "another_member"
        assert e.value.detail["member"] == {"user_id": "lee", "name": "Lee Chen"}
        assert e.value.detail["message"] == (
            "Waiting for Lee Chen to answer. You can send when their turn finishes."
        )

    @pytest.mark.asyncio
    async def test_the_member_whose_turn_waits_may_send_a_new_message(self, thread):
        from src.server.services.shared_workspaces.turns import refuse_others_turns

        thread.paused = PERSON
        await refuse_others_turns("t", self._request(), PERSON, replaced=None)

    @pytest.mark.asyncio
    async def test_one_that_only_adds_or_drops_only_your_turns_passes(self, thread):
        from src.server.services.shared_workspaces.turns import refuse_others_turns

        thread.others_from = 2
        await refuse_others_turns(
            "t", self._request(checkpoint_id="tip"), PERSON, replaced=None
        )
        await refuse_others_turns(
            "t", self._request(checkpoint_id="cp-3"), PERSON, replaced=3
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("fields", "replaced", "asked"),
        [
            ({"fork_from_turn": 2, "checkpoint_id": "cp"}, 2, []),
            ({"checkpoint_id": "cp-1"}, 1, ["cp-1"]),
            # An answer to a paused turn resumes its checkpoint; nothing is replaced.
            ({"checkpoint_id": "cp", "hitl_response": {"i": {}}}, None, []),
            ({}, None, []),
        ],
    )
    async def test_the_turn_a_request_replaces(self, monkeypatch, fields, replaced, asked):
        from src.server.handlers import checkpoint_handler

        seen = []

        async def _dropped(thread_id, checkpoint_id):
            seen.append(checkpoint_id)
            return 1

        monkeypatch.setattr(checkpoint_handler, "first_turn_dropped", _dropped)

        assert (
            await checkpoint_handler.first_turn_replaced("t", self._request(**fields))
            == replaced
        )
        assert seen == asked


class TestTheFirstTurnACheckpointDrops:
    @pytest.fixture
    def branch(self, monkeypatch):
        from types import SimpleNamespace

        from src.server.database.conversation import threads_read
        from src.server.handlers import checkpoint_handler
        from src.server.models.workflow import TurnCheckpointInfo
        from src.server.utils import checkpoint_helpers

        # Turns 0, 1, 2 start at a, c, e; b and d are inside turns 0 and 1;
        # g is the tip, f is a newer checkpoint built on it.
        parents = {"g": "e", "e": "d", "d": "c", "c": "b", "b": "a", "f": "g"}

        async def _read(thread_id):
            return SimpleNamespace(checkpoint_id="g", run_horizon=0)

        async def _on_branch(thread_id, checkpoint_id, tip, **_):
            node = tip
            while node is not None:
                if node == checkpoint_id:
                    return True
                node = parents.get(node)
            return False

        async def _turns(thread_id, tip):
            return SimpleNamespace(
                turns=[
                    TurnCheckpointInfo(turn_index=i, edit_checkpoint_id=c, regenerate_checkpoint_id=c)
                    for i, c in enumerate("ace")
                ]
            )

        monkeypatch.setattr(threads_read, "get_thread_branch_read", _read)
        monkeypatch.setattr(checkpoint_helpers, "checkpoint_on_branch", _on_branch)
        monkeypatch.setattr(checkpoint_handler, "_branch_turns", _turns)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("checkpoint", "dropped"),
        [("g", None), ("f", None), ("a", 0), ("b", 0), ("c", 1), ("d", 1), ("e", 2), ("x", 0)],
    )
    async def test_by_where_it_sits(self, branch, checkpoint, dropped):
        from src.server.handlers.checkpoint_handler import first_turn_dropped

        assert await first_turn_dropped("t", checkpoint) == dropped


class TestARunningTaskIsInstructedByItsLauncher:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("run", "refused"),
        [
            pytest.param({"user_id": "lee"}, True, id="another-members-task"),
            pytest.param({"user_id": PERSON}, False, id="own-task"),
            pytest.param({"user_id": None}, False, id="launcher-unrecorded"),
            pytest.param(None, False, id="not-running"),
        ],
    )
    async def test_by_whose_turn_launched_it(self, monkeypatch, run, refused):
        from unittest.mock import AsyncMock

        from src.server.database.runs import subagent_runs
        from src.server.services.shared_workspaces.turns import refuse_others_task

        active = AsyncMock(return_value=run)
        monkeypatch.setattr(subagent_runs, "get_active_task_run", active)
        if refused:
            with pytest.raises(HTTPException) as e:
                await refuse_others_task("t", "abc123", PERSON)
            assert (e.value.status_code, e.value.detail["type"]) == (403, "another_members_task")
        else:
            await refuse_others_task("t", "abc123", PERSON)
        active.assert_awaited_once_with("t", "abc123")


class TestAHumanReviewIsItsSendersToAskFor:
    """A review is a refund request for the turn, and agreement that the team
    reads the conversation: both belong to whoever sent and paid for it."""

    @pytest.fixture
    def sender(self, monkeypatch):
        from src.server.database.conversation import senders

        async def turn_sender(thread_id: str, turn_index: int):
            return PERSON if turn_index == 0 else None

        monkeypatch.setattr(senders, "turn_sender", turn_sender)

    @pytest.mark.asyncio
    async def test_the_sender_may(self, sender):
        from src.server.services.shared_workspaces.turns import refuse_others_review

        await refuse_others_review("thread", 0, PERSON)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("turn_index", [0, 7])
    async def test_another_member_may_not(self, sender, turn_index):
        from src.server.services.shared_workspaces.turns import refuse_others_review

        with pytest.raises(HTTPException) as e:
            await refuse_others_review("thread", turn_index, "another-member")

        assert e.value.status_code == 403
        assert e.value.detail["type"] == "another_members_turn"
