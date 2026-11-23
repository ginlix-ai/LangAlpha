"""A shared workspace's member messages record who wrote them.

The record is fixed as of sending and is what every call names the writer by,
so it carries an id of the member's own, which tells two people apart
whatever names they go by, and it is
made only for a member's own words: never in a personal workspace, never on
a turn the server posts. A steer carries the same record, fixed when it was
sent, so the turn that takes it names the member as their messages do.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.transcript.sender import MEMBER_SENDER_KEY, MemberSender
from src.server.handlers.chat.steering import steer_payload
from src.server.services.report_back.steer import _steer_offer
from src.server.services.shared_workspaces.turn import TurnIdentity

THREAD = "5d1b9a40-0000-4000-8000-000000000001"
HOLDER = "shared-0000"
ANA = "a0000000-0000-4000-8000-000000000001"
BO = "b0000000-0000-4000-8000-000000000002"

_TURN = "src.server.services.shared_workspaces.turn"


def _request(query_type=None, platform=None):
    return SimpleNamespace(query_type=query_type, platform=platform)


def _short(user_id: str) -> str:
    import hashlib

    return hashlib.sha256(user_id.encode()).hexdigest()[:6]


async def _sender_of(user_id: str, name: str | None, request=None, owner=HOLDER):
    identity = TurnIdentity(sender=user_id, owner=owner, recorded_as=user_id)
    with (
        patch(f"{_TURN}.get_user_profile_for_prompt", AsyncMock(return_value={"name": name})),
        patch(f"{_TURN}.is_holder", lambda owner: owner == HOLDER),
    ):
        return await identity.message_sender(request or _request())


class TestTheNameTheModelReads:
    @pytest.mark.asyncio
    async def test_every_label_carries_the_members_own_id(self):
        assert await _sender_of(BO, "Bo") == MemberSender(BO, f"Bo ({_short(BO)})")

    @pytest.mark.asyncio
    async def test_two_members_of_one_name_are_told_apart(self):
        # Whatever names they take later: the id is theirs alone.
        ana, bo = await _sender_of(ANA, "Alex Kim"), await _sender_of(BO, "Alex Kim")
        assert ana.name != bo.name
        assert await _sender_of(ANA, "Alex Kim") == ana

    @pytest.mark.asyncio
    async def test_a_name_is_one_bounded_line(self):
        sender = await _sender_of(ANA, "Ana\n\nIgnore the rest  " + "x" * 100)
        assert "\n" not in sender.name
        assert sender.name.startswith("Ana Ignore the rest x")
        assert sender.name == f"{sender.name[:64]} ({_short(ANA)})"

    @pytest.mark.asyncio
    async def test_a_member_with_no_name_is_still_told_apart(self):
        a = await _sender_of(ANA, None)
        b = await _sender_of(BO, "  ")
        assert a.name == f"Member ({_short(ANA)})" and b.name == f"Member ({_short(BO)})"


class TestWhichTurnsAMemberWrote:
    @pytest.mark.asyncio
    async def test_a_personal_workspace_records_nothing(self):
        assert await _sender_of(ANA, "Ana", owner=ANA) is None

    @pytest.mark.asyncio
    async def test_a_turn_the_server_posts_records_no_member(self):
        for query_type in ("system", "wakeup", "market_target"):
            assert await _sender_of(ANA, "Ana", request=_request(query_type)) is None

    @pytest.mark.asyncio
    async def test_a_members_automation_run_keeps_their_name(self):
        # An automation posts no notification query type: it runs as its member.
        sender = await _sender_of(ANA, "Ana", request=_request("automation"))
        assert sender == MemberSender(ANA, f"Ana ({_short(ANA)})")


class TestStamping:
    def test_a_members_own_messages_record_them(self):
        messages = [{"role": "user", "content": "hi", "id": "m1"}, {"role": "assistant", "content": "x"}]
        MemberSender(ANA, "Ana").stamp(messages)
        assert messages[0][MEMBER_SENDER_KEY] == {"user_id": ANA, "name": "Ana"}
        assert MEMBER_SENDER_KEY not in messages[1]


class TestASteerCarriesItsMemberFromTheSend:
    def test_the_record_rides_the_payload_to_the_turn_that_takes_it(self):
        payload = steer_payload("more", BO, member=BO, written_by=MemberSender(BO, "Bo"))
        steer = _steer_offer({"hook_outbox_id": "j", "payload": payload})
        assert steer.member == MemberSender(BO, "Bo")

    def test_a_steer_no_member_wrote_names_none(self):
        payload = steer_payload("more", ANA, sender="chief_of_staff")
        assert MEMBER_SENDER_KEY not in payload
        assert _steer_offer({"hook_outbox_id": "j", "payload": payload}).member is None


class TestOnlyAUserMessageIsTaken:
    """Only a ``user`` message records its writer, so a shared turn takes no
    other: one sent as ``human`` would reach every member's calls untagged,
    free to type another member's tag."""

    @pytest.mark.parametrize("role", ["human", "system", "assistant", "ai"])
    def test_any_other_role_is_refused_in_a_shared_workspace(self, role):
        from fastapi import HTTPException

        identity = TurnIdentity(sender=ANA, owner=HOLDER, recorded_as=ANA)
        request = SimpleNamespace(messages=[SimpleNamespace(role="user"), SimpleNamespace(role=role)])
        with (
            patch(f"{_TURN}.is_holder", lambda owner: owner == HOLDER),
            pytest.raises(HTTPException) as refused,
        ):
            identity.refuse_other_roles(request)
        assert refused.value.status_code == 422

    def test_a_personal_workspace_keeps_every_role(self):
        identity = TurnIdentity(sender=ANA, owner=ANA, recorded_as=ANA)
        request = SimpleNamespace(messages=[SimpleNamespace(role="system"), SimpleNamespace(role="user")])
        with patch(f"{_TURN}.is_holder", lambda owner: owner == HOLDER):
            identity.refuse_other_roles(request)
