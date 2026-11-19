"""Who may take a shared workspace's work outside it (``share_out``).

A restricted workspace's managers choose managers only (the default), any
member, or no one. Under "no one" links may still be made and open only for
the workspace's people, and nothing is published. Open to everyone, nothing
applies and the stored value waits. A personal workspace never reaches here.
"""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.shared_workspaces import SharedRow
from src.server.services import share_audience
from src.server.services.account_contract import Org
from src.server.services.shared_workspaces import share_out
from src.server.services.shared_workspaces.access import Access, Seat, SharedAccess

pytestmark = pytest.mark.asyncio

HOLDER = "shared-00000000-0000-4000-8000-0000000000aa"
ROW = SharedRow("ws-1", HOLDER, "org-n", everyone=False, created_by="maya", share_out="managers")


def _access(role: str, value: str, *, everyone: bool = False) -> Access:
    row = replace(ROW, everyone=everyone, share_out=value)
    return Access(HOLDER, SharedAccess(row, "lee", role))


async def _may(access: Access) -> tuple[bool, bool]:
    rule, manager = await share_out.rule_for(access.shared)
    return share_out.may_share_out(rule, manager), share_out.may_change_links(rule, manager)


@pytest.fixture
def managers_active():
    with patch(
        "src.server.services.shared_workspaces.access.has_active_manager",
        AsyncMock(return_value=True),
    ) as found:
        yield found


async def test_the_stored_value_waits_while_open():
    assert share_out.in_force(replace(ROW, share_out="none")) == "none"
    assert share_out.in_force(replace(ROW, share_out="none", everyone=True)) is None
    assert share_out.in_force(None) is None


@pytest.mark.parametrize(
    ("role", "value", "out", "links"),
    [
        ("member", "managers", False, False),
        ("manager", "managers", True, True),
        ("member", "members", True, True),
        ("member", "none", False, True),
        ("manager", "none", False, True),
    ],
)
async def test_who_may_share_out_and_change_links(managers_active, role, value, out, links):
    assert await _may(_access(role, value)) == (out, links)


async def test_open_to_everyone_applies_nothing(managers_active):
    access = _access("member", "none", everyone=True)
    assert await _may(access) == (True, True)
    await share_out.refuse_publish(access)
    await share_out.refuse_link_change(access)


async def test_a_personal_workspace_is_never_refused():
    owner = Access("alice")
    assert await _may(owner) == (True, True)
    await share_out.refuse_publish(owner)
    await share_out.refuse_link_change(owner)


async def test_with_no_active_manager_a_member_shares_as_one():
    with patch(
        "src.server.services.shared_workspaces.access.has_active_manager",
        AsyncMock(return_value=False),
    ):
        assert await _may(_access("member", "managers")) == (True, True)


async def test_refusals_name_who_can(managers_active):
    with pytest.raises(HTTPException) as member:
        await share_out.refuse_link_change(_access("member", "managers"))
    assert member.value.status_code == 403
    assert member.value.detail["type"] == share_out.MANAGERS_ONLY
    with pytest.raises(HTTPException) as kept:
        await share_out.refuse_publish(_access("manager", "none"))
    assert kept.value.detail["type"] == share_out.KEPT_IN
    assert "manager" in kept.value.detail["message"]


@pytest.mark.parametrize(
    ("value", "everyone", "org", "listed", "opens"),
    [
        ("none", False, "org-n", True, True),
        ("none", False, "org-n", False, False),
        # The feature off for the organization: nothing is narrowed.
        ("none", False, None, False, True),
        ("none", True, "org-n", False, True),
        ("managers", False, "org-n", False, True),
        ("members", False, "org-n", False, True),
    ],
)
async def test_a_link_under_no_one_opens_for_the_workspace_only(value, everyone, org, listed, opens):
    row = replace(ROW, share_out=value, everyone=everyone)
    with (
        patch("src.server.database.shared_workspaces.shared_of_holder", AsyncMock(return_value=row)),
        patch("src.server.database.shared_workspaces.get_member_role", AsyncMock(return_value="member" if listed else None)),
        patch(
            "src.server.services.shared_workspaces.share_out.caller_seat",
            AsyncMock(return_value=Seat(org) if org else None),
        ),
    ):
        assert await share_out.link_opens_for(HOLDER, "viewer") is opens


async def test_admits_member_asks_for_a_holders_link_only(monkeypatch):
    org = Org(id="org-n", name="N")
    monkeypatch.setattr(share_audience, "teams_door_open", lambda: True)
    monkeypatch.setattr(share_audience, "member_of", AsyncMock(return_value=org))
    monkeypatch.setattr(
        "src.server.services.account.member_of", AsyncMock(return_value=org)
    )
    asked = AsyncMock(return_value=False)
    with patch.object(share_audience, "link_opens_for", asked):
        assert await share_audience.admits_member("viewer", "alice")
        asked.assert_not_awaited()
        assert not await share_audience.admits_member("viewer", HOLDER)
        asked.assert_awaited_once_with(HOLDER, "viewer")


async def test_a_change_is_recorded_and_told_to_the_workspace():
    from src.server.services.shared_workspaces import service

    record, told = MagicMock(), AsyncMock()
    with (
        patch("src.server.database.shared_workspaces.set_share_out", AsyncMock(return_value="managers")),
        patch("src.server.services.activity.record", record),
        patch.object(service, "publish_workspace_changed", told),
    ):
        await service.set_share_out(_access("manager", "managers").shared, "none")
        await service.set_share_out(_access("manager", "none").shared, "none")
    record.assert_called_once()
    assert record.call_args.kwargs["detail"] == {"sharing": "none", "was": "managers"}
    told.assert_awaited_once_with("org-n", "ws-1", "updated")


@pytest.mark.parametrize(
    ("role", "active", "manages"),
    [("member", True, False), ("member", False, True), ("manager", True, True)],
    ids=["member", "acting-manager", "manager"],
)
async def test_the_block_says_who_manages_as_the_gates_decide(role, active, manages):
    # A member acting for absent managers is shown the controls the
    # manager's acts (``acts_as_manager``) let them use.
    from src.server.services.shared_workspaces import service

    with patch(
        "src.server.services.shared_workspaces.access.has_active_manager",
        AsyncMock(return_value=active),
    ):
        block = await service.shared_block(_access(role, "managers").shared)
    assert (block.can_manage, block.can_share_out, block.can_change_links) == (manages,) * 3
