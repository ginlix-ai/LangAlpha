"""The trust rules for a shared workspace's holder, each refusal pinned.

A holder owns a shared workspace's computer and rows and is not a person:
nothing of any member's reaches its computer, it is never sent to the auth
service, and its policy keeps trading, own machines, connectors signed in as
a member, public links and creating shared workspaces closed.
"""

from __future__ import annotations

from types import SimpleNamespace

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.services.account import HOLDER_POLICY
from src.server.database.shared_workspaces import is_holder, new_holder_id
from src.server.services.shared_workspaces.holder import HolderNotAPerson, refuse_holder

HOLDER = "shared-5f193e9b-ead2-45b8-8b78-d5b8c2668cc6"
PERSON = "b7000000-0000-4000-8000-000000000003"


def test_a_holder_is_known_by_its_id_alone():
    assert is_holder(HOLDER) and is_holder(new_holder_id())
    assert not is_holder(PERSON) and not is_holder(None) and not is_holder("")


def test_refuse_holder_is_loud_and_lets_a_person_through():
    refuse_holder(PERSON, "x")
    refuse_holder(None, "x")
    with pytest.raises(HolderNotAPerson):
        refuse_holder(HOLDER, "x")


def test_the_holder_policy_keeps_every_personal_reach_closed():
    assert HOLDER_POLICY.trading is False
    assert HOLDER_POLICY.own_machines is False
    assert HOLDER_POLICY.user_connectors is False
    assert HOLDER_POLICY.public_links is False
    assert HOLDER_POLICY.org_library.publish is False
    assert HOLDER_POLICY.shared_workspaces.create is False


# --- never sent to the auth service --------------------------------------


def test_no_service_call_names_a_holder():
    from src.server.services.platform_client import service_headers

    assert service_headers(PERSON)["X-User-Id"] == PERSON
    with pytest.raises(HolderNotAPerson):
        service_headers(HOLDER)


@pytest.mark.asyncio
async def test_a_holders_membership_is_never_fetched():
    from src.server.services.membership import read_membership

    with pytest.raises(HolderNotAPerson):
        await read_membership(HOLDER)


@pytest.mark.asyncio
async def test_a_holders_standing_is_answered_here_with_the_closed_policy():
    from src.server.services import account

    fetch = AsyncMock()
    with (
        patch("src.server.services.membership.read_membership", fetch),
        patch(
            "src.server.database.shared_workspaces.shared_of_holder",
            AsyncMock(return_value=SimpleNamespace(org_id="org-n")),
        ),
    ):
        standing = await account.read_standing(HOLDER)
    fetch.assert_not_awaited()
    assert standing.account.org.id == "org-n"
    assert standing.account.policy is HOLDER_POLICY
    assert not standing.suspended


def test_a_holder_credential_is_refused_at_the_door():
    from src.server.utils.api import refuse_holder_credential

    refuse_holder_credential(PERSON)
    with pytest.raises(HTTPException) as e:
        refuse_holder_credential(HOLDER)
    assert e.value.status_code == 401


@pytest.mark.asyncio
async def test_a_holder_has_no_plan_tier():
    from src.server.services import features

    with (
        patch.object(features, "has_plan_gated_features", return_value=True),
        patch.object(features.settings, "HOST_MODE", "platform"),
    ):
        assert await features._access_tier(HOLDER) is None


# --- no member's credential on a holder's computer -----------------------


@pytest.mark.asyncio
async def test_no_data_token_is_minted_as_a_holder():
    from src.server.services import data_tokens

    with pytest.raises(HolderNotAPerson):
        await data_tokens._mint_once(HOLDER, "c-1", 1.0)
    with patch.object(data_tokens.db, "load", AsyncMock()) as load:
        await data_tokens.keep("c-1", HOLDER, "sb-1", AsyncMock())
    load.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_cli_token_is_kept_on_a_holders_computer():
    from src.server.services import cli_tokens

    assert await cli_tokens.keep("c-1", HOLDER, "sb-1", "daytona", AsyncMock()) is False


@pytest.mark.asyncio
async def test_the_credit_gate_never_reserves_for_a_holder():
    from src.server.services.credit_gate_port import refuse_holder as gate_refusal

    with pytest.raises(HolderNotAPerson):
        gate_refusal(HOLDER, "credit gate acquire")


def test_a_shared_turn_reaches_only_the_workspaces_own_memory():
    from ptc_agent.agent.filesystem_routes import resolve_identity_gates

    with patch(
        "src.config.settings.get_workflow_orchestration_config",
        return_value=MagicMock(enabled=True),
    ):
        gates = resolve_identity_gates(
            store=object(), user_id=HOLDER, own_data=False, workspace_id="ws-1",
            disable_subagents=False,
        )
        personal = resolve_identity_gates(
            store=object(), user_id=PERSON, own_data=True, workspace_id="ws-1",
            disable_subagents=False,
        )
    assert gates.workspace_memory is True
    assert not any(
        (gates.user_memory, gates.memo, gates.user_data, gates.workflow_fs,
         gates.workflow_tool, gates.channels)
    )
    assert personal.user_memory and personal.user_data


# --- turns posted for someone never post as the holder -------------------


@pytest.mark.asyncio
async def test_a_wakeup_naming_no_sender_in_a_shared_thread_is_dropped():
    from src.server.services.report_back._owner import turn_owner

    with patch(
        "src.server.database.conversation.threads_read.get_thread_auth_meta",
        AsyncMock(return_value={"user_id": HOLDER, "workspace_id": "ws-1"}),
    ), patch(
        "src.server.database.conversation.threads_read.get_thread_by_id",
        AsyncMock(return_value={"workspace_id": "ws-1"}),
    ):
        user_id, workspace_id = await turn_owner({}, "t-1")
    assert (user_id, workspace_id) == (None, "ws-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", [HOLDER, PERSON])
async def test_every_build_reads_a_shared_turns_profile_trimmed(owner):
    """The trim is in the turn's one profile reader, which a build between
    turns (compaction, a hand-off brief) reads as the turn does."""
    from src.server.services.shared_workspaces.turn import TurnIdentity

    whole = {
        "name": "Maya",
        "timezone": "Europe/London",
        "locale": "en-GB",
        "agent_preference": {"style": "terse"},
        "investment_profile": {"risk": "high", "holdings": "NVDA"},
    }
    with patch(
        "src.server.services.shared_workspaces.turn.get_user_profile_for_prompt",
        AsyncMock(return_value=dict(whole)),
    ):
        profile = await TurnIdentity(PERSON, owner, PERSON).profile() or {}

    if owner == HOLDER:
        assert set(profile) <= {"name", "timezone", "locale"}
        assert profile["name"] == "Maya" and profile["timezone"] == "Europe/London"
    else:
        assert profile["investment_profile"] == whole["investment_profile"]
