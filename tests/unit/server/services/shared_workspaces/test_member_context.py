"""Whether a member's own context reaches the agent in a shared workspace.

The organization grants it (a grant-gated feature a deployment can switch
off), the workspace's managers may keep it out, and only a turn a member
wrote on a runtime that takes per-call context gets it. Anything that cannot
be read grants nothing, and a cached yes is asked again before it is acted
on.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from ptc_agent.agent.transcript.sender import MemberSender
from src.config.features import FEATURES, FeatureGate
from src.config.models import FeatureFlagOverride, InfrastructureConfig
from src.server.database.shared_workspaces import SharedRow
from src.server.models.shared_workspace import MemberContextView, SharingUpdate
from src.server.services import account, features, membership
from src.server.services.account_contract import Account, Org
from src.server.services.shared_workspaces import member_context
from src.server.services.shared_workspaces.access import SharedAccess
from src.server.services.shared_workspaces.turn import TurnIdentity

HOLDER = "shared-00000000-0000-4000-8000-0000000000aa"
ORG = "org-n"
ANA = "a0000000-0000-4000-8000-000000000001"
ROW = SharedRow("ws-1", HOLDER, ORG, everyone=False, created_by=ANA, share_out="managers")
SHARED = TurnIdentity(sender=ANA, owner=HOLDER, recorded_as=ANA, shared_row=ROW)
GRANTED = {"teams": True, "shared_workspaces": True, member_context.FEATURE: True}
UNGRANTED = {"teams": True, "shared_workspaces": True}


def _standing(granted=GRANTED, *, org: str = ORG, suspended: bool = False):
    found = Account(
        org=Org(id=org, name="Org", suspended=suspended),
        features=MappingProxyType(dict(granted)),
    )
    return account.Standing(found, found)


def _reads(cached, rechecked=None):
    """``read_standing`` answering ``cached`` from the cache and ``rechecked``
    when asked past it."""

    async def read(user_id, *, recheck=False, **_):
        return (rechecked if rechecked is not None else cached) if recheck else cached

    return AsyncMock(side_effect=read)


def _deployment(**overrides):
    """The feature catalog as a deployment with shared workspaces on resolves
    it, with ``overrides`` from its ``features:`` section."""
    entries = {"teams": {"enabled": True}, "shared_workspaces": {"enabled": True}, **overrides}
    config = InfrastructureConfig(
        features={key: FeatureFlagOverride(**value) for key, value in entries.items()}
    )
    return patch("src.config.features.get_infrastructure_config", lambda: config)


def _seams(read):
    """The account read, the door open, and no user overrides."""
    return (
        patch.object(account, "read_standing", read),
        patch.object(membership, "teams_door_open", lambda: True),
        patch.object(features, "_get_overrides", AsyncMock(return_value={})),
    )


def _prefs(other: dict | None = None) -> AsyncMock:
    """The sender's preferences row, holding ``other`` as their
    ``other_preference``."""
    return AsyncMock(return_value={"other_preference": other} if other is not None else None)


async def _grant(read, identity=SHARED, *, prefs=None, **deployment):
    a, b, c = _seams(read)
    with a, b, c, _deployment(**deployment), patch.object(
        member_context, "get_user_preferences", prefs or _prefs()
    ):
        return await member_context.grant(identity)


class TestTheGrant:
    @pytest.mark.asyncio
    async def test_granted_when_everything_allows_it(self):
        read = _reads(_standing())
        assert await _grant(read) is True
        # The cached yes is asked again past the cache, and only an answer
        # counts there.
        assert [c.kwargs.get("recheck", False) for c in read.await_args_list] == [False, True]
        assert read.await_args_list[1].kwargs.get("answered_only") is True

    @pytest.mark.asyncio
    async def test_a_cached_yes_the_auth_service_now_refuses_grants_nothing(self):
        assert await _grant(_reads(_standing(), _standing(UNGRANTED))) is False

    @pytest.mark.asyncio
    async def test_a_cached_no_is_taken_as_read(self):
        read = _reads(_standing(UNGRANTED))
        assert await _grant(read) is False
        assert read.await_count == 1

    @pytest.mark.parametrize(
        "standing",
        [
            account.Standing(None, None),
            _standing(UNGRANTED),
            _standing({**GRANTED, "shared_workspaces": False}),
            _standing(org="another-org"),
            _standing(suspended=True),
            account.Standing(account.NO_ORG, account.NO_ORG),
        ],
        ids=["unknown", "not-granted", "no-shared-workspaces", "other-org", "suspended", "no-org"],
    )
    @pytest.mark.asyncio
    async def test_an_account_that_does_not_grant_it(self, standing):
        assert await _grant(_reads(standing)) is False

    @pytest.mark.asyncio
    async def test_a_failed_read_grants_nothing(self):
        assert await _grant(AsyncMock(side_effect=RuntimeError("down"))) is False

    @pytest.mark.asyncio
    async def test_the_deployment_switches_it_off_like_any_feature(self):
        off = {member_context.FEATURE: {"enabled": False}}
        assert await _grant(_reads(_standing()), **off) is False

    @pytest.mark.parametrize(
        "identity",
        [
            replace(SHARED, shared_row=replace(ROW, member_context=False)),
            replace(SHARED, shared_row=None),
            TurnIdentity(sender=ANA, owner=ANA, recorded_as=ANA),
            TurnIdentity(sender=None, owner=HOLDER, recorded_as=None, shared_row=ROW),
        ],
        ids=["workspace-off", "no-row", "personal", "no-sender"],
    )
    @pytest.mark.asyncio
    async def test_refused_before_any_account_or_preference_is_read(self, identity):
        read, prefs = _reads(_standing()), _prefs()
        assert await _grant(read, identity, prefs=prefs) is False
        read.assert_not_awaited()
        prefs.assert_not_awaited()


class TestTheMembersOwnSwitch:
    @pytest.mark.asyncio
    async def test_refused_where_the_member_turned_it_off(self):
        prefs = _prefs({member_context.OWN_SWITCH: False})
        assert await _grant(_reads(_standing()), prefs=prefs) is False
        # Read past the cache: turning it off holds from the next turn.
        prefs.assert_awaited_once_with(ANA, cached=False)

    @pytest.mark.parametrize(
        "other",
        [None, {}, {"auto_approve_handoffs": True}, {member_context.OWN_SWITCH: True}],
        ids=["no-row", "no-settings", "other-settings", "on"],
    )
    @pytest.mark.asyncio
    async def test_absent_means_on(self, other):
        prefs = AsyncMock(return_value=None) if other is None else _prefs(other)
        assert await _grant(_reads(_standing()), prefs=prefs) is True

    @pytest.mark.asyncio
    async def test_a_failed_read_keeps_it_out(self):
        prefs = AsyncMock(side_effect=RuntimeError("down"))
        assert await _grant(_reads(_standing()), prefs=prefs) is False

    @pytest.mark.asyncio
    async def test_it_does_not_outrank_the_organization(self):
        prefs = _prefs({member_context.OWN_SWITCH: True})
        assert await _grant(_reads(_standing(UNGRANTED)), prefs=prefs) is False

    def test_the_setting_is_a_strict_boolean(self):
        from src.server.models.user import OtherPreference

        assert OtherPreference(own_context_in_shared_workspaces=False).own_context_in_shared_workspaces is False
        assert OtherPreference().own_context_in_shared_workspaces is None
        with pytest.raises(ValidationError):
            OtherPreference(own_context_in_shared_workspaces="no")

    @pytest.mark.asyncio
    async def test_a_personal_turn_reads_nothing_for_it(self):
        from src.server.models.chat import ChatRequest

        prefs, read = _prefs(), _reads(_standing())
        personal = TurnIdentity(sender=ANA, owner=ANA, recorded_as=ANA)
        request = ChatRequest.model_validate({"messages": [{"role": "user", "content": "hi"}]})
        a, b, c = _seams(read)
        with a, b, c, patch.object(member_context, "get_user_preferences", prefs):
            assert await member_context.for_turn(personal, request, None, {}) is None
            # Even handed a sender, a personal turn has no row to grant it.
            assert await member_context.for_turn(personal, request, MemberSender(ANA, "Ana"), {}) is None
        prefs.assert_not_awaited()
        read.assert_not_awaited()


class TestTheFeature:
    def test_an_organizations_grant_on_by_default_in_a_deployment(self):
        spec = FEATURES[member_context.FEATURE]
        assert spec.gate is FeatureGate.GRANT and spec.enabled is True
        assert spec.requires == ("shared_workspaces",)


class TestTheManagersChoice:
    def _shared(self, on: bool = True, sharing: str = "managers") -> SharedAccess:
        return SharedAccess(replace(ROW, member_context=on, share_out=sharing), ANA, "manager")

    @pytest.mark.asyncio
    async def test_both_settings_are_one_change_recorded_and_told_once(self):
        from src.server.services.shared_workspaces import service

        record, told = MagicMock(), AsyncMock()
        stored = AsyncMock(return_value=self._shared().row)
        with (
            patch("src.server.database.shared_workspaces.update_settings", stored),
            patch("src.server.services.activity.record", record),
            patch.object(service, "publish_workspace_changed", told),
        ):
            await service.update_settings(self._shared(), sharing="none", member_context=False)
        stored.assert_awaited_once_with("ws-1", share_out="none", member_context=False)
        record.assert_called_once()
        assert record.call_args.args[1] == "shared_workspace.sharing_changed"
        assert record.call_args.kwargs["detail"] == {
            "sharing": "none", "was": "managers",
            "member_context": False, "was_member_context": True,
        }
        told.assert_awaited_once_with(ORG, "ws-1", "updated")

    @pytest.mark.asyncio
    async def test_only_what_changed_is_recorded(self):
        # Measured against the row the update found, so a request that read
        # it before another manager's save still has its own written.
        from src.server.services.shared_workspaces import service

        record, told = MagicMock(), AsyncMock()
        stored = AsyncMock(side_effect=[self._shared().row, self._shared(False).row])
        with (
            patch("src.server.database.shared_workspaces.update_settings", stored),
            patch("src.server.services.activity.record", record),
            patch.object(service, "publish_workspace_changed", told),
        ):
            await service.update_settings(self._shared(), sharing="managers", member_context=False)
            await service.update_settings(self._shared(), member_context=False)
        assert stored.await_count == 2
        record.assert_called_once()
        assert record.call_args.kwargs["detail"] == {"member_context": False, "was_member_context": True}
        told.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_patch_takes_it_from_a_manager_alone(self):
        from src.server.app import shared_workspaces as routes

        shared = self._shared(True)
        found = AsyncMock(return_value=shared)
        view = object()
        with (
            patch.object(routes, "_shared", found),
            patch.object(routes.service, "update_settings", AsyncMock()) as changed,
            patch.object(routes.service, "set_audience", AsyncMock()) as audience,
            patch.object(routes.service, "members_view", AsyncMock(return_value=view)),
        ):
            got = await routes.update_sharing(
                "ws-1",
                SharingUpdate(member_context=False),
                SimpleNamespace(user_id=ANA, computer_id=None),
            )
        assert got is view
        assert found.await_args_list[0].kwargs == {"manage": True}
        changed.assert_awaited_once_with(shared, sharing=None, member_context=False)
        audience.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_computer_token_cannot_change_it(self):
        from src.server.app import shared_workspaces as routes

        with (
            patch.object(routes, "_shared", AsyncMock(return_value=self._shared())),
            patch.object(routes.service, "update_settings", AsyncMock()) as changed,
            pytest.raises(HTTPException) as refused,
        ):
            await routes.update_sharing(
                "ws-1",
                SharingUpdate(member_context=True),
                SimpleNamespace(user_id=ANA, computer_id="computer-1"),
            )
        assert refused.value.status_code == 403
        changed.assert_not_awaited()

    def test_a_string_is_not_a_choice(self):
        with pytest.raises(ValidationError):
            SharingUpdate(member_context="false")

    def test_it_alone_is_a_change(self):
        assert SharingUpdate(member_context=True).member_context is True
        with pytest.raises(ValidationError):
            SharingUpdate()

    @pytest.mark.parametrize(
        ("granted", "on", "expected"),
        [
            (GRANTED, True, (True, True)),
            (GRANTED, False, (True, False)),
            (UNGRANTED, True, (False, True)),
        ],
    )
    @pytest.mark.asyncio
    async def test_the_people_view_says_both(self, granted, on, expected):
        a, b, c = _seams(_reads(_standing(granted)))
        with a, b, c, _deployment():
            got = await member_context.view_for(ANA, replace(ROW, member_context=on))
        assert got == MemberContextView(organization=expected[0], workspace=expected[1], offered=True)

    @pytest.mark.asyncio
    async def test_the_deployment_turning_it_off_is_not_offered(self):
        a, b, c = _seams(_reads(_standing(UNGRANTED)))
        with a, b, c, _deployment(shared_workspaces_member_context={"enabled": False}):
            got = await member_context.view_for(ANA, ROW)
        assert got.offered is False


class TestTheTurnsMember:
    @pytest.fixture(autouse=True)
    def _offered(self):
        with _deployment():
            yield

    @pytest.mark.asyncio
    async def test_the_deployment_off_reads_no_provenance(self):
        stored = AsyncMock(return_value=True)
        retry = _request().model_copy(update={"retry_of_run_id": "run-1"})
        resume = _request().model_copy(update={"hitl_response": {"i-1": {"decisions": []}}})
        with (
            _deployment(shared_workspaces_member_context={"enabled": False}),
            patch.object(member_context, "run_turn_typed", stored),
            patch.object(member_context, "resumed_turn_typed", stored),
        ):
            for request in (retry, resume):
                assert await member_context.for_turn(SHARED, request, MemberSender(ANA, "Ana"), {}, thread_id="t-1") is None
        stored.assert_not_awaited()

    @pytest.mark.parametrize("typed", [True, False])
    @pytest.mark.asyncio
    async def test_a_retry_replaying_the_checkpoint_is_told_by_the_run_it_retries(self, typed):
        retry = _request(checkpoint_id="cp-1", messages=[]).model_copy(update={"retry_of_run_id": "run-1"})
        with (
            patch.object(member_context, "grant", AsyncMock(return_value=True)),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
            patch.object(member_context, "run_turn_typed", AsyncMock(return_value=typed)),
        ):
            found = await member_context.for_turn(SHARED, retry, MemberSender(ANA, "Ana"), {})
        assert (found is not None) is typed

    @pytest.mark.asyncio
    async def test_granted_it_names_the_member_as_their_messages_do(self):
        from src.server.services.shared_workspaces.turn import short_id

        profile = {"name": "Ana", "agent_preference": {"tone": "terse"}, "investment_profile": {"risk_level": "high"}}
        with (
            patch.object(member_context, "grant", AsyncMock(return_value=True)),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
        ):
            found = await member_context.for_turn(
                SHARED, _request(), MemberSender(ANA, "Ana (1a2b3c)"), profile
            )
        assert found.user_id == ANA and found.name == "Ana (1a2b3c)"
        assert found.handle == short_id(ANA) and len(found.handle) == 6
        # Answer preferences only: nothing of the investment profile.
        assert dict(found.agent_preference) == {"tone": "terse"}
        # Their memory's aliases are taken in this workspace, with the
        # deployment's own key.
        assert found.workspace_id == ROW.workspace_id
        assert found.alias_key == b"k" * 32

    @pytest.mark.asyncio
    async def test_the_alias_key_is_the_migrations_read_once_a_process(self, monkeypatch):
        read = AsyncMock(return_value=b"k" * 32)
        monkeypatch.setattr(member_context, "_alias_key", None)
        monkeypatch.setattr(member_context, "get_server_key", read)
        assert await member_context.alias_key() == await member_context.alias_key() == b"k" * 32
        read.assert_awaited_once_with("member_memory_alias")

    @pytest.mark.asyncio
    async def test_none_without_a_grant_or_a_record(self):
        sender = MemberSender(ANA, "Ana")
        with patch.object(member_context, "grant", AsyncMock(return_value=False)):
            assert await member_context.for_turn(SHARED, _request(), sender, {}) is None
        granted = AsyncMock(return_value=True)
        with patch.object(member_context, "grant", granted):
            assert await member_context.for_turn(SHARED, _request(), None, {}) is None
        granted.assert_not_awaited()

    @pytest.mark.parametrize(
        ("fields", "typed"),
        [
            ({}, True),
            ({"query_type": "system"}, False),
            ({"query_type": "wakeup"}, False),
            ({"origin": {"type": "automation"}}, False),
            ({"origin": {"type": "agent"}}, False),
            ({"origin": {"type": "system"}}, False),
        ],
    )
    @pytest.mark.asyncio
    async def test_only_a_message_the_member_typed_carries_it(self, fields, typed):
        granted = AsyncMock(return_value=True)
        with (
            patch.object(member_context, "grant", granted),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
        ):
            found = await member_context.for_turn(SHARED, _request(**fields), MemberSender(ANA, "Ana"), {})
        assert (found is not None) is typed
        assert granted.await_count == int(typed)

    @pytest.mark.parametrize("typed", [True, False])
    @pytest.mark.asyncio
    async def test_a_retry_carries_it_only_for_a_turn_the_member_typed(self, typed):
        # A retry's request comes back with no origin, so an automation's
        # run is told from the turn's stored row.
        retry = _request().model_copy(update={"retry_of_run_id": "run-1"})
        stored = AsyncMock(return_value=typed)
        with (
            patch.object(member_context, "grant", AsyncMock(return_value=True)),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
            patch.object(member_context, "run_turn_typed", stored),
        ):
            found = await member_context.for_turn(SHARED, retry, MemberSender(ANA, "Ana"), {})
        assert (found is not None) is typed
        stored.assert_awaited_once_with("run-1")

    @pytest.mark.parametrize("typed", [True, False])
    @pytest.mark.asyncio
    async def test_a_resume_carries_it_only_for_a_turn_the_member_typed(self, typed):
        # A resume comes back with no origin too, so the turn it carries on
        # is told from the thread's stored rows.
        resume = _request(hitl_response={"i-1": {"decisions": [{"type": "approve"}]}})
        stored = AsyncMock(return_value=typed)
        with (
            patch.object(member_context, "grant", AsyncMock(return_value=True)),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
            patch.object(member_context, "resumed_turn_typed", stored),
        ):
            found = await member_context.for_turn(
                SHARED, resume, MemberSender(ANA, "Ana"), {}, thread_id="t-1"
            )
            unknown = await member_context.for_turn(SHARED, resume, MemberSender(ANA, "Ana"), {})
        assert (found is not None) is typed
        assert unknown is None  # no thread to check: withheld
        stored.assert_awaited_once_with("t-1")


    @pytest.mark.parametrize("typed", [True, False, None])
    @pytest.mark.parametrize("messages", [[], [{"role": "user", "content": "hi"}]])
    @pytest.mark.asyncio
    async def test_a_regenerate_carries_it_only_for_a_turn_the_member_typed(self, typed, messages):
        # A regenerate replays the turn from the checkpoint, or resends its
        # stored message when it never ran, with no origin either way; the
        # turn it names was read before START wrote its row anew.
        regenerate = _request(checkpoint_id="cp-1", fork_from_turn=3).model_copy(
            update={"messages": messages}
        )
        with (
            patch.object(member_context, "grant", AsyncMock(return_value=True)),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
        ):
            found = await member_context.for_turn(
                SHARED, regenerate, MemberSender(ANA, "Ana"), {}, thread_id="t-1", replaced_typed=typed
            )
        assert (found is not None) is bool(typed)  # unread: withheld

    @pytest.mark.asyncio
    async def test_the_replaced_turn_is_read_only_for_a_fork_where_it_may_reach_them(self):
        stored = AsyncMock(return_value=True)
        fork = _request(checkpoint_id="cp-1", fork_from_turn=3)
        with patch.object(member_context, "turn_typed", stored), _deployment():
            assert await member_context.replaced_turn_typed(SHARED, fork, "t-1") is True
            assert await member_context.replaced_turn_typed(SHARED, _request(), "t-1") is None
            personal = TurnIdentity(sender=ANA, owner=ANA, recorded_as=ANA)
            assert await member_context.replaced_turn_typed(personal, fork, "t-1") is None
            off = TurnIdentity(sender=ANA, owner=HOLDER, recorded_as=ANA, shared_row=replace(ROW, member_context=False))
            assert await member_context.replaced_turn_typed(off, fork, "t-1") is None
        stored.assert_awaited_once_with("t-1", 3)
        with patch.object(member_context, "turn_typed", stored), _deployment(
            shared_workspaces_member_context={"enabled": False}
        ):
            assert await member_context.replaced_turn_typed(SHARED, fork, "t-1") is None
        stored.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_replay_naming_no_turn_is_withheld(self):
        replay = _request(checkpoint_id="cp-1").model_copy(update={"messages": []})
        sent = _request(checkpoint_id="cp-1")  # a new message sent from a checkpoint
        with (
            patch.object(member_context, "grant", AsyncMock(return_value=True)),
            patch.object(member_context, "alias_key", AsyncMock(return_value=b"k" * 32)),
        ):
            assert await member_context.for_turn(SHARED, replay, MemberSender(ANA, "Ana"), {}, thread_id="t-1") is None
            assert await member_context.for_turn(SHARED, sent, MemberSender(ANA, "Ana"), {}, thread_id="t-1") is not None

def _request(**fields):
    from src.server.models.chat import ChatRequest

    return ChatRequest.model_validate({"messages": [{"role": "user", "content": "hi"}], **fields})
